package s3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type AWSClient interface {
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
}

type uploadAPI interface {
	UploadObject(context.Context, *transfermanager.UploadObjectInput, ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}
type headAPI interface {
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
}

type Client struct {
	uploader uploadAPI
	verifier headAPI
	Timeout  time.Duration
	Bucket   string
	client   AWSClient
	logger   *slog.Logger
}

// Ensure Client implement storage.Client.
var _ storage.Client = (*Client)(nil)

func NewClient(ctx context.Context, logger *slog.Logger, bucket string, region string, forcePathStyle bool, endpoint string) (*Client, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("s3 client: bucket name is empty")
	}

	if logger == nil {
		logger = slog.Default()
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithLogger(sdkLogger(ctx, logger)),
		awsconfig.WithHTTPClient(&http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}),
	)

	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	sdkClient := awss3.NewFromConfig(
		awsConfig,
		func(opts *awss3.Options) {
			opts.UsePathStyle = forcePathStyle

			if endpoint != "" {
				opts.BaseEndpoint = &endpoint
			}
		},
	)
	return &Client{Bucket: bucket, client: sdkClient, verifier: sdkClient,
		uploader: transfermanager.New(sdkClient, func(o *transfermanager.Options) {
			o.Concurrency = 2
			o.PartSizeBytes = 16 * 1024 * 1024
			o.FailTimeout = 5 * time.Second
		}),
		Timeout: 30 * time.Minute, logger: logger.With("component", "s3")}, nil
}

func (c *Client) ListObjects(ctx context.Context) (result []storage.Object, err error) {
	ctx, finish := telemetry.Operation(ctx, "s3.list")
	defer func() { finish(err) }()
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	paginator := awss3.NewListObjectsV2Paginator(
		c.client,
		&awss3.ListObjectsV2Input{
			Bucket: &c.Bucket,
		},
	)

	var objects []storage.Object

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects from bucket %q: %w", c.Bucket, err)
		}

		for _, obj := range page.Contents {
			c.log().DebugContext(ctx, "S3 object found", "event", "storage.object_found", "bucket", c.Bucket, "key", aws.ToString(obj.Key))
			objects = append(objects, storage.Object{
				Key:          aws.ToString(obj.Key),
				LastModified: aws.ToTime(obj.LastModified),
				Size:         aws.ToInt64(obj.Size),
			})
		}
	}

	return objects, nil
}

func (c *Client) DeleteObject(ctx context.Context, key string) (err error) {
	ctx, finish := telemetry.Operation(ctx, "s3.delete")
	defer func() { finish(err) }()
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	input := awss3.DeleteObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}

	_, err = c.client.DeleteObject(ctx, &input)

	if err != nil {
		return fmt.Errorf("delete key %q from bucket %q: %w", key, c.Bucket, err)
	}

	c.log().InfoContext(ctx,
		"object deleted", "event", "storage.deleted",
		"bucket", c.Bucket,
		"key", key,
	)

	return nil
}

func (c *Client) log() *slog.Logger {
	if c.logger == nil {
		return slog.Default()
	}
	return c.logger
}

// Upload commits the object then verifies its size and SHA-256 metadata.
// Transport checksums are handled by the AWS SDK; multipart ETags are not hashes.
func (c *Client) Upload(ctx context.Context, key string, file *os.File, size int64, checksum string) (err error) {
	ctx, finish := telemetry.Operation(ctx, "s3.upload")
	defer func() { finish(err) }()
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	_, err = c.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: file, ContentLength: aws.Int64(size),
		Metadata: map[string]string{"sha256": checksum}, ContentType: aws.String("application/octet-stream"),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return fmt.Errorf("upload %q: %w", key, err)
	}
	head, err := c.verifier.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("verify %q: %w", key, err)
	}
	if head.ContentLength == nil || *head.ContentLength != size || head.Metadata["sha256"] != checksum {
		return fmt.Errorf("verify %q: object size or SHA-256 metadata mismatch", key)
	}
	return nil
}

// SDK diagnostics use the same structured log stream as command events.
func sdkLogger(ctx context.Context, logger *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(classification logging.Classification, format string, args ...interface{}) {
		level := slog.LevelDebug
		if classification == logging.Warn {
			level = slog.LevelWarn
		}
		logger.Log(ctx, level, "AWS SDK diagnostic", "event", "aws.diagnostic", "component", "s3", "message", fmt.Sprintf(format, args...))
	})
}
