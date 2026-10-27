package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
)

type objectAPI interface {
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

func objectError(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return fmt.Errorf("%w: %v", storage.ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict", "412", "409":
			return fmt.Errorf("%w: %v", storage.ErrConflict, err)
		}
	}
	return err
}
func (c *Client) GetObject(ctx context.Context, key string) (io.ReadCloser, string, error) {
	cancel := func() {}
	if c.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
	}
	response, err := c.objects.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)})
	if err != nil {
		cancel()
		return nil, "", objectError(err)
	}
	return &cancelBody{ReadCloser: response.Body, cancel: cancel}, aws.ToString(response.ETag), nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
func (c *Client) PutObject(ctx context.Context, key string, data []byte, previousETag string) (string, error) {
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	input := &awss3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentType: aws.String("application/json")}
	if previousETag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(previousETag)
	}
	result, err := c.objects.PutObject(ctx, input)
	if err != nil {
		return "", objectError(err)
	}
	if aws.ToString(result.ETag) == "" {
		return "", fmt.Errorf("metadata write returned no ETag")
	}
	return *result.ETag, nil
}
func (c *Client) HeadObject(ctx context.Context, key string) (storage.Details, error) {
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	head, err := c.verifier.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)})
	if err != nil {
		return storage.Details{}, objectError(err)
	}
	if head.ContentLength == nil {
		return storage.Details{}, fmt.Errorf("object %s has no size", key)
	}
	return storage.Details{Size: *head.ContentLength, SHA256: head.Metadata["sha256"], ETag: aws.ToString(head.ETag)}, nil
}
