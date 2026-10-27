package s3

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Embed only to satisfy unused SDK methods. All operations exercised by uploads
// are overridden, so no HTTP calls or credential lookup can occur.
type transferS3 struct {
	checksumMismatch bool
	*awss3.Client
	put, parts, complete, abort, head int
	size                              int64
	checksum                          string
	uploadErr                         bool
	headErr                           bool
	mismatch                          bool
}

func (f *transferS3) PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.put++
	if f.uploadErr {
		return nil, errors.New("upload denied")
	}
	if in.IfNoneMatch == nil || *in.IfNoneMatch != "*" {
		return nil, errors.New("missing overwrite protection")
	}
	if in.Bucket == nil || *in.Bucket != "bucket" || in.Key == nil || *in.Key != "docs/node/file.snapshot" {
		return nil, errors.New("incorrect destination")
	}
	n, err := io.Copy(io.Discard, in.Body)
	if err != nil {
		return nil, err
	}
	f.size = n
	f.checksum = in.Metadata["sha256"]
	return &awss3.PutObjectOutput{ETag: aws.String("etag")}, nil
}
func (f *transferS3) CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	f.checksum = in.Metadata["sha256"]
	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
}
func (f *transferS3) UploadPart(ctx context.Context, in *awss3.UploadPartInput, opts ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	f.parts++
	if f.uploadErr {
		return nil, errors.New("part failed")
	}
	n, err := io.Copy(io.Discard, in.Body)
	f.size += n
	return &awss3.UploadPartOutput{ETag: aws.String("etag")}, err
}
func (f *transferS3) CompleteMultipartUpload(ctx context.Context, in *awss3.CompleteMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	if in.IfNoneMatch == nil || *in.IfNoneMatch != "*" {
		return nil, errors.New("missing multipart overwrite protection")
	}
	f.complete++
	return &awss3.CompleteMultipartUploadOutput{}, nil
}
func (f *transferS3) AbortMultipartUpload(ctx context.Context, in *awss3.AbortMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	f.abort++
	return &awss3.AbortMultipartUploadOutput{}, nil
}
func (f *transferS3) HeadObject(ctx context.Context, in *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	f.head++
	if f.headErr {
		return nil, errors.New("head denied")
	}
	n := f.size
	if f.mismatch {
		n++
	}
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(n), Metadata: map[string]string{"sha256": func() string {
		if f.checksumMismatch {
			return "wrong"
		}
		return f.checksum
	}()}}, nil
}
func TestUploadAndVerify(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		size                                           int64
		uploadErr, headErr, mismatch, checksumMismatch bool
	}{
		{name: "single object", size: 17},
		{name: "multipart", size: 6 * 1024 * 1024},
		{name: "put failure", size: 17, uploadErr: true},
		{name: "multipart abort", size: 6 * 1024 * 1024, uploadErr: true},
		{name: "head failure", size: 17, headErr: true},
		{name: "size mismatch", size: 17, mismatch: true},
		{name: "checksum metadata mismatch", size: 17, checksumMismatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &transferS3{uploadErr: tc.uploadErr, headErr: tc.headErr, mismatch: tc.mismatch, checksumMismatch: tc.checksumMismatch}
			upload := transfermanager.New(sdk, func(o *transfermanager.Options) {
				o.Concurrency = 1
				o.PartSizeBytes = 5 * 1024 * 1024
				o.MultipartUploadThreshold = 5 * 1024 * 1024
			})
			c := Client{Bucket: "bucket", uploader: upload, verifier: sdk}
			file, err := os.CreateTemp(t.TempDir(), "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if err = file.Truncate(tc.size); err != nil {
				t.Fatal(err)
			}
			err = c.Upload(context.Background(), "docs/node/file.snapshot", file, tc.size, "digest")
			if (err != nil) != (tc.uploadErr || tc.headErr || tc.mismatch || tc.checksumMismatch) {
				t.Fatalf("error=%v", err)
			}
			if tc.uploadErr && sdk.head != 0 {
				t.Fatal("verified a failed upload")
			}
			if tc.size > 5*1024*1024 {
				if tc.uploadErr {
					if sdk.abort != 1 || sdk.complete != 0 {
						t.Fatalf("abort=%d complete=%d", sdk.abort, sdk.complete)
					}
				} else if sdk.parts != 2 || sdk.complete != 1 {
					t.Fatalf("parts=%d complete=%d", sdk.parts, sdk.complete)
				}
			} else if sdk.put != 1 {
				t.Fatalf("put=%d", sdk.put)
			}
		})
	}
}
