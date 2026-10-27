package s3

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"io"
	"testing"
)

type objectStub struct {
	input *awss3.PutObjectInput
	err   error
}

func (s *objectStub) PutObject(ctx context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	s.input = input
	return &awss3.PutObjectOutput{ETag: aws.String("etag")}, s.err
}
func (s *objectStub) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewBufferString("manifest")), ETag: aws.String("etag")}, s.err
}
func TestMetadataWritesAreConditional(t *testing.T) {
	for _, previous := range []string{"", "previous"} {
		api := &objectStub{}
		client := Client{Bucket: "bucket", objects: api}
		if _, err := client.PutObject(context.Background(), "runs/run.json", []byte("data"), previous); err != nil {
			t.Fatal(err)
		}
		if previous == "" {
			if aws.ToString(api.input.IfNoneMatch) != "*" || api.input.IfMatch != nil {
				t.Fatal(api.input)
			}
		} else if aws.ToString(api.input.IfMatch) != previous || api.input.IfNoneMatch != nil {
			t.Fatal(api.input)
		}
	}
}
func TestObjectErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		code string
		want error
	}{{"NoSuchKey", storage.ErrNotFound}, {"NotFound", storage.ErrNotFound}, {"PreconditionFailed", storage.ErrConflict}, {"ConditionalRequestConflict", storage.ErrConflict}} {
		err := objectError(&smithy.GenericAPIError{Code: tc.code})
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v", tc.code, err)
		}
	}
}
