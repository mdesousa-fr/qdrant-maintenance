package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
)

type fakeS3Client struct {
	listObjectsCalls              int
	listObjectsOutputs            []*awss3.ListObjectsV2Output
	listObjectsContinuationTokens []*string
	listObjectsErr                error

	deleteObjectInput *awss3.DeleteObjectInput
	deleteObjectErr   error
}

func (c *fakeS3Client) ListObjectsV2(ctx context.Context, input *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	c.listObjectsContinuationTokens = append(c.listObjectsContinuationTokens, input.ContinuationToken)
	c.listObjectsCalls++
	if c.listObjectsErr != nil {
		return nil, c.listObjectsErr
	}
	output := c.listObjectsOutputs[c.listObjectsCalls-1]
	return output, nil
}

func (c *fakeS3Client) DeleteObject(ctx context.Context, input *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	c.deleteObjectInput = input
	if c.deleteObjectErr != nil {
		return nil, c.deleteObjectErr
	}
	return nil, nil
}

func TestListObjects(t *testing.T) {
	fake := fakeS3Client{
		listObjectsOutputs: []*awss3.ListObjectsV2Output{
			{
				Contents: []types.Object{{
					Key:          aws.String("documents/node-0/snapshot-1.snapshot"),
					Size:         aws.Int64(1024),
					LastModified: aws.Time(time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC)),
				}},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("next"),
			},
			{
				Contents: []types.Object{{
					Key:          aws.String("documents/node-0/snapshot-2.snapshot"),
					Size:         aws.Int64(1024),
					LastModified: aws.Time(time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC)),
				}},
				IsTruncated: aws.Bool(false),
			},
		},
	}

	client := Client{
		Bucket: "qdrant-snapshots",
		client: &fake,
	}

	objects, err := client.ListObjects(context.Background())

	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}

	if len(objects) != 2 {
		t.Fatalf("len(objects) = %d, want %d", len(objects), 2)
	}

	if fake.listObjectsCalls != 2 {
		t.Fatalf("calls = %d, want %d", fake.listObjectsCalls, 2)
	}

	if fake.listObjectsContinuationTokens[0] != nil {
		t.Errorf("continuationTokens[0] = %v, want nil", fake.listObjectsContinuationTokens[0])
	}

	if aws.ToString(fake.listObjectsContinuationTokens[1]) != "next" {
		t.Errorf("continuationTokens[1] = %q, want %q", aws.ToString(fake.listObjectsContinuationTokens[1]), "next")
	}

	expected := storage.Object{
		Key:          "documents/node-0/snapshot-1.snapshot",
		Size:         1024,
		LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
	}

	if objects[0] != expected {
		t.Errorf("got %+v, want %+v", objects[0], expected)
	}
}

func TestListObjectsError(t *testing.T) {
	expectedErr := errors.New("connection refused")
	fake := fakeS3Client{
		listObjectsErr: expectedErr,
	}
	client := Client{
		Bucket: "qdrant-snapshots",
		client: &fake,
	}
	objects, err := client.ListObjects(context.Background())
	if objects != nil {
		t.Fatalf("objects = %+v, want nil", objects)
	}
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("err = %v, want wrapped error %v", err, expectedErr)
	}
}

func TestNewClientEmptyBucket(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty bucket", input: ""},
		{name: "space bucket", input: "   "},
		{name: "tab bucket", input: "\t"},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				client, err := NewClient(context.Background(), nil, test.input, "eu-west1", true, "empty")
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				if client != nil {
					t.Fatalf("client = %+v, want nil", client)
				}
			},
		)
	}
}

func TestDeleteObject(t *testing.T) {
	expected := struct {
		Bucket string
		Key    string
	}{
		Bucket: "qdrant-snapshots", Key: "documents/node-0/snapshot-1.snapshot",
	}

	fake := fakeS3Client{}
	client := Client{
		Bucket: "qdrant-snapshots",
		client: &fake,
	}

	if err := client.DeleteObject(context.Background(), "documents/node-0/snapshot-1.snapshot"); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if fake.deleteObjectInput == nil {
		t.Fatal("deleteObjectInput = nil, want non-nil")
	}

	if aws.ToString(fake.deleteObjectInput.Bucket) != expected.Bucket {
		t.Errorf("bucket = %q, want %q", aws.ToString(fake.deleteObjectInput.Bucket), expected.Bucket)
	}

	if aws.ToString(fake.deleteObjectInput.Key) != expected.Key {
		t.Errorf("key = %q, want %q", aws.ToString(fake.deleteObjectInput.Key), expected.Key)
	}
}

func TestDeleteObjectError(t *testing.T) {
	expectedErr := errors.New("access denied")

	fake := fakeS3Client{deleteObjectErr: expectedErr}
	client := Client{
		Bucket: "qdrant-snapshots",
		client: &fake,
	}

	err := client.DeleteObject(context.Background(), "documents/node-0/snapshot-1.snapshot")

	if err == nil {
		t.Fatalf("expected an error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("err = %v, want wrapped error %v", err, expectedErr)
	}
}
