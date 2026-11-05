package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type parallelMultipart struct {
	transferS3
	mu              sync.Mutex
	active, maximum int
	sizes           []int64
	started         chan struct{}
	release         chan struct{}
}

func (s *parallelMultipart) UploadPart(ctx context.Context, in *awss3.UploadPartInput, opts ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	s.mu.Lock()
	s.active++
	s.maximum = max(s.maximum, s.active)
	s.mu.Unlock()
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	n, err := io.Copy(io.Discard, in.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	s.size += n
	s.sizes = append(s.sizes, n)
	return &awss3.UploadPartOutput{ETag: aws.String("etag")}, err
}
func TestConfiguredMultipartTransfers(t *testing.T) {
	for _, tc := range []UploadOptions{{Concurrency: 1, PartSizeMiB: 5}, {Concurrency: 3, PartSizeMiB: 7}} {
		t.Run(fmt.Sprintf("workers-%d-part-%dMiB", tc.Concurrency, tc.PartSizeMiB), func(t *testing.T) {
			sdk := &parallelMultipart{started: make(chan struct{}, 8), release: make(chan struct{})}
			client := Client{Bucket: "bucket", uploader: newTransferManager(sdk, tc, aws.RequestChecksumCalculationUnset), verifier: sdk}
			file, err := os.CreateTemp(t.TempDir(), "multipart")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			const size = int64(21 * 1024 * 1024)
			if err = file.Truncate(size); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- client.Upload(ctx, "docs/node/file.snapshot", file, size, "digest") }()
			for range tc.Concurrency {
				select {
				case <-sdk.started:
				case <-time.After(5 * time.Second):
					t.Fatal("multipart workers not configured")
				}
			}
			close(sdk.release)
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("multipart did not finish")
			}
			if err != nil {
				t.Fatal(err)
			}
			part := tc.PartSizeMiB * 1024 * 1024
			if sdk.maximum != tc.Concurrency || int64(len(sdk.sizes)) != (size+part-1)/part || sdk.complete != 1 || sdk.head != 1 {
				t.Fatalf("maximum=%d parts=%v", sdk.maximum, sdk.sizes)
			}
			for _, n := range sdk.sizes {
				if n > part {
					t.Fatalf("part size=%d, configured=%d", n, part)
				}
			}
		})
	}
}
func TestInvalidUploadOptionsFailBeforeAWSConfiguration(t *testing.T) {
	for _, opts := range []UploadOptions{{Concurrency: -1}, {PartSizeMiB: 4}, {PartSizeMiB: 5121}} {
		if _, err := NewClient(context.Background(), nil, "bucket", "region", false, "", false, WithUploadOptions(opts)); err == nil {
			t.Fatalf("accepted %+v", opts)
		}
	}
}
