//go:build integration

package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
)

// TestNetApp uses a unique prefix in an existing bucket. It never lists or deletes
// other objects. Run explicitly with S3_INTEGRATION_TEST=1 and the integration tag.
func TestNetApp(t *testing.T) {
	if os.Getenv("S3_INTEGRATION_TEST") != "1" {
		t.Skip("set S3_INTEGRATION_TEST=1 to contact a real S3 backend")
	}
	cfg, err := config.LoadStorage()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint == "" {
		t.Fatal("S3_ENDPOINT is required for NetApp qualification")
	}
	var options []ClientOption
	switch cfg.RequestChecksum {
	case "when_required":
		options = append(options, WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired))
	case "when_supported":
		options = append(options, WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenSupported))
	default:
		t.Fatal("set S3_REQUEST_CHECKSUM explicitly to when_required or when_supported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client, err := NewClient(ctx, nil, cfg.Bucket, cfg.Region, cfg.ForcePathStyle, cfg.Endpoint, cfg.InsecureSkipVerify, options...)
	if err != nil {
		t.Fatal(err)
	}
	client.Timeout = cfg.Timeout
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		t.Fatal(err)
	}
	prefix := "qdrant-maintenance-integration/" + hex.EncodeToString(id) + "/"
	t.Logf("policy=%s prefix=%s", cfg.RequestChecksum, prefix)
	cleanup := func(t *testing.T, key string) {
		t.Helper()
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := client.DeleteObject(cleanupCtx, key); err != nil {
				t.Errorf("cleanup failed; residual object %s: %v", key, err)
			}
		})
	}
	for _, tc := range []struct {
		name string
		size int64
	}{{"single", 1024}, {"multipart", 33 * 1024 * 1024}} {
		t.Run(tc.name, func(t *testing.T) {
			key := prefix + tc.name + ".snapshot"
			cleanup(t, key)
			file, err := os.CreateTemp(t.TempDir(), "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			hash := sha256.New()
			if _, err := io.CopyN(io.MultiWriter(file, hash), rand.Reader, tc.size); err != nil {
				t.Fatal(err)
			}
			digest := hex.EncodeToString(hash.Sum(nil))
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := client.Upload(ctx, key, file, tc.size, digest); err != nil {
				t.Fatal(err)
			}
			body, _, err := client.GetObject(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			readHash := sha256.New()
			count, readErr := io.Copy(readHash, body)
			closeErr := body.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatal(err)
			}
			if count != tc.size || hex.EncodeToString(readHash.Sum(nil)) != digest {
				t.Fatal("downloaded content hash or size mismatch")
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := client.Upload(ctx, key, file, tc.size, digest); err == nil {
				t.Fatal("existing snapshot was overwritten")
			} else {
				if !errors.Is(objectError(err), storage.ErrConflict) {
					t.Fatalf("expected conditional conflict, got %v", err)
				}
			}
		})
	}
	t.Run("manifest", func(t *testing.T) {
		key := prefix + "manifest.json"
		cleanup(t, key)
		etag, err := client.PutObject(ctx, key, []byte(`{"revision":1}`), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutObject(ctx, key, []byte(`{}`), ""); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("expected create conflict, got %v", err)
		}
		updated := []byte(`{"revision":2}`)
		if _, err := client.PutObject(ctx, key, updated, etag); err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutObject(ctx, key, []byte(`{}`), etag); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("expected stale ETag conflict, got %v", err)
		}
		body, _, err := client.GetObject(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(body)
		if err := errors.Join(readErr, body.Close()); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, updated) {
			t.Fatal("manifest content mismatch")
		}
	})
}
