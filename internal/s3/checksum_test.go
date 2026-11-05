package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type checksumTransport struct {
	t         *testing.T
	mu        sync.Mutex
	supported bool
	writes    int
}

func (r *checksumTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Body != nil {
		_, err := io.Copy(io.Discard, req.Body)
		if err != nil {
			return nil, err
		}
	}
	headers := http.Header{"Etag": {`"etag"`}}
	response := ""
	switch {
	case req.Method == "HEAD":
		r.t.Error("unexpected real HEAD; verifier should be stubbed")
	case req.Method == "POST" && req.URL.Query().Has("uploads"):
		response = `<InitiateMultipartUploadResult><UploadId>test</UploadId></InitiateMultipartUploadResult>`
	case req.Method == "POST":
		if req.Header.Get("If-None-Match") != "*" {
			r.t.Error("multipart overwrite protection missing")
		}
		response = `<CompleteMultipartUploadResult><ETag>"etag"</ETag></CompleteMultipartUploadResult>`
	case req.Method == "PUT":
		r.writes++
		hasChecksum := req.Header.Get("X-Amz-Sdk-Checksum-Algorithm") != "" || req.Header.Get("X-Amz-Checksum-Crc32") != "" || strings.Contains(req.Header.Get("X-Amz-Content-Sha256"), "TRAILER")
		if hasChecksum != r.supported {
			r.t.Errorf("checksum=%v, want %v (headers=%v)", hasChecksum, r.supported, req.Header)
		}
		if !r.supported && (req.Header.Get("X-Amz-Trailer") != "" || strings.Contains(req.Header.Get("Content-Encoding"), "aws-chunked")) {
			r.t.Error("unexpected streaming checksum")
		}
		if !req.URL.Query().Has("uploadId") && req.Header.Get("If-None-Match") != "*" && req.Header.Get("If-Match") != `"etag"` {
			r.t.Error("overwrite protection missing")
		}
	}
	if (req.Method == "POST" && req.URL.Query().Has("uploads")) || (req.Method == "PUT" && !req.URL.Query().Has("uploadId") && !strings.HasSuffix(req.URL.Path, "manifest.json")) {
		if req.Header.Get("X-Amz-Meta-Sha256") != "digest" {
			r.t.Error("SHA-256 metadata lost")
		}
	}
	return &http.Response{StatusCode: 200, Header: headers, Body: io.NopCloser(strings.NewReader(response)), Request: req}, nil
}

func TestChecksumHTTP(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		policy                         aws.RequestChecksumCalculation
		awsPolicy                      string
		uploadChecksum, directChecksum bool
	}{
		{"default", aws.RequestChecksumCalculationUnset, "", true, true},
		{"AWS env only", aws.RequestChecksumCalculationUnset, "when_required", true, false},
		{"required overrides AWS", aws.RequestChecksumCalculationWhenRequired, "when_supported", false, false},
		{"supported overrides AWS", aws.RequestChecksumCalculationWhenSupported, "when_required", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_ACCESS_KEY_ID", "test")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
			t.Setenv("AWS_SESSION_TOKEN", "")
			t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", tc.awsPolicy)
			transport := &checksumTransport{t: t, supported: tc.uploadChecksum}
			previous := http.DefaultTransport
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = previous })
			c, err := NewClient(context.Background(), nil, "bucket", "us-east-1", true, "https://s3.test", false, WithRequestChecksumCalculation(tc.policy))
			if err != nil {
				t.Fatal(err)
			}
			// Head metadata verification is covered separately; exercise real SDK upload serialization here.
			verifier := &transferS3{checksum: "digest"}
			c.verifier = verifier
			for _, size := range []int64{17, 33 * 1024 * 1024} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					file, err := os.CreateTemp(t.TempDir(), "snapshot")
					if err != nil {
						t.Fatal(err)
					}
					defer file.Close()
					if err := file.Truncate(size); err != nil {
						t.Fatal(err)
					}
					verifier.size = size
					if err := c.Upload(context.Background(), "test.snapshot", file, size, "digest"); err != nil {
						t.Fatal(err)
					}
				})
			}
			transport.supported = tc.directChecksum
			if _, err := c.PutObject(context.Background(), "manifest.json", []byte(`{}`), ""); err != nil {
				t.Fatal(err)
			}
			if _, err := c.PutObject(context.Background(), "manifest.json", []byte(`{"revision":2}`), `"etag"`); err != nil {
				t.Fatal(err)
			}
			if transport.writes != 6 {
				t.Fatalf("expected simple, three multipart parts and two manifest writes, got %d", transport.writes)
			}
		})
	}
}
