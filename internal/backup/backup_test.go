package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
)

type uploadFunc func(context.Context, string, *os.File, int64, string) error

func (f uploadFunc) Upload(c context.Context, k string, b *os.File, n int64, h string) error {
	return f(c, k, b, n, h)
}

func TestBackupSafety(t *testing.T) {
	for _, tc := range []struct {
		name           string
		createStatus   int
		downloadStatus int
		size           uint64
		checksum       string
		uploadErr      bool
		cleanupErr     bool
		keep           bool
		wantUpload     bool
		wantDelete     bool
		wantErr        bool
	}{
		{name: "success", size: 7, wantUpload: true, wantDelete: true},
		{name: "keep source", size: 7, keep: true, wantUpload: true},
		{name: "create failure", createStatus: 500, wantErr: true},
		{name: "download failure", downloadStatus: 500, size: 7, wantErr: true},
		{name: "short download", size: 8, wantErr: true},
		{name: "checksum mismatch", size: 7, checksum: strings.Repeat("0", 64), wantErr: true},
		{name: "upload or verification failure", size: 7, uploadErr: true, wantUpload: true, wantErr: true},
		{name: "cleanup failure", size: 7, cleanupErr: true, wantUpload: true, wantDelete: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			uploaded, deleted := false, false
			var tempPath string
			sum := sha256.Sum256([]byte("archive"))
			checksum := hex.EncodeToString(sum[:])
			expected := tc.checksum
			if expected == "" {
				expected = checksum
			}
			server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("api-key") != "secret" {
					t.Error("missing API key")
				}
				switch r.Method {
				case http.MethodPost:
					if r.URL.Query().Get("wait") != "true" {
						t.Error("creation must be synchronous")
					}
					if tc.createStatus != 0 {
						w.WriteHeader(tc.createStatus)
						return
					}
					fmt.Fprintf(w, `{"result":{"name":"backup.snapshot","creation_time":"2026-10-27T10:00:00","size":%d,"checksum":%q}}`, tc.size, expected)
				case http.MethodGet:
					if r.URL.Path != "/collections/docs/snapshots/backup.snapshot" {
						t.Error(r.URL.Path)
					}
					if tc.downloadStatus != 0 {
						w.WriteHeader(tc.downloadStatus)
						return
					}
					fmt.Fprint(w, "archive")
				case http.MethodDelete:
					if !uploaded {
						t.Error("source deleted before upload verification")
					}
					deleted = true
					if tc.cleanupErr {
						w.WriteHeader(500)
						return
					}
					fmt.Fprint(w, `{"result":true}`)
				default:
					t.Fatal(r.Method)
				}
			}))
			q := qdrant.NewClient(server.URL, "secret")
			q.HTTPClient = server.Client()
			store := uploadFunc(func(ctx context.Context, key string, f *os.File, n int64, h string) error {
				uploaded = true
				tempPath = f.Name()
				data, err := io.ReadAll(f)
				if err != nil {
					t.Fatal(err)
				}
				if key != "docs/node-0/backup.snapshot" || string(data) != "archive" || n != 7 || h != checksum {
					t.Fatalf("unexpected upload: %s %q %d %s", key, data, n, h)
				}
				if tc.uploadErr {
					return errors.New("storage unavailable")
				}
				return nil
			})
			result, err := Run(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), q, store, "docs", "node-0", tc.keep)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
			if uploaded != tc.wantUpload || deleted != tc.wantDelete {
				t.Fatalf("uploaded=%t deleted=%t", uploaded, deleted)
			}
			if result.Stored != (uploaded && !tc.uploadErr) {
				t.Fatalf("stored=%t", result.Stored)
			}
			if tc.wantErr && result.Error == "" {
				t.Fatal("result missing error")
			}
			if tempPath != "" {
				if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
					t.Fatalf("temporary file left behind: %v", err)
				}
			}
			entries, err := os.ReadDir(os.Getenv("TMPDIR"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary files remain: %v %v", entries, err)
			}
		})
	}
}

func TestCancelledBackupNeverDeletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := testhttp.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("HTTP call after cancellation") }))
	q := qdrant.NewClient(server.URL, "")
	q.HTTPClient = server.Client()
	store := uploadFunc(func(context.Context, string, *os.File, int64, string) error {
		t.Fatal("upload after cancellation")
		return nil
	})
	_, err := Run(ctx, slog.Default(), q, store, "docs", "node-0", false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
