package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
)

type memoryStore struct {
	data               map[string][]byte
	revisions          map[string]int
	metadata           map[string]storage.Details
	objects            []storage.Object
	deleted, uploaded  []string
	listErr, deleteErr error
}

func (s *memoryStore) ListObjects(context.Context) ([]storage.Object, error) {
	objects := append([]storage.Object{}, s.objects...)
	for key, data := range s.data {
		objects = append(objects, storage.Object{Key: key, Size: int64(len(data))})
	}
	return objects, s.listErr
}
func (s *memoryStore) DeleteObject(ctx context.Context, key string) error {
	s.deleted = append(s.deleted, key)
	if s.deleteErr == nil {
		delete(s.data, key)
		delete(s.metadata, key)
	}
	return s.deleteErr
}
func (s *memoryStore) Upload(ctx context.Context, key string, f *os.File, size int64, checksum string) error {
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return errors.New("size mismatch")
	}
	s.init()
	s.data[key] = data
	s.metadata[key] = storage.Details{Size: size, SHA256: checksum}
	s.uploaded = append(s.uploaded, key)
	return nil
}
func testApp(t *testing.T, store *memoryStore) (*application, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	app := newApplication(slog.New(slog.NewJSONHandler(&out, nil)))
	app.storage = func(context.Context) (snapshotStore, error) { return store, nil }
	app.qconfig = func(bool) (config.QdrantConfig, error) {
		t.Error("unexpected Qdrant configuration lookup")
		return config.QdrantConfig{}, errors.New("unexpected")
	}
	app.retentionConfig = func() (config.RetentionConfig, error) { return config.RetentionConfig{KeepLast: 1}, nil }
	return app, &out
}
func TestListReadsBucketWithoutQdrant(t *testing.T) {
	store := &memoryStore{objects: []storage.Object{
		{Key: "docs/node-0/a.snapshot", LastModified: time.Unix(1, 0), Size: 17},
		{Key: "docs/node-1/b.snapshot", LastModified: time.Unix(2, 0), Size: 19},
		{Key: "docs/no-node.snapshot", LastModified: time.Unix(0, 0)},
		{Key: "other/node-0/c.snapshot"}, {Key: "docs/readme.txt"},
	}}
	app, out := testApp(t, store)
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	records := readLogs(t, out)
	snapshots := []map[string]any{}
	for _, record := range records {
		if record["msg"] == "snapshot found" {
			snapshots = append(snapshots, record)
		}
	}
	if len(snapshots) != 2 || snapshots[0]["node"] != "node-1" || snapshots[1]["node"] != "node-0" {
		t.Fatalf("snapshots=%+v", snapshots)
	}

	if len(store.uploaded) != 0 || len(store.deleted) != 0 {
		t.Fatal("listing mutated bucket")
	}
}
func TestListEmptyAndFailure(t *testing.T) {
	app, out := testApp(t, &memoryStore{})
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, out), "snapshots listed", "snapshot_count", float64(0))
	expected := errors.New("list denied")
	app, out = testApp(t, &memoryStore{listErr: expected})
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); !errors.Is(err, expected) {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, out), "command failed", "error", expected.Error())
}
func TestDistributedCreateContinuesAfterPartialFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("fail=", fail), func(t *testing.T) {
			store := &memoryStore{}
			app, out := testApp(t, store)
			app.qconfig = func(requireNodes bool) (config.QdrantConfig, error) {
				if !requireNodes {
					t.Fatal("backup did not require node configuration")
				}
				return config.QdrantConfig{Nodes: []config.Node{{ID: "node-0", URL: "http://node-0"}, {ID: "node-1", URL: "http://node-1"}}}, nil
			}
			deleted := []string{}
			app.qdrant = func(cfg config.QdrantConfig, node config.Node) backup.Qdrant {
				server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.URL.Path == "/cluster":
						peer := 1
						if node.ID == "node-1" {
							peer = 2
						}
						fmt.Fprintf(w, `{"result":{"status":"enabled","peer_id":%d,"peers":{"1":{},"2":{}}}}`, peer)
					case r.URL.Path == "/collections":
						fmt.Fprint(w, `{"result":{"collections":[{"name":"z"},{"name":"a"}]}}`)
					case r.Method == http.MethodPost:
						if fail && node.ID == "node-0" && strings.Contains(r.URL.Path, "/a/") {
							w.WriteHeader(500)
							return
						}
						fmt.Fprint(w, `{"result":{"name":"same.snapshot","creation_time":"2026-10-27T10:00:00","size":7}}`)
					case r.Method == http.MethodGet:
						fmt.Fprint(w, "archive")
					case r.Method == http.MethodDelete:
						deleted = append(deleted, node.ID+r.URL.Path)
						fmt.Fprint(w, `{"result":true}`)
					default:
						t.Error(r.Method, r.URL)
					}
				}))
				q := qdrant.NewClient(node.URL, "")
				q.HTTPClient = server.Client()
				return q
			}
			err := app.run(context.Background(), []string{"cli", "snapshots", "create", "--all"})
			if (err != nil) != fail {
				t.Fatalf("error=%v", err)
			}
			want := 4
			if fail {
				want = 3
			}
			if len(store.uploaded) != want || len(deleted) != want {
				t.Fatalf("uploads=%v deleted=%v", store.uploaded, deleted)
			}
			found := map[string]bool{}
			for _, key := range store.uploaded {
				if found[key] {
					t.Fatal("node key collision")
				}
				found[key] = true
			}
			records := readLogs(t, out)
			assertLog(t, records, "backup run finished", "successful_count", float64(want))
			assertLog(t, records, "backup run finished", "expected_count", float64(4))
			id := records[0]["run_id"]
			if id == nil {
				t.Fatal("missing run ID")
			}
			for _, record := range records {
				if record["run_id"] != id || record["event"] == nil {
					t.Fatalf("missing correlation or event: %+v", record)
				}
			}
			results := []map[string]any{}
			for _, record := range records {
				if record["msg"] == "backup completed" || record["msg"] == "backup failed" {
					results = append(results, record)
				}
			}
			if len(results) != 4 {
				t.Fatalf("results=%+v", results)
			}
			if fail && (results[0]["error"] == nil || results[0]["stored"] != false) {
				t.Fatal(results[0])
			}

		})
	}
}
func TestRetentionCommandSafety(t *testing.T) {
	store := &memoryStore{objects: []storage.Object{{Key: "docs/node-0/old.snapshot", LastModified: time.Unix(1, 0)}, {Key: "docs/node-0/new.snapshot", LastModified: time.Unix(2, 0)}}}
	app, _ := testApp(t, store)
	if err := app.run(context.Background(), []string{"cli", "retention", "plan"}); err != nil {
		t.Fatal(err)
	}
	if len(store.deleted) != 0 {
		t.Fatal("plan deleted data")
	}
	app.retentionConfig = func() (config.RetentionConfig, error) { return config.RetentionConfig{KeepLast: 0}, nil }
	if err := app.run(context.Background(), []string{"cli", "retention", "apply"}); err == nil {
		t.Fatal("zero retention applied without force")
	}
	if len(store.deleted) != 0 {
		t.Fatal("unsafe deletion")
	}
	store.deleteErr = errors.New("delete denied")
	if err := app.run(context.Background(), []string{"cli", "retention", "apply", "--force"}); !errors.Is(err, store.deleteErr) {
		t.Fatal(err)
	}
	if len(store.deleted) != 2 {
		t.Fatalf("partial failures did not attempt remaining deletes: %v", store.deleted)
	}
}
func TestHelpDoesNotInitializeClients(t *testing.T) {
	for _, args := range [][]string{{"cli", "help"}, {"cli", "snapshots", "--help"}, {"cli", "snapshots", "create", "--help"}, {"cli", "retention", "apply", "--help"}} {
		app, _ := testApp(t, &memoryStore{})
		app.storage = func(context.Context) (snapshotStore, error) { t.Fatal("client initialized for help"); return nil, nil }
		if err := app.run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotNamesUseStructuredLogs(t *testing.T) {
	app, logs := testApp(t, &memoryStore{objects: []storage.Object{{Key: "docs/node-0/backup.snapshot"}}})
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, logs), "snapshot found", "name", "backup.snapshot")
}
func TestErrorLogLevelSuppressesSuccessfulListings(t *testing.T) {
	app, _ := testApp(t, &memoryStore{objects: []storage.Object{{Key: "docs/node-0/backup.snapshot"}}})
	var logs bytes.Buffer
	app.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Fatal(logs.String())
	}
}
func TestEmptySnapshotListLogsZeroCount(t *testing.T) {
	app, logs := testApp(t, &memoryStore{})
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, logs), "snapshots listed", "snapshot_count", float64(0))
}

func TestListLogsOnlySnapshotCount(t *testing.T) {
	objects := []storage.Object{
		{Key: "docs/node-0/a.snapshot"},
		{Key: "docs/node-1/b.snapshot"},
		{Key: "docs/node-0/c.snapshot"},
	}
	objects = append(objects, storage.Object{Key: "docs/no-node.snapshot"})
	for i := 0; i < 6; i++ {
		objects = append(objects, storage.Object{Key: fmt.Sprintf("docs/file-%d.txt", i)})
	}
	app, _ := testApp(t, &memoryStore{objects: objects})
	var logs bytes.Buffer
	app.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := app.run(context.Background(), []string{"cli", "snapshots", "list", "docs"}); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&logs)
	found := false
	for decoder.More() {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"object_count", "objects", "matching_snapshots"} {
			if _, ok := entry[key]; ok {
				t.Fatalf("unexpected field %s: %+v", key, entry)
			}
		}
		if entry["msg"] == "snapshots listed" {
			found = true
			if entry["snapshot_count"] != float64(3) {
				t.Fatalf("expected 3 snapshots among 10 objects: %+v", entry)
			}
		}
	}
	if !found {
		t.Fatal("missing snapshot listing log")
	}
}

func readLogs(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	records := []map[string]any{}
	decoder := json.NewDecoder(logs)
	for {
		var entry map[string]any
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid structured log: %v", err)
		}
		records = append(records, entry)
	}
	return records
}
func assertLog(t *testing.T, records []map[string]any, message, key string, want any) {
	t.Helper()
	for _, record := range records {
		if record["msg"] == message {
			if record[key] != want {
				t.Fatalf("%s: %s=%v want %v", message, key, record[key], want)
			}
			return
		}
	}
	t.Fatalf("missing log %q in %+v", message, records)
}

func (s *memoryStore) init() {
	if s.data == nil {
		s.data = map[string][]byte{}
		s.revisions = map[string]int{}
		s.metadata = map[string]storage.Details{}
	}
}
func (s *memoryStore) GetObject(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	data, ok := s.data[key]
	if !ok {
		return nil, "", storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), strconv.Itoa(s.revisions[key]), nil
}
func (s *memoryStore) PutObject(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.init()
	_, exists := s.data[key]
	if (etag == "" && exists) || (etag != "" && (!exists || etag != strconv.Itoa(s.revisions[key]))) {
		return "", storage.ErrConflict
	}
	s.data[key] = bytes.Clone(data)
	s.revisions[key]++
	return strconv.Itoa(s.revisions[key]), nil
}
func (s *memoryStore) HeadObject(ctx context.Context, key string) (storage.Details, error) {
	if err := ctx.Err(); err != nil {
		return storage.Details{}, err
	}
	d, ok := s.metadata[key]
	if !ok {
		return d, storage.ErrNotFound
	}
	return d, nil
}
