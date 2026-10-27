package runs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
	"io"
	"os"
	"strconv"
	"testing"
)

// Memory storage mocks S3 conditional writes and records mutation ordering.
type memoryStore struct {
	data        map[string][]byte
	revisions   map[string]int
	details     map[string]storage.Details
	deleted     []string
	writes      int
	failWriteAt int
	deleteError error
}

func newStore() *memoryStore {
	return &memoryStore{data: map[string][]byte{}, revisions: map[string]int{}, details: map[string]storage.Details{}}
}
func (s *memoryStore) GetObject(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	b, ok := s.data[key]
	if !ok {
		return nil, "", storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), strconv.Itoa(s.revisions[key]), nil
}
func (s *memoryStore) PutObject(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.writes++
	if s.writes == s.failWriteAt {
		return "", fmt.Errorf("simulated write failure")
	}
	_, exists := s.data[key]
	if (etag == "" && exists) || (etag != "" && (!exists || etag != strconv.Itoa(s.revisions[key]))) {
		return "", storage.ErrConflict
	}
	s.data[key] = bytes.Clone(data)
	s.revisions[key]++
	return strconv.Itoa(s.revisions[key]), nil
}
func (s *memoryStore) ListObjects(context.Context) ([]storage.Object, error) {
	result := []storage.Object{}
	for key, data := range s.data {
		result = append(result, storage.Object{Key: key, Size: int64(len(data))})
	}
	return result, nil
}
func (s *memoryStore) DeleteObject(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.deleteError != nil {
		return s.deleteError
	}
	s.deleted = append(s.deleted, key)
	delete(s.data, key)
	delete(s.details, key)
	return nil
}
func (s *memoryStore) HeadObject(ctx context.Context, key string) (storage.Details, error) {
	if err := ctx.Err(); err != nil {
		return storage.Details{}, err
	}
	d, ok := s.details[key]
	if !ok {
		return d, storage.ErrNotFound
	}
	return d, nil
}
func inventory() topology.Inventory {
	return topology.Inventory{ClusterID: topology.ClusterIdentity([]topology.Node{{ID: "node-0", PeerID: 1}}), Nodes: []topology.Node{{ID: "node-0", PeerID: 1}}, Collections: []string{"docs"}, Pairs: []topology.Pair{{Collection: "docs", Node: "node-0"}}}
}
func TestManifestConditionalUpdates(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	repo := Repository{Store: store}
	record := Record{Manifest: New("run-1", "test", "", inventory(), false)}
	if err := repo.Save(ctx, &record); err != nil {
		t.Fatal(err)
	}
	stale, err := repo.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	record.Manifest.State = "incomplete"
	record.Manifest.Writer = ""
	if err := repo.Save(ctx, &record); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, &stale); err == nil {
		t.Fatal("stale writer overwrote newer manifest")
	}
	stale = record
	stale.ETag = ""
	if err := repo.Save(ctx, &stale); err == nil {
		t.Fatal("create overwrote manifest")
	}
	records, err := repo.List(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%v err=%v", records, err)
	}
}
func TestInvalidManifestFailsClosed(t *testing.T) {
	for _, mutate := range []func(*Manifest){func(m *Manifest) { m.SchemaVersion = 99 }, func(m *Manifest) { m.Entries = nil }, func(m *Manifest) { m.State = "complete" }, func(m *Manifest) { m.Entries[0].Key = "other/node/bad.snapshot" }} {
		record := Record{Manifest: New("run", "test", "", inventory(), false)}
		mutate(&record.Manifest)
		if err := (Repository{Store: newStore()}).Save(context.Background(), &record); err == nil {
			t.Fatalf("invalid manifest accepted: %+v", record.Manifest)
		}
	}
}

func (s *memoryStore) Upload(ctx context.Context, key string, file *os.File, size int64, checksum string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	s.data[key] = data
	s.details[key] = storage.Details{Size: size, SHA256: checksum}
	return nil
}
