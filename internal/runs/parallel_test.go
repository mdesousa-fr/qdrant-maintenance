package runs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

// Protect the existing test store: uploads and coordinator writes now overlap.
type parallelStore struct {
	mu sync.Mutex
	*memoryStore
	failCheckpoint bool
	ambiguous      bool
}

func (s *parallelStore) GetObject(ctx context.Context, key string) (io.ReadCloser, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.GetObject(ctx, key)
}
func (s *parallelStore) PutObject(ctx context.Context, key string, data []byte, etag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return "", err
	}
	if s.failCheckpoint {
		for _, e := range m.Entries {
			if e.Stored {
				if s.ambiguous {
					if _, err := s.memoryStore.PutObject(ctx, key, data, etag); err != nil {
						return "", err
					}
				}
				return "", errors.New("checkpoint unavailable")
			}
		}
	}
	return s.memoryStore.PutObject(ctx, key, data, etag)
}
func (s *parallelStore) Upload(ctx context.Context, key string, file *os.File, size int64, checksum string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.Upload(ctx, key, file, size, checksum)
}
func (s *parallelStore) HeadObject(ctx context.Context, key string) (storage.Details, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.HeadObject(ctx, key)
}

type parallelProbe struct {
	mu                          sync.Mutex
	active                      map[string]bool
	maxActive, created, deleted int
	overlap                     bool
	started                     chan string
	downloads                   chan string
	release                     chan struct{}
	blockDownload               bool
	failCollection              string
}
type parallelQdrant struct {
	backup.Qdrant // unused discovery methods are provided by Engine.Inspect
	node          string
	probe         *parallelProbe
	repo          Repository
}

func (q *parallelQdrant) CreateSnapshot(ctx context.Context, collection string) (qdrant.Snapshot, error) {
	p := q.probe
	p.mu.Lock()
	p.overlap = p.overlap || p.active[q.node]
	p.active[q.node] = true
	p.maxActive = max(p.maxActive, len(p.active))
	p.created++
	p.mu.Unlock()
	p.started <- q.node
	select {
	case <-p.release:
	case <-ctx.Done():
		return qdrant.Snapshot{}, ctx.Err()
	}
	if collection == p.failCollection {
		p.mu.Lock()
		delete(p.active, q.node)
		p.mu.Unlock()
		return qdrant.Snapshot{}, errors.New("creation failed")
	}
	return qdrant.Snapshot{Name: collection + ".snapshot", Size: 7}, nil
}
func (q *parallelQdrant) DownloadSnapshot(ctx context.Context, collection, name string, dst io.Writer) error {
	record, err := q.repo.Load(ctx, "parallel")
	if err != nil {
		return err
	}
	recorded := false
	for _, entry := range record.Manifest.Entries {
		if entry.Node == q.node && entry.Collection == collection {
			recorded = entry.Snapshot != nil && entry.Snapshot.Name == name
		}
	}
	if !recorded {
		return errors.New("download before durable snapshot identity")
	}
	q.probe.downloads <- q.node
	if q.probe.blockDownload {
		<-ctx.Done()
		return ctx.Err()
	}
	_, err = io.WriteString(dst, "archive")
	return err
}
func (q *parallelQdrant) DeleteSnapshot(ctx context.Context, collection, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := q.repo.Load(ctx, "parallel")
	if err != nil {
		return err
	}
	found := false
	for _, e := range r.Manifest.Entries {
		if e.Node == q.node && e.Collection == collection {
			found = e.Stored && e.Snapshot != nil && e.Snapshot.Name == name
		}
	}
	if !found {
		return errors.New("source deletion without durable upload")
	}
	q.probe.mu.Lock()
	defer q.probe.mu.Unlock()
	q.probe.deleted++
	delete(q.probe.active, q.node)
	return nil
}
func parallelFixture(nodes, concurrency int) (Engine, Manifest, *parallelProbe, *parallelStore) {
	store := &parallelStore{memoryStore: newStore()}
	probe := &parallelProbe{active: map[string]bool{}, started: make(chan string, 32), downloads: make(chan string, 32), release: make(chan struct{})}
	inv := topology.Inventory{Collections: []string{"a", "b"}}
	clients := map[string]backup.Qdrant{}
	repo := Repository{Store: store}
	for i := range nodes {
		node := fmt.Sprintf("node-%d", i)
		inv.Nodes = append(inv.Nodes, topology.Node{ID: node, PeerID: uint64(i + 1)})
		for _, collection := range inv.Collections {
			inv.Pairs = append(inv.Pairs, topology.Pair{Node: node, Collection: collection})
		}
		clients[node] = &parallelQdrant{node: node, probe: probe, repo: repo}
	}
	inv.ClusterID = topology.ClusterIdentity(inv.Nodes)
	engine := Engine{Repo: repo, Store: store, Clients: clients, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ParallelNodes: concurrency, Inspect: func(context.Context) (topology.Inventory, error) { return inv, nil }}
	return engine, New("parallel", "test", "", inv, false), probe, store
}

type parallelResult struct {
	manifest Manifest
	err      error
}

func runParallel(ctx context.Context, e Engine, m Manifest) <-chan parallelResult {
	done := make(chan parallelResult, 1)
	go func() { m, err := e.Create(ctx, m); done <- parallelResult{m, err} }()
	return done
}
func waitResult(t *testing.T, done <-chan parallelResult) parallelResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not finish")
		return parallelResult{}
	}
}
func waitStarted(t *testing.T, ch <-chan string, n int) {
	t.Helper()
	for range n {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("expected concurrent progress")
		}
	}
}
func TestParallelNodesBoundedAndCheckpointed(t *testing.T) {
	for _, limit := range []int{0, 1, 2, 99} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			e, m, p, _ := parallelFixture(3, limit)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := runParallel(ctx, e, m)
			expected := min(max(limit, 1), 3)
			waitStarted(t, p.started, expected)
			close(p.release)
			r := waitResult(t, done)
			if r.err != nil || r.manifest.State != "complete" {
				t.Fatalf("%+v %v", r.manifest, r.err)
			}
			if p.maxActive != expected || p.overlap || p.created != 6 || p.deleted != 6 {
				t.Fatalf("probe=%+v", p)
			}
			for i, e := range r.manifest.Entries {
				if e.Node != m.Entries[i].Node || e.Collection != m.Entries[i].Collection || !e.Stored || !e.SourceDeleted {
					t.Fatal("entry order/state changed", e)
				}
			}
		})
	}
}
func TestParallelOrdinaryFailureContinues(t *testing.T) {
	e, m, p, _ := parallelFixture(3, 2)
	p.failCollection = "a"
	close(p.release)
	r := waitResult(t, runParallel(context.Background(), e, m))
	if r.err == nil || r.manifest.State != "incomplete" || p.created != 6 || p.deleted != 3 {
		t.Fatalf("%+v %+v", r, p)
	}
	for _, entry := range r.manifest.Entries {
		if entry.Stored != (entry.Collection == "b") {
			t.Fatal(entry)
		}
	}
}
func TestParallelCancellationAndRetry(t *testing.T) {
	e, m, p, _ := parallelFixture(3, 2)
	p.blockDownload = true
	close(p.release)
	ctx, cancel := context.WithCancel(context.Background())
	done := runParallel(ctx, e, m)
	waitStarted(t, p.downloads, 2)
	cancel()
	r := waitResult(t, done)
	if !errors.Is(r.err, context.Canceled) || r.manifest.State != "incomplete" || r.manifest.Writer != "" || p.deleted != 0 || p.created != 2 {
		t.Fatalf("%+v %+v", r, p)
	}
	for _, entry := range r.manifest.Entries {
		if entry.Stored || entry.SourceDeleted {
			t.Fatal(entry)
		}
	}
	p.blockDownload = false
	// Reset probe bookkeeping for operations cancelled before cleanup.
	p.active = map[string]bool{}
	result, err := e.Retry(context.Background(), m.ID, false)
	if err != nil || result.State != "complete" || p.created != 6 || p.deleted != 6 {
		t.Fatalf("%+v %v %+v", result, err, p)
	}
}
func TestParallelCheckpointFailureRetainsSourcesAndReservation(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			e, m, p, s := parallelFixture(3, 2)
			s.failCheckpoint = true
			s.ambiguous = ambiguous
			close(p.release)
			r := waitResult(t, runParallel(context.Background(), e, m))
			if r.err == nil || p.deleted != 0 {
				t.Fatalf("%+v %+v", r, p)
			}
			record, err := e.Repo.Load(context.Background(), m.ID)
			if err != nil || record.Manifest.Writer == "" || record.Manifest.State == "complete" {
				t.Fatalf("%+v %v", record, err)
			}
			s.failCheckpoint = false
			p.active = map[string]bool{}
			if _, err = e.Retry(context.Background(), m.ID, false); err == nil {
				t.Fatal("missing takeover requirement")
			}
			result, err := e.Retry(context.Background(), m.ID, true)
			if err != nil || result.State != "complete" || p.deleted != 6 {
				t.Fatalf("%+v %v %+v", result, err, p)
			}
		})
	}
}

func TestParallelMissingClientIsRecorded(t *testing.T) {
	e, m, p, _ := parallelFixture(2, 2)
	close(p.release)
	delete(e.Clients, "node-0")
	r := waitResult(t, runParallel(context.Background(), e, m))
	if r.err == nil || r.manifest.State != "incomplete" {
		t.Fatalf("%+v", r)
	}
	for _, entry := range r.manifest.Entries {
		if entry.Node == "node-0" && entry.Error == "" {
			t.Fatal("missing recorded error", entry)
		}
		if entry.Node == "node-1" && !entry.Stored {
			t.Fatal("other node skipped", entry)
		}
	}
}
