package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

func testEngine(t *testing.T, store *memoryStore) (Engine, *int, *int) {
	t.Helper()
	created, deleted := 0, 0
	server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			created++
			fmt.Fprint(w, `{"result":{"name":"file.snapshot","creation_time":"2026-10-27T10:00:00","size":7}}`)
		case http.MethodGet:
			fmt.Fprint(w, "archive")
		case http.MethodDelete:
			record, err := (Repository{Store: store}).Load(context.Background(), "run")
			if err != nil || !record.Manifest.Entries[0].Stored {
				t.Fatalf("source deleted without durable upload record: %+v %v", record, err)
			}
			deleted++
			fmt.Fprint(w, `{"result":true}`)
		}
	}))
	client := qdrant.NewClient(server.URL, "")
	client.HTTPClient = server.Client()
	engine := Engine{Repo: Repository{Store: store}, Store: store, Clients: map[string]backup.Qdrant{"node-0": client}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Inspect: func(context.Context) (topology.Inventory, error) { return inventory(), nil }}
	return engine, &created, &deleted
}
func TestManifestFailureNeverDeletesUnrecordedSource(t *testing.T) {
	for _, failAt := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			store := newStore()
			store.failWriteAt = failAt
			engine, created, deleted := testEngine(t, store)
			m, err := engine.Create(context.Background(), New("run", "test", "", inventory(), false))
			if (err != nil) != (failAt != 0) {
				t.Fatal(err)
			}
			if failAt != 0 && *deleted != 0 {
				t.Fatal("deleted source after persistence failure")
			}
			if failAt == 1 && *created != 0 {
				t.Fatal("created snapshot without initial manifest")
			}
			if failAt == 0 {
				if m.State != "complete" || *deleted != 1 {
					t.Fatal(m)
				}
				record, err := engine.Repo.Load(context.Background(), "run")
				if err != nil || record.Manifest.Writer != "" {
					t.Fatalf("record=%+v error=%v", record, err)
				}
			}
		})
	}
}
func TestTopologyChangeLeavesIncompleteRun(t *testing.T) {
	store := newStore()
	engine, _, _ := testEngine(t, store)
	engine.Inspect = func(context.Context) (topology.Inventory, error) {
		return topology.Inventory{}, fmt.Errorf("membership changed")
	}
	m, err := engine.Create(context.Background(), New("run", "test", "", inventory(), true))
	if err == nil || m.State != "incomplete" {
		t.Fatalf("manifest=%+v error=%v", m, err)
	}
	record, err := engine.Repo.Load(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(record.Manifest)
	if len(data) == 0 || record.Manifest.Writer != "" {
		t.Fatal(record)
	}
}
