package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
	"net/http"
	"testing"
)

func TestPerformanceFlagsValidateBeforeClients(t *testing.T) {
	for _, command := range [][]string{{"snapshots", "create", "--all"}, {"runs", "retry", "run"}} {
		for _, env := range []string{"invalid", "0", "-1"} {
			t.Run(command[0]+env, func(t *testing.T) {
				t.Setenv("QDRANT_PARALLEL_NODES", env)
				app, _ := testApp(t, &memoryStore{})
				app.storage = func(context.Context) (snapshotStore, error) {
					t.Fatal("initialized storage before validation")
					return nil, nil
				}
				if err := app.run(context.Background(), append([]string{"cli"}, command...)); err == nil {
					t.Fatal("accepted invalid environment")
				}
			})
		}
	}
	for _, command := range [][]string{{"snapshots", "create", "--parallel-nodes", "0", "--all"}, {"runs", "retry", "--parallel-nodes", "-2", "run"}} {
		app, _ := testApp(t, &memoryStore{})
		app.storage = func(context.Context) (snapshotStore, error) {
			t.Fatal("initialized storage before validation")
			return nil, nil
		}
		if err := app.run(context.Background(), append([]string{"cli"}, command...)); err == nil {
			t.Fatal("accepted invalid flag")
		}
	}
}
func TestExplicitParallelFlagOverridesInvalidEnvironment(t *testing.T) {
	t.Setenv("QDRANT_PARALLEL_NODES", "invalid")
	sentinel := errors.New("configuration reached")
	app, _ := testApp(t, &memoryStore{})
	app.qconfig = func(bool) (config.QdrantConfig, error) { return config.QdrantConfig{}, sentinel }
	err := app.run(context.Background(), []string{"cli", "snapshots", "create", "--parallel-nodes", "3", "--all"})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	app, _ = testApp(t, &memoryStore{})
	app.storage = func(context.Context) (snapshotStore, error) { return nil, sentinel }
	err = app.run(context.Background(), []string{"cli", "runs", "retry", "--parallel-nodes", "3", "run"})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

// One node keeps the command's in-memory store serial while still checking that
// both commands pass the requested concurrency to the engine (which caps it).
func TestParallelSettingReachesCreateAndRetry(t *testing.T) {
	t.Setenv("QDRANT_PARALLEL_NODES", "2")
	app, out := testApp(t, &memoryStore{})
	app.qconfig = func(bool) (config.QdrantConfig, error) {
		return config.QdrantConfig{Nodes: []config.Node{{ID: "node-0", URL: "http://node-0"}}}, nil
	}
	server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cluster":
			fmt.Fprint(w, `{"result":{"status":"enabled","peer_id":1,"peers":{"1":{}}}}`)
		case r.URL.Path == "/collections":
			fmt.Fprint(w, `{"result":{"collections":[{"name":"docs"}]}}`)
		case r.Method == http.MethodPost:
			fmt.Fprint(w, `{"result":{"name":"file.snapshot","creation_time":"2026-10-27T10:00:00","size":7}}`)
		case r.Method == http.MethodGet:
			fmt.Fprint(w, "archive")
		case r.Method == http.MethodDelete:
			fmt.Fprint(w, `{"result":true}`)
		}
	}))
	app.qdrant = func(config.QdrantConfig, config.Node) backup.Qdrant {
		client := qdrant.NewClient(server.URL, "")
		client.HTTPClient = server.Client()
		return client
	}
	if err := app.run(context.Background(), []string{"cli", "snapshots", "create", "--all"}); err != nil {
		t.Fatal(err)
	}
	id := app.runID
	records := readLogs(t, out)
	assertLog(t, records, "backup concurrency configured", "requested_parallel_nodes", float64(2))
	assertLog(t, records, "backup run finished", "parallel_nodes", float64(1))
	out.Reset()
	if err := app.run(context.Background(), []string{"cli", "runs", "retry", id}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, out), "backup concurrency configured", "requested_parallel_nodes", float64(2))
	out.Reset()
	t.Setenv("QDRANT_PARALLEL_NODES", "invalid")
	if err := app.run(context.Background(), []string{"cli", "runs", "retry", "--parallel-nodes", "3", id}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, out), "backup concurrency configured", "requested_parallel_nodes", float64(3))
	out.Reset()
	if err := app.run(context.Background(), []string{"cli", "snapshots", "create", "--parallel-nodes", "3", "--all"}); err != nil {
		t.Fatal(err)
	}
	assertLog(t, readLogs(t, out), "backup concurrency configured", "requested_parallel_nodes", float64(3))
}
