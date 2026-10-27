package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
)

func TestAllowApply(t *testing.T) {
	for _, tc := range []struct {
		keep           int
		force, wantErr bool
	}{{5, false, false}, {5, true, false}, {0, false, true}, {0, true, false}, {-1, true, true}} {
		if err := allowApply(tc.keep, tc.force); (err != nil) != tc.wantErr {
			t.Fatalf("keep=%d force=%t error=%v", tc.keep, tc.force, err)
		}
	}
}
func TestRoutingAndValidation(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		wantErr bool
	}{
		{nil, true}, {[]string{"cli"}, true}, {[]string{"cli", "unknown"}, true},
		{[]string{"cli", "version"}, false}, {[]string{"cli", "version", "extra"}, true},
		{[]string{"cli", "snapshots", "unknown"}, true}, {[]string{"cli", "retention", "unknown"}, true},
		{[]string{"cli", "snapshots", "create", "--all", "docs"}, true},
		{[]string{"cli", "snapshots", "list", "docs", "extra"}, true},
		{[]string{"cli", "collections", "--json"}, true},
	} {
		t.Run(fmt.Sprint(tc.args), func(t *testing.T) {
			app, logs := testApp(t, &memoryStore{})
			err := app.run(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
			records := readLogs(t, logs)
			if tc.wantErr {
				assertLog(t, records, "command failed", "level", "ERROR")
			} else {
				assertLog(t, records, "application version", "version", version)
			}
		})
	}
}
func TestCollectionsLogsAndHTTPFailures(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			app, logs := testApp(t, &memoryStore{})
			app.qconfig = func(requireNodes bool) (config.QdrantConfig, error) {
				if requireNodes {
					t.Fatal("collections required node config")
				}
				return config.QdrantConfig{URL: "http://qdrant.test"}, nil
			}
			app.qdrant = func(config.QdrantConfig, config.Node) backup.Qdrant {
				server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if fail {
						w.WriteHeader(500)
						return
					}
					fmt.Fprint(w, `{"result":{"collections":[{"name":"z"},{"name":"a"}]}}`)
				}))
				client := qdrant.NewClient(server.URL, "")
				client.HTTPClient = server.Client()
				return client
			}
			err := app.run(context.Background(), []string{"cli", "collections"})
			records := readLogs(t, logs)
			if fail {
				var httpErr *qdrant.HTTPError
				if !errors.As(err, &httpErr) || httpErr.StatusCode != 500 {
					t.Fatalf("error=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertLog(t, records, "collections listed", "collection_count", float64(2))
				if records[0]["collection"] != "a" || records[1]["collection"] != "z" {
					t.Fatal(records)
				}
			}
		})
	}
}

// Exercise the real entry point in child processes, including flag package error
// paths. Every stderr line must be a JSON log and stdout must remain empty.
func TestProcessOutputIsOnlyJSONLogs(t *testing.T) {
	if os.Getenv("QDRANT_MAINTENANCE_TEST_PROCESS") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"cli"}, os.Args[i+1:]...)
				os.Exit(mainExit())
			}
		}
		os.Exit(2)
	}
	for _, tc := range []struct {
		args    []string
		wantErr bool
	}{
		{nil, true}, {[]string{"help"}, false}, {[]string{"version"}, false},
		{[]string{"version", "--bad-flag"}, true},
		{[]string{"collections", "--bad-flag"}, true},
		{[]string{"snapshots", "--help"}, false},
		{[]string{"snapshots", "create", "--help"}, false},
		{[]string{"snapshots", "list", "--bad-flag"}, true},
		{[]string{"retention", "apply", "--help"}, false},
		{[]string{"retention", "apply", "--bad-flag"}, true},
	} {
		t.Run(fmt.Sprint(tc.args), func(t *testing.T) {
			args := append([]string{"-test.run=^TestProcessOutputIsOnlyJSONLogs$", "--"}, tc.args...)
			cmd := exec.Command(os.Args[0], args...)
			cmd.Env = append(os.Environ(), "QDRANT_MAINTENANCE_TEST_PROCESS=1", "OTEL_SDK_DISABLED=true", "LOG_LEVEL=INFO")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v stderr=%s", err, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("unexpected stdout: %s", stdout.String())
			}
			if records := readLogs(t, &stderr); len(records) == 0 {
				t.Fatal("missing logs")
			}
		})
	}
}
