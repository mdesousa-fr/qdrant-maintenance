package qdrant

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
)

type fakeHTTPClient struct{}

func (fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	r := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("hello")),
	}

	return r, nil
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "without trailing slash", input: "http://localhost:6333", expected: "http://localhost:6333"},
		{name: "with trailing slash", input: "http://localhost:6333/", expected: "http://localhost:6333"},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				client := NewClient(test.input, "secret")

				if client.URL != test.expected {
					t.Errorf("URL = %q, want %q", client.URL, test.expected)
				}
			},
		)
	}
}

func TestGetCollections(t *testing.T) {
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("HTTP method = %q, want %q", r.Method, http.MethodGet)
			}

			if r.URL.Path != "/collections" {
				t.Errorf("URL path = %q, want %q", r.URL.Path, "/collections")
			}

			if r.Header.Get("api-key") != "secret" {
				t.Errorf("Header api-key = %q, want %q", r.Header.Get("api-key"), "secret")
			}

			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `
			{
			  "result": {
			    "collections": [
			      {"name": "documents"},
			      {"name": "products"}
			    ]
			  }
			}
			`)
		},
	)
	server := testhttp.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, "secret")
	client.HTTPClient = server.Client()
	collections, err := client.GetCollections(context.Background())
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}

	if len(collections) != 2 {
		t.Fatalf("len(collections) = '%d', want '%d'", len(collections), 2)
	}

	if collections[0].Name != "documents" {
		t.Errorf("Collection = %q, want %q", collections[0].Name, "documents")
	}

	if collections[1].Name != "products" {
		t.Errorf("Collection = %q, want %q", collections[1].Name, "products")
	}
}

func TestGetCollectionsHTTPError(t *testing.T) {
	tests := []struct {
		name  string
		input int
	}{
		{name: "status code below 200", input: http.StatusContinue},
		{name: "status code above 299", input: http.StatusInternalServerError},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				handler := http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(test.input)
						fmt.Fprint(w, "something went wrong")
					},
				)
				server := testhttp.NewServer(handler)
				defer server.Close()
				client := NewClient(server.URL, "secret")
				client.HTTPClient = server.Client()
				collections, err := client.GetCollections(context.Background())
				if err == nil {
					t.Errorf("expected an error, got nil")
				}
				if collections != nil {
					t.Errorf("collections = '%v', want 'nil'", collections)
				}
			},
		)
	}
}

func TestGetCollectionsJSONError(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "incompatible JSON structure", input: `
		{
		  "result": [
		      {"name": "documents"},
		      {"name": "products"}
		    ]
		}
		`},
		{name: "invalid JSON", input: `{"result":`},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {

				handler := http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						fmt.Fprint(w, test.input)
					},
				)
				server := testhttp.NewServer(handler)
				defer server.Close()

				client := NewClient(server.URL, "secret")
				client.HTTPClient = server.Client()
				collections, err := client.GetCollections(context.Background())
				if err == nil {
					t.Errorf("expected an error, got nil")
				}
				if collections != nil {
					t.Errorf("collections = '%v', want 'nil'", collections)
				}
			},
		)
	}
}

func TestSnapshotJSONToSnapshot(t *testing.T) {
	input := snapshotJSON{
		Name:         "snapshot-123.snapshot",
		CreationTime: "2026-10-03T12:11:28",
		Size:         1024,
		Checksum:     "abc123",
	}
	expected := time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC)

	snapshot, err := input.toSnapshot()

	if err != nil {
		t.Fatalf("error = '%v', want 'nil'", err)
	}

	if !snapshot.CreationTime.Equal(expected) {
		t.Errorf("creation time = %v, want %v", snapshot.CreationTime.String(), expected.String())
	}
}

func TestSnapshotJSONToSnapshotInvalidTime(t *testing.T) {
	input := snapshotJSON{
		Name:         "snapshot-123.snapshot",
		CreationTime: "not-a-date",
		Size:         1024,
		Checksum:     "abc123",
	}
	expected := Snapshot{}

	snapshot, err := input.toSnapshot()

	if err == nil {
		t.Fatalf("expected an error, got nil")
	}

	if snapshot != expected {
		t.Errorf("snapshot = %v, want %v", snapshot, expected)
	}
}

func TestGetSnapshots(t *testing.T) {
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("HTTP method = %q, want %q", r.Method, http.MethodGet)
			}

			if r.URL.Path != "/collections/documents/snapshots" {
				t.Errorf("URL path = %q, want %q", r.URL.Path, "/collections/documents/snapshots")
			}

			if r.Header.Get("api-key") != "secret" {
				t.Errorf("Header api-key = %q, want %q", r.Header.Get("api-key"), "secret")
			}

			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `
				{
				  "result": [
				    {
				      "name": "snapshot-1.snapshot",
				      "creation_time": "2026-10-03T12:11:28",
				      "size": 1024,
				      "checksum": "abc123"
				    }
				  ]
				}
			`)
		},
	)
	server := testhttp.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, "secret")
	client.HTTPClient = server.Client()
	snapshots, err := client.GetSnapshots(context.Background(), "documents")
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("len(snapshots) = '%d', want '%d'", len(snapshots), 1)
	}

	expected := Snapshot{
		Name:         "snapshot-1.snapshot",
		CreationTime: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
		Size:         1024,
		Checksum:     "abc123",
	}

	if snapshots[0] != expected {
		t.Errorf("snapshot = %+v, want %+v", snapshots[0], expected)
	}
}

func TestCreateSnapshot(t *testing.T) {
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("HTTP method = %q, want %q", r.Method, http.MethodPost)
			}

			if r.URL.Path != "/collections/documents/snapshots" {
				t.Errorf("URL path = %q, want %q", r.URL.Path, "/collections/documents/snapshots")
			}

			if r.Header.Get("api-key") != "secret" {
				t.Errorf("Header api-key = %q, want %q", r.Header.Get("api-key"), "secret")
			}

			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `
				{
				  "result": {
				    "name": "snapshot-1.snapshot",
				    "creation_time": "2026-10-03T12:11:28",
				    "size": 1024,
					"checksum": "abc123"
				  }
				}
			`)
		},
	)
	server := testhttp.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, "secret")
	client.HTTPClient = server.Client()
	snapshot, err := client.CreateSnapshot(context.Background(), "documents")
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}

	expected := Snapshot{
		Name:         "snapshot-1.snapshot",
		CreationTime: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
		Size:         1024,
		Checksum:     "abc123",
	}

	if snapshot != expected {
		t.Errorf("snapshot = %+v, want %+v", snapshot, expected)
	}
}

func TestHTTPClientInterface(t *testing.T) {
	client := NewClient("http://qdrant", "secret")
	client.HTTPClient = fakeHTTPClient{}
	body, _ := client.do(
		context.Background(),
		http.MethodGet,
		"/test",
	)

	if string(body) != "hello" {
		t.Fatalf("got %q, want %q", string(body), "hello")
	}
}
