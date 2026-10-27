package topology

import (
	"context"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/testhttp"
	"net/http"
	"testing"
)

func endpoint(id string, peer int, members, collections string, fail bool) Endpoint {
	server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/cluster" {
			fmt.Fprintf(w, `{"result":{"status":"enabled","peer_id":%d,"peers":%s}}`, peer, members)
		} else {
			fmt.Fprintf(w, `{"result":{"collections":%s}}`, collections)
		}
	}))
	client := qdrant.NewClient(server.URL, "")
	client.HTTPClient = server.Client()
	return Endpoint{ID: id, Client: client}
}
func TestCoverageValidation(t *testing.T) {
	const peers = `{"1":{},"2":{}}`
	const collections = `[{"name":"docs"}]`
	for _, tc := range []struct {
		name  string
		nodes []Endpoint
		ok    bool
	}{
		{"complete", []Endpoint{endpoint("a", 1, peers, collections, false), endpoint("b", 2, peers, collections, false)}, true},
		{"missing", []Endpoint{endpoint("a", 1, peers, collections, false)}, false},
		{"duplicate", []Endpoint{endpoint("a", 1, peers, collections, false), endpoint("b", 1, peers, collections, false)}, false},
		{"inconsistent", []Endpoint{endpoint("a", 1, peers, collections, false), endpoint("b", 2, `{"2":{}}`, collections, false)}, false},
		{"unreachable", []Endpoint{endpoint("a", 1, peers, collections, true)}, false},
		{"collections differ", []Endpoint{endpoint("a", 1, peers, collections, false), endpoint("b", 2, peers, `[]`, false)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inventory, err := Inspect(context.Background(), tc.nodes, "")
			if (err == nil) != tc.ok {
				t.Fatalf("error=%v", err)
			}
			if tc.ok && (len(inventory.Pairs) != 2 || inventory.ClusterID == "") {
				t.Fatal(inventory)
			}
		})
	}
}
func TestSignatureDetectsChangedMembershipAndCollections(t *testing.T) {
	first, err := Inspect(context.Background(), []Endpoint{endpoint("a", 1, `{"1":{}}`, `[{"name":"docs"}]`, false)}, "")
	if err != nil {
		t.Fatal(err)
	}
	after, err := Inspect(context.Background(), []Endpoint{endpoint("a", 2, `{"2":{}}`, `[{"name":"docs"}]`, false)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Signature() == after.Signature() {
		t.Fatal("membership change missed")
	}
	after, err = Inspect(context.Background(), []Endpoint{endpoint("a", 1, `{"1":{}}`, `[{"name":"other"}]`, false)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Signature() == after.Signature() {
		t.Fatal("collection change missed")
	}
}
