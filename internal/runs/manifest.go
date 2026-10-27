// Package runs records durable, complete distributed backup sets.
package runs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

const SchemaVersion = 1
const maxManifestBytes = 32 * 1024 * 1024

type Entry struct {
	Collection    string           `json:"collection"`
	Node          string           `json:"node"`
	Snapshot      *qdrant.Snapshot `json:"snapshot,omitempty"`
	Key           string           `json:"key,omitempty"`
	Size          int64            `json:"size"`
	SHA256        string           `json:"sha256,omitempty"`
	Stored        bool             `json:"stored"`
	SourceDeleted bool             `json:"source_deleted"`
	Error         string           `json:"error,omitempty"`
	VerifiedAt    time.Time        `json:"verified_at,omitempty"`
	VerifyError   string           `json:"verify_error,omitempty"`
	Deleted       bool             `json:"deleted"`
}
type Manifest struct {
	SchemaVersion int                `json:"schema_version"`
	ID            string             `json:"run_id"`
	Version       string             `json:"cli_version"`
	State         string             `json:"state"`
	Scope         string             `json:"scope"` // empty means all collections
	Inventory     topology.Inventory `json:"inventory"`
	StartedAt     time.Time          `json:"started_at"`
	FinishedAt    time.Time          `json:"finished_at,omitempty"`
	CompletedAt   time.Time          `json:"completed_at,omitempty"`
	KeepSource    bool               `json:"keep_source"`
	Entries       []Entry            `json:"entries"`
	Error         string             `json:"error,omitempty"`
	// Writer reserves side effects across retry, cleanup, verification and pruning.
	// A terminated process may leave it set; explicit takeover requires its ETag.
	Writer string `json:"writer,omitempty"`
}

func New(id, version, scope string, inventory topology.Inventory, keep bool) Manifest {
	m := Manifest{SchemaVersion: SchemaVersion, ID: id, Version: version, State: "running", Scope: scope, Inventory: inventory, StartedAt: time.Now().UTC(), KeepSource: keep, Writer: id}
	for _, pair := range inventory.Pairs {
		m.Entries = append(m.Entries, Entry{Collection: pair.Collection, Node: pair.Node})
	}
	return m
}
func (m Manifest) Validate() error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported manifest schema %d", m.SchemaVersion)
	}
	if !config.ValidSegment(m.ID) || m.StartedAt.IsZero() || m.Inventory.ClusterID == "" || len(m.Entries) == 0 {
		return fmt.Errorf("incomplete manifest identity or inventory")
	}
	switch m.State {
	case "running", "incomplete", "complete", "pruning", "pruned", "expired":
	default:
		return fmt.Errorf("invalid run state %q", m.State)
	}
	nodes := map[string]bool{}
	peers := map[uint64]bool{}
	for _, n := range m.Inventory.Nodes {
		if !config.ValidSegment(n.ID) || nodes[n.ID] || peers[n.PeerID] {
			return fmt.Errorf("invalid manifest node inventory")
		}
		nodes[n.ID] = true
		peers[n.PeerID] = true
	}
	if m.Inventory.ClusterID != topology.ClusterIdentity(m.Inventory.Nodes) {
		return fmt.Errorf("cluster identity does not match peer inventory")
	}
	selected := m.Inventory.Collections
	if m.Scope != "" {
		if !slices.Contains(selected, m.Scope) {
			return fmt.Errorf("scope missing from inventory")
		}
		selected = []string{m.Scope}
	}
	expected := map[string]bool{}
	for node := range nodes {
		for _, collection := range selected {
			if !config.ValidSegment(collection) {
				return fmt.Errorf("invalid collection")
			}
			key := collection + "/" + node
			if expected[key] {
				return fmt.Errorf("duplicate inventory pair")
			}
			expected[key] = true
		}
	}
	if len(expected) != len(m.Entries) || len(expected) != len(m.Inventory.Pairs) {
		return fmt.Errorf("manifest coverage mismatch")
	}
	pairs := map[string]bool{}
	for _, pair := range m.Inventory.Pairs {
		key := pair.Collection + "/" + pair.Node
		if !expected[key] || pairs[key] {
			return fmt.Errorf("invalid expected pair")
		}
		pairs[key] = true
	}
	seen := map[string]bool{}
	for _, e := range m.Entries {
		pair := e.Collection + "/" + e.Node
		if !expected[pair] || seen[pair] {
			return fmt.Errorf("invalid manifest entry coverage")
		}
		seen[pair] = true
		if e.Snapshot != nil {
			if !config.ValidSegment(e.Snapshot.Name) || !strings.HasSuffix(e.Snapshot.Name, ".snapshot") || e.Snapshot.Name == ".snapshot" || e.Key != pair+"/"+e.Snapshot.Name {
				return fmt.Errorf("invalid snapshot reference")
			}
		} else if e.Key != "" || e.Stored {
			return fmt.Errorf("snapshot identity missing")
		}
		if e.Stored {
			digest, err := hex.DecodeString(e.SHA256)
			if err != nil || len(digest) != 32 || e.Size < 0 || uint64(e.Size) != e.Snapshot.Size {
				return fmt.Errorf("invalid verified snapshot metadata")
			}
		}
		if e.SourceDeleted && !e.Stored {
			return fmt.Errorf("source deleted before storage verification")
		}
		if m.State == "complete" && (!e.Stored || e.Deleted || m.CompletedAt.IsZero()) {
			return fmt.Errorf("complete run has missing snapshots")
		}
	}
	return nil
}

type Store interface {
	storage.Client
	storage.Repository
}
type Repository struct{ Store Store }
type Record struct {
	Manifest Manifest
	ETag     string
}

func Key(id string) string { return "runs/" + id + ".json" }
func (r Repository) Load(ctx context.Context, id string) (Record, error) {
	if !config.ValidSegment(id) {
		return Record{}, fmt.Errorf("invalid run ID")
	}
	body, etag, err := r.Store.GetObject(ctx, Key(id))
	if err != nil {
		return Record{}, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxManifestBytes+1))
	if err != nil {
		return Record{}, err
	}
	if len(data) > maxManifestBytes {
		return Record{}, fmt.Errorf("manifest too large")
	}
	var m Manifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return Record{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Record{}, fmt.Errorf("trailing manifest data")
	}
	if m.ID != id || etag == "" {
		return Record{}, fmt.Errorf("manifest ID or ETag mismatch")
	}
	if err := m.Validate(); err != nil {
		return Record{}, fmt.Errorf("run %s: %w", id, err)
	}
	return Record{Manifest: m, ETag: etag}, nil
}
func (r Repository) Save(ctx context.Context, record *Record) error {
	if err := record.Manifest.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(record.Manifest)
	if err != nil {
		return err
	}
	if len(data) > maxManifestBytes {
		return fmt.Errorf("manifest too large")
	}
	etag, err := r.Store.PutObject(ctx, Key(record.Manifest.ID), data, record.ETag)
	if err != nil {
		return fmt.Errorf("save run %s: %w", record.Manifest.ID, err)
	}
	if etag == "" {
		return fmt.Errorf("manifest write returned no ETag")
	}
	record.ETag = etag
	return nil
}
func (r Repository) List(ctx context.Context) ([]Record, error) {
	objects, err := r.Store.ListObjects(ctx)
	if err != nil {
		return nil, err
	}
	records := []Record{}
	for _, object := range objects {
		if !strings.HasPrefix(object.Key, "runs/") || !strings.HasSuffix(object.Key, ".json") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(object.Key, "runs/"), ".json")
		if strings.Contains(id, "/") {
			continue
		}
		record, err := r.Load(ctx, id)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i].Manifest, records[j].Manifest
		if a.StartedAt.Equal(b.StartedAt) {
			return a.ID < b.ID
		}
		return a.StartedAt.After(b.StartedAt)
	})
	return records, nil
}
