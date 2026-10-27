package runs

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

// Covers requires the same peers and stable node labels. A scoped check may use
// a whole-cluster backup, but a whole-cluster check cannot use a scoped backup.
func Covers(m Manifest, expected topology.Inventory, scope string) bool {
	if m.Inventory.ClusterID != expected.ClusterID || !slices.Equal(m.Inventory.Nodes, expected.Nodes) {
		return false
	}
	if scope == "" {
		return m.Scope == "" && m.Inventory.Signature() == expected.Signature()
	}
	found := map[string]bool{}
	for _, e := range m.Entries {
		if e.Collection == scope {
			found[e.Node] = true
		}
	}
	for _, node := range expected.Nodes {
		if !found[node.ID] {
			return false
		}
	}
	return true
}
func Check(ctx context.Context, repo Repository, expected topology.Inventory, scope string, maxAge time.Duration, now time.Time) (Manifest, error) {
	if maxAge <= 0 {
		return Manifest{}, fmt.Errorf("max-age must be positive")
	}
	records, err := repo.List(ctx)
	if err != nil {
		return Manifest{}, err
	}
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i].Manifest, records[j].Manifest
		if a.CompletedAt.Equal(b.CompletedAt) {
			return a.ID < b.ID
		}
		return a.CompletedAt.After(b.CompletedAt)
	})
	for _, record := range records {
		m := record.Manifest
		if m.State != "complete" || !Covers(m, expected, scope) {
			continue
		}
		age := now.Sub(m.CompletedAt)
		if age < 0 {
			return m, fmt.Errorf("latest complete backup timestamp is in the future")
		}
		if age > maxAge {
			return m, fmt.Errorf("latest complete backup is stale: age %s exceeds %s", age, maxAge)
		}
		for _, entry := range m.Entries {
			if scope != "" && entry.Collection != scope {
				continue
			}
			if err := CheckObject(ctx, repo.Store, entry); err != nil {
				return m, err
			}
		}
		return m, nil
	}
	return Manifest{}, fmt.Errorf("no complete backup covers the expected cluster and collections")
}
