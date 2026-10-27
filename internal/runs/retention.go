package runs

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type RunPlan struct {
	KeepLast   int
	Mode       string
	Candidates []Record
	Keys       []string
	// Every observed ETag participates in the pre-deletion consistency check.
	Revisions map[string]string
	Protected map[string]bool
	Retained  []Record
}

func PlanRetention(ctx context.Context, repo Repository, keep int) (RunPlan, error) {
	if keep < 0 {
		return RunPlan{}, fmt.Errorf("keep-last must not be negative")
	}
	records, err := repo.List(ctx)
	if err != nil {
		return RunPlan{}, err
	}
	selected := map[string]bool{}
	groups := map[string][]Record{}
	for _, record := range records {
		m := record.Manifest
		if m.State == "pruning" && m.PruneTarget == "pruned" {
			selected[m.ID] = true
			continue
		}
		if m.State != "complete" || m.Writer != "" {
			continue
		}
		healthy := true
		for _, e := range m.Entries {
			healthy = healthy && e.VerifyError == ""
		}
		if healthy {
			group := m.Inventory.ClusterID + "/" + m.Scope
			groups[group] = append(groups[group], record)
		}
	}
	retained := []Record{}
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool {
			a, b := group[i].Manifest, group[j].Manifest
			if a.StartedAt.Equal(b.StartedAt) {
				return a.ID < b.ID
			}
			return a.StartedAt.After(b.StartedAt)
		})
		for i, record := range group {
			if i < keep {
				retained = append(retained, record)
			} else {
				selected[record.Manifest.ID] = true
			}
		}
	}
	plan := makePlan(records, selected, "retention")
	plan.KeepLast = keep
	plan.Retained = retained
	return plan, nil
}

// Expiry is explicit and separate from ordinary retention. Only recorded,
// verified S3 uploads are eligible; Qdrant sources and unmanaged keys are left alone.
func PlanExpiry(ctx context.Context, repo Repository, olderThan time.Duration, now time.Time) (RunPlan, error) {
	if olderThan <= 0 {
		return RunPlan{}, fmt.Errorf("older-than must be positive")
	}
	records, err := repo.List(ctx)
	if err != nil {
		return RunPlan{}, err
	}
	selected := map[string]bool{}
	for _, record := range records {
		m := record.Manifest
		if (m.State == "incomplete" || m.State == "running" || (m.State == "pruning" && m.PruneTarget == "expired")) && m.StartedAt.Before(now.Add(-olderThan)) {
			selected[m.ID] = true
		}
	}
	return makePlan(records, selected, "expire"), nil
}
func makePlan(records []Record, selected map[string]bool, mode string) RunPlan {
	plan := RunPlan{Mode: mode, Revisions: map[string]string{}, Protected: map[string]bool{}}
	for _, record := range records {
		m := record.Manifest
		plan.Revisions[m.ID] = record.ETag
		if selected[m.ID] {
			plan.Candidates = append(plan.Candidates, record)
			continue
		}
		if m.State == "pruned" || m.State == "expired" {
			continue
		}
		for _, entry := range m.Entries {
			if entry.Key != "" && !entry.Deleted {
				plan.Protected[entry.Key] = true
			}
		}
	}
	keys := map[string]bool{}
	for _, record := range plan.Candidates {
		for _, entry := range record.Manifest.Entries {
			if entry.Stored && !entry.Deleted && !plan.Protected[entry.Key] {
				keys[entry.Key] = true
			}
		}
	}
	for key := range keys {
		plan.Keys = append(plan.Keys, key)
	}
	sort.Strings(plan.Keys)
	sort.Slice(plan.Candidates, func(i, j int) bool { return plan.Candidates[i].Manifest.ID < plan.Candidates[j].Manifest.ID })
	return plan
}
