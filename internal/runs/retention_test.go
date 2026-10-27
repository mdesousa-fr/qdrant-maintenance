package runs

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestRunRetentionProtectsIncompleteAndUnmanagedObjects(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	repo := Repository{Store: store}
	now := time.Now()
	old := completeRecord(t, store, "old", now.Add(-2*time.Hour))
	completeRecord(t, store, "new", now.Add(-time.Hour))
	partial := completeRecord(t, store, "partial", now)
	partial.Manifest.State = "incomplete"
	partial.Manifest.CompletedAt = time.Time{}
	if err := repo.Save(ctx, &partial); err != nil {
		t.Fatal(err)
	}
	store.data["docs/node-0/unmanaged.snapshot"] = []byte("unmanaged")
	plan, err := PlanRetention(ctx, repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].Manifest.ID != "old" || len(plan.Keys) != 1 || plan.Keys[0] != old.Manifest.Entries[0].Key {
		t.Fatalf("plan=%+v", plan)
	}
	if len(plan.Retained) != 1 || plan.Retained[0].Manifest.ID != "new" {
		t.Fatal(plan.Retained)
	}
}
func TestRunRetentionPreservesSharedReferences(t *testing.T) {
	store := newStore()
	old := completeRecord(t, store, "old", time.Now().Add(-time.Hour))
	newer := completeRecord(t, store, "new", time.Now())
	newer.Manifest.Entries = old.Manifest.Entries
	if err := (Repository{Store: store}).Save(context.Background(), &newer); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanRetention(context.Background(), Repository{Store: store}, 1)
	if err != nil || len(plan.Keys) != 0 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
}
func TestExpiryRequiresAgeAndNeverSelectsCompleteRuns(t *testing.T) {
	store := newStore()
	old := completeRecord(t, store, "old", time.Now().Add(-48*time.Hour))
	completeRecord(t, store, "healthy", time.Now().Add(-48*time.Hour))
	old.Manifest.State = "incomplete"
	old.Manifest.CompletedAt = time.Time{}
	if err := (Repository{Store: store}).Save(context.Background(), &old); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanExpiry(context.Background(), Repository{Store: store}, 24*time.Hour, time.Now())
	if err != nil || len(plan.Candidates) != 1 || plan.Candidates[0].Manifest.ID != "old" {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
}

func TestRunPruningAndPartialFailureRecovery(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	repo := Repository{Store: store}
	now := time.Now()
	old := completeRecord(t, store, "old", now.Add(-time.Hour))
	completeRecord(t, store, "new", now)
	plan, err := PlanRetention(ctx, repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	store.deleteError = fmt.Errorf("denied")
	if err := plan.Apply(ctx, repo, slog.Default(), false); err == nil {
		t.Fatal("delete failure ignored")
	}
	record, err := repo.Load(ctx, "old")
	if err != nil || record.Manifest.State != "pruning" || record.Manifest.Writer != "" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	store.deleteError = nil
	plan, err = PlanRetention(ctx, repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Apply(ctx, repo, slog.Default(), false); err != nil {
		t.Fatal(err)
	}
	record, err = repo.Load(ctx, "old")
	if err != nil || record.Manifest.State != "pruned" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if len(store.deleted) != 1 || store.deleted[0] != old.Manifest.Entries[0].Key {
		t.Fatal(store.deleted)
	}
}
func TestPruningStopsForStaleInventoryOrDamagedRetainedRun(t *testing.T) {
	for _, damage := range []string{"changed", "missing", "cancelled"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			store := newStore()
			repo := Repository{Store: store}
			now := time.Now()
			completeRecord(t, store, "old", now.Add(-time.Hour))
			newer := completeRecord(t, store, "new", now)
			plan, err := PlanRetention(ctx, repo, 1)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "changed":
				newer.Manifest.Error = "changed"
				if err := repo.Save(ctx, &newer); err != nil {
					t.Fatal(err)
				}
			case "missing":
				delete(store.details, newer.Manifest.Entries[0].Key)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := plan.Apply(ctx, repo, slog.Default(), false); err == nil {
				t.Fatal("unsafe plan applied")
			}
			if len(store.deleted) != 0 {
				t.Fatal(store.deleted)
			}
		})
	}
}
