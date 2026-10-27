package runs

import (
	"context"
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
