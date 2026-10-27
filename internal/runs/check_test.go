package runs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"testing"
	"time"
)

func completeRecord(t *testing.T, store *memoryStore, id string, when time.Time) Record {
	t.Helper()
	m := New(id, "test", "", inventory(), false)
	m.StartedAt = when.Add(-time.Minute)
	m.FinishedAt = when
	m.CompletedAt = when
	m.Writer = ""
	m.State = "complete"
	digest := sha256.Sum256([]byte("archive"))
	hash := hex.EncodeToString(digest[:])
	m.Entries[0] = Entry{Collection: "docs", Node: "node-0", Snapshot: &qdrant.Snapshot{Name: id + ".snapshot", Size: 7}, Key: "docs/node-0/" + id + ".snapshot", Size: 7, SHA256: hash, Stored: true}
	store.data[m.Entries[0].Key] = []byte("archive")
	store.details[m.Entries[0].Key] = storage.Details{Size: 7, SHA256: hash}
	record := Record{Manifest: m}
	if err := (Repository{Store: store}).Save(context.Background(), &record); err != nil {
		t.Fatal(err)
	}
	return record
}
func TestFreshnessUsesLatestCompleteAndChecksObjects(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		age    time.Duration
		damage string
		ok     bool
	}{
		{"fresh", time.Minute, "", true}, {"stale", 48 * time.Hour, "", false}, {"missing object", time.Minute, "missing", false}, {"metadata mismatch", time.Minute, "metadata", false}, {"failed verification", time.Minute, "corrupt", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore()
			record := completeRecord(t, store, "run", now.Add(-tc.age))
			key := record.Manifest.Entries[0].Key
			switch tc.damage {
			case "missing":
				delete(store.details, key)
			case "metadata":
				store.details[key] = storage.Details{Size: 1}
			case "corrupt":
				record.Manifest.Entries[0].VerifyError = "corrupt"
				if err := (Repository{Store: store}).Save(context.Background(), &record); err != nil {
					t.Fatal(err)
				}
			}
			_, err := Check(context.Background(), Repository{Store: store}, inventory(), "", time.Hour, now)
			if (err == nil) != tc.ok {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
func TestFreshnessRejectsNoCompleteCoverage(t *testing.T) {
	store := newStore()
	if _, err := Check(context.Background(), Repository{Store: store}, inventory(), "", time.Hour, time.Now()); err == nil {
		t.Fatal("empty bucket accepted")
	}
	record := completeRecord(t, store, "run", time.Now())
	record.Manifest.State = "incomplete"
	record.Manifest.CompletedAt = time.Time{}
	if err := (Repository{Store: store}).Save(context.Background(), &record); err != nil {
		t.Fatal(err)
	}
	if _, err := Check(context.Background(), Repository{Store: store}, inventory(), "", time.Hour, time.Now()); err == nil {
		t.Fatal("incomplete run accepted")
	}
}

func TestFreshnessDoesNotHideLatestDamageWithOlderHealthyRun(t *testing.T) {
	store := newStore()
	now := time.Now()
	completeRecord(t, store, "older", now.Add(-20*time.Minute))
	latest := completeRecord(t, store, "latest", now.Add(-time.Minute))
	delete(store.details, latest.Manifest.Entries[0].Key)
	if _, err := Check(context.Background(), Repository{Store: store}, inventory(), "", time.Hour, now); err == nil {
		t.Fatal("fell back to older healthy run")
	}
}
