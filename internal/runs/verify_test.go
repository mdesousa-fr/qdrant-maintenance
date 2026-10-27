package runs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestVerificationDetectsContentCorruptionAndPersistsHealth(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	record := completeRecord(t, store, "run", time.Now().Add(-time.Minute))
	repo := Repository{Store: store}
	key := record.Manifest.Entries[0].Key
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store.data[key] = []byte("corrupt") // Same length, unchanged metadata: HEAD alone passes.
	if _, err := repo.Verify(ctx, "run", "", "", false, logger); err == nil {
		t.Fatal("corruption accepted")
	}
	loaded, err := repo.Load(ctx, "run")
	if err != nil || loaded.Manifest.Entries[0].VerifyError == "" || loaded.Manifest.Writer != "" {
		t.Fatalf("record=%+v error=%v", loaded, err)
	}
	if _, err := Check(ctx, repo, inventory(), "", time.Hour, time.Now()); err == nil {
		t.Fatal("damaged run passed freshness")
	}
	store.data[key] = []byte("archive")
	if _, err := repo.Verify(ctx, "run", "", "", false, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := Check(ctx, repo, inventory(), "", time.Hour, time.Now()); err != nil {
		t.Fatal(err)
	}
}
func TestVerificationFailsForMissingChecksumOrCancelledContext(t *testing.T) {
	store := newStore()
	if err := VerifyObject(context.Background(), store, "key", 1, ""); err == nil {
		t.Fatal("missing checksum accepted")
	}
	record := completeRecord(t, store, "run", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := VerifyObject(ctx, store, record.Manifest.Entries[0].Key, 7, record.Manifest.Entries[0].SHA256); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestVerificationRejectsTruncatedAndMissingObjects(t *testing.T) {
	for _, missing := range []bool{false, true} {
		store := newStore()
		record := completeRecord(t, store, "run", time.Now())
		entry := record.Manifest.Entries[0]
		if missing {
			delete(store.data, entry.Key)
		} else {
			store.data[entry.Key] = []byte("short")
		}
		if err := VerifyObject(context.Background(), store, entry.Key, entry.Size, entry.SHA256); err == nil {
			t.Fatal("invalid object accepted")
		}
	}
}
