package runs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
)

func (r Repository) Claim(ctx context.Context, record *Record, takeover bool) error {
	if record.Manifest.Writer != "" && !takeover {
		return fmt.Errorf("run %s has an active or interrupted writer; stop that process before explicit --takeover", record.Manifest.ID)
	}
	record.Manifest.Writer = rand.Text()
	return r.Save(ctx, record)
}
func revisionsMatch(ctx context.Context, repo Repository, expected map[string]string) error {
	current, err := repo.List(ctx)
	if err != nil {
		return err
	}
	if len(current) != len(expected) {
		return storage.ErrConflict
	}
	for _, record := range current {
		if expected[record.Manifest.ID] != record.ETag {
			return storage.ErrConflict
		}
	}
	return nil
}

// Apply reserves every candidate before deleting anything, rechecks the inventory,
// and persists deletion checkpoints. A failed apply can be planned and resumed.
func (p RunPlan) Apply(ctx context.Context, repo Repository, logger *slog.Logger, takeover bool) (err error) {
	ctx, finish := telemetry.Operation(ctx, "retention.run_apply")
	defer func() { finish(err) }()
	if err := revisionsMatch(ctx, repo, p.Revisions); err != nil {
		return fmt.Errorf("run inventory changed: %w", err)
	}
	for _, retained := range p.Retained {
		for _, entry := range retained.Manifest.Entries {
			if err := CheckObject(ctx, repo.Store, entry); err != nil {
				return fmt.Errorf("retained run %s is damaged; refusing pruning: %w", retained.Manifest.ID, err)
			}
		}
	}
	work := append([]Record{}, p.Candidates...)
	claimed := 0
	released := map[int]bool{}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for i := 0; i < claimed; i++ {
			if !released[i] {
				work[i].Manifest.Writer = ""
				if releaseErr := repo.Save(releaseCtx, &work[i]); releaseErr != nil {
					err = errors.Join(err, releaseErr)
				}
			}
		}
	}()
	revisions := maps.Clone(p.Revisions)
	for i := range work {
		target := "pruned"
		if p.Mode == "expire" {
			target = "expired"
		}
		work[i].Manifest.State = "pruning"
		work[i].Manifest.PruneTarget = target
		if err := repo.Claim(ctx, &work[i], takeover); err != nil {
			return err
		}
		claimed++
		revisions[work[i].Manifest.ID] = work[i].ETag
	}
	if err := revisionsMatch(ctx, repo, revisions); err != nil {
		return fmt.Errorf("run inventory changed before deletion: %w", err)
	}
	outcomes := map[string]error{}
	var errs []error
	for i := range work {
		record := &work[i]
		allDeleted := true
		for j := range record.Manifest.Entries {
			entry := &record.Manifest.Entries[j]
			if !entry.Stored || entry.Deleted || p.Protected[entry.Key] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return errors.Join(append(errs, err)...)
			}
			deletionErr, done := outcomes[entry.Key]
			if !done {
				deletionErr = repo.Store.DeleteObject(ctx, entry.Key)
				outcomes[entry.Key] = deletionErr
				if deletionErr == nil {
					telemetry.Deleted(ctx)
					logger.InfoContext(ctx, "snapshot pruned", "event", "retention.deleted", "backup_run_id", record.Manifest.ID, "key", entry.Key)
				}
			}
			if deletionErr != nil {
				allDeleted = false
				entry.Error = deletionErr.Error()
				errs = append(errs, fmt.Errorf("delete %s: %w", entry.Key, deletionErr))
				continue
			}
			entry.Deleted = true
			saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			saveErr := repo.Save(saveCtx, record)
			cancel()
			if saveErr != nil {
				return errors.Join(append(errs, saveErr)...)
			}
		}
		if allDeleted {
			record.Manifest.State = record.Manifest.PruneTarget
			record.Manifest.Error = ""
		} else {
			record.Manifest.Error = "snapshot pruning failed; rebuild the plan to retry"
		}
		record.Manifest.Writer = ""
		if err := repo.Save(ctx, record); err != nil {
			return errors.Join(append(errs, err)...)
		}
		released[i] = true
	}
	return errors.Join(errs...)
}
