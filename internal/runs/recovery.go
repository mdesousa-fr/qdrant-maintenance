package runs

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
)

func recoverable(m Manifest) error {
	switch m.State {
	case "running", "incomplete", "complete":
		return nil
	default:
		return fmt.Errorf("run %s is %s and cannot be recovered", m.ID, m.State)
	}
}
func (e Engine) loadRecovery(ctx context.Context, id string, takeover bool) (Record, error) {
	record, err := e.Repo.Load(ctx, id)
	if err != nil {
		return record, err
	}
	if err := recoverable(record.Manifest); err != nil {
		return record, err
	}
	current, err := e.Inspect(ctx)
	if err != nil {
		return record, err
	}
	if current.Signature() != record.Manifest.Inventory.Signature() {
		return record, fmt.Errorf("current topology or collection inventory differs from run %s", id)
	}
	if record.Manifest.Writer != "" && !takeover {
		return record, fmt.Errorf("run has an active or interrupted writer; stop it before --takeover")
	}
	return record, nil
}
func (e Engine) Retry(ctx context.Context, id string, takeover bool) (result Manifest, err error) {
	ctx, finish := telemetry.Operation(ctx, "backup.retry")
	defer func() { finish(err) }()
	record, err := e.loadRecovery(ctx, id, takeover)
	if err != nil {
		return record.Manifest, err
	}
	record.Manifest.State = "running"
	record.Manifest.Error = ""
	if err := e.Repo.Claim(ctx, &record, takeover); err != nil {
		return record.Manifest, err
	}
	e.Recover = true
	return e.execute(ctx, &record)
}

// recoveryUpload accepts an existing destination only if it matches freshly
// downloaded source data. It never overwrites a conflicting destination.
type recoveryUpload struct{ BackupStore }

func (s recoveryUpload) Upload(ctx context.Context, key string, file *os.File, size int64, checksum string) error {
	details, err := s.HeadObject(ctx, key)
	if err == nil {
		if details.Size == size && details.SHA256 == checksum {
			return nil
		}
		return fmt.Errorf("destination %s already exists with different metadata", key)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return s.BackupStore.Upload(ctx, key, file, size, checksum)
}

var _ backup.Storage = recoveryUpload{}

// Cleanup removes only recorded Qdrant sources with healthy committed S3 copies.
// Explicit cleanup overrides the original keep-source preference.
func (e Engine) Cleanup(ctx context.Context, id string, takeover bool) (manifest Manifest, err error) {
	ctx, finish := telemetry.Operation(ctx, "backup.cleanup")
	defer func() { finish(err) }()
	record, err := e.loadRecovery(ctx, id, takeover)
	if err != nil {
		return record.Manifest, err
	}
	if err := e.Repo.Claim(ctx, &record, takeover); err != nil {
		return record.Manifest, err
	}
	defer func() {
		record.Manifest.Writer = ""
		record.Manifest.Error = ""
		if err != nil {
			record.Manifest.Error = err.Error()
		}
		if saveErr := e.save(ctx, &record); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
		manifest = record.Manifest
	}()
	var errs []error
	for i := range record.Manifest.Entries {
		entry := &record.Manifest.Entries[i]
		if entry.SourceDeleted {
			continue
		}
		if !entry.Stored || entry.Deleted || entry.Snapshot == nil {
			e.Logger.InfoContext(ctx, "source cleanup skipped", "event", "source.cleanup_skipped", "node", entry.Node, "collection", entry.Collection, "reason", "no verified S3 copy")
			continue
		}
		if err := ctx.Err(); err != nil {
			return record.Manifest, errors.Join(append(errs, err)...)
		}
		if checkErr := CheckObject(ctx, e.Store, *entry); checkErr != nil {
			entry.VerifyError = checkErr.Error()
			record.Manifest.State = "incomplete"
			errs = append(errs, checkErr)
			continue
		}
		deleteErr := e.Clients[entry.Node].DeleteSnapshot(ctx, entry.Collection, entry.Snapshot.Name)
		var status *qdrant.HTTPError
		if deleteErr != nil && !(errors.As(deleteErr, &status) && status.StatusCode == 404) {
			entry.Error = deleteErr.Error()
			errs = append(errs, deleteErr)
			continue
		}
		entry.SourceDeleted = true
		entry.Error = ""
		if saveErr := e.save(ctx, &record); saveErr != nil {
			return record.Manifest, errors.Join(append(errs, saveErr)...)
		}
		e.Logger.InfoContext(ctx, "source snapshot cleaned up", "event", "source.cleaned", "node", entry.Node, "collection", entry.Collection, "key", entry.Key)
	}
	return record.Manifest, errors.Join(errs...)
}
