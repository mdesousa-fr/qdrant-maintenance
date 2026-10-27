package runs

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
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
func (e Engine) Retry(ctx context.Context, id string, takeover bool) (Manifest, error) {
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
