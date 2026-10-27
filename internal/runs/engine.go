package runs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

type BackupStore interface {
	Store
	backup.Storage
}
type Engine struct {
	Repo    Repository
	Store   BackupStore
	Clients map[string]backup.Qdrant
	Logger  *slog.Logger
	Inspect func(context.Context) (topology.Inventory, error)
}

func (e Engine) Create(ctx context.Context, m Manifest) (Manifest, error) {
	record := Record{Manifest: m}
	if err := e.Repo.Save(ctx, &record); err != nil {
		return m, err
	}
	return e.execute(ctx, &record)
}
func (e Engine) execute(ctx context.Context, record *Record) (Manifest, error) {
	var errs []error
	for i := range record.Manifest.Entries {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		entry := &record.Manifest.Entries[i]
		operationErr, persistErr := e.transfer(ctx, record, i)
		if persistErr != nil {
			return record.Manifest, errors.Join(append(errs, persistErr)...)
		}
		if operationErr != nil {
			errs = append(errs, operationErr)
		}
		attrs := []any{"event", "backup.completed", "node", entry.Node, "collection", entry.Collection, "key", entry.Key, "size", entry.Size, "sha256", entry.SHA256, "stored", entry.Stored, "source_deleted", entry.SourceDeleted}
		if operationErr != nil {
			attrs[1] = "backup.failed"
			e.Logger.ErrorContext(ctx, "backup failed", append(attrs, "error", operationErr)...)
		} else {
			e.Logger.InfoContext(ctx, "backup completed", attrs...)
		}
	}
	after, checkErr := e.Inspect(ctx)
	if checkErr != nil {
		errs = append(errs, fmt.Errorf("final topology check: %w", checkErr))
	} else if after.Signature() != record.Manifest.Inventory.Signature() {
		errs = append(errs, fmt.Errorf("cluster topology changed during backup"))
	}
	complete := checkErr == nil && after.Signature() == record.Manifest.Inventory.Signature()
	for _, entry := range record.Manifest.Entries {
		complete = complete && entry.Stored && !entry.Deleted
	}
	record.Manifest.State = "incomplete"
	if complete {
		record.Manifest.State = "complete"
		if record.Manifest.CompletedAt.IsZero() {
			record.Manifest.CompletedAt = time.Now().UTC()
		}
	}
	record.Manifest.FinishedAt = time.Now().UTC()
	record.Manifest.Writer = ""
	record.Manifest.Error = ""
	if err := errors.Join(errs...); err != nil {
		record.Manifest.Error = err.Error()
	}
	// Cancellation must still leave a durable incomplete state when S3 is reachable.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := e.Repo.Save(saveCtx, record); err != nil {
		errs = append(errs, err)
	}
	return record.Manifest, errors.Join(errs...)
}
func (e Engine) transfer(ctx context.Context, record *Record, index int) (operationErr, persistErr error) {
	entry := &record.Manifest.Entries[index]
	client := e.Clients[entry.Node]
	if client == nil {
		return nil, fmt.Errorf("node %s has no client", entry.Node)
	}
	entry.Error = ""
	fail := func(err error) (error, error) { entry.Error = err.Error(); return err, e.save(ctx, record) }
	if entry.Snapshot == nil {
		snapshot, err := client.CreateSnapshot(ctx, entry.Collection)
		if err != nil {
			return fail(fmt.Errorf("create snapshot: %w", err))
		}
		entry.Snapshot = &snapshot
		entry.Key = entry.Collection + "/" + entry.Node + "/" + snapshot.Name
		if err := e.save(ctx, record); err != nil {
			return nil, err
		}
	}
	if !entry.Stored {
		result, err := backup.Transfer(ctx, e.Logger, client, e.Store, entry.Collection, entry.Node, *entry.Snapshot)
		if err != nil {
			return fail(err)
		}
		entry.Size = result.Size
		entry.SHA256 = result.SHA256
		entry.Stored = true
		// This conditional write must succeed before any Qdrant DELETE request.
		if err := e.save(ctx, record); err != nil {
			return nil, err
		}
	} else if err := CheckObject(ctx, e.Store, *entry); err != nil {
		return fail(err)
	}
	if !record.Manifest.KeepSource && !entry.SourceDeleted {
		err := client.DeleteSnapshot(ctx, entry.Collection, entry.Snapshot.Name)
		var status *qdrant.HTTPError
		if err != nil && !(errors.As(err, &status) && status.StatusCode == 404) {
			return fail(fmt.Errorf("backup stored but source cleanup failed: %w", err))
		}
		entry.SourceDeleted = true
		if err := e.save(ctx, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
func CheckObject(ctx context.Context, store storage.Repository, entry Entry) error {
	if !entry.Stored || entry.Deleted || entry.VerifyError != "" {
		return fmt.Errorf("snapshot %s is not healthy", entry.Key)
	}
	details, err := store.HeadObject(ctx, entry.Key)
	if err != nil {
		return fmt.Errorf("check %s: %w", entry.Key, err)
	}
	if details.Size != entry.Size || details.SHA256 != entry.SHA256 {
		return fmt.Errorf("snapshot %s size or checksum metadata mismatch", entry.Key)
	}
	return nil
}

func (e Engine) save(ctx context.Context, record *Record) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return e.Repo.Save(saveCtx, record)
}
