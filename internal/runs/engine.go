package runs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

type BackupStore interface {
	Store
	backup.Storage
}
type Engine struct {
	Repo          Repository
	Store         BackupStore
	Clients       map[string]backup.Qdrant
	Logger        *slog.Logger
	Recover       bool
	ParallelNodes int
	Inspect       func(context.Context) (topology.Inventory, error)
}

func (e Engine) Create(ctx context.Context, m Manifest) (result Manifest, err error) {
	ctx, finish := telemetry.Operation(ctx, "backup.run")
	defer func() { finish(err) }()
	record := Record{Manifest: m}
	if err := e.Repo.Save(ctx, &record); err != nil {
		return m, err
	}
	return e.execute(ctx, &record)
}
func (e Engine) execute(ctx context.Context, record *Record) (Manifest, error) {
	// Workers own private entries. Only this coordinator mutates the manifest
	// and its ETag. A checkpoint acknowledgement gates subsequent side effects.
	type update struct {
		index        int
		entry        Entry
		ack          chan error
		operationErr error
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	updates := make(chan update)
	groups := [][]int{}
	nodeGroup := map[string]int{}
	for i, entry := range record.Manifest.Entries {
		g, exists := nodeGroup[entry.Node]
		if !exists {
			g = len(groups)
			nodeGroup[entry.Node] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], i)
	}
	parallel := e.ParallelNodes
	if parallel <= 0 {
		parallel = 1
	}
	parallel = min(parallel, len(groups))
	e.Logger.InfoContext(ctx, "backup concurrency configured", "event", "backup.concurrency", "parallel_nodes", parallel, "requested_parallel_nodes", e.ParallelNodes)
	// Copy all work before workers start; the coordinator will mutate record.
	entries := append([]Entry(nil), record.Manifest.Entries...)
	keepSource := record.Manifest.KeepSource
	jobs := make(chan []int)
	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for indexes := range jobs {
				for _, i := range indexes {
					if workCtx.Err() != nil {
						break
					}
					entry := entries[i]
					checkpoint := func() error {
						ack := make(chan error, 1)
						updates <- update{index: i, entry: entry, ack: ack}
						return <-ack
					}
					opErr, persistErr := e.transfer(workCtx, &entry, keepSource, checkpoint)
					if persistErr != nil {
						return
					}
					updates <- update{index: i, entry: entry, operationErr: opErr}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, indexes := range groups {
			if workCtx.Err() != nil {
				return
			}
			select {
			case jobs <- indexes:
			case <-workCtx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(updates) }()
	var errs []error
	var persistErr error
	for u := range updates {
		if u.ack != nil {
			if persistErr == nil {
				record.Manifest.Entries[u.index] = u.entry
				persistErr = e.save(ctx, record)
				if persistErr != nil {
					e.Logger.ErrorContext(ctx, "backup checkpoint failed", "event", "backup.checkpoint_failed", "node", u.entry.Node, "collection", u.entry.Collection, "key", u.entry.Key, "error", persistErr)
					cancelWork()
				}
			}
			u.ack <- persistErr
			continue
		}
		entry := u.entry
		if u.operationErr != nil {
			errs = append(errs, u.operationErr)
		}
		attrs := []any{"event", "backup.completed", "node", entry.Node, "collection", entry.Collection, "key", entry.Key, "size", entry.Size, "sha256", entry.SHA256, "stored", entry.Stored, "source_deleted", entry.SourceDeleted}
		if u.operationErr != nil {
			attrs[1] = "backup.failed"
			e.Logger.ErrorContext(ctx, "backup failed", append(attrs, "error", u.operationErr)...)
		} else {
			e.Logger.InfoContext(ctx, "backup completed", attrs...)
		}
	}
	// An uncertain write must retain the writer reservation for explicit takeover.
	if persistErr != nil {
		return record.Manifest, errors.Join(append(errs, persistErr)...)
	}
	if ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	after, checkErr := e.Inspect(ctx)
	if checkErr != nil {
		errs = append(errs, fmt.Errorf("final topology check: %w", checkErr))
	} else if after.Signature() != record.Manifest.Inventory.Signature() {
		errs = append(errs, fmt.Errorf("cluster topology changed during backup"))
	}
	complete := checkErr == nil && after.Signature() == record.Manifest.Inventory.Signature()
	for _, entry := range record.Manifest.Entries {
		complete = complete && entry.Stored && !entry.Deleted && entry.VerifyError == ""
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
		record.Manifest.State = "incomplete"
		record.Manifest.Error = errors.Join(errs...).Error()
	}
	return record.Manifest, errors.Join(errs...)
}
func (e Engine) transfer(ctx context.Context, entry *Entry, keepSource bool, checkpoint func() error) (operationErr, persistErr error) {
	ctx = telemetry.BackupContext(ctx, entry.Node, entry.Collection)
	client := e.Clients[entry.Node]
	if client == nil {
		err := fmt.Errorf("node %s has no client", entry.Node)
		entry.Error = err.Error()
		return err, checkpoint()
	}
	entry.Error = ""
	fail := func(err error) (error, error) { entry.Error = err.Error(); return err, checkpoint() }
	if entry.Snapshot == nil {
		phaseCtx, finish := telemetry.Phase(ctx, e.Logger, "snapshot.create")
		snapshot, err := client.CreateSnapshot(phaseCtx, entry.Collection)
		finish(int64(snapshot.Size), err)
		if err != nil {
			return fail(fmt.Errorf("create snapshot: %w", err))
		}
		entry.Snapshot = &snapshot
		entry.Key = entry.Collection + "/" + entry.Node + "/" + snapshot.Name
		e.Logger.InfoContext(ctx, "snapshot identity received", "event", "snapshot.received", "node", entry.Node, "collection", entry.Collection, "key", entry.Key)
		if err := checkpoint(); err != nil {
			return nil, err
		}
	}

	if entry.Stored {
		if err := CheckObject(ctx, e.Store, *entry); err != nil {
			if entry.VerifyError != "" {
				return fail(fmt.Errorf("recorded verification failure must be resolved with snapshots verify before recovery: %w", err))
			}
			if !errors.Is(err, storage.ErrNotFound) || entry.SourceDeleted {
				entry.VerifyError = err.Error()
				return fail(err)
			}
			entry.Stored = false
			if err := checkpoint(); err != nil {
				return nil, err
			}
		}
	}
	if !entry.Stored {
		var target backup.Storage = e.Store
		if e.Recover {
			target = recoveryUpload{e.Store}
		}
		result, err := backup.Transfer(ctx, e.Logger, client, target, entry.Collection, entry.Node, *entry.Snapshot)
		if err != nil {
			var status *qdrant.HTTPError
			if errors.As(err, &status) && status.StatusCode == 404 {
				err = fmt.Errorf("source snapshot %s is missing; refusing to replace it: %w", entry.Snapshot.Name, err)
			}
			return fail(err)
		}
		entry.Size = result.Size
		entry.SHA256 = result.SHA256
		entry.Stored = true
		entry.VerifyError = ""
		if err := checkpoint(); err != nil {
			return nil, err
		}
	}

	if !keepSource && !entry.SourceDeleted {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		err := client.DeleteSnapshot(ctx, entry.Collection, entry.Snapshot.Name)
		var status *qdrant.HTTPError
		if err != nil && !(errors.As(err, &status) && status.StatusCode == 404) {
			return fail(fmt.Errorf("backup stored but source cleanup failed: %w", err))
		}
		entry.SourceDeleted = true
		if err := checkpoint(); err != nil {
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
