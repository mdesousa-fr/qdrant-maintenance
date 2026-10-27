package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/retention"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/runs"
)

func (a *application) verify(ctx context.Context, args []string) error {
	fs := quietFlags("snapshots verify")
	id := fs.String("run", "", "verify a recorded run")
	collection := fs.String("collection", "", "collection selector")
	node := fs.String("node", "", "node selector")
	takeover := fs.Bool("takeover", false, "take over only after the previous process has terminated")
	help, err := a.parse(ctx, fs, args, "qdrant-maintenance snapshots verify --run <run_id> | --collection <collection> --node <node> [--takeover]")
	if err != nil || help {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *id != "" && (*collection != "" || *node != "") {
		return fmt.Errorf("--run cannot be combined with collection/node selectors")
	}
	if *id == "" && (!config.ValidSegment(*collection) || !config.ValidSegment(*node)) {
		return fmt.Errorf("--run or both --collection and --node are required")
	}
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	repo := runs.Repository{Store: store}
	count := 0
	var errs []error
	if *id != "" {
		count, err = repo.Verify(ctx, *id, "", "", *takeover, a.logger)
		if err != nil {
			errs = append(errs, err)
		}
	} else {
		records, err := repo.List(ctx)
		if err != nil {
			return err
		}
		managed := map[string]bool{}
		for _, record := range records {
			match := false
			for _, entry := range record.Manifest.Entries {
				if entry.Key != "" {
					managed[entry.Key] = true
				}
				if entry.Stored && !entry.Deleted && entry.Collection == *collection && entry.Node == *node {
					match = true
				}
			}
			if !match || record.Manifest.State == "pruned" || record.Manifest.State == "expired" {
				continue
			}
			n, err := repo.Verify(ctx, record.Manifest.ID, *collection, *node, *takeover, a.logger)
			count += n
			if err != nil {
				errs = append(errs, err)
			}
		}
		objects, err := store.ListObjects(ctx)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, snapshot := range retention.BuildInventory(objects) {
			if snapshot.Collection != *collection || snapshot.Node != *node || managed[snapshot.Key] {
				continue
			}
			count++
			details, verifyErr := store.HeadObject(ctx, snapshot.Key)
			if verifyErr == nil {
				verifyErr = runs.VerifyObject(ctx, store, snapshot.Key, details.Size, details.SHA256)
			}
			if verifyErr != nil {
				errs = append(errs, verifyErr)
				a.logger.ErrorContext(ctx, "unmanaged snapshot verification failed", "event", "snapshot.verification_failed", "key", snapshot.Key, "error", verifyErr)
			} else {
				a.logger.InfoContext(ctx, "unmanaged snapshot verified", "event", "snapshot.verified", "key", snapshot.Key, "managed", false)
			}
		}
	}
	if count == 0 {
		errs = append(errs, fmt.Errorf("no stored snapshots selected for verification"))
	}
	a.logger.InfoContext(ctx, "snapshot verification finished", "event", "snapshot.verification_summary", "snapshot_count", count, "failed", len(errs) > 0)
	return errors.Join(errs...)
}
