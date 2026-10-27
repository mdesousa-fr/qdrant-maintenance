package main

import (
	"context"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/runs"
)

func (a *application) runs(ctx context.Context, args []string) error {
	const usage = "qdrant-maintenance runs list"
	if len(args) == 0 {
		return fmt.Errorf("runs action is required")
	}
	if args[0] == "--help" || args[0] == "-h" {
		a.help(ctx, usage)
		return nil
	}
	if args[0] != "list" {
		return fmt.Errorf("unknown runs action: %s", args[0])
	}
	fs := quietFlags("runs list")
	help, err := a.parse(ctx, fs, args[1:], usage)
	if err != nil || help {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	records, err := (runs.Repository{Store: store}).List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		m := record.Manifest
		a.logger.InfoContext(ctx, "backup run found", "event", "run.found", "backup_run_id", m.ID, "cluster_id", m.Inventory.ClusterID, "state", m.State, "started_at", m.StartedAt, "completed_at", m.CompletedAt, "snapshot_count", len(m.Entries), "error", m.Error)
	}
	a.logger.InfoContext(ctx, "backup runs listed", "event", "runs.listed", "run_count", len(records))
	return nil
}
