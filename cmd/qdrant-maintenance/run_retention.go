package main

import (
	"context"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/runs"
)

func (a *application) logRunPlan(ctx context.Context, plan runs.RunPlan) {
	a.logger.InfoContext(ctx, "run retention plan built", "event", "retention.run_plan", "mode", plan.Mode, "keep_last", plan.KeepLast, "run_count", len(plan.Candidates), "snapshot_count", len(plan.Keys))
	for _, record := range plan.Candidates {
		a.logger.InfoContext(ctx, "run selected for pruning", "event", "retention.run_selected", "backup_run_id", record.Manifest.ID)
	}
	for _, key := range plan.Keys {
		a.logger.InfoContext(ctx, "snapshot selected for removal", "event", "retention.selected", "key", key)
	}
}
func (a *application) runRetention(ctx context.Context, store snapshotStore, keep int, action string) error {
	plan, err := runs.PlanRetention(ctx, runs.Repository{Store: store}, keep)
	if err != nil {
		return err
	}
	a.logRunPlan(ctx, plan)
	if action == "apply" {
		return fmt.Errorf("run retention application is not implemented yet")
	}
	return nil
}
