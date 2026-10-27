package main

import (
	"context"
	"fmt"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/runs"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
	"time"
)

func (a *application) runs(ctx context.Context, args []string) error {
	const usage = "qdrant-maintenance runs list | runs check --max-age <duration> [--collection <name>]"
	if len(args) == 0 {
		return fmt.Errorf("runs action is required")
	}
	if args[0] == "--help" || args[0] == "-h" {
		a.help(ctx, usage)
		return nil
	}
	if args[0] == "retry" {
		return a.recoverRun(ctx, args[1:], false)
	}
	if args[0] == "expire" {
		return a.expireRuns(ctx, args[1:])
	}
	if args[0] == "check" {
		return a.checkRuns(ctx, args[1:])
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

func (a *application) inspect(ctx context.Context, scope string) (topology.Inventory, map[string]backup.Qdrant, []topology.Endpoint, error) {
	cfg, err := a.qconfig(true)
	if err != nil {
		return topology.Inventory{}, nil, nil, err
	}
	clients := map[string]backup.Qdrant{}
	endpoints := []topology.Endpoint{}
	for _, node := range cfg.Nodes {
		client := a.qdrant(cfg, node)
		clients[node.ID] = client
		endpoints = append(endpoints, topology.Endpoint{ID: node.ID, Client: client})
	}
	inventory, err := topology.Inspect(ctx, endpoints, scope)
	return inventory, clients, endpoints, err
}
func (a *application) checkRuns(ctx context.Context, args []string) error {
	fs := quietFlags("runs check")
	maxAge := fs.Duration("max-age", 0, "maximum acceptable backup age")
	scope := fs.String("collection", "", "check only this collection")
	help, err := a.parse(ctx, fs, args, "qdrant-maintenance runs check --max-age <duration> [--collection <name>]")
	if err != nil || help {
		return err
	}
	if fs.NArg() != 0 || *maxAge <= 0 {
		return fmt.Errorf("a positive --max-age is required; no positional arguments allowed")
	}
	if *scope != "" && !config.ValidSegment(*scope) {
		return fmt.Errorf("invalid collection")
	}
	inventory, _, _, err := a.inspect(ctx, *scope)
	if err != nil {
		return err
	}
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	m, err := runs.Check(ctx, runs.Repository{Store: store}, inventory, *scope, *maxAge, time.Now().UTC())
	if err != nil {
		a.logger.ErrorContext(ctx, "backup freshness check failed", "event", "run.check_failed", "backup_run_id", m.ID, "error", err)
		return err
	}
	age := time.Since(m.StartedAt)
	telemetry.BackupAge(ctx, age.Seconds())
	a.logger.InfoContext(ctx, "backup freshness check passed", "event", "run.check_passed", "backup_run_id", m.ID, "cluster_id", m.Inventory.ClusterID, "age_seconds", age.Seconds(), "snapshot_count", len(m.Entries))
	return nil
}

func (a *application) expireRuns(ctx context.Context, args []string) error {
	fs := quietFlags("runs expire")
	age := fs.Duration("older-than", 0, "minimum age of incomplete runs")
	apply := fs.Bool("apply", false, "apply expiry instead of logging the plan")
	takeover := fs.Bool("takeover", false, "take over only after the previous process has terminated")
	help, err := a.parse(ctx, fs, args, "qdrant-maintenance runs expire --older-than <duration> [--apply] [--takeover]")
	if err != nil || help {
		return err
	}
	if fs.NArg() != 0 || *age <= 0 {
		return fmt.Errorf("a positive --older-than is required")
	}
	if *takeover && !*apply {
		return fmt.Errorf("--takeover requires --apply")
	}
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	repo := runs.Repository{Store: store}
	plan, err := runs.PlanExpiry(ctx, repo, *age, time.Now().UTC())
	if err != nil {
		return err
	}
	a.logRunPlan(ctx, plan)
	if *apply {
		return plan.Apply(ctx, repo, a.logger, *takeover)
	}
	return nil
}

func (a *application) recoverRun(ctx context.Context, args []string, cleanup bool) error {
	action := "retry"
	if cleanup {
		action = "cleanup"
	}
	fs := quietFlags("runs " + action)
	takeover := fs.Bool("takeover", false, "take over only after the previous process has terminated")
	help, err := a.parse(ctx, fs, args, "qdrant-maintenance runs "+action+" [--takeover] <run_id>")
	if err != nil || help {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("one run ID is required")
	}
	id := fs.Arg(0)
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	repo := runs.Repository{Store: store}
	record, err := repo.Load(ctx, id)
	if err != nil {
		return err
	}
	a.logger = a.logger.With("run_id", id)
	_, clients, endpoints, err := a.inspect(ctx, record.Manifest.Scope)
	if err != nil {
		return err
	}
	engine := runs.Engine{Repo: repo, Store: store, Clients: clients, Logger: a.logger, Inspect: func(ctx context.Context) (topology.Inventory, error) {
		return topology.Inspect(ctx, endpoints, record.Manifest.Scope)
	}}
	_, err = engine.Retry(ctx, id, *takeover)
	return err
}
