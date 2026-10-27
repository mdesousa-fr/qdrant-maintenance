package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/backup"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/retention"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/runs"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/s3"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/topology"
)

const version = "1.0.0"

func main() { os.Exit(mainExit()) }
func mainExit() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, shutdown, err := telemetry.Start(ctx, version, os.Stderr)
	slog.SetDefault(logger)
	if err == nil {
		err = run(ctx, logger, os.Args)
	} else {
		logger.Error("initialize telemetry", "event", "telemetry.initialization_failed", "error", err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if flushErr := shutdown(flushCtx); flushErr != nil {
		logger.Error("flush telemetry", "event", "telemetry.shutdown_failed", "error", flushErr)
		err = errors.Join(err, flushErr)
	}
	if err != nil {
		return 1
	}
	return 0
}

// Command results, help, and errors all use the logger. stdout is never used.
type snapshotStore interface {
	storage.Client
	backup.Storage
	storage.Repository
}
type application struct {
	logger          *slog.Logger
	runID           string
	storage         func(context.Context) (snapshotStore, error)
	qdrant          func(config.QdrantConfig, config.Node) backup.Qdrant
	qconfig         func(bool) (config.QdrantConfig, error)
	retentionConfig func() (config.RetentionConfig, error)
}

func newApplication(logger *slog.Logger) *application {
	app := &application{logger: logger, qconfig: config.LoadQdrant, retentionConfig: config.LoadRetention,
		qdrant: func(cfg config.QdrantConfig, node config.Node) backup.Qdrant {
			client := qdrant.NewClient(node.URL, cfg.APIKey)
			client.HTTPClient.(*http.Client).Timeout = cfg.Timeout
			return client
		}}
	app.storage = func(ctx context.Context) (snapshotStore, error) {
		cfg, err := config.LoadStorage()
		if err != nil {
			return nil, err
		}
		client, err := s3.NewClient(ctx, app.logger, cfg.Bucket, cfg.Region, cfg.ForcePathStyle, cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		client.Timeout = cfg.Timeout
		return client, nil
	}
	return app
}
func run(ctx context.Context, logger *slog.Logger, args []string) error {
	return newApplication(logger).run(ctx, args)
}
func (a *application) run(ctx context.Context, args []string) (err error) {
	originalLogger := a.logger
	a.runID = rand.Text()
	a.logger = a.logger.With("execution_id", a.runID)
	defer func() { a.logger = originalLogger }()
	command := ""
	if len(args) > 1 {
		command = args[1]
	}
	operation := "command.unknown"
	switch command {
	case "collections", "snapshots", "runs", "retention", "version", "help", "-h", "--help":
		operation = "command." + command
	}
	ctx, finish := telemetry.Operation(ctx, operation)
	defer func() {
		if err != nil {
			a.logger.ErrorContext(ctx, "command failed", "event", "command.failed", "command", command, "error", err)
		} else {
			a.logger.InfoContext(ctx, "command completed", "event", "command.completed", "command", command)
		}
		finish(err)
	}()
	if command == "" {
		return fmt.Errorf("command is required")
	}
	args = args[2:]
	switch command {
	case "help", "-h", "--help":
		if len(args) > 0 {
			return fmt.Errorf("too many arguments")
		}
		a.help(ctx, "qdrant-maintenance <collections|snapshots|runs|retention|version|help>")
		return nil
	case "version":
		fs := quietFlags("version")
		help, err := a.parse(ctx, fs, args, "qdrant-maintenance version")
		if err != nil || help {
			return err
		}
		if fs.NArg() > 0 {
			return fmt.Errorf("too many arguments")
		}
		a.logger.InfoContext(ctx, "application version", "event", "application.version", "version", version)
		return nil
	case "collections":
		return a.collections(ctx, args)
	case "snapshots":
		return a.snapshots(ctx, args)
	case "runs":
		return a.runs(ctx, args)
	case "retention":
		return a.retention(ctx, args)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}
func quietFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}
func (a *application) help(ctx context.Context, usage string) {
	a.logger.InfoContext(ctx, "command help", "event", "command.help", "usage", usage)
}
func (a *application) parse(ctx context.Context, fs *flag.FlagSet, args []string, usage string) (bool, error) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.help(ctx, usage)
			return true, nil
		}
		return false, fmt.Errorf("parse arguments: %w", err)
	}
	return false, nil
}
func allowApply(keepLast int, force bool) error {
	if keepLast < 0 {
		return fmt.Errorf("negative keepLast is forbidden")
	}
	if keepLast == 0 && !force {
		return fmt.Errorf("retention KeepLast is 0 and may delete all snapshots; use --force to continue")
	}
	return nil
}
func (a *application) collections(ctx context.Context, args []string) error {
	fs := quietFlags("collections")
	help, err := a.parse(ctx, fs, args, "qdrant-maintenance collections")
	if err != nil || help {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments")
	}
	cfg, err := a.qconfig(false)
	if err != nil {
		return err
	}
	client := a.qdrant(cfg, config.Node{URL: cfg.URL})
	collections, err := client.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}
	sort.Slice(collections, func(i, j int) bool { return collections[i].Name < collections[j].Name })
	for _, c := range collections {
		a.logger.InfoContext(ctx, "collection found", "event", "collection.found", "collection", c.Name)
	}
	a.logger.InfoContext(ctx, "collections listed", "event", "collections.listed", "collection_count", len(collections))
	return nil
}
func (a *application) retention(ctx context.Context, args []string) error {
	const usage = "qdrant-maintenance retention <plan|apply> [--force]"
	if len(args) == 0 {
		return fmt.Errorf("retention action is required")
	}
	action := args[0]
	if action == "--help" || action == "-h" {
		a.help(ctx, usage)
		return nil
	}
	if action != "plan" && action != "apply" {
		return fmt.Errorf("unknown retention action: %s", action)
	}
	fs := quietFlags("retention " + action)
	force, takeover := false, false
	if action == "apply" {
		fs.BoolVar(&takeover, "takeover", false, "take over pruning only after the previous process has terminated")
		fs.BoolVar(&force, "force", false, "allow deleting all snapshots when RETENTION_KEEP_LAST=0")
	}
	help, err := a.parse(ctx, fs, args[1:], usage)
	if err != nil || help {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	cfg, err := a.retentionConfig()
	if err != nil {
		return err
	}
	if action == "apply" {
		if err := allowApply(cfg.KeepLast, force); err != nil {
			return err
		}
	}
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	if cfg.Policy == "run" {
		return a.runRetention(ctx, store, cfg.KeepLast, action, takeover)
	}
	if takeover {
		return fmt.Errorf("--takeover applies only to run retention")
	}
	plan, err := retention.BuildPlan(ctx, store, cfg.KeepLast)
	if err != nil {
		return err
	}
	a.logger.InfoContext(ctx, "retention plan built", "event", "retention.planned", "keep_last", plan.KeepLast, "snapshot_count", len(plan.SnapshotsToRemove))
	for _, s := range plan.SnapshotsToRemove {
		a.logger.InfoContext(ctx, "snapshot selected for removal", "event", "retention.selected", "key", s.Key, "collection", s.Collection, "node", s.Node)
	}
	if action == "apply" {
		if err = plan.Apply(ctx, store); err != nil {
			return fmt.Errorf("apply retention: %w", err)
		}
		a.logger.InfoContext(ctx, "retention applied", "event", "retention.applied", "snapshot_count", len(plan.SnapshotsToRemove))
	}
	return nil
}
func (a *application) snapshots(ctx context.Context, args []string) error {
	const usage = "qdrant-maintenance snapshots list <collection> | snapshots create [--all | <collection>] [--keep-source] (flags before collection)"
	if len(args) == 0 {
		return fmt.Errorf("snapshots action is required")
	}
	action := args[0]
	if action == "verify" {
		return a.verify(ctx, args[1:])
	}
	if action == "--help" || action == "-h" {
		a.help(ctx, usage)
		return nil
	}
	if action != "list" && action != "create" {
		return fmt.Errorf("unknown snapshot action: %s", action)
	}
	fs := quietFlags("snapshots " + action)
	all, keepSource := false, false
	if action == "create" {
		fs.BoolVar(&all, "all", false, "back up all collections on every configured node")
		fs.BoolVar(&keepSource, "keep-source", false, "retain snapshots on Qdrant after upload")
	}
	help, err := a.parse(ctx, fs, args[1:], usage)
	if err != nil || help {
		return err
	}
	if all && fs.NArg() != 0 {
		return fmt.Errorf("--all cannot be combined with a collection name")
	}
	if !all && fs.NArg() == 0 {
		return fmt.Errorf("collection name is required")
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}
	collection := fs.Arg(0)
	if !all && !config.ValidSegment(collection) {
		return fmt.Errorf("invalid collection name")
	}
	if action == "list" {
		store, err := a.storage(ctx)
		if err != nil {
			return err
		}
		objects, err := store.ListObjects(ctx)
		if err != nil {
			return err
		}
		snapshots := []retention.Snapshot{}
		for _, s := range retention.BuildInventory(objects) {
			if s.Collection == collection {
				snapshots = append(snapshots, s)
			}
		}
		retention.SortNewestFirst(snapshots)
		for _, s := range snapshots {
			a.logger.InfoContext(ctx, "snapshot found", "event", "snapshot.found", "name", s.Name+".snapshot", "key", s.Key, "collection", s.Collection, "node", s.Node, "last_modified", s.CreatedAt.UTC(), "size", s.Size)
		}
		a.logger.InfoContext(ctx, "snapshots listed", "event", "snapshots.listed", "collection", collection, "snapshot_count", len(snapshots))
		return nil
	}
	a.logger = a.logger.With("run_id", a.runID)
	cfg, err := a.qconfig(true)
	if err != nil {
		return err
	}
	clients := map[string]backup.Qdrant{}
	endpoints := []topology.Endpoint{}
	for _, node := range cfg.Nodes {
		client := a.qdrant(cfg, node)
		clients[node.ID] = client
		endpoints = append(endpoints, topology.Endpoint{ID: node.ID, Client: client})
	}
	inventory, err := topology.Inspect(ctx, endpoints, collection)
	if err != nil {
		return err
	}
	a.logger.InfoContext(ctx, "cluster coverage validated", "event", "cluster.validated", "cluster_id", inventory.ClusterID, "expected_count", len(inventory.Pairs))
	store, err := a.storage(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	engine := runs.Engine{Repo: runs.Repository{Store: store}, Store: store, Clients: clients, Logger: a.logger, Inspect: func(ctx context.Context) (topology.Inventory, error) {
		return topology.Inspect(ctx, endpoints, collection)
	}}
	manifest, runErr := engine.Create(ctx, runs.New(a.runID, version, collection, inventory, keepSource))
	successful, failed, skipped, cleanupFailures := 0, 0, 0, 0
	var bytes int64
	for _, entry := range manifest.Entries {
		if entry.Stored {
			successful++
			bytes += entry.Size
			if entry.Error != "" {
				cleanupFailures++
			}
		} else if entry.Error != "" {
			failed++
		} else {
			skipped++
		}
	}
	outcome := "success"
	if runErr != nil {
		outcome = "error"
	}
	a.logger.InfoContext(ctx, "backup run finished", "event", "backup.run_summary", "expected_count", len(manifest.Entries), "successful_count", successful, "failed_count", failed, "skipped_count", skipped, "cleanup_failed_count", cleanupFailures, "uploaded_bytes", bytes, "duration_seconds", time.Since(start).Seconds(), "outcome", outcome, "complete", manifest.State == "complete")
	return runErr
}
