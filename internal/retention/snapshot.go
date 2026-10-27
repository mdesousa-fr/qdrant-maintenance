package retention

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
)

type Snapshot struct {
	Key        string    `json:"key"`
	Collection string    `json:"collection"`
	Node       string    `json:"node"`
	Name       string    `json:"name"`
	CreatedAt  time.Time `json:"last_modified"`
	Size       int64     `json:"size"`
}

func ToSnapshot(object storage.Object) (Snapshot, bool) {
	if !strings.HasSuffix(object.Key, ".snapshot") {
		return Snapshot{}, false
	}

	fields := strings.Split(object.Key, "/")

	if len(fields) != 3 {
		return Snapshot{}, false
	}

	collection := fields[0]
	node := fields[1]
	if node == "" || node == "." || node == ".." {
		return Snapshot{}, false
	}
	name := strings.TrimSuffix(fields[2], ".snapshot")

	if collection == "" || name == "" {
		return Snapshot{}, false
	}

	return Snapshot{
		Key:        object.Key,
		Collection: collection,
		Node:       node,
		Name:       name,
		CreatedAt:  object.LastModified,
		Size:       object.Size,
	}, true
}

func BuildInventory(objects []storage.Object) []Snapshot {
	var snapshots []Snapshot

	for _, object := range objects {
		snapshot, ok := ToSnapshot(object)

		if !ok {
			continue
		}

		snapshots = append(snapshots, snapshot)
	}

	return snapshots
}

func SortNewestFirst(snapshots []Snapshot) {
	slices.SortFunc(snapshots, func(a, b Snapshot) int {
		if n := b.CreatedAt.Compare(a.CreatedAt); n != 0 {
			return n
		}
		return cmp.Compare(a.Key, b.Key)
	})
}

func KeepLast(s []Snapshot, count int) (keep []Snapshot, remove []Snapshot, err error) {
	if count < 0 {
		err = errors.New("negative count is forbidden on KeepLast")
		return
	}

	snapshots := slices.Clone(s)

	SortNewestFirst(snapshots)

	if count >= len(snapshots) {
		keep = snapshots
		return
	}

	keep = snapshots[:count]
	remove = snapshots[count:]
	return
}

func GroupByCollection(s []Snapshot) map[string][]Snapshot {
	snapshots := make(map[string][]Snapshot)
	for _, snapshot := range s {
		group := snapshot.Collection + "/" + snapshot.Node
		snapshots[group] = append(snapshots[group], snapshot)
	}
	return snapshots
}

type CollectionRetention struct {
	Keep   []Snapshot
	Remove []Snapshot
}

func KeepLastByCollection(s []Snapshot, keepLast int) (map[string]CollectionRetention, error) {
	if keepLast < 0 {
		return nil, errors.New("negative keepLast is forbidden")
	}
	output := make(map[string]CollectionRetention)

	snapshotsByCollection := GroupByCollection(s)

	for collection, snapshots := range snapshotsByCollection {
		keep, remove, err := KeepLast(snapshots, keepLast)
		if err != nil {
			return nil, fmt.Errorf("keep last by collection: %w", err)
		}
		output[collection] = CollectionRetention{
			Keep:   keep,
			Remove: remove,
		}
	}

	return output, nil
}

type Plan struct {
	KeepLast          int        `json:"keep_last"`
	SnapshotsToRemove []Snapshot `json:"snapshots_to_remove"`
}

func NewPlan(s map[string]CollectionRetention, keepLast int) Plan {
	plan := Plan{
		KeepLast:          keepLast,
		SnapshotsToRemove: []Snapshot{},
	}
	for _, retention := range s {
		plan.SnapshotsToRemove = append(plan.SnapshotsToRemove, retention.Remove...)
	}
	SortNewestFirst(plan.SnapshotsToRemove)
	return plan
}

func (p Plan) Apply(ctx context.Context, client storage.Client) (err error) {
	ctx, finish := telemetry.Operation(ctx, "retention.apply")
	defer func() { finish(err) }()
	var errs []error

	for _, snapshot := range p.SnapshotsToRemove {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := client.DeleteObject(ctx, snapshot.Key); err != nil {
			errs = append(errs, err)
		} else {
			telemetry.Deleted(ctx)
		}
	}

	return errors.Join(errs...)
}

func BuildPlan(ctx context.Context, client storage.Client, keepLast int) (Plan, error) {
	if keepLast < 0 {
		return Plan{}, errors.New("negative keepLast is forbidden")
	}
	objects, err := client.ListObjects(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("build plan: %w", err)
	}
	snapshots := BuildInventory(objects)
	retentionByCollection, err := KeepLastByCollection(snapshots, keepLast)
	if err != nil {
		return Plan{}, fmt.Errorf("build plan: %w", err)
	}
	return NewPlan(retentionByCollection, keepLast), nil
}
