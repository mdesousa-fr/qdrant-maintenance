package retention

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
)

func TestToSnapshot(t *testing.T) {
	tests := []struct {
		name             string
		input            storage.Object
		expectedSnapshot Snapshot
		expectedStatus   bool
	}{
		{
			name: "valid snapshot",
			input: storage.Object{
				Key:          "documents/node-0/snapshot-1.snapshot",
				LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
				Size:         1024,
			},
			expectedSnapshot: Snapshot{
				Key:        "documents/node-0/snapshot-1.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-1",
				CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			expectedStatus: true,
		},
		{
			name: "txt file",
			input: storage.Object{
				Key: "documents/node-0/readme.txt",
			},
			expectedSnapshot: Snapshot{},
			expectedStatus:   false,
		},
		{
			name: "empty snapshot name",
			input: storage.Object{
				Key: "documents/node-0/.snapshot",
			},
			expectedSnapshot: Snapshot{},
			expectedStatus:   false,
		},
		{
			name: "more than 3 fields",
			input: storage.Object{
				Key: "documents/daily/extra/snapshot-1.snapshot",
			},
			expectedSnapshot: Snapshot{},
			expectedStatus:   false,
		},
		{
			name: "no delimiter",
			input: storage.Object{
				Key: "snapshot-1.snapshot",
			},
			expectedSnapshot: Snapshot{},
			expectedStatus:   false,
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				snapshot, ok := ToSnapshot(test.input)

				if ok != test.expectedStatus {
					t.Errorf("status = %v, want %v", ok, test.expectedStatus)
				}

				if snapshot != test.expectedSnapshot {
					t.Errorf("snapshot = %+v, want %+v", snapshot, test.expectedSnapshot)
				}
			},
		)
	}
}

func TestBuildInventory(t *testing.T) {
	objects := []storage.Object{
		{
			Key:          "documents/node-0/snapshot-1.snapshot",
			LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
		{
			Key:          "documents/node-0/readme.txt",
			LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
		{
			Key:          "users/node-0/snapshot-2.snapshot",
			LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
		{
			Key:          "logs/application.log",
			LastModified: time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
	}
	expected := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "users/node-0/snapshot-2.snapshot",
			Collection: "users",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}

	inventory := BuildInventory(objects)

	if len(inventory) != len(expected) {
		t.Fatalf("len(inventory) = %d, want %d", len(inventory), len(expected))
	}

	for i, snapshot := range inventory {
		if snapshot != expected[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected[i])
		}
	}
}

func TestSortNewestFirst(t *testing.T) {
	snapshots := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-3.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-3",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}
	expected := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-3.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-3",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}

	SortNewestFirst(snapshots)

	for i, snapshot := range snapshots {
		if snapshot != expected[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected[i])
		}
	}
}

func TestKeepLast(t *testing.T) {
	snapshots := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-5.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-5",
			CreatedAt:  time.Date(2026, time.October, 5, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-4.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-4",
			CreatedAt:  time.Date(2026, time.October, 4, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-3.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-3",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}
	expected := struct {
		Snapshots []Snapshot
		Keep      []Snapshot
		Remove    []Snapshot
	}{
		Snapshots: []Snapshot{
			{
				Key:        "documents/node-0/snapshot-1.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-1",
				CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-5.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-5",
				CreatedAt:  time.Date(2026, time.October, 5, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-4.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-4",
				CreatedAt:  time.Date(2026, time.October, 4, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-3.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-3",
				CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-2.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-2",
				CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
		},
		Keep: []Snapshot{
			{
				Key:        "documents/node-0/snapshot-5.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-5",
				CreatedAt:  time.Date(2026, time.October, 5, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-4.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-4",
				CreatedAt:  time.Date(2026, time.October, 4, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
		},
		Remove: []Snapshot{
			{
				Key:        "documents/node-0/snapshot-3.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-3",
				CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-2.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-2",
				CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-1.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-1",
				CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
		},
	}

	keep, remove, err := KeepLast(snapshots, 2)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if len(keep) != 2 {
		t.Fatalf("len(keep) = %d, want %d", len(keep), 2)
	}

	if len(remove) != 3 {
		t.Fatalf("len(remove) = %d, want %d", len(remove), 3)
	}

	for i, snapshot := range snapshots {
		if snapshot != expected.Snapshots[i] {
			t.Errorf("snapshot in inventory = %+v, want %+v", snapshot, expected.Snapshots[i])
		}
	}

	for i, snapshot := range keep {
		if snapshot != expected.Keep[i] {
			t.Errorf("snapshot to keep = %+v, want %+v", snapshot, expected.Keep[i])
		}
	}

	for i, snapshot := range remove {
		if snapshot != expected.Remove[i] {
			t.Errorf("snapshot to remove = %+v, want %+v", snapshot, expected.Remove[i])
		}
	}
}

func TestKeepLastOutOfRange(t *testing.T) {
	snapshots := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}

	keep, remove, err := KeepLast(snapshots, 5)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if len(keep) != 2 {
		t.Fatalf("len(keep) = %d, want %d", len(keep), 2)
	}

	if len(remove) != 0 {
		t.Fatalf("len(remove) = %d, want %d", len(remove), 0)
	}

	for i, snapshot := range keep {
		if snapshot != snapshots[i] {
			t.Errorf("snapshot to keep = %+v, want %+v", snapshot, snapshots[i])
		}
	}
}

func TestKeepLastNegativeCount(t *testing.T) {
	snapshots := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}

	expected := slices.Clone(snapshots)

	keep, remove, err := KeepLast(snapshots, -1)

	if err == nil {
		t.Fatalf("expected an error, got nil")
	}

	if keep != nil {
		t.Errorf("keep = %+v, want nil", keep)
	}

	if remove != nil {
		t.Errorf("remove = %+v, want nil", remove)
	}

	for i, snapshot := range snapshots {
		if snapshot != expected[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected[i])
		}
	}
}

func TestGroupByCollection(t *testing.T) {
	snapshots := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "users/node-0/snapshot-1.snapshot",
			Collection: "users",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}
	expected := map[string][]Snapshot{
		"documents/node-0": {
			{
				Key:        "documents/node-0/snapshot-2.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-2",
				CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
			{
				Key:        "documents/node-0/snapshot-1.snapshot",
				Collection: "documents",
				Node:       "node-0",
				Name:       "snapshot-1",
				CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
		},
		"users/node-0": {
			{
				Key:        "users/node-0/snapshot-1.snapshot",
				Collection: "users",
				Node:       "node-0",
				Name:       "snapshot-1",
				CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
				Size:       1024,
			},
		},
	}

	groupedSnapshots := GroupByCollection(snapshots)

	if len(groupedSnapshots) != 2 {
		t.Fatalf("len(groupedSnapshots) = %d, want %d", len(groupedSnapshots), 2)
	}

	documentsSnapshots, ok := groupedSnapshots["documents/node-0"]

	if !ok {
		t.Fatal(`groupedSnapshots["documents/node-0"] does not exist`)
	}

	usersSnapshots, ok := groupedSnapshots["users/node-0"]
	if !ok {
		t.Fatal(`groupedSnapshots["users/node-0"] does not exist`)
	}

	if len(documentsSnapshots) != 2 {
		t.Fatalf("len(groupedSnapshots[\"documents\"]) = %d, want %d", len(documentsSnapshots), 2)
	}

	if len(usersSnapshots) != 1 {
		t.Fatalf("len(groupedSnapshots[\"users\"]) = %d, want %d", len(usersSnapshots), 1)
	}

	for i, snapshot := range documentsSnapshots {
		if snapshot != expected["documents/node-0"][i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["documents/node-0"][i])
		}
	}

	for i, snapshot := range usersSnapshots {
		if snapshot != expected["users/node-0"][i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["users/node-0"][i])
		}
	}
}

func TestKeepLastByCollection(t *testing.T) {
	input := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-3.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-3",
			CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "users/node-0/snapshot-1.snapshot",
			Collection: "users",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "users/node-0/snapshot-2.snapshot",
			Collection: "users",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}
	expected := map[string]CollectionRetention{
		"documents/node-0": {
			Keep: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-3.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-3",
					CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-2.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
				{
					Key:        "documents/node-0/snapshot-1.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
		"users/node-0": {
			Keep: []Snapshot{
				{
					Key:        "users/node-0/snapshot-2.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "users/node-0/snapshot-1.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
	}

	snapshotsRetentionByCollection, err := KeepLastByCollection(input, 1)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if len(snapshotsRetentionByCollection) != len(expected) {
		t.Fatalf("len(snapshotsRetentionByCollection) = %d, want %d", len(snapshotsRetentionByCollection), len(expected))
	}

	documentsSnapshotsRetention, ok := snapshotsRetentionByCollection["documents/node-0"]

	if !ok {
		t.Fatal(`snapshotsRetentionByCollection["documents/node-0"] does not exist`)
	}

	usersSnapshotsRetention, ok := snapshotsRetentionByCollection["users/node-0"]
	if !ok {
		t.Fatal(`snapshotsRetentionByCollection["users/node-0"] does not exist`)
	}

	if len(documentsSnapshotsRetention.Keep) != len(expected["documents/node-0"].Keep) {
		t.Fatalf("documentsSnapshotsRetention.Keep = %d, want %d", len(documentsSnapshotsRetention.Keep), len(expected["documents/node-0"].Keep))
	}

	if len(documentsSnapshotsRetention.Remove) != len(expected["documents/node-0"].Remove) {
		t.Fatalf("documentsSnapshotsRetention.Remove = %d, want %d", len(documentsSnapshotsRetention.Remove), len(expected["documents/node-0"].Remove))
	}

	if len(usersSnapshotsRetention.Keep) != len(expected["users/node-0"].Keep) {
		t.Fatalf("usersSnapshotsRetention.Keep = %d, want %d", len(usersSnapshotsRetention.Keep), len(expected["users/node-0"].Keep))
	}

	if len(usersSnapshotsRetention.Remove) != len(expected["users/node-0"].Remove) {
		t.Fatalf("usersSnapshotsRetention.Remove = %d, want %d", len(usersSnapshotsRetention.Remove), len(expected["users/node-0"].Remove))
	}

	for i, snapshot := range documentsSnapshotsRetention.Keep {
		if snapshot != expected["documents/node-0"].Keep[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["documents/node-0"].Keep[i])
		}
	}

	for i, snapshot := range documentsSnapshotsRetention.Remove {
		if snapshot != expected["documents/node-0"].Remove[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["documents/node-0"].Remove[i])
		}
	}

	for i, snapshot := range usersSnapshotsRetention.Keep {
		if snapshot != expected["users/node-0"].Keep[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["users/node-0"].Keep[i])
		}
	}

	for i, snapshot := range usersSnapshotsRetention.Remove {
		if snapshot != expected["users/node-0"].Remove[i] {
			t.Errorf("snapshot = %+v, want %+v", snapshot, expected["users/node-0"].Remove[i])
		}
	}
}

func TestNewPlan(t *testing.T) {
	input := map[string]CollectionRetention{
		"documents/node-0": {
			Keep: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-3.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-3",
					CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-2.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
				{
					Key:        "documents/node-0/snapshot-1.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
		"users/node-0": {
			Keep: []Snapshot{
				{
					Key:        "users/node-0/snapshot-2.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "users/node-0/snapshot-1.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
	}

	expected := []Snapshot{
		{
			Key:        "documents/node-0/snapshot-2.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-2",
			CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "documents/node-0/snapshot-1.snapshot",
			Collection: "documents",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
		{
			Key:        "users/node-0/snapshot-1.snapshot",
			Collection: "users",
			Node:       "node-0",
			Name:       "snapshot-1",
			CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:       1024,
		},
	}

	plan := NewPlan(input, 1)

	if len(plan.SnapshotsToRemove) != len(expected) {
		t.Fatalf("len(plan.SnapshotsToRemove) = %d, want %d", len(plan.SnapshotsToRemove), len(expected))
	}

	for _, snapshot := range plan.SnapshotsToRemove {
		if !slices.Contains(expected, snapshot) {
			t.Fatalf("unexpected snapshot in plan: %+v", snapshot)
		}
	}
}

var _ storage.Client = (*fakeStorageClient)(nil)

type fakeStorageClient struct {
	ObjectKeysToDelete []string

	DeleteObjectErr   error
	DeleteObjectErrOn int

	ListObjectsOutput []storage.Object
	ListObjectsErr    error
}

func (c *fakeStorageClient) ListObjects(ctx context.Context) ([]storage.Object, error) {
	if c.ListObjectsErr != nil {
		return nil, c.ListObjectsErr
	}

	return c.ListObjectsOutput, nil
}

func (c *fakeStorageClient) DeleteObject(ctx context.Context, key string) error {
	c.ObjectKeysToDelete = append(c.ObjectKeysToDelete, key)

	if c.DeleteObjectErrOn == len(c.ObjectKeysToDelete) {
		return c.DeleteObjectErr
	}

	return nil
}

func TestPlanApplyWithError(t *testing.T) {
	input := map[string]CollectionRetention{
		"documents/node-0": {
			Keep: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-3.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-3",
					CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-2.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
				{
					Key:        "documents/node-0/snapshot-1.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
		"users/node-0": {
			Keep: []Snapshot{
				{
					Key:        "users/node-0/snapshot-2.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "users/node-0/snapshot-1.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
	}
	expectedKeys := []string{
		"documents/node-0/snapshot-2.snapshot",
		"documents/node-0/snapshot-1.snapshot",
		"users/node-0/snapshot-1.snapshot",
	}

	expectedError := errors.New("access denied")
	client := &fakeStorageClient{
		DeleteObjectErr:   expectedError,
		DeleteObjectErrOn: 2,
	}

	plan := NewPlan(input, 1)
	err := plan.Apply(context.Background(), client)

	if !errors.Is(err, expectedError) {
		t.Fatalf("err = %q, want %q", err, expectedError)
	}

	if len(client.ObjectKeysToDelete) != len(expectedKeys) {
		t.Fatalf("len(client.ObjectKeysToDelete) = %d, want %d", len(client.ObjectKeysToDelete), len(expectedKeys))
	}

	for _, key := range client.ObjectKeysToDelete {
		if !slices.Contains(expectedKeys, key) {
			t.Errorf("unexpected key: %+v", key)
		}
	}
}

func TestPlanApplyWithoutError(t *testing.T) {
	input := map[string]CollectionRetention{
		"documents/node-0": {
			Keep: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-3.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-3",
					CreatedAt:  time.Date(2026, time.October, 3, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "documents/node-0/snapshot-2.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
				{
					Key:        "documents/node-0/snapshot-1.snapshot",
					Collection: "documents",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
		"users/node-0": {
			Keep: []Snapshot{
				{
					Key:        "users/node-0/snapshot-2.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-2",
					CreatedAt:  time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
			Remove: []Snapshot{
				{
					Key:        "users/node-0/snapshot-1.snapshot",
					Collection: "users",
					Node:       "node-0",
					Name:       "snapshot-1",
					CreatedAt:  time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
					Size:       1024,
				},
			},
		},
	}

	client := &fakeStorageClient{}

	plan := NewPlan(input, 1)
	if err := plan.Apply(context.Background(), client); err != nil {
		t.Fatalf("err = %q, want nil", err)
	}
}

func TestBuildPlanListObjectsError(t *testing.T) {
	expectedError := errors.New("access denied")

	client := fakeStorageClient{
		ListObjectsErr: expectedError,
	}

	_, err := BuildPlan(context.Background(), &client, 2)

	if !errors.Is(err, expectedError) {
		t.Fatalf("err = %q, want %q", err, expectedError)
	}
}

func TestBuildPlan(t *testing.T) {
	expectedKey := "documents/node-0/snapshot-1.snapshot"
	output := []storage.Object{
		{
			Key:          "documents/node-0/snapshot-1.snapshot",
			LastModified: time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
		{
			Key:          "documents/node-0/snapshot-2.snapshot",
			LastModified: time.Date(2026, time.October, 2, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
		{
			Key:          "documents/node-0/readme.txt",
			LastModified: time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
	}
	client := &fakeStorageClient{
		ListObjectsOutput: output,
	}

	plan, err := BuildPlan(context.Background(), client, 1)

	if err != nil {
		t.Fatalf("err = %q, want nil", err)
	}

	if len(plan.SnapshotsToRemove) != 1 {
		t.Fatalf("len(plan.SnapshotsToRemove) = %d, want 1", len(plan.SnapshotsToRemove))
	}

	if plan.SnapshotsToRemove[0].Key != expectedKey {
		t.Errorf("Snapshot key = %q, want %q", plan.SnapshotsToRemove[0].Key, expectedKey)
	}
}

func TestBuildPlanNegativeKeepLast(t *testing.T) {
	output := []storage.Object{
		{
			Key:          "documents/node-0/snapshot-1.snapshot",
			LastModified: time.Date(2026, time.October, 1, 12, 11, 28, 0, time.UTC),
			Size:         1024,
		},
	}
	client := &fakeStorageClient{
		ListObjectsOutput: output,
	}
	plan, err := BuildPlan(context.Background(), client, -1)

	if err == nil {
		t.Fatalf("expected an error, got nil")
	}

	if len(plan.SnapshotsToRemove) != 0 {
		t.Fatalf("len(plan.SnapshotsToRemove) = %d, want 0", len(plan.SnapshotsToRemove))
	}
}

func TestRetentionIsIndependentPerNode(t *testing.T) {
	objects := []storage.Object{}
	for _, group := range []string{"docs/node-0", "docs/node-1"} {
		objects = append(objects, storage.Object{Key: group + "/old.snapshot", LastModified: time.Unix(1, 0)}, storage.Object{Key: group + "/new.snapshot", LastModified: time.Unix(2, 0)})
	}
	client := &fakeStorageClient{ListObjectsOutput: objects}
	plan, err := BuildPlan(context.Background(), client, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SnapshotsToRemove) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	for _, s := range plan.SnapshotsToRemove {
		if s.Name != "old" {
			t.Fatalf("removed newest snapshot: %+v", s)
		}
	}
	// Equal timestamps resolve by key, independently of S3 listing order.
	equal := []Snapshot{{Key: "b", CreatedAt: time.Unix(1, 0)}, {Key: "a", CreatedAt: time.Unix(1, 0)}}
	SortNewestFirst(equal)
	if equal[0].Key != "a" {
		t.Fatal(equal)
	}
}
func TestEmptyInventoryRejectsNegativeRetention(t *testing.T) {
	if _, err := BuildPlan(context.Background(), &fakeStorageClient{}, -1); err == nil {
		t.Fatal("negative retention accepted")
	}
}
func TestCancelledPlanStopsDeleting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeStorageClient{}
	plan := Plan{SnapshotsToRemove: []Snapshot{{Key: "docs/node/one.snapshot"}}}
	if err := plan.Apply(ctx, client); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(client.ObjectKeysToDelete) != 0 {
		t.Fatal("deleted after cancellation")
	}
}

func TestSnapshotKeysRequireNode(t *testing.T) {
	for _, key := range []string{"docs/file.snapshot", "docs//file.snapshot", "docs/./file.snapshot", "docs/../file.snapshot", "docs/node-0/.snapshot", "/node-0/file.snapshot", "docs/node-0/nested/file.snapshot"} {
		t.Run(key, func(t *testing.T) {
			if snapshot, ok := ToSnapshot(storage.Object{Key: key}); ok {
				t.Fatalf("invalid snapshot accepted: %+v", snapshot)
			}
		})
	}
}
func TestRetentionIgnoresKeysWithoutNode(t *testing.T) {
	client := &fakeStorageClient{ListObjectsOutput: []storage.Object{
		{Key: "docs/no-node.snapshot"},
		{Key: "docs/node-0/valid.snapshot"},
	}}
	plan, err := BuildPlan(context.Background(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SnapshotsToRemove) != 1 || plan.SnapshotsToRemove[0].Key != "docs/node-0/valid.snapshot" {
		t.Fatalf("plan=%+v", plan)
	}
	if err := plan.Apply(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if len(client.ObjectKeysToDelete) != 1 || client.ObjectKeysToDelete[0] != "docs/node-0/valid.snapshot" {
		t.Fatalf("deleted=%v", client.ObjectKeysToDelete)
	}
}
