// Package backup coordinates safe, node-local snapshot transfers.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Qdrant interface {
	GetCluster(context.Context) (qdrant.Cluster, error)
	GetCollections(context.Context) ([]qdrant.Collection, error)
	CreateSnapshot(context.Context, string) (qdrant.Snapshot, error)
	DownloadSnapshot(context.Context, string, string, io.Writer) error
	DeleteSnapshot(context.Context, string, string) error
}

// Upload must return success only after verifying the committed object's size
// and SHA-256 metadata. The file is seekable for retries and multipart uploads.
type Storage interface {
	Upload(context.Context, string, *os.File, int64, string) error
}
type Result struct {
	Collection    string `json:"collection"`
	Node          string `json:"node"`
	Key           string `json:"key"`
	Size          int64  `json:"size"`
	SHA256        string `json:"sha256"`
	SourceDeleted bool   `json:"source_deleted"`
	Stored        bool   `json:"stored"`
	Error         string `json:"error,omitempty"`
}

func Run(ctx context.Context, logger *slog.Logger, q Qdrant, store Storage, collection, node string, keepSource bool) (result Result, err error) {
	ctx, finish := telemetry.Operation(ctx, "backup")
	defer func() {
		if err != nil {
			result.Error = err.Error()
		}
		finish(err)
	}()
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("collection", collection), attribute.String("node", node))
	result.Collection = collection
	result.Node = node
	if !config.ValidSegment(collection) || !config.ValidSegment(node) {
		return result, fmt.Errorf("invalid collection or node ID")
	}
	snapshot, err := q.CreateSnapshot(ctx, collection)
	if err != nil {
		return result, fmt.Errorf("create snapshot: %w", err)
	}
	if !config.ValidSegment(snapshot.Name) || !strings.HasSuffix(snapshot.Name, ".snapshot") || snapshot.Name == ".snapshot" {
		return result, fmt.Errorf("invalid snapshot name returned by Qdrant")
	}
	result.Key = collection + "/" + node + "/" + snapshot.Name
	logger.InfoContext(ctx, "snapshot created", "event", "snapshot.created", "collection", collection, "node", node, "key", result.Key)
	file, err := os.CreateTemp("", "qdrant-snapshot-*")
	if err != nil {
		return result, fmt.Errorf("create temporary file: %w", err)
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	hash := sha256.New()
	if err = q.DownloadSnapshot(ctx, collection, snapshot.Name, io.MultiWriter(file, hash)); err != nil {
		return result, fmt.Errorf("download snapshot (source retained): %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return result, err
	}
	result.Size = info.Size()
	result.SHA256 = hex.EncodeToString(hash.Sum(nil))
	if uint64(result.Size) != snapshot.Size {
		return result, fmt.Errorf("snapshot size mismatch: got %d, expected %d (source retained)", result.Size, snapshot.Size)
	}
	if snapshot.Checksum != "" && !strings.EqualFold(snapshot.Checksum, result.SHA256) {
		return result, fmt.Errorf("snapshot checksum mismatch (source retained)")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	if err = store.Upload(ctx, result.Key, file, result.Size, result.SHA256); err != nil {
		return result, fmt.Errorf("upload/verify snapshot (source retained): %w", err)
	}
	result.Stored = true
	telemetry.Bytes(ctx, result.Size)
	logger.InfoContext(ctx, "snapshot uploaded and verified", "event", "snapshot.stored", "key", result.Key, "size", result.Size)
	if !keepSource {
		if err = q.DeleteSnapshot(ctx, collection, snapshot.Name); err != nil {
			return result, fmt.Errorf("backup stored at %s but source cleanup failed: %w", result.Key, err)
		}
		result.SourceDeleted = true
	}
	return result, nil
}
