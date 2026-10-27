package runs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/storage"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/telemetry"
)

// VerifyObject hashes the streamed object against an independent expected digest.
func VerifyObject(ctx context.Context, store storage.Repository, key string, size int64, digest string) (err error) {
	ctx, finish := telemetry.Operation(ctx, "snapshot.verify")
	defer func() { finish(err) }()
	expected, err := hex.DecodeString(digest)
	if err != nil || len(expected) != sha256.Size || size < 0 {
		return fmt.Errorf("snapshot %s has no valid SHA-256 or size", key)
	}
	body, _, err := store.GetObject(ctx, key)
	if err != nil {
		return err
	}
	hash := sha256.New()
	count, copyErr := io.Copy(hash, body)
	closeErr := body.Close()
	if err := errors.Join(copyErr, closeErr, ctx.Err()); err != nil {
		return err
	}
	if count != size || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), digest) {
		return fmt.Errorf("snapshot %s content hash or size mismatch", key)
	}
	return nil
}
func (r Repository) Verify(ctx context.Context, id, collection, node string, takeover bool, logger *slog.Logger) (count int, err error) {
	record, err := r.Load(ctx, id)
	if err != nil {
		return 0, err
	}
	if err := recoverable(record.Manifest); err != nil {
		return 0, err
	}
	logger = logger.With("run_id", id)
	if err := r.Claim(ctx, &record, takeover); err != nil {
		return 0, err
	}
	save := func() error {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return r.Save(saveCtx, &record)
	}
	defer func() {
		record.Manifest.Writer = ""
		if saveErr := save(); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
	}()
	var errs []error
	for i := range record.Manifest.Entries {
		entry := &record.Manifest.Entries[i]
		if collection != "" && (entry.Collection != collection || entry.Node != node) {
			continue
		}
		if ctx.Err() != nil {
			return count, errors.Join(append(errs, ctx.Err())...)
		}
		count++
		entry.VerifyError = "verification interrupted"
		entry.VerifiedAt = time.Time{}
		if err := save(); err != nil {
			return count, errors.Join(append(errs, err)...)
		}
		copy := *entry
		copy.VerifyError = ""
		verifyErr := CheckObject(ctx, r.Store, copy)
		if verifyErr == nil {
			verifyErr = VerifyObject(ctx, r.Store, entry.Key, entry.Size, entry.SHA256)
		}
		entry.VerifiedAt = time.Now().UTC()
		entry.VerifyError = ""
		if verifyErr != nil {
			entry.VerifyError = verifyErr.Error()
			errs = append(errs, verifyErr)
			logger.ErrorContext(ctx, "snapshot content verification failed", "event", "snapshot.verification_failed", "key", entry.Key, "error", verifyErr)
		} else {
			logger.InfoContext(ctx, "snapshot content verified", "event", "snapshot.verified", "key", entry.Key, "size", entry.Size)
		}
		if err := save(); err != nil {
			return count, errors.Join(append(errs, err)...)
		}
	}
	return count, errors.Join(errs...)
}
