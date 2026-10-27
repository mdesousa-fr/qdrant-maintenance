package storage

import (
	"context"
	"errors"
	"io"
)

var ErrNotFound = errors.New("object not found")
var ErrConflict = errors.New("object changed concurrently")

type Details struct {
	Size   int64
	SHA256 string
	ETag   string
}

// Repository supports streamed reads and conditional, small metadata writes.
// An empty previousETag creates a new object; other values require If-Match.
type Repository interface {
	GetObject(ctx context.Context, key string) (io.ReadCloser, string, error)
	PutObject(ctx context.Context, key string, data []byte, previousETag string) (string, error)
	HeadObject(ctx context.Context, key string) (Details, error)
}
