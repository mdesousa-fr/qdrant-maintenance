package storage

import "context"

type Client interface {
	ListObjects(ctx context.Context) ([]Object, error)
	DeleteObject(ctx context.Context, key string) error
}
