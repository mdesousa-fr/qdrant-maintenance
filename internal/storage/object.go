package storage

import "time"

type Object struct {
	Key          string
	LastModified time.Time
	Size         int64
}
