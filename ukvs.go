package ukvs

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound _
var ErrNotFound = errors.New("Not found")

// IStore _
type IStore interface {
	Get(key string) ([]byte, error)
	// TODO: always ask expiration
	Set(key string, val []byte) error
	GetAll(ctx context.Context) (results chan []byte, failures chan error)
	FindAll(ctx context.Context, pattern string) (results chan []byte, failures chan error)
	Destroy(key string) error
	ExpireAt(key string, time time.Time) error
	Persist() error
	Closed() <-chan struct{}
}
