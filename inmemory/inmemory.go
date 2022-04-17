package inmemory

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/wailorman/ukvs"
)

var errExpired = errors.New("Container expired")
var errInvalidStoredValue = errors.New("Invalid stored value")

// Client _
type Client struct {
	storage sync.Map
	ctx     context.Context
	lock    sync.Mutex
	closed  chan struct{}
}

// Container _
type Container struct {
	Val       []byte
	ExpiresAt *time.Time
}

// NewClient _
func NewClient(ctx context.Context) *Client {
	c := &Client{
		storage: sync.Map{},
		ctx:     ctx,
		lock:    sync.Mutex{},
		closed:  make(chan struct{}),
	}

	go func() {
		<-c.ctx.Done()
		close(c.closed)
		return
	}()

	return c
}

func (c *Client) extractContainer(key string, result interface{}) ([]byte, error) {
	cont, ok := result.(Container)

	if !ok {
		return nil, errInvalidStoredValue
	}

	if cont.ExpiresAt != nil && time.Now().After(*cont.ExpiresAt) {
		c.Destroy(key)
		return nil, errExpired
	}

	return cont.Val, nil
}

// Get _
func (c *Client) Get(key string) ([]byte, error) {
	result, ok := c.storage.Load(key)

	if !ok || result == nil {
		return nil, ukvs.ErrNotFound
	}

	val, err := c.extractContainer(key, result)

	if err != nil && (errors.Is(err, errExpired) || errors.Is(err, errInvalidStoredValue)) {
		return nil, ukvs.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	return val, nil
}

// Set _
func (c *Client) Set(key string, val []byte) error {
	cont := Container{
		Val:       val,
		ExpiresAt: nil,
	}

	c.storage.Store(key, cont)
	return nil
}

// GetAll _
func (c *Client) GetAll(fctx context.Context) (chan []byte, chan error) {
	return c.FindAll(fctx, "*")
}

// FindAll _
func (c *Client) FindAll(fctx context.Context, pattern string) (chan []byte, chan error) {
	results := make(chan []byte, 1)
	failures := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(failures)

		c.storage.Range(func(key, value interface{}) bool {
			skey, ok := key.(string)

			if !ok {
				// TODO: log invalid key type
				return true
			}

			keyMatches, err := filepath.Match(pattern, skey)

			if err != nil {
				failures <- err
				return false
			}

			if !keyMatches {
				return true
			}

			bval, err := c.extractContainer(skey, value)

			if errors.Is(err, errExpired) || errors.Is(err, errInvalidStoredValue) {
				// TODO: log invalid stored value type
				return true
			}

			if err != nil {
				failures <- err
				return false
			}

			select {
			case <-c.ctx.Done():
				return false
			case <-fctx.Done():
				return false
			case results <- bval:
				return true
			}
		})

		failures <- nil
	}()

	return results, failures
}

// Destroy _
func (c *Client) Destroy(key string) error {
	c.storage.Delete(key)
	return nil
}

// ExpireAt _
func (c *Client) ExpireAt(key string, t time.Time) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	result, ok := c.storage.Load(key)

	if !ok || result == nil {
		return ukvs.ErrNotFound
	}

	cont, ok := result.(Container)

	if !ok {
		return ukvs.ErrNotFound
	}

	if cont.ExpiresAt != nil && time.Now().After(*cont.ExpiresAt) {
		c.Destroy(key)
		return ukvs.ErrNotFound
	}

	if time.Now().After(t) {
		c.Destroy(key)
		return nil
	}

	cont.ExpiresAt = &t

	c.storage.Store(key, cont)
	return nil
}

// Closed _
func (c *Client) Closed() <-chan struct{} {
	return c.closed
}

// Persist _
func (c *Client) Persist() error {
	return nil
}
