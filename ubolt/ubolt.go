package ubolt

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/wailorman/chwg"
	"github.com/wailorman/ukvs"
)

// BucketName _
var BucketName = []byte("v1_ukvs")

var errExpired = errors.New("Container expired")
var errEmptyValue = errors.New("Container has empty value")

// ErrTimeoutReached _
var ErrTimeoutReached = errors.New("timeout reached")

const valSendTimeout = 10 * time.Second

// Client _
type Client struct {
	storagePath string
	ctx         context.Context
	lock        sync.Mutex
	db          *bolt.DB
	wg          *chwg.ChannelledWaitGroup
}

// Container _
type Container struct {
	Val       []byte     `json:"val"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// NewClient _
func NewClient(ctx context.Context, storagePath string) (*Client, error) {
	var err error

	c := &Client{
		ctx:         ctx,
		storagePath: storagePath,
		lock:        sync.Mutex{},
		wg:          chwg.New(),
	}

	c.db, err = bolt.Open(storagePath, 0600, nil)

	if err != nil {
		return nil, errors.Wrap(err, "Initializing bolt db")
	}

	err = c.db.Update(func(tx *bolt.Tx) error {
		var err error
		_, err = tx.CreateBucketIfNotExists(BucketName)

		return err
	})

	if err != nil {
		return nil, errors.Wrap(err, "Creating bolt bucket")
	}

	c.wg.Add(1)

	go func() {
		<-ctx.Done()
		c.db.Close()
		c.wg.Done()
	}()

	return c, nil
}

// Get _
func (c *Client) Get(key string) ([]byte, error) {
	var found []byte

	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(BucketName)
		bCont := b.Get([]byte(key))

		if len(bCont) == 0 {
			return ukvs.ErrNotFound
		}

		cont, err := unmarshalContainer(bCont)

		if errors.Is(err, errEmptyValue) || errors.Is(err, errExpired) {
			return ukvs.ErrNotFound
		}

		found = cont.Val
		return nil
	})

	return found, err
}

// Set _
func (c *Client) Set(key string, val []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(BucketName)
		existing := b.Get([]byte(key))

		newValue, err := updateContainer(existing, val, nil)

		if err != nil && !errors.Is(err, errEmptyValue) {
			return err
		}

		return b.Put([]byte(key), newValue)
	})
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

		err := c.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(BucketName)
			cur := b.Cursor()

			for bKey, bCont := cur.First(); bKey != nil; bKey, bCont = cur.Next() {
				if fctx.Err() != nil || c.ctx.Err() != nil {
					return nil
				}

				keyMatches, matchErr := filepath.Match(pattern, string(bKey))

				if matchErr != nil {
					return errors.Wrap(matchErr, "Pattern error")
				}

				if !keyMatches {
					continue
				}

				cont, err := unmarshalContainer(bCont)

				if err != nil {
					continue
				}

				select {
				case <-c.ctx.Done():
					return c.ctx.Err()
				case <-fctx.Done():
					return fctx.Err()
				case results <- cont.Val:
					continue
				case <-time.After(valSendTimeout):
					return ErrTimeoutReached
				}
			}

			return nil
		})

		if err != nil {
			failures <- err
		}
	}()

	return results, failures
}

// Destroy _
func (c *Client) Destroy(key string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(BucketName)
		return b.Put([]byte(key), nil)
	})
}

// ExpireAt _
func (c *Client) ExpireAt(key string, t time.Time) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(BucketName)
		bCont := b.Get([]byte(key))

		cont, err := unmarshalContainer(bCont)

		if err != nil {
			return nil
		}

		newBCont, _ := updateContainer(bCont, cont.Val, &t)

		return b.Put([]byte(key), newBCont)
	})
}

// Closed _
func (c *Client) Closed() <-chan struct{} {
	return c.wg.Closed()
}

// Persist _
func (c *Client) Persist() error {
	return nil
}

func unmarshalContainer(bCont []byte) (*Container, error) {
	cont := &Container{}

	err := json.Unmarshal(bCont, cont)

	if err != nil {
		return nil, errEmptyValue
	}

	if cont.Val == nil || len(cont.Val) == 0 {
		return nil, errEmptyValue
	}

	if cont.ExpiresAt != nil && time.Now().After(*cont.ExpiresAt) {
		return nil, errExpired
	}

	return cont, nil
}

func updateContainer(bCont []byte, value []byte, expiresAt *time.Time) ([]byte, error) {
	cont, err := unmarshalContainer(bCont)

	if err != nil {
		cont = &Container{}
	}

	cont.Val = value

	if expiresAt != nil {
		cont.ExpiresAt = expiresAt
	}

	newBCont, err := json.Marshal(cont)

	if err != nil {
		return nil, errors.Wrap(err, "Marshaling new container")
	}

	return newBCont, nil
}
