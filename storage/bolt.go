// +build bolt

package storage

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	Version = "Bolt"

	Driver = "github.com/janelia-flyem/dvid/storage/bolt.go"
)

type BoltDB struct {
	// Directory of datastore
	directory string

	// Config at time of Open()
	config dvid.Config

	options *boltOptions
	db      *bolt.DB
}

type boltOptions struct {
}

func GetOptions(create bool, config dvid.Config) (*boltOptions, error) {
	return &boltOptions{}, nil
}

// NewStore returns a bolt backend.
func NewStore(path string, create bool, config dvid.Config) (Engine, error) {
	opt, err := GetOptions(create, config)
	if err != nil {
		return nil, err
	}

	db, err := bolt.Open(path, 0666)
	if err != nil {
		return nil, err
	}
	boltdb := &BoltDB{
		directory: path,
		config:    config,
		options:   opt,
		db:        db,
	}

	// Create buckets for each key type.
	db.Update(func(tx *bolt.Tx) error {
		if err := tx.CreateBucket(KeyDatasets.String()); err != nil {
			return err
		}
		if err := tx.CreateBucket(KeyDataset.String()); err != nil {
			return err
		}
		if err := tx.CreateBucket(KeyData.String()); err != nil {
			return err
		}
		if err := tx.CreateBucket(KeySync.String()); err != nil {
			return err
		}
		return nil
	})

	return boltdb, nil
}

// RepairStore tries to repair a damaged database
func RepairStore(path string, config dvid.Config) error {
	return fmt.Errorf("The Bolt database should not require repairs.")
}

// ---- Engine interface ----

func (bdb *BoltDB) GetName() string {
	return "bolt Go database"
}

func (bdb *BoltDB) GetConfig() dvid.Config {
	return bdb.config
}

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (bdb *BoltDB) Close() {
	if bdb.db != nil {
		bdb.db.Close()
	}
}

// ---- KeyValueGetter interface ------

// Get returns a value given a key.
func (bdb *BoltDB) Get(k Key) (v []byte, err error) {
	err = bdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", k.KeyType().String())
		}
		v = bucket.Get(k.Bytes())
		StoreValueBytesRead <- len(v)
		return nil
	})
	return
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  It is assumed that all keys are
// within one bucket.
func (bdb *BoltDB) GetRange(kStart, kEnd Key) (values []KeyValue, err error) {
	values = []KeyValue{}
	err = bdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kStart.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", kStart.KeyType().String())
		}

		endBytes := kEnd.Bytes()
		c := bucket.Cursor()
		k, v := c.Seek(kStart.Bytes())
		for {
			StoreKeyBytesRead <- len(k)
			StoreValueBytesRead <- len(v)
			if k == nil || bytes.Compare(k, endBytes) > 0 {
				return nil
			}
			// Convert byte representation of key to storage.Key
			var key Key
			key, err = kStart.BytesToKey(k)
			if err != nil {
				return err
			}
			values = append(values, KeyValue{key, v})
			k, v = c.Next()
		}
		return nil
	})
	return
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).
// For bolt database, values are read but not returned.
func (bdb *BoltDB) KeysInRange(kStart, kEnd Key) (keys []Key, err error) {
	keys = []Key{}
	err = bdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kStart.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", kStart.KeyType().String())
		}

		endBytes := kEnd.Bytes()
		c := bucket.Cursor()
		k, v := c.Seek(kStart.Bytes())
		for {
			StoreKeyBytesRead <- len(k)
			StoreValueBytesRead <- len(v)
			if k == nil || bytes.Compare(k, endBytes) > 0 {
				return nil
			}
			// Convert byte representation of key to storage.Key
			var key Key
			key, err = kStart.BytesToKey(k)
			if err != nil {
				return err
			}
			keys = append(keys, key)
			k, v = c.Next()
		}
		return nil
	})
	return
}

// ProcessRange sends a range of key-value pairs to chunk handlers.
func (bdb *BoltDB) ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) error {
	return bdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kStart.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", kStart.KeyType().String())
		}

		endBytes := kEnd.Bytes()
		c := bucket.Cursor()
		k, v := c.Seek(kStart.Bytes())
		for {
			StoreKeyBytesRead <- len(k)
			StoreValueBytesRead <- len(v)
			if k == nil || bytes.Compare(k, endBytes) > 0 {
				return nil
			}
			// Convert byte representation of key to storage.Key
			key, err := kStart.BytesToKey(k)
			if err != nil {
				return err
			}
			if op.Wg != nil {
				op.Wg.Add(1)
			}
			chunk := &Chunk{
				op,
				KeyValue{key, v},
			}
			f(chunk)
			k, v = c.Next()
		}
		return nil
	})
}

// ---- KeyValueSetter interface ------

// Put writes a value with given key.
func (bdb *BoltDB) Put(k Key, v []byte) error {
	kBytes := k.Bytes()
	return bdb.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", k.KeyType().String())
		}
		if err := bucket.Put(kBytes, v); err != nil {
			return err
		}
		StoreKeyBytesWritten <- len(kBytes)
		StoreValueBytesWritten <- len(v)
		return nil
	})
}

// PutRange puts key/value pairs that have been sorted in sequential key order.
// Current implementation in levigo driver simply does a batch write.
func (bdb *BoltDB) PutRange(values []KeyValue) error {
	return bdb.db.Update(func(tx *bolt.Tx) error {
		if values == nil || len(values) == 0 {
			return nil
		}
		bucket := tx.Bucket(values[0].K.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", values[0].K.KeyType().String())
		}
		for _, kv := range values {
			kBytes := kv.K.Bytes()
			if err := bucket.Put(kBytes, kv.V); err != nil {
				return err
			}
			StoreKeyBytesWritten <- len(kBytes)
			StoreValueBytesWritten <- len(kv.V)
		}
		return nil
	})
}

// Delete removes a value with given key.
func (bdb *BoltDB) Delete(k Key) error {
	kBytes := k.Bytes()
	return bdb.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.KeyType().String())
		if bucket == nil {
			return fmt.Errorf("Bucket '%s' does not exist.", k.KeyType().String())
		}
		if err := bucket.Delete(kBytes); err != nil {
			return err
		}
		return nil
	})
}

// --- Batcher interface ----

// Use goroutine and channels to handle transaction within a closure.

type batch struct {
	db     *bolt.DB
	tx     *bolt.Tx
	bucket *bolt.Bucket
	ch     chan batchOp
}

type batchOp struct {
	op Op
	kv KeyValue
}

// NewBatch returns an implementation that allows batch writes.  Note that the bolt
// implementation spawns a goroutine that will live until a Commit() is issued.
func (bdb *BoltDB) NewBatch() Batch {
	b := new(batch)
	b.db = bdb.db
	b.ch = make(chan batchOp)

	go func() {
		tx, err := b.db.Begin(true)
		if err != nil {
			dvid.Error("Error on beginning bolt db batch write: %s", err.Error())
			return
		}
		for {
			curOp := <-b.ch

			// If this is the first op, determine the Bucket based on the key type.
			if b.bucket == nil {
				if curOp.kv.K == nil {
					dvid.Error("Nil key sent to bolt db batch routine!")
					return
				}
				bucketName := curOp.kv.K.KeyType().String()
				b.bucket = tx.Bucket(bucketName)
			}

			// Do the operation.
			switch curOp.op {
			case PutOp:
				kBytes := curOp.kv.K.Bytes()
				if err := b.bucket.Put(kBytes, curOp.kv.V); err != nil {
					dvid.Error("Error in bolt db batch Put: %s", err.Error())
					return
				}
				StoreKeyBytesWritten <- len(kBytes)
				StoreValueBytesWritten <- len(curOp.kv.V)
			case DeleteOp:
				kBytes := curOp.kv.K.Bytes()
				if err := b.bucket.Delete(kBytes); err != nil {
					dvid.Error("Error in bolt db batch Delete: %s", err.Error())
					return
				}
			case CommitOp:
				tx.Commit()
				return
			default:
				dvid.Error("Unknown batch op %d", curOp.op)
				return
			}
		}
	}()

	return b
}

// --- Batch interface ---

func (b *batch) Delete(k Key) {
	if b != nil && b.ch != nil {
		b.ch <- batchOp{DeleteOp, KeyValue{k, nil}}
	}
}

func (b *batch) Put(k Key, v []byte) {
	if b != nil && b.ch != nil {
		b.ch <- batchOp{PutOp, KeyValue{k, v}}
	}
}

func (b *batch) Commit() error {
	if b != nil && b.ch != nil {
		b.ch <- batchOp{CommitOp, KeyValue{}}
	}
	return nil
}
