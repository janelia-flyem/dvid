// +build lmdb

package local

import (
	"bytes"
	"fmt"
	"os"

	"github.com/janelia-flyem/dvid/dvid"

	lmdb "github.com/DocSavage/gomdb"
)

const (
	Version = "Lightning MDB (static Cgo)"

	Driver = "github.com/DocSavage/gomdb"
)

type LMDB struct {
	// Path to datastore
	path string

	// Config at time of Open()
	config dvid.Config

	options *lmdbOptions
	env     *lmdb.Env
	dbi     lmdb.DBI
}

type lmdbOptions struct {
	GBytes int
}

func GetOptions(create bool, config dvid.Config) (*lmdbOptions, error) {
	sizeInGB, found, err := config.GetInt("Size")
	if err != nil {
		return nil, err
	}
	if !found && create {
		return nil, fmt.Errorf("Must specify 'Size=...' in gigabytes during Lightning MDB creation.")
	}
	if found {
		return &lmdbOptions{GBytes: sizeInGB}, nil
	}
	return &lmdbOptions{}, nil
}

// NewKeyValueStore returns a lmdb backend.
func NewKeyValueStore(path string, create bool, config dvid.Config) (Engine, error) {
	// Create the directory if it doesn't exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0770); err != nil {
			return nil, fmt.Errorf("Datastore (%s) doesn't exist and couldn't be created: %s", err.Error())
		}
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt, err := GetOptions(create, config)
	if err != nil {
		return nil, err
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	if err = env.SetMapSize(uint64(opt.GBytes * dvid.Giga)); err != nil {
		return nil, err
	}

	if err = env.Open(path, lmdb.NOSYNC|lmdb.WRITEMAP, 0664); err != nil {
		return nil, err
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		return nil, fmt.Errorf("Cannot begin transaction: %s", err.Error())
	}

	dbi, err := txn.DBIOpen(nil, 0)
	if err != nil {
		return nil, fmt.Errorf("Cannot create DBI: %s", err.Error())
	}

	db := &LMDB{
		path:    path,
		config:  config,
		options: opt,
		env:     env,
		dbi:     dbi,
	}
	txn.Abort()

	return db, nil
}

// RepairStore tries to repair a damaged database
func RepairStore(path string, config dvid.Config) error {
	return fmt.Errorf("The lightning mdb database should not require repairs.")
}

// ---- Engine interface ----

func (db *LMDB) String() string {
	return "lmdb Cgo database"
}

func (db *LMDB) GetConfig() dvid.Config {
	return db.config
}

// Close closes the db.
func (db *LMDB) Close() {
	if db.env != nil {
		db.env.Close()
	}
}

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (db *LMDB) Get(k Key) ([]byte, error) {
	if db == nil || db.env == nil {
		return nil, fmt.Errorf("Cannot Get() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, lmdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer txn.Abort()

	value, err := txn.Get(db.dbi, k.Bytes())
	if err != nil {
		return nil, err
	}
	return value, nil
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  It is assumed that all keys are
// within one bucket.
func (db *LMDB) GetRange(kStart, kEnd Key) ([]KeyValue, error) {
	if db == nil || db.env == nil {
		return nil, fmt.Errorf("Cannot GetRange() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, lmdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer txn.Abort()
	cursor, err := txn.CursorOpen(db.dbi)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	seekKey := kStart.Bytes()
	endBytes := kEnd.Bytes()
	values := []KeyValue{}
	var cursorOp uint = lmdb.SET_RANGE
	for {
		k, v, rc := cursor.Get(seekKey, cursorOp)
		if rc != nil {
			break
		}
		seekKey = nil
		cursorOp = lmdb.NEXT
		StoreKeyBytesRead <- len(k)
		StoreValueBytesRead <- len(v)
		if k == nil || bytes.Compare(k, endBytes) > 0 {
			break
		}
		// Convert byte representation of key to storage.Key
		var key Key
		key, err = kStart.BytesToKey(k)
		if err != nil {
			return nil, err
		}
		values = append(values, KeyValue{key, v})
	}
	return values, nil
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).
// For lmdb database, values are read but not returned.
func (db *LMDB) KeysInRange(kStart, kEnd Key) ([]Key, error) {
	if db == nil || db.env == nil {
		return nil, fmt.Errorf("Cannot run KeysInRange() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, lmdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer txn.Abort()
	cursor, err := txn.CursorOpen(db.dbi)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	seekKey := kStart.Bytes()
	endBytes := kEnd.Bytes()
	keys := []Key{}
	var cursorOp uint = lmdb.SET_RANGE
	for {
		k, v, rc := cursor.Get(seekKey, cursorOp)
		if rc != nil {
			break
		}
		seekKey = nil
		cursorOp = lmdb.NEXT
		StoreKeyBytesRead <- len(k)
		StoreValueBytesRead <- len(v)
		if k == nil || bytes.Compare(k, endBytes) > 0 {
			break
		}
		// Convert byte representation of key to storage.Key
		var key Key
		key, err = kStart.BytesToKey(k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// ProcessRange sends a range of key-value pairs to chunk handlers.
func (db *LMDB) ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) error {
	if db == nil || db.env == nil {
		return fmt.Errorf("Cannot ProcessRange() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, lmdb.RDONLY)
	if err != nil {
		return err
	}
	defer txn.Abort()
	cursor, err := txn.CursorOpen(db.dbi)
	if err != nil {
		return err
	}
	defer cursor.Close()

	seekKey := kStart.Bytes()
	endBytes := kEnd.Bytes()
	var cursorOp uint = lmdb.SET_RANGE
	for {
		k, v, rc := cursor.Get(seekKey, cursorOp)
		if rc != nil {
			break
		}
		seekKey = nil
		cursorOp = lmdb.NEXT
		StoreKeyBytesRead <- len(k)
		StoreValueBytesRead <- len(v)
		if k == nil || bytes.Compare(k, endBytes) > 0 {
			break
		}
		// Convert byte representation of key to storage.Key
		var key Key
		key, err = kStart.BytesToKey(k)
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
	}
	return nil
}

// ---- OrderedKeyValueSetter interface ------

// Put writes a value with given key.
func (db *LMDB) Put(k Key, v []byte) error {
	if db == nil || db.env == nil {
		return fmt.Errorf("Cannot Put() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer txn.Commit()
	kBytes := k.Bytes()
	if v == nil || len(v) == 0 {
		v = []byte{0}
	}
	if err := txn.Put(db.dbi, kBytes, v, 0); err != nil {
		return err
	}
	StoreKeyBytesWritten <- len(kBytes)
	StoreValueBytesWritten <- len(v)
	return nil
}

// PutRange puts key-value pairs that have been sorted in sequential key order.
func (db *LMDB) PutRange(values []KeyValue) error {
	if db == nil || db.env == nil {
		return fmt.Errorf("Cannot run PutRange() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer txn.Commit()

	for _, kv := range values {
		kBytes := kv.K.Bytes()
		v := kv.V
		if v == nil || len(v) == 0 {
			v = []byte{0}
		}
		if err := txn.Put(db.dbi, kBytes, v, 0); err != nil {
			return err
		}
		StoreKeyBytesRead <- len(kBytes)
		StoreValueBytesRead <- len(v)
	}
	return nil
}

// Delete removes a value with given key.
// If the key does not exist, it returns without error.
func (db *LMDB) Delete(k Key) error {
	if db == nil || db.env == nil {
		return fmt.Errorf("Cannot GetRange() on invalid database.")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer txn.Commit()
	return txn.Del(db.dbi, k.Bytes(), nil)
}

// --- Batcher interface ----

// Use goroutine and channels to handle transaction within a closure.

type batch struct {
	env *lmdb.Env
	txn *lmdb.Txn
	dbi lmdb.DBI
}

// NewBatch returns an implementation that allows batch writes.  This lmdb implementation
// uses a transaction for the batch.
func (db *LMDB) NewBatch() Batch {
	if db == nil || db.env == nil {
		dvid.Error("Cannot do NewBatch() of lmdb with nil database")
		return nil
	}
	b := new(batch)
	b.env = db.env
	b.dbi = db.dbi

	dvid.StartCgo()
	defer dvid.StopCgo()

	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		dvid.Error("Error in BeginTxn() for NewBatch() of lmdb")
		return nil
	}
	b.txn = txn

	return b
}

// --- Batch interface ---

func (b *batch) Delete(k Key) {
	if b != nil {
		dvid.StartCgo()
		defer dvid.StopCgo()
		if err := b.txn.Del(b.dbi, k.Bytes(), nil); err != nil {
			dvid.Error("Error in batch Delete: %s", err.Error())
		}
	}
}

func (b *batch) Put(k Key, v []byte) {
	if b != nil {
		dvid.StartCgo()
		defer dvid.StopCgo()
		kBytes := k.Bytes()
		if v == nil || len(v) == 0 {
			v = []byte{0}
		}
		if err := b.txn.Put(b.dbi, kBytes, v, 0); err != nil {
			dvid.Error("Error in batch Put: %s", err.Error())
			return
		}
		StoreKeyBytesWritten <- len(kBytes)
		StoreValueBytesWritten <- len(v)
	}
}

func (b *batch) Commit() error {
	if b == nil {
		return fmt.Errorf("Illegal Commit() on a nil batch")
	}
	dvid.StartCgo()
	defer dvid.StopCgo()
	return b.txn.Commit()
}
