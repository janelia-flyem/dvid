// +build gcloud

// This is a placeholder and not ready for use.
// TODO: Either finish or move to Google Bigtable service.

package storage

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/janelia-flyem/dvid/dvid"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

const (
	Version = "Google AppEngine"

	Driver = "github.com/janelia-flyem/dvid/storage/appengine.go"
)

type Entity struct {
	Value []byte
}

// --- datastore.PropertyLoadSaver interface -----

func (v *ValueGAE) Load(c <-chan datastore.Property) error {
	if err := datastore.LoadStruct(v, c); err != nil {
		return err
	}
	StoreValueBytesRead <- len(v.Value)
	return nil
}

func (v *ValueGAE) Save(c chan<- Property) error {
	defer close(c)
	return datastore.SaveStruct(v, c)
}

// GAEContext provides information necessary to compose proper keys for versioning
// and cloud datastores.
type GAEContext struct {
	ancestors  []DataAncestors
	gaeContext appengine.Context
}

func NewStorageContext(r *http.Request, ancestors []DataAncestors) *Context {
	c := appengine.NewServerContext(r)
	return &GAEContext{ancestors, c}
}

// ---- Context inteface ------

func (c *GAEContext) Depth() int {
	return len(c.ancestors)
}

func (c *GAEContext) Ancestor(depth int) *DataAncestor {
	if depth >= len(c.ancestors) {
		return nil
	}
	return c.ancestors[depth]
}

// -------------

type AppEngineDatastore struct {
	// Config at time of Open()
	config dvid.Config
}

// NewAppEngineDatastore returns a Google datastore backend
func NewAppEngineDatastore(config dvid.Config) (Engine, error) {
	return &AppEngineDatastore{config}, nil
}

// ---- Engine interface ----

func (ds *AppEngineDatastore) String() string {
	return "Google AppEngine datastore"
}

// GetConfig returns configuration data at time of datastore initialization.
func (ds *AppEngineDatastore) GetConfig() dvid.Config {
	return ds.config
}

// Close is a null op for Google datastore service.
func (ds *AppEngineDatastore) Close() {}

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (ds *GAEContext) Get(k Key) ([]byte, error) {
	if ds == nil {
		return nil, fmt.Errorf("Cannot Get() on invalid database.")
	}
	e := new(Entity)
	parent := nil
	aek := datastore.NewKey(ctx, "Entity", k.BytesString(), 0, parent)
	if err := datastore.Get(ctx, aek, e); err != nil {
		return nil, err
	}
	return e.Value, nil
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  It is assumed that all keys are
// within one bucket.
func (ds *GAEContext) GetRange(kStart, kEnd Key) ([]KeyValue, error) {
	if ds == nil {
		return nil, fmt.Errorf("Cannot Get() on invalid database.")
	}

	seekKey := kStart.Bytes()
	endBytes := kEnd.Bytes()
	values := []KeyValue{}
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
func (ds *GAEContext) KeysInRange(kStart, kEnd Key) ([]Key, error) {
	if ds == nil {
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
func (ds *GAEContext) ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) error {
	if ds == nil {
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
func (ds *GAEContext) Put(k Key, v []byte) error {
	if ds == nil {
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
func (ds *GAEContext) PutRange(values []KeyValue) error {
	if ds == nil {
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
func (ds *GAEContext) Delete(k Key) error {
	if ds == nil {
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
func (ds *GAEContext) NewBatch() Batch {
	if ds == nil {
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
			dvid.Error("Error in batch Delete: %v", err)
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
			dvid.Error("Error in batch Put: %v", err)
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
