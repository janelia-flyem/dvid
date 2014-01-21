// +build !levigo,!hyperleveldb,!goleveldb

package storage

import (
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	Version = "Stub Storage Engine"

	Driver = "github.com/janelia-flyem/dvid/storage/engine_stub.go"
)

// --- The EngineStub Implementation must satisfy a Engine interface ----

// EngineStub is a placeholder that fulfills storage.Engine and storage.KeyValueDB
// interfaces.
type EngineStub struct {
	dvid.Config
}

func NewStore(path string, create bool, config dvid.Config) (Engine, error) {
	db := &EngineStub{config}
	return db, nil
}

// ---- Engine interface ----

func (db *EngineStub) IsBatcher() bool    { return false }
func (db *EngineStub) IsBulkIniter() bool { return false }
func (db *EngineStub) IsBulkWriter() bool { return false }

func (db *EngineStub) GetConfig() dvid.Config {
	return db.Config
}

// ---- KeyValueDB interface -----

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *EngineStub) Close() {}

// Get returns a value given a key.
func (db *EngineStub) Get(k Key) (v []byte, err error) {
	return
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.
func (db *EngineStub) GetRange(kStart, kEnd Key) (values []KeyValue, err error) {
	return
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.
func (db *EngineStub) KeysInRange(kStart, kEnd Key) (keys []Key, err error) {
	return
}

// ProcessRange sends a range of key-value pairs to chunk handlers.
func (db *EngineStub) ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) error {
	return nil
}

// Put writes a value with given key.
func (db *EngineStub) Put(k Key, v []byte) error {
	return nil
}

// PutRange puts key/value pairs that have been sorted in sequential key order.
// Current implementation in levigo driver simply does a batch write.
func (db *EngineStub) PutRange(values []KeyValue) error {
	return nil
}

// Delete removes a value with given key.
func (db *EngineStub) Delete(k Key) (err error) {
	return
}
