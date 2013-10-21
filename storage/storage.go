/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces and the Engine interface provides a way to query
	which interfaces are implemented by a given storage engine.

	Each storage engine must implement the following:

		NewEngine(path string, create bool, options Options) (Engine, error)
*/
package storage

import (
	"log"
	"sync"
)

// ChunkOp is a type-specific operation with an optional WaitGroup to
// sync mapping before reduce.
type ChunkOp struct {
	Op interface{}
	Wg *sync.WaitGroup
}

// Chunk is the unit passed down channels to chunk handlers.
type Chunk struct {
	*ChunkOp
	KeyValue
}

// Options encapsulates settings passed in as well as database-specific environments
// created as a result of the settings.  Each implementation creates methods on this
// type to support options.
type Options struct {
	// Settings provides values for database-specific options.
	Settings map[string]interface{}

	// Database-specific implementation of options
	options interface{}
}

// IntSetting returns an int or sets ok to false if it can't cast the value or
// the setting key doesn't exist.
func (opt *Options) IntSetting(key string) (setting int, ok bool) {
	if opt == nil {
		ok = false
		return
	}
	var value interface{}
	value, ok = opt.Settings[key]
	if !ok {
		return
	}
	setting, ok = value.(int)
	if !ok {
		log.Fatalf("Could not cast to int: Setting[%s] = %s\n", key, value)
	}
	return
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
}

// Engine implementations can fulfill a variety of interfaces.  Other parts
// of DVID, most notably the data type implementations, need to know what's available.
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type Engine interface {
	KeyValueDB

	IsBatcher() bool
	IsBulkIniter() bool
	IsBulkWriter() bool

	GetOptions() *Options
}

// KeyValueDB provides an interface to the simplest storage API: a key/value store.
type KeyValueDB interface {
	// Closes datastore.
	Close()

	// Get returns a value given a key.
	Get(k Key) (v []byte, err error)

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(kStart, kEnd Key) (values []KeyValue, err error)

	// ProcessRange sends a range of key/value pairs to type-specific chunk handlers.
	ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) (err error)

	// Put writes a value with given key.
	Put(k Key, v []byte) error

	// Put key-value pairs that have already been sorted in sequential key order.
	PutRange(values []KeyValue) error

	// Delete removes an entry given key.
	Delete(k Key) error
}

// Batchers allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type Batcher interface {
	NewBatch() Batch
}

// Batch groups operations into a transaction.
type Batch interface {
	// Commits a batch of operations.
	Commit() error

	// Delete removes from the batch a put using the given key.
	Delete(k Key)

	// Put adds to the batch a put using the given key/value.
	Put(k Key, v []byte)

	// Clear clears the contents of a write batch
	Clear()

	// Close closes a write batch
	Close()
}

// BulkIniters can employ even more aggressive optimization in loading large
// data since they can assume an uninitialized blank database.
type BulkIniter interface {
}

// BulkWriter employ some sort of optimization to efficiently write large
// amount of data.  For some key-value databases, this requires keys to
// be presorted.
type BulkWriter interface {
}
