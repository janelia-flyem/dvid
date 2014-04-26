/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces in addition to the core Engine interface, which all
	storage engines should satisfy.

	Initially we are concentrating on key-value backends but expect to support
	graph and perhaps relational databases.

	Each storage engine must implement the following package function:

	func NewStore(path string, create bool, options *Options) (Engine, error)

	Currently only one storage engine file is compiled through the use of build
	tags like "leveldb", "hyperleveldb", and "basholeveldb".
*/
package storage

import (
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

func Shutdown() {
	ShutdownFUSE()
}

// Op enumerates the types of single key-value operations that can be performed for storage engines.
type Op uint8

const (
	GetOp Op = iota
	PutOp
	DeleteOp
	CommitOp
)

// ChunkOp is a type-specific operation with an optional WaitGroup to
// sync mapping before reduce.
type ChunkOp struct {
	Op interface{}
	Wg *sync.WaitGroup
}

// Chunk is the unit passed down channels to chunk handlers.  Chunks can be passed
// from lower-level database access functions to type-specific chunk processing.
type Chunk struct {
	*ChunkOp
	KeyValue
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
}

// Engine implementations can fulfill a variety of interfaces and can be checked by
// runtime cast checks, e.g., myGetter, ok := myEngine.(OrderedKeyValueGetter)
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type Engine interface {
	GetName() string
	GetConfig() dvid.Config
	Close()
}

type KeyValueGetter interface {
	// Get returns a value given a key.
	Get(k Key) (v []byte, err error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(kStart, kEnd Key) (values []KeyValue, err error)

	// KeysInRange returns a range of keys spanning (kStart, kEnd).
	KeysInRange(kStart, kEnd Key) (keys []Key, err error)

	// ProcessRange sends a range of key/value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key/value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype.voxels.ProcessChunk() for an example.
	ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) (err error)
}

type KeyValueSetter interface {
	// Put writes a value with given key.
	Put(k Key, v []byte) error

	// Delete removes an entry given key.
	Delete(k Key) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(values []KeyValue) error
}

// KeyValueDB provides an interface to the simplest storage API: a key/value store.
type KeyValueDB interface {
	KeyValueGetter
	KeyValueSetter
}

// OrderedKeyValueDB addes range queries and range puts to a base KeyValueDB.
type OrderedKeyValueDB interface {
	OrderedKeyValueGetter
	OrderedKeyValueSetter
}

// Batchers allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type Batcher interface {
	NewBatch() Batch
}

// Batch groups operations into a transaction.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(k Key)

	// Put adds to the batch a put using the given key/value.
	Put(k Key, v []byte)

	// Commits a batch of operations and closes the write batch.
	Commit() error
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
