/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces and the DataHandler interface provides a way to query
	which interfaces are implemented by a given storage engine.

	Each storage engine must implement the following:

		NewDataHandler(path string, create bool, options Options) (DataHandler, error)

	While Key data has a number of components so key space can be managed
	more efficiently by the storage engine, values are simply []byte at
	this level.  We assume serialization/deserialization occur above the
	storage level.
*/
package storage

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

/*
	Key holds DVID-centric data like version (UUID), data set, and index
	identifiers and that follow a convention of how to collapse those
	identifiers into a []byte key.  Ideally, we'd like to keep Key within
	the datastore package and have storage independent of DVID concepts,
	but in order to optimize the layout of data in some storage engines,
	the backend drivers need the additional DVID information.  For example,
	Couchbase allows configuration at the bucket level (RAM cache, CPUs)
	and datasets could be placed in different buckets.

	Keys have the following components:

	1) Dataset: The DVID server-specific 32-bit ID for a dataset.
	2) Data: The DVID server-specific data index that is unique per dataset.
	3) Version: The DVID server-specific version index that is
	     fewer bytes than a complete UUID and unique per dataset.
	4) Index: The datatype-specific (usually spatiotemporal) index that allows
	     partitioning of the data.  In the case of voxels, this could be a
	     (x, y, z) coordinate packed into a slice of bytes.
*/
type Key struct {
	Dataset dvid.LocalID32
	Data    dvid.LocalID
	Version dvid.LocalID
	Index   dvid.Index
}

// BytesToKey returns a Key given a slice of bytes
func (key *Key) BytesToKey(b []byte) (*Key, error) {
	dataset, start := dvid.LocalID32FromBytes(b)
	data, length := dvid.LocalIDFromBytes(b[start:])
	start += length
	version, _ := dvid.LocalIDFromBytes(b[start:])
	start += length
	index, err := key.Index.IndexFromBytes(b[start:])
	return &Key{dataset, data, version, index}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *Key) Bytes() (b []byte) {
	b = key.Dataset.Bytes()
	b = append(b, key.Data.Bytes()...)
	b = append(b, key.Version.Bytes()...)
	b = append(b, key.Index.Bytes()...)
	return
}

// Bytes returns a string derived from the concatenation of the key elements.
func (key *Key) BytesString() string {
	return string(key.Bytes())
}

// String returns a hexadecimal representation of the bytes encoding a key
// so it is readable on a terminal.
func (key *Key) String() string {
	return fmt.Sprintf("%x", key.Bytes())
}

// KeyValue stores a key-value pair.
type KeyValue struct {
	K *Key
	V []byte
}

// Deserialize returns a key-value pair where the value has been deserialized.
func (kv KeyValue) Deserialize(uncompress bool) (KeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return KeyValue{kv.K, value}, err
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K.Bytes(), kv[j].K.Bytes()) <= 0
}

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

// DataHandler implementations can fulfill a variety of interfaces.  Other parts
// of DVID, most notably the data type implementations, need to know what's available.
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type DataHandler interface {
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
	Get(k *Key) (v []byte, err error)

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(kStart, kEnd *Key) (values []KeyValue, err error)

	// ProcessRange sends a range of key/value pairs to type-specific chunk handlers.
	ProcessRange(kStart, kEnd *Key, op *ChunkOp, f func(*Chunk)) (err error)

	// Put writes a value with given key.
	Put(k *Key, v []byte) (err error)

	// Put key-value pairs that have already been sorted in sequential key order.
	PutRange(values []KeyValue) (err error)

	// Delete removes an entry given key.
	Delete(k *Key) (err error)
}

// Batchers allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type Batcher interface {
	NewBatch() Batch
}

// Batch groups operations into a transaction.
type Batch interface {
	// Commits a batch of operations.
	Commit() (err error)

	// Delete removes from the batch a put using the given key.
	Delete(k *Key)

	// Put adds to the batch a put using the given key/value.
	Put(k *Key, v []byte)

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
