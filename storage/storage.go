/*
	Package storage provides a unified interface to a number of datastore
	implementations.  Since each datastore has different capabilities, this
	package defines a number of interfaces and the DataHandler interface provides
	a way to query which interfaces are implemented by a given DVID instance's
	backend.

	While Key data has a number of components so key space can be managed
	more efficiently by the backend storage, values are simply []byte at
	this level.  We assume serialization/deserialization occur above the
	storage level.
*/
package storage

import (
	"fmt"
	"log"
)

/*
	Key holds DVID-centric data like version (UUID), data set, and block
	identifiers and that follow a convention of how to collapse those
	identifiers into a []byte.  Ideally, we'd like to keep Key within
	the datastore package and have storage independent of DVID concepts,
	but in order to optimize the layout of data in some backend databases,
	the backend drivers need the additional DVID information.  For example,
	Couchbase allows configuration at the bucket level (RAM cache, CPUs)
	and a dataset should be placed within a bucket.
	data within the key space.  The first byte of the key is a prefix that
	separates general categories of data.  It also allows some headroom to
	create different versions of datastore layout.

	If compressed key components are provided, those are used when
	constructing keys to minimize key size.
*/
type Key struct {
	DatasetKey []byte
	VersionKey []byte
	BlockKey   []byte

	CompressedVersion []byte
	CompressedDataset []byte
	CompressedBlock   []byte
}

// Bytes returns a compressed Key as a slice of bytes.  If compressed key components
// are not empty, they are used instead of the full string key components.
func (key *Key) Bytes() (b []byte) {
	b = []byte{}
	if key.CompressedDataset != nil {
		b = append(b, key.CompressedDataset...)
	} else {
		b = append(b, key.DatasetKey...)
	}
	if key.CompressedVersion != nil {
		b = append(b, key.CompressedVersion...)
	} else {
		b = append(b, key.VersionKey...)
	}
	if key.CompressedBlock != nil {
		b = append(b, key.CompressedBlock...)
	} else {
		b = append(b, key.BlockKey...)
	}
	return
}

// String returns a hexadecimal representation of the bytes encoding a key
// so it is readable on a terminal.
func (key *Key) String() string {
	return fmt.Sprintf("%x", key.Bytes())
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
	Iterator      bool
	JSONDatastore bool
	BulkIniter    bool
	BulkLoader    bool
	Batcher       bool
}

// Each backend database must implement the following:
// NewDataHandler(path string, create bool, options Options) (DataHandler, error)

// DataHandler implementations can fulfill a variety of interfaces.  Other parts
// of DVID, most notably the data type implementations, need to know what's available.
// Data types can throw a warning at init time if the backend doesn't support required 
// interfaces, or they can choose to implement multiple ways of handling data.
type DataHandler interface {
	// All backends must supply basic key/value store.
	KeyValueDB

	// Let clients know which interfaces are available with this backend.
	ProvidesIterator() bool
	IsJSONDatastore() bool
	IsBulkIniter() bool
	IsBulkLoader() bool
	IsBatcher() bool

	GetOptions() *Options
}

// KeyValueDB provides an interface to a number of key/value
// implementations, e.g., C++ or Go leveldb libraries.
type KeyValueDB interface {
	// Closes datastore.
	Close()

	// Get returns a value given a key.
	Get(k Key) (v []byte, err error)

	// Put writes a value with given key.
	Put(k Key, v []byte) (err error)

	// Delete removes an entry given key
	Delete(k Key) (err error)
}

// Batchers are backends that allow batching a series of operations into an atomic unit.
// For some backends, this may also improve throughput.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type Batcher interface {
	NewBatchWrite() Batch
}

// Batch implements an atomic batch write or transaction.
type Batch interface {
	// Write commits a batch of operations.
	Write() (err error)

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
// data since they can assume a blank database.
type BulkIniter interface {
}

// BulkLoaders employ some sort of optimization to efficiently load large
// amount of data.  For some key-value databases, this requires keys to
// be presorted.
type BulkLoader interface {
}

// IteratorMakers are backends that support Iterators.
type IteratorMaker interface {
	// NewIterator returns a read-only Iterator. 
	NewIterator() (it Iterator, err error)
}

// Iterator provides an interface to a read-only iterator that allows
// easy sequential scanning of key/value pairs.  
type Iterator interface {
	// Close deallocates the iterator and freeing any underlying struct. 
	Close()

	// GetError returns any error that occured during iteration. 
	GetError() error

	// Key returns a copy of the key for current iterator position. 
	Key() []byte

	// Next moves the iterator to the next sequential key. 
	Next()

	// Prev moves the iterator to the previous sequential key. 
	Prev()

	// Seek moves the iterator to the position of the given key. 
	Seek(key Key)

	// SeekToFirst moves the iterator to the first key in the datastore. 
	SeekToFirst()

	// SeekToLast moves the iterator to the last key in the datastore. 
	SeekToLast()

	// Valid returns false if the iterator has iterated before the first key
	// or past the last key. 
	Valid() bool

	// Value returns a copy of the value for current iterator position. 
	Value() []byte
}
