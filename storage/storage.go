/*
	Package storage provides a unified interface to a number of datastore
	implementations.  Since each datastore has different capabilities, this
	package defines a number of interfaces and the DataHandler interface provides
	a way to query which interfaces are implemented by a given DVID instance's
	backend.
*/
package storage

import (
	"fmt"
	_ "log"

	"github.com/janelia-flyem/dvid/dvid"
)

type Key []byte
type Value []byte

func (key Key) String() string {
	return fmt.Sprintf("%x", []byte(key))
}

func (value Value) String() string {
	return fmt.Sprintf("%x", []byte(value))
}

// Size returns the # of bytes in this Value.
func (value Value) Size() int {
	return len(value)
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

type DataHandlerRequirements struct {
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

	GetOptions() Options
}

// KeyValueDB provides an interface to a number of key/value
// implementations, e.g., C++ or Go leveldb libraries.
type KeyValueDB interface {
	// Closes datastore.
	Close()

	// Get returns a value given a key.
	Get(k Key) (v Value, err error)

	// Put writes a value with given key.
	Put(k Key, v Value) (err error)

	// Delete removes an entry given key
	Delete(k Key) (err error)

	// NewIterator returns a read-only Iterator. 
	NewIterator() (it Iterator, err error)
}

// Batchers allow batching a series of operations into an atomic unit.  For
// some backends, this may also improve throughput.
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
	Put(k Key, v Value)

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

// Iterator provides an interface to a read-only iterator that allows
// easy sequential scanning of key/value pairs.  
type Iterator interface {
	// Close deallocates the iterator and freeing any underlying struct. 
	Close()

	// GetError returns any error that occured during iteration. 
	GetError() error

	// Key returns a copy of the key for current iterator position. 
	Key() Key

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
	Value() Value
}
