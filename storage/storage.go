/*
	Package storage provides a unified interface to a number of datastore
	implementations.  Since each datastore has different capabilities, this
	package defines a number of interfaces and the DataHandler interface provides
	a way to query which interfaces are implemented by a given DVID instance's
	backend.

	Each backend database must implement the following:

		NewDataHandler(path string, create bool, options Options) (DataHandler, error)

	While Key data has a number of components so key space can be managed
	more efficiently by the backend storage, values are simply []byte at
	this level.  We assume serialization/deserialization occur above the
	storage level.
*/
package storage

import (
	_ "encoding/binary"
	"fmt"
	"log"

	"github.com/janelia-flyem/dvid/dvid"
)

const (
	KeyDataGlobal    dvid.LocalID = 0
	KeyVersionGlobal dvid.LocalID = 0
)

var (
	// DiskAccess is a mutex to make sure we don't have goroutines simultaneously trying
	// to access the key-value database on disk.
	// TODO: Reexamine this in the context of parallel disk drives during cluster use.
	DiskAccess sync.Mutex
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

	1) Data: The DVID server-specific data index where the 0 index is
	     a global dataset like DVID configuration data.
	2) Version: The DVID server-specific version index that is
	     fewer bytes than a complete UUID.
	3) Index: The datatype-specific index (e.g., spatiotemporal) that allows
	     partitioning of the data.
*/
type Key struct {
	Data    dvid.LocalID
	Version dvid.LocalID
	Index   []byte
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *Key) Bytes() (b []byte) {
	b = key.Data.Bytes()
	b = append(b, key.Version.Bytes()...)
	b = append(b, key.Index...)
	return
}

// Bytes returns a string derived from the concatenation of the key elements.
func (key *Key) BytesString() string {
	b = key.Data.Bytes()
	b = append(b, key.Version.Bytes()...)
	b = append(b, key.Index...)
	return string(b)
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

// OpChunk is the unit of data passed from datastore to datatype-specific
// chunk handlers.
type OpChunk struct {
	Operation
	Chunk    []byte
	Key      storage.Key
	shutdown bool
}

// Each key can be hashed onto a consistent chunk channel within the slice of channels.
type OpChunkChannels ([]chan *OpChunk)

// Operation provides an abstract interface for a mapping, which performs
// a batch read of keys and sends chunks (or nil values if keys don't exist)
// to the data chunk handler.
type Operation interface {
	// ChunkChannels returns the array of channels available for this op's dataset.
	ChunkChannels() OpChunkChannels

	// DataLocalID returns a DVID instance-specific id for this data.  The id
	// can be held in a relatively small number of bytes and is a key component.
	DataLocalID() dvid.LocalID

	// The version node on which we are performing the operation.
	VersionLocalID() dvid.LocalID

	// Should we map, i.e., send chunks to handlers over channel, if a given
	// chunk key is absent?  This tends to be true for PUT operations and
	// false for GET operations if absent keys resolve to empty chunk values.
	MapOnAbsentKey() bool

	// Returns an IndexIterator that steps through all chunks for this operation.
	IndexIterator() IndexIterator

	// WaitAdd increments the wait queue for the operation.
	WaitAdd()

	// Wait blocks until the operation is complete.
	Wait()
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	JSONDatastore bool
	BulkIniter    bool
	BulkLoader    bool
}

// DataHandler implementations can fulfill a variety of interfaces.  Other parts
// of DVID, most notably the data type implementations, need to know what's available.
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type DataHandler interface {
	// All backends must supply basic key/value store.
	KeyValueDB

	// All backends must allow mapping of an operation by sending chunks of data
	// down channel to datatype-specific handlers.
	Map(op Operation)

	// Let clients know which interfaces are available with this backend.
	IsJSONDatastore() bool
	IsBulkIniter() bool
	IsBulkLoader() bool

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
