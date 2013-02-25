/*
	This file defines a number of interfaces and common types among leveldb
	implementations.  Some types are defined in the associated implementation
	Go files, e.g., leveldbgo.go and levigo.go.
*/

package keyvalue

import (
	"fmt"
	_ "log"

	"github.com/janelia-flyem/dvid/dvid"
)

// Default size of leveldb cache
const DefaultCacheSize = 1 * dvid.Giga

type Key []byte
type Value []byte

func (key Key) String() string {
	return fmt.Sprintf("%x", []byte(key))
}

func (value Value) String() string {
	return fmt.Sprintf("%x", []byte(value))
}

// ReadOptions provides an interface to leveldb read options
type ReadOptions interface {
	// If true, all data read from underlying storage will be verified
	// against coresponding checksums, thereby making reads slower.
	SetVerifyChecksums(on bool)

	// If true, iteration caching will be disabled.  This might be of
	// use during bulk scans.
	SetDontFillCache(on bool)
}

// WriteOptions provides an interface to leveldb write options
type WriteOptions interface {
	// If SetSync(true), the write will be flushed from the operating system
	// buffer cache is considered complete.  If set, writes will be slower.
	//
	// If SetSync(false), and the machine crashes, some recent
	// writes may be lost.  Note that if it is just the process that
	// crashes (i.e., the machine does not reboot), no writes will be
	// lost even if SetSync(false).
	//
	// In other words, a DB write with sync==false has similar
	// crash semantics as the "write()" system call.  A DB write
	// with sync==true has similar crash semantics to a "write()"
	// system call followed by "fsync()".
	SetSync(on bool)
}

// writeBatch provides an interface to a batch write.
type WriteBatch interface {

	// Delete removes from the batch a put using the given key.
	Delete(k Key)

	// Put adds to the batch a put using the given key/value.
	Put(k Key, v Value)

	// Reset clears the contents of a batch
	Reset()
}

// KeyValueOptions allows setting of a number of key/value datastore
// options.
type KeyValueOptions interface {
	// SetWriteBufferSize sets the size of the buffer in memory used to store writes
	// until writing to a sorted on-disk file.
	SetWriteBufferSize(nBytes int)
	GetWriteBufferSize() (nBytes int)

	// Number of open files that can be used by the DB.  You may need to
	// increase this if your database has a large working set (budget
	// one open file per 2MB of working set).
	SetMaxOpenFiles(nFiles int)
	GetMaxOpenFiles() (nFiles int)

	// SetBlockSize sets the size of blocks, the unit of transfer to and from
	// persistent storage.  Adjacent keys are grouped together into the same block.
	SetBlockSize(nBytes int)
	GetBlockSize() (nBytes int)

	// SetCache sets the size of the LRU cache that caches frequently used 
	// uncompressed blocks.
	SetLRUCacheSize(nBytes int)
	GetLRUCacheSize() (nBytes int)

	// SetBloomFilter sets the bits per key for a bloom filter.  This filter
	// will reduce the number of unnecessary disk reads needed for Get() calls
	// by a large factor.
	SetBloomFilterBitsPerKey(bitsPerKey int)
	GetBloomFilterBitsPerKey() (bitsPerKey int)
}

// KeyValueDB provides an interface to a number of key/value
// implementations, e.g., C++ or Go leveldb libraries.
type KeyValueDB interface {
	// Closes datastore.
	Close()

	// Get returns a value given a key.
	Get(k Key, ro ReadOptions) (v Value, err error)

	// Put writes a value with given key.
	Put(k Key, v Value, wo WriteOptions) (err error)

	// Delete removes an entry given key
	Delete(k Key, wo WriteOptions) (err error)

	// Write allows you to batch a series of key/value puts.
	Write(batch WriteBatch, wo WriteOptions) (err error)

	// GetApproximateSizes returns the approximate number of bytes of
	// file system space used by one or more key ranges.
	GetApproximateSizes(ranges Ranges) (sizes Sizes, err error)
}
