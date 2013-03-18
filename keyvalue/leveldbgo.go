// +build purego

package keyvalue

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

const Version = "github.com/syndtr/goleveldb/leveldb"

type Ranges []leveldb.Range

type Sizes struct {
	leveldb.Sizes
}

// --- The Leveldb Implementation ----

type goLDB struct {
	// Directory of datastore
	directory string

	// Leveldb options at time of Open()
	opts goKeyValueOptions

	// File I/O abstraction
	stor *storage.FileStorage

	// Leveldb connection
	ldb *leveldb.DB
}

// Open will open and possibly create a datastore at the given directory.
func OpenLeveldb(path string, create bool, kvOpts KeyValueOptions) (db KeyValueDB, err error) {
	goOpts := kvOpts.(*goKeyValueOptions)
	if goOpts == nil {
		err = fmt.Errorf("Nil pointer passed in as key-value options to Openleveldb()!")
		return
	}

	leveldb_stor, err := storage.OpenFile(path)
	if err != nil {
		return
	}

	// Set the CreateIfMissing flag.
	if create {
		goOpts.Options.Flag |= opt.OFCreateIfMissing
		goOpts.Options.Flag |= opt.OFErrorIfExist
	}

	// Open the leveldb
	leveldb_db, err := leveldb.Open(leveldb_stor, goOpts.Options)
	if err != nil {
		return
	}
	db = &goLDB{
		directory: path,
		opts:      *goOpts, // We want a copy at time of Open()
		stor:      leveldb_stor,
		ldb:       leveldb_db,
	}
	return
}

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *goLDB) Close() {
	db.ldb.Close()
	db.stor.Close()
}

// Get returns a value given a key.
func (db *goLDB) Get(k Key, ro ReadOptions) (v Value, err error) {
	v, err = db.ldb.Get(k, ro.(*goReadOptions).ReadOptions)
	return
}

// Put writes a value with given key.
func (db *goLDB) Put(k Key, v Value, wo WriteOptions) (err error) {
	err = db.ldb.Put(k, v, wo.(*goWriteOptions).WriteOptions)
	return
}

// Delete removes a value with given key.
func (db *goLDB) Delete(k Key, wo WriteOptions) (err error) {
	err = db.ldb.Delete(k, wo.(*goWriteOptions).WriteOptions)
	return
}

// Write allows you to batch a series of key/value puts.
func (db *goLDB) Write(batch WriteBatch, wo WriteOptions) (err error) {
	err = db.ldb.Write(batch.(*goBatch).Batch, wo.(*goWriteOptions).WriteOptions)
	return
}

// GetApproximateSizes returns the approximate number of bytes of
// file system space used by one or more key ranges.
func (db *goLDB) GetApproximateSizes(ranges Ranges) (sizes Sizes, err error) {
	sizes.Sizes, err = db.ldb.GetApproximateSizes([]leveldb.Range(ranges))
	return
}

// NewIterator returns a read-only Iterator. 
func (db *goLDB) NewIterator(ro ReadOptions) (it Iterator, err error) {
	it = db.ldb.NewIterator(ro.(*goReadOptions).ReadOptions)
	err = nil
	return
}

// --- Read Options -----

type goReadOptions struct {
	*opt.ReadOptions
}

// NewReadOptions returns a specific implementation of ReadOptions
func NewReadOptions() (ro ReadOptions) {
	ro = &goReadOptions{}
	return
}

func (ro *goReadOptions) SetVerifyChecksums(on bool) {
	if on {
		ro.ReadOptions.Flag |= opt.RFVerifyChecksums
	} else {
		ro.ReadOptions.Flag &^= opt.RFVerifyChecksums
	}
}

func (ro *goReadOptions) SetDontFillCache(on bool) {
	if on {
		ro.ReadOptions.Flag |= opt.RFDontFillCache
	} else {
		ro.ReadOptions.Flag &^= opt.RFDontFillCache
	}
}

// --- Write Options -----

type goWriteOptions struct {
	*opt.WriteOptions
}

// NewWriteOptions returns a specific implementation of ReadOptions
func NewWriteOptions() (wo WriteOptions) {
	wo = &goWriteOptions{}
	return
}

func (wo *goWriteOptions) SetSync(on bool) {
	if on {
		wo.Flag |= opt.WFSync
	} else {
		wo.Flag &^= opt.WFSync
	}
}

// --- Write Batch ----

type goBatch struct {
	*leveldb.Batch
}

// NewWriteBatch returns an implementation that allows batch writes
func NewWriteBatch() (batch WriteBatch) {
	batch = &goBatch{}
	return
}

func (batch *goBatch) Delete(k Key) {
	batch.Delete(k)
}

func (batch *goBatch) Put(k Key, v Value) {
	batch.Put(k, v)
}

// Clear clears the contents of a batch
func (batch *goBatch) Clear() {
	batch.Reset()
}

// Close is a no-op for goleveldb.
func (batch *goBatch) Close() {
	// no-op
}

// --- Options ----

type goKeyValueOptions struct {
	*opt.Options

	// Keep leveldb settings for quick recall and checks on set
	nLRUCacheBytes  int
	bloomBitsPerKey int
	writeBufferSize int
	maxOpenFiles    int
	blockSize       int

	// Keep pointers for associated data structures for close
	cache  *cache.LRUCache
	filter *filter.BloomFilter
}

// NewKeyValueOptions returns an implementation of KeyValueOptions
func NewKeyValueOptions() (opts KeyValueOptions) {
	pOpt := &goKeyValueOptions{
		Options:         &opt.Options{},
		nLRUCacheBytes:  DefaultCacheSize,
		bloomBitsPerKey: DefaultBloomBits,
	}

	// Create associated data structures with default values
	pOpt.SetLRUCacheSize(DefaultCacheSize)
	pOpt.SetBloomFilterBitsPerKey(DefaultBloomBits)

	pOpt.SetWriteBufferSize(DefaultWriteBufferSize)
	pOpt.SetMaxOpenFiles(DefaultMaxOpenFiles)
	pOpt.SetBlockSize(DefaultBlockSize)

	pOpt.Options.CompressionType = opt.SnappyCompression

	opts = pOpt

	return
}

// Amount of data to build up in memory (backed by an unsorted log
// on disk) before converting to a sorted on-disk file.
//
// Larger values increase performance, especially during bulk loads.
// Up to two write buffers may be held in memory at the same time,
// so you may wish to adjust this parameter to control memory usage.
// Also, a larger write buffer will result in a longer recovery time
// the next time the database is opened.
func (opts *goKeyValueOptions) SetWriteBufferSize(nBytes int) {
	opts.Options.WriteBuffer = nBytes
}

func (opts *goKeyValueOptions) GetWriteBufferSize() (nBytes int) {
	nBytes = opts.Options.WriteBuffer
	return
}

// Number of open files that can be used by the DB.  You may need to
// increase this if your database has a large working set (budget
// one open file per 2MB of working set).
func (opts *goKeyValueOptions) SetMaxOpenFiles(nFiles int) {
	opts.Options.MaxOpenFiles = nFiles
}

func (opts *goKeyValueOptions) GetMaxOpenFiles() (nFiles int) {
	nFiles = opts.Options.MaxOpenFiles
	return
}

// Approximate size of user data packed per block.  Note that the
// block size specified here corresponds to uncompressed data.  The
// actual size of the unit read from disk may be smaller if
// compression is enabled.  This parameter can be changed dynamically.
func (opts *goKeyValueOptions) SetBlockSize(nBytes int) {
	opts.Options.BlockSize = nBytes
}

func (opts *goKeyValueOptions) GetBlockSize() (nBytes int) {
	nBytes = opts.Options.BlockSize
	return
}

// SetCache sets the size of the LRU cache that caches frequently used 
// uncompressed blocks.  NOTE: For goleveldb, this is basically
// ignored until I figure out how its being used with the namespace
// concept.
func (opts *goKeyValueOptions) SetLRUCacheSize(nBytes int) {
	if nBytes != opts.nLRUCacheBytes {
		opts.nLRUCacheBytes = nBytes
	}
}

func (opts *goKeyValueOptions) GetLRUCacheSize() (nBytes int) {
	nBytes = opts.nLRUCacheBytes
	return
}

// SetBloomFilter sets the bits per key for a bloom filter.  This filter
// will reduce the number of unnecessary disk reads needed for Get() calls
// by a large factor.
func (opts *goKeyValueOptions) SetBloomFilterBitsPerKey(bitsPerKey int) {
	if bitsPerKey != opts.bloomBitsPerKey {
		if opts.filter != nil {
			// NOTE -- No destructor for bloom filter in goleveldb?
		}
		opts.filter = filter.NewBloomFilter(bitsPerKey)
		opts.Options.Filter = opts.filter
		opts.bloomBitsPerKey = bitsPerKey
	}
}

func (opts *goKeyValueOptions) GetBloomFilterBitsPerKey() (bitsPerKey int) {
	bitsPerKey = opts.bloomBitsPerKey
	return
}
