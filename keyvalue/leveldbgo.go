// +build purego

package keyvalue

import (
	"github.com/janelia-flyem/dvid"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/descriptor"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Ranges []leveldb.Range

type Sizes struct {
	leveldb.Sizes
}

// Open will open and possibly create a datastore at the given directory.
func OpenLeveldb(path string, create bool, kvOpts KeyValueOptions) (db KeyValueDB, err error) {
	leveldb_desc, err := descriptor.OpenFile(path)
	if err != nil {
		return
	}

	opts := kvOpts.(*goKeyValueOptions)

	// Set the CreateIfMissing flag.
	opts.Options.Flag |= opt.OFCreateIfMissing

	// Create the LRU cache
	if opts.nLRUCacheBytes < 8*dvid.Mega {
		opts.nLRUCacheBytes = 8 * dvid.Mega
	}
	opts.Options.BlockCache = cache.NewLRUCache(opts.nLRUCacheBytes)

	// Handle optional Bloom filter
	if opts.bloomBitsPerKey > 0 {
		opts.Options.Filter = filter.NewBloomFilter(opts.bloomBitsPerKey)
	}

	// Open the leveldb
	leveldb_db, err := leveldb.Open(leveldb_desc, &(opts.Options))
	if err != nil {
		return
	}
	db = &goLDB{
		directory: path,
		opts:      *opts, // We want a copy at time of Open()
		desc:      leveldb_desc,
		ldb:       leveldb_db,
	}
	return
}

// GetReadOptions returns a specific implementation of ReadOptions
func GetReadOptions() (ro ReadOptions) {
	ro = &goReadOptions{}
	return
}

// GetWriteOptions returns a specific implementation of ReadOptions
func GetWriteOptions() (wo WriteOptions) {
	wo = &goWriteOptions{}
	return
}

// GetWriteBatch returns an implementation that allows batch writes
func GetWriteBatch() (batch WriteBatch) {
	batch = &goBatch{}
	return
}

// GetKeyValueOptions returns an implementation of KeyValueOptions
func GetKeyValueOptions() (opts KeyValueOptions) {
	opts = &goKeyValueOptions{
		Options:        opt.Options{},
		nLRUCacheBytes: DefaultCacheSize,
	}
	return
}

// GoLDB is a pure go implementation of the KeyValueDB interface
type goLDB struct {
	// Directory of datastore
	directory string

	// Leveldb options at time of Open()
	opts goKeyValueOptions

	// File I/O abstraction
	desc *descriptor.FileDesc

	// Leveldb connection
	ldb *leveldb.DB
}

type goReadOptions struct {
	opt.ReadOptions
}

type goWriteOptions struct {
	opt.WriteOptions
}

type goBatch struct {
	leveldb.Batch
}

type goKeyValueOptions struct {
	opt.Options
	nLRUCacheBytes  int
	bloomBitsPerKey int
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

func (wo *goWriteOptions) SetSync(on bool) {
	if on {
		wo.Flag |= opt.WFSync
	} else {
		wo.Flag &^= opt.WFSync
	}
}

// Delete removes from the batch a put using the given key.
func (batch *goBatch) Delete(k Key) {
	batch.Batch.Delete(k)
}

// Put adds to the batch a put using the given key/value.
func (batch *goBatch) Put(k Key, v Value) {
	batch.Batch.Put(k, v)
}

// Reset clears the contents of a batch
func (batch *goBatch) Reset() {
	batch.Batch.Reset()
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
	nBytes = opts.Options.GetWriteBuffer()
	return
}

// Number of open files that can be used by the DB.  You may need to
// increase this if your database has a large working set (budget
// one open file per 2MB of working set).
func (opts *goKeyValueOptions) SetMaxOpenFiles(nFiles int) {
	opts.Options.MaxOpenFiles = nFiles
}

func (opts *goKeyValueOptions) GetMaxOpenFiles() (nFiles int) {
	nFiles = opts.Options.GetMaxOpenFiles()
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
	nBytes = opts.GetBlockSize()
	return
}

// SetCache sets the size of the LRU cache that caches frequently used 
// uncompressed blocks.
func (opts *goKeyValueOptions) SetLRUCacheSize(nBytes int) {
	opts.nLRUCacheBytes = nBytes
}

func (opts *goKeyValueOptions) GetLRUCacheSize() (nBytes int) {
	nBytes = opts.nLRUCacheBytes
	return
}

// SetBloomFilter sets the bits per key for a bloom filter.  This filter
// will reduce the number of unnecessary disk reads needed for Get() calls
// by a large factor.
func (opts *goKeyValueOptions) SetBloomFilterBitsPerKey(bitsPerKey int) {
	opts.bloomBitsPerKey = bitsPerKey
}

func (opts *goKeyValueOptions) GetBloomFilterBitsPerKey() (bitsPerKey int) {
	bitsPerKey = opts.bloomBitsPerKey
	return
}

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *goLDB) Close() {
	db.ldb.Close()
	db.desc.Close()
}

// Get returns a value given a key.
func (db *goLDB) Get(k Key, ro ReadOptions) (v Value, err error) {
	v, err = db.ldb.Get(k, &(ro.(*goReadOptions).ReadOptions))
	return
}

// Put writes a value with given key.
func (db *goLDB) Put(k Key, v Value, wo WriteOptions) (err error) {
	err = db.ldb.Put(k, v, &(wo.(*goWriteOptions).WriteOptions))
	return
}

// Delete removes a value with given key.
func (db *goLDB) Delete(k Key, wo WriteOptions) (err error) {
	err = db.ldb.Delete(k, &(wo.(*goWriteOptions).WriteOptions))
	return
}

// Write allows you to batch a series of key/value puts.
func (db *goLDB) Write(batch WriteBatch, wo WriteOptions) (err error) {
	err = db.ldb.Write(&(batch.(*goBatch).Batch),
		&(wo.(*goWriteOptions).WriteOptions))
	return
}

// GetApproximateSizes returns the approximate number of bytes of
// file system space used by one or more key ranges.
func (db *goLDB) GetApproximateSizes(ranges Ranges) (sizes Sizes, err error) {
	sizes.Sizes, err = db.ldb.GetApproximateSizes([]leveldb.Range(ranges))
	return
}
