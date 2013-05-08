// +build !purego

package keyvalue

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/jmhodges/levigo"
)

const Version = "github.com/jmhodges/levigo"

type Ranges []levigo.Range

type Sizes []uint64

// --- The Leveldb Implementation ----

type goLDB struct {
	// Directory of datastore
	directory string

	// Leveldb options at time of Open()
	opts goKeyValueOptions

	// Leveldb connection
	ldb *levigo.DB
}

// Open will open and possibly create a datastore at the given directory.
func OpenLeveldb(path string, create bool, kvOpts KeyValueOptions) (db KeyValueDB, err error) {
	goOpts := kvOpts.(*goKeyValueOptions)
	if goOpts == nil {
		err = fmt.Errorf("Nil pointer passed in as key-value options to OpenLeveldb()!")
		return
	}

	goOpts.Options.SetCreateIfMissing(create)
	goOpts.Options.SetErrorIfExists(create)
	goOpts.Options.SetCompression(levigo.SnappyCompression)

	var leveldb_db *levigo.DB
	leveldb_db, err = levigo.Open(path, goOpts.Options)
	if err != nil {
		return
	}

	db = &goLDB{
		directory: path,
		opts:      *goOpts, // We want a copy at time of Open()
		ldb:       leveldb_db,
	}
	return
}

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *goLDB) Close() {
	if db != nil {
		if db.ldb != nil {
			db.ldb.Close()
		}
		if db.opts.Options != nil {
			db.opts.Options.Close()
		}
		if db.opts.filter != nil {
			db.opts.filter.Close()
		}
		if db.opts.cache != nil {
			db.opts.cache.Close()
		}
		if db.opts.env != nil {
			db.opts.env.Close()
		}
	}
}

// Get returns a value given a key.
func (db *goLDB) Get(k Key, ro ReadOptions) (v Value, err error) {
	v, err = db.ldb.Get(ro.(*goReadOptions).ReadOptions, k)
	return
}

// Put writes a value with given key.
func (db *goLDB) Put(k Key, v Value, wo WriteOptions) (err error) {
	err = db.ldb.Put(wo.(*goWriteOptions).WriteOptions, k, v)
	return
}

// Delete removes a value with given key.
func (db *goLDB) Delete(k Key, wo WriteOptions) (err error) {
	err = db.ldb.Delete(wo.(*goWriteOptions).WriteOptions, k)
	return
}

// Write allows you to batch a series of key/value puts.
func (db *goLDB) Write(batch WriteBatch, wo WriteOptions) (err error) {
	err = db.ldb.Write(wo.(*goWriteOptions).WriteOptions, batch.(*goBatch).WriteBatch)
	return
}

// GetApproximateSizes returns the approximate number of bytes of
// file system space used by one or more key ranges.
func (db *goLDB) GetApproximateSizes(ranges Ranges) (sizes Sizes, err error) {
	sizes = db.ldb.GetApproximateSizes([]levigo.Range(ranges))
	err = nil
	return
}

// NewIterator returns a read-only Iterator. 
func (db *goLDB) NewIterator(ro ReadOptions) (it Iterator, err error) {
	it = goIterator{db.ldb.NewIterator(ro.(*goReadOptions).ReadOptions)}
	err = nil
	return
}

// --- Iterator ---

type goIterator struct {
	*levigo.Iterator
}

func (it goIterator) Key() Key {
	return Key(it.Iterator.Key())
}

func (it goIterator) Seek(key Key) {
	it.Iterator.Seek([]byte(key))
}

func (it goIterator) Value() Value {
	return it.Iterator.Value()
}

// --- Read Options -----

type goReadOptions struct {
	*levigo.ReadOptions
}

// NewReadOptions returns a specific implementation of ReadOptions
func NewReadOptions() (ro ReadOptions) {
	ro = &goReadOptions{levigo.NewReadOptions()}
	return
}

func (ro *goReadOptions) SetDontFillCache(on bool) {
	ro.ReadOptions.SetFillCache(!on)
}

// --- Write Options -----

type goWriteOptions struct {
	*levigo.WriteOptions
}

// NewWriteOptions returns a specific implementation of ReadOptions
func NewWriteOptions() (wo WriteOptions) {
	wo = &goWriteOptions{levigo.NewWriteOptions()}
	return
}

// --- Write Batch ----

type goBatch struct {
	*levigo.WriteBatch
}

// NewWriteBatch returns an implementation that allows batch writes
func NewWriteBatch() (batch WriteBatch) {
	batch = &goBatch{levigo.NewWriteBatch()}
	return
}

func (batch *goBatch) Delete(k Key) {
	batch.Delete(k)
}

func (batch *goBatch) Put(k Key, v Value) {
	batch.Put(k, v)
}

// --- Options ----

type goKeyValueOptions struct {
	*levigo.Options

	// Keep leveldb settings for quick recall and checks on set
	nLRUCacheBytes  int
	bloomBitsPerKey int
	writeBufferSize int
	maxOpenFiles    int
	blockSize       int

	// Keep pointers for associated data structures for close
	cache  *levigo.Cache
	filter *levigo.FilterPolicy
	env    *levigo.Env
}

// NewKeyValueOptions returns an implementation of KeyValueOptions
func NewKeyValueOptions() KeyValueOptions {
	pOpt := &goKeyValueOptions{
		Options: levigo.NewOptions(),
		env:     levigo.NewDefaultEnv(),
	}

	// Create associated data structures with default values
	pOpt.SetBloomFilterBitsPerKey(DefaultBloomBits)
	pOpt.SetLRUCacheSize(DefaultCacheSize)
	pOpt.SetWriteBufferSize(DefaultWriteBufferSize)
	pOpt.SetMaxOpenFiles(DefaultMaxOpenFiles)
	pOpt.SetBlockSize(DefaultBlockSize)
	pOpt.SetInfoLog(nil)
	pOpt.SetParanoidChecks(false)
	//opts.SetBlockRestartInterval(8)
	pOpt.SetCompression(levigo.SnappyCompression)
	return pOpt
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
	if nBytes != opts.writeBufferSize {
		dvid.Log(dvid.Debug, "Write buffer set to %d bytes.\n", nBytes)
		opts.Options.SetWriteBufferSize(nBytes)
		opts.writeBufferSize = nBytes
	}
}

func (opts *goKeyValueOptions) GetWriteBufferSize() (nBytes int) {
	nBytes = opts.writeBufferSize
	return
}

// Number of open files that can be used by the DB.  You may need to
// increase this if your database has a large working set (budget
// one open file per 2MB of working set).
// See: http://leveldb.googlecode.com/svn/trunk/doc/impl.html
func (opts *goKeyValueOptions) SetMaxOpenFiles(nFiles int) {
	if nFiles != opts.maxOpenFiles {
		opts.Options.SetMaxOpenFiles(nFiles)
		opts.maxOpenFiles = nFiles
	}
}

func (opts *goKeyValueOptions) GetMaxOpenFiles() (nFiles int) {
	nFiles = opts.maxOpenFiles
	return
}

// Approximate size of user data packed per block.  Note that the
// block size specified here corresponds to uncompressed data.  The
// actual size of the unit read from disk may be smaller if
// compression is enabled.  This parameter can be changed dynamically.
func (opts *goKeyValueOptions) SetBlockSize(nBytes int) {
	if nBytes != opts.blockSize {
		dvid.Log(dvid.Debug, "Block size set to %d bytes.\n", nBytes)
		opts.Options.SetBlockSize(nBytes)
		opts.blockSize = nBytes
	}
}

func (opts *goKeyValueOptions) GetBlockSize() (nBytes int) {
	nBytes = opts.blockSize
	return
}

// SetCache sets the size of the LRU cache that caches frequently used 
// uncompressed blocks.
func (opts *goKeyValueOptions) SetLRUCacheSize(nBytes int) {
	if nBytes != opts.nLRUCacheBytes {
		if opts.cache != nil {
			opts.cache.Close()
		}
		dvid.Log(dvid.Debug, "LRU cache size set to %d bytes.\n", nBytes)
		opts.cache = levigo.NewLRUCache(nBytes)
		opts.nLRUCacheBytes = nBytes
		opts.Options.SetCache(opts.cache)
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
			opts.filter.Close()
		}
		opts.filter = levigo.NewBloomFilter(DefaultBloomBits)
		opts.bloomBitsPerKey = bitsPerKey
		opts.Options.SetFilterPolicy(opts.filter)
	}
}

func (opts *goKeyValueOptions) GetBloomFilterBitsPerKey() (bitsPerKey int) {
	bitsPerKey = opts.bloomBitsPerKey
	return
}
