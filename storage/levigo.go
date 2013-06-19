// +build levigo

package storage

import (
	"github.com/janelia-flyem/dvid/dvid"
	"log"

	"github.com/jmhodges/levigo"
)

const (
	Version = "github.com/jmhodges/levigo"

	// Default size of LRU cache that caches frequently used uncompressed blocks.
	DefaultCacheSize = 768 * dvid.Mega

	// Default # bits for Bloom Filter.  The filter reduces the number of unnecessary
	// disk reads needed for Get() calls by a large factor.
	DefaultBloomBits = 10

	// Number of open files that can be used by the datastore.  You may need to
	// increase this if your datastore has a large working set (budget one open
	// file per 2MB of working set).
	DefaultMaxOpenFiles = 1000

	// Approximate size of user data packed per block.  Note that the
	// block size specified here corresponds to uncompressed data.  The
	// actual size of the unit read from disk may be smaller if
	// compression is enabled.  This parameter can be changed dynamically.
	DefaultBlockSize = 256 * dvid.Kilo

	// Amount of data to build up in memory (backed by an unsorted log
	// on disk) before converting to a sorted on-disk file.  Increasing
	// this value will automatically increase the size of the datastore
	// compared to the actual stored data.
	//
	// Larger values increase performance, especially during bulk loads.
	// Up to two write buffers may be held in memory at the same time,
	// so you may wish to adjust this parameter to control memory usage.
	// Also, a larger write buffer will result in a longer recovery time
	// the next time the database is opened.
	DefaultWriteBufferSize = 100 * dvid.Mega

	// Write Options

	// If Sync=true, the write will be flushed from the operating system
	// buffer cache is considered complete.  If set, writes will be slower.
	//
	// If Sync=false, and the machine crashes, some recent
	// writes may be lost.  Note that if it is just the process that
	// crashes (i.e., the machine does not reboot), no writes will be
	// lost even if Sync=false.
	//
	// In other words, a DB write with sync==false has similar
	// crash semantics as the "write()" system call.  A DB write
	// with sync==true has similar crash semantics to a "write()"
	// system call followed by "fsync()".
	DefaultSync = false

	// Read Options

	// If true, all data read from underlying storage will be verified
	// against coresponding checksums, thereby making reads slower.
	DefaultVerifyChecksums = false

	// If true, iteration caching will be disabled.  This might be of
	// use during bulk scans.
	DefaultDontFillCache = false
)

type Ranges []levigo.Range

type Sizes []uint64

// --- The Leveldb Implementation must satisfy a DataHandler interface ----

type goLDB struct {
	// Directory of datastore
	directory string

	// Settings for the leveldb
	settings map[string]interface{}

	// Options at time of Open()
	options *leveldbOptions

	// Leveldb connection
	ldb *levigo.DB
}

// NewDataHandler returns a leveldb backend.
func NewDataHandler(path string, create bool, options *Options) (db DataHandler, err error) {
	ldbOptions := options.initBySettings(create)
	ldbDB, err := levigo.Open(path, options.ldb().Options)
	if err != nil {
		return
	}
	db = &goLDB{
		directory: path,
		settings:  options.Settings,
		options:   ldbOptions,
		ldb:       ldbDB,
	}
	return
}

// --- DataHandler interface ----

func (db *goLDB) IsKeyValueDB() bool     { return true }
func (db *goLDB) IsJSONDatastore() bool  { return false }
func (db *goLDB) ProvidesIterator() bool { return true }
func (db *goLDB) IsBulkIniter() bool     { return true }
func (db *goLDB) IsBulkLoader() bool     { return true }
func (db *goLDB) IsBatcher() bool        { return true }
func (db *goLDB) GetOptions() *Options   { return &Options{db.settings, db.options} }

// ---- KeyValueDB interface -----

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *goLDB) Close() {
	if db != nil {
		if db.ldb != nil {
			db.ldb.Close()
		}
		if db.options.Options != nil {
			db.options.Options.Close()
		}
		if db.options.ReadOptions != nil {
			db.options.ReadOptions.Close()
		}
		if db.options.WriteOptions != nil {
			db.options.WriteOptions.Close()
		}
		if db.options.filter != nil {
			db.options.filter.Close()
		}
		if db.options.cache != nil {
			db.options.cache.Close()
		}
		if db.options.env != nil {
			db.options.env.Close()
		}
	}
}

// Get returns a value given a key.
func (db *goLDB) Get(k Key) (v []byte, err error) {
	ro := db.options.ReadOptions
	v, err = db.ldb.Get(ro, k.Bytes())
	return
}

// Put writes a value with given key.
func (db *goLDB) Put(k Key, v []byte) (err error) {
	wo := db.options.WriteOptions
	err = db.ldb.Put(wo, k.Bytes(), v)
	return
}

// Delete removes a value with given key.
func (db *goLDB) Delete(k Key) (err error) {
	wo := db.options.WriteOptions
	err = db.ldb.Delete(wo, k.Bytes())
	return
}

// GetApproximateSizes returns the approximate number of bytes of
// file system space used by one or more key ranges.
func (db *goLDB) GetApproximateSizes(ranges Ranges) (sizes Sizes, err error) {
	sizes = db.ldb.GetApproximateSizes([]levigo.Range(ranges))
	err = nil
	return
}

// --- IteratorMaker interface ---

func (db *goLDB) NewIterator() (it Iterator, err error) {
	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	it = &goIterator{db.ldb.NewIterator(ro)}
	return
}

// --- Iterator interface ---

// The embedded levigo.Iterator handles all other interface requirements
// except for the ones below, which mainly handle typing conversion between
// Key, Value and []byte.

type goIterator struct {
	*levigo.Iterator
}

func (it goIterator) Key() []byte {
	return it.Iterator.Key()
}

func (it goIterator) Seek(key Key) {
	it.Iterator.Seek(key.Bytes())
}

func (it goIterator) Value() []byte {
	return it.Iterator.Value()
}

// --- Batcher interface ----

type goBatch struct {
	*levigo.WriteBatch
	wo  *levigo.WriteOptions
	ldb *levigo.DB
}

// NewWriteBatch returns an implementation that allows batch writes
func (db *goLDB) NewBatchWrite() Batch {
	return &goBatch{levigo.NewWriteBatch(), db.options.WriteOptions, db.ldb}
}

// --- Batch interface ---

func (batch *goBatch) Write() (err error) {
	err = batch.ldb.Write(batch.wo, batch.WriteBatch)
	return
}

func (batch *goBatch) Delete(k Key) {
	batch.WriteBatch.Delete(k.Bytes())
}

func (batch *goBatch) Put(k Key, v []byte) {
	batch.WriteBatch.Put(k.Bytes(), v)
}

// --- Options ----

type leveldbOptions struct {
	*levigo.Options
	*levigo.ReadOptions  // Standard settings on NewReadOptions()
	*levigo.WriteOptions // Standard settings on NewWriteOptions()

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

// Cast the generic options interface{} to a leveldbOptions struct.
func (opt *Options) ldb() *leveldbOptions {
	ldbOptions, ok := opt.options.(*leveldbOptions)
	if !ok {
		log.Fatalf("getLeveldbOptions() -- bad cast!\n")
	}
	return ldbOptions
}

// Initialize Options using the settings.
func (opt *Options) initBySettings(create bool) *leveldbOptions {
	ldbOptions := &leveldbOptions{
		Options:      levigo.NewOptions(),
		ReadOptions:  levigo.NewReadOptions(),
		WriteOptions: levigo.NewWriteOptions(),
		env:          levigo.NewDefaultEnv(),
	}
	opt.options = ldbOptions

	// Set flags based on create parameter
	ldbOptions.Options.SetCreateIfMissing(create)
	ldbOptions.Options.SetErrorIfExists(create)

	// Create associated data structures with default values
	bloomBits, ok := opt.IntSetting("BloomFilterBitsPerKey")
	if !ok {
		bloomBits = DefaultBloomBits
	}
	ldbOptions.SetBloomFilterBitsPerKey(bloomBits)

	cacheSize, ok := opt.IntSetting("CacheSize")
	if !ok {
		cacheSize = DefaultCacheSize
	}
	ldbOptions.SetLRUCacheSize(cacheSize)

	writeBufferSize, ok := opt.IntSetting("WriteBufferSize")
	if !ok {
		writeBufferSize = DefaultWriteBufferSize
	}
	ldbOptions.SetWriteBufferSize(writeBufferSize)

	maxOpenFiles, ok := opt.IntSetting("MaxOpenFiles")
	if !ok {
		maxOpenFiles = DefaultMaxOpenFiles
	}
	ldbOptions.SetMaxOpenFiles(maxOpenFiles)

	blockSize, ok := opt.IntSetting("BlockSize")
	if !ok {
		blockSize = DefaultBlockSize
	}
	ldbOptions.SetBlockSize(blockSize)

	ldbOptions.SetInfoLog(nil)
	ldbOptions.SetParanoidChecks(false)
	//ldbOptions.SetBlockRestartInterval(8)

	// Don't bother with compression on leveldb side because it will
	// be selectively applied on DVID side.  We may way to return and
	// then transmit Snappy-compressed data without ever decompressing
	// on this side.
	//ldbOptions.SetCompression(levigo.SnappyCompression)
	ldbOptions.SetCompression(levigo.NoCompression)

	return ldbOptions
}

// Amount of data to build up in memory (backed by an unsorted log
// on disk) before converting to a sorted on-disk file.
//
// Larger values increase performance, especially during bulk loads.
// Up to two write buffers may be held in memory at the same time,
// so you may wish to adjust this parameter to control memory usage.
// Also, a larger write buffer will result in a longer recovery time
// the next time the database is opened.
func (opts *leveldbOptions) SetWriteBufferSize(nBytes int) {
	if nBytes != opts.writeBufferSize {
		dvid.Log(dvid.Debug, "Write buffer set to %d bytes.\n", nBytes)
		opts.Options.SetWriteBufferSize(nBytes)
		opts.writeBufferSize = nBytes
	}
}

func (opts *leveldbOptions) GetWriteBufferSize() (nBytes int) {
	nBytes = opts.writeBufferSize
	return
}

// Number of open files that can be used by the DB.  You may need to
// increase this if your database has a large working set (budget
// one open file per 2MB of working set).
// See: http://leveldb.googlecode.com/svn/trunk/doc/impl.html
func (opts *leveldbOptions) SetMaxOpenFiles(nFiles int) {
	if nFiles != opts.maxOpenFiles {
		opts.Options.SetMaxOpenFiles(nFiles)
		opts.maxOpenFiles = nFiles
	}
}

func (opts *leveldbOptions) GetMaxOpenFiles() (nFiles int) {
	nFiles = opts.maxOpenFiles
	return
}

// Approximate size of user data packed per block.  Note that the
// block size specified here corresponds to uncompressed data.  The
// actual size of the unit read from disk may be smaller if
// compression is enabled.  This parameter can be changed dynamically.
func (opts *leveldbOptions) SetBlockSize(nBytes int) {
	if nBytes != opts.blockSize {
		dvid.Log(dvid.Debug, "Block size set to %d bytes.\n", nBytes)
		opts.Options.SetBlockSize(nBytes)
		opts.blockSize = nBytes
	}
}

func (opts *leveldbOptions) GetBlockSize() (nBytes int) {
	nBytes = opts.blockSize
	return
}

// SetCache sets the size of the LRU cache that caches frequently used
// uncompressed blocks.
func (opts *leveldbOptions) SetLRUCacheSize(nBytes int) {
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

func (opts *leveldbOptions) GetLRUCacheSize() (nBytes int) {
	nBytes = opts.nLRUCacheBytes
	return
}

// SetBloomFilter sets the bits per key for a bloom filter.  This filter
// will reduce the number of unnecessary disk reads needed for Get() calls
// by a large factor.
func (opts *leveldbOptions) SetBloomFilterBitsPerKey(bitsPerKey int) {
	if bitsPerKey != opts.bloomBitsPerKey {
		if opts.filter != nil {
			opts.filter.Close()
		}
		opts.filter = levigo.NewBloomFilter(DefaultBloomBits)
		opts.bloomBitsPerKey = bitsPerKey
		opts.Options.SetFilterPolicy(opts.filter)
	}
}

func (opts *leveldbOptions) GetBloomFilterBitsPerKey() (bitsPerKey int) {
	bitsPerKey = opts.bloomBitsPerKey
	return
}
