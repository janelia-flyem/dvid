// +build couchbase

/*
	Couchbase-specific issues:

	1) Keys are not arbitrary sequences of bytes and impose memory requirements:
	(http://www.couchbase.com/docs/couchbase-devguide-2.1.0/couchbase-keys.html)
		- Keys are strings, typically enclosed by quotes for any given SDK.
		- No spaces are allowed in a key.
		- Separators and identifiers are allowed, such as underscore: 'person_93847'.
		- A key must be unique within a bucket; if you attempt to store the same key in a bucket,
		   it will either overwrite the value or return an error in the case of add().
		- Maximum key size is 250 bytes. Couchbase Server stores all keys in RAM and does not
		   remove these keys to free up space in RAM. Take this into consideration when you select
		   keys and key length for your application.

	2) There is a hierarchy of addressibility:  server -> pools -> buckets.

	The first point can lead to issues.  See the following:
	http://www.couchbase.com/docs/couchbase-devguide-2.1.0/couchbase-keys.html

	Key size is important.  If you consider the size of keys stored for tens or hundreds
	of millions of records. One hundred million keys which are 70 Bytes each plus meta data
	at 54 Bytes each will require about 23 GB of RAM for document meta data. As of Couchbase
	Server 2.0.1, metadata is 60 Bytes and as of Couchbase Server 2.1.0 it is 54 Bytes.

	Since Couchbase keys cannot have spaces and presumably need to be alphanumeric, we
	simply use hexadecimal representation for keys.  This can double the # of bytes
	needed to represent a key.  TODO -- choose a better key representation.
*/

package storage

import (
	"log"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/go/go-couchbase"
)

const (
	Version = "github.com/janelia-flyem/go/go-couchbase"

	// TODO -- Add couchbase-specific constants
)

// --- The Couchbase Implementation must satisfy a Engine interface ----

type goCouch struct {
	// url of datastore
	url string

	// couchbase client
	client *couchbase.Client
	pool   *couchbase.Pool
	bucket *couchbase.Bucket
}

// NewEngine returns a couchbase backend.
func NewEngine(url string, create bool, options *Options) (db Engine, err error) {
	if create {
		err = fmt.Errorf("Couchbase must be manually installed and configured.  'init' unavailable.")
		return
	}
	client, err := couchbase.Connect(url)
	if err != nil {
		return
	}
	// TODO -- Optimize pool and bucket depending on volumes and versions
	pool, err := client.GetPool("default")
	if err != nil {
		return
	}
	bucket, err := pool.GetBucket("default")
	if err != nil {
		return
	}
	db = &goCouch{
		url:    url,
		client: &client,
		pool:   &pool,
		bucket: bucket,
	}
	return
}

// --- Engine interface ----

func (db *goCouch) IsOrderedKeyValueDB() bool     { return true }
func (db *goCouch) IsJSONDatastore() bool  { return true }
func (db *goCouch) ProvidesIterator() bool { return true }
func (db *goCouch) IsBulkIniter() bool     { return true }
func (db *goCouch) IsBulkLoader() bool     { return true }
func (db *goCouch) IsBatcher() bool        { return true }
func (db *goCouch) GetOptions() *Options   { return &Options{} }

// ---- OrderedKeyValueDB interface -----

// Close closes the couchbase bucket
func (db *goCouch) Close() {
	if db != nil {
		if db.bucket != nil {
			db.bucket.Close()
		}
	}
}

// Get returns a value given a key.
func (db *goCouch) Get(k Key) (v []byte, err error) {
	v, err = db.bucket.GetRaw(k.String())
	return
}

// Put writes a value with given key.
func (db *goCouch) Put(k Key, v []byte) (err error) {
	// Expiration time set to 0 (permanent)
	err = db.bucket.SetRaw(k.String(), 0, v)
	return
}

// Delete removes a value with given key.
func (db *goCouch) Delete(k Key) (err error) {
	err = db.bucket.Delete(k.String())
	return
}

// --- IteratorMaker interface ---

func (db *goCouch) NewIterator() (it Iterator, err error) {
	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	it = &goIterator{db.ldb.NewIterator(ro)}
	return
}

// --- Iterator interface ---

type goIterator struct {
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
func (db *goCouch) NewBatchWrite() Batch {
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
