// +build hyperleveldb

package storage

import (
	"bytes"
	_ "fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/go/hyperleveldb"
)

const (
	Version = "HyperLevelDB"

	Driver = "github.com/janelia-flyem/go/hyperleveldb"

	// Default size of LRU cache that caches frequently used uncompressed blocks.
	DefaultCacheSize = 128 * dvid.Mega

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
	DefaultBlockSize = 64 * dvid.Kilo

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
	DefaultWriteBufferSize = 256 * dvid.Mega

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

// --- The Leveldb Implementation must satisfy a Engine interface ----

type LevelDB struct {
	// Directory of datastore
	directory string

	// Config at time of Open()
	config dvid.Config

	options *leveldbOptions
	ldb     *levigo.DB
}

// NewStore returns a leveldb backend.
func NewStore(path string, create bool, config dvid.Config) (Engine, error) {
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt := &leveldbOptions{
		Options:      levigo.NewOptions(),
		ReadOptions:  levigo.NewReadOptions(),
		WriteOptions: levigo.NewWriteOptions(),
		env:          levigo.NewDefaultEnv(),
	}

	leveldb := &LevelDB{
		directory: path,
		config:    config,
		options:   opt,
	}
	// Set flags based on create parameter
	opt.SetCreateIfMissing(create)
	opt.SetErrorIfExists(create)

	// Create associated data structures with default values
	bloomBits, found, err := config.GetInt("BloomFilterBitsPerKey")
	if err != nil {
		return nil, err
	}
	if !found {
		bloomBits = DefaultBloomBits
	}
	if create {
		opt.SetBloomFilterBitsPerKey(bloomBits)
	}

	cacheSize, found, err := config.GetInt("CacheSize")
	if err != nil {
		return nil, err
	}
	if !found {
		cacheSize = DefaultCacheSize
	} else {
		cacheSize *= dvid.Mega
	}
	if create {
		opt.SetLRUCacheSize(cacheSize)
	}

	writeBufferSize, found, err := config.GetInt("WriteBufferSize")
	if err != nil {
		return nil, err
	}
	if !found {
		writeBufferSize = DefaultWriteBufferSize
	} else {
		writeBufferSize *= dvid.Mega
	}
	if create {
		opt.SetWriteBufferSize(writeBufferSize)
	}

	maxOpenFiles, found, err := config.GetInt("MaxOpenFiles")
	if err != nil {
		return nil, err
	}
	if !found {
		maxOpenFiles = DefaultMaxOpenFiles
	}
	if create {
		opt.SetMaxOpenFiles(maxOpenFiles)
	}

	blockSize, found, err := config.GetInt("BlockSize")
	if err != nil {
		return nil, err
	}
	if !found {
		blockSize = DefaultBlockSize
	}
	if create {
		opt.SetBlockSize(blockSize)
	}

	opt.SetInfoLog(nil)
	opt.SetParanoidChecks(false)
	//opt.SetBlockRestartInterval(8)

	// Don't bother with compression on leveldb side because it will be
	// selectively applied on DVID side.  We may return and then transmit
	// Snappy-compressed data without ever decompressing on server-side.
	opt.SetCompression(levigo.NoCompression) // (levigo.SnappyCompression)

	ldb, err := levigo.Open(path, opt.Options)
	if err != nil {
		return nil, err
	}
	leveldb.ldb = ldb

	return leveldb, nil
}

// ---- Engine interface ----

func (db *LevelDB) IsBatcher() bool    { return true }
func (db *LevelDB) IsBulkIniter() bool { return true }
func (db *LevelDB) IsBulkWriter() bool { return true }

func (db *LevelDB) GetConfig() dvid.Config {
	return db.config
}

// ---- KeyValueDB interface -----

// Close closes the leveldb and then the I/O abstraction for leveldb.
func (db *LevelDB) Close() {
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
func (db *LevelDB) Get(k Key) (v []byte, err error) {
	dvid.StartCgo()
	ro := db.options.ReadOptions
	v, err = db.ldb.Get(ro, k.Bytes())
	dvid.StopCgo()
	StoreBytesRead <- len(v)
	return
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.
func (db *LevelDB) GetRange(kStart, kEnd Key) (values []KeyValue, err error) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	values = []KeyValue{}
	it.Seek(kStart.Bytes())
	endBytes := kEnd.Bytes()
	//	fmt.Printf("levigo.GetRange: %x -> %x\n", kStart.Bytes(), endBytes)
	for {
		if it.Valid() {
			value := it.Value()
			StoreBytesRead <- len(value)
			if bytes.Compare(it.Key(), endBytes) > 0 {
				return
			}
			//			fmt.Printf("          Valid: %x\n", it.Key())
			var key Key
			key, err = kStart.BytesToKey(it.Key())
			if err != nil {
				return
			}
			values = append(values, KeyValue{key, value})
			it.Next()
		} else {
			err = it.GetError()
			return
		}
	}
}

// ProcessRange sends a range of key-value pairs to chunk handlers.
func (db *LevelDB) ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) error {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	endBytes := kEnd.Bytes()
	it.Seek(kStart.Bytes())
	//	fmt.Printf("levigo.ProcessRange: %x -> %x\n", kStart.Bytes(), endBytes)
	for {
		if it.Valid() {
			value := it.Value()
			StoreBytesRead <- len(value)
			if bytes.Compare(it.Key(), endBytes) > 0 {
				return nil
			}
			//			fmt.Printf("              Valid: %x\n", it.Key())
			// Send to channel
			key, err := kStart.BytesToKey(it.Key())
			if err != nil {
				return err
			}

			if op.Wg != nil {
				op.Wg.Add(1)
			}
			chunk := &Chunk{
				op,
				KeyValue{key, value},
			}
			f(chunk)

			it.Next()
		} else {
			return it.GetError()
		}
	}
}

// Put writes a value with given key.
func (db *LevelDB) Put(k Key, v []byte) error {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	err := db.ldb.Put(wo, k.Bytes(), v)
	dvid.StopCgo()
	StoreBytesWritten <- len(v)
	return err
}

// PutRange puts key/value pairs that have been sorted in sequential key order.
// Current implementation in levigo driver simply does a batch write.
func (db *LevelDB) PutRange(values []KeyValue) error {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	wb := levigo.NewWriteBatch()
	defer func() {
		wb.Close()
		dvid.StopCgo()
	}()
	bytesPut := 0
	for _, kv := range values {
		wb.Put(kv.K.Bytes(), kv.V)
		bytesPut += len(kv.V)
	}
	err := db.ldb.Write(wo, wb)
	if err != nil {
		return err
	}
	StoreBytesWritten <- bytesPut
	return nil
}

// Delete removes a value with given key.
func (db *LevelDB) Delete(k Key) (err error) {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	err = db.ldb.Delete(wo, k.Bytes())
	dvid.StopCgo()
	return
}

// --- Batcher interface ----

type goBatch struct {
	*levigo.WriteBatch
	wo  *levigo.WriteOptions
	ldb *levigo.DB
}

// NewBatch returns an implementation that allows batch writes
func (db *LevelDB) NewBatch() Batch {
	dvid.StartCgo()
	defer dvid.StopCgo()
	return &goBatch{levigo.NewWriteBatch(), db.options.WriteOptions, db.ldb}
}

// --- Batch interface ---

func (batch *goBatch) Commit() error {
	dvid.StartCgo()
	defer dvid.StopCgo()
	return batch.ldb.Write(batch.wo, batch.WriteBatch)
}

func (batch *goBatch) Delete(k Key) {
	dvid.StartCgo()
	defer dvid.StopCgo()
	batch.WriteBatch.Delete(k.Bytes())
}

func (batch *goBatch) Put(k Key, v []byte) {
	dvid.StartCgo()
	defer dvid.StopCgo()
	StoreBytesWritten <- len(v)
	batch.WriteBatch.Put(k.Bytes(), v)
}

func (batch *goBatch) Clear() {
	dvid.StartCgo()
	defer dvid.StopCgo()
	batch.WriteBatch.Clear()
}

func (batch *goBatch) Close() {
	dvid.StartCgo()
	defer dvid.StopCgo()
	batch.WriteBatch.Close()
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
