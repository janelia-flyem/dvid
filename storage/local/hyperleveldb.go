// +build hyperleveldb

package local

import (
	"bytes"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	humanize "github.com/janelia-flyem/go/go-humanize"
	"github.com/janelia-flyem/go/hyperleveldb"
)

const (
	Version = "HyperLevelDB"

	Driver = "github.com/janelia-flyem/go/hyperleveldb"

	// Default size of LRU cache that caches frequently used uncompressed blocks.
	DefaultCacheSize = 1024 * dvid.Mega

	// Default # bits for Bloom Filter.  The filter reduces the number of unnecessary
	// disk reads needed for Get() calls by a large factor.
	DefaultBloomBits = 16

	// Number of open files that can be used by the datastore.  You may need to
	// increase this if your datastore has a large working set (budget one open
	// file per 2MB of working set).  You might need to do "ulimit -n 1200" or
	// some other number to make sure you can handle the default.
	DefaultMaxOpenFiles = 1024

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
	DefaultWriteBufferSize = 512 * dvid.Mega

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

func GetOptions(create bool, config dvid.Config) (*leveldbOptions, error) {
	opt := &leveldbOptions{
		Options:      levigo.NewOptions(),
		ReadOptions:  levigo.NewReadOptions(),
		WriteOptions: levigo.NewWriteOptions(),
		env:          levigo.NewDefaultEnv(),
	}
	opt.WriteOptions.SetSync(DefaultSync) // Huge performance penalty to set sync to true

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
		dvid.Log(dvid.Normal, "leveldb cache size: %s\n",
			humanize.Bytes(uint64(cacheSize)))
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
		dvid.Log(dvid.Normal, "leveldb write buffer size: %s\n",
			humanize.Bytes(uint64(writeBufferSize)))
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

	return opt, nil
}

// NewKeyValueStore returns a leveldb backend.  If create is true, the leveldb
// will be created at the path if it doesn't already exist.
func NewKeyValueStore(path string, create bool, config dvid.Config) (storage.Engine, error) {
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt, err := GetOptions(create, config)
	if err != nil {
		return nil, err
	}

	leveldb := &LevelDB{
		directory: path,
		config:    config,
		options:   opt,
	}

	ldb, err := levigo.Open(path, opt.Options)
	if err != nil {
		return nil, err
	}
	leveldb.ldb = ldb

	return leveldb, nil
}

// RepairStore tries to repair a damaged leveldb
func RepairStore(path string, config dvid.Config) error {
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt, err := GetOptions(false, config)
	if err != nil {
		return err
	}

	err = levigo.RepairDatabase(path, opt.Options)
	if err != nil {
		return err
	}
	return nil
}

// ---- Engine interface ----

func (db *LevelDB) GetName() string {
	return "HyperLevelDB + levigo driver"
}
func (db *LevelDB) GetConfig() dvid.Config {
	return db.config
}

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

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (db *LevelDB) Get(ctx storage.Context, k []byte) ([]byte, error) {
	if ctx.Versioned() {
		vctx, _ := ctx.(storage.VersionedContext)

		// Get all versions of this key and return the most recent
		values, err := db.getSingleKeyVersions(k)
		if err != nil {
			return nil, err
		}
		kv, err := vctx.VersionedKeyValue(values)
		if kv != nil {
			return kv.V, err
		}
		return nil, err
	} else {
		dvid.StartCgo()
		ro := db.options.ReadOptions
		v, err := db.ldb.Get(ro, k)
		dvid.StopCgo()
		StoreValueBytesRead <- len(v)
		return v, err
	}
}

// getSingleKeyVersions returns all versions of a key.  These key-value pairs will be sorted
// in ascending key order.
func (db *LevelDB) getSingleKeyVersions(vctx storage.VersionedContext, k []byte) ([]storage.KeyValue, error) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	values := []KeyValue{}
	kStart, err := vctx.MinVersionKey(k)
	if err != nil {
		return nil, err
	}
	kEnd, err := vctx.MaxVersionKey(k)
	if err != nil {
		return nil, err
	}

	it.Seek(kStart)
	for {
		if it.Valid() {
			itKey := it.Key()
			StoreKeyBytesRead <- len(itKey)
			if bytes.Compare(itKey, kEnd) > 0 {
				return values, nil
			}
			itValue := it.Value()
			StoreValueBytesRead <- len(itValue)

			it.Next()
		} else {
			return nil, it.GetError()
		}
	}
}

type errorableKV struct {
	*KeyValue
	error
}

// versionedRange sends a range of key-value pairs for a particular version down a channel.
func (db *LevelDB) versionedRange(vctx storage.VersionedContext, kStart, kEnd []byte, ch chan errorableKV, keysOnly bool) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := c.db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	minKey, err := vctx.MinVersionKey(kStart)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	maxKey, err = vctx.MaxVersionKey(kEnd)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}

	values := []KeyValue{}
	maxVersionKey, err := vctx.MaxVersionKey(kStart)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	it.Seek(minKey)
	var itValue []byte
	for {
		if it.Valid() {
			if !keysOnly {
				itValue = it.Value()
				StoreValueBytesRead <- len(itValue)
			}
			itKey := it.Key()
			StoreKeyBytesRead <- len(itKey)
			// Did we pass all versions for last key read?
			if bytes.Compare(itKey, maxVersionKey) > 0 {
				maxVersionKey, err = vctx.MaxVersionKey(itKey)
				if err != nil {
					ch <- errorableKV{nil, err}
					return
				}
				kv, err := vctx.VersionedKeyValue(values)
				if err != nil {
					ch <- errorableKV{nil, err}
					return
				}
				if kv.K != nil {
					ch <- errorableKV{kv, nil}
				}
				values = []KeyValue{}
			}
			// Did we pass the final key?
			if bytes.Compare(itKey, maxKey) > 0 {
				if len(values) > 0 {
					kv, err := vctx.VersionedKeyValue(values)
					if err != nil {
						ch <- errorableKV{nil, err}
						return
					}
					if kv.K != nil {
						ch <- errorableKV{kv, err}
					}
					ch <- nil
				}
				return
			}
			values = append(values, KeyValue{itKey, itValue})
			it.Next()
		} else {
			return it.GetError()
		}
	}
}

// unversionedRange sends a range of key-value pairs down a channel.
func (db *LevelDB) unversionedRange(ctx storage.Context, kStart, kEnd []byte, ch chan errorableKV, keysOnly bool) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := c.db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	var itValue []byte
	it.Seek(kStart)
	for {
		if it.Valid() {
			if !keysOnly {
				itValue = it.Value()
				StoreValueBytesRead <- len(itValue)
			}
			itKey := it.Key()
			StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, kEnd) > 0 {
				ch <- nil
				return
			}
			ch <- errorableKV{&KeyValue{itKey, itValue}, nil}
			it.Next()
		} else {
			return it.GetError()
		}
	}
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
func (db *LevelDB) KeysInRange(ctx storage.Context, kStart, kEnd []byte) ([][]byte, error) {
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx.Versioned() {
			db.versionedRange(ctx.(storage.VersionedContext), kStart, kEnd, ch, true)
		} else {
			db.unversionedRange(ctx, kStart, kEnd, ch, true)
		}
	}()

	// Consume the keys.
	values := [][]byte{}
	for {
		result := <-ch
		if result == nil {
			return values, nil
		}
		if result.error != nil {
			return nil, result.error
		}
		values = append(values, result.KeyValue.K)
	}
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  If the keys are versioned, all key-value
// pairs for the particular version will be returned.
func (db *LevelDB) GetRange(ctx storage.Context, kStart, kEnd []byte) ([]*storage.KeyValue, error) {
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx.Versioned() {
			db.versionedRange(ctx.(storage.VersionedContext), kStart, kEnd, ch, false)
		} else {
			db.unversionedRange(ctx, kStart, kEnd, ch, false)
		}
	}()

	// Consume the key-value pairs.
	values := []KeyValue{}
	for {
		result := <-ch
		if result == nil {
			return values, nil
		}
		if result.error != nil {
			return nil, result.error
		}
		values = append(values, result.KeyValue)
	}
}

// ProcessRange sends a range of key-value pairs to chunk handlers.  If the keys are versioned,
// only key-value pairs for kStart's version will be transmitted.
func (db *LevelDB) ProcessRange(ctx storage.Context, kStart, kEnd []byte, op *ChunkOp, f func(*Chunk)) error {
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx.Versioned() {
			db.versionedRange(ctx.(storage.VersionedContext), kStart, kEnd, ch, false)
		} else {
			db.unversionedRange(ctx, kStart, kEnd, ch, false)
		}
	}()

	// Consume the key-value pairs.
	values := []KeyValue{}
	for {
		result := <-ch
		if result == nil {
			return nil
		}
		if result.error != nil {
			return result.error
		}
		if op.Wg != nil {
			op.Wg.Add(1)
		}
		chunk := &Chunk{op, result.KeyValue}
		f(chunk)
	}
}

// ---- OrderedKeyValueSetter interface ------

// Put writes a value with given key.
func (db *LevelDB) Put(ctx storage.Context, k, v []byte) error {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	key := ctx.ConstructKey(k)
	err := db.ldb.Put(wo, key, v)
	dvid.StopCgo()
	StoreKeyBytesWritten <- len(key)
	StoreValueBytesWritten <- len(v)
	return err
}

// PutRange puts key-value pairs that have been sorted in sequential key order.
// Current implementation in levigo driver simply does a batch write.
func (db *LevelDB) PutRange(ctx storage.Context, values []storage.KeyValue) error {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	wb := levigo.NewWriteBatch()
	defer func() {
		wb.Close()
		dvid.StopCgo()
	}()
	var keyBytesPut, valueBytesPut int
	for _, kv := range values {
		key := ctx.ConstructKey(kv.K)
		wb.Put(key, kv.V)
		keyBytesPut += len(key)
		valueBytesPut += len(kv.V)
	}
	err := db.ldb.Write(wo, wb)
	if err != nil {
		return err
	}
	StoreKeyBytesWritten <- keyBytesPut
	StoreValueBytesWritten <- valueBytesPut
	return nil
}

// Delete removes a value with given key.
func (db *LevelDB) Delete(ctx storage.Context, k []byte) (err error) {
	dvid.StartCgo()
	wo := db.options.WriteOptions
	err = db.ldb.Delete(wo, ctx.ConstructKey(k))
	dvid.StopCgo()
	return
}

// --- Batcher interface ----

type goBatch struct {
	ctx storage.Context
	*levigo.WriteBatch
	wo  *levigo.WriteOptions
	ldb *levigo.DB
}

// NewBatch returns an implementation that allows batch writes
func (db *LevelDB) NewBatch(ctx storage.Context) Batch {
	dvid.StartCgo()
	defer dvid.StopCgo()
	return &goBatch{ctx, levigo.NewWriteBatch(), db.options.WriteOptions, db.ldb}
}

// --- Batch interface ---

func (batch *goBatch) Delete(k []byte) {
	dvid.StartCgo()
	defer dvid.StopCgo()
	batch.WriteBatch.Delete(batch.ctx.ConstructKey(k))
}

func (batch *goBatch) Put(k, v []byte) {
	dvid.StartCgo()
	defer dvid.StopCgo()
	key := batch.ctx.ConstructKey(k)
	StoreKeyBytesWritten <- len(key)
	StoreValueBytesWritten <- len(v)
	batch.WriteBatch.Put(key, v)
}

func (batch *goBatch) Commit() error {
	dvid.StartCgo()
	defer dvid.StopCgo()
	err := batch.ldb.Write(batch.wo, batch.WriteBatch)
	batch.WriteBatch.Close()
	return err
}

/** Clear and Close were removed due to how other key-value stores implement batches.
    It's easier to implement cross-database handling of a simple write/delete batch
    that commits then closes rather than something that clears.

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
**/

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
