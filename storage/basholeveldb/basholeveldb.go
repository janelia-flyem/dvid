// +build basholeveldb

package basholeveldb

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	levigo "github.com/janelia-flyem/go/basholeveldb"
	humanize "github.com/janelia-flyem/go/go-humanize"
	"github.com/janelia-flyem/go/semver"
	"github.com/janelia-flyem/go/uuid"
)

// These constants were guided by Basho documentation and their tuning of leveldb:
//   https://github.com/basho/leveldb/blob/develop/README
// See video on "Optimizing LevelDB for Performance and Scale" here:
//   http://www.youtube.com/watch?v=vo88IdglU_8
const (
	// Default size of LRU cache that caches frequently used uncompressed blocks.
	DefaultCacheSize = 536870912

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
	DefaultWriteBufferSize = 62914560

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

func init() {
	ver, err := semver.Make("0.9.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in basholeveldb: %v\n", err)
	}
	e := Engine{"basholeveldb", "Basho-tuned LevelDB", ver}
	storage.RegisterEngine(e)
}

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) IsDistributed() bool {
	return false
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

// NewStore returns a leveldb. The passed Config must contain "path" string.
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newLevelDB(config)
}

func parseConfig(config dvid.StoreConfig) (path string, testing bool, err error) {
	c := config.GetAll()

	v, found := c["path"]
	if !found {
		err = fmt.Errorf("%q must be specified for leveldb configuration", "path")
		return
	}
	var ok bool
	path, ok = v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string (%v)", "path", v)
		return
	}
	v, found = c["testing"]
	if found {
		testing, ok = v.(bool)
		if !ok {
			err = fmt.Errorf("%q setting must be a bool (%v)", "testing", v)
			return
		}
	}
	if testing {
		path = filepath.Join(os.TempDir(), path)
	}
	return
}

// newLevelDB returns a leveldb backend, creating leveldb
// at the path if it doesn't already exist.
func (e Engine) newLevelDB(config dvid.StoreConfig) (*LevelDB, bool, error) {
	path, _, err := parseConfig(config)
	if err != nil {
		return nil, false, err
	}

	// Is there a database already at this path?  If not, create.
	var created bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		dvid.TimeInfof("Database not already at path (%s). Creating directory...\n", path)
		created = true
		// Make a directory at the path.
		if err := os.MkdirAll(path, 0744); err != nil {
			return nil, true, fmt.Errorf("Can't make directory at %s: %v", path, err)
		}
	} else {
		dvid.TimeInfof("Found directory at %s (err = %v)\n", path, err)
	}

	// Open the database
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt, err := getOptions(config.Config)
	if err != nil {
		return nil, false, err
	}

	leveldb := &LevelDB{
		directory: path,
		config:    config,
		options:   opt,
	}

	dvid.TimeInfof("Opening basholeveldb @ path %s\n", path)
	ldb, err := levigo.Open(path, opt.Options)
	if err != nil {
		return nil, false, err
	}
	leveldb.ldb = ldb

	// if we know it's newly created, just return.
	if created {
		return leveldb, created, nil
	}

	// otherwise, check if there's been any metadata or we need to initialize it.
	metadataExists, err := leveldb.metadataExists()
	if err != nil {
		leveldb.Close()
		return nil, false, err
	}

	return leveldb, !metadataExists, nil
}

// ---- RepairableEngine interface implementation ------

// Repair tries to repair a damaged leveldb.  Requires "path" string.  Implements
// the RepairableEngine interface.
func (e Engine) Repair(path string) error {
	dvid.StartCgo()
	defer dvid.StopCgo()

	opt, err := getOptions(dvid.Config{})
	if err != nil {
		return err
	}

	err = levigo.RepairDatabase(path, opt.Options)
	if err != nil {
		return err
	}
	return nil
}

// ---- TestableEngine interface implementation -------

// AddTestConfig sets the basholeveldb as the default key-value backend.  If another
// engine is already set, it returns an error since only one key-value backend should
// be tested via tags.
func (e Engine) AddTestConfig(backend *storage.Backend) (storage.Alias, error) {
	alias := storage.Alias("basholeveldb")
	if backend.DefaultKVDB != "" {
		return alias, fmt.Errorf("basholeveldb can't be testable key-value.  DefaultKVDB already set to %s", backend.DefaultKVDB)
	}
	if backend.Metadata != "" {
		return alias, fmt.Errorf("basholeveldb can't be testable key-value.  Metadata already set to %s", backend.DefaultKVDB)
	}
	backend.Metadata = alias
	backend.DefaultKVDB = alias
	if backend.Stores == nil {
		backend.Stores = make(map[storage.Alias]dvid.StoreConfig)
	}
	tc := map[string]interface{}{
		"path":    fmt.Sprintf("dvid-test-basholeveldb-%x", uuid.NewV4().Bytes()),
		"testing": true,
	}
	var c dvid.Config
	c.SetAll(tc)
	backend.Stores[alias] = dvid.StoreConfig{Config: c, Engine: "basholeveldb"}
	return alias, nil
}

// Delete implements the TestableEngine interface by providing a way to dispose
// of testing databases.
func (e Engine) Delete(config dvid.StoreConfig) error {
	path, _, err := parseConfig(config)
	if err != nil {
		return err
	}

	// Delete the directory if it exists
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("Can't delete old datastore %q: %v", path, err)
		}
	}
	return nil
}

func (db *LevelDB) String() string {
	return fmt.Sprintf("basholeveldb @ %s", db.directory)
}

// --- The Leveldb Implementation must satisfy a Engine interface ----

type LevelDB struct {
	// Directory of datastore
	directory string

	// Config at time of Open()
	config dvid.StoreConfig

	options *leveldbOptions
	ldb     *levigo.DB
}

func getOptions(config dvid.Config) (*leveldbOptions, error) {
	opt := &leveldbOptions{
		Options:      levigo.NewOptions(),
		ReadOptions:  levigo.NewReadOptions(),
		WriteOptions: levigo.NewWriteOptions(),
		env:          levigo.NewDefaultEnv(),
	}
	opt.WriteOptions.SetSync(DefaultSync) // Huge performance penalty to set sync to true

	// Set flags based on create parameter
	opt.SetCreateIfMissing(true)
	opt.SetErrorIfExists(false)

	// Create associated data structures with default values
	bloomBits, found, err := config.GetInt("BloomFilterBitsPerKey")
	if err != nil {
		return nil, err
	}
	if !found {
		bloomBits = DefaultBloomBits
	}
	opt.SetBloomFilterBitsPerKey(bloomBits)

	cacheSize, found, err := config.GetInt("CacheSize")
	if err != nil {
		return nil, err
	}
	if !found {
		cacheSize = DefaultCacheSize
	} else {
		cacheSize *= dvid.Mega
	}
	dvid.TimeInfof("leveldb cache size: %s\n",
		humanize.Bytes(uint64(cacheSize)))
	opt.SetLRUCacheSize(cacheSize)

	writeBufferSize, found, err := config.GetInt("WriteBufferSize")
	if err != nil {
		return nil, err
	}
	if !found {
		writeBufferSize = DefaultWriteBufferSize
	} else {
		writeBufferSize *= dvid.Mega
	}
	dvid.TimeInfof("leveldb write buffer size: %s\n",
		humanize.Bytes(uint64(writeBufferSize)))
	opt.SetWriteBufferSize(writeBufferSize)

	maxOpenFiles, found, err := config.GetInt("MaxOpenFiles")
	if err != nil {
		return nil, err
	}
	if !found {
		maxOpenFiles = DefaultMaxOpenFiles
	}
	opt.SetMaxOpenFiles(maxOpenFiles)

	blockSize, found, err := config.GetInt("BlockSize")
	if err != nil {
		return nil, err
	}
	if !found {
		blockSize = DefaultBlockSize
	}
	opt.SetBlockSize(blockSize)
	opt.SetInfoLog(nil)
	opt.SetParanoidChecks(false)
	//opt.SetBlockRestartInterval(8)

	// Don't bother with compression on leveldb side because it will be
	// selectively applied on DVID side.  We may return and then transmit
	// Snappy-compressed data without ever decompressing on server-side.
	opt.SetCompression(levigo.NoCompression) // (levigo.SnappyCompression)

	return opt, nil
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
		db.ldb = nil
		db.options = nil
	}
}

// Equal returns true if the leveldb matches the given store configuration.
func (db *LevelDB) Equal(config dvid.StoreConfig) bool {
	path, _, err := parseConfig(config)
	if err != nil {
		return false
	}
	return db.directory == path
}

func (db *LevelDB) metadataExists() (bool, error) {
	var ctx storage.MetadataContext
	keyBeg, keyEnd := ctx.KeyRange()
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	it.Seek(keyBeg)
	for {
		if it.Valid() {
			// Did we pass the final key?
			if bytes.Compare(it.Key(), keyEnd) > 0 {
				break
			}
			return true, nil
		}
		break
	}
	if err := it.GetError(); err != nil {
		return false, err
	}
	dvid.TimeInfof("No metadata found for %s...\n", db)
	return false, nil
}

// ---- KeyValueChecker interface ------

// Exists returns true if the key exists.
func (db *LevelDB) Exists(ctx storage.Context, tk storage.TKey) (found bool, err error) {
	if db == nil {
		return false, fmt.Errorf("Can't call Exists() on nil LevelDB")
	}
	if db.options == nil {
		return false, fmt.Errorf("Can't call Exists() on db with nil options: %v", db)
	}
	if ctx == nil {
		return false, fmt.Errorf("Received nil context in Exists()")
	}
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	var key storage.Key
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return false, fmt.Errorf("Bad Exists(): context is versioned but doesn't fulfill interface: %v", ctx)
		}
		v := vctx.VersionID()
		key = vctx.ConstructKeyVersion(tk, v)
	} else {
		key = ctx.ConstructKey(tk)
	}
	it.Seek(key)
	if it.Valid() && bytes.Compare(it.Key(), key) == 0 {
		return true, nil
	}
	return false, nil
}

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (db *LevelDB) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GET on nil LevelDB")
	}
	if db.options == nil {
		return nil, fmt.Errorf("Can't call GET on db with nil options: %v", db)
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in Get()")
	}
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return nil, fmt.Errorf("Bad Get(): context is versioned but doesn't fulfill interface: %v", ctx)
		}

		// Get all versions of this key and return the most recent
		values, err := db.getSingleKeyVersions(vctx, tk)
		if err != nil {
			return nil, err
		}
		kv, err := vctx.VersionedKeyValue(values)
		if kv != nil {
			return kv.V, err
		}
		return nil, err
	} else {
		key := ctx.ConstructKey(tk)
		ro := db.options.ReadOptions
		dvid.StartCgo()
		v, err := db.ldb.Get(ro, key)
		dvid.StopCgo()
		storage.StoreValueBytesRead <- len(v)
		return v, err
	}
}

// getSingleKeyVersions returns all versions of a key.  These key-value pairs will be sorted
// in ascending key order and could include a tombstone key.
func (db *LevelDB) getSingleKeyVersions(vctx storage.VersionedCtx, tk []byte) ([]*storage.KeyValue, error) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	values := []*storage.KeyValue{}
	begKey, err := vctx.MinVersionKey(tk)
	if err != nil {
		return nil, err
	}
	endKey, err := vctx.MaxVersionKey(tk)
	if err != nil {
		return nil, err
	}

	it.Seek(begKey)
	for {
		if it.Valid() {
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			if bytes.Compare(itKey, endKey) > 0 {
				return values, nil
			}
			itValue := it.Value()
			storage.StoreValueBytesRead <- len(itValue)
			values = append(values, &storage.KeyValue{itKey, itValue})
			it.Next()
		} else {
			err = it.GetError()
			if err == nil {
				return values, nil
			}
			return nil, err
		}
	}
}

type errorableKV struct {
	*storage.KeyValue
	error
}

func sendKV(vctx storage.VersionedCtx, values []*storage.KeyValue, ch chan errorableKV) {
	if len(values) != 0 {
		kv, err := vctx.VersionedKeyValue(values)
		if err != nil {
			ch <- errorableKV{nil, err}
			return
		}
		if kv != nil {
			ch <- errorableKV{kv, nil}
		}
	}
}

// versionedRange sends a range of key-value pairs for a particular version down a channel.
func (db *LevelDB) versionedRange(vctx storage.VersionedCtx, begTKey, endTKey storage.TKey, ch chan errorableKV, done <-chan struct{}, keysOnly bool) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	minKey, err := vctx.MinVersionKey(begTKey)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	maxKey, err := vctx.MaxVersionKey(endTKey)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}

	values := []*storage.KeyValue{}
	maxVersionKey, err := vctx.MaxVersionKey(begTKey)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}

	it.Seek(minKey)
	var itValue []byte
	for {
		select {
		case <-done: // only happens if we don't care about rest of data.
			ch <- errorableKV{nil, nil}
			return
		default:
		}
		if it.Valid() {
			if !keysOnly {
				itValue = it.Value()
				storage.StoreValueBytesRead <- len(itValue)
			}
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)

			// Did we pass all versions for last key read?
			if bytes.Compare(itKey, maxVersionKey) > 0 {
				if storage.Key(itKey).IsDataKey() {
					indexBytes, err := storage.TKeyFromKey(itKey)
					if err != nil {
						ch <- errorableKV{nil, err}
						return
					}
					maxVersionKey, err = vctx.MaxVersionKey(indexBytes)
					if err != nil {
						ch <- errorableKV{nil, err}
						return
					}
				}
				sendKV(vctx, values, ch)
				values = []*storage.KeyValue{}
			}
			// Did we pass the final key?
			if bytes.Compare(itKey, maxKey) > 0 {
				if len(values) > 0 {
					sendKV(vctx, values, ch)
				}
				ch <- errorableKV{nil, nil}
				return
			}
			values = append(values, &storage.KeyValue{K: itKey, V: itValue})
			it.Next()
		} else {
			if err = it.GetError(); err != nil {
				ch <- errorableKV{nil, err}
			} else {
				sendKV(vctx, values, ch)
				ch <- errorableKV{nil, nil}
			}
			return
		}
	}
}

// unversionedRange sends a range of key-value pairs down a channel.
func (db *LevelDB) unversionedRange(ctx storage.Context, begTKey, endTKey storage.TKey, ch chan errorableKV, done <-chan struct{}, keysOnly bool) {
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	// Apply context if applicable
	begKey := ctx.ConstructKey(begTKey)
	endKey := ctx.ConstructKey(endTKey)

	var itValue []byte
	it.Seek(begKey)
	for {
		if it.Valid() {
			if !keysOnly {
				itValue = it.Value()
				storage.StoreValueBytesRead <- len(itValue)
			}
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, endKey) > 0 {
				break
			}
			select {
			case <-done:
				ch <- errorableKV{nil, nil}
				return
			case ch <- errorableKV{&storage.KeyValue{K: itKey, V: itValue}, nil}:
				it.Next()
			}
		} else {
			break
		}
	}
	if err := it.GetError(); err != nil {
		ch <- errorableKV{nil, err}
	} else {
		ch <- errorableKV{nil, nil}
	}
	return
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
func (db *LevelDB) KeysInRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]storage.TKey, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call KeysInRange on nil LevelDB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in KeysInRange()")
	}
	ch := make(chan errorableKV)
	done := make(chan struct{})
	defer close(done)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, done, true)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, done, true)
		}
	}()

	// Consume the keys.
	values := []storage.TKey{}
	for {
		result := <-ch
		if result.error != nil {
			return nil, result.error
		}
		if result.KeyValue == nil {
			return values, nil
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return nil, err
		}
		values = append(values, tk)
	}
}

// SendKeysInRange sends a range of keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
// End of range is marked by a nil key.
func (db *LevelDB) SendKeysInRange(ctx storage.Context, kStart, kEnd storage.TKey, kch storage.KeyChan) error {
	if db == nil {
		return fmt.Errorf("Can't call SendKeysInRange on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in SendKeysInRange()")
	}
	ch := make(chan errorableKV)
	done := make(chan struct{})
	defer close(done)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, done, true)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, done, true)
		}
	}()

	// Consume the keys.
	for {
		result := <-ch
		if result.error != nil {
			kch <- nil
			return result.error
		}
		if result.KeyValue == nil {
			kch <- nil
			return nil
		}
		kch <- result.KeyValue.K
	}
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  If the keys are versioned, all key-value
// pairs for the particular version will be returned.
func (db *LevelDB) GetRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]*storage.TKeyValue, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetRange on nil LevelDB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in GetRange()")
	}
	ch := make(chan errorableKV)
	done := make(chan struct{})
	defer close(done)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx == nil || !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, done, false)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, done, false)
		}
	}()

	// Consume the key-value pairs.
	values := []*storage.TKeyValue{}
	for {
		result := <-ch
		if result.error != nil {
			return nil, result.error
		}
		if result.KeyValue == nil {
			return values, nil
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return nil, err
		}
		tkv := storage.TKeyValue{tk, result.KeyValue.V}
		values = append(values, &tkv)
	}
}

// ProcessRange sends a range of key-value pairs to chunk handlers.  If the keys are versioned,
// only key-value pairs for kStart's version will be transmitted.  If f returns an error, the
// function is immediately terminated and returns an error.
func (db *LevelDB) ProcessRange(ctx storage.Context, kStart, kEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if db == nil {
		return fmt.Errorf("Can't call ProcessRange on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in ProcessRange()")
	}
	ch := make(chan errorableKV)
	done := make(chan struct{})
	defer close(done)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx == nil || !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, done, false)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, done, false)
		}
	}()

	// Consume the key-value pairs.
	for {
		result := <-ch
		if result.error != nil {
			return result.error
		}
		if result.KeyValue == nil {
			return nil
		}
		if op != nil && op.Wg != nil {
			op.Wg.Add(1)
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return err
		}
		tkv := storage.TKeyValue{tk, result.KeyValue.V}
		chunk := &storage.Chunk{op, &tkv}
		if err := f(chunk); err != nil {
			return err
		}
	}
}

// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
// retrieval like DVID-to-DVID communication and should not be used by data type
// implementations if possible.  A nil is sent down the channel when the
// range is complete.
func (db *LevelDB) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error {
	if db == nil {
		return fmt.Errorf("Can't call RawRangeQuery on nil LevelDB")
	}
	dvid.StartCgo()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
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
				storage.StoreValueBytesRead <- len(itValue)
			}
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, kEnd) > 0 {
				break
			}
			kv := storage.KeyValue{itKey, itValue}
			select {
			case out <- &kv:
			case <-cancel:
				return nil
			}
			//out <- &kv
			it.Next()
		} else {
			break
		}
	}
	out <- nil
	if err := it.GetError(); err != nil {
		return err
	}
	return nil
}

// ---- KeyValueSetter interface ------

// Put writes a value with given key.
func (db *LevelDB) Put(ctx storage.Context, tk storage.TKey, v []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call Put on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}
	wo := db.options.WriteOptions

	var err error
	key := ctx.ConstructKey(tk)
	if !ctx.Versioned() {
		dvid.StartCgo()
		err = db.ldb.Put(wo, key, v)
		dvid.StopCgo()
	} else {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Put(): %v", ctx)
		}
		tombstoneKey := vctx.TombstoneKey(tk)
		batch := db.NewBatch(vctx).(*goBatch)
		batch.WriteBatch.Delete(tombstoneKey)
		batch.WriteBatch.Put(key, v)
		if err = batch.Commit(); err != nil {
			dvid.Criticalf("Error on batch commit of Put: %v\n", err)
			err = fmt.Errorf("Error on batch commit of Put: %v", err)
		}
	}

	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)
	return err
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (db *LevelDB) RawPut(k storage.Key, v []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call RawPut on nil LevelDB")
	}
	wo := db.options.WriteOptions
	dvid.StartCgo()
	defer dvid.StopCgo()

	if err := db.ldb.Put(wo, k, v); err != nil {
		return err
	}

	storage.StoreKeyBytesWritten <- len(k)
	storage.StoreValueBytesWritten <- len(v)
	return nil
}

// Delete removes a value with given key.
func (db *LevelDB) Delete(ctx storage.Context, tk storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call Delete on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Delete()")
	}
	wo := db.options.WriteOptions

	var err error
	key := ctx.ConstructKey(tk)
	if !ctx.Versioned() {
		dvid.StartCgo()
		err = db.ldb.Delete(wo, key)
		dvid.StopCgo()
	} else {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Delete(): %v", ctx)
		}
		tombstoneKey := vctx.TombstoneKey(tk)
		batch := db.NewBatch(vctx).(*goBatch)
		batch.WriteBatch.Delete(key)
		batch.WriteBatch.Put(tombstoneKey, dvid.EmptyValue())
		if err = batch.Commit(); err != nil {
			dvid.Criticalf("Error on batch commit of Delete: %v\n", err)
			err = fmt.Errorf("Error on batch commit of Delete: %v", err)
		}
	}

	return err
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.  This can be used in conjunction with RawRangeQuery.
func (db *LevelDB) RawDelete(k storage.Key) error {
	if db == nil {
		return fmt.Errorf("Can't call RawDelete on nil LevelDB")
	}
	wo := db.options.WriteOptions
	dvid.StartCgo()
	defer dvid.StopCgo()
	return db.ldb.Delete(wo, k)
}

// ---- OrderedKeyValueSetter interface ------

// PutRange puts type key-value pairs that have been sorted in sequential key order.
// Current implementation in levigo driver simply does a batch write.
func (db *LevelDB) PutRange(ctx storage.Context, kvs []storage.TKeyValue) error {
	if db == nil {
		return fmt.Errorf("Can't call PutRange on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in PutRange()")
	}
	batch := db.NewBatch(ctx).(*goBatch)
	for _, kv := range kvs {
		batch.Put(kv.K, kv.V)
	}
	if err := batch.Commit(); err != nil {
		dvid.Criticalf("Error on batch commit of PutRange: %v\n", err)
		return err
	}
	return nil
}

// DeleteRange removes all key-value pairs with keys in the given range.
func (db *LevelDB) DeleteRange(ctx storage.Context, kStart, kEnd storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteRange on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}

	// For leveldb, we just iterate over keys in range and delete each one using batch.
	const BATCH_SIZE = 10000
	batch := db.NewBatch(ctx).(*goBatch)

	ch := make(chan errorableKV)
	done := make(chan struct{})
	defer close(done)

	// Run the keys-only range query in a goroutine.
	go func() {
		if ctx == nil || !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, done, true)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, done, true)
		}
	}()

	// Consume the key-value pairs.
	numKV := 0
	for {
		result := <-ch
		if result.KeyValue == nil {
			break
		}
		if result.error != nil {
			return result.error
		}

		// The key coming down channel is not index but full key, so no need to construct key using context.
		// If versioned, write a tombstone using current version id since we don't want to delete locked ancestors.
		// If unversioned, just delete.
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return err
		}
		batch.Delete(tk)

		if (numKV+1)%BATCH_SIZE == 0 {
			if err := batch.Commit(); err != nil {
				dvid.Criticalf("Error on batch commit of DeleteRange at key-value pair %d: %v\n", numKV, err)
				return fmt.Errorf("Error on batch commit of DeleteRange at key-value pair %d: %v\n", numKV, err)
			}
			batch = db.NewBatch(ctx).(*goBatch)
		}
		numKV++
	}
	if numKV%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			dvid.Criticalf("Error on last batch commit of DeleteRange: %v\n", err)
			return fmt.Errorf("Error on last batch commit of DeleteRange: %v\n", err)
		}
	}
	dvid.Debugf("Deleted %d key-value pairs via delete range for %s.\n", numKV, ctx)
	return nil
}

// DeleteAll deletes all key-value associated with a context (data instance and version).
func (db *LevelDB) DeleteAll(ctx storage.Context, allVersions bool) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteAll on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteAll()")
	}
	if allVersions {
		return db.deleteAllVersions(ctx)
	} else {
		vctx, versioned := ctx.(storage.VersionedCtx)
		if !versioned {
			return fmt.Errorf("Can't ask for versioned delete from unversioned context: %s", ctx)
		}
		return db.deleteSingleVersion(vctx)
	}
}

// ---- TKeyClassDeleter interface ------

func (db *LevelDB) DeleteTKeyClass(ctx storage.Context, tkc storage.TKeyClass, allVersions bool) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteTKeyClass() on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteTKeyClass()")
	}
	vctx, versioned := ctx.(storage.VersionedCtx)
	if !versioned {
		return fmt.Errorf("Can't call DeleteTKeyClass with an unversioned context: %s", ctx)
	}
	dvid.StartCgo()

	minTKey := storage.MinTKey(tkc)
	maxTKey := storage.MaxTKey(tkc)
	minKey, err := vctx.MinVersionKey(minTKey)
	if err != nil {
		return err
	}
	maxKey, err := vctx.MaxVersionKey(maxTKey)
	if err != nil {
		return err
	}

	const BATCH_SIZE = 10000
	batch := db.NewBatch(vctx).(*goBatch)

	timedLog := dvid.NewTimeLog()
	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()
	var numKV, numKVskipped uint64
	it.Seek(minKey)
	deleteVersion := vctx.VersionID()
	for {
		if err := it.GetError(); err != nil {
			return fmt.Errorf("Error iterating during DeleteTKeyClass for %s: %v", vctx, err)
		}
		if it.Valid() {
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, maxKey) > 0 {
				break
			}
			_, v, _, err := storage.DataKeyToLocalIDs(itKey)
			if err != nil {
				return fmt.Errorf("Error on DeleteTKeyClass: %v", err)
			}
			if allVersions || v == deleteVersion {
				batch.WriteBatch.Delete(itKey)
				if (numKV+1)%BATCH_SIZE == 0 {
					if err := batch.Commit(); err != nil {
						return fmt.Errorf("Error on batch commit of DeleteTKeyClass at key-value pair %d: %v", numKV, err)
					}
					batch = db.NewBatch(vctx).(*goBatch)
				}
				numKV++
			} else {
				numKVskipped++
			}

			if (numKV+numKVskipped)%BATCH_SIZE == 0 {
				timedLog.Debugf("Deleted %d of %d key-value pairs in ongoing DeleteTKeyClass for data %s", numKV, numKV+numKVskipped, vctx.Data().DataName())
			}

			it.Next()
		} else {
			break
		}
	}
	if numKV%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("Error on last batch commit of DeleteTKeyClass: %v", err)
		}
	}
	timedLog.Infof("Deleted %d of %d key-value pairs in DeleteTKeyClass for data %s", numKV, numKV+numKVskipped, vctx.Data().DataName())
	return nil
}

func (db *LevelDB) deleteSingleVersion(vctx storage.VersionedCtx) error {
	dvid.StartCgo()

	minTKey := storage.MinTKey(storage.TKeyMinClass)
	maxTKey := storage.MaxTKey(storage.TKeyMaxClass)
	minKey, err := vctx.MinVersionKey(minTKey)
	if err != nil {
		return err
	}
	maxKey, err := vctx.MaxVersionKey(maxTKey)
	if err != nil {
		return err
	}

	const BATCH_SIZE = 10000
	batch := db.NewBatch(vctx).(*goBatch)

	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	numKV := 0
	it.Seek(minKey)
	deleteVersion := vctx.VersionID()
	for {
		if err := it.GetError(); err != nil {
			return fmt.Errorf("Error iterating during DeleteAll for %s: %v", vctx, err)
		}
		if it.Valid() {
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, maxKey) > 0 {
				break
			}
			_, v, _, err := storage.DataKeyToLocalIDs(itKey)
			if err != nil {
				return fmt.Errorf("Error on DELETE ALL for version %d: %v", vctx.VersionID(), err)
			}
			if v == deleteVersion {
				batch.WriteBatch.Delete(itKey)
				if (numKV+1)%BATCH_SIZE == 0 {
					if err := batch.Commit(); err != nil {
						dvid.Criticalf("Error on batch commit of DeleteAll at key-value pair %d: %v\n", numKV, err)
						return fmt.Errorf("Error on batch commit of DeleteAll at key-value pair %d: %v", numKV, err)
					}
					batch = db.NewBatch(vctx).(*goBatch)
					dvid.Debugf("Deleted %d key-value pairs in ongoing DELETE ALL for %s.\n", numKV+1, vctx)
				}
				numKV++
			}

			it.Next()
		} else {
			break
		}
	}
	if numKV%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			dvid.Criticalf("Error on last batch commit of DeleteAll: %v\n", err)
			return fmt.Errorf("Error on last batch commit of DeleteAll: %v", err)
		}
	}
	dvid.Debugf("Deleted %d key-value pairs via DELETE ALL for %s.\n", numKV, vctx)
	return nil
}

func (db *LevelDB) deleteAllVersions(ctx storage.Context) error {
	dvid.StartCgo()

	var err error
	var minKey, maxKey storage.Key

	vctx, versioned := ctx.(storage.VersionedCtx)
	if versioned {
		// Don't have to worry about tombstones.  Delete all keys from all versions for this instance id.
		minTKey := storage.MinTKey(storage.TKeyMinClass)
		maxTKey := storage.MaxTKey(storage.TKeyMaxClass)
		minKey, err = vctx.MinVersionKey(minTKey)
		if err != nil {
			return err
		}
		maxKey, err = vctx.MaxVersionKey(maxTKey)
		if err != nil {
			return err
		}
	} else {
		minKey, maxKey = ctx.KeyRange()
	}

	const BATCH_SIZE = 10000
	batch := db.NewBatch(ctx).(*goBatch)

	ro := levigo.NewReadOptions()
	it := db.ldb.NewIterator(ro)
	defer func() {
		it.Close()
		dvid.StopCgo()
	}()

	numKV := 0
	it.Seek(minKey)
	for {
		if err := it.GetError(); err != nil {
			return fmt.Errorf("Error iterating during DeleteAll for %s: %v", ctx, err)
		}
		if it.Valid() {
			itKey := it.Key()
			storage.StoreKeyBytesRead <- len(itKey)
			// Did we pass the final key?
			if bytes.Compare(itKey, maxKey) > 0 {
				break
			}

			batch.WriteBatch.Delete(itKey)
			if (numKV+1)%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Criticalf("Error on batch commit of DeleteAll at key-value pair %d: %v\n", numKV, err)
					return fmt.Errorf("Error on batch commit of DeleteAll at key-value pair %d: %v", numKV, err)
				}
				batch = db.NewBatch(ctx).(*goBatch)
				dvid.Debugf("Deleted %d key-value pairs in ongoing DELETE ALL for %s.\n", numKV+1, ctx)
			}
			numKV++
			it.Next()
		} else {
			break
		}
	}
	if numKV%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			dvid.Criticalf("Error on last batch commit of DeleteAll: %v\n", err)
			return fmt.Errorf("Error on last batch commit of DeleteAll: %v", err)
		}
	}
	dvid.Debugf("Deleted %d key-value pairs via DELETE ALL for %s.\n", numKV, ctx)
	return nil
}

// --- Batcher interface ----

type goBatch struct {
	ctx  storage.Context
	vctx storage.VersionedCtx
	*levigo.WriteBatch
	wo  *levigo.WriteOptions
	ldb *levigo.DB
}

// NewBatch returns an implementation that allows batch writes
func (db *LevelDB) NewBatch(ctx storage.Context) storage.Batch {
	if db == nil {
		dvid.Criticalf("Can't call NewBatch on nil LevelDB\n")
		return nil
	}
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	var vctx storage.VersionedCtx
	var ok bool
	vctx, ok = ctx.(storage.VersionedCtx)
	if !ok {
		vctx = nil
	}
	return &goBatch{ctx, vctx, levigo.NewWriteBatch(), db.options.WriteOptions, db.ldb}
}

// --- Batch interface ---

func (batch *goBatch) Delete(tk storage.TKey) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Delete()\n")
		return
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	key := batch.ctx.ConstructKey(tk)
	if batch.vctx != nil {
		tombstone := batch.vctx.TombstoneKey(tk) // This will now have current version
		batch.WriteBatch.Put(tombstone, dvid.EmptyValue())
	}
	batch.WriteBatch.Delete(key)
}

func (batch *goBatch) Put(tk storage.TKey, v []byte) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Put()\n")
		return
	}
	dvid.StartCgo()
	defer dvid.StopCgo()

	key := batch.ctx.ConstructKey(tk)
	if batch.vctx != nil {
		tombstone := batch.vctx.TombstoneKey(tk) // This will now have current version
		batch.WriteBatch.Delete(tombstone)
	}
	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)
	batch.WriteBatch.Put(key, v)
}

func (batch *goBatch) Commit() error {
	if batch == nil {
		return fmt.Errorf("Received nil batch in batch.Commit()\n")
	}
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

// ---- SizeViewer interface ------

func (db *LevelDB) GetApproximateSizes(ranges []storage.KeyRange) ([]uint64, error) {
	lr := make([]levigo.Range, len(ranges))
	for i, kr := range ranges {
		lr[i] = levigo.Range{
			Start: []byte(kr.Start),
			Limit: []byte(kr.OpenEnd),
		}
	}
	sizes := db.ldb.GetApproximateSizes(lr)
	return sizes, nil
}

// ---- BlobStore interface ----

// PutBlob writes unversioned data and returns a filename-friendly base64 encoding of the reference.
func (db *LevelDB) PutBlob(v []byte) (ref string, err error) {
	if db == nil {
		return "", fmt.Errorf("Can't call PutBlob on nil LevelDB")
	}
	if db.options == nil {
		return "", fmt.Errorf("Can't call PutBlob on db with nil options: %v", db)
	}
	h := fnv.New128()
	if _, err = h.Write(v); err != nil {
		return
	}
	contentHash := h.Sum(nil)
	key := storage.ConstructBlobKey(contentHash)
	dvid.StartCgo()
	wo := db.options.WriteOptions
	err = db.ldb.Put(wo, key, v)
	dvid.StopCgo()

	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)

	b64key := base64.URLEncoding.EncodeToString(contentHash)
	return b64key, err
}

// GetBlob returns unversioned data given a reference.
func (db *LevelDB) GetBlob(ref string) (v []byte, err error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetBlob on nil LevelDB")
	}
	if db.options == nil {
		return nil, fmt.Errorf("Can't call GetBlob on db with nil options: %v", db)
	}
	var contentHash []byte
	if contentHash, err = base64.URLEncoding.DecodeString(ref); err != nil {
		return
	}
	key := storage.ConstructBlobKey(contentHash)
	ro := db.options.ReadOptions
	dvid.StartCgo()
	v, err = db.ldb.Get(ro, key)
	dvid.StopCgo()
	storage.StoreValueBytesRead <- len(v)
	return
}

// ---- Options ----

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
		dvid.TimeDebugf("Write buffer set to %d bytes.\n", nBytes)
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
		dvid.TimeDebugf("Block size set to %d bytes.\n", nBytes)
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
		dvid.TimeDebugf("LRU cache size set to %d bytes.\n", nBytes)
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
