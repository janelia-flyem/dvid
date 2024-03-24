//go:build badger
// +build badger

package badger

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/blang/semver"
	"github.com/dgraph-io/badger/v3"
	"github.com/twinj/uuid"
)

const (
	// DefaultValueThreshold is the size of values in bytes that if exceeded get stored in
	// value log instead of the LSM tree.  2022-03-26: Let Badger determine default because they might know better.
	// DefaultValueThreshold = 1 * dvid.Kilo

	// DefaultVersionsToKeep is the number of versions to keep per key.  Until we work out way
	// of reusing timestamp versioning for branched versioning, simply use the version-encoded key.
	DefaultVersionsToKeep = 1

	// DefaultSyncWrites is true if all writes are synced to disk, thereby making db resilient
	// at cost of speed.
	DefaultSyncWrites = false
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in badger: %v\n", err)
	}
	e := Engine{"badger", "BadgerDB", ver}
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

// NewStore returns a badger. The passed Config must contain "path" string.
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newDB(config)
}

func parseConfig(config dvid.StoreConfig) (path string, testing bool, err error) {
	c := config.GetAll()

	v, found := c["path"]
	if !found {
		err = fmt.Errorf("%q must be specified for BadgerDB configuration", "path")
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

// Periodically sync to prevent too many writes from being buffered
// if server crashes.
func syncPeriodically(db *BadgerDB) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-db.stopSyncCh:
			dvid.Infof("Stopping sync goroutine for badger @ %s\n", db.directory)
			return
		case <-ticker.C:
			db.bdp.Sync()
		}
	}
}

// newDB returns a Badger backend, creating one at path if it doesn't exist.
func (e Engine) newDB(config dvid.StoreConfig) (*BadgerDB, bool, error) {
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

	opts, err := getOptions(path, config.Config)
	if err != nil {
		return nil, false, err
	}
	opts.NumVersionsToKeep = 1
	opts.SyncWrites = false
	opts.ValueThreshold = 100

	badgerDB := &BadgerDB{
		directory:  path,
		config:     config,
		options:    opts,
		stopSyncCh: make(chan bool),
	}

	dvid.TimeInfof("Opening badger @ path %s\n", path)
	bdp, err := badger.Open(*opts)
	if err != nil {
		return nil, false, err
	}
	badgerDB.bdp = bdp

	go syncPeriodically(badgerDB)

	// if we know it's newly created, just return.
	if created {
		return badgerDB, created, nil
	}

	// otherwise, check if there's been any metadata or we need to initialize it.
	metadataExists, err := badgerDB.metadataExists()
	if err != nil {
		bdp.Close()
		return nil, false, err
	}

	return badgerDB, !metadataExists, nil
}

// ---- RepairableEngine interface not implemented ------

// ---- TestableEngine interface implementation -------

// AddTestConfig add this engine to be used for testing.
func (e Engine) AddTestConfig(backend *storage.Backend) (storage.Alias, error) {
	alias := storage.Alias("badger")
	if backend.DefaultKVDB == "" {
		backend.DefaultKVDB = alias
	}
	if backend.Metadata == "" {
		backend.Metadata = alias
	}
	if backend.Stores == nil {
		backend.Stores = make(map[storage.Alias]dvid.StoreConfig)
	}
	tc := map[string]interface{}{
		"path":    fmt.Sprintf("dvid-test-badger-%x", uuid.NewV4().Bytes()),
		"testing": true,
	}
	var c dvid.Config
	c.SetAll(tc)
	backend.Stores[alias] = dvid.StoreConfig{Config: c, Engine: "badger"}
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

func (db *BadgerDB) String() string {
	return fmt.Sprintf("badger @ %s", db.directory)
}

// --- The BadgerDB Implementation must satisfy a dvid.Store interface ----

type BadgerDB struct {
	// Directory of datastore
	directory string

	// Config at time of Open()
	config dvid.StoreConfig

	options *badger.Options
	bdp     *badger.DB

	// stopSyncCh is used to signal the sync goroutine to stop.
	stopSyncCh chan bool
}

// Close closes the BadgerDB
func (db *BadgerDB) Close() {
	if db != nil {
		if db.bdp != nil {
			db.stopSyncCh <- true
			db.bdp.Close()
			dvid.Infof("Closed Badger DB @ %s\n", db.directory)
		}
		db.bdp = nil
		db.options = nil
	}
}

// Equal returns true if the badger matches the given store configuration.
func (db *BadgerDB) Equal(config dvid.StoreConfig) bool {
	path, _, err := parseConfig(config)
	if err != nil {
		return false
	}
	return db.directory == path
}

// GetStoreConfig returns the configuration for this store.
func (db *BadgerDB) GetStoreConfig() dvid.StoreConfig {
	return db.config
}

func (db *BadgerDB) metadataExists() (bool, error) {
	var found bool
	db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // key only
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := storage.MetadataKeyPrefix()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			found = true
			return nil
		}
		return nil
	})
	return found, nil
}

// ---- KeyUsageViewer interface ------

func (db *BadgerDB) GetKeyUsage(ranges []storage.KeyRange) (hitsPerInstance []map[int]int, err error) {
	if db == nil {
		err = fmt.Errorf("can't call GetKeyUsage on nil BadgerDB")
		return
	}
	hitsPerInstance = make([]map[int]int, len(ranges))
	err = db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		dvid.Infof("Checking key usage for Badger @ %s ...\n", db.directory)
		for i, kr := range ranges {
			// Allocate histogram for this key range (i.e., a data instance)
			hitsPerInstance[i] = make(map[int]int)

			// Iterate and get all kv across versions for each key.
			maxVersionKey := storage.MaxVersionDataKeyFromKey(kr.Start)
			numVersions := 1
			for it.Seek(kr.Start); it.Valid(); it.Next() {
				kv := new(storage.KeyValue)
				item := it.Item()
				kv.K = item.KeyCopy(nil)
				storage.StoreKeyBytesRead <- len(kv.K)

				// Add version to the stats for this key.
				if bytes.Compare(kv.K, maxVersionKey) > 0 {
					maxVersionKey = storage.MaxVersionDataKeyFromKey(kv.K)
					hitsPerInstance[i][numVersions]++
					numVersions = 0
				}
				numVersions++

				// Did we pass the final key?
				if bytes.Compare(kv.K, kr.OpenEnd) > 0 {
					break
				}

			}
		}
		dvid.Infof("Key usage for Badger @ %s:\n  %v\n", db.directory, hitsPerInstance)
		return nil
	})
	return
}

// ---- KeyValueGetter interface ------

// Get returns a value given a key.
func (db *BadgerDB) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GET on nil BadgerDB")
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
		keys, err := db.getKeyVersions(vctx, tk)
		if err != nil {
			return nil, err
		}
		key, err := vctx.GetBestKeyVersion(keys)
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, nil
		}
		var value []byte
		err = db.bdp.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			value, err = item.ValueCopy(nil)
			return err
		})
		return value, err
	} else {
		key := []byte(ctx.ConstructKey(tk))
		var v []byte
		err := db.bdp.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			v, err = item.ValueCopy(nil)
			return err
		})
		storage.StoreValueBytesRead <- len(v)
		return v, err
	}
}

// Exists returns true if the key exists for exactly that version (not inherited).
func (db *BadgerDB) Exists(ctx storage.Context, tk storage.TKey) (exists bool, err error) {
	if db == nil {
		return false, fmt.Errorf("Can't call Exists() on nil BadgerDB")
	}
	if db.options == nil {
		return false, fmt.Errorf("Can't call Exists() on db with nil options: %v", db)
	}
	if ctx == nil {
		return false, fmt.Errorf("Received nil context in Exists()")
	}
	var key storage.Key
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return false, fmt.Errorf("Bad Exists(): context is versioned but doesn't fulfill interface: %v", ctx)
		}
		keys, err := db.getKeyVersions(vctx, tk)
		if err != nil {
			return false, err
		}
		key, err := vctx.GetBestKeyVersion(keys)
		if err != nil {
			return false, err
		}
		if key == nil {
			return false, nil
		}
		return true, nil
	} else {
		key = ctx.ConstructKey(tk)
		k := []byte(key)
		var retKey []byte
		db.bdp.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false // key only
			it := txn.NewIterator(opts)
			defer it.Close()
			it.Seek(k)
			if it.Valid() {
				item := it.Item()
				retKey = item.KeyCopy(nil)
			}
			return nil
		})
		return bytes.Equal(k, retKey), nil
	}
}

// getKeyVersions returns all versions of a key and could include a tombstone key.
func (db *BadgerDB) getKeyVersions(vctx storage.VersionedCtx, tk storage.TKey) ([]storage.Key, error) {
	dataKeyPrefix := vctx.UnversionedKeyPrefix(tk)
	var keys []storage.Key
	db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // key only
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(dataKeyPrefix); it.ValidForPrefix(dataKeyPrefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			storage.StoreKeyBytesRead <- len(key)
			keys = append(keys, key)
		}
		return nil
	})
	return keys, nil
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
func (db *BadgerDB) versionedRange(vctx storage.VersionedCtx, begTKey, endTKey storage.TKey, ch chan errorableKV, done <-chan struct{}, keysOnly bool) {
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
	maxVersionKey, err := vctx.MaxVersionKey(begTKey)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}

	values := []*storage.KeyValue{}
	err = db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		if keysOnly {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(minKey); it.Valid(); it.Next() {
			select {
			case <-done: // don't care about rest of data
				return nil
			default:
			}

			item := it.Item()
			kv := new(storage.KeyValue)
			kv.K = item.KeyCopy(nil)
			storage.StoreKeyBytesRead <- len(kv.K)

			// Did we pass all versions for last key read?
			if bytes.Compare(kv.K, maxVersionKey) > 0 {
				if storage.Key(kv.K).IsDataKey() {
					indexBytes, err := storage.TKeyFromKey(kv.K)
					if err != nil {
						return err
					}
					maxVersionKey, err = vctx.MaxVersionKey(indexBytes)
					if err != nil {
						return err
					}
				}
				sendKV(vctx, values, ch)
				values = []*storage.KeyValue{}
			}

			// Did we pass the final key?
			if bytes.Compare(kv.K, maxKey) > 0 {
				if len(values) > 0 {
					sendKV(vctx, values, ch)
				}
				return nil
			}
			if !keysOnly {
				var err error
				if kv.V, err = item.ValueCopy(nil); err != nil {
					return err
				}
				storage.StoreValueBytesRead <- len(kv.V)
			}
			values = append(values, kv)
		}
		if len(values) > 0 {
			sendKV(vctx, values, ch)
		}
		return nil
	})
	ch <- errorableKV{nil, err}
}

// unversionedRange sends a range of key-value pairs down a channel.
func (db *BadgerDB) unversionedRange(ctx storage.Context, begTKey, endTKey storage.TKey, ch chan errorableKV, done <-chan struct{}, keysOnly bool) {
	begKey := ctx.ConstructKey(begTKey)
	endKey := ctx.ConstructKey(endTKey)

	err := db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		if keysOnly {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(begKey); it.Valid(); it.Next() {
			item := it.Item()
			kv := new(storage.KeyValue)
			kv.K = item.KeyCopy(nil)
			storage.StoreKeyBytesRead <- len(kv.K)
			if bytes.Compare(item.Key(), endKey) > 0 {
				break
			}
			if !keysOnly {
				var err error
				if kv.V, err = item.ValueCopy(nil); err != nil {
					return err
				}
				storage.StoreValueBytesRead <- len(kv.V)
			}
			select {
			case <-done:
				return nil
			case ch <- errorableKV{kv, nil}:
				// continue
			}
		}
		return nil
	})
	ch <- errorableKV{nil, err}
	return
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
func (db *BadgerDB) KeysInRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]storage.TKey, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call KeysInRange on nil BadgerDB")
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
func (db *BadgerDB) SendKeysInRange(ctx storage.Context, kStart, kEnd storage.TKey, kch storage.KeyChan) error {
	if db == nil {
		return fmt.Errorf("Can't call SendKeysInRange on nil BadgerDB")
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
func (db *BadgerDB) GetRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]*storage.TKeyValue, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetRange on nil BadgerDB")
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
		tkv := storage.TKeyValue{K: tk, V: result.KeyValue.V}
		values = append(values, &tkv)
	}
}

// ProcessRange sends a range of key-value pairs to chunk handlers.  If the keys are versioned,
// only key-value pairs for kStart's version will be transmitted.  If f returns an error, the
// function is immediately terminated and returns an error.
func (db *BadgerDB) ProcessRange(ctx storage.Context, kStart, kEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if db == nil {
		return fmt.Errorf("Can't call ProcessRange on nil BadgerDB")
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
		tkv := storage.TKeyValue{K: tk, V: result.KeyValue.V}
		chunk := &storage.Chunk{ChunkOp: op, TKeyValue: &tkv}
		if err := f(chunk); err != nil {
			return err
		}
	}
}

// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
// retrieval like DVID-to-DVID communication and should not be used by data type
// implementations if possible.  A nil is sent down the channel when the
// range is complete.
func (db *BadgerDB) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error {
	if db == nil {
		return fmt.Errorf("Can't call RawRangeQuery on nil BadgerDB")
	}
	err := db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		if keysOnly {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(kStart); it.Valid(); it.Next() {
			kv := new(storage.KeyValue)
			item := it.Item()
			kv.K = item.KeyCopy(nil)
			storage.StoreKeyBytesRead <- len(kv.K)
			// Did we pass the final key?
			if bytes.Compare(kv.K, kEnd) > 0 {
				break
			}
			if !keysOnly {
				var err error
				if kv.V, err = item.ValueCopy(nil); err != nil {
					return err
				}
				storage.StoreValueBytesRead <- len(kv.V)
			}
			select {
			case out <- kv:
			case <-cancel:
				return nil
			}
		}
		out <- nil
		return nil
	})
	return err
}

// ---- KeyValueSetter interface ------

// Put writes a value with given key.
func (db *BadgerDB) Put(ctx storage.Context, tk storage.TKey, v []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call Put on nil BadgerDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}

	var err error
	key := ctx.ConstructKey(tk)
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Put(): %v", ctx)
		}
		tombstoneKey := vctx.TombstoneKey(tk)
		err = db.bdp.Update(func(txn *badger.Txn) error {
			if err := txn.Set(key, v); err != nil {
				return err
			}
			if err := txn.Delete(tombstoneKey); err != nil {
				return err
			}
			return nil
		})
	} else {
		err = db.bdp.Update(func(txn *badger.Txn) error {
			return txn.Set(key, v)
		})
	}
	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)
	return err
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (db *BadgerDB) RawPut(k storage.Key, v []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call RawPut on nil BadgerDB")
	}
	err := db.bdp.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
	if err != nil {
		return err
	}

	storage.StoreKeyBytesWritten <- len(k)
	storage.StoreValueBytesWritten <- len(v)
	return nil
}

// Delete removes a value with given key.
func (db *BadgerDB) Delete(ctx storage.Context, tk storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call Delete on nil BadgerDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Delete()")
	}
	var err error
	key := ctx.ConstructKey(tk)
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Put(): %v", ctx)
		}
		tombstoneKey := vctx.TombstoneKey(tk)
		err = db.bdp.Update(func(txn *badger.Txn) error {
			if err := txn.Delete(key); err != nil {
				return err
			}
			if err := txn.Set(tombstoneKey, dvid.EmptyValue()); err != nil {
				return err
			}
			return nil
		})
	} else {
		err = db.bdp.Update(func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
	}
	return err
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.  This can be used in conjunction with RawRangeQuery.
func (db *BadgerDB) RawDelete(k storage.Key) error {
	if db == nil {
		return fmt.Errorf("Can't call RawDelete on nil BadgerDB")
	}
	return db.bdp.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

// ---- OrderedKeyValueSetter interface ------

// PutRange puts type key-value pairs that have been sorted in sequential key order.
func (db *BadgerDB) PutRange(ctx storage.Context, kvs []storage.TKeyValue) error {
	if db == nil {
		return fmt.Errorf("Can't call PutRange on nil BadgerDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in PutRange()")
	}
	wb := db.NewBatch(ctx)
	for _, kv := range kvs {
		wb.Put(kv.K, kv.V)
	}
	return wb.Commit()
}

// DeleteRange removes all key-value pairs with keys in the given range.
func (db *BadgerDB) DeleteRange(ctx storage.Context, kStart, kEnd storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteRange on nil BadgerDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}

	// For badger, we just iterate over keys in range and delete each one using batch.
	const BATCH_SIZE = 1000
	wb := db.NewBatch(ctx).(*goBatch)

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

		// The key coming down channel is not index but full key, so no need to construct key
		// using context.  If versioned, write a tombstone using current version id since we
		// don't want to delete locked ancestors.  If unversioned, just delete.
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return err
		}
		wb.Delete(tk)

		if (numKV+1)%BATCH_SIZE == 0 {
			if err := wb.Commit(); err != nil {
				dvid.Criticalf("Error on flush of DeleteRange at key-value pair %d: %v\n", numKV, err)
				return fmt.Errorf("Error on flush of DeleteRange at key-value pair %d: %v\n", numKV, err)
			}
			wb = db.NewBatch(ctx).(*goBatch)
		}
		numKV++
	}
	if numKV%BATCH_SIZE != 0 {
		if err := wb.Commit(); err != nil {
			dvid.Criticalf("Error on last flush of DeleteRange: %v\n", err)
			return fmt.Errorf("Error on last flush of DeleteRange: %v\n", err)
		}
	}
	dvid.Debugf("Deleted %d key-value pairs via delete range for %s.\n", numKV, ctx)
	return nil
}

// DeleteAll deletes all key-value associated with a context (data instance and version).
func (db *BadgerDB) DeleteAll(ctx storage.Context) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteAll on nil BadgerDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteAll()")
	}

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
	numKV := 0

	wb := db.bdp.NewWriteBatch()
	defer wb.Cancel()

	err = db.bdp.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(minKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			storage.StoreKeyBytesRead <- len(k)
			// Did we pass the final key?
			if bytes.Compare(k, maxKey) > 0 {
				break
			}
			wb.Delete(k)
			if (numKV+1)%BATCH_SIZE == 0 {
				if err := wb.Flush(); err != nil {
					dvid.Criticalf("Error on flush of DeleteAll at key-value pair %d: %v\n", numKV, err)
					return fmt.Errorf("Error on flush of DeleteAll at key-value pair %d: %v", numKV, err)
				}
				wb = db.bdp.NewWriteBatch()
				dvid.Debugf("Deleted %d key-value pairs in ongoing DELETE ALL for %s.\n", numKV+1, vctx)
			}
			numKV++
		}
		if numKV%BATCH_SIZE != 0 {
			if err := wb.Flush(); err != nil {
				dvid.Criticalf("Error on last flush of DeleteAll: %v\n", err)
				return fmt.Errorf("Error on last flush of DeleteAll: %v", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	dvid.Debugf("Deleted %d key-value pairs via DELETE ALL for %s.\n", numKV, vctx)
	return nil
}

// --- Batcher interface ----

type goBatch struct {
	ctx  storage.Context
	vctx storage.VersionedCtx
	*badger.WriteBatch
}

// NewBatch returns an implementation that allows batch writes
func (db *BadgerDB) NewBatch(ctx storage.Context) storage.Batch {
	if db == nil {
		dvid.Criticalf("Can't call NewBatch on nil BadgerDB\n")
		return nil
	}
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}

	var vctx storage.VersionedCtx
	var ok bool
	vctx, ok = ctx.(storage.VersionedCtx)
	if !ok {
		vctx = nil
	}
	return &goBatch{ctx, vctx, db.bdp.NewWriteBatch()}
}

// --- Batch interface ---

func (batch *goBatch) Delete(tk storage.TKey) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Delete()\n")
		return
	}

	key := batch.ctx.ConstructKey(tk)
	if batch.vctx != nil {
		tombstone := batch.vctx.TombstoneKey(tk) // This will now have current version
		batch.WriteBatch.Set(tombstone, dvid.EmptyValue())
	}
	// fmt.Printf("Batch delete of key %v\n", key)
	if err := batch.WriteBatch.Delete(key); err != nil {
		dvid.Criticalf("unable to delete key %v: %v\n", key, err)
	}
}

func (batch *goBatch) Put(tk storage.TKey, v []byte) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Put()\n")
		return
	}

	key := batch.ctx.ConstructKey(tk)
	if batch.vctx != nil {
		tombstone := batch.vctx.TombstoneKey(tk) // This will now have current version
		batch.WriteBatch.Delete(tombstone)
	}
	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)
	if err := batch.WriteBatch.Set(key, v); err != nil {
		dvid.Criticalf("unable to write key-value with key %v, value %d bytes: %v\n", key, len(v), err)
	}
}

func (batch *goBatch) Commit() error {
	if batch == nil {
		return fmt.Errorf("Received nil batch in batch.Commit()\n")
	}
	// fmt.Printf("WriteBatch is being flushed in commit...\n")
	if err := batch.WriteBatch.Flush(); err != nil {
		return err
	}
	batch.WriteBatch.Cancel()
	return nil
}

// TODO: Consider contributing to Badger the below function (also in RocksDB) or modifying their
// db.PrintHistogram() to return data that could use a key range instead of a prefix.

// // ---- SizeViewer interface ------

// func (db *BadgerDB) GetApproximateSizes(ranges []storage.KeyRange) ([]uint64, error) {
// 	lr := make([]badger.Range, len(ranges))
// 	for i, kr := range ranges {
// 		lr[i] = badger.Range{
// 			Start: []byte(kr.Start),
// 			Limit: []byte(kr.OpenEnd),
// 		}
// 	}
// 	sizes := db.bdp.GetApproximateSizes(lr)
// 	return sizes, nil
// }

// ---- BlobStore interface ----

// PutBlob writes unversioned data and returns a filename-friendly base64 encoding of the reference.
func (db *BadgerDB) PutBlob(v []byte) (ref string, err error) {
	if db == nil {
		return "", fmt.Errorf("Can't call PutBlob on nil BadgerDB")
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

	err = db.bdp.Update(func(txn *badger.Txn) error {
		return txn.Set(key, v)
	})
	if err != nil {
		return "", err
	}

	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(v)

	b64key := base64.URLEncoding.EncodeToString(contentHash)
	return b64key, err
}

// GetBlob returns unversioned data given a reference.
func (db *BadgerDB) GetBlob(ref string) (v []byte, err error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetBlob on nil BadgerDB")
	}
	if db.options == nil {
		return nil, fmt.Errorf("Can't call GetBlob on db with nil options: %v", db)
	}
	var contentHash []byte
	if contentHash, err = base64.URLEncoding.DecodeString(ref); err != nil {
		return
	}
	key := storage.ConstructBlobKey(contentHash)

	err = db.bdp.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, err = item.ValueCopy(nil)
		return err
	})

	storage.StoreValueBytesRead <- len(v)
	return
}
