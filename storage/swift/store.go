// +build swift

package swift

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/ncw/swift"
)

const (
	// The maximum number of write operations sent to Swift in parallel.
	maxConcurrentOperations = 10

	// The initial delay upon a failure.
	initialDelay = 50 * time.Millisecond

	// The maximum delay after which we give up and an error is returned.
	maximumDelay = 20 * time.Minute
)

// rateLimit is a buffered channel used to limit the number of concurrent
// write operations sent to Swift.
var rateLimit = make(chan struct{}, maxConcurrentOperations)

// Store implements dvid.Store as an Openstack Swift store.
type Store struct {
	// The Swift container name.
	container string

	// The Swift connection.
	conn *swift.Connection

	// Locks held by batch commits.
	batchLocks         map[string]int // Maps Swift object names to number of locks held.
	batchLocksReleased chan struct{}  // One-time-use channel which signals the release of locks of a batch.
	batchLocksMutex    sync.Mutex     // Synchronize access to the lock map and channel.
}

func (s *Store) String() string {
	return fmt.Sprintf(`Openstack Swift store, user "%s", container "%s"`, s.conn.UserName, s.container)
}

// Close closes the store.
func (s *Store) Close() {
	// Nothing to close.
}

// Equal returns true if this store matches the given store configuration.
func (s *Store) Equal(config dvid.StoreConfig) bool {
	for param, value := range map[string]string{
		"user":      s.conn.UserName,
		"key":       s.conn.ApiKey,
		"auth":      s.conn.AuthUrl,
		"project":   s.conn.Tenant,
		"domain":    s.conn.TenantDomain,
		"container": s.container,
	} {
		v, ok, err := config.GetString(param)
		if !ok || err != nil || v != value {
			return false
		}
	}
	return true
}

// NewStore returns a new Swift store.
func NewStore(config dvid.StoreConfig) (*Store, bool, error) {
	// Make new store.
	s := &Store{
		conn:       &swift.Connection{},
		batchLocks: make(map[string]int),
	}

	// Get configuration values.
	var err error
	configString := func(param string, required bool) (string, error) {
		value, ok, e := config.GetString(param)
		if !ok {
			if required {
				return "", fmt.Errorf(`Configuration parameter "%s" missing`, param)
			}
			return "", nil
		}
		if e != nil {
			return "", fmt.Errorf(`Error retrieving configuration parameter "%s" (may not be a string): %s`, param, e)
		}
		if value == "" {
			return "", fmt.Errorf(`Configuration parameter "%s" must not be empty`, param)
		}
		return value, nil
	}
	s.conn.UserName, err = configString("user", true)
	if err != nil {
		return nil, false, err
	}
	s.conn.ApiKey, err = configString("key", true)
	if err != nil {
		return nil, false, err
	}
	s.conn.AuthUrl, err = configString("auth", true)
	if err != nil {
		return nil, false, err
	}
	s.conn.Tenant, err = configString("project", true)
	if err != nil {
		return nil, false, err
	}
	if s.conn.Tenant != "" {
		s.conn.AuthVersion = 3
	}
	s.conn.TenantDomain, err = configString("domain", true)
	if err != nil {
		return nil, false, err
	}
	s.container, err = configString("container", true)
	if err != nil {
		return nil, false, err
	}

	// Authenticate with Swift.
	if err := s.conn.Authenticate(); err != nil {
		return nil, false, fmt.Errorf(`Unable to authenticate with the Swift database: %s`, err)
	}
	dvid.Infof("Successfully authenticated to Openstack Swift with user \"%s\", container \"%s\" via %s\n", s.conn.UserName, s.container, s.conn.AuthUrl)

	// Check if container exists.
	_, _, err = s.conn.Container(s.container)
	if err == swift.ContainerNotFound {
		// Create a new container.
		if err = s.conn.ContainerCreate(s.container, nil); err != nil {
			return nil, false, fmt.Errorf(`Cannot create Swift container "%s": %s`, s.container, err)
		}
		dvid.Infof("Created new container \"%s\"\n", s.container)
	} else if err != nil {
		return nil, false, fmt.Errorf(`Unable to check if Swift container "%s" exists: %s`, s.container, err)
	}

	// Check if we already have metadata.
	var context storage.MetadataContext
	from, to := context.KeyRange()
	keys, err := s.objectNames(from, to)
	if err != nil {
		return nil, false, fmt.Errorf(`Unable to check for metadata objects: %s`, err)
	}

	return s, len(keys) == 0, nil
}

// objectNames queries Swift for a range of keys and returns the found keys.
func (s *Store) objectNames(from, to storage.Key) (keys []storage.Key, err error) {
	// A fix for ncw/swift is needed to make this function 100% correct. See
	// https://github.com/ncw/swift/issues/113 for details. But even without the
	// fix, we don't expect major errors as the default limit is 10,000 results.

	marker := encodeKey(from)
	endMarker := encodeKey(to)

	// Find starting object name as it's not included in the range request.
	_, _, err = s.conn.Object(s.container, marker)
	if err == nil {
		keys = append(keys, from)
	} else if err != swift.ObjectNotFound {
		return nil, err
	}

	// Run range request.
	var names []string
	names, err = s.conn.ObjectNames(s.container, &swift.ObjectsOpts{
		Marker:    marker,
		EndMarker: endMarker,
	})
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		key := decodeKey(name)
		if key == nil {
			continue
		}
		keys = append(keys, key)
	}

	// Find end object name as it's not included in the range request.
	_, _, err = s.conn.Object(s.container, endMarker)
	if err == nil {
		keys = append(keys, to)
	} else if err != swift.ObjectNotFound {
		return nil, err
	}

	err = nil
	return
}

// getObject retrieves an object for the given key, retrying a few times if
// there is an error. If the object does not exist, nil is returned.
func (s *Store) getObject(key storage.Key) ([]byte, error) {
	delay := initialDelay

	for {
		// Attempt to get the object.
		rateLimit <- struct{}{}
		contents, err := s.conn.ObjectGetBytes(s.container, encodeKey(key))
		<-rateLimit
		storage.StoreValueBytesRead <- len(contents)
		if err == swift.ObjectNotFound {
			return nil, nil
		} else if err == nil {
			if len(contents) == 0 {
				return []byte{}, nil
			} else {
				return contents, nil
			}
		}

		// There was an error. Retry with increasing delays.
		if delay > maximumDelay {
			return nil, fmt.Errorf(`Maximum object download retries exceeded: %s`, err)
		}
		time.Sleep(delay)
		delay *= 2
	}
}

// objectExists returns true if an object for the given key, retrying a few times if
// there is an error. This returns info about the object, which is presumably faster
// than actually reading the object.
func (s *Store) objectExists(key storage.Key) (bool, error) {
	delay := initialDelay

	for {
		// Attempt to get the object info
		rateLimit <- struct{}{}
		_, _, err := s.conn.Object(s.container, encodeKey(key))
		<-rateLimit
		if err == swift.ObjectNotFound {
			return false, nil
		} else if err == nil {
			return true, nil
		}

		// There was an error. Retry with increasing delays.
		if delay > maximumDelay {
			return false, fmt.Errorf(`Maximum object info download retries exceeded: %s`, err)
		}
		time.Sleep(delay)
		delay *= 2
	}
}

/*********** KeyValueGetter interface ***********/

// Get returns a value given a key.
func (s *Store) Get(context storage.Context, key storage.TKey) ([]byte, error) {
	var accessKey storage.Key
	if context.Versioned() {
		versionedContext, ok := context.(storage.VersionedCtx)
		if !ok {
			return nil, errors.New("Context is marked as versioned but isn't actually versioned")
		}

		// Determine the right key from a range of keys.
		startKey, err := versionedContext.MinVersionKey(key)
		if err != nil {
			return nil, err
		}
		endKey, err := versionedContext.MaxVersionKey(key)
		if err != nil {
			return nil, err
		}
		keys, err := s.objectNames(startKey, endKey)
		if err != nil {
			return nil, fmt.Errorf(`Unable to list Swift object names: %s`, err)
		}
		var keyValues []*storage.KeyValue
		for _, key := range keys {
			keyValues = append(keyValues, &storage.KeyValue{K: key})
		}
		keyValue, err := versionedContext.VersionedKeyValue(keyValues)
		if err != nil {
			return nil, fmt.Errorf(`Unable to determine correct version key from a list: %s`, err)
		} else if keyValue == nil {
			return nil, nil
		}
		accessKey = keyValue.K
	} else {
		// Context is unversioned.
		accessKey = context.ConstructKey(key)
	}

	// Load the object.
	return s.getObject(accessKey)
}

// Exists returns true if a key exists.
func (s *Store) Exists(context storage.Context, key storage.TKey) (bool, error) {
	var accessKey storage.Key
	if context.Versioned() {
		versionedContext, ok := context.(storage.VersionedCtx)
		if !ok {
			return false, errors.New("Context is marked as versioned but isn't actually versioned")
		}

		// Determine the right key from a range of keys.
		startKey, err := versionedContext.MinVersionKey(key)
		if err != nil {
			return false, err
		}
		endKey, err := versionedContext.MaxVersionKey(key)
		if err != nil {
			return false, err
		}
		keys, err := s.objectNames(startKey, endKey)
		if err != nil {
			return false, fmt.Errorf(`Unable to list Swift object names: %s`, err)
		}
		var keyValues []*storage.KeyValue
		for _, key := range keys {
			keyValues = append(keyValues, &storage.KeyValue{K: key})
		}
		keyValue, err := versionedContext.VersionedKeyValue(keyValues)
		if err != nil {
			return false, fmt.Errorf(`Unable to determine correct version key from a list: %s`, err)
		} else if keyValue == nil {
			return false, nil
		}
		accessKey = keyValue.K
	} else {
		// Context is unversioned.
		accessKey = context.ConstructKey(key)
	}
	return s.objectExists(accessKey)
}

/*********** KeyValueSetter interface ***********/

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (s *Store) RawPut(key storage.Key, value []byte) error {
	delay := initialDelay

	for {
		rateLimit <- struct{}{}
		err := s.conn.ObjectPutBytes(s.container, encodeKey(key), value, "application/octet-stream")
		<-rateLimit
		if err == nil {
			storage.StoreValueBytesWritten <- len(value)
			return nil
		}

		// There was an error. Retry with increasing delays.
		if delay > maximumDelay {
			return fmt.Errorf(`Maximum object deletion retries exceeded: %s`, err)
		}
		time.Sleep(delay)
		delay *= 2
	}

	return nil
}

// RawDelete is a low-level function.  It deletes a key-value pair using full
// keys without any context. This can be used in conjunction with RawRangeQuery.
func (s *Store) RawDelete(key storage.Key) error {
	delay := initialDelay

	for {
		rateLimit <- struct{}{}
		err := s.conn.ObjectDelete(s.container, encodeKey(key))
		<-rateLimit
		if err == nil || err == swift.ObjectNotFound {
			return nil
		}

		// There was an error. Retry with increasing delays.
		if delay > maximumDelay {
			return fmt.Errorf(`Maximum object deletion retries exceeded: %s`, err)
		}
		time.Sleep(delay)
		delay *= 2
	}

	return nil
}

// Put writes a value with given key in a possibly versioned context.
func (s *Store) Put(context storage.Context, typeKey storage.TKey, value []byte) error {
	key := context.ConstructKey(typeKey)

	// In a versioned context, we delete the tombstone.
	if context.Versioned() {
		versionedContext, ok := context.(storage.VersionedCtx)
		if !ok {
			return errors.New("Context is marked as versioned but isn't actually versioned")
		}

		// Delete tombstone.
		tombstone := versionedContext.TombstoneKey(typeKey)
		err := s.RawDelete(tombstone)
		if err != nil {
			return fmt.Errorf(`Could not delete tombstone object: %s`, err)
		}
	}

	// Save object.
	return s.RawPut(key, value)
}

// Delete deletes a key-value pair so that subsequent Get on the key returns
// nil.
func (s *Store) Delete(context storage.Context, typeKey storage.TKey) error {
	key := context.ConstructKey(typeKey)

	// Delete the given key.
	if err := s.RawDelete(key); err != nil {
		return fmt.Errorf(`Coult not delete key: %s`, err)
	}

	// If the context is unversioned, we're done.
	if !context.Versioned() {
		return nil
	}

	// In a versioned context, we insert a tombstone.
	versionedContext, ok := context.(storage.VersionedCtx)
	if !ok {
		return errors.New("Context is marked as versioned but isn't actually versioned")
	}
	tombstone := versionedContext.TombstoneKey(typeKey)
	return s.RawPut(tombstone, dvid.EmptyValue())
}

/*********** OrderedKeyValueGetter interface ***********/

// keyRange returns a range of (non-type) keys to be found for the given
// type-key range.
func (s *Store) keyRange(context storage.Context, kStart, kEnd storage.TKey) (k []storage.Key, e error) {
	if !context.Versioned() {
		// Get all unversioned keys in the specified range.
		from := context.ConstructKey(kStart)
		to := context.ConstructKey(kEnd)
		return s.objectNames(from, to)
	} else {
		// For each key in the specified range, get the latest version.
		versionedContext, ok := context.(storage.VersionedCtx)
		if !ok {
			return nil, errors.New("Context is marked as versioned but isn't actually versioned")
		}
		from, err := versionedContext.MinVersionKey(kStart)
		if err != nil {
			return nil, fmt.Errorf("Unable to extract minimum key: %s", err)
		}
		to, err := versionedContext.MaxVersionKey(kEnd)
		if err != nil {
			return nil, fmt.Errorf("Unable to extract maximum key: %s", err)
		}
		endBlock, err := versionedContext.MaxVersionKey(kStart)
		if err != nil {
			return nil, fmt.Errorf("Unable to extract block end key: %s", err)
		}
		keys, err := s.objectNames(from, to)
		if err != nil {
			return nil, err
		}
		var versionKeys []*storage.KeyValue
		for _, key := range keys {
			if bytes.Compare(key, endBlock) > 0 {
				// We're past the block.
				if len(versionKeys) > 0 {
					// Extract the correct version.
					keyValue, err := versionedContext.VersionedKeyValue(versionKeys)
					if err != nil {
						return nil, fmt.Errorf("Unable to extract correct version from keys: %s", err)
					}

					// Store it.
					if keyValue != nil {
						k = append(k, keyValue.K)
					}

					// Reset block.
					versionKeys = versionKeys[:0]
				}

				// Determine new block end.
				typeKey, err := storage.TKeyFromKey(key)
				if err != nil {
					return nil, fmt.Errorf("Unable to extract type key from key: %s", err)
				}
				endBlock, err = versionedContext.MaxVersionKey(typeKey)
				if err != nil {
					return nil, fmt.Errorf("Unable to extract new block end key: %s", err)
				}
			}

			// Add this key to the block.
			versionKeys = append(versionKeys, &storage.KeyValue{K: key})
		}

		// Process remaining block.
		if len(versionKeys) > 0 {
			// Extract the correct version.
			keyValue, err := versionedContext.VersionedKeyValue(versionKeys)
			if err != nil {
				return nil, fmt.Errorf("Unable to extract correct version from keys: %s", err)
			}

			// Store it.
			if keyValue != nil {
				k = append(k, keyValue.K)
			}
		}

		return
	}
}

// KeysInRange returns a range of type-specific key components spanning (kStart,
// kEnd).
func (s *Store) KeysInRange(context storage.Context, kStart, kEnd storage.TKey) (typeKeys []storage.TKey, e error) {
	// Get the keys and convert them to type keys.
	keys, err := s.keyRange(context, kStart, kEnd)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		typeKey, err := storage.TKeyFromKey(key)
		if err != nil {
			return nil, fmt.Errorf("Unable to extract type key from key: %s", err)
		}
		typeKeys = append(typeKeys, typeKey)
	}
	return
}

// SendKeysInRange sends a range of keys down a key channel.
func (s *Store) SendKeysInRange(context storage.Context, kStart, kEnd storage.TKey, ch storage.KeyChan) error {
	keys, err := s.keyRange(context, kStart, kEnd)
	if err != nil {
		return err
	}

	for _, key := range keys {
		ch <- key
	}

	return nil
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.
func (s *Store) GetRange(context storage.Context, kStart, kEnd storage.TKey) (keyValues []*storage.TKeyValue, e error) {
	keys, err := s.keyRange(context, kStart, kEnd)
	if err != nil {
		return nil, err
	}

	// Start workers that fetch these keys' objects.
	var (
		wg    sync.WaitGroup
		mutex sync.Mutex
	)
	ch := make(chan storage.Key)
	for index := 0; index < maxConcurrentOperations; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range ch {
				// Load object.
				value, err := s.getObject(key)

				mutex.Lock()
				if err != nil {
					e = err
				} else {
					// Store in slice.
					var typeKey storage.TKey
					typeKey, err = storage.TKeyFromKey(key)
					if err != nil {
						e = err
					} else {
						keyValues = append(keyValues, &storage.TKeyValue{K: typeKey, V: value})
					}
				}
				mutex.Unlock()

				// Stop if there was an error.
				if err != nil {
					return
				}
			}
		}()
	}

	// Create jobs for workers.
	for _, key := range keys {
		ch <- key

		mutex.Lock()
		err := e
		mutex.Unlock()
		if err != nil {
			break // One of the workers had an error. Stop.
		}
	}
	close(ch)
	wg.Wait()

	return
}

// ProcessRange sends a range of type key-value pairs to type-specific chunk
// handlers, allowing chunk processing to be concurrent with key-value
// sequential reads.
func (s *Store) ProcessRange(context storage.Context, kStart, kEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	keys, err := s.keyRange(context, kStart, kEnd)
	if err != nil {
		return err
	}

	// Load objects for these keys and send them to the chunk processing function.
	for _, key := range keys {
		value, err := s.getObject(key)
		if err != nil {
			return fmt.Errorf("Could not read Swift object: %s", err)
		}
		storage.StoreValueBytesRead <- len(value)
		if op != nil && op.Wg != nil {
			op.Wg.Add(1)
		}
		typeKey, err := storage.TKeyFromKey(key)
		if err != nil {
			return fmt.Errorf("Unable to extract type key from key: %s", err)
		}
		typeKeyValue := &storage.TKeyValue{K: typeKey, V: value}
		if err := f(&storage.Chunk{ChunkOp: op, TKeyValue: typeKeyValue}); err != nil {
			return err
		}
	}

	return nil
}

// RawRangeQuery sends a range of full keys.
func (s *Store) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error {
	// Get the object names for this range.
	keys, err := s.objectNames(kStart, kEnd)
	if err != nil {
		return fmt.Errorf("Unable to load object names for range: %s", err)
	}

	// Load objects for these names and send them over the channel.
	for _, key := range keys {
		var value []byte
		if !keysOnly {
			value, err = s.getObject(key)
			if err != nil {
				return fmt.Errorf("Could not read Swift object: %s", err)
			}
			storage.StoreValueBytesRead <- len(value)
		}
		select {
		case out <- &storage.KeyValue{K: key, V: value}:
		case <-cancel:
			return nil
		}
	}

	return nil
}

/*********** OrderedKeyValueSetter interface ***********/

// Put key-value pairs.
func (s *Store) PutRange(context storage.Context, typeKeyValues []storage.TKeyValue) (e error) {
	// Parallelize storing these values.
	var (
		wg    sync.WaitGroup
		mutex sync.RWMutex
	)

	jobs := make(chan storage.TKeyValue)
	for i := 0; i < maxConcurrentOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := s.Put(context, job.K, job.V); err != nil {
					mutex.Lock()
					e = err
					mutex.Unlock()
				}
			}
		}()
	}

	for _, job := range typeKeyValues {
		jobs <- job
		mutex.RLock()
		err := e
		mutex.RUnlock()
		if err != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	return
}

// DeleteRange removes all key-value pairs with keys in the given range.
func (s *Store) DeleteRange(context storage.Context, kStart, kEnd storage.TKey) (e error) {
	// Find all keys for this range.
	keys, err := s.keyRange(context, kStart, kEnd)
	if err != nil {
		return fmt.Errorf(`Unable to determine deletion key range: %s`, err)
	}

	// Parallelize deleting these keys.
	var (
		wg    sync.WaitGroup
		mutex sync.RWMutex
	)

	jobs := make(chan storage.Key)
	for i := 0; i < maxConcurrentOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				if err := s.RawDelete(key); err != nil {
					mutex.Lock()
					e = err
					mutex.Unlock()
				}
			}
		}()
	}

	for _, key := range keys {
		jobs <- key
		mutex.RLock()
		err := e
		mutex.RUnlock()
		if err != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	return
}

// DeleteAll removes all key-value pairs for the context.
func (s *Store) DeleteAll(context storage.Context) error {
	allVersions := true // DeleteAll was modified to not allow specific version removal, but keep code in case we re-add after larger refactor.
	var typeKeys []storage.TKey

	// Helper function which deletes all provided object names.
	deleteObjects := func(names []string) (e error) {
		// Parallelize deleting these objects.
		var (
			wg    sync.WaitGroup
			mutex sync.RWMutex
		)

		jobs := make(chan string)
		for i := 0; i < maxConcurrentOperations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for name := range jobs {
					rateLimit <- struct{}{}
					if err := s.RawDelete(decodeKey(name)); err != nil {
						mutex.Lock()
						e = err
						mutex.Unlock()
					}
					<-rateLimit
				}
			}()
		}

		for _, name := range names {
			jobs <- name
			mutex.RLock()
			err := e
			mutex.RUnlock()
			if err != nil {
				break
			}
		}
		close(jobs)
		wg.Wait()

		return
	}

	// Helper function which deletes the provided type keys.
	deleteTypeKeys := func(keys []storage.TKey) (e error) {
		// Parallelize deleting these keys.
		var (
			wg    sync.WaitGroup
			mutex sync.RWMutex
		)

		jobs := make(chan storage.TKey)
		for i := 0; i < maxConcurrentOperations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for typeKey := range jobs {
					if err := s.Delete(context, typeKey); err != nil {
						mutex.Lock()
						e = err
						mutex.Unlock()
					}
				}
			}()
		}

		for _, typeKey := range keys {
			jobs <- typeKey
			mutex.RLock()
			err := e
			mutex.RUnlock()
			if err != nil {
				break
			}
		}
		close(jobs)
		wg.Wait()

		return
	}

	// Process all objects in chunks.
	return s.conn.ObjectsWalk(s.container, nil, func(opts *swift.ObjectsOpts) (interface{}, error) {
		// Get the names for this batch. Retry in case of failures.
		var names []string
		delay := initialDelay
		for {
			var err error
			rateLimit <- struct{}{}
			names, err = s.conn.ObjectNames(s.container, opts)
			<-rateLimit
			if err == nil {
				break // We have the names.
			}
			if delay > maximumDelay {
				return nil, fmt.Errorf(`Maximum object list requests exceeded: %s`, err)
			}
			time.Sleep(delay)
			delay *= 2
		}

		// If we delete all versions, we delete everything.
		if allVersions {
			return names, deleteObjects(names)
		}

		// Extract the type keys.
		firstKey := len(typeKeys)
	NameLoop:
		for _, name := range names {
			key := decodeKey(name)
			if len(key) == 0 {
				continue
			}
			typeKey, err := storage.TKeyFromKey(key)
			if err != nil {
				return names, fmt.Errorf("Unable to extract type key from key: %s", err)
			}
			for _, tk := range typeKeys {
				// This is O(n^2) but it shouldn't matter in the big picture.
				if bytes.Compare(tk, typeKey) == 0 {
					continue NameLoop // We already have this type key.
				}
			}
			typeKeys = append(typeKeys, typeKey)
		}

		return names, deleteTypeKeys(typeKeys[firstKey:])
	})
}

/*********** KeyValueBatcher interface ***********/

func (s *Store) NewBatch(ctx storage.Context) storage.Batch {
	return newBatch(s, ctx)
}

// lockBatch locks the keys of the operations contained in the provided batch.
// If a lock is already held on one of the keys, this function returns as soon
// as it was able to secure a lock (i.e. after the other lock is released).
func (s *Store) lockBatch(batch *Batch) {
	// This helper function checks if a lock is held on the keys of this batch.
	locked := func() bool {
		for name := range batch.deletes {
			if locks := s.batchLocks[name]; locks > 0 {
				return true
			}
		}
		for name := range batch.puts {
			if locks := s.batchLocks[name]; locks > 0 {
				return true
			}
		}
		return false
	}

	// This helper function locks the keys of this batch.
	lock := func() {
		for name := range batch.deletes {
			if _, ok := s.batchLocks[name]; !ok {
				s.batchLocks[name] = 1
			} else {
				s.batchLocks[name]++
			}
		}
		for name := range batch.puts {
			if _, ok := s.batchLocks[name]; !ok {
				s.batchLocks[name] = 1
			} else {
				s.batchLocks[name]++
			}
		}
	}

	// Acquire lock after locks have been released.
	s.batchLocksMutex.Lock()
	for {
		// Are there locks on our keys?
		if !locked() {
			// No. Lock and proceed.
			lock()
			break
		}

		// Someone holds locks on our keys. Wait for a signal to check again.
		if s.batchLocksReleased == nil {
			// There's no signal channel yet. Create one.
			s.batchLocksReleased = make(chan struct{})
		}
		signal := s.batchLocksReleased

		// Wait for someone to post a release signal.
		s.batchLocksMutex.Unlock()
		<-signal
		s.batchLocksMutex.Lock()
	}
	s.batchLocksMutex.Unlock()
}

// unlockBatch releases the locks held by the keys of the operations contained
// in the provided batch.
func (s *Store) unlockBatch(batch *Batch) {
	s.batchLocksMutex.Lock()
	defer s.batchLocksMutex.Unlock()

	// Release locks.
	for name := range batch.deletes {
		if locks := s.batchLocks[name]; locks > 0 {
			s.batchLocks[name]--
		} else {
			delete(s.batchLocks, name)
		}
	}
	for name := range batch.puts {
		if locks := s.batchLocks[name]; locks > 0 {
			s.batchLocks[name]--
		} else {
			delete(s.batchLocks, name)
		}
	}

	// Signal any waiting batches.
	if s.batchLocksReleased != nil {
		close(s.batchLocksReleased)
		s.batchLocksReleased = nil
	}
}
