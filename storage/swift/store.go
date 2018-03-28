package swift

import (
	"errors"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/ncw/swift"
)

// Store implements dvid.Store as an Openstack Swift store.
type Store struct {
	// The Swift container name.
	container string

	// The Swift connection.
	conn *swift.Connection
}

func (s *Store) String() string {
	return fmt.Sprintf(`Openstack Swift store`)
}

// Close closes the store.
func (s *Store) Close() {
}

// Equal returns true if this store matches the given store configuration.
func (s *Store) Equal(config dvid.StoreConfig) bool {
	for param, value := range map[string]string{
		"user": s.conn.UserName,
		"key":  s.conn.ApiKey,
		"auth": s.conn.AuthUrl,
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
	s := &Store{conn: &swift.Connection{}}

	// Get configuration values.
	var err error
	configString := func(param string) (string, error) {
		value, ok, e := config.GetString(param)
		if !ok {
			return "", fmt.Errorf(`Configuration parameter "%s" missing`)
		}
		if e != nil {
			return "", fmt.Errorf(`Error retrieving configuration parameter "%s" (may not be a string): %s`, e)
		}
		if value == "" {
			return "", errors.New(`Configuration parameter "%s" may not be empty`)
		}
		return value, nil
	}
	s.conn.UserName, err = configString("user")
	if err != nil {
		return nil, false, err
	}
	s.conn.ApiKey, err = configString("key")
	if err != nil {
		return nil, false, err
	}
	s.conn.AuthUrl, err = configString("auth")
	if err != nil {
		return nil, false, err
	}
	s.container, err = configString("container")
	if err != nil {
		return nil, false, err
	}

	// Authenticate with Swift.
	if err := s.conn.Authenticate(); err != nil {
		return nil, false, fmt.Errorf(`Unable to authenticate with the Swift database: %s`, err)
	}

	return s, false, nil
}

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
		names, err := s.conn.ObjectNamesAll(s.container, &swift.ObjectsOpts{
			Marker:    encodeKey(startKey),
			EndMarker: encodeKey(endKey),
		})
		if err != nil {
			return nil, fmt.Errorf(`Unable to list Swift object names: %s`, err)
		}
		var keyValues []*storage.KeyValue
		for _, name := range names {
			keyValues = append(keyValues, &storage.KeyValue{K: decodeKey(name)})
		}
		keyValue, err := versionedContext.VersionedKeyValue(keyValues)
		if err != nil {
			return nil, fmt.Errorf(`Unable to determine correct version key from a list: %s`, err)
		}
		accessKey = keyValue.K
	} else {
		// Context is unversioned.
		accessKey = context.ConstructKey(key)
	}

	// Load the object.
	contents, err := s.conn.ObjectGetBytes(s.container, encodeKey(accessKey))
	if err == swift.ObjectNotFound {
		return nil, nil
	}
	storage.StoreValueBytesRead <- len(contents)
	return contents, err
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (s *Store) RawPut(key storage.Key, value []byte) error {
	defer func() {
		storage.StoreValueBytesWritten <- len(value)
	}()
	return s.conn.ObjectPutBytes(s.container, encodeKey(key), value, "application/octet-stream")
}

// RawDelete is a low-level function.  It deletes a key-value pair using full
// keys without any context. This can be used in conjunction with RawRangeQuery.
func (s *Store) RawDelete(key storage.Key) error {
	err := s.conn.ObjectDelete(s.container, encodeKey(key))
	if err == swift.ObjectNotFound {
		return nil
	}
	return err
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

	// In a versioned context, we insert a tombstone.
	if context.Versioned() {
		versionedContext, ok := context.(storage.VersionedCtx)
		if !ok {
			return errors.New("Context is marked as versioned but isn't actually versioned")
		}

		// Delete tombstone.
		tombstone := versionedContext.TombstoneKey(typeKey)
		err := s.RawPut(tombstone, dvid.EmptyValue())
		if err != nil {
			return fmt.Errorf(`Could not save tombstone object: %s`, err)
		}
	}

	// Delete the given key.
	return s.RawDelete(key)
}
