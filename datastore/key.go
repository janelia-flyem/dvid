/*
	This file contains types that implement storage.Key and define valid key spaces
	within a DVID key-value database.
*/

package datastore

import (
	"fmt"
	"reflect"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Key is an opaque type that enforces partitioning of the key space in a way that
// voids collisions. All keys used for a particular DVID server *must* use one set of
// Key implementations like the ones in package datastore.  Data types should not
// construct their own implementations of storage.Key.
// For a description of Go language opaque types, see the following:
//   http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html
type Key interface {
	storage.Key

	// Enforces opaque data type.
	implementsOpaque()
}

// KeyType is a portion of a serialized Key that partitions key space into
// distinct areas and makes sure keys don't conflict.
type KeyType byte

const (
	// Key group that hold data for the collection of Repos
	KeyRepos KeyType = iota

	// Key group that holds a Repo's data.
	KeyRepo

	// Key group that holds the data for all data instances.  Each Datatype figures out
	// how to partition its own key space using some datatype-specific indexing scheme.
	// That datatype-specific indexing is not visible at the storage package level.
	KeyData

	// Key group that holds Sync links between Data.  Sync key/value pairs designate
	// what values need to be updated when its linked data changes.
	KeySync
)

func (t KeyType) String() string {
	switch t {
	case KeyRepos:
		return "Repos Key Type"
	case KeyRepo:
		return "Repo Key Type"
	case KeyData:
		return "Data Key Type"
	case KeySync:
		return "Data Sync Key Type"
	default:
		return "Unknown Key Type"
	}
}

// ---- Key implementations -----

// UnversionedKey is useful for embedding and implements stubs for the versioned-related
// interface requirements for storage.Key
type UnversionedKey struct{}

func (k UniversionedKey) implementsOpaque() {}

// Versioned is true if multiple versions of a key/value could be available.
func (k UnversionedKey) Versioned() bool {
	return false
}

// Returns lower bound key for versions of given byte slice key representation.
func (k UnversionedKey) MinVersionKey([]byte) (Key, error) {
	return nil, fmt.Errorf("versioned keys requested from unversioned key")
}

// Returns upper bound key for versions of given byte slice key representation.
func (k UnversionedKey) MaxVersionKey([]byte) (Key, error) {
	return nil, fmt.Errorf("versioned keys requested from unversioned key")
}

// VersionedKeyValue returns the key/value pair corresponding to this key's version
// given a list of key/value pairs across many versions.
func (k UnversionedKey) VersionedKeyValue([]KeyValue) (KeyValue, error) {
	return nil, fmt.Errorf("versioned keys requested from unversioned key")
}

// VersionedKeyValues returns the key/value pairs corresponding to this key's version
// given a list of key/value pairs across many versions.
func (k UnversionedKey) VersionedKeyValues([]KeyValue) ([]KeyValue, error) {
	return nil, fmt.Errorf("versioned keys requested from unversioned key")

}

// ReposKey is an implementation of Key for Repos persistence
type ReposKey struct {
	UnversionedKey
}

func NewReposKey() ReposKey {
	return ReposKey{}
}

func (k ReposKey) implementsOpaque() {}

func (k ReposKey) KeyType() KeyType {
	return KeyRepos
}

func (k ReposKey) BytesToKey(b []byte) (Key, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("Malformed ReposKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyRepos) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into ReposKey", KeyType(b[0]))
	}
	return &ReposKey{}, nil
}

func (k ReposKey) Bytes() []byte {
	return []byte{byte(KeyRepos)}
}

func (k ReposKey) BytesString() string {
	return string(k.Bytes())
}

func (k ReposKey) String() string {
	return fmt.Sprintf("%x", k.Bytes())
}

// RepoKey is an implementation of Key for Repo persistence.
type RepoKey struct {
	UnversionedKey
	repo RepoID
}

func NewRepoKey(id RepoID) RepoKey {
	return RepoKey{repo: id}
}

func (k RepoKey) implementsOpaque() {}

func (k RepoKey) KeyType() KeyType {
	return KeyRepo
}

func (k RepoKey) BytesToKey(b []byte) (Key, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("Malformed RepoKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyRepo) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into RepoKey", KeyType(b[0]))
	}
	id, _ := RepoIDFromBytes(b[1:])
	return &RepoKey{repo: id}, nil
}

func (k RepoKey) Bytes() (b []byte) {
	b = []byte{byte(KeyRepo)}
	b = append(b, k.repo.Bytes()...)
	return
}

func (k RepoKey) String() string {
	return fmt.Sprintf("%x", k.Bytes())
}

func MinRepoKey() Key {
	return &RepoKey{repo: 0}
}

func MaxRepoKey() Key {
	return &RepoKey{repo: MaxRepoID}
}

// DataKey holds DVID-centric data like shortened identifiers for the data instance,
// version (node of DAG), and some datatype-specific key (Index).  This type should
// be embedded within datatype-specific key types.
type DataKey struct {
	index   dvid.Index
	version dvid.VersionID

	instance  dvid.InstanceID
	versioned bool

	repo *Repo
}

func NewDataKey(index dvid.Index, version VersionID, instance *DataInstance, repo *Repo) *DataKey {
	return DataKey{index, version, instance.ID(), instance.Versioned(), repo}
}

func (key *DataKey) implementsOpaque() {}

// The offset to the Index in bytes
const DataKeyIndexOffset = dvid.LocalID32Size + 1

// KeyToChunkIndexer takes a Key and returns an implementation of a ChunkIndexer if possible.
func KeyToChunkIndexer(key *DataKey) (dvid.ChunkIndexer, error) {
	ptIndex, ok := datakey.index.(dvid.ChunkIndexer)
	if !ok {
		return nil, fmt.Errorf("Can't convert DataKey.Index (%s) to ChunkIndexer",
			reflect.TypeOf(datakey.index))
	}
	return ptIndex, nil
}

// ------ Key Interface ----------

func (key *DataKey) KeyType() KeyType {
	return KeyData
}

// BytesToKey returns a DataKey given a slice of bytes
func (key *DataKey) BytesToKey(b []byte) (Key, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("Malformed DataKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyData) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into DataKey", KeyType(b[0]))
	}
	// Get instance ID as first component
	instance, _ := InstanceIDFromBytes(b[1:])

	// Get version ID as last component
	start := len(b) - dvid.LocalID32Size
	version, _ := VersionIDFromBytes(b[start:])

	// Whatever remains is the datatype-specific component of the key.
	var index dvid.Index
	var err error
	end := start
	start = DataKeyIndexOffset
	if end > start {
		index, err = key.index.IndexFromBytes(b[start:end])
	}
	return &DataKey{index, version, key.instance, key.versioned, key.repo}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *DataKey) Bytes() (b []byte) {
	b = []byte{byte(KeyData)}
	b = append(b, key.instance.Bytes()...)
	b = append(b, key.index.Bytes()...)
	b = append(b, key.version.Bytes()...)
	return
}

// Bytes returns a string derived from the concatenation of the key elements.
func (key *DataKey) BytesString() string {
	return string(key.Bytes())
}

// String returns a hexadecimal representation of the bytes encoding a key
// so it is readable on a terminal.
func (key *DataKey) String() string {
	return fmt.Sprintf("%x", key.Bytes())
}

// Returns lower bound key for versions of given byte slice key representation.
func (key *DataKey) MinVersionKey(b []byte) (Key, error) {
	minKey, err := key.BytesToKey(b)
	if err != nil {
		return nil, err
	}
	minKey.version = dvid.VersionID(0)
	return minKey, nil
}

// Returns upper bound key for versions of given byte slice key representation.
func (key *DataKey) MaxVersionKey([]byte) (Key, error) {
	maxKey, err := key.BytesToKey(b)
	if err != nil {
		return nil, err
	}
	maxKey.version = dvid.MaxVersionID
	return maxKey, nil
}

func (key *DataKey) Versioned() bool {
	return key.versioned
}

func (key *DataKey) VersionedKeyValue(values []storage.KeyValue) (storage.KeyValue, error) {
	// Iterate from the current node up the ancestors in the version DAG, checking if
	// current best is present.
	for it, err := key.repo.VersionIterator(key.DataKey); err == nil && it.Valid(); it.Next() {
		for _, kv := range values {
			if it.MatchedKey(kv.K) {
				return kv, nil
			}
		}
	}
	return storage.KeyValue{}, nil
}
