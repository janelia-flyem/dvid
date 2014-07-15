/*
	This file contains types that manage valid key space within a DVID key-value database
	and support versioning.
*/

package dvid

import (
	"bytes"
	"fmt"
)

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

// Key is an opaque type that enforces partitioning of the key space in a way that
// avoids collisions.  Datatype implementations typically use NewDataKey() and provide
// a datatype-specific index, which could be considered datatype-specific keys that
// adhere to the overall DVID key space partitioning.
// For a description of Go language opaque types, see the following:
//   http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html
type Key interface {
	// Bytes returns the key representation as a slice of bytes
	Bytes() []byte

	// BytesToKey returns a Key given its representation as a slice of bytes
	BytesToKey([]byte) (Key, error)

	String() string

	// Versioned is true if multiple versions of a key/value could be available and
	// this implementation of Key also implements the VersionedKey interface.
	Versioned() bool

	// Enforces opaque data type.
	implementsOpaque()
}

// VersionedKey extends Key with the minimal functions necessary to handle versioning
// in storage engines.  If a Data's Versioned() returns true, we may assert that
// the key also implements a VersionedKey interface.
type VersionedKey interface {
	Key

	// Returns lower bound key for versions of given byte slice key representation.
	MinVersionKey([]byte) (Key, error)

	// Returns upper bound key for versions of given byte slice key representation.
	MaxVersionKey([]byte) (Key, error)

	// VersionedKeyValue returns the key/value pair corresponding to this key's version
	// given a list of key/value pairs across many versions.
	VersionedKeyValue([]KeyValue) (KeyValue, error)
}

// VersionIterator allows iteration through ancestors of version DAG and determining
// if current ancestor is acceptable.
type VersionIterator interface {
	Valid() bool
	Next()
	AcceptableKey(Key) bool
}

// KeyValue stores a key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// Deserialize returns a key-value pair where the value has been deserialized.
func (kv KeyValue) Deserialize(uncompress bool) (KeyValue, error) {
	value, _, err := DeserializeData(kv.V, uncompress)
	return KeyValue{kv.K, value}, err
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K.Bytes(), kv[j].K.Bytes()) <= 0
}

// ---- Key implementations -----

// UnversionedKey is useful for embedding and implements stubs for the versioned-related
// interface requirements for storage.Key
type UnversionedKey struct{}

func (k UnversionedKey) implementsOpaque() {}

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
	return KeyValue{}, fmt.Errorf("versioned keys requested from unversioned key")
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

// DataKey holds DVID-centric data and some datatype-specific key (Index).  This type should
// be embedded within datatype-specific key types.
type DataKey struct {
	index   Index
	version VersionID
	data    Data
}

// NewDataKey provides a way for datatypes to create DataKey that adhere to a DVID-wide
// key space partitioning.  Since Key and VersionedKey interfaces are opaque, i.e., can
// only be implemented within package dvid, we force compatible implementations to embed
// DataKey and initialize it via this function.
func NewDataKey(index Index, version VersionID, data Data) *DataKey {
	return &DataKey{index, version, data}
}

func (key *DataKey) implementsOpaque() {}

// The offset to the Index in bytes
const DataKeyIndexOffset = LocalID32Size + 1

func (key *DataKey) Index() Index {
	return key.index
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
	instanceID, _ := InstanceIDFromBytes(b[1:])
	if instanceID != key.data.InstanceID() {
		return nil, fmt.Errorf("Key of instance %v (%d) used to convert bytes -> key of different instance (%d)",
			key.data.DataName(), key.data.InstanceID(), instanceID)
	}

	// Get version ID as last component
	start := len(b) - LocalID32Size
	version, _ := VersionIDFromBytes(b[start:])

	// Whatever remains is the datatype-specific component of the key.
	var index Index
	var err error
	end := start
	start = DataKeyIndexOffset
	if end > start {
		index, err = key.index.IndexFromBytes(b[start:end])
	}
	return &DataKey{index, version, key.data}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *DataKey) Bytes() (b []byte) {
	b = []byte{byte(KeyData)}
	b = append(b, key.data.InstanceID().Bytes()...)
	b = append(b, key.index.Bytes()...)
	b = append(b, key.version.Bytes()...)
	return
}

// Bytes returns a string derived from the concatenation of the key elements.
func (key *DataKey) BytesString() string {
	return string(key.Bytes())
}

// String returns a human-readable description of a DataKey
func (key *DataKey) String() string {
	return fmt.Sprintf("Key of data %v (%d), version %d, index = %x", key.data.DataName(),
		key.data.InstanceID(), key.index.Bytes())
}

// Returns lower bound key for versions of given byte slice key representation.
func (key *DataKey) MinVersionKey(b []byte) (Key, error) {
	k, err := key.BytesToKey(b)
	if err != nil {
		return nil, err
	}
	minKey, _ := k.(*DataKey) // If no error in BytesToKey, we have a *DataKey
	minKey.version = VersionID(0)
	return minKey, nil
}

// Returns upper bound key for versions of given byte slice key representation.
func (key *DataKey) MaxVersionKey(b []byte) (Key, error) {
	k, err := key.BytesToKey(b)
	if err != nil {
		return nil, err
	}
	maxKey, _ := k.(*DataKey) // If no error in BytesToKey, we have a *DataKey
	maxKey.version = MaxVersionID
	return maxKey, nil
}

func (key *DataKey) Versioned() bool {
	return key.data.Versioned()
}

func (key *DataKey) VersionedKeyValue(values []KeyValue) (KeyValue, error) {
	// This data needs to be Versioned or return an error.
	if !key.data.Versioned() {
		return KeyValue{}, fmt.Errorf("Data instance %v is not versioned so can't do VersionedKeyValue()",
			key.data.DataName())
	}

	vdata, ok := key.data.(VersionedData)
	if !ok {
		return KeyValue{}, fmt.Errorf("Data instance %v should have implemented GetIterator()",
			key.data.DataName())
	}

	// Iterate from the current node up the ancestors in the version DAG, checking if
	// current best is present.
	for it, err := vdata.GetIterator(key); err == nil && it.Valid(); it.Next() {
		for _, kv := range values {
			if it.AcceptableKey(kv.K) {
				return kv, nil
			}
		}
	}
	return KeyValue{}, nil
}
