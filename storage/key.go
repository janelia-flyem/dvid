/*
	This file contains code that supports partitioning of the key space as well as
	versioning of key/value pairs.
*/

package storage

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/janelia-flyem/dvid/dvid"
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
	// how to partition its own key space using some type-specific indexing scheme.
	// That Datatype-specific indexing is not visible at the storage package level.
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

// KeyValue stores a key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// Deserialize returns a key-value pair where the value has been deserialized.
func (kv KeyValue) Deserialize(uncompress bool) (KeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return KeyValue{kv.K, value}, err
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K.Bytes(), kv[j].K.Bytes()) <= 0
}

// Key holds instance-specific data useful for constructing keys.  When combined
// with a Context, we can compose keys that embody a particular data instance,
// version, and datatype-specific key (e.g., a 3d block coordinate).
type Key interface {
	KeyType() KeyType

	// Bytes returns the key representation as a slice of bytes
	Bytes() []byte

	// BytesToKey returns a Key given its representation as a slice of bytes
	BytesToKey([]byte) (Key, error)

	String() string
}

// ---- Key implementations -----

// ReposKey is an implementation of Key for Repos persistence
type ReposKey struct{}

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
	Repo dvid.RepoLocalID
}

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
	repo, _ := dvid.LocalID32FromBytes(b[1:])
	return &RepoKey{dvid.RepoLocalID(repo)}, nil
}

func (k RepoKey) Bytes() (b []byte) {
	b = []byte{byte(KeyRepo)}
	b = append(b, dvid.LocalID32(k.Repo).Bytes()...)
	return
}

func (k RepoKey) String() string {
	return fmt.Sprintf("%x", k.Bytes())
}

func MinRepoKey() Key {
	return &RepoKey{0}
}

func MaxRepoKey() Key {
	return &RepoKey{maxRepoID}
}

// DataKey holds DVID-centric data like shortened identifiers for the data instance,
// version (node of DAG), and some datatype-specific key (Index)
type DataKey struct {
	instance dvid.InstanceID
	version  dvid.VersionID
	index    dvid.Index
}

// The offset to the Index in bytes of a DataKey bytes representation
const DataKeyIndexOffset = 2*dvid.LocalID32Size + 1

// DataKey returns a DataKey for this data given a local version and a data-specific Index.
func (d *Data) DataKey(versionID dvid.VersionID, index dvid.Index) *DataKey {
	return &DataKey{d.ID, versionID, index}
}

// KeyToChunkIndexer takes a Key and returns an implementation of a ChunkIndexer if possible.
func KeyToChunkIndexer(key Key) (dvid.ChunkIndexer, error) {
	datakey, ok := key.(*DataKey)
	if !ok {
		return nil, fmt.Errorf("Can't convert Key (%s) to DataKey", key)
	}
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
	start := 1
	instance, length := dvid.LocalID32FromBytes(b[start:])
	start += length
	version, _ := dvid.LocalID32FromBytes(b[start:])
	start += length

	var index dvid.Index
	var err error
	if start < len(b) {
		index, err = key.index.IndexFromBytes(b[start:])
	}
	return &DataKey{dvid.InstanceID(instance), dvid.VersionID(version), index}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *DataKey) Bytes() (b []byte) {
	b = []byte{byte(KeyData)}
	b = append(b, dvid.LocalID32(key.instance).Bytes()...)
	b = append(b, dvid.LocalID32(key.version).Bytes()...)
	if key.index != nil {
		b = append(b, key.index.Bytes()...)
	}
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
