/*
	This file contains the Key interface and the KeyValue struct returned by range queries.

	While Key data has a number of components so key space can be managed
	more efficiently by the storage engine, values are simply []byte at
	this level.  We assume value serialization/deserialization occur above the
	storage level.
*/

package storage

import (
	"bytes"

	"github.com/janelia-flyem/dvid/dvid"
)

// KeyType is a portion of a serialized Key that partitions key space into
// distinct areas and makes sure keys don't conflict.
type KeyType byte

const (
	// Key group that hold data for Datasets
	KeyDatasets KeyType = iota

	// Key group that holds Dataset structs.  There can be many Dataset structs
	// persisted to a particular DVID datastore.
	KeyDataset

	// Key group that holds the Data.  Each Datatype figures out how to partition
	// its own key space using some type-specific indexing scheme.
	KeyData

	// Key group that holds Sync links between Data.  Sync key/value pairs designate
	// what values need to be updated when its linked data changes.
	KeySync
)

func (t KeyType) String() string {
	switch t {
	case KeyDatasets:
		return "Datasets Key Type"
	case KeyDataset:
		return "Dataset Key Type"
	case KeyData:
		return "Data Key Type"
	case KeySync:
		return "Data Sync Key Type"
	default:
		return "Unknown Key Type"
	}
}

type Key interface {
	KeyType() KeyType
	BytesToKey([]byte) (Key, error)
	Bytes() []byte
	BytesString() string
	String() string
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
