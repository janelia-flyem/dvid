/*
	This file contains the Key interface, an implementation of Key for data,
	and the highest-level partitioning of key space into areas for DVID server
	configuration and data itself.

	While Key data has a number of components so key space can be managed
	more efficiently by the storage engine, values are simply []byte at
	this level.  We assume value serialization/deserialization occur above the
	storage level.
*/

package storage

import (
	"bytes"
	"fmt"

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

/*
	DataKey holds DVID-centric data like shortened version/UUID, data set, and
	index identifiers and that follow a convention of how to collapse those
	identifiers into a []byte key.  Ideally, we'd like to keep Key within
	the datastore package and have storage independent of DVID concepts,
	but in order to optimize the layout of data in some storage engines,
	the backend drivers need the additional DVID information.  For example,
	Couchbase allows configuration at the bucket level (RAM cache, CPUs)
	and datasets could be placed in different buckets.
*/
type DataKey struct {
	// The DVID server-specific 32-bit ID for a dataset.
	Dataset dvid.LocalID32

	// The DVID server-specific data index that is unique per dataset.
	Data dvid.LocalID

	// The DVID server-specific version index that is fewer bytes than a
	// complete UUID and unique per dataset.
	Version dvid.LocalID

	// The datatype-specific (usually spatiotemporal) index that allows partitioning
	// of the data.  In the case of voxels, this could be a (x, y, z) coordinate
	// packed into a slice of bytes.
	Index dvid.Index
}

// ------ Key Interface ----------

func (key *DataKey) KeyType() KeyType {
	return KeyData
}

// BytesToKey returns a DataKey given a slice of bytes
func (key *DataKey) BytesToKey(b []byte) (Key, error) {
	if len(b) < 10 {
		return nil, fmt.Errorf("Malformed DataKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyData) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into DataKey", KeyType(b[0]))
	}
	start := 1
	dataset, length := dvid.LocalID32FromBytes(b[start:])
	start += length
	data, length := dvid.LocalIDFromBytes(b[start:])
	start += length
	version, _ := dvid.LocalIDFromBytes(b[start:])
	start += length
	index, err := key.Index.IndexFromBytes(b[start:])
	return &DataKey{dataset, data, version, index}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *DataKey) Bytes() (b []byte) {
	b = []byte{byte(KeyData)}
	b = append(b, key.Dataset.Bytes()...)
	b = append(b, key.Data.Bytes()...)
	b = append(b, key.Version.Bytes()...)
	b = append(b, key.Index.Bytes()...)
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
