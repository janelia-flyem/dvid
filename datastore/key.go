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

const maxDatasetLocalID = dvid.MaxLocalID32

const maxDataLocalID = dvid.MaxLocalID

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

type KeyType storage.KeyType

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

// EndKey returns the last possible Key of this KeyType.

// DatasetsKey is an implementation of storage.Key for Datasets persistence
type DatasetsKey struct{}

func (k DatasetsKey) KeyType() storage.KeyType {
	return storage.KeyType(KeyDatasets)
}

func (k DatasetsKey) BytesToKey(b []byte) (storage.Key, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("Malformed DatasetsKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyDatasets) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into DatasetsKey", KeyType(b[0]))
	}
	return &DatasetsKey{}, nil
}

func (k DatasetsKey) Bytes() []byte {
	return []byte{byte(KeyDatasets)}
}

func (k DatasetsKey) BytesString() string {
	return string(k.Bytes())
}

func (k DatasetsKey) String() string {
	return fmt.Sprintf("%x", k.Bytes())
}

// DatasetKey is an implementation of storage.Key for Dataset persistence.
type DatasetKey struct {
	Dataset dvid.DatasetLocalID
}

func (k DatasetKey) KeyType() storage.KeyType {
	return storage.KeyType(KeyDataset)
}

func (k DatasetKey) BytesToKey(b []byte) (storage.Key, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("Malformed DatasetKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyDataset) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into DatasetKey", KeyType(b[0]))
	}
	dataset, _ := dvid.LocalID32FromBytes(b[1:])
	return &DatasetKey{dvid.DatasetLocalID(dataset)}, nil
}

func (k DatasetKey) Bytes() (b []byte) {
	b = []byte{byte(KeyDataset)}
	b = append(b, dvid.LocalID32(k.Dataset).Bytes()...)
	return
}

func (k DatasetKey) BytesString() string {
	return string(k.Bytes())
}

func (k DatasetKey) String() string {
	return fmt.Sprintf("%x", k.Bytes())
}

func MinDatasetKey() storage.Key {
	return &DatasetKey{0}
}

func MaxDatasetKey() storage.Key {
	return &DatasetKey{maxDatasetLocalID}
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
	Dataset dvid.DatasetLocalID

	// The DVID server-specific data index that is unique per dataset.
	Data dvid.DataLocalID

	// The DVID server-specific version index that is fewer bytes than a
	// complete UUID and unique per dataset.
	Version dvid.VersionLocalID

	// The datatype-specific (usually spatiotemporal) index that allows partitioning
	// of the data.  In the case of voxels, this could be a (x, y, z) coordinate
	// packed into a slice of bytes.
	Index dvid.Index
}

// The offset to the Index in bytes of a DataKey bytes representation
const DataKeyIndexOffset = dvid.LocalIDSize*2 + dvid.LocalID32Size + 1

// DataKey returns a DataKey for this data given a local version and a data-specific Index.
func (d *Data) DataKey(versionID dvid.VersionLocalID, index dvid.Index) *DataKey {
	return &DataKey{d.DsetID, d.ID, versionID, index}
}

// KeyToChunkIndexer takes a Key and returns an implementation of a ChunkIndexer if possible.
func KeyToChunkIndexer(key storage.Key) (dvid.ChunkIndexer, error) {
	datakey, ok := key.(*DataKey)
	if !ok {
		return nil, fmt.Errorf("Can't convert Key (%s) to DataKey", key)
	}
	ptIndex, ok := datakey.Index.(dvid.ChunkIndexer)
	if !ok {
		return nil, fmt.Errorf("Can't convert DataKey.Index (%s) to ChunkIndexer",
			reflect.TypeOf(datakey.Index))
	}
	return ptIndex, nil
}

// ------ Key Interface ----------

func (key *DataKey) KeyType() storage.KeyType {
	return storage.KeyType(KeyData)
}

// BytesToKey returns a DataKey given a slice of bytes
func (key *DataKey) BytesToKey(b []byte) (storage.Key, error) {
	if len(b) < 9 {
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

	var index dvid.Index
	var err error
	if start < len(b) {
		index, err = key.Index.IndexFromBytes(b[start:])
	}
	return &DataKey{dvid.DatasetLocalID(dataset), dvid.DataLocalID(data), dvid.VersionLocalID(version), index}, err
}

// Bytes returns a slice of bytes derived from the concatenation of the key elements.
func (key *DataKey) Bytes() (b []byte) {
	b = []byte{byte(KeyData)}
	b = append(b, dvid.LocalID32(key.Dataset).Bytes()...)
	b = append(b, dvid.LocalID(key.Data).Bytes()...)
	b = append(b, dvid.LocalID(key.Version).Bytes()...)
	if key.Index != nil {
		b = append(b, key.Index.Bytes()...)
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
