/*
	Package labels provides various indexing options for labels and label maps.  It defines
	interfaces that can be used by other packages implementing 64-bit labels and label maps.
*/
package labels

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// Labelers can store label data
type Labeler interface {
	DataKey(dvid.VersionLocalID, dvid.Index) *datastore.DataKey
}

type KeyType byte

// Label indexing is handled through a variety of key spaces that optimize
// throughput for access patterns required by our API.  For dcumentation purposes,
// consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
//   v: # of voxels for a label
const (
	// KeyInverseMap have keys of form 'b+a'
	KeyInverseMap KeyType = iota

	// KeyForwardMap have keys of form 'a+b'
	// For superpixel->body maps, this key would be superpixel+body.
	KeyForwardMap

	// KeySpatialMap have keys of form 's+a+b'
	// They are useful for composing label maps for a spatial index.
	KeySpatialMap

	// KeyLabelSpatialMap have keys of form 'b+s' and have a sparse volume
	// encoding for its value. They are useful for returning all blocks
	// intersected by a label.
	KeyLabelSpatialMap

	// KeyLabelSizes have keys of form 'v+b'.
	// They allow rapid size range queries.
	KeyLabelSizes
)

var (
	zeroLabelBytes = make([]byte, 8, 8)
)

// ZeroBytes returns a slice of bytes that represents the zero label.
func ZeroBytes() []byte {
	return zeroLabelBytes
}

func (t KeyType) String() string {
	switch t {
	case KeyInverseMap:
		return "Inverse Label Map"
	case KeyForwardMap:
		return "Forward Label Map"
	case KeySpatialMap:
		return "Spatial Index to Labels Map"
	case KeyLabelSpatialMap:
		return "Forward Label to Spatial Index Map"
	case KeyLabelSizes:
		return "Forward Label sorted by volume"
	default:
		return "Unknown Key Type"
	}
}

// NewLabelSpatialMapKey returns a datastore.DataKey that encodes a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func NewLabelSpatialMapKey(labeler Labeler, vID dvid.VersionLocalID, label uint64, block dvid.IndexZYX) *datastore.DataKey {
	index := make([]byte, 1+8+dvid.IndexZYXSize)
	index[0] = byte(KeyLabelSpatialMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:9+dvid.IndexZYXSize], block.Bytes())
	return labeler.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSizesKey returns a datastore.DataKey that encodes a "size + mapped label".
func NewLabelSizesKey(labeler Labeler, vID dvid.VersionLocalID, size, label uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return labeler.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSurfaceKey returns a datastore.DataKey that provides a surface for a given label.
func NewLabelSurfaceKey(labeler Labeler, vID dvid.VersionLocalID, label uint64) *datastore.DataKey {
	index := make([]byte, 8)
	binary.BigEndian.PutUint64(index, label)
	return labeler.DataKey(vID, dvid.IndexBytes(index))
}

// NewForwardMapKey returns a datastore.DataKey that encodes a "label + mapping", where
// the label and mapping are both uint64.
func NewForwardMapKey(labeler Labeler, vID dvid.VersionLocalID, label []byte, mapping uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], label)
	binary.BigEndian.PutUint64(index[9:17], mapping)
	return labeler.DataKey(vID, dvid.IndexBytes(index))
}

// NewSpatialMapKey returns a datastore.DataKey that encodes a "spatial index + label + mapping".
func NewSpatialMapKey(labeler Labeler, vID dvid.VersionLocalID, blockIndex dvid.Index, label []byte,
	mapping uint64) *datastore.DataKey {

	index := make([]byte, 1+dvid.IndexZYXSize+8+8) // s + a + b
	index[0] = byte(KeySpatialMap)
	i := 1 + dvid.IndexZYXSize
	copy(index[1:i], blockIndex.Bytes())
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mapping)
	return labeler.DataKey(vID, dvid.IndexBytes(index))
}

// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
func StoreKeyLabelSpatialMap(labeler Labeler, batcher storage.Batcher, versionID dvid.VersionLocalID, zyxBytes []byte,
	labelRLEs map[uint64]dvid.RLEs) {

	batch := batcher.NewBatch()
	defer func() {
		if err := batch.Commit(); err != nil {
			dvid.Infof("Error on batch PUT of KeyLabelSpatialMap: %s\n", err.Error())
		}
	}()
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(KeyLabelSpatialMap)
	copy(bsIndex[9:9+dvid.IndexZYXSize], zyxBytes)
	for b, rles := range labelRLEs {
		binary.BigEndian.PutUint64(bsIndex[1:9], b)
		key := labeler.DataKey(versionID, dvid.IndexBytes(bsIndex))
		runsBytes, err := rles.MarshalBinary()
		if err != nil {
			dvid.Infof("Error encoding KeyLabelSpatialMap keys for mapped label %d: %s\n", b, err.Error())
			return
		}
		batch.Put(key, runsBytes)
	}
}

func labelFromLabelSizesKey(key storage.Key) uint64 {
	dataKey := key.(*datastore.DataKey)
	indexBytes := dataKey.Index.Bytes()
	return binary.BigEndian.Uint64(indexBytes[9:17])
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func ComputeSizes(labeler Labeler, sizeCh chan *storage.Chunk, db storage.OrderedKeyValueSetter,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	const BATCH_SIZE = 10000
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Infof("Storage engine does not support Batch PUT.  Aborting\n")
		return
	}
	batch := batcher.NewBatch()

	defer func() {
		wg.Done()
	}()

	// Sequentially process all the sparse volume data for each label
	var curLabel, curSize uint64
	putsInBatch := 0
	notFirst := false
	for {
		chunk := <-sizeCh
		if chunk == nil {
			key := NewLabelSizesKey(labeler, versionID, curSize, curLabel)
			batch.Put(key, dvid.EmptyValue())
			if err := batch.Commit(); err != nil {
				dvid.Infof("Error on batch PUT of label sizes: %s\n", err.Error())
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)

		// Compute the size
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(chunk.V); err != nil {
			dvid.Infof("Error deserializing RLEs: %s\n", err.Error())
			return
		}
		numVoxels, _ := rles.Stats()

		// If we are a new label, store size
		if notFirst && label != curLabel {
			key := NewLabelSizesKey(labeler, versionID, curSize, curLabel)
			curSize = 0
			batch.Put(key, dvid.EmptyValue())
			putsInBatch++
			if putsInBatch%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Infof("Error on batch PUT of label sizes: %s\n", err.Error())
					return
				}
				batch = batcher.NewBatch()
			}
		}
		curLabel = label
		curSize += uint64(numVoxels)
		notFirst = true
	}
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
// If maxSize is 0, all mapped labels are returned >= minSize.
func GetSizeRange(labeler Labeler, uuid dvid.UUID, minSize, maxSize uint64) (string, error) {
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
		return "{}", err
	}

	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return "{}", err
	}

	// Get the start/end keys for the size range.
	firstKey := NewLabelSizesKey(labeler, versionID, minSize, 0)
	var upperBound uint64
	if maxSize != 0 {
		upperBound = maxSize
	} else {
		upperBound = math.MaxUint64
	}
	lastKey := NewLabelSizesKey(labeler, versionID, upperBound, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return "{}", err
	}

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		labels[i] = labelFromLabelSizesKey(key)
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}
