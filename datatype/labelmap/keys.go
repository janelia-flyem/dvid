/*
	This file supports keyspaces for label map data types.
*/

package labelmap

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// keyType is the first byte of a type-specific index, allowing partitioning of the
// type-specific key space.
type keyType byte

// For dcumentation purposes, consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown keyType = iota

	// keyForwardMap have keys of form 'a+b'
	// For superpixel->body maps, this key would be superpixel+body.
	keyForwardMap = 112

	// keyInverseMap have keys of form 'b+a'
	keyInverseMap = 113

	// keyBlockMap have keys of form 's+a+b'
	// They are useful for composing label maps for a spatial index.
	keyBlockMap = 114
)

func (t keyType) String() string {
	switch t {
	case keyForwardMap:
		return "Forward Label Map"
	case keyInverseMap:
		return "Inverse Label Map"
	case keyBlockMap:
		return "Spatial Index to Labels Map"
	default:
		return "Unknown Key Type"
	}
}

// NewForwardMapIndex returns an index for mapping a label into another label.
// Index = a+b
// For dcumentation purposes, consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
//   v: # of voxels for a label
func NewForwardMapIndex(label []byte, mapping uint64) dvid.IndexBytes {
	index := make([]byte, 17)
	index[0] = byte(keyForwardMap)
	copy(index[1:9], label)
	binary.BigEndian.PutUint64(index[9:17], mapping)
	return dvid.IndexBytes(index)
}

// NewInverseMapIndex returns an index for mapping a label into another label.
// Index = b+a
func NewInverseMapIndex(label []byte, mapping uint64) dvid.IndexBytes {
	index := make([]byte, 17)
	index[0] = byte(keyInverseMap)
	binary.BigEndian.PutUint64(index[1:9], mapping)
	copy(index[9:17], label)
	return dvid.IndexBytes(index)
}

type BlockMapIndex dvid.IndexBytes

// NewBlockMapIndex returns an index optimizing access to label maps for a given
// spatial index. Index = s+a+b
func NewBlockMapIndex(blockIndex dvid.Index, label []byte, mappedLabel uint64) BlockMapIndex {
	indexBytes := blockIndex.Bytes()
	sz := len(indexBytes)
	index := make([]byte, 1+sz+8+8) // s + a + b
	index[0] = byte(keyBlockMap)
	i := 1 + sz
	copy(index[1:i], indexBytes)
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mappedLabel)
	return BlockMapIndex(index)
}

func (index BlockMapIndex) UpdateBlockMapIndex(label []byte, mappedLabel uint64) {
	spatialSize := len(index) - 17
	i := 1 + spatialSize
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mappedLabel)
}

// DecodeBlockMapKey returns a label mapping from a spatial map key.
func DecodeBlockMapKey(key []byte) (label []byte, mappedLabel uint64, err error) {
	var ctx storage.DataContext
	var index []byte
	index, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if index[0] != byte(keyBlockMap) {
		err = fmt.Errorf("Expected keyBlockMap index, got %d byte instead", index[0])
		return
	}
	labelOffset := 1 + dvid.IndexZYXSize // index here = s + a + b
	label = index[labelOffset : labelOffset+8]
	mappedLabel = binary.BigEndian.Uint64(index[labelOffset+8 : labelOffset+16])
	return
}

/*
// NewLabelBlockMapIndex returns an identifier for storing a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func NewLabelBlockMapIndex(label uint64, blockBytes []byte) dvid.IndexBytes {
	sz := len(blockBytes)
	index := make([]byte, 1+8+sz)
	index[0] = byte(keyLabelBlockMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:], blockBytes)
	return dvid.IndexBytes(index)
}

// DecodeLabelBlockMapKey returns a label and block index bytes from a LabelBlockMap key.
// The block index bytes are returned because different block indices may be used (e.g., CZYX),
// and its up to caller to determine which one is used for this particular key.
func DecodeLabelBlockMapKey(key []byte) (label uint64, blockBytes []byte, err error) {
	var ctx storage.DataContext
	var index []byte
	index, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if index[0] != byte(keyLabelBlockMap) {
		err = fmt.Errorf("Expected keyLabelBlockMap index, got %d byte instead", index[0])
		return
	}
	label = binary.BigEndian.Uint64(index[1:9])
	blockBytes = index[9:]
	return
}
*/
