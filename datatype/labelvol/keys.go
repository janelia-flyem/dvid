/*
	This file supports keyspaces for label block data types.
*/

package labelvol

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// keyType is the first byte of a type-specific index, allowing partitioning of the
// type-specific key space.
type keyType byte

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown keyType = iota

	// keyLabelBlockRLE have keys of form 'b+s' and have a sparse volume
	// encoding for its value. They are also useful for returning all blocks
	// intersected by a label.
	keyLabelBlockRLE = 227
)

func (t keyType) String() string {
	switch t {
	case keyUnknown:
		return "Unknown key Type"
	case keyLabelBlockRLE:
		return "Label Block RLEs"
	default:
		return "Unknown key Type"
	}
}

// NewIndex returns an identifier for storing a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func NewIndex(label uint64, blockBytes []byte) dvid.IndexBytes {
	sz := len(blockBytes)
	index := make([]byte, 1+8+sz)
	index[0] = byte(keyLabelBlockRLE)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:], blockBytes)
	return dvid.IndexBytes(index)
}

// DecodeKey returns a label and block index bytes from a label block RLE key.
// The block index bytes are returned because different block indices may be used (e.g., CZYX),
// and its up to caller to determine which one is used for this particular key.
func DecodeKey(key []byte) (label uint64, blockBytes []byte, err error) {
	var ctx storage.DataContext
	var index []byte
	index, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if index[0] != byte(keyLabelBlockRLE) {
		err = fmt.Errorf("Expected keyLabelBlockRLE index, got %d byte instead", index[0])
		return
	}
	label = binary.BigEndian.Uint64(index[1:9])
	blockBytes = index[9:]
	return
}
