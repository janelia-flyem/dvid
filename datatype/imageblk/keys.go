/*
	This file supports keyspaces for image block data types.
*/

package imageblk

import (
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

	// use different id from other label-type keys to improve odds that any bad use of
	// arbitrary key decoding will result in error.
	keyImageBlock = 23
)

func (t keyType) String() string {
	switch t {
	case keyImageBlock:
		return "Image block"
	default:
		return "Unknown Key Type"
	}
}

// NewIndexByCoord returns an index for a block coord in string format.
func NewIndexByCoord(blockCoord string) []byte {
	sz := len(blockCoord)
	index := make([]byte, 1+sz)
	index[0] = byte(keyImageBlock)
	copy(index[1:], blockCoord)
	return dvid.IndexBytes(index)
}

// NewIndex returns an index for an image block.
// Index = s
func NewIndex(blockIndex dvid.Index) []byte {
	coord := string(blockIndex.Bytes())
	return NewIndexByCoord(coord)
}

// DecodeKey returns a spatial index from a image block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeKey(key []byte) (*dvid.IndexZYX, error) {
	var ctx storage.DataContext
	index, err := ctx.IndexFromKey(key)
	if err != nil {
		return nil, err
	}
	if index[0] != byte(keyImageBlock) {
		return nil, fmt.Errorf("Expected keyImageBlock index, got %d byte instead", index[0])
	}
	var zyx dvid.IndexZYX
	if err = zyx.IndexFromBytes(index[1:]); err != nil {
		return nil, fmt.Errorf("Cannot recover ZYX index from key %v: %s\n", key, err.Error())
	}
	return &zyx, nil
}
