/*
	This file supports keyspaces for label block data types.
*/

package labelblk

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
	keyUnknown keyType = 0

	// use different id from other label-type keys to improve odds that any bad use of
	// arbitrary key decoding will result in error.
	keyLabelBlock = 37
)

func (t keyType) String() string {
	switch t {
	case keyLabelBlock:
		return "Label block"
	default:
		return "Unknown Key Type"
	}
}

// NewIndexByCoord returns an index for a block coord in string format.
func NewIndexByCoord(bcoord dvid.IZYXString) []byte {
	sz := len(bcoord)
	ibytes := make([]byte, 1+sz)
	ibytes[0] = byte(keyLabelBlock)
	copy(ibytes[1:], []byte(bcoord))
	return ibytes
}

// NewIndex returns an index for a label block, which is a slice suitable for
// lexicographical ordering on zyx coordinates.
func NewIndex(idx dvid.Index) []byte {
	izyx := idx.(*dvid.IndexZYX)
	return NewIndexByCoord(izyx.ToIZYXString())
}

// DecodeKey returns a spatial index from a label block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeKey(key []byte) (*dvid.IndexZYX, error) {
	var ctx storage.DataContext
	ibytes, err := ctx.IndexFromKey(key)
	if err != nil {
		return nil, err
	}
	if ibytes[0] != byte(keyLabelBlock) {
		return nil, fmt.Errorf("Expected keyLabelBlock index, got %d byte instead", ibytes[0])
	}
	var zyx dvid.IndexZYX
	if err = zyx.IndexFromBytes(ibytes[1:]); err != nil {
		return nil, fmt.Errorf("Cannot recover ZYX index from key %v: %s\n", key, err.Error())
	}
	return &zyx, nil
}
