/*
	This file supports keyspaces for label block data types.
*/

package labelblk

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = 0

	// since we are reusing imageblk read code, need to use the same key.
	keyLabelBlock = 23
)

// NewTKeyByCoord returns a TKey for a block coord in string format.
func NewTKeyByCoord(izyx dvid.IZYXString) storage.TKey {
	return storage.NewTKey(keyLabelBlock, []byte(izyx))
}

// NewTKey returns a TKey for a label block, which is a slice suitable for
// lexicographical ordering on zyx coordinates.
func NewTKey(idx dvid.Index) storage.TKey {
	izyx := idx.(*dvid.IndexZYX)
	return NewTKeyByCoord(izyx.ToIZYXString())
}

// DecodeKey returns a spatial index from a label block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeTKey(tk storage.TKey) (*dvid.IndexZYX, error) {
	ibytes, err := tk.ClassBytes(keyLabelBlock)
	if err != nil {
		return nil, err
	}
	var zyx dvid.IndexZYX
	if err = zyx.IndexFromBytes(ibytes); err != nil {
		return nil, fmt.Errorf("Cannot recover ZYX index from image block key %v: %s\n", tk, err.Error())
	}
	return &zyx, nil
}
