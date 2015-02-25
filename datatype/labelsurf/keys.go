/*
	This file supports keyspaces for label surface data types.
*/

package labelsurf

import (
	"encoding/binary"

	"github.com/janelia-flyem/dvid/dvid"
)

// keyType is the first byte of a type-specific index, allowing partitioning of the
// type-specific key space.
type keyType byte

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown keyType = iota

	// keyLabelSurface have keys that are just the label
	keyLabelSurface = 71
)

func (t keyType) String() string {
	switch t {
	case keyLabelSurface:
		return "Label Surface"
	default:
		return "Unknown key Type"
	}
}

// NewLabelSurfaceIndex returns an identifier for a given label's surface.
func NewLabelSurfaceIndex(label uint64) dvid.IndexBytes {
	index := make([]byte, 1+8)
	index[0] = byte(keyLabelSurface)
	binary.BigEndian.PutUint64(index[1:9], label)
	return dvid.IndexBytes(index)
}
