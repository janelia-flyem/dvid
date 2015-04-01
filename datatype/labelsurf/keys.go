/*
	This file supports keyspaces for label surface data types.
*/

package labelsurf

import (
	"encoding/binary"

	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// keyLabelSurface have keys that are just the label
	keyLabelSurface = 71
)

// NewLabelSurfaceIndex returns an identifier for a given label's surface.
func NewLabelSurfaceIndex(label uint64) []byte {
	ibytes := make([]byte, 1+8)
	ibytes[0] = byte(keyLabelSurface)
	binary.BigEndian.PutUint64(ibytes[1:9], label)
	return ibytes
}
