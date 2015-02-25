/*
	This file supports keyspaces for label size data types.
*/

package labelsz

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

	// keyLabelSizes keys are a concatenation of size + label
	// They allow rapid size range queries.
	keyLabelSizes = 90
)

func (t keyType) String() string {
	switch t {
	case keyLabelSizes:
		return "Label sorted by volume"
	default:
		return "Unknown key Type"
	}
}

// NewLabelSizesIndex returns an identifier for storing a "size + label".
func NewIndex(size, label uint64) dvid.IndexBytes {
	index := make([]byte, 17)
	index[0] = byte(keyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return dvid.IndexBytes(index)
}

// DecodeLabelSizesKey returns a size and a label from a label sizes key.
func DecodeKey(key []byte) (label, size uint64, err error) {
	ctx := &storage.DataContext{}
	var indexBytes []byte
	indexBytes, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if indexBytes[0] != byte(keyLabelSizes) {
		err = fmt.Errorf("Expected keyLabelSizes header, got %d byte instead", indexBytes[0])
		return
	}
	label = binary.BigEndian.Uint64(indexBytes[9:17])
	size = binary.BigEndian.Uint64(indexBytes[1:9])
	return
}
