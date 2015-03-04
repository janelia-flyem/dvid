/*
	This file supports keyspaces for label size data types.
*/

package labelsz

import (
	"encoding/binary"
	"fmt"

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
	keyLabelSize = 90

	// keySizeLabel keys are a concatenation of label + size.
	// They allow rapid return of the size of a particular label.
	keySizeLabel = 91
)

func (t keyType) String() string {
	switch t {
	case keySizeLabel:
		return "Label sorted by volume"
	case keyLabelSize:
		return "Volume of each label"
	default:
		return "Unknown key Type"
	}
}

// NewSizeLabelIndex returns an identifier for storing a "size + label".
func NewSizeLabelIndex(size, label uint64) []byte {
	ibytes := make([]byte, 17)
	ibytes[0] = byte(keySizeLabel)
	binary.BigEndian.PutUint64(ibytes[1:9], size)
	binary.BigEndian.PutUint64(ibytes[9:17], label)
	return ibytes
}

// DecodeSizeLabelKey returns a size and a label from a size+label key.
func DecodeSizeLabelKey(key []byte) (label, size uint64, err error) {
	ctx := &storage.DataContext{}
	var ibytes []byte
	ibytes, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if ibytes[0] != byte(keySizeLabel) {
		err = fmt.Errorf("Expected keySizeLabel header, got %d byte instead", ibytes[0])
		return
	}
	label = binary.BigEndian.Uint64(ibytes[9:17])
	size = binary.BigEndian.Uint64(ibytes[1:9])
	return
}

// NewLabelSizeIndex returns an identifier for storing a "label + size".
func NewLabelSizeIndex(label, size uint64) []byte {
	ibytes := make([]byte, 17)
	ibytes[0] = byte(keyLabelSize)
	binary.BigEndian.PutUint64(ibytes[1:9], label)
	binary.BigEndian.PutUint64(ibytes[9:17], size)
	return ibytes
}

// DecodeLabelSizeKey returns a size and a label from a lavel+size key.
func DecodeLabelSizeKey(key []byte) (label, size uint64, err error) {
	ctx := &storage.DataContext{}
	var ibytes []byte
	ibytes, err = ctx.IndexFromKey(key)
	if err != nil {
		return
	}
	if ibytes[0] != byte(keyLabelSize) {
		err = fmt.Errorf("Expected keyLabelSize header, got %d byte instead", ibytes[0])
		return
	}
	label = binary.BigEndian.Uint64(ibytes[1:9])
	size = binary.BigEndian.Uint64(ibytes[9:17])
	return
}
