/*
	This file supports keyspaces for label size data types.
*/

package labelsz

import (
	"encoding/binary"

	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// keyLabelSizes keys are a concatenation of size + label
	// They allow rapid size range queries.
	keyLabelSize = 90

	// keySizeLabel keys are a concatenation of label + size.
	// They allow rapid return of the size of a particular label.
	keySizeLabel = 91
)

// NewSizeLabelTKey returns a key component for storing a "size + label".
func NewSizeLabelTKey(size, label uint64) storage.TKey {
	ibytes := make([]byte, 16)
	binary.BigEndian.PutUint64(ibytes[0:8], size)
	binary.BigEndian.PutUint64(ibytes[8:16], label)
	return storage.NewTKey(keySizeLabel, ibytes)
}

// DecodeSizeLabelTKey returns a size and a label from a size+label key component.
func DecodeSizeLabelTKey(tk storage.TKey) (label, size uint64, err error) {
	ibytes, err := tk.ClassBytes(keySizeLabel)
	if err != nil {
		return
	}
	size = binary.BigEndian.Uint64(ibytes[0:8])
	label = binary.BigEndian.Uint64(ibytes[8:16])
	return
}

// NewLabelSizeTKey returns a key component for a "label + size".
func NewLabelSizeTKey(label, size uint64) storage.TKey {
	ibytes := make([]byte, 16)
	binary.BigEndian.PutUint64(ibytes[0:8], label)
	binary.BigEndian.PutUint64(ibytes[8:16], size)
	return storage.NewTKey(keyLabelSize, ibytes)
}

// DecodeLabelSizeTKey returns a size and a label from a lavel+size key.
func DecodeLabelSizeTKey(tk storage.TKey) (label, size uint64, err error) {
	ibytes, err := tk.ClassBytes(keyLabelSize)
	if err != nil {
		return
	}
	label = binary.BigEndian.Uint64(ibytes[0:8])
	size = binary.BigEndian.Uint64(ibytes[8:16])
	return
}
