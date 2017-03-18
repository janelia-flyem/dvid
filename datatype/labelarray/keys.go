/*
	This file supports keyspaces for label block data types.
*/

package labelarray

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = 0

	// Since we are reusing imageblk read code, need to use the same key.
	keyLabelBlock = 23

	// key = label, value = labels.LabelMeta
	keyLabelIndex = 187

	// Used to store max label on commit for each version of the instance.
	keyLabelMax = 237

	// Stores the single repo-wide max label for the instance.  Used for new labels on split.
	keyRepoLabelMax = 238
)

var (
	maxLabelTKey     = storage.NewTKey(keyLabelMax, nil)
	maxRepoLabelTKey = storage.NewTKey(keyRepoLabelMax, nil)
)

// NewBlockTKey returns a TKey for a label block, which is a slice suitable for
// lexicographical ordering on zyx coordinates.
func NewBlockTKey(idx dvid.Index) storage.TKey {
	izyx := idx.(*dvid.IndexZYX)
	return NewBlockTKeyByCoord(izyx.ToIZYXString())
}

// NewBlockTKeyByCoord returns a TKey for a block coord in string format.
func NewBlockTKeyByCoord(izyx dvid.IZYXString) storage.TKey {
	return storage.NewTKey(keyLabelBlock, []byte(izyx))
}

// DecodeBlockTKey returns a spatial index from a label block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeBlockTKey(tk storage.TKey) (*dvid.IndexZYX, error) {
	ibytes, err := tk.ClassBytes(keyLabelBlock)
	if err != nil {
		return nil, err
	}
	var zyx dvid.IndexZYX
	if err = zyx.IndexFromBytes(ibytes); err != nil {
		return nil, fmt.Errorf("Cannot recover ZYX index from image block key %v: %v\n", tk, err)
	}
	return &zyx, nil
}

// NewLabelIndexTKey returns a TKey corresponding to a label.  Value will hold block coords that contain the label.
func NewLabelIndexTKey(label uint64) storage.TKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, label)
	return storage.NewTKey(keyLabelIndex, buf)
}

// DecodeLabelIndexTKey parses a TKey and returns the corresponding label.
func DecodeLabelIndexTKey(tk storage.TKey) (label uint64, err error) {
	ibytes, err := tk.ClassBytes(keyLabelIndex)
	if err != nil {
		return
	}
	label = binary.BigEndian.Uint64(ibytes[0:8])
	return
}
