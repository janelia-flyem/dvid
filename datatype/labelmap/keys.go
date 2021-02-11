/*
	This file supports keyspaces for label block data types.
*/

package labelmap

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = 0

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// key = scale + block coord
	keyLabelBlock = 186

	// key = label. value = datatype/common/proto/LabelIndex serialization
	keyLabelIndex = 187

	// key = label.  value = datatype/common/proto/AffinityTable serialization
	keyAffinities = 188

	// Used to store max label on commit for each version of the instance.
	keyLabelMax = 237

	// Stores the single repo-wide max label for the instance.  Used for new labels on split.
	keyRepoLabelMax = 238

	// Stores the single repo-wide next label for the instance.  Overrides max labels.
	keyRepoNextLabel = 239
)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyProperties:
		return "labelmap properties key"
	case keyLabelBlock:
		return "labelmap scale + block coord key"
	case keyLabelIndex:
		return "labelmap label index key"
	case keyAffinities:
		return "labelmap affinities key"
	case keyLabelMax:
		return "labelmap label max key"
	case keyRepoLabelMax:
		return "labelmap repo label max key"
	case keyRepoNextLabel:
		return "labelmap next label key"
	default:
	}
	return "unknown labelmap key"
}

var (
	maxLabelTKey     = storage.NewTKey(keyLabelMax, nil)
	maxRepoLabelTKey = storage.NewTKey(keyRepoLabelMax, nil)
	nextLabelTKey    = storage.NewTKey(keyRepoNextLabel, nil)
)

// NewBlockTKey returns a TKey for a label block, which is a slice suitable for
// lexicographical ordering on zyx coordinates.
func NewBlockTKey(scale uint8, idx dvid.Index) storage.TKey {
	izyx := idx.(*dvid.IndexZYX)
	return NewBlockTKeyByCoord(scale, izyx.ToIZYXString())
}

// NewBlockTKeyByCoord returns a TKey for a block coord in string format.
func NewBlockTKeyByCoord(scale uint8, izyx dvid.IZYXString) storage.TKey {
	buf := make([]byte, 13)
	buf[0] = byte(scale)
	copy(buf[1:], []byte(izyx))
	return storage.NewTKey(keyLabelBlock, buf)
}

// DecodeBlockTKey returns a spatial index from a label block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeBlockTKey(tk storage.TKey) (scale uint8, idx *dvid.IndexZYX, err error) {
	ibytes, err := tk.ClassBytes(keyLabelBlock)
	if err != nil {
		return 0, nil, err
	}
	if len(ibytes) != 13 {
		err = fmt.Errorf("bad labelmap block key of %d bytes: %v", len(ibytes), ibytes)
		return
	}
	scale = uint8(ibytes[0])
	idx = new(dvid.IndexZYX)
	if err = idx.IndexFromBytes(ibytes[1:]); err != nil {
		err = fmt.Errorf("Cannot recover ZYX index from image block key %v: %v\n", tk, err)
	}
	return
}

// NewLabelIndexTKey returns a TKey corresponding to a label.
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

// NewAffinitiesTKey returns a TKey corresponding to a label's affinities.
func NewAffinitiesTKey(label uint64) storage.TKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, label)
	return storage.NewTKey(keyAffinities, buf)
}

// DecodeAffinitiesTKey parses a TKey and returns the corresponding label.
func DecodeAffinitiesTKey(tk storage.TKey) (label uint64, err error) {
	ibytes, err := tk.ClassBytes(keyAffinities)
	if err != nil {
		return
	}
	label = binary.BigEndian.Uint64(ibytes[0:8])
	return
}
