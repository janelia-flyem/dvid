/*
	This file supports keyspaces for point annotation data type.
*/

package annotation

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// key is tag id.  value is serialization of the tag name and all synaptic elements it contains.
	keyTag = 70

	// key is label.  value is serialization of all synaptic elements associated with a label.
	keyLabel = 71

	// key is block coordinate.  value is serialization of synaptic elements.
	keyBlock = 72
)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyTag:
		return "annotation tag key"
	case keyLabel:
		return "annotation label key"
	case keyBlock:
		return "annotation block coord key"
	default:
	}
	return "unknown annotation key"
}

// NewTagTKey returns a TKey for a given tag.
func NewTagTKey(tag Tag) (storage.TKey, error) {
	if len(tag) == 0 {
		return nil, fmt.Errorf("empty tag not permitted")
	}
	return storage.NewTKey(keyTag, append([]byte(tag), 0)), nil
}

// DecodeTagTKey returns the Tag corresponding to this type-specific key.
func DecodeTagTKey(tk storage.TKey) (Tag, error) {
	ibytes, err := tk.ClassBytes(keyTag)
	if err != nil {
		return "", err
	}
	sz := len(ibytes) - 1
	if sz <= 0 {
		return "", fmt.Errorf("empty tag")
	}
	if ibytes[sz] != 0 {
		return "", fmt.Errorf("expected 0 byte ending tag key, got %d", ibytes[sz])
	}
	return Tag(ibytes[:sz]), nil
}

func NewLabelTKey(label uint64) storage.TKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, label)
	return storage.NewTKey(keyLabel, buf)
}

func DecodeLabelTKey(tk storage.TKey) (label uint64, err error) {
	ibytes, err := tk.ClassBytes(keyLabel)
	if err != nil {
		return
	}
	label = binary.BigEndian.Uint64(ibytes[0:8])
	return
}

func NewBlockTKey(pt dvid.ChunkPoint3d) storage.TKey {
	idx := dvid.IndexZYX(pt)
	return storage.NewTKey(keyBlock, idx.Bytes())
}

func DecodeBlockTKey(tk storage.TKey) (pt dvid.ChunkPoint3d, err error) {
	ibytes, err := tk.ClassBytes(keyBlock)
	if err != nil {
		return
	}
	var idx dvid.IndexZYX
	if err = idx.IndexFromBytes(ibytes); err != nil {
		return
	}
	pt = dvid.ChunkPoint3d(idx)
	return
}
