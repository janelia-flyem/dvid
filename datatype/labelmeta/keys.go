/*
	This file supports keyspaces for point labelmeta data type.
*/

package labelmeta

import (
	"encoding/binary"
	"encoding/json"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// key is tag id.  value is serialization of the tag name and all labelmeta data it contains.
	keyTag = 70

	// key is label.  value is serialization of all labelmeta data associated with a label.
	keyLabel = 71

	// key is block coordinate.  value is serialization of labelmeta data.
	keyBlock = 72

	// key is label status. value is serialization of the label status name and all labelmeta data it contains
	keyStatus = 73
)

func NewStatusTKey(status LabelmetaStatus) storage.TKey {
     ibytes, err := json.Marshal(status)
     if err != nil {
             return nil
     }
     return storage.NewTKey(keyStatus, ibytes)
}

func DecodeStatusTKey(tk storage.TKey) ([]byte, error) {
     ibytes, err := tk.ClassBytes(keyStatus)
     if err != nil {
             return nil, err
     }
     return ibytes, nil
}

func NewTagTKey(tag Tag) storage.TKey {
	return storage.NewTKey(keyTag, []byte(tag))
}

func DecodeTagTKey(tk storage.TKey) (Tag, error) {
	ibytes, err := tk.ClassBytes(keyTag)
	if err != nil {
		return "", err
	}
	return Tag(ibytes), nil
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
