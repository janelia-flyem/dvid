/*
	This file supports keyspaces for image block data types.
*/

package imageblk

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// use different id from other label-type keys to improve odds that any bad use of
	// arbitrary key decoding will result in error.
	keyImageBlock = 23

	// legacy key class where extents property is stored
	metaKeyClass = 24
)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyProperties:
		return "imageblk properties key"
	case keyImageBlock:
		return "imageblk block coord key"
	default:
		return "unknown imageblk key"
	}
}

// NewTKeyByCoord returns a TKey for a block coord in string format.
func NewTKeyByCoord(izyx dvid.IZYXString) storage.TKey {
	return storage.NewTKey(keyImageBlock, []byte(izyx))
}

// NewTKey returns a type-specific key component for an image block.
// TKey = s
func NewTKey(idx dvid.Index) storage.TKey {
	izyx := idx.(*dvid.IndexZYX)
	return NewTKeyByCoord(izyx.ToIZYXString())
}

// MetaTKey provides a TKey for metadata (extents)
func MetaTKey() storage.TKey {
	return storage.NewTKey(metaKeyClass, nil)
}

// DecodeTKey returns a spatial index from a image block key.
// TODO: Extend this when necessary to allow any form of spatial indexing like CZYX.
func DecodeTKey(tk storage.TKey) (*dvid.IndexZYX, error) {
	ibytes, err := tk.ClassBytes(keyImageBlock)
	if err != nil {
		return nil, err
	}
	var zyx dvid.IndexZYX
	if err = zyx.IndexFromBytes(ibytes); err != nil {
		return nil, fmt.Errorf("Cannot recover ZYX index from image block key %v: %v\n", tk, err)
	}
	return &zyx, nil
}
