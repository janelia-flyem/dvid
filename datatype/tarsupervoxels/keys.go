/*
	This file supports the keyspace for the tarsupervoxels data type.
*/

package tarsupervoxels

import (
	"fmt"
	"strconv"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// the byte id for a standard key of a tarsupervoxels keyvalue
	keyStandard = 133
)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	if tkc == keyStandard {
		return "supervoxel data filename key"
	}
	return "unknown tarsupervoxels key"
}

// NewTKey returns the type-specific key corresponding to a supervoxel id in
// simple ASCII bytes.
func NewTKey(supervoxel uint64, ext string) (storage.TKey, error) {
	filename := strconv.FormatUint(supervoxel, 10) + "." + ext
	return storage.NewTKey(keyStandard, []byte(filename)), nil
}

// DecodeTKey returns the supervoxel id corresponding to the type-specific ke.y
func DecodeTKey(tk storage.TKey) (supervoxel uint64, ext string, err error) {
	var fnameBytes []byte
	if fnameBytes, err = tk.ClassBytes(keyStandard); err != nil {
		return
	}
	_, err = fmt.Sscanf(string(fnameBytes), "%s.%d", &supervoxel, &ext)
	return
}
