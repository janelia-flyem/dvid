/*
	This file supports the keyspace for the tarsupervoxels data type.
*/

package tarsupervoxels

import (
	"fmt"
	"strconv"

	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// the byte id for a standard key of a tarsupervoxels keyvalue
	keyStandard = 133
)

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
