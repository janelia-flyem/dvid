/*
	This file supports the keyspace for the tarsupervoxels data type.
*/

package tarsupervoxels

import (
	"encoding/binary"
	"fmt"

	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// the byte id for a standard key of a tarsupervoxels keyvalue
	keyStandard = 133
)

// NewTKey returns the type-specific key corresponding to a supervoxel id.
func NewTKey(supervoxel uint64) (storage.TKey, error) {
	ibytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(ibytes, supervoxel)
	return storage.NewTKey(keyStandard, ibytes), nil
}

// DecodeTKey returns the supervoxel id corresponding to the type-specific ke.y
func DecodeTKey(tk storage.TKey) (uint64, error) {
	ibytes, err := tk.ClassBytes(keyStandard)
	if err != nil {
		return 0, err
	}
	if len(ibytes) != 8 {
		return 0, fmt.Errorf("expected 8 bytes for type-specific key, got %d bytes", len(ibytes))
	}
	return binary.LittleEndian.Uint64(ibytes), nil
}
