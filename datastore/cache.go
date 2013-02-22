/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

import (
	"github.com/janelia-flyem/dvid/dvid"
)

type cachedData struct {
	// The default version of the datastore
	Head UUID

	// Holds all UUIDs in open datastore.  When we construct keys, use the smaller
	// unique int per datastore instead of the full 16 byte value.  This can save
	// more than 15 bytes per key.
	Uuids map[string]int
}

type block struct {
	offset   dvid.VoxelCoord
	data     []byte
	dirty    []bool
	numDirty int
}
type blockCache map[SpatialIndex]block
