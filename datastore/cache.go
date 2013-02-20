/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

type cachedData struct {
	// The default version of the datastore
	Head UUID

	// Holds all UUIDs in open datastore.  When we construct keys, use the smaller
	// unique int per datastore instead of the full 16 byte value.  This can save
	// more than 15 bytes per key.
	Uuids map[string]int
}

// blockBuffer will be allocated to datastore block size (nx x ny x nz x # bytes/voxel)
// on initialization.  A running count of # of bytes overwritten lets us know when
// we can write the block.
type blockBuffer struct {
	data         map[string][]byte
	dirty        map[string][]byte
	writtenBytes int
}
