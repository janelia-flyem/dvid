/*
	This file supports spatial indexing of blocks using a variety of schemes.
*/

package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/janelia-flyem/dvid/dvid"
)

// SpatialIndex represents a n-dimensional coordinate according to some
// spatial indexing scheme.
type SpatialIndex string

// SpatialIndexScheme represents a particular spatial indexing scheme that
// hopefully maximizes sequential reads/writes when used as part of a key.
type SpatialIndexScheme byte

// Types of spatial indexing
const (
	// Simple indexing on Z, then Y, then X
	SIndexZYX SpatialIndexScheme = iota

	// Hilbert curve
	SIndexHilbert

	// Diagonal indexing within 2d plane, z separate
	SIndexXYDiagonal
)

// MakeSpatialIndex returns a string encoding a coordinate depending on the
// spatial index scheme of the data type.
func MakeSpatialIndex(t TypeService, coord dvid.BlockCoord) SpatialIndex {
	// Create buffer for spatial index
	index := make([]byte, 12, 12)
	switch t.SpatialIndexing() {
	case SIndexZYX:
		binary.LittleEndian.PutUint32(index[:4], uint32(coord[2]))
		binary.LittleEndian.PutUint32(index[4:8], uint32(coord[1]))
		binary.LittleEndian.PutUint32(index[8:], uint32(coord[0]))
	default:
		panic(fmt.Sprintf("MakeSpatialIndex: Unsupported spatial indexing scheme (%d)!",
			t.SpatialIndexing()))
	}
	return SpatialIndex(index)
}

// BlockCoord decodes a spatial index into a block coordinate
func (si SpatialIndex) BlockCoord(t TypeService) (coord dvid.BlockCoord) {
	switch t.SpatialIndexing() {
	case SIndexZYX:
		coord[2] = binary.LittleEndian.Uint32([]byte(si[0:4]))
		coord[1] = binary.LittleEndian.Uint32([]byte(si[4:8]))
		coord[0] = binary.LittleEndian.Uint32([]byte(si[8:12]))
	default:
		panic(fmt.Sprintf("BlockCoord: Unsupported spatial indexing scheme (%d)!",
			t.SpatialIndexing()))
	}
	return
}

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the spatial index.
func (si SpatialIndex) OffsetToBlock(t TypeService) (coord dvid.VoxelCoord) {
	blockCoord := si.BlockCoord(t)
	blockSize := t.BlockSize()
	coord[0] = blockCoord[0] * blockSize[0]
	coord[1] = blockCoord[1] * blockSize[1]
	coord[2] = blockCoord[2] * blockSize[2]
	return
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values. 
func (si SpatialIndex) Hash(t TypeService, n int) int {
	switch t.SpatialIndexing() {
	case SIndexZYX:
		z := binary.LittleEndian.Uint32([]byte(si[0:4]))
		y := binary.LittleEndian.Uint32([]byte(si[4:8]))
		x := binary.LittleEndian.Uint32([]byte(si[8:12]))
		// Make sure that any scans along x, y, or z directions will 
		// cause distribution to different handlers. 
		return (x + y + z) % n
	default:
		panic(fmt.Sprintf("BlockCoord: Unsupported spatial indexing scheme (%d)!",
			t.SpatialIndexing()))
	}
	return 0
}

func (si SpatialIndex) String() string {
	return hex.EncodeToString([]byte(si))
}

// SpatialIterator is a function that returns a sequence of spatial keys and ends with nil. 
type SpatialIterator func() []byte

// Iterator returns a SpatialIterator for a given data type and volume to traverse.
func NewSpatialIterator(t TypeService, v dvid.Volume) SpatialIterator {
	// Create buffer for spatial key
	spatialKey := make([]byte, 12, 12)

	// Setup traversal
	startVoxel := v.Origin()
	endVoxel := v.EndVoxel()
	switch t.SpatialIndexing() {
	case SIndexZYX:
		// Returns a closure that iterates in x, then y, then z
		startBlockCoord := startVoxel.BlockCoord(t.BlockSize())
		endBlockCoord := endVoxel.BlockCoord(t.BlockSize())
		z := startBlockCoord[2]
		y := startBlockCoord[1]
		x := startBlockCoord[0]
		return func() []byte {
			if z > endBlockCoord[2] {
				return nil
			}
			binary.LittleEndian.PutUint32(spatialKey[:4], uint32(z))
			binary.LittleEndian.PutUint32(spatialKey[4:8], uint32(y))
			binary.LittleEndian.PutUint32(spatialKey[8:], uint32(x))
			x++
			if x > endBlockCoord[0] {
				x = startBlockCoord[0]
				y++
			}
			if y > endBlockCoord[1] {
				y = startBlockCoord[1]
				z++
			}
			return spatialKey
		}
	default:
		panic(fmt.Sprintf("Unimplemented Iterator called for spatial index scheme: %s",
			t.SpatialIndexing()))
	}
}
