/*
	This file supports indexing of blocks uing a variety of schemes.
*/

package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// Index encodes partitioning of a dataset, typically using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to
// a 1-D index.
type Index string

// IndexScheme represents a particular indexing scheme.  Indexing schemes
// should try to maximize sequential reads/writes when used as part of a key.
type IndexScheme byte

// Types of indexing
const (
	// Simple indexing on Z, then Y, then X
	SchemeIndexZYX IndexScheme = iota

	// TODO -- Morton (Z-order) curve
	SchemeIndexMorton

	// TODO -- Hilbert curve
	SchemeIndexHilbert

	// TODO -- Diagonal indexing within 2d plane, z separate
	SchemeIndexXYDiagonal

	// TODO -- Spatiotemporal indexing that handles time as well as 3/4D
)

func (scheme IndexScheme) String() string {
	switch scheme {
	case SchemeIndexZYX:
		return "ZYX Indexing"
	case SchemeIndexMorton:
		return "Morton/Z-order Indexing"
	case SchemeIndexHilbert:
		return "Hilbert Indexing"
	case SchemeIndexXYDiagonal:
		return "Diagonal indexing within XY, then Z"
	}
	return "Unknown Indexing Scheme"
}

// MakeIndex returns a string encoding a subdivision depending on the
// index scheme of the data type.
func MakeIndex(t TypeService, coord dvid.BlockCoord) []byte {
	// Create buffer for index
	index := make([]byte, 12, 12)
	switch t.IndexScheme() {
	case SchemeIndexZYX:
		binary.LittleEndian.PutUint32(index[0:4], uint32(coord[2]))
		binary.LittleEndian.PutUint32(index[4:8], uint32(coord[1]))
		binary.LittleEndian.PutUint32(index[8:12], uint32(coord[0]))
	default:
		panic(fmt.Sprintf("MakeIndex: Unsupported indexing scheme (%d)!", t.IndexScheme()))
	}
	return index
}

// BlockCoord decodes a spatial index into a block coordinate
func (i Index) BlockCoord(t TypeService) (coord dvid.BlockCoord) {
	switch t.IndexScheme() {
	case SchemeIndexZYX:
		coord[2] = int32(binary.LittleEndian.Uint32([]byte(i[0:4])))
		coord[1] = int32(binary.LittleEndian.Uint32([]byte(i[4:8])))
		coord[0] = int32(binary.LittleEndian.Uint32([]byte(i[8:12])))
	default:
		panic(fmt.Sprintf("BlockCoord: Unsupported indexing scheme (%d)!", t.IndexScheme()))
	}
	return
}

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i Index) OffsetToBlock(t TypeService) (coord dvid.VoxelCoord) {
	blockCoord := i.BlockCoord(t)
	blockSize := t.BlockSize()
	coord[0] = blockCoord[0] * blockSize[0]
	coord[1] = blockCoord[1] * blockSize[1]
	coord[2] = blockCoord[2] * blockSize[2]
	return
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values. 
func (i Index) Hash(t TypeService, n int) int {
	switch t.IndexScheme() {
	case SchemeIndexZYX:
		z := binary.LittleEndian.Uint32([]byte(i[0:4]))
		y := binary.LittleEndian.Uint32([]byte(i[4:8]))
		x := binary.LittleEndian.Uint32([]byte(i[8:12]))
		// Make sure that any scans along x, y, or z directions will 
		// cause distribution to different handlers. 
		return int(x+y+z) % n
	default:
		panic(fmt.Sprintf("BlockCoord: Unsupported indexing scheme (%d)!", t.IndexScheme()))
	}
	return 0
}

func (i Index) String() string {
	return hex.EncodeToString([]byte(i))
}

// IndexIterator is a function that returns a sequence of indices and ends with nil. 
type IndexIterator func() []byte

// Iterator returns a NewIndexIterator for a given data type and volume to traverse.
func NewIndexIterator(data DataStruct) IndexIterator {
	// Setup traversal
	startVoxel := data.Origin()
	endVoxel := data.EndVoxel()

	switch data.IndexScheme() {
	case SchemeIndexZYX:
		// Returns a closure that iterates in x, then y, then z
		startBlockCoord := startVoxel.BlockCoord(data.BlockSize())
		endBlockCoord := endVoxel.BlockCoord(data.BlockSize())
		z := startBlockCoord[2]
		y := startBlockCoord[1]
		x := startBlockCoord[0]
		return func() []byte {
			//dvid.Fmt(dvid.Debug, "IndexIterator: start at (%d,%d,%d)\n", x, y, z)
			if z > endBlockCoord[2] {
				return nil
			}
			indexKey := MakeIndex(data, dvid.BlockCoord{x, y, z})
			x++
			if x > endBlockCoord[0] {
				x = startBlockCoord[0]
				y++
			}
			if y > endBlockCoord[1] {
				y = startBlockCoord[1]
				z++
			}
			return indexKey
		}
	default:
		panic(fmt.Sprintf("Unimplemented IndexIterator called for index scheme: %s",
			data.IndexScheme()))
	}
	return nil
}
