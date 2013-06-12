/*
	This file supports indexing using a variety of schemes.
*/

package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	_ "fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// ChunkIndex represents an index to a datatype-specific partition of data.
// For example, a voxels data type would have a 3d block coordinate.  Chunk
// indices do *not* have to be 3d coordinates but can be optimized for
// a data type's internal data structure.
type ChunkIndex interface {
	// MakeIndex returns a one-dimensional index.
	MakeIndex() (Index, error)

	// String returns a human-readable representation of the block index.
	String() string
}

// Index is a one-dimensional index, typically constructed using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to a 1-D index.
// It is assumed that implementations for this interface are castable to []byte.
type Index interface {
	// Bytes gives a slice of bytes.
	Bytes() []byte

	// InvertIndex returns a chunk index given a one-dimensional Index.
	// It is the inverse of MakeIndex().
	InvertIndex() ChunkIndex

	// Hash provides a consistent mapping from an Index to an integer (0,n]
	Hash(n int) int

	// Scheme returns a string describing the indexing scheme.
	Scheme() string

	// String returns a hexadecimal string representation
	String() string
}

// IndexIterator is a function that returns a sequence of indices and ends with nil.
type IndexIterator func() Index

// IndexIteratorMakers can make new IndexIterators.
type IndexIteratorMaker interface {
	NewIndexIterator() IndexIterator
}

// --- Standard implementations of above interfaces

// IndexZYX implements the Index interface and provides simple indexing on Z,
// then Y, then X.
type IndexZYX []byte

func (i IndexZYX) String() string {
	return hex.EncodeToString([]byte(i))
}

func (i IndexZYX) Bytes() []byte {
	return i
}

// InvertIndex decodes a spatial index into a block coordinate
func (i IndexZYX) InvertIndex() ChunkIndex {
	z := int32(binary.LittleEndian.Uint32([]byte(i[0:4])))
	y := int32(binary.LittleEndian.Uint32([]byte(i[4:8])))
	x := int32(binary.LittleEndian.Uint32([]byte(i[8:12])))
	return BlockZYX{dvid.BlockCoord{x, y, z}}
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.
func (i IndexZYX) Hash(n int) int {
	z := binary.LittleEndian.Uint32([]byte(i[0:4]))
	y := binary.LittleEndian.Uint32([]byte(i[4:8]))
	x := binary.LittleEndian.Uint32([]byte(i[8:12]))
	// Make sure that any scans along x, y, or z directions will
	// cause distribution to different handlers.
	return int(x+y+z) % n
}

func (i IndexZYX) Scheme() string {
	return "ZYX Indexing"
}

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i IndexZYX) OffsetToBlock(blockSize dvid.Point3d) (coord dvid.VoxelCoord) {
	z := int32(binary.LittleEndian.Uint32([]byte(i[0:4])))
	y := int32(binary.LittleEndian.Uint32([]byte(i[4:8])))
	x := int32(binary.LittleEndian.Uint32([]byte(i[8:12])))
	coord[0] = x * blockSize[0]
	coord[1] = y * blockSize[1]
	coord[2] = z * blockSize[2]
	return
}

// TODO -- Morton (Z-order) curve
type IndexMorton []byte

func (i IndexMorton) Scheme() string {
	return "Morton/Z-order Indexing"
}

// TODO -- Hilbert curve
type IndexHilbert []byte

func (i IndexHilbert) Scheme() string {
	return "Hilbert Indexing"
}

// BlockZYX implements the BlockIndex interface and provides blocking of
// a 3d coordinate space.
type BlockZYX struct {
	dvid.BlockCoord
}

// MakeIndex returns a 1d Index from a 3d ZYX coordinate.
func (bc BlockZYX) MakeIndex() (i Index, err error) {
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, bc.BlockCoord[2])
	if err != nil {
		return
	}
	err = binary.Write(buf, binary.BigEndian, bc.BlockCoord[1])
	if err != nil {
		return
	}
	err = binary.Write(buf, binary.BigEndian, bc.BlockCoord[0])
	i = IndexZYX(buf.Bytes())
	return
}
