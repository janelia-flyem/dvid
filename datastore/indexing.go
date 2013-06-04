/*
	This file supports indexing using a variety of schemes.
*/

package datastore

import (
	"encoding/binary"
	"encoding/hex"
	_ "fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// BlockIndex is an abtraction for datatype-specific block representations.
// For example, a voxels data type would have a 3d block coordinate.  Block
// indices do *not* have to be 3d coordinates but can be optimized for
// a data type's internal data structure.
type BlockIndex interface {
	// MakeIndex returns a one-dimensional index.
	MakeIndex() Index

	// String returns a human-readable representation of the block index.
	String() string
}

// Index is a one-dimensional index, typically constructed using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to a 1-D index.
// It is assumed that implementations for this interface are castable to []byte.
type Index interface {
	// Bytes gives a slice of bytes.
	Bytes() []byte

	// InvertIndex returns a block representation given a one-dimensional Index.
	// It is the inverse of MakeIndex().
	InvertIndex() BlockIndex

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

type indexType []byte

func (i indexType) String() string {
	return hex.EncodeToString([]byte(i))
}

func (i indexType) Bytes() []byte {
	return i
}

// IndexZYX implements the Index interface and provides simple indexing on Z, 
// then Y, then X.
type IndexZYX indexType

// InvertIndex decodes a spatial index into a block coordinate
func (i IndexZYX) InvertIndex() BlockIndex {
	var coord dvid.BlockCoord
	coord[2] = int32(binary.LittleEndian.Uint32([]byte(i[0:4])))
	coord[1] = int32(binary.LittleEndian.Uint32([]byte(i[4:8])))
	coord[0] = int32(binary.LittleEndian.Uint32([]byte(i[8:12])))
	return coord
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

// TODO -- Morton (Z-order) curve
type IndexMorton indexType

func (i IndexMorton) Scheme() string {
	return "Morton/Z-order Indexing"
}

// TODO -- Hilbert curve
type IndexHilbert indexType

func (i IndexHilbert) Scheme() string {
	return "Hilbert Indexing"
}

// BlockZYX implements the BlockIndex interface and provides blocking of
// a 3d coordinate space.
type BlockZYX dvid.BlockCoord

func (bc *BlockZYX) MakeIndex() Index {
	// Create buffer for index
	index := make(IndexZYX, 12, 12)
	binary.LittleEndian.PutUint32(index[0:4], uint32(coord[2]))
	binary.LittleEndian.PutUint32(index[4:8], uint32(coord[1]))
	binary.LittleEndian.PutUint32(index[8:12], uint32(coord[0]))
	return index
}
