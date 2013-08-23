/*
	This file supports voxel-based implementations of dvid.Index, etc.
*/

package voxels

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/janelia-flyem/dvid/dvid"
)

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
func (i IndexZYX) InvertIndex() dvid.ChunkIndex {
	z := int32(binary.BigEndian.Uint32([]byte(i[0:4])))
	y := int32(binary.BigEndian.Uint32([]byte(i[4:8])))
	x := int32(binary.BigEndian.Uint32([]byte(i[8:12])))
	return BlockZYX{BlockCoord{x, y, z}}
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.
func (i IndexZYX) Hash(n int) int {
	z := binary.BigEndian.Uint32([]byte(i[0:4]))
	y := binary.BigEndian.Uint32([]byte(i[4:8]))
	x := binary.BigEndian.Uint32([]byte(i[8:12]))
	// Make sure that any scans along x, y, or z directions will
	// cause distribution to different handlers.
	return int(x+y+z) % n
}

func (i IndexZYX) Scheme() string {
	return "ZYX Indexing"
}

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i IndexZYX) OffsetToBlock(blockSize Point3d) (coord Coord) {
	z := int32(binary.BigEndian.Uint32([]byte(i[0:4])))
	y := int32(binary.BigEndian.Uint32([]byte(i[4:8])))
	x := int32(binary.BigEndian.Uint32([]byte(i[8:12])))
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
	BlockCoord
}

// MakeIndex returns a 1d Index from a 3d ZYX coordinate.
func (bc BlockZYX) MakeIndex() (i dvid.Index, err error) {
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
