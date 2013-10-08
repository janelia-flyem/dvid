/*
	This file supports multichannel voxel-based implementations of dvid.Index, etc.
*/

package multichan16

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

// IndexCZYX implements the Index interface and provides simple indexing on C, then Z,
// then Y, then X.
type IndexCZYX struct {
	Channel int32
	voxels.BlockCoord
}

func (i IndexCZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i IndexCZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i.Channel)
	binary.Write(buf, binary.BigEndian, i.BlockCoord[2])
	binary.Write(buf, binary.BigEndian, i.BlockCoord[1])
	binary.Write(buf, binary.BigEndian, i.BlockCoord[0])
	return buf.Bytes()
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IndexCZYX) Hash(n int) int {
	return int(i.BlockCoord[0]+i.BlockCoord[1]+i.BlockCoord[2]) % n
}

func (i IndexCZYX) Scheme() string {
	return "CZYX Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexCZYX) IndexFromBytes(b []byte) (dvid.Index, error) {
	c := int32(binary.BigEndian.Uint16(b[0:4]))
	z := int32(binary.BigEndian.Uint32(b[4:8]))
	y := int32(binary.BigEndian.Uint32(b[8:12]))
	x := int32(binary.BigEndian.Uint32(b[12:16]))
	return &IndexCZYX{c, voxels.BlockCoord{x, y, z}}, nil
}

// ------- voxels.ZYXIndexer interface ----------

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i IndexCZYX) OffsetToBlock(blockSize voxels.Point3d) (coord voxels.Coord) {
	coord[0] = i.BlockCoord[0] * blockSize[0]
	coord[1] = i.BlockCoord[1] * blockSize[1]
	coord[2] = i.BlockCoord[2] * blockSize[2]
	return
}

func (i IndexCZYX) X() int32 {
	return i.BlockCoord[0]
}

func (i IndexCZYX) Y() int32 {
	return i.BlockCoord[1]
}

func (i IndexCZYX) Z() int32 {
	return i.BlockCoord[2]
}

func (i *IndexCZYX) ExtendMin(izyx voxels.ZYXIndexer) {
	if i.BlockCoord[0] > izyx.X() {
		i.BlockCoord[0] = izyx.X()
	}
	if i.BlockCoord[1] > izyx.Y() {
		i.BlockCoord[1] = izyx.Y()
	}
	if i.BlockCoord[2] > izyx.Z() {
		i.BlockCoord[2] = izyx.Z()
	}
}

func (i *IndexCZYX) ExtendMax(izyx voxels.ZYXIndexer) {
	if i.BlockCoord[0] < izyx.X() {
		i.BlockCoord[0] = izyx.X()
	}
	if i.BlockCoord[1] < izyx.Y() {
		i.BlockCoord[1] = izyx.Y()
	}
	if i.BlockCoord[2] < izyx.Z() {
		i.BlockCoord[2] = izyx.Z()
	}
}
