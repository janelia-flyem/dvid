/*
	This file supports voxel-based implementations of dvid.Index, etc.
*/

package voxels

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// ZYXIndexer adds access to X, Y, Z coordinates to an index.
type ZYXIndexer interface {
	dvid.Index

	OffsetToBlock(blockSize Point3d) Coord
	X() int32
	Y() int32
	Z() int32

	// ExtendMin sets this ZYXIndexer to the minimum of its value and the passed one.
	ExtendMin(ZYXIndexer)

	// ExtendMax sets this ZYXIndexer to the maximum of its value and the passed one.
	ExtendMax(ZYXIndexer)
}

// KeyToZYXIndexer takes a Key and returns an implementation of a ZYXIndexer if possible.
func KeyToZYXIndexer(key storage.Key) (ZYXIndexer, error) {
	datakey, ok := key.(*datastore.DataKey)
	if !ok {
		return nil, fmt.Errorf("Can't convert Key (%s) to DataKey", key)
	}
	zyx, ok := datakey.Index.(ZYXIndexer)
	if !ok {
		return nil, fmt.Errorf("Can't convert DataKey.Index (%s) to ZYXIndexer",
			reflect.TypeOf(datakey.Index))
	}
	return zyx, nil
}

// IndexZYX implements the Index interface and provides simple indexing on Z,
// then Y, then X.
type IndexZYX BlockCoord

func (i IndexZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i IndexZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i[2])
	binary.Write(buf, binary.BigEndian, i[1])
	binary.Write(buf, binary.BigEndian, i[0])
	return buf.Bytes()
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IndexZYX) Hash(n int) int {
	return int(i[0]+i[1]+i[2]) % n
}

func (i IndexZYX) Scheme() string {
	return "ZYX Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexZYX) IndexFromBytes(b []byte) (dvid.Index, error) {
	z := int32(binary.BigEndian.Uint32(b[0:4]))
	y := int32(binary.BigEndian.Uint32(b[4:8]))
	x := int32(binary.BigEndian.Uint32(b[8:12]))
	return &IndexZYX{x, y, z}, nil
}

// ------- ZYXIndexer interface ----------

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i IndexZYX) OffsetToBlock(blockSize Point3d) (coord Coord) {
	coord[0] = i[0] * blockSize[0]
	coord[1] = i[1] * blockSize[1]
	coord[2] = i[2] * blockSize[2]
	return
}

func (i *IndexZYX) ExtendMin(izyx ZYXIndexer) {
	if i[0] > izyx.X() {
		i[0] = izyx.X()
	}
	if i[1] > izyx.Y() {
		i[1] = izyx.Y()
	}
	if i[2] > izyx.Z() {
		i[2] = izyx.Z()
	}
}

func (i *IndexZYX) ExtendMax(izyx ZYXIndexer) {
	if i[0] < izyx.X() {
		i[0] = izyx.X()
	}
	if i[1] < izyx.Y() {
		i[1] = izyx.Y()
	}
	if i[2] < izyx.Z() {
		i[2] = izyx.Z()
	}
}

func (i IndexZYX) X() int32 {
	return i[0]
}

func (i IndexZYX) Y() int32 {
	return i[1]
}

func (i IndexZYX) Z() int32 {
	return i[2]
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
