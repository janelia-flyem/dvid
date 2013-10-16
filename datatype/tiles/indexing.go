/*
	This file supports tile-based implementations of dvid.Index, etc.
*/

package tiles

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// IndexTile implements the Index interface and provides simple indexing on Z,
// then Y, then X.
type IndexTile struct {
	plane   dvid.DataShape
	scaling uint8
	coord   dvid.Point
}

func (i IndexTile) Duplicate() dvid.Index {
	return IndexTile{i.plane.Duplicate(), i.scaling, i.coord.Duplicate()}
}

func (i IndexTile) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i IndexTile) Bytes() []byte {
	buf := bytes.NewBuffer(i.plane.Bytes())
	buf.WriteByte(byte(i.scaling))
	buf.WriteByte(byte(i.coord.NumDims()))
	numDims := int(i.coord.NumDims())
	for dim := numDims - 1; dim >= 0; dim-- {
		binary.Write(buf, binary.BigEndian, i.coord.Value(uint8(dim)))
	}
	return buf.Bytes()
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IndexTile) Hash(n int) int {
	var sum int32
	for dim := uint8(0); dim < i.coord.NumDims(); dim++ {
		sum += i.coord.Value(dim)
	}
	return int(sum) % n
}

func (i IndexTile) Scheme() string {
	return "Tile Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexTile) IndexFromBytes(b []byte) (dvid.Index, error) {
	if len(b) < 21 {
		return nil, fmt.Errorf("Illegal IndexTile: too few bytes (%d)", len(b))
	}
	dims := int(b[dvid.DataShapeBytes+1])
	coord := make([]int32, dims)
	for dim := dims - 1; dim >= 0; dim-- {
		i := dvid.DataShapeBytes + 2 + 4*dim
		j := i + 4
		coord[dim] = int32(binary.BigEndian.Uint32(b[i:j]))
	}
	dataShape, err := dvid.BytesToDataShape(b[0:dvid.DataShapeBytes])
	if err != nil {
		return nil, err
	}
	point, err := dvid.SliceToPoint(coord)
	if err != nil {
		return nil, err
	}
	index := &IndexTile{
		plane:   dataShape,
		scaling: uint8(b[dvid.DataShapeBytes]),
		coord:   point,
	}
	return index, nil
}
