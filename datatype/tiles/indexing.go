/*
	This file supports tile-based implementations of dvid.Index, etc.
*/

package tiles

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

// IndexTile implements the Index interface and provides simple indexing on Z,
// then Y, then X.
type IndexTile struct {
	plane   voxels.DataShape
	scaling uint8
	coord   []int32
}

func (i IndexTile) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i IndexTile) Bytes() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(i.plane))
	buf.WriteByte(byte(i.scaling))
	binary.Write(buf, binary.BigEndian, i.coord[2])
	binary.Write(buf, binary.BigEndian, i.coord[1])
	binary.Write(buf, binary.BigEndian, i.coord[0])
	return buf.Bytes()
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IndexTile) Hash(n int) int {
	return int(i.coord[0]+i.coord[1]+i.coord[2]) % n
}

func (i IndexTile) Scheme() string {
	return "Tile Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexTile) IndexFromBytes(b []byte) (dvid.Index, error) {
	if len(b) < 14 {
		return nil, fmt.Errorf("Illegal IndexTile: too few bytes (%d)", len(b))
	}
	z := int32(binary.BigEndian.Uint32(b[2:6]))
	y := int32(binary.BigEndian.Uint32(b[6:10]))
	x := int32(binary.BigEndian.Uint32(b[10:14]))
	index := &IndexTile{
		plane:   voxels.DataShape(b[0]),
		scaling: uint8(b[1]),
		coord:   []int32{x, y, z},
	}
	return index, nil
}
