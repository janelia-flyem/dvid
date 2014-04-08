/*
	This file supports multiscale2d-specific implementations of dvid.Index, etc.
*/

package multiscale2d

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// IndexTile implements the Index interface and contains tile coordinates where (0,0,...) is
// at the coordinate (not index) origin.  Tile coordinates are converted to unsigned integers
// when serialized to bytes.
type IndexTile struct {
	dvid.IndexZYX
	plane   dvid.DataShape
	scaling Scaling
}

func NewIndexTile(i dvid.IndexZYX, plane dvid.DataShape, scaling Scaling) *IndexTile {
	return &IndexTile{i, plane, scaling}
}

func (i IndexTile) Duplicate() dvid.Index {
	dupIndex := i.IndexZYX.Duplicate().(dvid.IndexZYX)
	return IndexTile{dupIndex, i.plane.Duplicate(), i.scaling}
}

func (i IndexTile) String() string {
	return hex.EncodeToString(i.Bytes())
}

func (i IndexTile) Scheme() string {
	return "Tile Indexing"
}

// Bytes returns a byte representation of the Index.
func (i IndexTile) Bytes() []byte {
	buf := bytes.NewBuffer(i.plane.Bytes())
	buf.WriteByte(byte(i.scaling))
	buf.WriteByte(byte(i.IndexZYX.NumDims()))
	buf.Write(i.IndexZYX.Bytes())
	return buf.Bytes()
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexTile) IndexFromBytes(b []byte) (dvid.Index, error) {
	if len(b) < 21 {
		return nil, fmt.Errorf("Illegal IndexTile: too few bytes (%d)", len(b))
	}
	dataShape, err := dvid.BytesToDataShape(b[0:dvid.DataShapeBytes])
	if err != nil {
		return nil, err
	}
	scaling := Scaling(b[dvid.DataShapeBytes])
	index, err := i.IndexZYX.IndexFromBytes(b[dvid.DataShapeBytes+2:])
	if err != nil {
		return nil, err
	}
	indexZYX, ok := index.(dvid.IndexZYX)
	if !ok {
		return nil, fmt.Errorf("Could not decode index tile bytes to IndexZYX: %x",
			b[dvid.DataShapeBytes+2:])
	}
	return &IndexTile{indexZYX, dataShape, scaling}, nil
}
