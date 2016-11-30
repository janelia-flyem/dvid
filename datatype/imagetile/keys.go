/*
	This file supports imagetile-specific keys.
*/

package imagetile

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// TODO -- Introduce imagetile key type to allow for expansion in future.  Since we currently
// have datasets using old keys, hold off on upgrade until a breaking change is introduced for all keys.

//const (
//	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
//	keyUnknown storage.TKeyClass = iota
//
//	keyTileCoord = 177
//)

type TileReq struct {
	tile  dvid.ChunkPoint3d
	plane dvid.DataShape
	scale Scaling
}

func (d *Data) ParseTileReq(r *http.Request, parts []string) (TileReq, error) {
	if len(parts) < 7 {
		return TileReq{}, fmt.Errorf("'tile' request must be following by plane, scale level, and tile coordinate")
	}
	shapeStr, scalingStr, coordStr := parts[4], parts[5], parts[6]

	// Construct the index for this tile
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		err = fmt.Errorf("Illegal tile plane: %s (%v)", planeStr, err)
		return TileReq{}, err
	}
	scale, err := strconv.ParseUint(scalingStr, 10, 8)
	if err != nil {
		err = fmt.Errorf("Illegal tile scale: %s (%v)", scalingStr, err)
		return TileReq{}, err
	}
	tileCoord, err := dvid.StringToChunkPoint3d(coordStr, "_")
	if err != nil {
		err = fmt.Errorf("Illegal tile coordinate: %s (%v)", coordStr, err)
		return TileReq{}, err
	}
	return TileReq{tileCoord, plane, Scaling(scale)}, nil
}

func NewTileReq(tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling) TileReq {
	return TileReq{tile, plane, scale}
}

// NewTKeyByTileReq returns an imagetile-specific key component based on a tile request.
func NewTKeyByTileReq(req TileReq) storage.TKey {
	return NewTKey(req.tile, req.plane, req.scale)
}

// NewTKey returns an imagetile-specific key component based on the components of a tile request.
func NewTKey(tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling) storage.TKey {
	var buf bytes.Buffer
	buf.Write(plane.Bytes())
	buf.WriteByte(byte(scale))
	buf.WriteByte(byte(3))
	idx := dvid.IndexZYX(tile)
	buf.Write(idx.Bytes())
	return buf.Bytes()
}

// DecodeTKey returns the components of a tile request based on an imagetile-specific key component.
func DecodeTKey(tk storage.TKey) (tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling, err error) {
	if len(tk) < 21 {
		err = fmt.Errorf("imagetile-specific key component has too few bytes (%d)", len(tk))
		return
	}
	plane, err = dvid.BytesToDataShape(tk[0:dvid.DataShapeBytes])
	if err != nil {
		return
	}
	scale = Scaling(tk[dvid.DataShapeBytes])
	var idx dvid.IndexZYX
	if err = idx.IndexFromBytes(tk[dvid.DataShapeBytes+2:]); err != nil {
		return
	}
	tile = dvid.ChunkPoint3d(idx)
	return
}
