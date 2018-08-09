/*
	This file supports imagetile-specific keys.
*/

package imagetile

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// TODO -- Introduce imagetile key type to allow for expansion in future.  Since we currently
// have datasets using old keys, hold off on upgrade until a breaking change is introduced for all keys.

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// key = legacy 2d tile
	keyTile2d = 2

	// key = legacy 3d tile
	keyTile3d = 3
)

func describeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyTile2d:
		return "imagetile 2d tile key"
	case keyTile3d:
		return "imagetile 3d tile key"
	default:
		return "unknown imagetile key class"
	}
}

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	return describeTKeyClass(tkc)
}

// NewTKey returns an imagetile-specific key component based on the components of a tile request.
func NewTKey(tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling) (tk storage.TKey, err error) {
	var tkc storage.TKeyClass
	switch plane.TotalDimensions() {
	case 2:
		tkc = keyTile2d
	case 3:
		tkc = keyTile3d
	default:
		err = fmt.Errorf("imagetile only supports 2d and 3d tiles at this time")
		return
	}
	var buf bytes.Buffer

	planeBytes := plane.Bytes() // only necessary to handle legacy case before introduction of TKeyClass
	buf.Write(planeBytes[1:])
	buf.WriteByte(byte(scale))
	buf.WriteByte(byte(3))
	idx := dvid.IndexZYX(tile)
	buf.Write(idx.Bytes())
	return storage.NewTKey(tkc, buf.Bytes()), nil
}

// NewTKeyByTileReq returns an imagetile-specific key component based on a tile request.
func NewTKeyByTileReq(req TileReq) (storage.TKey, error) {
	return NewTKey(req.tile, req.plane, req.scale)
}

// DecodeTKey returns the components of a tile request based on an imagetile-specific key component.
func DecodeTKey(tk storage.TKey) (tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling, err error) {
	var tkc storage.TKeyClass
	tkc, err = tk.Class()
	if err != nil {
		return
	}
	switch tkc {
	case keyTile2d:
	case keyTile3d:
	default:
		err = fmt.Errorf("bad imagetile key: %s", describeTKeyClass(tkc))
		return
	}
	dataShapeBytes := make([]byte, dvid.DataShapeBytes)
	dataShapeBytes[0] = byte(tkc)
	tkbytes, _ := tk.ClassBytes(tkc)
	copy(dataShapeBytes[1:], tkbytes[:dvid.DataShapeBytes-1])
	plane, err = dvid.BytesToDataShape(dataShapeBytes)
	if err != nil {
		return
	}
	scale = Scaling(tkbytes[dvid.DataShapeBytes-1])
	var idx dvid.IndexZYX
	if err = idx.IndexFromBytes(tkbytes[dvid.DataShapeBytes+1:]); err != nil {
		return
	}
	tile = dvid.ChunkPoint3d(idx)
	return
}

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
