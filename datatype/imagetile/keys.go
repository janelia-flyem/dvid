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

	// key = legacy tile.  Must be 3.
	keyLegacyTile = 3
)

func describeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyLegacyTile:
		return "imagetile tile key"
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
	// NOTE: For legacy reasons, first two bytes must be 3 and then 2.  The current storage.TKey
	// formatting has a TKeyClass for first byte and then 1 for second, but this second byte is only
	// used for min/max key generation, so using 2 is ok as well.
	var buf bytes.Buffer
	buf.Write(plane.Bytes())

	buf.WriteByte(byte(scale))
	buf.WriteByte(byte(3))
	idx := dvid.IndexZYX(tile)
	buf.Write(idx.Bytes())
	return buf.Bytes(), nil
}

// NewTKeyByTileReq returns an imagetile-specific key component based on a tile request.
func NewTKeyByTileReq(req TileReq) (storage.TKey, error) {
	return NewTKey(req.tile, req.plane, req.scale)
}

// DecodeTKey returns the components of a tile request based on an imagetile-specific key component.
func DecodeTKey(tk storage.TKey) (tile dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling, err error) {
	if len(tk) != 21 {
		err = fmt.Errorf("expected 21 bytes for imagetile type-specific key, got %d bytes instead", len(tk))
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
