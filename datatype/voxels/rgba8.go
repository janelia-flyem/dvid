/*
	Data type rgba8 tailors the voxels data type for 8-bit RGBA images.  It simply
	wraps the voxels package, setting Channels (4) and BytesPerValue(1).
*/

package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func init() {
	values := dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "red",
		},
		{
			T:     dvid.T_uint8,
			Label: "green",
		},
		{
			T:     dvid.T_uint8,
			Label: "blue",
		},
		{
			T:     dvid.T_uint8,
			Label: "alpha",
		},
	}
	interpolable := true
	rgba := NewDatatype(values, interpolable)
	rgba.DatatypeID = &datastore.DatatypeID{
		Name:    "rgba8",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/rgba8.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(rgba)
}
