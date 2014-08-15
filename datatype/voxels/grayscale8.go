package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func init() {
	values := dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "grayscale",
		},
	}
	interpolable := true
	grayscale := NewType(values, interpolable)
	grayscale.Type.Name = "grayscale8"
	grayscale.Type.URL = "github.com/janelia-flyem/dvid/datatype/voxels/grayscale8.go"
	grayscale.Type.Version = "0.7"

	datastore.Register(grayscale)
}
