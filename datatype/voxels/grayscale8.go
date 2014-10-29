package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var grayscale8EncodeFormat dvid.DataValues

func init() {
	grayscale8EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "grayscale",
		},
	}
	interpolable := true
	grayscale := NewType(grayscale8EncodeFormat, interpolable)
	grayscale.Type.Name = "grayscale8"
	grayscale.Type.URL = "github.com/janelia-flyem/dvid/datatype/voxels/grayscale8.go"
	grayscale.Type.Version = "0.7"

	datastore.Register(grayscale)
}

func Grayscale8EncodeFormat() dvid.DataValues {
	return grayscale8EncodeFormat
}
