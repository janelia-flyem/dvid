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
	grayscale := NewDatatype(values, interpolable)
	grayscale.DatatypeID = &datastore.DatatypeID{
		Name:    "grayscale8",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/grayscale8.go",
		Version: "0.6",
	}
	datastore.Register(grayscale)
}
