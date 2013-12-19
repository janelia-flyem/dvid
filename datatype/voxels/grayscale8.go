package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	values := DataValues{
		{
			DataType: "uint8",
			Label:    "grayscale",
		},
	}
	grayscale := NewDatatype(values)
	grayscale.DatatypeID = &datastore.DatatypeID{
		Name:    "grayscale8",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/grayscale8.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(grayscale)
}
