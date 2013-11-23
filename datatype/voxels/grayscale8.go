package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	values := []DataValue{
		{
			DataType: "uint8",
			Label:    "grayscale",
		},
	}
	grayscale := NewDatatype(1, 1, values)
	grayscale.DatatypeID = &datastore.DatatypeID{
		Name:    "grayscale8",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/grayscale8.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(grayscale)
}
