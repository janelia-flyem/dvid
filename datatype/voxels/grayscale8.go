package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	grayscale := NewDatatype(1, 1)
	grayscale.DatatypeID = &datastore.DatatypeID{
		Name:    "grayscale8",
		Url:     "github.com/janelia-flyem/dvid/voxels/grayscale8.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(grayscale)
}
