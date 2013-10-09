/*
	Data type labels64 tailors the voxels data type for 64-bit label images.  It simply
	wraps the voxels package, setting Channels (1) and BytesPerChannel(4).
*/

package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	labels := NewDatatype(1, 8)
	labels.DatatypeID = &datastore.DatatypeID{
		Name:    "labels64",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/labels64.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(labels)
}
