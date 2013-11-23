/*
	Data type labels64 tailors the voxels data type for 64-bit label images.  It simply
	wraps the voxels package, setting Channels (1) and BytesPerValue(4).
*/

package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	values := []DataValue{
		{
			DataType: "uint64",
			Label:    "labels64",
		},
	}
	labels := NewDatatype(1, 8, values)
	labels.DatatypeID = &datastore.DatatypeID{
		Name:    "labels64",
		Url:     "github.com/janelia-flyem/dvid/datatype/voxels/labels64.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(labels)
}
