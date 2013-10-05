/*
	Data type labels32 tailors the voxels data type for 32-bit label images.  It simply
	wraps the voxels package, setting Channels (1) and BytesPerChannel(4).
*/

package voxels

import (
	"github.com/janelia-flyem/dvid/datastore"
)

func init() {
	labels := NewDatatype(1, 4)
	labels.DatatypeID = &datastore.DatatypeID{
		Name:    "labels32",
		Url:     "github.com/janelia-flyem/dvid/voxels/labels32.go",
		Version: "0.6",
	}
	datastore.RegisterDatatype(labels)
}
