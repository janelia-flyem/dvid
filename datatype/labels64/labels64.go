/*
	Package labels64 tailors the voxels data type for 64-bit label images.  It simply
	wraps the voxels package, setting NumChannels (1) and BytesPerVoxel(8).
*/
package labels64

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
)

const Version = "0.6"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/labels64"

type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax voxels.Point3d = voxels.Point3d{16, 16, 16}

func init() {
	labels := voxels.NewDatatype()
	labels.DatatypeID = datastore.MakeDatatypeID("labels64", RepoUrl, Version)
	labels.NumChannels = 1
	labels.BytesPerVoxel = 8

	// Data types must be registered with the datastore to be used.
	datastore.RegisterDatatype(labels)
}
