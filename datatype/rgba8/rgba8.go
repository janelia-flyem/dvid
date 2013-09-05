/*
	Package rgba8 implements DVID support for 8-bit RGBA images.  It simply
	wraps the voxels package, setting NumChannels (4) and BytesPerVoxel(1).
*/
package rgba8

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
)

const Version = "0.6"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/rgba8"

type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax voxels.Point3d = voxels.Point3d{16, 16, 16}

func init() {
	rgba := voxels.NewDatatype()
	rgba.DatatypeID = datastore.MakeDatatypeID("rgba8", RepoUrl, Version)
	rgba.NumChannels = 4
	rgba.BytesPerVoxel = 1

	// Data types must be registered with the datastore to be used.
	datastore.RegisterDatatype(rgba)
}
