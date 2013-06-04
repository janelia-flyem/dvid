/*
	Package grayscale8 implements DVID support for 8-bit grayscale images.  It simply
	wraps the voxels package, setting NumChannels (1) and BytesPerVoxel(1).
*/
package grayscale8

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.6"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

// Grayscale8 Datatype simply embeds voxels.Datatype to create a unique type
// (grayscale8.Datatype) with grayscale functions.
type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

func init() {
	grayscale := voxels.NewDatatype()
	grayscale.DatatypeID = datastore.MakeDatatypeID("grayscale8", RepoUrl, Version)
	grayscale.NumChannels = 1
	grayscale.BytesPerVoxel = 1

	// Data types must be registered with the datastore to be used.
	datastore.RegisterDatatype(grayscale)
}
