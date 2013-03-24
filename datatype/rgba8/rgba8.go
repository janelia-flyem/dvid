/*
	Package rgba8 implements DVID support for 8-bit RGBA images.  It simply
	wraps the voxels package, setting NumChannels (4) and BytesPerVoxel(1).
*/
package rgba8

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.6"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/rgba8"

type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

func init() {
	datastore.RegisterDatatype(&Datatype{
		voxels.Datatype{
			Datatype: datastore.Datatype{
				DatatypeID:  datastore.MakeDatatypeID("rgba8", RepoUrl, Version),
				BlockMax:    DefaultBlockMax,
				Indexing:    datastore.SIndexZYX,
				IsolateData: true,
			},
			NumChannels:   4,
			BytesPerVoxel: 1,
		},
	})
}
