package grayscale8

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.6"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

const helpMessage = `
	grayscale8 is simply a 'voxels' data type with 8-bits per voxel and 1 channel.
	See voxels help for usage.
`

// Grayscale8 Datatype simply embeds voxels.Datatype to create a unique type
// (grayscale8.Datatype) with grayscale functions.
type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

func init() {
	datastore.RegisterDatatype(&Datatype{
		voxels.Datatype{
			Datatype: datastore.Datatype{
				DatatypeID:  datastore.MakeDatatypeID("grayscale8", RepoUrl, Version),
				BlockMax:    DefaultBlockMax,
				Indexing:    datastore.SIndexZYX,
				IsolateData: true,
			},
			NumChannels:   1,
			BytesPerVoxel: 1,
		},
	})
}
