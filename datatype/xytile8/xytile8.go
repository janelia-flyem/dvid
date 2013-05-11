/*
	Package xytile8 implements DVID support for 8-bit grayscale images laid out
	in an XY-oriented tile.  It simply wraps the voxels package, setting 
	NumChannels (1) and BytesPerVoxel(1), and using a tile-shaped block size.
*/
package xytile8

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.5"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/xytile8"

// xytile8 Datatype simply embeds voxels.Datatype to create a unique type
// (xytile8.Datatype) with grayscale functions.
type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{100, 100, 1}

func init() {
	datastore.RegisterDatatype(&Datatype{
		voxels.Datatype{
			Datatype: datastore.Datatype{
				DatatypeID:  datastore.MakeDatatypeID("xytile8", RepoUrl, Version),
				BlockMax:    DefaultBlockMax,
				Indexing:    datastore.SchemeIndexZYX,
				IsolateData: true,
			},
			NumChannels:   1,
			BytesPerVoxel: 1,
		},
	})
}
