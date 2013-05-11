/*
	Package xytile32 implements DVID support for 32-bit images laid out
	in an XY-oriented tile.  It simply wraps the voxels package, setting 
	NumChannels (1) and BytesPerVoxel(4), and using a tile-shaped block size.
*/
package xytile32

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.5"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/xytile32"

// xytile32 Datatype simply embeds voxels.Datatype to create a unique type
// (xytile32.Datatype) with grayscale functions.
type Datatype struct {
	voxels.Datatype
}

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{100, 100, 1}

func init() {
	datastore.RegisterDatatype(&Datatype{
		voxels.Datatype{
			Datatype: datastore.Datatype{
				DatatypeID:  datastore.MakeDatatypeID("xytile32", RepoUrl, Version),
				BlockMax:    DefaultBlockMax,
				Indexing:    datastore.SchemeIndexZYX,
				IsolateData: true,
			},
			NumChannels:   1,
			BytesPerVoxel: 4,
		},
	})
}
