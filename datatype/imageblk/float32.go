/*
	Data type float32 tailors the image block data type for 32-bit float images.  It simply
	wraps the voxels package, setting Channels (1) and BytesPerValue(4).
*/

package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var float32EncodeFormat dvid.DataValues

func init() {
	float32EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_float32,
			Label: "float32",
		},
	}
	interpolable := true
	floatimg := NewType(float32EncodeFormat, interpolable)
	floatimg.Type.Name = "float32blk"
	floatimg.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/float32.go"
	floatimg.Type.Version = "0.1"

	datastore.Register(&floatimg)
}
