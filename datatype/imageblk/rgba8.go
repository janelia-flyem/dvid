/*
	Data type rgba8 tailors the image block data type for 8-bit RGBA images.  It simply
	wraps the voxels package, setting Channels (4) and BytesPerValue(1).
*/

package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var rgba8EncodeFormat dvid.DataValues

func init() {
	rgba8EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "red",
		},
		{
			T:     dvid.T_uint8,
			Label: "green",
		},
		{
			T:     dvid.T_uint8,
			Label: "blue",
		},
		{
			T:     dvid.T_uint8,
			Label: "alpha",
		},
	}
	interpolable := true
	rgba := NewType(rgba8EncodeFormat, interpolable)
	rgba.Type.Name = "rgba8blk"
	rgba.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/rgba8.go"
	rgba.Type.Version = "0.2"

	datastore.Register(&rgba)
}
