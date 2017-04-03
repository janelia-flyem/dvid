package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var uint32EncodeFormat dvid.DataValues

func init() {
	uint32EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint32,
			Label: "uint32",
		},
	}
	interpolable := true
	imgtype := NewType(uint32EncodeFormat, interpolable)
	imgtype.Type.Name = "uint32blk"
	imgtype.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/uint32.go"
	imgtype.Type.Version = "0.1"

	datastore.Register(&imgtype)
}
