package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var uint16EncodeFormat dvid.DataValues

func init() {
	uint16EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint16,
			Label: "uint16",
		},
	}
	interpolable := true
	imgtype := NewType(uint16EncodeFormat, interpolable)
	imgtype.Type.Name = "uint16blk"
	imgtype.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/uint16.go"
	imgtype.Type.Version = "0.1"

	datastore.Register(&imgtype)
}
