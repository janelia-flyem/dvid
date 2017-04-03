package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var uint64EncodeFormat dvid.DataValues

func init() {
	uint64EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint64,
			Label: "uint64",
		},
	}
	interpolable := true
	imgtype := NewType(uint64EncodeFormat, interpolable)
	imgtype.Type.Name = "uint64blk"
	imgtype.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/uint64.go"
	imgtype.Type.Version = "0.1"

	datastore.Register(&imgtype)
}
