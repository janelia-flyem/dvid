package imageblk

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var uint8EncodeFormat dvid.DataValues

func init() {
	uint8EncodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint8,
			Label: "uint8",
		},
	}
	interpolable := true
	grayscale := NewType(uint8EncodeFormat, interpolable)
	grayscale.Type.Name = "uint8blk"
	grayscale.Type.URL = "github.com/janelia-flyem/dvid/datatype/imageblk/uint8.go"
	grayscale.Type.Version = "0.2"

	datastore.Register(&grayscale)
}
