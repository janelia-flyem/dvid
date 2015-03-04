package imageblk

import "github.com/janelia-flyem/dvid/dvid"

// Events for imageblk
const (
	ChangeBlockEvent = "BLOCK_CHANGE"
)

// Block encodes a 3d block coordinate and block data.  It is the unit of delta for
// a ChangeBlockEvent.
type Block struct {
	Index *dvid.IndexZYX
	Data  []byte
}
