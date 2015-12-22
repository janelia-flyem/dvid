package imageblk

import "github.com/janelia-flyem/dvid/dvid"

// Events for imageblk
const (
	IngestBlockEvent = "BLOCK_INGEST"
	MutateBlockEvent = "BLOCK_MUTATE"
	DeleteBlockEvent = "BLOCK_DELETE"
)

// Block encodes a 3d block coordinate and block data.  It is the unit of delta for
// a IngestBlockEvent.
type Block struct {
	Index *dvid.IndexZYX
	Data  []byte
}

// MutatedBlock encodes a 3d block coordinate and previous and updated block data.
// It is the unit of delta for a MutateBlockEvent.
type MutatedBlock struct {
	Index *dvid.IndexZYX
	Prev  []byte
	Data  []byte
}
