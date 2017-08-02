package labels

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
)

// MergeOp represents the merging of a set of labels into a target label.
type MergeOp struct {
	Target uint64
	Merged Set
}

// SplitOp represents a split with the sparse volume of the new label.
type SplitOp struct {
	Target   uint64
	NewLabel uint64
	RLEs     dvid.RLEs
	Coarse   bool // true if the RLEs are block coords (coarse split), not voxels.
}

// SplitFineOp is a split using RLEs for a block.

func (op MergeOp) String() string {
	return fmt.Sprintf("merge %s -> label %d", op.Merged, op.Target)
}

// MergeTuple represents a merge of labels.  Its first element is the destination label
// and all later elements in the slice are labels to be merged.  It's an easy JSON
// representation as a list of labels.
type MergeTuple []uint64

// Op converts a MergeTuple into a MergeOp.
func (t MergeTuple) Op() (MergeOp, error) {
	var op MergeOp
	if t == nil || len(t) == 1 {
		return op, fmt.Errorf("invalid merge tuple %v, need at least target and to-merge labels", t)
	}
	op.Target = t[0]
	op.Merged = make(Set, len(t)-1)
	for _, label := range t[1:] {
		if label == 0 {
			return op, fmt.Errorf("invalid merge tuple %v -- cannot contain background label 0", t)
		}
		op.Merged[label] = struct{}{}
	}
	return op, nil
}

// DeltaNewSize is a new label being introduced.
type DeltaNewSize struct {
	Label uint64
	Size  uint64
}

// DeltaDeleteSize gives info to delete a label's size.
type DeltaDeleteSize struct {
	Label    uint64
	OldSize  uint64
	OldKnown bool // true if OldSize is valid, otherwise delete all size k/v for this label.
}

// DeltaModSize gives info to modify an existing label size without knowing the old size.
type DeltaModSize struct {
	Label      uint64
	SizeChange int64 // Adds to old label size
}

// DeltaReplaceSize gives info to precisely remove an old label size and add the updated size.
type DeltaReplaceSize struct {
	Label   uint64
	OldSize uint64
	NewSize uint64
}

// DeltaMerge describes the labels and blocks affected by a merge operation.  It is sent
// during a MergeBlockEvent.
type DeltaMerge struct {
	MergeOp
	Blocks       dvid.IZYXSlice               // not nil if labelarray used.
	BlockMap     map[dvid.IZYXString]struct{} // not nil if labelblk used, to be deprecated.
	TargetVoxels uint64
	MergedVoxels uint64
}

// DeltaMergeStart is the data sent during a MergeStartEvent.
type DeltaMergeStart struct {
	MergeOp
}

// DeltaMergeEnd is the data sent during a MergeEndEvent.
type DeltaMergeEnd struct {
	MergeOp
}

// DeltaSplit describes the voxels modified during a split operation.
// The Split field may be null if this is a coarse split only defined by block indices.
type DeltaSplit struct {
	OldLabel     uint64
	NewLabel     uint64
	Split        dvid.BlockRLEs
	SortedBlocks dvid.IZYXSlice
	SplitVoxels  uint64
}

// DeltaSplitStart is the data sent during a SplitStartEvent.
type DeltaSplitStart struct {
	OldLabel uint64
	NewLabel uint64
}

// DeltaSplitEnd is the data sent during a SplitEndEvent.
type DeltaSplitEnd struct {
	OldLabel uint64
	NewLabel uint64
}

// DeltaSparsevol describes a change to an existing label.
type DeltaSparsevol struct {
	Label uint64
	Mods  dvid.BlockRLEs
}

// Label change event identifiers
const (
	IngestBlockEvent    = imageblk.IngestBlockEvent
	MutateBlockEvent    = imageblk.MutateBlockEvent
	DeleteBlockEvent    = imageblk.DeleteBlockEvent
	SparsevolStartEvent = "SPARSEVOL_START"
	SparsevolModEvent   = "SPARSEVOL_MOD"
	SparsevolEndEvent   = "SPARSEVOL_END"
	ChangeSizeEvent     = "LABEL_SIZE_CHANGE"
	MergeStartEvent     = "MERGE_START"
	MergeBlockEvent     = "MERGE_BLOCK"
	MergeEndEvent       = "MERGE_END"
	SplitStartEvent     = "SPLIT_START"
	SplitLabelEvent     = "SPLIT_LABEL"
	SplitEndEvent       = "SPLIT_END"
)
