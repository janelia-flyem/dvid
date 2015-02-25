/*
	Package labels supports label-based data types like labelblk, labelvol, labelsurf, labelsz, etc.
	Basic 64-bit label data and deltas are kept here so all label-based data types can use them without
	cyclic package dependencies, especially when writing code to synchronize across data instances.
*/
package labels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

// Block encodes a 3d block coordinate and uint64 label data.  In order to interpret the label data,
// the block size needs to be used for the particular data instance.
type Block struct {
	Index *dvid.IndexZYX
	Data  []byte
}

// MergeTuple represents a merge of labels.  Its first element is the destination label
// and all later elements in the slice are labels to be merged.
type MergeTuple []uint64

type MergeTuples []MergeTuple

func (mt *MergeTuples) AddMerge(fromLabel, toLabel uint64) {
	for i, merges := range *mt {
		if (*mt)[i][0] != toLabel {
			continue
		}
		merges = append(merges, fromLabel)
		(*mt)[i] = merges
	}
}

// BlockRLEs is a single label's map of block coordinates to RLEs for that label.
// The key is a string of the serialized block coordinate.
type BlockRLEs map[string]dvid.RLEs

// NumVoxels is the number of voxels contained within a label's block RLEs.
func (brles BlockRLEs) NumVoxels() uint64 {
	var size uint64
	for _, rles := range brles {
		numVoxels, _ := rles.Stats()
		size += uint64(numVoxels)
	}
	return size
}

// DeltaSize is a unit of change suitable for describing label volume size changes.
type DeltaSize struct {
	Label   uint64
	OldSize uint64
	NewSize uint64
}

// DeltaSizes is a slice of DeltaSize, sent during a ChangeLabelSize event.
type DeltaSizes []DeltaSize

// DeltaMerge describes the labels affected by a merge operation
type DeltaMerge struct {
	MergeTuple
}

// DeltaSplit describes the voxels modified during a split operation
type DeltaSplit struct {
	NewLabel uint64
	OldLabel uint64
	Split    BlockRLEs
}

// Label change events, each of which have a different representation of the change
const (
	UnknownEvent datastore.SyncEvent = iota
	ChangeBlockEvent
	ChangeSparsevolEvent
	ChangeSizeEvent
	MergeEvent
	SplitEvent
)
