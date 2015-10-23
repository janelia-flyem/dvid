/*
	This file contains code that manages long-lived merge/split operations using small
	amount of globally-coordinated metadata.
*/

package labelvol

import (
	"fmt"
	"io"
	"sort"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	// These are the labels that are in the process of modification from merge, split, or other sync events.
	dirtyLabels labels.DirtyCache
)

type sizeChange struct {
	oldSize, newSize uint64
}

// MergeLabels handles merging of any number of labels throughout the various label data
// structures.  It assumes that the merges aren't cascading, e.g., there is no attempt
// to merge label 3 into 4 and also 4 into 5.  The caller should have flattened the merges.
// TODO: Provide some indication that subset of labels are under evolution, returning
//   an "unavailable" status or 203 for non-authoritative response.  This might not be
//   feasible for clustered DVID front-ends due to coordination issues.
//
// EVENTS
//
// labels.MergeStartEvent occurs at very start of merge and transmits labels.DeltaMergeStart struct.
//
// labels.MergeBlockEvent occurs for every block of a merged label and transmits labels.DeltaMerge struct.
//
// labels.MergeEndEvent occurs at end of merge and transmits labels.DeltaMergeEnd struct.
//
func (d *Data) MergeLabels(v dvid.VersionID, m labels.MergeOp) error {
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	dvid.Debugf("Merging %s into label %d ...\n", m.Merged, m.Target)

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataName(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{v, labels.DeltaMergeStart{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	// Mark these labels as dirty until done.
	iv := dvid.InstanceVersion{d.DataName(), v}
	dirtyLabels.AddMerge(iv, m)
	defer dirtyLabels.RemoveMerge(iv, m)

	// All blocks that have changed during this merge.  Key = string of block index
	blocksChanged := make(map[dvid.IZYXString]struct{})

	// Get the block-level RLEs for the toLabel
	toLabel := m.Target
	toLabelRLEs, err := d.GetLabelRLEs(v, toLabel)
	if err != nil {
		return fmt.Errorf("Can't get block-level RLEs for label %d: %v", toLabel, err)
	}
	toLabelSize := toLabelRLEs.NumVoxels()

	// Iterate through all labels to be merged.
	var addedVoxels uint64
	for fromLabel := range m.Merged {
		dvid.Debugf("Merging label %d to label %d...\n", fromLabel, toLabel)

		fromLabelRLEs, err := d.GetLabelRLEs(v, fromLabel)
		if err != nil {
			return fmt.Errorf("Can't get block-level RLEs for label %d: %v", fromLabel, err)
		}
		fromLabelSize := fromLabelRLEs.NumVoxels()
		if fromLabelSize == 0 || len(fromLabelRLEs) == 0 {
			dvid.Debugf("Label %d is empty.  Skipping.\n", fromLabel)
			continue
		}
		addedVoxels += fromLabelSize

		// Notify linked labelsz instances
		delta := labels.DeltaDeleteSize{
			Label:    fromLabel,
			OldSize:  fromLabelSize,
			OldKnown: true,
		}
		evt := datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{v, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			return err
		}

		// Append or insert RLE runs from fromLabel blocks into toLabel blocks.
		for blockStr, fromRLEs := range fromLabelRLEs {
			// Mark the fromLabel blocks as modified
			blocksChanged[blockStr] = struct{}{}

			// Get the toLabel RLEs for this block and add the fromLabel RLEs
			toRLEs, found := toLabelRLEs[blockStr]
			if found {
				toRLEs.Add(fromRLEs)
			} else {
				toRLEs = fromRLEs
			}
			toLabelRLEs[blockStr] = toRLEs
		}

		// Delete all fromLabel RLEs since they are all integrated into toLabel RLEs
		minTKey := NewTKey(fromLabel, dvid.MinIndexZYX.ToIZYXString())
		maxTKey := NewTKey(fromLabel, dvid.MaxIndexZYX.ToIZYXString())
		ctx := datastore.NewVersionedCtx(d, v)
		if err := store.DeleteRange(ctx, minTKey, maxTKey); err != nil {
			return fmt.Errorf("Can't delete label %d RLEs: %v", fromLabel, err)
		}
	}

	if len(blocksChanged) == 0 {
		dvid.Debugf("No changes needed when merging %s into %d.  Aborting.\n", m.Merged, m.Target)
		return nil
	}

	// Publish block-level merge
	evt = datastore.SyncEvent{d.DataName(), labels.MergeBlockEvent}
	msg = datastore.SyncMessage{v, labels.DeltaMerge{m, blocksChanged}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	// Update datastore with all toLabel RLEs that were changed
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	for blockStr := range blocksChanged {
		tk := NewTKey(toLabel, blockStr)
		serialization, err := toLabelRLEs[blockStr].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Error serializing RLEs for label %d: %v\n", toLabel, err)
		}
		batch.Put(tk, serialization)
	}
	if err := batch.Commit(); err != nil {
		dvid.Errorf("Error on updating RLEs for label %d: %v\n", toLabel, err)
	}
	delta := labels.DeltaReplaceSize{
		Label:   toLabel,
		OldSize: toLabelSize,
		NewSize: toLabelSize + addedVoxels,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	evt = datastore.SyncEvent{d.DataName(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{v, labels.DeltaMergeEnd{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	return nil
}

// SplitLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume and should
// preferably be the smaller portion of a labeled region.  In other words, the caller should chose
// to submit for relabeling the smaller portion of any split.  It is assumed that the given split
// voxels are within the fromLabel set of voxels and will generate unspecified behavior if this is
// not the case.
//
// EVENTS
//
// labels.SplitStartEvent occurs at very start of split and transmits labels.DeltaSplitStart struct.
//
// labels.SplitBlockEvent occurs for every block of a split label and transmits labels.DeltaSplit struct.
//
// labels.SplitEndEvent occurs at end of split and transmits labels.DeltaSplitEnd struct.
//
func (d *Data) SplitLabels(v dvid.VersionID, fromLabel, splitLabel uint64, r io.ReadCloser) (toLabel uint64, err error) {
	store, err := storage.MutableStore()
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// Mark these labels as dirty until done.
	iv := dvid.InstanceVersion{d.DataName(), v}
	dirtyLabels.Incr(iv, fromLabel)
	defer dirtyLabels.Decr(iv, fromLabel)

	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting subset of label %d into given label %d ...\n", fromLabel, splitLabel)
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	// Signal that we are starting a split.
	evt := datastore.SyncEvent{d.DataName(), labels.SplitStartEvent}
	msg := datastore.SyncMessage{v, labels.DeltaSplitStart{fromLabel, toLabel}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Read the sparse volume from reader.
	var split dvid.RLEs
	split, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	toLabelSize, _ := split.Stats()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	splitmap, err = split.Partition(d.BlockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	// Publish split event
	evt = datastore.SyncEvent{d.DataName(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{v, labels.DeltaSplit{fromLabel, toLabel, splitmap, splitblks}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Write the split sparse vol.
	if err = d.writeLabelVol(v, toLabel, splitmap, splitblks); err != nil {
		return
	}

	// Iterate through the split blocks, read the original block.  If the RLEs
	// are identical, just delete the original.  If not, modify the original.
	// TODO: Modifications should be transactional since it's GET-PUT, therefore use
	// hash on block coord to direct it to blockLabel, splitLabel-specific goroutine; we serialize
	// requests to handle concurrency.
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	for _, splitblk := range splitblks {

		// Get original block
		tk := NewTKey(fromLabel, splitblk)
		val, err := store.Get(ctx, tk)
		if err != nil {
			return toLabel, err
		}

		if val == nil {
			return toLabel, fmt.Errorf("Split RLEs at block %s are not part of original label %d", splitblk.Print(), fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk.Print())
		}

		// Compare and process based on modifications required.
		remain, err := rles.Split(splitmap[splitblk])
		if err != nil {
			return toLabel, err
		}
		if len(remain) == 0 {
			batch.Delete(tk)
		} else {
			rleBytes, err := remain.MarshalBinary()
			if err != nil {
				return toLabel, fmt.Errorf("can't serialize remain RLEs for split of %d: %v\n", fromLabel, err)
			}
			batch.Put(tk, rleBytes)
		}
	}

	if err := batch.Commit(); err != nil {
		dvid.Errorf("Batch PUT during split of %q label %d: %v\n", d.DataName(), fromLabel, err)
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta2}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataName(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{v, labels.DeltaSplitEnd{fromLabel, toLabel}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	return toLabel, nil
}

// SplitCoarseLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume defined by block
// coordinates and should be the smaller portion of a labeled region-to-be-split.
//
// EVENTS
//
// labels.SplitStartEvent occurs at very start of split and transmits labels.DeltaSplitStart struct.
//
// labels.SplitBlockEvent occurs for every block of a split label and transmits labels.DeltaSplit struct.
//
// labels.SplitEndEvent occurs at end of split and transmits labels.DeltaSplitEnd struct.
//
func (d *Data) SplitCoarseLabels(v dvid.VersionID, fromLabel, splitLabel uint64, r io.ReadCloser) (toLabel uint64, err error) {
	store, err := storage.MutableStore()
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// Mark these labels as dirty until done.
	iv := dvid.InstanceVersion{d.DataName(), v}
	dirtyLabels.Incr(iv, fromLabel)
	defer dirtyLabels.Decr(iv, fromLabel)

	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting coarse subset of label %d into given label %d ...\n", fromLabel, splitLabel)
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting coarse subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	// Signal that we are starting a split.
	evt := datastore.SyncEvent{d.DataName(), labels.SplitStartEvent}
	msg := datastore.SyncMessage{v, labels.DeltaSplitStart{fromLabel, toLabel}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Read the sparse volume from reader.
	var splits dvid.RLEs
	splits, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	numBlocks, _ := splits.Stats()

	// Order the split blocks
	splitblks := make(dvid.IZYXSlice, numBlocks)
	n := 0
	for _, rle := range splits {
		p := rle.StartPt()
		run := rle.Length()
		for i := int32(0); i < run; i++ {
			izyx := dvid.IndexZYX{p[0] + i, p[1], p[2]}
			splitblks[n] = izyx.ToIZYXString()
			n++
		}
	}
	sort.Sort(splitblks)

	// Publish split event
	evt = datastore.SyncEvent{d.DataName(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{v, labels.DeltaSplit{fromLabel, toLabel, nil, splitblks}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Iterate through the split blocks, read the original block and change labels.
	// TODO: Modifications should be transactional since it's GET-PUT, therefore use
	// hash on block coord to direct it to block-specific goroutine; we serialize
	// requests to handle concurrency.
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	var toLabelSize uint64
	for _, splitblk := range splitblks {
		// Get original block
		tk := NewTKey(fromLabel, splitblk)
		val, err := store.Get(ctx, tk)
		if err != nil {
			return toLabel, err
		}
		if val == nil {
			return toLabel, fmt.Errorf("Split block %s is not part of original label %d", splitblk.Print(), fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk.Print())
		}
		numVoxels, _ := rles.Stats()
		toLabelSize += numVoxels

		// Delete the old block and save the sparse volume but under a new label.
		batch.Delete(tk)
		tk2 := NewTKey(toLabel, splitblk)
		batch.Put(tk2, val)
	}

	if err := batch.Commit(); err != nil {
		dvid.Errorf("Batch PUT during split of %q label %d: %v\n", d.DataName(), fromLabel, err)
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta2}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataName(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{v, labels.DeltaSplitEnd{fromLabel, toLabel}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}
	dvid.Infof("Split %d voxels from label %d to label %d\n", toLabelSize, fromLabel, toLabel)

	return toLabel, nil
}

// write label volume in sorted order if available.
func (d *Data) writeLabelVol(v dvid.VersionID, label uint64, brles dvid.BlockRLEs, sortblks []dvid.IZYXString) error {
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	if sortblks != nil {
		for _, izyxStr := range sortblks {
			serialization, err := brles[izyxStr].MarshalBinary()
			if err != nil {
				return fmt.Errorf("Error serializing RLEs for label %d: %v\n", label, err)
			}
			batch.Put(NewTKey(label, izyxStr), serialization)
		}
	} else {
		for izyxStr, rles := range brles {
			serialization, err := rles.MarshalBinary()
			if err != nil {
				return fmt.Errorf("Error serializing RLEs for label %d: %v\n", label, err)
			}
			batch.Put(NewTKey(label, izyxStr), serialization)
		}
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Error on updating RLEs for label %d: %v\n", label, err)
	}
	return nil
}
