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

type sizeChange struct {
	oldSize, newSize uint64
}

// Returns the InstanceVersion for the synced labelblk if available or
// defaults to its own instance.
func (d *Data) getMergeIV(v dvid.VersionID) dvid.InstanceVersion {
	syncedLabelblk, err := d.GetSyncedLabelblk()
	if err != nil {
		return dvid.InstanceVersion{d.DataUUID(), v}
	}
	return dvid.InstanceVersion{syncedLabelblk.DataUUID(), v}
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
	dvid.Infof("Merging data %q (labels %s) into label %d ...\n", d.DataName(), m.Merged, m.Target)

	// Mark these labels as dirty until done, and make sure we can actually initiate the merge.
	if err := labels.MergeStart(d.getMergeIV(v), m); err != nil {
		return err
	}
	d.StartUpdate()

	go func() {
		if err := labels.LogMerge(d, v, m); err != nil {
			dvid.Errorf("logging merge %q: %v\n", d.DataName(), err)
		}
	}()

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{labels.MergeStartEvent, v, labels.DeltaMergeStart{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		d.StopUpdate()
		return err
	}

	// Asynchronously perform merge and handle any concurrent requests using the cache map until
	// labelvol and labelblk are updated and consistent.
	go func() {
		fmt.Printf("Starting merge %v\n", m)
		d.asyncMergeLabels(v, m)
		fmt.Printf("Finished merge %v\n", m)

		// Remove dirty labels and updating flag when done.
		labels.MergeStop(d.getMergeIV(v), m)
		d.StopUpdate()
		dvid.Infof("Finished with merge of labels %s.\n", m)
	}()
	fmt.Printf("async return from merge on %v\n", m)

	return nil
}

func (d *Data) asyncMergeLabels(v dvid.VersionID, m labels.MergeOp) {
	// Get storage objects
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// All blocks that have changed during this merge.  Key = string of block index
	blocksChanged := make(map[dvid.IZYXString]struct{})

	// Get the block-level RLEs for the toLabel
	toLabel := m.Target
	toLabelRLEs, err := d.GetLabelRLEs(v, toLabel)
	if err != nil {
		dvid.Criticalf("Can't get block-level RLEs for label %d: %v", toLabel, err)
		return
	}
	toLabelSize := toLabelRLEs.NumVoxels()

	// Iterate through all labels to be merged.
	var addedVoxels uint64
	for fromLabel := range m.Merged {
		dvid.Debugf("Merging label %d to label %d...\n", fromLabel, toLabel)

		fromLabelRLEs, err := d.GetLabelRLEs(v, fromLabel)
		if err != nil {
			dvid.Errorf("Can't get block-level RLEs for label %d: %v", fromLabel, err)
			return
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
		evt := datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
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
		fmt.Printf("Deleting all versions of label %d, key %v to %v\n", fromLabel, minTKey, maxTKey)
		if err := store.DeleteRange(ctx, minTKey, maxTKey); err != nil {
			dvid.Criticalf("Can't delete label %d RLEs: %v", fromLabel, err)
		}
	}

	if len(blocksChanged) == 0 {
		dvid.Debugf("No changes needed when merging %s into %d.  Aborting.\n", m.Merged, m.Target)
		return
	}

	// Publish block-level merge
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeBlockEvent}
	msg := datastore.SyncMessage{labels.MergeBlockEvent, v, labels.DeltaMerge{MergeOp: m, BlockMap: blocksChanged}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	// Update datastore with all toLabel RLEs that were changed
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	for blockStr := range blocksChanged {
		tk := NewTKey(toLabel, blockStr)
		serialization, err := toLabelRLEs[blockStr].MarshalBinary()
		if err != nil {
			dvid.Errorf("Error serializing RLEs for label %d: %v\n", toLabel, err)
		}
		fmt.Printf("Updating new merged key %v\n", tk)
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
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{labels.MergeEndEvent, v, labels.DeltaMergeEnd{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}
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
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

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

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	if err := labels.SplitStart(d.getMergeIV(v), splitOpStart); err != nil {
		return toLabel, err
	}
	defer labels.SplitStop(d.getMergeIV(v), splitOpEnd)

	// Signal that we are starting a split.
	msg := datastore.SyncMessage{labels.SplitStartEvent, v, splitOpStart}
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

	mutID := d.NewMutationID()
	splitOp := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     split,
	}
	go func() {
		if err := labels.LogSplit(d, v, splitOp); err != nil {
			dvid.Errorf("logging split %q: %v\n", d.DataName(), err)
		}
	}()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	splitmap, err = split.Partition(d.BlockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	// Publish split event
	deltaSplit := labels.DeltaSplit{
		MutID:        mutID,
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        splitmap,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
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
			return toLabel, fmt.Errorf("Split RLEs at block %s are not part of original label %d", splitblk, fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk)
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

	if err = batch.Commit(); err != nil {
		err = fmt.Errorf("Batch PUT during split of %q label %d: %v\n", d.DataName(), fromLabel, err)
		return
	}

	// Write the split sparse vol.
	if err = d.writeLabelVol(v, toLabel, splitmap, splitblks); err != nil {
		return
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta2}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{labels.SplitEndEvent, v, splitOpEnd}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
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
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

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

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	if err := labels.SplitStart(d.getMergeIV(v), splitOpStart); err != nil {
		return toLabel, err
	}
	defer labels.SplitStop(d.getMergeIV(v), splitOpEnd)

	// Signal that we are starting a split.
	msg := datastore.SyncMessage{labels.SplitStartEvent, v, splitOpStart}
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

	mutID := d.NewMutationID()
	splitOp := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     splits,
		Coarse:   true,
	}
	go func() {
		if err := labels.LogSplit(d, v, splitOp); err != nil {
			dvid.Errorf("logging split %q: %v\n", d.DataName(), err)
		}
	}()

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
			return toLabel, fmt.Errorf("Split block %s is not part of original label %d", splitblk, fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk)
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

	// Publish split event
	deltaSplit := labels.DeltaSplit{
		MutID:        mutID,
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta2}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{labels.SplitEndEvent, v, splitOpEnd}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}
	dvid.Infof("Split %d voxels from label %d to label %d\n", toLabelSize, fromLabel, toLabel)

	return toLabel, nil
}

// write label volume in sorted order if available.
func (d *Data) writeLabelVol(v dvid.VersionID, label uint64, brles dvid.BlockRLEs, sortblks []dvid.IZYXString) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
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
