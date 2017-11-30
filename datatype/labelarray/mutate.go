/*
	This file contains code that manages labelblk mutations at a low-level, using sharding
	to specific goroutines depending on the block coordinate being mutated.
	TODO: Move ingest/mutate/delete block ops in write.go into the same system.  Currently,
	we assume that merge/split ops in a version do not overlap the raw block label mutations.
*/

package labelarray

import (
        "encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
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
func (d *Data) MergeLabels(v dvid.VersionID, op labels.MergeOp) error {
	dvid.Debugf("Merging %s into label %d ...\n", op.Merged, op.Target)

	// Only do one large mutation at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	// Asynchronously perform merge and handle any concurrent requests using the cache map until
	// labelarray is updated and consistent.  Mark these labels as dirty until done.
	d.StartUpdate()
	iv := dvid.InstanceVersion{Data: d.DataUUID(), Version: v}
	if err := labels.MergeStart(iv, op); err != nil {
		d.StopUpdate()
		return err
	}

	mutID := d.NewMutationID()
	go func() {
		if err := labels.LogMerge(d, v, mutID, op); err != nil {
			dvid.Errorf("logging of merge: %v\n", err)
		}
	}()

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{labels.MergeStartEvent, v, labels.DeltaMergeStart{op}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		d.StopUpdate()
		return err
	}

	ctx := datastore.NewVersionedCtx(d, v)
	go func() {
		defer func() {
			d.StopUpdate()
			labels.MergeStop(iv, op)
		}()

		// Get all the affected blocks in the merge.
		targetMeta, err := d.getLabelMeta(ctx, labels.NewSet(op.Target), 0, dvid.Bounds{})
		if err != nil {
			dvid.Errorf("can't get block indices of to merge target label %d\n", op.Target)
			return
		}
		mergedMeta, err := d.getLabelMeta(ctx, op.Merged, 0, dvid.Bounds{})
		if err != nil {
			dvid.Errorf("can't get block indices of to merge labels %s\n", op.Merged)
			return
		}

		delta := labels.DeltaMerge{
			MergeOp:      op,
			Blocks:       targetMeta.Blocks.MergeCopy(mergedMeta.Blocks),
			TargetVoxels: targetMeta.Voxels,
			MergedVoxels: mergedMeta.Voxels,
		}
		if err := d.processMerge(v, delta); err != nil {
			dvid.Criticalf("unable to process merge: %v\n", err)
		}
		dvid.Infof("processed merge for %q in gofunc\n", d.DataName())
	}()

        // send kafka merge event to instance-uuid topic 
        // msg: {"action": "merge", "target": targetlabel, "labels": [merge labels]}

        // create topic (repo ID + data instance uuid)
        // NOTE: Kafka server must be configured to allow topic creation from
        // messages sent to a non-existent topic
        rootuuid, _ := d.DAGRootUUID()
        datauuid := d.DataUUID()
        topic := "dvidrepo-" + string(rootuuid) + "-inst-" + string(datauuid)

        // create msg
        labels := make([]uint64,0,len(op.Merged))
	for label := range op.Merged {
                labels = append(labels, label)
        }

        versionuuid, _ :=  datastore.UUIDFromVersion(v)

        msginfo := map[string]interface{}{
            "Action": "merge",
            "Target":  op.Target,
            "Labels": labels,
            "UUID": string(versionuuid),
        }
        jsonmsg, _ := json.Marshal(msginfo)

        // send message if kafka initialized
        dvid.KafkaProduceMsg(jsonmsg, topic)

	return nil
}

// handle block and label index mods for a merge.
func (d *Data) processMerge(v dvid.VersionID, delta labels.DeltaMerge) error {
	timedLog := dvid.NewTimeLog()

	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeBlockEvent}
	msg := datastore.SyncMessage{labels.MergeBlockEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return fmt.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	mutID := d.NewMutationID()
	downresMut := downres.NewMutation(d, v, mutID)
	for _, izyx := range delta.Blocks {
		n := izyx.Hash(numMutateHandlers)
		d.MutAdd(mutID)
		op := mergeOp{mutID: mutID, MergeOp: delta.MergeOp, bcoord: izyx, downresMut: downresMut}
		d.mutateCh[n] <- procMsg{op: op, v: v}
	}

	// When we've processed all the delta blocks, we can remove this merge op
	// from the merge cache since all labels will have completed.
	d.MutWait(mutID)
	d.MutDelete(mutID)
	timedLog.Debugf("labelarray block-level merge (%d blocks) of %s -> %d", len(delta.Blocks), delta.MergeOp.Merged, delta.MergeOp.Target)

	// Merge the new blocks into the target label block index.
	mergebdm := make(blockDiffMap, len(delta.Blocks))
	for _, izyx := range delta.Blocks {
		mergebdm[izyx] = labelDiff{delta: int32(delta.MergedVoxels), present: true}
	}
	shard := delta.Target % numLabelHandlers
	d.indexCh[shard] <- labelChange{v: v, label: delta.Target, bdm: mergebdm}

	// Delete all the merged label block index kv pairs.
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data %q merge had error initializing store: %v\n", d.DataName(), err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data %q merge requires batch-enabled store, which %q is not\n", d.DataName(), store)
	}

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	for merged := range delta.Merged {
		tk := NewLabelIndexTKey(merged)
		batch.Delete(tk)
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Error on commiting block indices for label %d, data %q: %v\n", delta.Target, d.DataName(), err)
	}

	deltaRep := labels.DeltaReplaceSize{
		Label:   delta.Target,
		OldSize: delta.TargetVoxels,
		NewSize: delta.TargetVoxels + delta.MergedVoxels,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, deltaRep}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{labels.MergeEndEvent, v, labels.DeltaMergeEnd{delta.MergeOp}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	downresMut.Done()

	dvid.Infof("Merged %s -> %d, data %q, resulting in %d blocks\n", delta.Merged, delta.Target, d.DataName(), len(delta.Blocks))

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
	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting subset of label %d into given label %d ...\n", fromLabel, splitLabel)
		if err = d.updateMaxLabel(v, splitLabel); err != nil {
			return
		}
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	// Only do one large mutation at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	d.StartUpdate()
	iv := dvid.InstanceVersion{Data: d.DataUUID(), Version: v}
	if err = labels.SplitStart(iv, splitOpStart); err != nil {
		d.StopUpdate()
		return
	}
	defer func() {
		labels.SplitStop(iv, splitOpEnd)
		d.StopUpdate()
	}()

	// Signal that we are starting a split.
	msg := datastore.SyncMessage{labels.SplitStartEvent, v, splitOpStart}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
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
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     split,
	}
	go func() {
		if err = labels.LogSplit(d, v, mutID, splitOp); err != nil {
			dvid.Errorf("logging split: %v\n", err)
		}
	}()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("Can't do split because block size for instance %s is not 3d: %v", d.DataName(), d.BlockSize())
		return
	}
	splitmap, err = split.Partition(blockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	// Do the split
	deltaSplit := labels.DeltaSplit{
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        splitmap,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}
	if err = d.processSplit(v, mutID, deltaSplit); err != nil {
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
	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting coarse subset of label %d into given label %d ...\n", fromLabel, splitLabel)
		if err = d.updateMaxLabel(v, splitLabel); err != nil {
			return
		}
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting coarse subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	// Only do one request at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	iv := dvid.InstanceVersion{Data: d.DataUUID(), Version: v}
	if err := labels.SplitStart(iv, splitOpStart); err != nil {
		return toLabel, err
	}
	defer labels.SplitStop(iv, splitOpEnd)

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
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     splits,
		Coarse:   true,
	}
	go func() {
		if err = labels.LogSplit(d, v, mutID, splitOp); err != nil {
			dvid.Errorf("logging split: %v\n", err)
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

	// Publish split event
	deltaSplit := labels.DeltaSplit{
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        nil,
		SortedBlocks: splitblks,
	}
	if err = d.processSplit(v, mutID, deltaSplit); err != nil {
		return
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	dvid.Infof("Coarsely split %d blocks from label %d to label %d\n", numBlocks, fromLabel, toLabel)
	return toLabel, nil
}

func (d *Data) processSplit(v dvid.VersionID, mutID uint64, delta labels.DeltaSplit) error {
	timedLog := dvid.NewTimeLog()

	downresMut := downres.NewMutation(d, v, mutID)

	var doneCh chan struct{}
	var deleteBlks dvid.IZYXSlice
	if delta.Split == nil {
		// Coarse Split so block indexing simple because all split blocks are removed from old label.
		deleteBlks = delta.SortedBlocks
		for _, izyx := range delta.SortedBlocks {
			n := izyx.Hash(numMutateHandlers)
			d.MutAdd(mutID)
			op := splitOp{
				mutID: mutID,
				SplitOp: labels.SplitOp{
					Target:   delta.OldLabel,
					NewLabel: delta.NewLabel,
				},
				bcoord:     izyx,
				downresMut: downresMut,
			}
			d.mutateCh[n] <- procMsg{op: op, v: v}
		}
	} else {
		// Fine Split could partially split within a block so both old and new labels have same valid block.
		doneCh = make(chan struct{})
		deleteBlkCh := make(chan dvid.IZYXString) // blocks that should be fully deleted from old label.
		go func() {
			for {
				select {
				case blk := <-deleteBlkCh:
					deleteBlks = append(deleteBlks, blk)
				case <-doneCh:
					return
				}
			}
		}()

		for izyx, blockRLEs := range delta.Split {
			n := izyx.Hash(numMutateHandlers)
			d.MutAdd(mutID)
			op := splitOp{
				mutID: mutID,
				SplitOp: labels.SplitOp{
					Target:   delta.OldLabel,
					NewLabel: delta.NewLabel,
					RLEs:     blockRLEs,
				},
				bcoord:      izyx,
				deleteBlkCh: deleteBlkCh,
				downresMut:  downresMut,
			}
			d.mutateCh[n] <- procMsg{op: op, v: v}
		}

		// Publish change in label sizes.
		deltaNewSize := labels.DeltaNewSize{
			Label: delta.NewLabel,
			Size:  delta.SplitVoxels,
		}
		evt := datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{labels.ChangeSizeEvent, v, deltaNewSize}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
		}

		deltaModSize := labels.DeltaModSize{
			Label:      delta.OldLabel,
			SizeChange: int64(-delta.SplitVoxels),
		}
		evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, deltaModSize}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
		}
	}

	// Wait for all blocks to be split then modify label indices and mark end of split op.
	d.MutWait(mutID)
	d.MutDelete(mutID)
	if doneCh != nil {
		close(doneCh)
	}
	if err := d.splitIndices(v, delta, deleteBlks); err != nil {
		return err
	}
	if delta.Split == nil {
		timedLog.Debugf("labelarray sync complete for coarse split (%d blocks) of %d -> %d", len(delta.SortedBlocks), delta.OldLabel, delta.NewLabel)
	} else {
		timedLog.Debugf("labelarray sync complete for split (%d blocks) of %d -> %d", len(delta.Split), delta.OldLabel, delta.NewLabel)
	}

	// Publish split event
	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg := datastore.SyncMessage{labels.SplitLabelEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{labels.SplitEndEvent, v, labels.DeltaSplitEnd{delta.OldLabel, delta.NewLabel}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return fmt.Errorf("Unable to notify subscribers to data %q for evt %v\n", d.DataName(), evt)
	}

	downresMut.Done()
	return nil
}

// handles modification of the old and new label's block indices on split.
func (d *Data) splitIndices(v dvid.VersionID, delta labels.DeltaSplit, deleteBlks dvid.IZYXSlice) error {
	// note that the blocks to be deleted from old label != split blocks since there
	// may be partial block split.
	deletebdm := make(blockDiffMap, len(deleteBlks))
	for _, izyx := range deleteBlks {
		deletebdm[izyx] = labelDiff{present: false}
	}
	shard := delta.OldLabel % numLabelHandlers
	d.indexCh[shard] <- labelChange{v: v, label: delta.OldLabel, bdm: deletebdm}

	var splitbdm blockDiffMap
	if delta.Split == nil {
		splitbdm = make(blockDiffMap, len(delta.SortedBlocks))
		for _, izyx := range delta.SortedBlocks {
			splitbdm[izyx] = labelDiff{present: true}
		}
	} else {
		splitbdm = make(blockDiffMap, len(delta.Split))
		for izyx := range delta.Split {
			splitbdm[izyx] = labelDiff{present: true}
		}
	}
	shard = delta.NewLabel % numLabelHandlers
	d.indexCh[shard] <- labelChange{v: v, label: delta.NewLabel, bdm: splitbdm}
	return nil
}

// Serializes block operations so despite having concurrent merge/split label requests,
// we make sure any particular block isn't concurrently GET/POSTED.  If more throughput is required
// and the backend is distributed, we can spawn many mutateBlock() goroutines as long as we uniquely
// shard blocks across them, so the same block will always be directed to the same goroutine.
func (d *Data) mutateBlock(ch <-chan procMsg) {
	for {
		msg, more := <-ch
		if !more {
			return
		}

		ctx := datastore.NewVersionedCtx(d, msg.v)
		switch op := msg.op.(type) {
		case mergeOp:
			d.mergeBlock(ctx, op)

		case splitOp:
			d.splitBlock(ctx, op)

		// TODO
		// case ingestOp:
		// 	d.ingestBlock(ctx, op)

		// case mutateOp:
		// 	d.mutateBlock(ctx, op)

		// case deleteOp:
		// 	d.deleteBlock(ctx, op)

		default:
			dvid.Criticalf("Received unknown processing msg in mutateBlock: %v\n", msg)
		}
	}
}

// relabels a single block during a merge operation.
func (d *Data) mergeBlock(ctx *datastore.VersionedCtx, op mergeOp) {
	defer d.MutDone(op.mutID)

	var scale uint8
	pb, err := d.getLabelBlock(ctx, scale, op.bcoord)
	if err != nil {
		dvid.Errorf("error in merge block %s: %v\n", op.bcoord, err)
		return
	}

	block, err := pb.MergeLabels(op.MergeOp)
	if err != nil {
		dvid.Errorf("error merging labels %s for data %q: %v\n", op.Merged, d.DataName(), err)
		return
	}
	pb.Block = *block

	if err := d.putLabelBlock(ctx, scale, pb); err != nil {
		dvid.Errorf("error putting label block %s for data %q: %v\n", op.bcoord, d.DataName(), err)
		return
	}

	if err := op.downresMut.BlockMutated(op.bcoord, &(pb.Block)); err != nil {
		dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
	}
}

// splits a set of voxels to a specified label within a block
func (d *Data) splitBlock(ctx *datastore.VersionedCtx, op splitOp) {
	defer d.MutDone(op.mutID)

	var scale uint8
	pb, err := d.getLabelBlock(ctx, scale, op.bcoord)
	if err != nil {
		dvid.Errorf("error in merge block %s: %v\n", op.bcoord, err)
		return
	}
	if pb == nil {
		dvid.Infof("split on block %s attempted but block doesn't exist\n", op.bcoord)
		return
	}

	// Modify the block using either voxel-level changes or coarser block-level mods.
	// If we are doing coarse block split, we can only get change in # voxels after going through
	// block-level splits, unlike when provided the RLEs for split itself.  Also, we don't know
	// whether block indices can be maintained for fine split until we do split and see if any
	// old label remains.
	var toLabelSize uint64
	var splitBlock *labels.Block
	if op.RLEs != nil {
		var keptSize uint64
		splitBlock, keptSize, toLabelSize, err = pb.Split(op.SplitOp)
		if err != nil {
			dvid.Errorf("can't store label %d RLEs into block %s: %v\n", op.NewLabel, op.bcoord, err)
			return
		}
		dvid.Infof("Split block %s, label %d -> %d: kept %d voxels, split %d voxels\n", pb.BCoord, op.Target, op.NewLabel, keptSize, toLabelSize)
		if splitBlock == nil {
			dvid.Infof("Attempt to split missing label %d in block %s!\n", op.SplitOp.Target, pb)
			return
		}
		if keptSize == 0 {
			op.deleteBlkCh <- op.bcoord
		}
	} else {
		// We are doing coarse split and will replace all
		splitBlock, toLabelSize, err = pb.ReplaceLabel(op.Target, op.NewLabel)
		if err != nil {
			dvid.Errorf("can't replace label %d with %d in block %s: %v\n", op.Target, op.NewLabel, op.bcoord, err)
			return
		}
		delta := labels.DeltaNewSize{
			Label: op.Target,
			Size:  toLabelSize,
		}
		evt := datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{labels.ChangeSizeEvent, ctx.VersionID(), delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Criticalf("Unable to notify subscribers to data %q for evt %v\n", d.DataName(), evt)
		}

		delta2 := labels.DeltaModSize{
			Label:      op.Target,
			SizeChange: int64(-toLabelSize),
		}
		evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg = datastore.SyncMessage{labels.ChangeSizeEvent, ctx.VersionID(), delta2}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Criticalf("Unable to notify subscribers to data %q for evt %v\n", d.DataName(), evt)
		}

	}

	splitpb := labels.PositionedBlock{*splitBlock, op.bcoord}
	if err := d.putLabelBlock(ctx, scale, &splitpb); err != nil {
		dvid.Errorf("unable to put block %s in split of label %d, data %q: %v\n", op.bcoord, op.Target, d.DataName(), err)
		return
	}

	if err := op.downresMut.BlockMutated(op.bcoord, splitBlock); err != nil {
		dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
	}
}
