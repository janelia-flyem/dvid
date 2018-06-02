/*
	This file contains code that manages labelblk mutations at a low-level, using sharding
	to specific goroutines depending on the block coordinate being mutated.
	TODO: Move ingest/mutate/delete block ops in write.go into the same system.  Currently,
	we assume that merge/split ops in a version do not overlap the raw block label mutations.
*/

package labelmap

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
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
func (d *Data) MergeLabels(v dvid.VersionID, op labels.MergeOp, info dvid.ModInfo) error {
	dvid.Debugf("Merging %s into label %d ...\n", op.Merged, op.Target)

	d.StartUpdate()
	defer d.StopUpdate()

	timedLog := dvid.NewTimeLog()
	mutID := d.NewMutationID()

	// send kafka merge event to instance-uuid topic
	// msg: {"action": "merge", "target": targetlabel, "labels": [merge labels]}
	lbls := make([]uint64, 0, len(op.Merged))
	for label := range op.Merged {
		lbls = append(lbls, label)
	}

	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":     "merge",
		"Target":     op.Target,
		"Labels":     lbls,
		"UUID":       string(versionuuid),
		"MutationID": mutID,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("can't send merge op for %q to kafka: %v\n", d.DataName(), err)
	}

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{labels.MergeStartEvent, v, labels.DeltaMergeStart{op}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	// Get all the affected blocks in the merge.
	targetIdx, err := GetLabelIndex(d, v, op.Target, false)
	if err != nil {
		return fmt.Errorf("can't get block indices of to merge target label %d: %v", op.Target, err)
	}
	if targetIdx == nil {
		return fmt.Errorf("can't merge into a non-existant label %d", op.Target)
	}
	mergeIdx, err := GetMultiLabelIndex(d, v, op.Merged, dvid.Bounds{})
	if err != nil {
		return fmt.Errorf("can't get block indices of merge labels %s: %v", op.Merged, err)
	}

	if err := addMergeToMapping(d, v, mutID, op.Target, mergeIdx); err != nil {
		return err
	}

	delta := labels.DeltaMerge{
		MergeOp:      op,
		TargetVoxels: targetIdx.NumVoxels(),
		MergedVoxels: mergeIdx.NumVoxels(),
	}
	if mergeIdx != nil && len(mergeIdx.Blocks) != 0 {
		if err := targetIdx.Add(mergeIdx); err != nil {
			return err
		}
		targetIdx.LastMutId = mutID
		targetIdx.LastModUser = info.User
		targetIdx.LastModTime = info.Time
		targetIdx.LastModApp = info.App
		if err := PutLabelIndex(d, v, op.Target, targetIdx); err != nil {
			return err
		}
	}
	for merged := range delta.Merged {
		DeleteLabelIndex(d, v, merged)
	}

	dvid.Infof("merged label %d: supervoxels %v\n", op.Target, mergeIdx.GetSupervoxels())
	dvid.Infof("merged label %d: blocks %v\n", op.Target, mergeIdx.Blocks)

	delta.Blocks = targetIdx.GetBlockIndices()
	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeBlockEvent}
	msg = datastore.SyncMessage{labels.MergeBlockEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return fmt.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{labels.MergeEndEvent, v, labels.DeltaMergeEnd{delta.MergeOp}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	timedLog.Infof("Merged %s -> %d, data %q, resulting in %d blocks", delta.Merged, delta.Target, d.DataName(), len(delta.Blocks))

	// send kafka merge complete event to instance-uuid topic
	msginfo = map[string]interface{}{
		"Action":     "merge-complete",
		"MutationID": mutID,
		"UUID":       string(versionuuid),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	return d.ProduceKafkaMsg(jsonmsg)
}

// CleaveLabel cleaves a label given supervoxels to be cleaved.  Requires JSON in request body
// using the following format:
//	[supervoxel1, supervoxel2, ...]
// Each element of the JSON array is a supervoxel to be cleaved from the label and either
// given a new label or the one optionally supplied via the "cleavelabel" query string.
// A cleave label can be specified via the "toLabel" parameter, which if 0 will have an
// automatic label ID selected for the cleaved body.
func (d *Data) CleaveLabel(v dvid.VersionID, label uint64, info dvid.ModInfo, r io.ReadCloser) (cleaveLabel uint64, err error) {
	if r == nil {
		return 0, fmt.Errorf("no cleave supervoxels JSON was POSTed")
	}

	cleaveLabel, err = d.NewLabel(v)
	if err != nil {
		return
	}
	dvid.Debugf("Cleaving subset of label %d into new label %d.\n", label, cleaveLabel)

	var data []byte
	data, err = ioutil.ReadAll(r)
	if err != nil {
		return cleaveLabel, fmt.Errorf("bad POSTed data for merge; should be JSON parsable: %v", err)
	}
	if len(data) == 0 {
		return cleaveLabel, fmt.Errorf("no cleave supervoxels JSON was POSTed")
	}
	var cleaveSupervoxels []uint64
	if err = json.Unmarshal(data, &cleaveSupervoxels); err != nil {
		return cleaveLabel, fmt.Errorf("bad cleave supervoxels JSON: %v", err)
	}

	// send kafka cleave event to instance-uuid topic
	mutID := d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":             "cleave",
		"OrigLabel":          label,
		"CleavedLabel":       cleaveLabel,
		"CleavedSupervoxels": cleaveSupervoxels,
		"MutationID":         mutID,
		"UUID":               string(versionuuid),
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split op to kafka: %v", err)
	}

	d.StartUpdate()
	defer d.StopUpdate()

	op := labels.CleaveOp{
		MutID:              mutID,
		Target:             label,
		CleavedLabel:       cleaveLabel,
		CleavedSupervoxels: cleaveSupervoxels,
	}
	evt := datastore.SyncEvent{d.DataUUID(), labels.CleaveLabelEvent}
	msg := datastore.SyncMessage{labels.CleaveLabelEvent, v, op}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		err = fmt.Errorf("can't notify subscribers for event %v: %v", evt, err)
		return
	}

	if err = CleaveIndex(d, v, op, info); err != nil {
		return
	}
	if err = addCleaveToMapping(d, v, op); err != nil {
		return
	}

	msginfo = map[string]interface{}{
		"Action":     "cleave-complete",
		"MutationID": mutID,
		"UUID":       string(versionuuid),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending cleave complete op to kafka: %v", err)
	}
	return
}

// SplitLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume and should
// preferably be the smaller portion of a labeled region.  In other words, the caller should chose
// to submit for relabeling the smaller portion of any split.  It is assumed that the given split
// voxels are within the fromLabel set of voxels and will generate unspecified behavior if this is
// not the case.
func (d *Data) SplitLabels(v dvid.VersionID, fromLabel uint64, r io.ReadCloser, info dvid.ModInfo) (toLabel uint64, err error) {

	timedLog := dvid.NewTimeLog()

	// Create a new label id for this version that will persist to store
	toLabel, err = d.NewLabel(v)
	if err != nil {
		return
	}
	dvid.Debugf("Splitting subset of label %d into new label %d ...\n", fromLabel, toLabel)

	// Read the sparse volume from reader.
	var split dvid.RLEs
	split, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	toLabelSize, _ := split.Stats()

	// Only do one large mutation at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	// store split info into separate data.
	var splitData []byte
	if splitData, err = split.MarshalBinary(); err != nil {
		return
	}
	var splitRef string
	if splitRef, err = d.PutBlob(splitData); err != nil {
		dvid.Errorf("error storing split data: %v", err)
	}

	// send kafka split event to instance-uuid topic
	mutID := d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":     "split",
		"Target":     fromLabel,
		"NewLabel":   toLabel,
		"Split":      splitRef,
		"MutationID": mutID,
		"UUID":       string(versionuuid),
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split op to kafka: %v", err)
	}
	op := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     split,
	}
	go func() {
		if err := labels.LogSplit(d, v, mutID, op); err != nil {
			dvid.Errorf("logging split %q: %v\n", d.DataName(), err)
		}
	}()

	d.StartUpdate()
	defer d.StopUpdate()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("can't do split because block size for instance %s is not 3d: %v", d.DataName(), d.BlockSize())
		return
	}
	splitmap, err = split.Partition(blockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	downresMut := downres.NewMutation(d, v, mutID)
	doneCh := make(chan struct{})
	blockCh := make(chan blockSplitCounts)
	svsplit := new(labels.SVSplitMap)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go d.splitIndexAndBlocks(v, op, info, downresMut, blockCh, doneCh, wg)

	for izyx, blockRLEs := range splitmap {
		n := izyx.Hash(numMutateHandlers)
		d.MutAdd(mutID)
		op := splitOp{
			mutID: mutID,
			SplitOp: labels.SplitOp{
				Target:   fromLabel,
				NewLabel: toLabel,
				RLEs:     blockRLEs,
			},
			bcoord:     izyx,
			downresMut: downresMut,
			deltaCh:    blockCh,
			mapping:    svsplit,
		}
		d.mutateCh[n] <- procMsg{op: op, v: v}
	}

	// Wait for all blocks to be split then modify mapping and label indices.
	d.MutWait(mutID)
	d.MutDelete(mutID)
	if doneCh != nil {
		close(doneCh)
	}
	wg.Wait()
	if err = addSplitToMapping(d, v, mutID, fromLabel, toLabel, svsplit); err != nil {
		return
	}

	timedLog.Debugf("completed labelmap split (%d blocks) of %d -> %d", len(splitmap), fromLabel, toLabel)
	downresMut.Done()

	deltaSplit := labels.DeltaSplit{
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        splitmap,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}
	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg := datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	msginfo = map[string]interface{}{
		"Action":     "split-complete",
		"MutationID": mutID,
		"UUID":       string(versionuuid),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split complete op to kafka: %v", err)
	}

	return toLabel, nil
}

// SplitSupervoxel splits a portion of a supervoxel's voxels into two new labels.  The input is a
// binary sparse volume and should be totally contained by the given supervoxel.  The first
// returned label is assigned to the split voxels while the second returned label is assigned
// to the remainder voxels.
func (d *Data) SplitSupervoxel(v dvid.VersionID, svlabel uint64, r io.ReadCloser, info dvid.ModInfo) (splitSupervoxel, remainSupervoxel uint64, err error) {

	// Create new labels for this split that will persist to store
	splitSupervoxel, err = d.NewLabel(v)
	if err != nil {
		return
	}
	remainSupervoxel, err = d.NewLabel(v)
	if err != nil {
		return
	}
	dvid.Debugf("Splitting subset of label %d into new label %d and renaming remainder to label %d...\n", svlabel, splitSupervoxel, remainSupervoxel)

	// Read the sparse volume from reader.
	var split dvid.RLEs
	split, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}

	// Only do one large mutation at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	// store split info into separate data.
	var splitData []byte
	if splitData, err = split.MarshalBinary(); err != nil {
		return
	}
	var splitRef string
	if splitRef, err = d.PutBlob(splitData); err != nil {
		dvid.Errorf("error storing split data: %v", err)
	}

	// send kafka split event to instance-uuid topic
	mutID := d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":           "split-supervoxel",
		"Supervoxel":       svlabel,
		"SplitSupervoxel":  splitSupervoxel,
		"RemainSupervoxel": remainSupervoxel,
		"Split":            splitRef,
		"MutationID":       mutID,
		"UUID":             string(versionuuid),
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split op to kafka: %v", err)
	}

	d.StartUpdate()
	defer func() {
		d.StopUpdate()
	}()

	var splitmap dvid.BlockRLEs
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("can't do split because block size for instance %s is not 3d: %v", d.DataName(), d.BlockSize())
		return
	}
	splitmap, err = split.Partition(blockSize)
	if err != nil {
		return
	}
	op := labels.SplitSupervoxelOp{
		MutID:            mutID,
		Supervoxel:       svlabel,
		SplitSupervoxel:  splitSupervoxel,
		RemainSupervoxel: remainSupervoxel,
		Split:            splitmap,
	}
	if err = addSupervoxelSplitToMapping(d, v, op); err != nil {
		return
	}
	timedLog := dvid.NewTimeLog()
	downresMut := downres.NewMutation(d, v, mutID)

	svblocks, err := SplitSupervoxelIndex(d, v, op, info)
	if err != nil {
		return splitSupervoxel, remainSupervoxel, err
	}

	for _, izyx := range svblocks {
		n := izyx.Hash(numMutateHandlers)
		d.MutAdd(op.MutID)
		sop := splitSupervoxelOp{
			SplitSupervoxelOp: op,
			mutID:             op.MutID,
			bcoord:            izyx,
			downresMut:        downresMut,
		}
		d.mutateCh[n] <- procMsg{op: sop, v: v}
	}

	// Wait for all blocks in supervoxel to be relabeled before returning.
	d.MutWait(op.MutID)
	d.MutDelete(op.MutID)

	downresMut.Done()

	timedLog.Debugf("labelmap supervoxel %d split complete (%d blocks split)", op.Supervoxel, len(op.Split))

	evt := datastore.SyncEvent{d.DataUUID(), labels.SupervoxelSplitEvent}
	msg := datastore.SyncMessage{labels.SupervoxelSplitEvent, v, op}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	msginfo = map[string]interface{}{
		"Action":     "split-supervoxel-complete",
		"MutationID": mutID,
		"UUID":       string(versionuuid),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split complete op to kafka: %v", err)
	}

	return splitSupervoxel, remainSupervoxel, nil
}

// Serializes block operations so despite having concurrent merge/split label requests,
// we make sure any particular block isn't concurrently GET/POSTED.
// TODO: consolidate this block processing stream with d.putChunk() used for POST /raw
// and also POST /blocks.  For now, we assume there is never SPLIT ops at same time as
// POST /raw or /blocks --> ingest phase is distinct from merge/split phase.
func (d *Data) mutateBlock(ch <-chan procMsg) {
	for {
		msg, more := <-ch
		if !more {
			return
		}

		ctx := datastore.NewVersionedCtx(d, msg.v)
		switch op := msg.op.(type) {
		case splitOp:
			d.splitBlock(ctx, op)

		case splitSupervoxelOp:
			d.splitSupervoxelBlock(ctx, op)

		default:
			dvid.Criticalf("Received unknown processing msg in mutateBlock: %v\n", msg)
		}
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
	f := func() (uint64, error) {
		return d.NewLabel(ctx.VersionID())
	}
	splitBlock, counts, err := pb.SplitWithStats(op.SplitOp, op.mapping, f)
	if err != nil {
		dvid.Errorf("issue with creating split in block %s: %v\n", op.Target, op.bcoord, err)
		return
	}
	op.deltaCh <- blockSplitCounts{
		bcoord: op.bcoord,
		counts: counts,
	}

	splitpb := labels.PositionedBlock{Block: *splitBlock, BCoord: op.bcoord}
	if err := d.putLabelBlock(ctx, scale, &splitpb); err != nil {
		dvid.Errorf("unable to put block %s in split of label %d, data %q: %v\n", op.bcoord, op.Target, d.DataName(), err)
		return
	}

	if err := op.downresMut.BlockMutated(op.bcoord, splitBlock); err != nil {
		dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
	}
}

// splits a set of voxels to a specified label within a block
func (d *Data) splitSupervoxelBlock(ctx *datastore.VersionedCtx, op splitSupervoxelOp) {
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

	var toLabelSize uint64
	var splitBlock *labels.Block
	if op.Split != nil {
		var keptSize uint64
		splitBlock, keptSize, toLabelSize, err = pb.SplitSupervoxel(op.SplitSupervoxelOp)
		if err != nil {
			dvid.Errorf("can't store split supervoxel %d RLEs into block %s: %v\n", op.Supervoxel, op.bcoord, err)
			return
		}
	}

	splitpb := labels.PositionedBlock{*splitBlock, op.bcoord}
	if err := d.putLabelBlock(ctx, scale, &splitpb); err != nil {
		dvid.Errorf("unable to put block %s in split of label %d, data %q: %v\n", op.bcoord, op.Supervoxel, d.DataName(), err)
		return
	}

	if err := op.downresMut.BlockMutated(op.bcoord, splitBlock); err != nil {
		dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
	}
}
