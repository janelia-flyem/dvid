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
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
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
	if err := labels.LogMerge(d, v, op); err != nil {
		return err
	}

	dvid.Infof("merged label %d: supervoxels %v, %d blocks\n", op.Target, mergeIdx.GetSupervoxels(), len(mergeIdx.Blocks))

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
	if err = labels.LogCleave(d, v, op); err != nil {
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

// created while iterating over all split RLEs and computing what the
// split supervoxels should be and the # voxels split for each supervoxel per block.
type blockSplitsMap map[uint64]map[uint64]labels.SVSplitCount

// 1st pass: go through blocks intersecting split RLEs and get mappings of current supervoxels
// to new split supervoxels as well as all blocks with affected supervoxels.
func (d *Data) splitPass1(ctx *datastore.VersionedCtx, splitmap dvid.BlockRLEs, splitblks dvid.IZYXSlice) (blockSplitsMap, *labels.SVSplitMap, error) {
	svsplit := new(labels.SVSplitMap)
	newLabelFunc := func() (uint64, error) {
		return d.NewLabel(ctx.VersionID())
	}

	blockCh := make(chan *labels.PositionedBlock, len(splitblks))
	defer close(blockCh)

	wg := new(sync.WaitGroup)
	blockSplits := make(blockSplitsMap)
	go func() {
		var counts map[uint64]labels.SVSplitCount
		var err error
		for pb := range blockCh {
			blockRLEs := splitmap[pb.BCoord]
			counts, err = pb.SplitStats(blockRLEs, svsplit, newLabelFunc)
			var zyx uint64
			if err != nil {
				dvid.Errorf("issue with computing split remap in block %s: %v\n", pb.BCoord, err)
			} else {
				zyx, err = labels.IZYXStringToBlockIndex(pb.BCoord)
				if err != nil {
					dvid.Errorf("unable to convert block %s to block index: %v\n", pb.BCoord, err)
				} else {
					blockSplits[zyx] = counts
				}
			}
			wg.Done()
		}
	}()

	var scale uint8
	for _, izyx := range splitblks {
		pb, err := d.getLabelBlock(ctx, scale, izyx)
		if err != nil {
			return nil, nil, err
		}
		if pb == nil {
			return nil, nil, fmt.Errorf("split on block %s attempted but block doesn't exist", izyx)
		}
		wg.Add(1)
		blockCh <- pb
	}

	wg.Wait()
	return blockSplits, svsplit, nil
}

// if every split supervoxel in a split block is fully replaceable, we can just switch header.
func checkSplitBlockReplace(svsplits map[uint64]labels.SVSplitCount, svc *proto.SVCount) (map[uint64]uint64, bool) {
	mapping := make(map[uint64]uint64, len(svc.Counts))
	replaceable := true
	for supervoxel, count := range svc.Counts {
		svc, found := svsplits[supervoxel]
		if found {
			if svc.Voxels != count { // this is not a complete replacement.
				replaceable = false
				break
			}
			mapping[supervoxel] = svc.Split
		}
	}
	return mapping, replaceable
}

func checkNonsplitBlockAffected(svsplits map[uint64]labels.SVSplit, svc *proto.SVCount) (map[uint64]uint64, bool) {
	mapping := make(map[uint64]uint64, len(svc.Counts))
	for sv, svsplit := range svsplits {
		_, found := svc.Counts[sv]
		if found {
			mapping[sv] = svsplit.Remain // since this is a non-split block (not in split RLE)
		}
	}
	return mapping, len(mapping) > 0
}

type headerMod struct {
	bcoord  dvid.IZYXString
	mapping map[uint64]uint64
}

func (mod headerMod) BlockCoord() dvid.IZYXString {
	return mod.bcoord
}

type voxelMod struct {
	bcoord   dvid.IZYXString
	split    dvid.RLEs
	svsplits map[uint64]labels.SVSplitCount
}

func (mod voxelMod) BlockCoord() dvid.IZYXString {
	return mod.bcoord
}

type blockMod interface {
	BlockCoord() dvid.IZYXString
}

func (d *Data) modifyBlocks(ctx *datastore.VersionedCtx, downresMut *downres.Mutation, modCh chan interface{}, errCh chan error) {
	var scale uint8
	for mod := range modCh {
		bcoord := mod.(blockMod).BlockCoord()
		pb, err := d.getLabelBlock(ctx, scale, bcoord)
		if err != nil {
			errCh <- fmt.Errorf("error in getting block %s: %v", bcoord, err)
			return
		}
		if pb == nil {
			errCh <- fmt.Errorf("block %s doesn't exist for split modification", bcoord)
			return
		}
		var block *labels.Block
		switch m := mod.(type) {
		case headerMod:
			block, _, err = pb.ReplaceLabels(m.mapping)
			if err != nil {
				errCh <- fmt.Errorf("issue with header modification, block %s: %v", bcoord, err)
				return
			}
		case voxelMod:
			block, err = pb.SplitSupervoxels(m.split, m.svsplits)
			if err != nil {
				errCh <- fmt.Errorf("issue with voxel modification, block %s: %v", bcoord, err)
				return
			}
		default:
			errCh <- fmt.Errorf("received bad mod type: %v", mod)
			return
		}
		splitpb := labels.PositionedBlock{Block: *block, BCoord: bcoord}
		if err := d.putLabelBlock(ctx, scale, &splitpb); err != nil {
			errCh <- fmt.Errorf("unable to put block %s in split, data %q: %v", bcoord, d.DataName(), err)
			return
		}
		if err := downresMut.BlockMutated(bcoord, block); err != nil {
			errCh <- fmt.Errorf("data %q publishing downres: %v", d.DataName(), err)
			return
		}
		errCh <- nil
	}
}

// 2nd pass: go through all blocks with affected supervoxels, compare affected supervoxels counts
// in each block, and either modify header or rewrite the voxel labels.  Activate downres for affected
// blocks.
func (d *Data) splitPass2(ctx *datastore.VersionedCtx, downresMut *downres.Mutation, idx *labels.Index, svsplits map[uint64]labels.SVSplit, splitmap dvid.BlockRLEs, blockSplits blockSplitsMap) error {
	// note that the blockSplits give supervoxel relabelings and counts for blocks in split volume,
	// but does not include blocks that may have been affected outside of split volume.
	modCh := make(chan interface{}, len(idx.Blocks))
	errCh := make(chan error, len(idx.Blocks))

	defer close(modCh)

	go d.modifyBlocks(ctx, downresMut, modCh, errCh)

	var numHeaderMod, numVoxelMod int
	for izyx, isvc := range idx.Blocks {
		izyxStr := labels.BlockIndexToIZYXString(izyx)
		bsvc, inSplit := blockSplits[izyx]
		if inSplit {
			splitRLEs, found := splitmap[izyxStr]
			if !found {
				return fmt.Errorf("block %s supposedly in split but not found in splitmap", izyxStr)
			}
			mapping, canReplace := checkSplitBlockReplace(bsvc, isvc)
			if canReplace {
				numHeaderMod++
				modCh <- headerMod{bcoord: izyxStr, mapping: mapping}
			} else {
				numVoxelMod++
				modCh <- voxelMod{bcoord: izyxStr, split: splitRLEs, svsplits: bsvc}
			}
		} else {
			mapping, affected := checkNonsplitBlockAffected(svsplits, isvc)
			if affected {
				numHeaderMod++
				modCh <- headerMod{bcoord: izyxStr, mapping: mapping}
			}
		}

	}
	var numErr int
	var err error
	for i := 0; i < numHeaderMod+numVoxelMod; i++ {
		processErr := <-errCh
		if processErr != nil {
			err = processErr
			numErr++
		}
	}
	if err != nil {
		return fmt.Errorf("had %d errors in splitting data %q blocks, last one: %v", numErr, len(blockSplits), err)
	}
	dvid.Infof("Modifed %d blocks: %d header changes, %d voxel relabels for split\n", numHeaderMod+numVoxelMod, numHeaderMod, numVoxelMod)
	return nil
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

	// Only do voxel-based mutations one at a time.  This lets us remove handling for block-level concurrency.
	d.voxelMu.Lock()
	defer d.voxelMu.Unlock()

	// store split info into separate data.
	var splitData []byte
	if splitData, err = split.MarshalBinary(); err != nil {
		return
	}
	var splitRef string
	if splitRef, err = d.PutBlob(splitData); err != nil {
		dvid.Errorf("error storing split data: %v", err)
	}

	d.StartUpdate()
	defer d.StopUpdate()

	// Partition the split spans into blocks.
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("can't do split because block size for instance %s is not 3d: %v", d.DataName(), d.BlockSize())
		return
	}
	var splitmap dvid.BlockRLEs
	splitmap, err = split.Partition(blockSize)
	if err != nil {
		return
	}
	splitblks := splitmap.SortedKeys() // sorted list of blocks that cover split

	// 1st pass: go through blocks intersecting split RLEs and get mappings of current supervoxels
	// to new split supervoxels as well as all blocks with affected supervoxels.
	ctx := datastore.NewVersionedCtx(d, v)
	var blockSplits blockSplitsMap
	var svsplit *labels.SVSplitMap
	if blockSplits, svsplit, err = d.splitPass1(ctx, splitmap, splitblks); err != nil {
		return
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
		"SVSplits":   svsplit.Splits,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split op to kafka: %v", err)
	}

	// 2nd pass: go through all blocks with affected supervoxels, compare affected supervoxels counts
	// in each block, and either modify header or rewrite the voxel labels.  Activate downres for affected
	// blocks.
	shard := fromLabel % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	var idx *labels.Index
	idx, err = getCachedLabelIndex(d, v, fromLabel)
	if err != nil {
		err = fmt.Errorf("modify split index for data %q, label %d: %v", d.DataName(), fromLabel, err)
		return
	}
	if idx == nil {
		err = fmt.Errorf("unable to modify split index for data %q: missing label %d", d.DataName(), fromLabel)
		return
	}

	downresMut := downres.NewMutation(d, v, mutID)
	if err = d.splitPass2(ctx, downresMut, idx, svsplit.Splits, splitmap, blockSplits); err != nil {
		return
	}

	// adjust the indices and mappings
	op := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     split,
		SplitMap: svsplit.Splits,
	}
	if err = d.splitIndex(v, info, op, idx, splitmap, blockSplits); err != nil {
		return
	}
	if err = addSplitToMapping(d, v, op); err != nil {
		return
	}
	if err = labels.LogSplit(d, v, op); err != nil {
		return
	}
	if err = downresMut.Execute(); err != nil {
		return
	}

	timedLog.Debugf("completed labelmap split (%d blocks) of %d -> %d", len(splitmap), fromLabel, toLabel)

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

	// Only do voxel-based mutations one at a time.  This lets us remove handling for block-level concurrency.
	d.voxelMu.Lock()
	defer d.voxelMu.Unlock()

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
	if err = labels.LogSupervoxelSplit(d, v, op); err != nil {
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

	if err = downresMut.Execute(); err != nil {
		return
	}

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
		case splitSupervoxelOp:
			d.splitSupervoxelBlock(ctx, op)

		default:
			dvid.Criticalf("Received unknown processing msg in mutateBlock: %v\n", msg)
		}
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

	var splitBlock *labels.Block
	if op.Split != nil {
		splitBlock, _, _, err = pb.SplitSupervoxel(op.SplitSupervoxelOp)
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
