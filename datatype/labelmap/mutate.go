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
	"sort"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
)

type sizeChange struct {
	oldSize, newSize uint64
}

// MergeLabels synchronously merges any number of labels throughout the various label
// data structures.  It assumes that the merges aren't cascading, e.g., there is no
// attempt to merge label 3 into 4 and also 4 into 5.  The caller should have flattened
// the merges.
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
func (d *Data) MergeLabels(v dvid.VersionID, op labels.MergeOp, info dvid.ModInfo) (mutID uint64, err error) {
	dvid.Debugf("Merging %s into label %d ...\n", op.Merged, op.Target)

	d.StartUpdate()
	defer d.StopUpdate()

	timedLog := dvid.NewTimeLog()
	mutID = d.NewMutationID()

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
		"Timestamp":  time.Now().String(),
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("can't send merge op for %q to kafka: %v\n", d.DataName(), err)
	}

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{labels.MergeStartEvent, v, labels.DeltaMergeStart{op}}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	// Get all the affected blocks in the merge.
	var targetIdx, mergeIdx *labels.Index
	if targetIdx, err = GetLabelIndex(d, v, op.Target, false); err != nil {
		err = fmt.Errorf("can't get block indices of to merge target label %d: %v", op.Target, err)
		return
	}
	if targetIdx == nil {
		err = fmt.Errorf("can't merge into a non-existent label %d", op.Target)
		return
	}
	if mergeIdx, err = GetMultiLabelIndex(d, v, op.Merged, dvid.Bounds{}); err != nil {
		err = fmt.Errorf("can't get block indices of merge labels %s: %v", op.Merged, err)
		return
	}

	if err = addMergeToMapping(d, v, mutID, op.Target, mergeIdx); err != nil {
		return
	}

	delta := labels.DeltaMerge{
		MergeOp:      op,
		TargetVoxels: targetIdx.NumVoxels(),
		MergedVoxels: mergeIdx.NumVoxels(),
	}
	if mergeIdx != nil && len(mergeIdx.Blocks) != 0 {
		if err = targetIdx.Add(mergeIdx); err != nil {
			return
		}
		targetIdx.LastMutId = mutID
		targetIdx.LastModUser = info.User
		targetIdx.LastModTime = info.Time
		targetIdx.LastModApp = info.App
		if err = PutLabelIndex(d, v, op.Target, targetIdx); err != nil {
			return
		}
	}
	for merged := range delta.Merged {
		DeleteLabelIndex(d, v, merged)
	}
	if err = labels.LogMerge(d, v, op); err != nil {
		return
	}

	dvid.Infof("merged label %d: supervoxels %v, %d blocks\n", op.Target, mergeIdx.GetSupervoxels(), len(mergeIdx.Blocks))

	delta.Blocks = targetIdx.GetBlockIndices()
	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeBlockEvent}
	msg = datastore.SyncMessage{labels.MergeBlockEvent, v, delta}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		err = fmt.Errorf("can't notify subscribers for event %v: %v\n", evt, err)
		return
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
		"Timestamp":  time.Now().String(),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	err = d.ProduceKafkaMsg(jsonmsg)
	return
}

// CleaveLabel synchornously cleaves a label given supervoxels to be cleaved.
// Requires JSON in request body using the following format:
//	[supervoxel1, supervoxel2, ...]
// Each element of the JSON array is a supervoxel to be cleaved from the label and either
// given a new label or the one optionally supplied via the "cleavelabel" query string.
// A cleave label can be specified via the "toLabel" parameter, which if 0 will have an
// automatic label ID selected for the cleaved body.
func (d *Data) CleaveLabel(v dvid.VersionID, label uint64, info dvid.ModInfo, r io.ReadCloser) (cleaveLabel, mutID uint64, err error) {
	if r == nil {
		err = fmt.Errorf("no cleave supervoxels JSON was POSTed")
		return
	}

	cleaveLabel, err = d.newLabel(v)
	if err != nil {
		return
	}
	dvid.Debugf("Cleaving subset of label %d into new label %d.\n", label, cleaveLabel)

	var data []byte
	data, err = ioutil.ReadAll(r)
	if err != nil {
		err = fmt.Errorf("bad POSTed data for merge; should be JSON parsable: %v", err)
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("no cleave supervoxels JSON was POSTed")
		return
	}
	var cleaveSupervoxels []uint64
	if err = json.Unmarshal(data, &cleaveSupervoxels); err != nil {
		err = fmt.Errorf("bad cleave supervoxels JSON: %v", err)
		return
	}

	// send kafka cleave event to instance-uuid topic
	mutID = d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":             "cleave",
		"OrigLabel":          label,
		"CleavedLabel":       cleaveLabel,
		"CleavedSupervoxels": cleaveSupervoxels,
		"MutationID":         mutID,
		"UUID":               string(versionuuid),
		"Timestamp":          time.Now().String(),
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
	if err = CleaveIndex(d, v, op, info); err != nil {
		return
	}
	if err = addCleaveToMapping(d, v, op); err != nil {
		return
	}
	if err = labels.LogCleave(d, v, op); err != nil {
		return
	}

	// notify syncs after processing because downstream sync might rely on changes
	evt := datastore.SyncEvent{d.DataUUID(), labels.CleaveLabelEvent}
	msg := datastore.SyncMessage{labels.CleaveLabelEvent, v, op}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		err = fmt.Errorf("can't notify subscribers for event %v: %v", evt, err)
		return
	}

	msginfo = map[string]interface{}{
		"Action":     "cleave-complete",
		"MutationID": mutID,
		"UUID":       string(versionuuid),
		"Timestamp":  time.Now().String(),
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

// 1st pass: retrieve and check blocks intersecting split RLEs and get mappings of current supervoxels
// to new split supervoxels.
func (d *Data) splitPass1(ctx *datastore.VersionedCtx, splitmap dvid.BlockRLEs, splitblks dvid.IZYXSlice) (blockSplitsMap, *labels.SVSplitMap, error) {
	timedLog := dvid.NewTimeLog()
	svsplit := new(labels.SVSplitMap)
	newLabelFunc := func() (uint64, error) {
		return d.newLabel(ctx.VersionID())
	}

	errCh := make(chan error, len(splitblks))
	blockCh := make(chan *labels.PositionedBlock, len(splitblks))
	defer close(blockCh)

	var maxQueue int
	blockSplits := make(blockSplitsMap)
	go func() {
		for pb := range blockCh {
			blockRLEs := splitmap[pb.BCoord]
			counts, err := pb.SplitStats(blockRLEs, svsplit, newLabelFunc)
			if err != nil {
				errCh <- fmt.Errorf("issue with computing split remap in block %s: %v", pb.BCoord, err)
			} else {
				zyx, err := labels.IZYXStringToBlockIndex(pb.BCoord)
				if err != nil {
					errCh <- fmt.Errorf("unable to convert block %s to block index: %v", pb.BCoord, err)
				} else {
					blockSplits[zyx] = counts
					errCh <- nil
				}
			}
			curQueue := len(blockCh)
			if curQueue > maxQueue {
				maxQueue = curQueue
			}
		}
	}()

	var numBlocks int
	var scale uint8
	getLog := dvid.NewTimeLog()
	for _, izyx := range splitblks {
		pb, err := d.getLabelBlock(ctx, scale, izyx)
		if err != nil {
			return nil, nil, err
		}
		if pb == nil {
			return nil, nil, fmt.Errorf("split on block %s attempted but block doesn't exist", izyx)
		}
		numBlocks++
		blockCh <- pb
	}
	getLog.Debugf("split pass 1: got %d blocks under split volume", numBlocks)

	var numErr int
	var lastErr error
	for i := 0; i < numBlocks; i++ {
		processErr := <-errCh
		if processErr != nil {
			lastErr = processErr
			numErr++
		}
	}

	timedLog.Debugf("split pass 1 completed: %d blocks with %d errors (max queue %d/%d)\n", numBlocks, numErr, maxQueue, len(splitblks))
	return blockSplits, svsplit, lastErr
}

// if every split supervoxel in a split block is fully replaceable, we can just switch header.
func checkSplitBlockReplace(blockSplits map[uint64]labels.SVSplitCount, allSplits map[uint64]labels.SVSplit, idxsvc *proto.SVCount) (map[uint64]uint64, bool) {
	mapping := make(map[uint64]uint64, len(idxsvc.Counts))
	replaceable := true
	for supervoxel, count := range idxsvc.Counts {
		split, found := blockSplits[supervoxel] // the supervoxels split in this particular block
		if found {
			if split.Voxels != count { // this is not a complete replacement.
				replaceable = false
				break
			}
			mapping[supervoxel] = split.Split
		} else {
			split, found := allSplits[supervoxel]
			if found {
				// this is supervoxel split elsewhere but not in this block, so it
				// must be part of remainder.
				mapping[supervoxel] = split.Remain
			}
		}
	}
	return mapping, replaceable
}

func checkNonsplitBlockAffected(allSplits map[uint64]labels.SVSplit, idxsvc *proto.SVCount) (map[uint64]uint64, bool) {
	mapping := make(map[uint64]uint64, len(idxsvc.Counts))
	for sv, svsplit := range allSplits {
		_, found := idxsvc.Counts[sv]
		if found {
			mapping[sv] = svsplit.Remain // since this is a non-split block (not in split RLE)
		}
	}
	return mapping, len(mapping) > 0
}

type headerMod struct {
	bcoord  dvid.IZYXString
	mapping map[uint64]uint64
	pb      *labels.PositionedBlock
}

type voxelMod struct {
	bcoord   dvid.IZYXString
	split    dvid.RLEs
	svsplits map[uint64]labels.SVSplit
	pb       *labels.PositionedBlock
}

func (d *Data) modifyBlocks(ctx *datastore.VersionedCtx, downresMut *downres.Mutation, modCh chan interface{}, errCh chan error) {
	var err error
	var scale uint8
	for mod := range modCh {
		var bcoord dvid.IZYXString
		var block *labels.Block
		switch m := mod.(type) {
		case headerMod:
			bcoord = m.bcoord
			block, _, err = m.pb.ReplaceLabels(m.mapping)
			if err != nil {
				errCh <- fmt.Errorf("issue with header modification, block %s: %v", bcoord, err)
				return
			}
		case voxelMod:
			bcoord = m.bcoord
			block, err = m.pb.SplitSupervoxels(m.split, m.svsplits)
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
func (d *Data) splitPass2(ctx *datastore.VersionedCtx, downresMut *downres.Mutation, idx *labels.Index, affectedBlocks dvid.IZYXSlice, svsplits map[uint64]labels.SVSplit, splitmap dvid.BlockRLEs, blockSplits blockSplitsMap) error {
	// note that the blockSplits give supervoxel relabelings and counts for blocks in split volume,
	// but does not include blocks that may have been affected outside of split volume.
	timedLog := dvid.NewTimeLog()
	modCh := make(chan interface{}, len(affectedBlocks))
	errCh := make(chan error, len(affectedBlocks))

	defer close(modCh)

	numHandlers := 16
	for i := 0; i < numHandlers; i++ {
		go d.modifyBlocks(ctx, downresMut, modCh, errCh)
	}

	var scale uint8
	var maxQueue int
	var numHeaderMod, numVoxelMod int
	getLog := dvid.NewTimeLog()
	for _, izyxStr := range affectedBlocks {
		zyx, err := labels.IZYXStringToBlockIndex(izyxStr)
		isvc, found := idx.Blocks[zyx]
		if !found {
			return fmt.Errorf("split affected block %s was not found in index", izyxStr)
		}
		pb, err := d.getLabelBlock(ctx, scale, izyxStr)
		if err != nil {
			return fmt.Errorf("error in getting block %s: %v", izyxStr, err)
		}
		if pb == nil {
			return fmt.Errorf("block %s doesn't exist for split modification", izyxStr)
		}
		bsvc, inSplit := blockSplits[zyx]
		if inSplit {
			splitRLEs, found := splitmap[izyxStr]
			if !found {
				return fmt.Errorf("block %s supposedly in split but not found in splitmap", izyxStr)
			}
			mapping, canReplace := checkSplitBlockReplace(bsvc, svsplits, isvc)
			if canReplace {
				numHeaderMod++
				modCh <- headerMod{bcoord: izyxStr, mapping: mapping, pb: pb}
			} else {
				numVoxelMod++
				modCh <- voxelMod{bcoord: izyxStr, split: splitRLEs, svsplits: svsplits, pb: pb}
			}
		} else {
			mapping, affected := checkNonsplitBlockAffected(svsplits, isvc)
			if affected {
				numHeaderMod++
				modCh <- headerMod{bcoord: izyxStr, mapping: mapping, pb: pb}
			}
		}
		curQueue := len(modCh)
		if curQueue > maxQueue {
			maxQueue = curQueue
		}
	}
	getLog.Infof("split pass 2: got %d out of %d affected blocks\n", numHeaderMod+numVoxelMod, len(affectedBlocks))
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
	timedLog.Infof("split pass 2 completed: %d blocks (max queue %d/%d): %d header changes, %d voxel relabels\n", numHeaderMod+numVoxelMod, maxQueue, len(idx.Blocks), numHeaderMod, numVoxelMod)
	return nil
}

func getAffectedBlocks(idx *labels.Index, svsplit *labels.SVSplitMap) (affectedBlocks dvid.IZYXSlice) {
	for zyx, svc := range idx.Blocks {
		for supervoxel := range svsplit.Splits {
			_, found := svc.Counts[supervoxel]
			if found {
				izyxStr := labels.BlockIndexToIZYXString(zyx)
				affectedBlocks = append(affectedBlocks, izyxStr)
				break
			}
		}
	}
	sort.Sort(affectedBlocks)
	return
}

// SplitLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume and should
// preferably be the smaller portion of a labeled region.  In other words, the caller should chose
// to submit for relabeling the smaller portion of any split.  It is assumed that the given split
// voxels are within the fromLabel set of voxels and will generate unspecified behavior if this is
// not the case.
func (d *Data) SplitLabels(v dvid.VersionID, fromLabel uint64, r io.ReadCloser, info dvid.ModInfo) (toLabel, mutID uint64, err error) {
	timedLog := dvid.NewTimeLog()

	// Create a new label id for this version that will persist to store
	toLabel, err = d.newLabel(v)
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
	splitSize, _ := split.Stats()
	if splitSize == 0 {
		err = fmt.Errorf("bad split since split volume was zero voxels")
		return
	}

	// read label index and do simple check on split size
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
	fromLabelSize := idx.NumVoxels()
	if splitSize >= fromLabelSize {
		err = fmt.Errorf("split volume of %d voxels >= %d of label %d", splitSize, fromLabelSize, fromLabel)
		return
	}

	// Only do voxel-based mutations one at a time.  This lets us remove handling for block-level concurrency.
	d.voxelMu.Lock()
	defer d.voxelMu.Unlock()

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
	// to new split supervoxels
	ctx := datastore.NewVersionedCtx(d, v)
	var blockSplits blockSplitsMap
	var svsplit *labels.SVSplitMap
	if blockSplits, svsplit, err = d.splitPass1(ctx, splitmap, splitblks); err != nil {
		return
	}
	labelSupervoxels := idx.GetSupervoxels()
	for supervoxel := range svsplit.Splits {
		if _, found := labelSupervoxels[supervoxel]; !found {
			err = fmt.Errorf("supervoxel %d was part of split volume yet was not part of body label %d", supervoxel, fromLabel)
			return
		}
	}
	affectedBlocks := getAffectedBlocks(idx, svsplit)

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
	mutID = d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":     "split",
		"Target":     fromLabel,
		"NewLabel":   toLabel,
		"Split":      splitRef,
		"MutationID": mutID,
		"UUID":       string(versionuuid),
		"SVSplits":   svsplit.Splits,
		"Timestamp":  time.Now().String(),
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split op to kafka: %v", err)
	}

	// 2nd pass: go through all blocks with affected supervoxels, compare affected supervoxels counts
	// in each block, and either modify header or rewrite the voxel labels.  Activate downres for affected
	// blocks.
	downresMut := downres.NewMutation(d, v, mutID)
	if err = d.splitPass2(ctx, downresMut, idx, affectedBlocks, svsplit.Splits, splitmap, blockSplits); err != nil {
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

	timedLog.Debugf("completed labelmap split (%d affected, %d split blocks) of %d -> %d", len(affectedBlocks), len(splitmap), fromLabel, toLabel)

	deltaSplit := labels.DeltaSplit{
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        splitmap,
		SortedBlocks: splitblks,
		SplitVoxels:  splitSize,
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
		"Timestamp":  time.Now().String(),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split complete op to kafka: %v", err)
	}
	return
}

// SplitSupervoxel splits a portion of a supervoxel's voxels into two new labels, which can be
// optionally set to desired labels if passed in (see splitlabel and remainlabel in parameters).
// The input is a binary sparse volume and should be totally contained by the given supervoxel.
// The first returned label is assigned to the split voxels while the second returned label is
// assigned to the remainder voxels.
func (d *Data) SplitSupervoxel(v dvid.VersionID, svlabel, splitlabel, remainlabel uint64, r io.ReadCloser, info dvid.ModInfo, downscale bool) (splitSupervoxel, remainSupervoxel, mutID uint64, err error) {
	timedLog := dvid.NewTimeLog()

	// Create new labels for this split that will persist to store
	if splitlabel != 0 {
		splitSupervoxel = splitlabel
		if _, err = d.updateMaxLabel(v, splitlabel); err != nil {
			return
		}
	} else if splitSupervoxel, err = d.newLabel(v); err != nil {
		return
	}
	if remainlabel != 0 {
		remainSupervoxel = remainlabel
		if _, err = d.updateMaxLabel(v, remainlabel); err != nil {
			return
		}
	} else if remainSupervoxel, err = d.newLabel(v); err != nil {
		return
	}
	dvid.Debugf("Splitting subset of label %d into new label %d and renaming remainder to label %d...\n", svlabel, splitSupervoxel, remainSupervoxel)

	// Read the sparse volume from reader.
	var split dvid.RLEs
	split, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	splitSize, _ := split.Stats()
	if splitSize == 0 {
		dvid.Infof("split on supervoxel %d -> %d was given split size of 0\n", svlabel, remainlabel)
	}

	// read parent label index and do simple check on split size
	var mapping *SVMap
	mapping, err = getMapping(d, v)
	if err != nil {
		return
	}
	label := svlabel
	if mapping != nil {
		if mapped, found := mapping.MappedLabel(v, svlabel); found {
			if mapped == 0 {
				err = fmt.Errorf("cannot get label for supervoxel %d, which has been split and doesn't exist anymore", svlabel)
			}
			label = mapped
		}
	}
	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	idx, err := getCachedLabelIndex(d, v, label)
	if err != nil {
		err = fmt.Errorf("split supervoxel index for data %q, supervoxel %d: %v", d.DataName(), svlabel, err)
		return
	}
	if idx == nil {
		err = fmt.Errorf("unable to split supervoxel %d for data %q: missing label index %d", svlabel, d.DataName(), label)
		return
	}
	svSize := idx.GetSupervoxelCount(svlabel)
	if splitSize > svSize {
		err = fmt.Errorf("split volume of %d > %d of supervoxel %d", splitSize, svSize, svlabel)
		return
	}
	if splitSize == svSize {
		dvid.Infof("split on supervoxel %d -> %d was given split size %d, which is entire supervoxel\n", svlabel, splitlabel, splitSize)
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
	mutID = d.NewMutationID()
	versionuuid, _ := datastore.UUIDFromVersion(v)
	msginfo := map[string]interface{}{
		"Action":           "split-supervoxel",
		"Supervoxel":       svlabel,
		"SplitSupervoxel":  splitSupervoxel,
		"RemainSupervoxel": remainSupervoxel,
		"Split":            splitRef,
		"MutationID":       mutID,
		"UUID":             string(versionuuid),
		"Timestamp":        time.Now().String(),
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
	var downresMut *downres.Mutation
	if downscale {
		downresMut = downres.NewMutation(d, v, mutID)
	}

	var splitblks dvid.IZYXSlice
	if splitblks, err = d.splitSupervoxelIndex(v, info, op, idx); err != nil {
		return
	}

	getLog := dvid.NewTimeLog()
	blockCh := make(chan *labels.PositionedBlock, len(splitblks))
	errCh := make(chan error, len(splitblks))
	ctx := datastore.NewVersionedCtx(d, v)

	numHandlers := 16
	for i := 0; i < numHandlers; i++ {
		go d.splitSupervoxelThread(ctx, downresMut, op, idx.Blocks, blockCh, errCh)
	}

	origBlocks := make([]*labels.PositionedBlock, len(splitblks))
	var numBlocks int
	var scale uint8
	for _, izyx := range splitblks {
		var pb *labels.PositionedBlock
		pb, err = d.getLabelBlock(ctx, scale, izyx)
		if err != nil {
			d.restoreOldBlocks(ctx, numBlocks, origBlocks)
			return
		}
		if pb == nil {
			dvid.Errorf("supervoxel split of %d: block %s should have been split but was nil\n", svlabel, izyx)
			continue
		}
		origBlocks[numBlocks] = pb
		numBlocks++
		blockCh <- pb
	}

	// Wait for all blocks in supervoxel to be relabeled before returning.
	getLog.Debugf("supervoxel split of %d: got %d blocks", svlabel, numBlocks)
	var numErr int
	for i := 0; i < numBlocks; i++ {
		processErr := <-errCh
		if processErr != nil {
			err = processErr
			numErr++
		}
	}
	close(blockCh)
	if err != nil {
		err = fmt.Errorf("supervoxel split of %d: %d errors, last one: %v", svlabel, numErr, err)
		d.restoreOldBlocks(ctx, numBlocks, origBlocks)
		return
	}
	if err = addSupervoxelSplitToMapping(d, v, op); err != nil {
		return
	}
	if err = labels.LogSupervoxelSplit(d, v, op); err != nil {
		return
	}
	// store the new split index
	if err = putCachedLabelIndex(d, v, idx); err != nil {
		d.restoreOldBlocks(ctx, numBlocks, origBlocks)
		err = fmt.Errorf("split supervoxel index for data %q, supervoxel %d: %v", d.DataName(), op.Supervoxel, err)
		return
	}

	if downresMut != nil {
		if err = downresMut.Execute(); err != nil {
			dvid.Criticalf("down-res compute of supervoxel split %d failed with error: %v\n", svlabel, err)
			dvid.Criticalf("down-res error can lead to sync issue between scale 0 and higher affecting these blocks: %s\n", splitblks)
			return
		}
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
		"Timestamp":  time.Now().String(),
	}
	jsonmsg, _ = json.Marshal(msginfo)
	if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
		dvid.Errorf("error on sending split complete op to kafka: %v", err)
	}
	return
}

func (d *Data) restoreOldBlocks(ctx *datastore.VersionedCtx, numBlocks int, blocks []*labels.PositionedBlock) {
	var scale uint8
	for i := 0; i < numBlocks; i++ {
		if err := d.putLabelBlock(ctx, scale, blocks[i]); err != nil {
			dvid.Criticalf("unable to put back block %s, data %q after error: %v", blocks[i].BCoord, d.DataName(), err)
		}
	}
}

// splits a set of voxels to a specified label within a block
func (d *Data) splitSupervoxelThread(ctx *datastore.VersionedCtx, downresMut *downres.Mutation, op labels.SplitSupervoxelOp, idxblocks map[uint64]*proto.SVCount, blockCh chan *labels.PositionedBlock, errCh chan error) {
	var scale uint8
	for pb := range blockCh {
		zyx, err := labels.IZYXStringToBlockIndex(pb.BCoord)
		if err != nil {
			errCh <- fmt.Errorf("couldn't convert block coord %s to block index: %v", pb.BCoord, err)
			continue
		}
		svc, found := idxblocks[zyx]
		if !found {
			errCh <- fmt.Errorf("tried to supervoxel %d split block %s but was not in label index", op.Supervoxel, pb.BCoord)
			continue
		}
		idxKeptSize := uint64(svc.Counts[op.RemainSupervoxel])
		idxSplitSize := uint64(svc.Counts[op.SplitSupervoxel])

		splitBlock, keptSize, splitSize, err := pb.SplitSupervoxel(op)
		if err != nil {
			errCh <- fmt.Errorf("can't modify supervoxel %d, block %s: %v", op.Supervoxel, pb.BCoord, err)
			continue
		}
		if keptSize != idxKeptSize || splitSize != idxSplitSize {
			errCh <- fmt.Errorf("ran supervoxel %d split on block %s: got %d split, %d remain voxels, different from label index %d split, %d remain", op.Supervoxel, pb.BCoord, splitSize, keptSize, idxSplitSize, idxKeptSize)
			continue
		}

		splitpb := labels.PositionedBlock{*splitBlock, pb.BCoord}
		if err := d.putLabelBlock(ctx, scale, &splitpb); err != nil {
			errCh <- fmt.Errorf("unable to put block %s in split of label %d, data %q: %v", pb.BCoord, op.Supervoxel, d.DataName(), err)
			continue
		}

		if downresMut != nil {
			if err = downresMut.BlockMutated(pb.BCoord, splitBlock); err != nil {
				err = fmt.Errorf("data %q publishing downres, block %s: %v", d.DataName(), pb.BCoord, err)
			}
		}
		errCh <- err
	}
}
