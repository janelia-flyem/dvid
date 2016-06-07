package labelmeta

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBufferSize = 100

// BlockOnUpdating blocks until the given data is not updating from syncs.
// This is primarily used during testing.
func BlockOnUpdating(uuid dvid.UUID, name dvid.InstanceName) error {
	time.Sleep(100 * time.Millisecond)
	d, err := GetByUUID(uuid, name)
	if err != nil {
		return err
	}
	for d.Updating() {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

type LabelLabelmetas map[uint64]Labelmetas

func (lm LabelLabelmetas) add(label uint64, lmeta Labelmeta) {
	if label == 0 {
		return
	}
	lmetas, found := lm[label]
	if found {
		lmetas = append(lmetas, lmeta)
		lm[label] = lmetas
	} else {
		lm[label] = Labelmetas{lmeta}
	}
}

type LabelPoints map[uint64][]dvid.Point3d

func (lp LabelPoints) add(label uint64, pt dvid.Point3d) {
	if label == 0 {
		return
	}
	pts, found := lp[label]
	if found {
		pts = append(pts, pt)
		lp[label] = pts
	} else {
		lp[label] = []dvid.Point3d{pt}
	}
}

// GetSyncSubs implements the datastore.Syncer interface.  Returns a list of subscriptions
// to the sync data instance that will notify the receiver.
func (d *Data) GetSyncSubs(syncData dvid.Data) datastore.SyncSubs {
	// Our syncing depends on the datatype we are syncing.
	switch syncData.TypeName() {
	case "labelblk":
		return d.initSyncLabelblk(syncData.DataName())
	case "labelvol":
		return d.initSyncLabelvol(syncData.DataName())
	default:
		dvid.Errorf("Unable to sync %s with %s since datatype %q is not supported.", d.DataName(), syncData.DataName(), syncData.TypeName())
	}
	return nil
}

func (d *Data) initSyncLabelblk(name dvid.InstanceName) datastore.SyncSubs {
	syncCh := make(chan datastore.SyncMessage, syncBufferSize)
	doneCh := make(chan struct{})

	subs := datastore.SyncSubs{
		{
			Event:  datastore.SyncEvent{name, labels.IngestBlockEvent},
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
		{
			Event:  datastore.SyncEvent{name, labels.MutateBlockEvent},
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
		{
			Event:  datastore.SyncEvent{name, labels.DeleteBlockEvent},
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
	}

	// Launch handlers of sync events.
	go d.handleBlockEvent(syncCh, doneCh)

	return subs
}

func (d *Data) initSyncLabelvol(name dvid.InstanceName) datastore.SyncSubs {
	mergeCh := make(chan datastore.SyncMessage, 100)
	mergeDone := make(chan struct{})

	splitCh := make(chan datastore.SyncMessage, 10) // Splits can be a lot bigger due to sparsevol
	splitDone := make(chan struct{})

	subs := datastore.SyncSubs{
		datastore.SyncSub{
			Event:  datastore.SyncEvent{name, labels.MergeBlockEvent},
			Notify: d.DataName(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{name, labels.SplitLabelEvent},
			Notify: d.DataName(),
			Ch:     splitCh,
			Done:   splitDone,
		},
	}

	// Launch handlers of sync events.
	go d.syncMerge(mergeCh, mergeDone)
	go d.syncSplit(splitCh, splitDone)

	return subs
}

// Processes each labelblk change as we get it.
func (d *Data) handleBlockEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		dvid.Errorf("handleBlockEvent %v\n", err)
		return
	}

	for msg := range in {
		select {
		case <-done:
			return
		default:
			d.StartUpdate()
			ctx := datastore.NewVersionedCtx(d, msg.Version)
			switch delta := msg.Delta.(type) {
			case imageblk.Block:
				d.ingestBlock(ctx, delta, batcher)
			case imageblk.MutatedBlock:
				d.mutateBlock(ctx, delta, batcher)
			case labels.DeleteBlock:
				d.deleteBlock(ctx, delta, batcher)
			default:
				dvid.Criticalf("Cannot sync labelmeta from block event.  Got unexpected delta: %v\n", msg)
			}
			d.StopUpdate()
		}
	}
}

// If a block of labels is ingested, adjust each label's labelmeta list.
func (d *Data) ingestBlock(ctx *datastore.VersionedCtx, block imageblk.Block, batcher storage.KeyValueBatcher) {
	// Get the labelmetas for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	lmetas, err := getLabelmetas(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting labelmetas for block %s: %v\n", chunkPt, err)
		return
	}
	if len(lmetas) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all labelmeta positions, finding corresponding label and storing labelmetas.
	toAdd := LabelLabelmetas{}
	for _, lmeta := range lmetas {
		pt := lmeta.Pos.Point3dInChunk(blockSize)
		i := (pt[2]*bY+pt[1])*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		toAdd.add(label, lmeta)
	}

	// Add any non-zero label labelmetas to their respective label k/v.
	for label, addLmetas := range toAdd {
		tk := NewLabelTKey(label)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting labelmetas for label %d: %v\n", label, err)
			return
		}
		lmetas.add(addLmetas)
		val, err := json.Marshal(lmetas)
		if err != nil {
			dvid.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in labelmetas %q after delete block: %v\n", d.DataName(), err)
	}
}

// If a block of labels is mutated, adjust any label that was either removed or added.
func (d *Data) mutateBlock(ctx *datastore.VersionedCtx, block imageblk.MutatedBlock, batcher storage.KeyValueBatcher) {
	// Get the labelmetas for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	lmetas, err := getLabelmetas(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting labelmetas for block %s: %v\n", chunkPt, err)
		return
	}
	if len(lmetas) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all labelmeta positions, finding corresponding label and storing labelmetas.
	labels := make(map[uint64]struct{})
	toAdd := LabelLabelmetas{}
	toDel := LabelPoints{}
	for _, lmeta := range lmetas {
		pt := lmeta.Pos.Point3dInChunk(blockSize)
		i := pt[2]*bY + pt[1]*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		var prev uint64
		if len(block.Prev) != 0 {
			prev = binary.LittleEndian.Uint64(block.Prev[i : i+8])
		}
		if label == prev {
			continue
		}
		if label != 0 {
			toAdd.add(label, lmeta)
			labels[label] = struct{}{}
		}
		if prev != 0 {
			toDel.add(prev, lmeta.Pos)
			labels[prev] = struct{}{}
		}
	}

	// Modify any modified label k/v.
	for label := range labels {
		tk := NewLabelTKey(label)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting labelmetas for label %d: %v\n", label, err)
			return
		}
		additions, found := toAdd[label]
		if found {
			lmetas.add(additions)
		}
		deletions, found := toDel[label]
		if found {
			for _, pt := range deletions {
				lmetas.delete(pt)
			}
		}
		val, err := json.Marshal(lmetas)
		if err != nil {
			dvid.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)
	}
	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in labelmetas %q after delete block: %v\n", d.DataName(), err)
	}
}

// If a block of labels is deleted, the associated labelmetas should be changed to zero label labelmetas.
func (d *Data) deleteBlock(ctx *datastore.VersionedCtx, block labels.DeleteBlock, batcher storage.KeyValueBatcher) {
	// Get the labelmetas for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	lmetas, err := getLabelmetas(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting labelmetas for block %s: %v\n", chunkPt, err)
		return
	}
	if len(lmetas) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all labelmeta positions, finding corresponding label and storing labelmetas.
	toDel := LabelPoints{}
	for _, lmeta := range lmetas {
		pt := lmeta.Pos.Point3dInChunk(blockSize)
		i := pt[2]*bY + pt[1]*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		toDel.add(label, lmeta.Pos)
	}

	// Delete any non-zero label labelmetas from their respective label k/v.
	for label, pts := range toDel {
		tk := NewLabelTKey(label)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting labelmetas for label %d: %v\n", label, err)
			return
		}
		save := false
		for _, pt := range pts {
			_, changed := lmetas.delete(pt)
			if changed {
				save = true
			}
		}
		if save {
			if len(lmetas) == 0 {
				batch.Delete(tk)
			} else {
				val, err := json.Marshal(lmetas)
				if err != nil {
					dvid.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
					return
				}
				batch.Put(tk, val)
			}
		}
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in labelmetas %q after delete block: %v\n", d.DataName(), err)
	}
}

// If one or more labels are merged, remove old label->labelmetas k/v and add to target label.
func (d *Data) syncMerge(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		dvid.Errorf("syncMerge %v\n", err)
		return
	}
	for msg := range in {
		select {
		case <-done:
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaMergeStart:
				// don't worry about it.
			case labels.DeltaMerge:
				if err := d.mergeLabels(batcher, msg.Version, delta.MergeOp); err != nil {
					dvid.Errorf("error on merging labels for data %q: %v\n", d.DataName(), err)
					continue
				}
			default:
				dvid.Criticalf("Unknown delta %v received in labelmeta instance %q\n", delta, d.DataName())
			}
		}
	}
}

func (d *Data) mergeLabels(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.MergeOp) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Get the target label
	targetTk := NewLabelTKey(op.Target)
	targetLmetas, err := getLabelmetas(ctx, targetTk)
	if err != nil {
		return fmt.Errorf("get labelmetas for instance %q, target %d, in syncMerge: %v\n", d.DataName(), op.Target, err)
	}

	// Iterate through each merged label, read old labelmetas, delete that k/v, then add it to the current target labelmetas.
	lmetasAdded := 0
	for label := range op.Merged {
		tk := NewLabelTKey(label)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get labelmetas for instance %q, label %d in syncMerge: %v\n", d.DataName(), label, err)
		}
		if lmetas == nil || len(lmetas) == 0 {
			continue
		}
		batch.Delete(tk)
		lmetasAdded += len(lmetas)
		targetLmetas = append(targetLmetas, lmetas...)
	}
	if lmetasAdded > 0 {
		val, err := json.Marshal(targetLmetas)
		if err != nil {
			return fmt.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
		}
		batch.Put(targetTk, val)
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("unable to commit merge for instance %q: %v\n", d.DataName(), err)
		}
	}
	d.StopUpdate()
	return nil
}

func (d *Data) syncSplit(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		dvid.Errorf("syncSplit: %v\n", err)
		return
	}
	for msg := range in {
		select {
		case <-done:
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaSplitStart:
				// Don't worry about it.
			case labels.DeltaSplit:
				if delta.Split == nil {
					// This is a coarse split.
					if d.splitLabelsCoarse(batcher, msg.Version, delta); err != nil {
						dvid.Errorf("error on splitting label for data %q: %v\n", d.DataName(), err)
						continue
					}
				} else {
					if d.splitLabelsFine(batcher, msg.Version, delta); err != nil {
						dvid.Errorf("error on splitting label for data %q: %v\n", d.DataName(), err)
						continue
					}
				}
			default:
				dvid.Criticalf("labelmeta split sync: bad delta in split event: %v\n", msg.Delta)
				continue
			}
		}
	}
}

func (d *Data) splitLabelsCoarse(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Get the labelmetas for the old label.
	oldTk := NewLabelTKey(op.OldLabel)
	oldLmetas, err := getLabelmetas(ctx, oldTk)
	if err != nil {
		return fmt.Errorf("unable to get labelmetas for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.OldLabel, err)
	}

	// Create a map to test each point.
	splitBlocks := make(map[dvid.IZYXString]struct{})
	for _, zyxStr := range op.SortedBlocks {
		splitBlocks[zyxStr] = struct{}{}
	}

	// Separate any labelmetas that are within the split blocks.
	toDel := make(map[int]struct{})
	toAdd := Labelmetas{}
	blockSize := d.blockSize(ctx.VersionID())
	for i, lmeta := range oldLmetas {
		zyxStr := lmeta.Pos.ToIZYXString(blockSize)
		if _, found := splitBlocks[zyxStr]; found {
			toDel[i] = struct{}{}
			toAdd = append(toAdd, lmeta)
		}
	}
	if len(toDel) == 0 {
		return nil
	}

	// Store split labelmetas into new label labelmetas.
	newTk := NewLabelTKey(op.NewLabel)
	newLmetas, err := getLabelmetas(ctx, newTk)
	if err != nil {
		return fmt.Errorf("unable to get labelmetas for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.NewLabel, err)
	}
	newLmetas.add(toAdd)
	val, err := json.Marshal(newLmetas)
	if err != nil {
		return fmt.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
	}
	batch.Put(newTk, val)

	// Delete any split from old label labelmetas.
	filtered := oldLmetas[:0]
	for i, lmeta := range oldLmetas {
		if _, found := toDel[i]; !found {
			filtered = append(filtered, lmeta)
		}
	}

	// Delete or store k/v depending on what remains.
	if len(filtered) == 0 {
		batch.Delete(oldTk)
	} else {
		val, err := json.Marshal(filtered)
		if err != nil {
			return fmt.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
		}
		batch.Put(oldTk, val)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in labelmetas %q after split: %v\n", d.DataName(), err)
	}
	return nil
}

func (d *Data) splitLabelsFine(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	toAdd := Labelmetas{}
	toDel := []dvid.Point3d{}

	// Iterate through each split block, get the labelmetas, and then modify the previous and new label k/v.
	for izyx, rles := range op.Split {
		// Get the labelmetas for this block.
		blockPt, err := izyx.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(blockPt)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting labelmetas for block %s: %v\n", blockPt, err)
		}
		if len(lmetas) == 0 {
			return nil
		}

		// For any labelmeta within the split RLEs, add to the delete and addition lists.
		for _, lmeta := range lmetas {
			for _, rle := range rles {
				if rle.Within(lmeta.Pos) {
					toAdd = append(toAdd, lmeta)
					toDel = append(toDel, lmeta.Pos)
					break
				}
			}
		}
	}

	// Modify the old label k/v
	if len(toDel) != 0 {
		tk := NewLabelTKey(op.OldLabel)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get labelmetas for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.OldLabel, err)
		}
		save := false
		for _, pt := range toDel {
			_, changed := lmetas.delete(pt)
			if changed {
				save = true
			}
		}
		if save {
			if len(lmetas) == 0 {
				batch.Delete(tk)
			} else {
				val, err := json.Marshal(lmetas)
				if err != nil {
					return fmt.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
				}
				batch.Put(tk, val)
			}
		}
	}

	// Modify the new label k/v
	if len(toAdd) != 0 {
		tk := NewLabelTKey(op.NewLabel)
		lmetas, err := getLabelmetas(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get labelmetas for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.NewLabel, err)
		}
		lmetas.add(toAdd)
		val, err := json.Marshal(lmetas)
		if err != nil {
			return fmt.Errorf("couldn't serialize labelmetas in instance %q: %v\n", d.DataName(), err)
		}
		batch.Put(tk, val)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in labelmetas %q after split: %v\n", d.DataName(), err)
	}
	return nil
}
