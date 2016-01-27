package annotation

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

type LabelElements map[uint64]Elements

func (le LabelElements) add(label uint64, elem Element) {
	if label == 0 {
		return
	}
	elems, found := le[label]
	if found {
		elems = append(elems, elem)
		le[label] = elems
	} else {
		le[label] = Elements{elem}
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
	store, err := storage.MutableStore()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
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
				dvid.Criticalf("Cannot sync synapse from block event.  Got unexpected delta: %v\n", msg)
			}
			d.StopUpdate()
		}
	}
}

// If a block of labels is ingested, adjust each label's synaptic element list.
func (d *Data) ingestBlock(ctx *datastore.VersionedCtx, block imageblk.Block, batcher storage.KeyValueBatcher) {
	// Get the synaptic elements for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	elems, err := getElements(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting elements for block %s: %v\n", chunkPt, err)
		return
	}
	if len(elems) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all element positions, finding corresponding label and storing elements.
	toAdd := LabelElements{}
	for _, elem := range elems {
		pt := elem.Pos.Point3dInChunk(blockSize)
		i := (pt[2]*bY+pt[1])*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		toAdd.add(label, elem)
	}

	// Add any non-zero label elements to their respective label k/v.
	for label, addElems := range toAdd {
		tk := NewLabelTKey(label)
		elems, err := getElements(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting elements for label %d: %v\n", label, err)
			return
		}
		elems.add(addElems)
		val, err := json.Marshal(elems)
		if err != nil {
			dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in annotations %q after delete block: %v\n", d.DataName(), err)
	}
}

// If a block of labels is mutated, adjust any label that was either removed or added.
func (d *Data) mutateBlock(ctx *datastore.VersionedCtx, block imageblk.MutatedBlock, batcher storage.KeyValueBatcher) {
	// Get the synaptic elements for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	elems, err := getElements(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting elements for block %s: %v\n", chunkPt, err)
		return
	}
	if len(elems) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all element positions, finding corresponding label and storing elements.
	labels := make(map[uint64]struct{})
	toAdd := LabelElements{}
	toDel := LabelPoints{}
	for _, elem := range elems {
		pt := elem.Pos.Point3dInChunk(blockSize)
		i := (pt[2]*bY+pt[1])*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		prev := binary.LittleEndian.Uint64(block.Prev[i : i+8])
		if label == prev {
			continue
		}
		if label != 0 {
			toAdd.add(label, elem)
			labels[label] = struct{}{}
		}
		if prev != 0 {
			toDel.add(prev, elem.Pos)
			labels[prev] = struct{}{}
		}
	}

	// Modify any modified label k/v.
	for label := range labels {
		tk := NewLabelTKey(label)
		elems, err := getElements(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting elements for label %d: %v\n", label, err)
			return
		}
		additions, found := toAdd[label]
		if found {
			elems.add(additions)
		}
		deletions, found := toDel[label]
		if found {
			for _, pt := range deletions {
				elems.delete(pt)
			}
		}
		val, err := json.Marshal(elems)
		if err != nil {
			dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)
	}
	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in annotations %q after delete block: %v\n", d.DataName(), err)
	}
}

// If a block of labels is deleted, the associated synapse elements should be changed to zero label elements.
func (d *Data) deleteBlock(ctx *datastore.VersionedCtx, block labels.DeleteBlock, batcher storage.KeyValueBatcher) {
	// Get the synaptic elements for this block
	chunkPt := dvid.ChunkPoint3d(*block.Index)
	tk := NewBlockTKey(chunkPt)
	elems, err := getElements(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting elements for block %s: %v\n", chunkPt, err)
		return
	}
	if len(elems) == 0 {
		return
	}
	blockSize := d.blockSize(ctx.VersionID())
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all element positions, finding corresponding label and storing elements.
	toDel := LabelPoints{}
	for _, elem := range elems {
		pt := elem.Pos.Point3dInChunk(blockSize)
		i := (pt[2]*bY+pt[1])*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		toDel.add(label, elem.Pos)
	}

	// Delete any non-zero label elements from their respective label k/v.
	for label, pts := range toDel {
		tk := NewLabelTKey(label)
		elems, err := getElements(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting elements for label %d: %v\n", label, err)
			return
		}
		save := false
		for _, pt := range pts {
			_, changed := elems.delete(pt)
			if changed {
				save = true
			}
		}
		if save {
			if len(elems) == 0 {
				batch.Delete(tk)
			} else {
				val, err := json.Marshal(elems)
				if err != nil {
					dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
					return
				}
				batch.Put(tk, val)
			}
		}
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in annotations %q after delete block: %v\n", d.DataName(), err)
	}
}

// If one or more labels are merged, remove old label->elements k/v and add to target label.
func (d *Data) syncMerge(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	store, err := storage.MutableStore()
	if err != nil {
		dvid.Errorf("Data type annotation had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
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
				dvid.Criticalf("Unknown delta %v received in annotation instance %q\n", delta, d.DataName())
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
	targetElems, err := getElements(ctx, targetTk)
	if err != nil {
		return fmt.Errorf("get annotations for instance %q, target %d, in syncMerge: %v\n", d.DataName(), op.Target, err)
	}

	// Iterate through each merged label, read old elements, delete that k/v, then add it to the current target elements.
	elemsAdded := 0
	for label := range op.Merged {
		tk := NewLabelTKey(label)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get annotation elements for instance %q, label %d in syncMerge: %v\n", d.DataName(), label, err)
		}
		if elems == nil || len(elems) == 0 {
			continue
		}
		batch.Delete(tk)
		elemsAdded += len(elems)
		targetElems = append(targetElems, elems...)
	}
	if elemsAdded > 0 {
		val, err := json.Marshal(targetElems)
		if err != nil {
			return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
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
	store, err := storage.MutableStore()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelblk requires batch-enabled store, which %q is not\n", store)
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
				if d.splitLabels(batcher, msg.Version, delta); err != nil {
					dvid.Errorf("error on splitting label for data %q: %v\n", d.DataName(), err)
					continue
				}
			default:
				dvid.Criticalf("bad delta in split event: %v\n", delta)
				continue
			}
		}
	}
}

func (d *Data) splitLabels(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	toAdd := Elements{}
	toDel := []dvid.Point3d{}

	// Iterate through each split block, get the elements, and then modify the previous and new label k/v.
	for izyx, rles := range op.Split {
		// Get the elements for this block.
		blockPt, err := izyx.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(blockPt)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for block %s: %v\n", blockPt, err)
		}
		if len(elems) == 0 {
			return nil
		}

		// For any element within the split RLEs, add to the delete and addition lists.
		for _, elem := range elems {
			for _, rle := range rles {
				if rle.Within(elem.Pos) {
					toAdd = append(toAdd, elem)
					toDel = append(toDel, elem.Pos)
					break
				}
			}
		}
	}

	// Modify the old label k/v
	if len(toDel) != 0 {
		tk := NewLabelTKey(op.OldLabel)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.OldLabel, err)
		}
		save := false
		for _, pt := range toDel {
			_, changed := elems.delete(pt)
			if changed {
				save = true
			}
		}
		if save {
			if len(elems) == 0 {
				batch.Delete(tk)
			} else {
				val, err := json.Marshal(elems)
				if err != nil {
					return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
				}
				batch.Put(tk, val)
			}
		}
	}

	// Modify the new label k/v
	if len(toAdd) != 0 {
		tk := NewLabelTKey(op.NewLabel)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.NewLabel, err)
		}
		elems.add(toAdd)
		val, err := json.Marshal(elems)
		if err != nil {
			return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
		}
		batch.Put(tk, val)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in annotations %q after split: %v\n", d.DataName(), err)
	}

	d.StopUpdate()
	return nil
}
