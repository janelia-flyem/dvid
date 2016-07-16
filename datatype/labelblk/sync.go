/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelblk

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// BlockOnUpdating blocks until the given data is not updating from syncs.
// This is primarily used during testing.
func BlockOnUpdating(uuid dvid.UUID, name dvid.InstanceName) error {
	d, err := GetByUUIDName(uuid, name)
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	for d.Updating() {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(synced dvid.Data) datastore.SyncSubs {
	mergeCh := make(chan datastore.SyncMessage, 100)
	mergeDone := make(chan struct{})

	splitCh := make(chan datastore.SyncMessage, 10) // Splits can be a lot bigger due to sparsevol
	splitDone := make(chan struct{})

	subs := datastore.SyncSubs{
		// {
		// 	Event:  datastore.SyncEvent{synced.DataUUID(), labels.SparsevolModEvent},
		//  Notify: d.DataUUID(),
		// 	Ch:     make(chan datastore.SyncMessage, 100),
		// 	Done:   make(chan struct{}),
		// },
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeStartEvent},
			Notify: d.DataUUID(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		// {
		// 	Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeEndEvent},
		//  Notify: d.DataUUID(),
		// 	Ch:     mergeCh,
		// 	Done:   mergeDone,
		// },
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeBlockEvent},
			Notify: d.DataUUID(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitStartEvent},
			Notify: d.DataUUID(),
			Ch:     splitCh,
			Done:   splitDone,
		},
		// {
		// 	Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitEndEvent},
		//  Notify: d.DataUUID(),
		// 	Ch:     splitCh,
		// 	Done:   splitDone,
		// },
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitLabelEvent},
			Notify: d.DataUUID(),
			Ch:     splitCh,
			Done:   splitDone,
		},
	}

	// Launch go routines to handle sync events.
	go d.syncMerge(synced.DataUUID(), mergeCh, mergeDone)
	go d.syncSplit(splitCh, splitDone)

	return subs
}

func hashStr(s dvid.IZYXString, n int) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(s))
	if err != nil {
		dvid.Criticalf("Could not write to fnv hash in labelblk.hashStr()")
		return 0
	}
	return int(hash.Sum32()) % n
}

type mergeOp struct {
	labels.MergeOp
	ctx  *datastore.VersionedCtx
	izyx dvid.IZYXString
	wg   *sync.WaitGroup
}

func (d *Data) syncMerge(synced dvid.UUID, in <-chan datastore.SyncMessage, done <-chan struct{}) {
	// Start N goroutines to process blocks.  Don't need transactional support for
	// GET-PUT combo if each spatial coordinate (block) is only handled serially by a one goroutine.
	const numprocs = 32
	const mergeBufSize = 100
	var pch [numprocs]chan mergeOp
	for i := 0; i < numprocs; i++ {
		pch[i] = make(chan mergeOp, mergeBufSize)
		go d.mergeBlock(pch[i])
	}

	// Process incoming merge messages
	for msg := range in {
		select {
		case <-done:
			for i := 0; i < numprocs; i++ {
				close(pch[i])
			}
			return
		default:
			iv := dvid.InstanceVersion{synced, msg.Version}
			switch delta := msg.Delta.(type) {
			case labels.DeltaMerge:
				ctx := datastore.NewVersionedCtx(d, msg.Version)
				wg := new(sync.WaitGroup)
				for izyxStr := range delta.Blocks {
					n := hashStr(izyxStr, numprocs)
					wg.Add(1)
					pch[n] <- mergeOp{delta.MergeOp, ctx, izyxStr, wg}
				}
				// When we've processed all the delta blocks, we can remove this merge op
				// from the merge cache since all labels will have completed.
				go func(wg *sync.WaitGroup) {
					wg.Wait()
					labels.MergeStop(iv, delta.MergeOp)
				}(wg)

			case labels.DeltaMergeStart:
				// Add this merge into the cached blockRLEs
				d.StartUpdate()
				labels.MergeStart(iv, delta.MergeOp)
				d.StopUpdate()

			default:
				dvid.Criticalf("bad delta in merge event: %v\n", delta)
				continue
			}
		}
	}
}

// Goroutine that handles relabeling of blocks during a merge operation.
// Since the same block coordinate always gets mapped to the same goroutine, we handle
// concurrency by serializing GET/PUT for a particular block coordinate.
func (d *Data) mergeBlock(in <-chan mergeOp) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)

	for op := range in {
		tk := NewTKeyByCoord(op.izyx)
		data, err := store.Get(op.ctx, tk)
		if err != nil {
			dvid.Errorf("Error on GET of labelblk with coord string %q\n", op.izyx)
			op.wg.Done()
			continue
		}
		if data == nil {
			dvid.Errorf("nil label block where merge was done!\n")
			op.wg.Done()
			continue
		}

		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			dvid.Criticalf("unable to deserialize label block in '%s': %v\n", d.DataName(), err)
			op.wg.Done()
			continue
		}
		if len(blockData) != blockBytes {
			dvid.Criticalf("After labelblk deserialization got back %d bytes, expected %d bytes\n", len(blockData), blockBytes)
			op.wg.Done()
			continue
		}

		// Iterate through this block of labels and relabel if label in merge.
		for i := 0; i < blockBytes; i += 8 {
			label := binary.LittleEndian.Uint64(blockData[i : i+8])
			if _, merged := op.Merged[label]; merged {
				binary.LittleEndian.PutUint64(blockData[i:i+8], op.Target)
			}
		}

		// Store this block.
		serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
		if err != nil {
			dvid.Criticalf("Unable to serialize block in %q: %v\n", d.DataName(), err)
			op.wg.Done()
			continue
		}
		if err := store.Put(op.ctx, tk, serialization); err != nil {
			dvid.Errorf("Error in putting key %v: %v\n", tk, err)
		}
		op.wg.Done()
	}
}

type splitOp struct {
	labels.DeltaSplit
	ctx datastore.VersionedCtx
}

func (d *Data) syncSplit(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	// Start N goroutines to process blocks.  Don't need transactional support for
	// GET-PUT combo if each spatial coordinate (block) is only handled serially by a one goroutine.
	const numprocs = 32
	const splitBufSize = 10
	var pch [numprocs]chan splitOp
	for i := 0; i < numprocs; i++ {
		pch[i] = make(chan splitOp, splitBufSize)
		go d.splitBlock(pch[i])
	}

	for msg := range in {
		select {
		case <-done:
			for i := 0; i < numprocs; i++ {
				close(pch[i])
			}
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaSplit:
				d.StartUpdate()
				ctx := datastore.NewVersionedCtx(d, msg.Version)
				n := delta.OldLabel % numprocs
				pch[n] <- splitOp{delta, *ctx}
				d.StopUpdate()
			default:
				// Don't need to process other events
				continue
			}
		}
	}
}

// Goroutine that handles splits across a lot of blocks for one label.
func (d *Data) splitBlock(in <-chan splitOp) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelblk requires batch-enabled store, which %q is not\n", store)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)

	for op := range in {
		splitOpStart := labels.DeltaSplitStart{op.OldLabel, op.NewLabel}
		splitOpEnd := labels.DeltaSplitEnd{op.OldLabel, op.NewLabel}

		// Mark the old label is under transition
		labels.SplitStart(op.ctx.InstanceVersion(), splitOpStart)

		// Iterate through all the modified blocks, inserting the new label using the RLEs for that block.
		timedLog := dvid.NewTimeLog()
		batch := batcher.NewBatch(&op.ctx)
		for _, zyxStr := range op.SortedBlocks {
			// Read the block.
			tk := NewTKeyByCoord(zyxStr)
			data, err := store.Get(&op.ctx, tk)
			if err != nil {
				dvid.Errorf("Error on GET of labelblk with coord string %v\n", []byte(zyxStr))
				continue
			}
			if data == nil {
				dvid.Errorf("nil label block where split was done, coord %v\n", []byte(zyxStr))
				continue
			}
			bdata, _, err := dvid.DeserializeData(data, true)
			if err != nil {
				dvid.Criticalf("unable to deserialize label block in '%s' key %v: %v\n", d.DataName(), []byte(zyxStr), err)
				continue
			}
			if len(bdata) != blockBytes {
				dvid.Criticalf("splitBlock: coord %v got back %d bytes, expected %d bytes\n", []byte(zyxStr), len(bdata), blockBytes)
				continue
			}

			// Modify the block using either voxel-level changes or coarser block-level mods.
			if op.Split != nil {
				rles, found := op.Split[zyxStr]
				if !found {
					dvid.Errorf("split block %s not present in block RLEs\n", zyxStr.Print())
					continue
				}
				if err := d.storeRLEs(bdata, op.NewLabel, zyxStr, rles); err != nil {
					dvid.Errorf("can't store label %d RLEs into block %s: %v\n", op.NewLabel, zyxStr.Print(), err)
					continue
				}
			} else {
				// We are doing coarse split and will replace all
				if err := d.replaceLabel(bdata, op.OldLabel, op.NewLabel); err != nil {
					dvid.Errorf("can't replace label %d with %d in block %s: %v\n", op.OldLabel, op.NewLabel, zyxStr.Print(), err)
					continue
				}
			}

			// Write the modified block.
			serialization, err := dvid.SerializeData(bdata, d.Compression(), d.Checksum())
			if err != nil {
				dvid.Criticalf("Unable to serialize block %s in %q: %v\n", zyxStr.Print(), d.DataName(), err)
				continue
			}
			batch.Put(tk, serialization)
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Batch PUT during %q block split of %d: %v\n", d.DataName(), op.OldLabel, err)
		}
		labels.SplitStop(op.ctx.InstanceVersion(), splitOpEnd)
		timedLog.Debugf("labelblk sync complete for split of %d -> %d", op.OldLabel, op.NewLabel)
	}
}

// Replace a label in a block.
func (d *Data) replaceLabel(data []byte, fromLabel, toLabel uint64) error {
	n := len(data)
	if n%8 != 0 {
		return fmt.Errorf("label data in block not aligned to uint64: %d bytes", n)
	}
	for i := 0; i < n; i += 8 {
		label := binary.LittleEndian.Uint64(data[i : i+8])
		if label == fromLabel {
			binary.LittleEndian.PutUint64(data[i:i+8], toLabel)
		}
	}
	return nil
}

// Store a label into a block using RLEs.
func (d *Data) storeRLEs(data []byte, toLabel uint64, zyxStr dvid.IZYXString, rles dvid.RLEs) error {
	// Get the block coordinate
	bcoord, err := zyxStr.ToChunkPoint3d()
	if err != nil {
		return err
	}

	// Get the first voxel offset
	blockSize := d.BlockSize()
	offset := bcoord.MinPoint(blockSize)

	// Iterate through rles, getting span for this block of bytes.
	nx := blockSize.Value(0) * 8
	nxy := nx * blockSize.Value(1)
	for _, rle := range rles {
		p := rle.StartPt().Sub(offset)
		i := p.Value(2)*nxy + p.Value(1)*nx + p.Value(0)*8
		for n := int32(0); n < rle.Length(); n++ {
			binary.LittleEndian.PutUint64(data[i:i+8], toLabel)
			i += 8
		}
	}
	return nil
}
