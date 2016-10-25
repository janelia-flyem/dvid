package labelvol

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// TODO
// var (
// 	// These are the labels that are in the process of modification from merge, split, or other sync events.
// 	dirtyBlocks dvid.DirtyBlocks
// )

// Number of change messages we can buffer before blocking on sync channel.
const syncBufferSize = 100

// InitDataHandlers launches goroutines to handle each labelvol instance's syncs.
func (d *Data) InitDataHandlers() error {
	if d.syncCh != nil || d.syncDone != nil {
		return nil
	}
	d.syncCh = make(chan datastore.SyncMessage, syncBufferSize)
	d.syncDone = make(chan *sync.WaitGroup)

	go d.handleBlockEvent()
	return nil
}

// Shutdown terminates blocks until syncs are done then terminates background goroutines processing data.
func (d *Data) Shutdown() {
	if d.syncDone != nil {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		d.syncDone <- wg
		wg.Wait() // Block until we are done.
	}
}

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(synced dvid.Data) (datastore.SyncSubs, error) {
	if d.syncCh == nil {
		if err := d.InitDataHandlers(); err != nil {
			return nil, fmt.Errorf("unable to initialize handlers for data %q: %v\n", d.DataName(), err)
		}
	}

	subs := datastore.SyncSubs{
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.IngestBlockEvent},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		},
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.MutateBlockEvent},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		},
		{
			Event:  datastore.SyncEvent{synced.DataUUID(), labels.DeleteBlockEvent},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		},
	}
	return subs, nil
}

// Processes each change as we get it.
// TODO -- accumulate larger # of changes before committing to prevent
// excessive compaction time?  This assumes LSM storage engine, which
// might not always hold in future, so stick with incremental update
// until proven to be a bottleneck.
func (d *Data) handleBlockEvent() {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}
	var stop bool
	var wg *sync.WaitGroup
	for {
		select {
		case wg = <-d.syncDone:
			queued := len(d.syncCh)
			if queued > 0 {
				dvid.Infof("Received shutdown signal for %q sync events (%d in queue)\n", d.DataName(), queued)
				stop = true
			} else {
				dvid.Infof("Shutting down sync event handler for instance %q...\n", d.DataName())
				wg.Done()
				return
			}
		case msg := <-d.syncCh:
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
				dvid.Criticalf("Cannot sync labelvol from block event.  Got unexpected delta: %v\n", msg)
			}
			d.StopUpdate()

			if stop && len(d.syncCh) == 0 {
				dvid.Infof("Shutting down sync even handler for instance %q after draining sync events.\n", d.DataName())
				wg.Done()
				return
			}
		}
	}
}

func (d *Data) deleteBlock(ctx *datastore.VersionedCtx, block labels.DeleteBlock, batcher storage.KeyValueBatcher) {
	batch := batcher.NewBatch(ctx)

	// Iterate through this block of labels and get set of labels.
	blockBytes := len(block.Data)
	if blockBytes != int(d.BlockSize.Prod())*8 {
		dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			blockBytes, d.BlockSize.Prod())
		return
	}
	labelSet := make(map[uint64]struct{})
	for i := 0; i < blockBytes; i += 8 {
		label := binary.LittleEndian.Uint64(block.Data[i : i+8])
		if label != 0 {
			labelSet[label] = struct{}{}
		}
	}

	// Go through all non-zero labels and delete the corresponding labelvol k/v pair.
	zyx := block.Index.ToIZYXString()
	for label := range labelSet {
		tk := NewTKey(label, zyx)
		batch.Delete(tk)
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("Bad sync in labelvol.  Couldn't commit block %s\n", zyx)
	}
	return
}

// Note that this does not delete any removed labels in the block since we only get the CURRENT block
// and not PAST blocks.  To allow mutation of label blocks, use mutateBlock().
func (d *Data) ingestBlock(ctx *datastore.VersionedCtx, block imageblk.Block, batcher storage.KeyValueBatcher) {
	// Iterate through this block of labels and create RLEs for each label.
	blockBytes := len(block.Data)
	if blockBytes != int(d.BlockSize.Prod())*8 {
		dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			blockBytes, d.BlockSize.Prod())
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := block.Index.MinPoint(d.BlockSize)
	lastPt := block.Index.MaxPoint(d.BlockSize)

	var curStart dvid.Point3d
	var voxelLabel, curLabel, maxLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				voxelLabel = binary.LittleEndian.Uint64(block.Data[start : start+8])
				if maxLabel < voxelLabel {
					maxLabel = voxelLabel
				}
				start += 8

				// If we hit background or have switched label, save old run and start new one.
				if voxelLabel == 0 || voxelLabel != curLabel {
					// Save old run
					if curRun > 0 {
						labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					}
					// Start new one if not zero label.
					if voxelLabel != 0 {
						curStart = dvid.Point3d{x, y, z}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = voxelLabel
				} else {
					curRun++
				}
			}
			// Force break of any runs when we finish x scan.
			if curRun > 0 {
				labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
				curLabel = 0
				curRun = 0
			}
		}
	}

	// Store the RLEs for each label in this block.
	if maxLabel > 0 {
		batch := batcher.NewBatch(ctx)
		blockStr := block.Index.ToIZYXString()
		for label, rles := range labelRLEs {
			tk := NewTKey(label, blockStr)
			rleBytes, err := rles.MarshalBinary()
			if err != nil {
				dvid.Errorf("Bad encoding labelvol keys for label %d: %v\n", label, err)
				return
			}
			batch.Put(tk, rleBytes)
		}
		// compare-and-set MaxLabel and batch commit
		d.casMaxLabel(batch, ctx.VersionID(), maxLabel)
	}
}

func (d *Data) mutateBlock(ctx *datastore.VersionedCtx, block imageblk.MutatedBlock, batcher storage.KeyValueBatcher) {
	// Iterate through previous and current labels, detecting set of previous labels and RLEs for current labels.
	blockBytes := len(block.Data)
	if blockBytes != int(d.BlockSize.Prod())*8 {
		dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			blockBytes, d.BlockSize.Prod())
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	labelDiff := make(map[uint64]bool, 10)

	firstPt := block.Index.MinPoint(d.BlockSize)
	lastPt := block.Index.MaxPoint(d.BlockSize)

	var curStart dvid.Point3d
	var voxelLabel, curLabel, maxLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				var pastLabel uint64
				if block.Prev == nil || len(block.Prev) == 0 {
					pastLabel = 0
				} else {
					pastLabel = binary.LittleEndian.Uint64(block.Prev[start : start+8])
				}
				voxelLabel = binary.LittleEndian.Uint64(block.Data[start : start+8])
				if maxLabel < voxelLabel {
					maxLabel = voxelLabel
				}
				if pastLabel != 0 {
					if pastLabel != voxelLabel {
						labelDiff[pastLabel] = true
					} else {
						_, found := labelDiff[pastLabel]
						if !found {
							labelDiff[pastLabel] = false
						}
					}
				}
				start += 8

				// If we hit background or have switched label, save old run and start new one.
				if voxelLabel == 0 || voxelLabel != curLabel {
					// Save old run
					if curRun > 0 {
						labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					}
					// Start new one if not zero label.
					if voxelLabel != 0 {
						curStart = dvid.Point3d{x, y, z}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = voxelLabel
				} else {
					curRun++
				}
			}
			// Force break of any runs when we finish x scan.
			if curRun > 0 {
				labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
				curLabel = 0
				curRun = 0
			}
		}
	}

	// If a previous label has no change with current label RLE, then delete the label RLE since no changes
	// are necessary.  Else if previous label is not present in current label RLEs, delete labelvol.
	var deletes []storage.TKey
	blockStr := block.Index.ToIZYXString()
	for label, diff := range labelDiff {
		_, found := labelRLEs[label]
		if diff && !found {
			// mark previous label's RLEs for deletion
			tk := NewTKey(label, blockStr)
			deletes = append(deletes, tk)
		} else if !diff && found {
			// delete current label's RLEs because there's no difference with past RLE
			delete(labelRLEs, label)
		}
	}
	if len(deletes) > 0 {
		batch := batcher.NewBatch(ctx)
		for _, tk := range deletes {
			batch.Delete(tk)
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("batch commit on deleting previous labels' labelvols: %v\n", err)
		}
	}

	// Store the RLEs for each label in this block that are new or modified.
	if len(labelRLEs) > 0 {
		batch := batcher.NewBatch(ctx)
		for label, rles := range labelRLEs {
			tk := NewTKey(label, blockStr)
			rleBytes, err := rles.MarshalBinary()
			if err != nil {
				dvid.Errorf("Bad encoding labelvol keys for label %d: %v\n", label, err)
				return
			}
			batch.Put(tk, rleBytes)
		}
		// compare-and-set MaxLabel and batch commit
		d.casMaxLabel(batch, ctx.VersionID(), maxLabel)
	}
}
