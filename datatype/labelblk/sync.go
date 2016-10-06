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

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numBlockHandlers = 32
)

type procMsg struct {
	op interface{}
	v  dvid.VersionID
	wg *sync.WaitGroup
}

const (
	blockIngest blockOpType = iota
	blockMutate
	blockDelete
)

type blockOpType uint8

type blockOp struct {
	blockOpType
	delta interface{}
}

type mergeOp struct {
	labels.MergeOp
	block dvid.IZYXString
}

type splitOp struct {
	oldLabel uint64
	newLabel uint64
	rles     dvid.RLEs
	block    dvid.IZYXString
}

// InitDataHandlers launches goroutines to handle each labelblk instance's syncs.
func (d *Data) InitDataHandlers() error {
	d.syncCh = make(chan datastore.SyncMessage, 100)
	d.syncDone = make(chan struct{})

	// Start N goroutines to process mutations for each block that will be consistently
	// assigned to one of the N goroutines.
	var procCh [numBlockHandlers]chan procMsg
	for i := 0; i < numBlockHandlers; i++ {
		procCh[i] = make(chan procMsg, 100)
		go d.processBlock(procCh[i])
	}

	go d.processEvents(procCh)
	return nil
}

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(synced dvid.Data) datastore.SyncSubs {
	// Our syncing depends on the datatype we are syncing.
	var evts []string
	switch synced.TypeName() {
	case "labelblk":
		evts = []string{labels.IngestBlockEvent, labels.MutateBlockEvent, labels.DeleteBlockEvent}
	case "labelvol":
		evts = []string{labels.MergeStartEvent, labels.MergeBlockEvent, labels.SplitStartEvent, labels.SplitLabelEvent}
	default:
		dvid.Errorf("Unable to sync %s with %s since datatype %q is not supported.", d.DataName(), synced.DataName(), synced.TypeName())
		return nil
	}

	subs := make(datastore.SyncSubs, len(evts))
	for i, evt := range evts {
		subs[i] = datastore.SyncSub{
			Event:  datastore.SyncEvent{synced.DataUUID(), evt},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
			Done:   d.syncDone,
		}
	}
	return subs
}

// gets all the changes relevant to labelblk, then breaks up any multi-block op into
// separate block ops and puts them onto channels to index-specific handlers.
func (d *Data) processEvents(procCh [numBlockHandlers]chan procMsg) {
	for msg := range d.syncCh {
		switch delta := msg.Delta.(type) {
		case imageblk.Block:
			n := delta.Index.Hash(numBlockHandlers)
			procCh[n] <- procMsg{op: blockOp{blockIngest, delta}, v: msg.Version}

		case imageblk.MutatedBlock:
			n := delta.Index.Hash(numBlockHandlers)
			procCh[n] <- procMsg{op: blockOp{blockMutate, delta}, v: msg.Version}

		case labels.DeleteBlock:
			n := delta.Index.Hash(numBlockHandlers)
			procCh[n] <- procMsg{op: blockOp{blockDelete, delta}, v: msg.Version}

		case labels.DeltaMergeStart:
			// Add this merge into the cached blockRLEs
			iv := dvid.InstanceVersion{d.DataUUID(), msg.Version}
			d.StartUpdate()
			labels.MergeStart(iv, delta.MergeOp)
			d.StopUpdate()

		case labels.DeltaMerge:
			wg := new(sync.WaitGroup)
			for izyxStr := range delta.Blocks {
				n := getHandlerNum(izyxStr)
				wg.Add(1)
				op := mergeOp{MergeOp: delta.MergeOp, block: izyxStr}
				procCh[n] <- procMsg{op: op, v: msg.Version, wg: wg}
			}
			// When we've processed all the delta blocks, we can remove this merge op
			// from the merge cache since all labels will have completed.
			iv := dvid.InstanceVersion{d.DataUUID(), msg.Version}
			go func() {
				wg.Wait()
				labels.MergeStop(iv, delta.MergeOp)
			}()

		case labels.DeltaSplit:
			timedLog := dvid.NewTimeLog()
			splitOpStart := labels.DeltaSplitStart{delta.OldLabel, delta.NewLabel}
			splitOpEnd := labels.DeltaSplitEnd{delta.OldLabel, delta.NewLabel}
			iv := dvid.InstanceVersion{d.DataUUID(), msg.Version}
			labels.SplitStart(iv, splitOpStart)

			wg := new(sync.WaitGroup)
			d.StartUpdate()
			if delta.Split == nil {
				// Coarse Split
				for _, izyxStr := range delta.SortedBlocks {
					n := getHandlerNum(izyxStr)
					wg.Add(1)
					op := splitOp{
						oldLabel: delta.OldLabel,
						newLabel: delta.NewLabel,
						block:    izyxStr,
					}
					procCh[n] <- procMsg{op: op, v: msg.Version, wg: wg}
				}
			} else {
				// Fine Split
				for izyxStr, blockRLEs := range delta.Split {
					n := getHandlerNum(izyxStr)
					wg.Add(1)
					op := splitOp{
						oldLabel: delta.OldLabel,
						newLabel: delta.NewLabel,
						rles:     blockRLEs,
						block:    izyxStr,
					}
					procCh[n] <- procMsg{op: op, v: msg.Version, wg: wg}
				}
			}
			// Wait for all blocks to be split then mark end of split op.
			go func() {
				wg.Wait()
				labels.SplitStop(iv, splitOpEnd)
				timedLog.Debugf("labelblk sync complete for split of %d -> %d", delta.OldLabel, delta.NewLabel)
				d.StopUpdate()
			}()

		default:
		}
	}
}

func getHandlerNum(s dvid.IZYXString) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(s))
	if err != nil {
		dvid.Criticalf("Could not write to fnv hash in labelblk.hashStr()")
		return 0
	}
	return int(hash.Sum32()) % numBlockHandlers
}

// Handles a stream of block operations for a unique shard of block coordinates.
// Since the same block coordinate always gets mapped to the same goroutine we can
// do a GET/PUT without worrying about interleaving PUT from other goroutines, as
// long as there is only one DVID server.
func (d *Data) processBlock(ch <-chan procMsg) {
	for msg := range ch {
		ctx := datastore.NewVersionedCtx(d, msg.v)
		switch op := msg.op.(type) {
		case blockOp:
			// Handle down-res operation for upstream labelblk.

		case mergeOp:
			d.mergeBlock(ctx, op)
			msg.wg.Done()

		case splitOp:
			d.splitBlock(ctx, op)
			msg.wg.Done()

		default:
			dvid.Criticalf("Received unknown processing msg in processBlock: %v\n", msg)
		}
	}
}

// handles relabeling of blocks during a merge operation.
func (d *Data) mergeBlock(ctx *datastore.VersionedCtx, op mergeOp) {
	store, err := d.GetKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}

	tk := NewTKeyByCoord(op.block)
	data, err := store.Get(ctx, tk)
	if err != nil {
		dvid.Errorf("Error on GET of labelblk with coord string %q\n", op.block)
		return
	}
	if data == nil {
		dvid.Errorf("nil label block where merge was done!\n")
		return
	}

	blockData, _, err := dvid.DeserializeData(data, true)
	if err != nil {
		dvid.Criticalf("unable to deserialize label block in '%s': %v\n", d.DataName(), err)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)
	if len(blockData) != blockBytes {
		dvid.Criticalf("After labelblk deserialization got back %d bytes, expected %d bytes\n", len(blockData), blockBytes)
		return
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
		return
	}
	if err := store.Put(ctx, tk, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %v\n", tk, err)
	}
}

// Goroutine that handles splits across a lot of blocks for one label.
func (d *Data) splitBlock(ctx *datastore.VersionedCtx, op splitOp) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}

	// Read the block.
	tk := NewTKeyByCoord(op.block)
	data, err := store.Get(ctx, tk)
	if err != nil {
		dvid.Errorf("Error on GET of labelblk with coord string %v\n", []byte(op.block))
		return
	}
	if data == nil {
		dvid.Errorf("nil label block where split was done, coord %v\n", []byte(op.block))
		return
	}
	bdata, _, err := dvid.DeserializeData(data, true)
	if err != nil {
		dvid.Criticalf("unable to deserialize label block in '%s' key %v: %v\n", d.DataName(), []byte(op.block), err)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)
	if len(bdata) != blockBytes {
		dvid.Criticalf("splitBlock: coord %v got back %d bytes, expected %d bytes\n", []byte(op.block), len(bdata), blockBytes)
		return
	}

	// Modify the block using either voxel-level changes or coarser block-level mods.
	if op.rles != nil {
		if err := d.storeRLEs(bdata, op.newLabel, op.block, op.rles); err != nil {
			dvid.Errorf("can't store label %d RLEs into block %s: %v\n", op.newLabel, op.block.Print(), err)
			return
		}
	} else {
		// We are doing coarse split and will replace all
		if err := d.replaceLabel(bdata, op.oldLabel, op.newLabel); err != nil {
			dvid.Errorf("can't replace label %d with %d in block %s: %v\n", op.oldLabel, op.newLabel, op.block.Print(), err)
			return
		}
	}

	// Write the modified block.
	serialization, err := dvid.SerializeData(bdata, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Criticalf("Unable to serialize block %s in %q: %v\n", op.block.Print(), d.DataName(), err)
		return
	}
	if err := store.Put(ctx, tk, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %v\n", tk, err)
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
