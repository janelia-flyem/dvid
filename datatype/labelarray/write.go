package labelarray

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type putOperation struct {
	data       []byte // the full label volume sent to PUT
	scale      uint8
	subvol     *dvid.Subvolume
	indexZYX   dvid.IndexZYX
	version    dvid.VersionID
	mutate     bool   // if false, we just ingest without needing to GET previous value
	mutID      uint64 // should be unique within a server's uptime.
	downresMut *downres.Mutation
	blockCh    chan blockChange
}

// PutLabels persists voxels from a subvolume into the storage engine.  This involves transforming
// a supplied volume of uint64 with known geometry into many labels.Block that tiles the subvolume.
// Messages are sent to subscribers for ingest or mutate events.
func (d *Data) PutLabels(v dvid.VersionID, subvol *dvid.Subvolume, data []byte, roiname dvid.InstanceName, mutate bool) error {
	if subvol.DataShape().ShapeDimensions() != 3 {
		return fmt.Errorf("cannot store labels for data %q in non 3D format", d.DataName())
	}

	// Make sure data is block-aligned
	if !dvid.BlockAligned(subvol, d.BlockSize()) {
		return fmt.Errorf("cannot store labels for data %q in non-block aligned geometry %s -> %s", d.DataName(), subvol.StartPoint(), subvol.EndPoint())
	}

	// Make sure the received data buffer is of appropriate size.
	labelBytes := subvol.Size().Prod() * 8
	if labelBytes != int64(len(data)) {
		return fmt.Errorf("expected %d bytes for data %q label PUT but only received %d bytes", labelBytes, d.DataName(), len(data))
	}

	r, err := imageblk.GetROI(v, roiname, subvol)
	if err != nil {
		return err
	}

	// Only do one request at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	// Keep track of changing extents, labels and mark repo as dirty if changed.
	var extentChanged bool
	defer func() {
		if extentChanged {
			err := datastore.SaveDataByVersion(v, d)
			if err != nil {
				dvid.Infof("Error in trying to save repo on change: %v\n", err)
			}
		}
	}()

	// Track point extents
	ctx := datastore.NewVersionedCtx(d, v)
	extents := d.Extents()
	if extents.AdjustPoints(subvol.StartPoint(), subvol.EndPoint()) {
		extentChanged = true
		if err := d.PostExtents(ctx, extents.MinPoint, extents.MaxPoint); err != nil {
			return err
		}
	}

	// extract buffer interface if it exists
	var putbuffer storage.RequestBuffer
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}
	if req, ok := store.(storage.KeyValueRequester); ok {
		putbuffer = req.NewBuffer(ctx)
	}

	// Iterate through index space for this data.
	mutID := d.NewMutationID()
	downresMut := downres.NewMutation(d, v, mutID)

	wg := new(sync.WaitGroup)

	blockCh := make(chan blockChange, 100)
	go d.aggregateBlockChanges(v, blockCh)

	blocks := 0
	for it, err := subvol.NewIndexZYXIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		i0, i1, err := it.IndexSpan()
		if err != nil {
			close(blockCh)
			return err
		}
		ptBeg := i0.Duplicate().(dvid.ChunkIndexer)
		ptEnd := i1.Duplicate().(dvid.ChunkIndexer)

		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)

		if extents.AdjustIndices(ptBeg, ptEnd) {
			extentChanged = true
		}

		wg.Add(int(endX-begX) + 1)
		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndex := dvid.IndexZYX(c)

			// Don't PUT if this index is outside a specified ROI
			if r != nil && r.Iter != nil && !r.Iter.InsideFast(curIndex) {
				wg.Done()
				continue
			}

			putOp := &putOperation{
				data:       data,
				subvol:     subvol,
				indexZYX:   curIndex,
				version:    v,
				mutate:     mutate,
				mutID:      mutID,
				downresMut: downresMut,
				blockCh:    blockCh,
			}
			server.CheckChunkThrottling()
			go d.putChunk(putOp, wg, putbuffer)
			blocks++
		}
	}
	wg.Wait()
	close(blockCh)

	// if a bufferable op, flush
	if putbuffer != nil {
		putbuffer.Flush()
	}

	return downresMut.Execute()
}

// Puts a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) putChunk(op *putOperation, wg *sync.WaitGroup, putbuffer storage.RequestBuffer) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		wg.Done()
	}()

	bcoord := op.indexZYX.ToIZYXString()
	ctx := datastore.NewVersionedCtx(d, op.version)

	// If we are mutating, get the previous label Block
	var scale uint8
	var oldBlock *labels.PositionedBlock
	if op.mutate {
		var err error
		if oldBlock, err = d.getLabelBlock(ctx, scale, bcoord); err != nil {
			dvid.Errorf("Unable to load previous block in %q, key %v: %v\n", d.DataName(), bcoord, err)
			return
		}
	}

	// Get the current label Block from the received label array
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		dvid.Errorf("can't putChunk() on data %q with non-3d block size: %s", d.DataName(), d.BlockSize())
		return
	}
	curBlock, err := labels.SubvolumeToBlock(op.subvol, op.data, op.indexZYX, blockSize)
	if err != nil {
		dvid.Errorf("error creating compressed block from label array at %s", op.subvol)
		return
	}
	go d.updateBlockMaxLabel(op.version, curBlock)

	blockData, _ := curBlock.MarshalBinary()
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %v\n", d.DataName(), err)
		return
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("Data type imageblk had error initializing store: %v\n", err)
		return
	}

	callback := func(ready chan error) {
		if ready != nil {
			if resperr := <-ready; resperr != nil {
				dvid.Errorf("Unable to PUT voxel data for block %v: %v\n", bcoord, resperr)
				return
			}
		}
		var event string
		var delta interface{}
		if oldBlock != nil && op.mutate {
			event = labels.MutateBlockEvent
			block := MutatedBlock{op.mutID, bcoord, &(oldBlock.Block), curBlock}
			d.handleBlockMutate(op.version, op.blockCh, block)
			delta = block
		} else {
			event = labels.IngestBlockEvent
			block := IngestedBlock{op.mutID, bcoord, curBlock}
			d.handleBlockIndexing(op.version, op.blockCh, block)
			delta = block
		}
		if err := op.downresMut.BlockMutated(bcoord, curBlock); err != nil {
			dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
		}
		evt := datastore.SyncEvent{d.DataUUID(), event}
		msg := datastore.SyncMessage{event, op.version, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("Unable to notify subscribers of event %s in %s\n", event, d.DataName())
		}
	}

	// put data -- use buffer if available
	tk := NewBlockTKeyByCoord(op.scale, bcoord)
	if putbuffer != nil {
		ready := make(chan error, 1)
		go callback(ready)
		putbuffer.PutCallback(ctx, tk, serialization, ready)
	} else {
		if err := store.Put(ctx, tk, serialization); err != nil {
			dvid.Errorf("Unable to PUT voxel data for block %s: %v\n", bcoord, err)
			return
		}
		callback(nil)
	}
}

// Writes a XY image into the blocks that intersect it.  This function assumes the
// blocks have been allocated and if necessary, filled with old data.
func (d *Data) writeXYImage(v dvid.VersionID, vox *imageblk.Voxels, b storage.TKeyValues) (extentChanged bool, err error) {

	// Setup concurrency in image -> block transfers.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Iterate through index space for this data using ZYX ordering.
	blockSize := d.BlockSize()
	var startingBlock int32

	for it, err := vox.NewIndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return extentChanged, err
		}

		ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
		ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)

		// Track point extents
		if d.Extents().AdjustIndices(ptBeg, ptEnd) {
			extentChanged = true
		}

		// Do image -> block transfers in concurrent goroutines.
		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)

		server.CheckChunkThrottling()
		wg.Add(1)
		go func(blockNum int32) {
			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				curIndex := dvid.IndexZYX(c)
				b[blockNum].K = NewBlockTKey(0, &curIndex)

				// Write this slice data into the block.
				vox.WriteBlock(&(b[blockNum]), blockSize)
				blockNum++
			}
			server.HandlerToken <- 1
			wg.Done()
		}(startingBlock)

		startingBlock += (endX - begX + 1)
	}
	return
}

// KVWriteSize is the # of key-value pairs we will write as one atomic batch write.
const KVWriteSize = 500

// TODO -- Clean up all the writing and simplify now that we have block-aligned writes.
// writeBlocks ingests blocks of voxel data asynchronously using batch writes.
func (d *Data) writeBlocks(v dvid.VersionID, b storage.TKeyValues, wg1, wg2 *sync.WaitGroup) error {
	batcher, err := datastore.GetKeyValueBatcher(d)
	if err != nil {
		return err
	}

	preCompress, postCompress := 0, 0
	blockSize := d.BlockSize().(dvid.Point3d)

	ctx := datastore.NewVersionedCtx(d, v)
	evt := datastore.SyncEvent{d.DataUUID(), labels.IngestBlockEvent}

	server.CheckChunkThrottling()
	blockCh := make(chan blockChange, 100)
	go d.aggregateBlockChanges(v, blockCh)
	go func() {
		defer func() {
			wg1.Done()
			wg2.Done()
			dvid.Debugf("Wrote voxel blocks.  Before %s: %d bytes.  After: %d bytes\n", d.Compression(), preCompress, postCompress)
			close(blockCh)
			server.HandlerToken <- 1
		}()

		mutID := d.NewMutationID()
		batch := batcher.NewBatch(ctx)
		for i, block := range b {
			preCompress += len(block.V)
			lblBlock, err := labels.MakeBlock(block.V, blockSize)
			if err != nil {
				dvid.Errorf("unable to compute dvid block compression in %q: %v\n", d.DataName(), err)
				return
			}
			go d.updateBlockMaxLabel(v, lblBlock)

			compressed, _ := lblBlock.MarshalBinary()
			serialization, err := dvid.SerializeData(compressed, d.Compression(), d.Checksum())
			if err != nil {
				dvid.Errorf("Unable to serialize block in %q: %v\n", d.DataName(), err)
				return
			}
			postCompress += len(serialization)
			batch.Put(block.K, serialization)

			_, indexZYX, err := DecodeBlockTKey(block.K)
			if err != nil {
				dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
				return
			}

			block := IngestedBlock{mutID, indexZYX.ToIZYXString(), lblBlock}
			d.handleBlockIndexing(v, blockCh, block)

			msg := datastore.SyncMessage{labels.IngestBlockEvent, v, block}
			if err := datastore.NotifySubscribers(evt, msg); err != nil {
				dvid.Errorf("Unable to notify subscribers of ChangeBlockEvent in %s\n", d.DataName())
				return
			}

			// Check if we should commit
			if i%KVWriteSize == KVWriteSize-1 {
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on trying to write batch: %v\n", err)
					return
				}
				batch = batcher.NewBatch(ctx)
			}
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Error on trying to write batch: %v\n", err)
			return
		}
	}()
	return nil
}
