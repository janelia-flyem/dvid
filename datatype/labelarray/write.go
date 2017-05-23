package labelarray

import (
	"fmt"
	"io"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type putOperation struct {
	data     []byte // the full label volume sent to PUT
	subvol   *dvid.Subvolume
	indexZYX dvid.IndexZYX
	version  dvid.VersionID
	mutate   bool   // if false, we just ingest without needing to GET previous value
	mutID    uint64 // should be unique within a server's uptime.
	blockCh  chan blockChange
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

	// Keep track of changing extents and mark repo as dirty if changed.
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
	extents := d.Extents()
	if extents.AdjustPoints(subvol.StartPoint(), subvol.EndPoint()) {
		extentChanged = true
	}

	// extract buffer interface if it exists
	var putbuffer storage.RequestBuffer
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}
	if req, ok := store.(storage.KeyValueRequester); ok {
		ctx := datastore.NewVersionedCtx(d, v)
		putbuffer = req.NewBuffer(ctx)
	}

	// Iterate through index space for this data.
	mutID := d.NewMutationID()
	fmt.Printf("Starting PutLabels, mutation %d\n", mutID)

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
				data:     data,
				subvol:   subvol,
				indexZYX: curIndex,
				version:  v,
				mutate:   mutate,
				mutID:    mutID,
				blockCh:  blockCh,
			}
			<-server.HandlerToken
			go d.putChunk(putOp, wg, putbuffer)
			blocks++
		}
	}
	wg.Wait()
	fmt.Printf("Done with PutLabels %d block-level ops, mutation %d\n", blocks, mutID)
	close(blockCh)

	// if a bufferable op, flush
	if putbuffer != nil {
		putbuffer.Flush()
	}

	// Let any synced downres instance that we've completed block-level ops.
	d.publishDownsizeCommit(v, mutID)

	return nil
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
	var oldBlock *labels.PositionedBlock
	if op.mutate {
		var err error
		if oldBlock, err = d.getLabelBlock(ctx, bcoord); err != nil {
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
	blockData, _ := curBlock.MarshalBinary()
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %v\n", d.DataName(), err)
		return
	}

	store, err := d.GetOrderedKeyValueDB()
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
		if op.mutate {
			event = MutateBlockEvent
			block := MutatedBlock{op.mutID, bcoord, &(oldBlock.Block), curBlock}
			d.handleIndexBlockMutate(op.version, op.blockCh, block)
			delta = block
		} else {
			event = IngestBlockEvent
			block := IngestedBlock{op.mutID, bcoord, curBlock}
			d.handleIndexBlockIngest(op.version, op.blockCh, block)
			delta = block
		}
		evt := datastore.SyncEvent{d.DataUUID(), event}
		msg := datastore.SyncMessage{event, op.version, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("Unable to notify subscribers of event %s in %s\n", event, d.DataName())
		}
	}
	// put data -- use buffer if available
	tk := NewBlockTKeyByCoord(bcoord)
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

// PutBlocks stores blocks of data in a span along X
func (d *Data) PutBlocks(v dvid.VersionID, mutID uint64, start dvid.ChunkPoint3d, span int, data io.ReadCloser, mutate bool) error {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		return err
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		return fmt.Errorf("can't PutBlocks() on data %q with non-3d block size: %s", d.DataName(), d.BlockSize())
	}

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Read blocks from the stream until we can output a batch put.
	const BatchSize = 1000
	var readBlocks int
	numBlockBytes := d.BlockSize().Prod()
	chunkPt := start
	buf := make([]byte, numBlockBytes)
	for {
		// Read a block's worth of data
		readBytes := int64(0)
		for {
			n, err := data.Read(buf[readBytes:])
			readBytes += int64(n)
			if readBytes == numBlockBytes {
				break
			}
			if err == io.EOF {
				return fmt.Errorf("Block data ceased before all block data read")
			}
			if err != nil {
				return fmt.Errorf("Error reading blocks: %v\n", err)
			}
		}

		if readBytes != numBlockBytes {
			return fmt.Errorf("Expected %d bytes in block read, got %d instead!  Aborting.", numBlockBytes, readBytes)
		}

		curBlock, err := labels.MakeBlock(buf, blockSize)
		if err != nil {
			return err
		}
		blockData, _ := curBlock.MarshalBinary()
		serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
		if err != nil {
			return err
		}
		bcoord := chunkPt.ToIZYXString()

		// If we are mutating, get the previous block of data.
		var oldBlock *labels.PositionedBlock
		if mutate {
			var err error
			if oldBlock, err = d.getLabelBlock(ctx, bcoord); err != nil {
				return fmt.Errorf("Unable to load previous block in %q, block %s: %v\n", d.DataName(), bcoord, err)
			}
		}

		// Write the new block
		tk := NewBlockTKeyByCoord(bcoord)
		batch.Put(tk, serialization)

		// Notify any subscribers that you've changed block.
		var event string
		var delta interface{}
		if mutate {
			event = MutateBlockEvent
			delta = MutatedBlock{mutID, bcoord, &(oldBlock.Block), curBlock}
		} else {
			event = IngestBlockEvent
			delta = IngestedBlock{mutID, bcoord, curBlock}
		}
		evt := datastore.SyncEvent{d.DataUUID(), event}
		msg := datastore.SyncMessage{event, v, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			return err
		}

		// Advance to next block
		chunkPt[0]++
		readBlocks++
		finish := (readBlocks == span)
		if finish || readBlocks%BatchSize == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("Error on batch commit, block %d: %v\n", readBlocks, err)
			}
			batch = batcher.NewBatch(ctx)
		}
		if finish {
			break
		}
	}
	return nil
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

		<-server.HandlerToken
		wg.Add(1)
		go func(blockNum int32) {
			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				curIndex := dvid.IndexZYX(c)
				b[blockNum].K = NewBlockTKey(&curIndex)

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
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		return err
	}

	preCompress, postCompress := 0, 0
	blockSize := d.BlockSize().(dvid.Point3d)

	ctx := datastore.NewVersionedCtx(d, v)
	evt := datastore.SyncEvent{d.DataUUID(), labels.IngestBlockEvent}

	<-server.HandlerToken
	go func() {
		defer func() {
			wg1.Done()
			wg2.Done()
			dvid.Debugf("Wrote voxel blocks.  Before %s: %d bytes.  After: %d bytes\n", d.Compression(), preCompress, postCompress)
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
			compressed, _ := lblBlock.MarshalBinary()
			serialization, err := dvid.SerializeData(compressed, d.Compression(), d.Checksum())
			if err != nil {
				dvid.Errorf("Unable to serialize block in %q: %v\n", d.DataName(), err)
				return
			}
			postCompress += len(serialization)
			batch.Put(block.K, serialization)

			indexZYX, err := DecodeBlockTKey(block.K)
			if err != nil {
				dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
				return
			}
			msg := datastore.SyncMessage{labels.IngestBlockEvent, v, imageblk.Block{indexZYX, block.V, mutID}}
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
