package imageblk

import (
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// WriteBlock writes a subvolume or 2d image into a possibly intersecting block.
func (v *Voxels) WriteBlock(block *storage.TKeyValue, blockSize dvid.Point) error {
	return v.writeBlock(block, blockSize)
}
func (v *Voxels) writeBlock(block *storage.TKeyValue, blockSize dvid.Point) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := v.ComputeTransform(block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()
	bytesPerVoxel := int64(v.Values().BytesPerElement())

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * bytesPerVoxel
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
			blockI := bz*bY + blockBegY*bX + blockBegX*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				copy(block.V[blockI:blockI+bytesPerVoxel], data[dataI:dataI+bytesPerVoxel])
				blockI += bX
				dataI += bytesPerVoxel
			}
			bz++
		}

	case v.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		return fmt.Errorf("DVID currently does not support 2d in n-d space.")

	case v.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBegX * bytesPerVoxel
		dX := int64(v.Size().Value(0)) * bytesPerVoxel
		dY := int64(v.Size().Value(1)) * dX
		dataOffset := int64(dataBeg.Value(0)) * bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		blockZ := blockBegZ

		for dataZ := int64(dataBeg.Value(2)); dataZ <= int64(dataEnd.Value(2)); dataZ++ {
			blockY := blockBegY
			for dataY := int64(dataBeg.Value(1)); dataY <= int64(dataEnd.Value(1)); dataY++ {
				dataI := dataZ*dY + dataY*dX + dataOffset
				blockI := blockZ*bY + blockY*bX + blockOffset
				copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot writeBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

type putOperation struct {
	voxels   *Voxels
	indexZYX dvid.IndexZYX
	version  dvid.VersionID
	mutate   bool   // if false, we just ingest without needing to GET previous value
	mutID    uint64 // should be unique within a server's uptime.
}

type patchGeo struct {
	patchstart dvid.Point3d // offset in block where patch data begins
	patchend   dvid.Point3d // location in block where patch data ends
}

// IngestVoxels ingests voxels from a subvolume into the storage engine.
// The subvolume must be aligned to blocks of the data instance, which simplifies
// the routine since we are simply replacing a value instead of modifying values (GET + PUT).
func (d *Data) IngestVoxels(v dvid.VersionID, mutID uint64, vox *Voxels, roiname dvid.InstanceName) error {
	return d.PutVoxels(v, mutID, vox, roiname, false)
}

// MutateVoxels mutates voxels from a subvolume into the storage engine.  This differs from
// the IngestVoxels function in firing off a MutateBlockEvent instead of an IngestBlockEvent,
// which tells subscribers that a previous value has changed instead of a completely new
// key/value being inserted.  There will be some decreased performance due to cleanup of prior
// denormalizations compared to IngestVoxels.
func (d *Data) MutateVoxels(v dvid.VersionID, mutID uint64, vox *Voxels, roiname dvid.InstanceName) error {
	return d.PutVoxels(v, mutID, vox, roiname, true)
}

// PutVoxels persists voxels from a subvolume into the storage engine.
// The subvolume must be aligned to blocks of the data instance, which simplifies
// the routine if the PUT is a mutation (signals MutateBlockEvent) instead of ingestion.
func (d *Data) PutVoxels(v dvid.VersionID, mutID uint64, vox *Voxels, roiname dvid.InstanceName, mutate bool) error {
	r, err := GetROI(v, roiname, vox)
	if err != nil {
		return err
	}

	// extract buffer interface if it exists
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	// extract buffer interface
	_, hasbuffer := store.(storage.KeyValueRequester)

	// Make sure vox is block-aligned
	if !dvid.BlockAligned(vox, d.BlockSize()) {
		return fmt.Errorf("cannot store voxels in non-block aligned geometry %s -> %s", vox.StartPoint(), vox.EndPoint())
	}

	// Only do one request at a time, although each request can start many goroutines.
	if !hasbuffer {
		if vox.NumVoxels() > 256*256*256 {
			server.LargeMutationMutex.Lock()
			defer server.LargeMutationMutex.Unlock()
		}
	}

	// create some buffer for handling requests
	finishedRequests := make(chan error, 1000)
	putrequests := 0

	// Post new extents if there was a change (will always require 1 GET which should
	// not be a big deal for large posts or for distributed back-ends)
	// (assumes rest of the command will finish correctly which seems reasonable)
	ctx := datastore.NewVersionedCtx(d, v)
	putrequests++
	go func() {
		err := d.PostExtents(ctx, vox.StartPoint(), vox.EndPoint())
		finishedRequests <- err
	}()

	// Iterate through index space for this data.
	for it, err := vox.NewIndexIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		i0, i1, err := it.IndexSpan()
		if err != nil {
			return err
		}
		ptBeg := i0.Duplicate().(dvid.ChunkIndexer)
		ptEnd := i1.Duplicate().(dvid.ChunkIndexer)

		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)

		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndex := dvid.IndexZYX(c)

			// Don't PUT if this index is outside a specified ROI
			if r != nil && r.Iter != nil && !r.Iter.InsideFast(curIndex) {
				continue
			}

			kv := &storage.TKeyValue{K: NewTKey(&curIndex)}
			putOp := &putOperation{vox, curIndex, v, mutate, mutID}
			op := &storage.ChunkOp{putOp, nil}
			putrequests++
			d.PutChunk(&storage.Chunk{op, kv}, hasbuffer, finishedRequests)
		}
	}
	// wait for everything to finish
	for i := 0; i < putrequests; i++ {
		errjob := <-finishedRequests
		if errjob != nil {
			err = errjob
		}
	}
	return err
}

// PutBlocks stores blocks of data in a span along X
func (d *Data) PutBlocks(v dvid.VersionID, mutID uint64, start dvid.ChunkPoint3d, span int, data io.ReadCloser, mutate bool) error {
	batcher, err := datastore.GetKeyValueBatcher(d)
	if err != nil {
		return err
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
				return fmt.Errorf("block data ceased before all block data read")
			}
			if err != nil {
				return fmt.Errorf("error reading blocks: %v", err)
			}
		}

		if readBytes != numBlockBytes {
			return fmt.Errorf("expected %d bytes in block read, got %d instead, aborting", numBlockBytes, readBytes)
		}

		serialization, err := dvid.SerializeData(buf, d.Compression(), d.Checksum())
		if err != nil {
			return err
		}
		zyx := dvid.IndexZYX(chunkPt)
		tk := NewTKey(&zyx)

		// If we are mutating, get the previous block of data.
		var oldBlock []byte
		if mutate {
			oldBlock, err = d.GetBlock(v, tk)
			if err != nil {
				return fmt.Errorf("unable to load previous block in %q, key %v: %v", d.DataName(), tk, err)
			}
		}

		// Write the new block
		batch.Put(tk, serialization)

		// Notify any subscribers that you've changed block.
		var event string
		var delta interface{}
		if mutate {
			event = MutateBlockEvent
			delta = MutatedBlock{&zyx, oldBlock, buf, mutID}
		} else {
			event = IngestBlockEvent
			delta = Block{&zyx, buf, mutID}
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
				return fmt.Errorf("error on batch commit, block %d: %v", readBlocks, err)
			}
			if finish {
				break
			} else {
				batch = batcher.NewBatch(ctx)
			}
		}
	}
	return nil
}

// PutChunk puts a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) PutChunk(chunk *storage.Chunk, hasbuffer bool, finishedRequests chan error) error {
	if !hasbuffer {
		// if storage engine handles buffering, limited advantage to throttling here
		server.CheckChunkThrottling()
	}

	go d.putChunk(chunk, hasbuffer, finishedRequests)
	return nil
}

func (d *Data) putChunk(chunk *storage.Chunk, hasbuffer bool, finishedRequests chan error) {
	var err error
	defer func() {
		// After processing a chunk, return the token.
		if !hasbuffer {
			server.HandlerToken <- 1
		}
		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}

		finishedRequests <- err
	}()

	op, ok := chunk.Op.(*putOperation)

	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}

	// Make sure our received chunk is valid.
	if chunk == nil {
		dvid.Errorf("Received nil chunk in ProcessChunk.  Ignoring chunk.\n")
		err = fmt.Errorf("Received nil chunk in ProcessChunk.  Ignoring chunk.\n")
		return
	}
	if chunk.K == nil {
		dvid.Errorf("Received nil chunk key in ProcessChunk.  Ignoring chunk.\n")
		err = fmt.Errorf("Received nil chunk key in ProcessChunk.  Ignoring chunk.\n")
		return
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var blockData []byte
	if chunk.V == nil {
		blockData = d.BackgroundBlock()
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in %q: %v\n", d.DataName(), err)
			return
		}
	}

	// If we are mutating, get the previous block of data.
	var oldBlock []byte
	if op.mutate {
		oldBlock, err = d.GetBlock(op.version, chunk.K)
		if err != nil {
			dvid.Errorf("Unable to load previous block in %q, key %v: %v\n", d.DataName(), chunk.K, err)
			return
		}
	}

	// Perform the operation.
	block := &storage.TKeyValue{K: chunk.K, V: blockData}
	if err = op.voxels.WriteBlock(block, d.BlockSize()); err != nil {
		dvid.Errorf("Unable to WriteBlock() in %q: %v\n", d.DataName(), err)
		return
	}
	var serialization []byte
	serialization, err = dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %v\n", d.DataName(), err)
		return
	}
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("Data type imageblk had error initializing store: %v\n", err)
		return
	}

	ready := make(chan error, 1)
	callback := func() {
		// Notify any subscribers that you've changed block.
		resperr := <-ready
		if resperr != nil {
			dvid.Errorf("Unable to PUT voxel data for key %v: %v\n", chunk.K, resperr)
			err = fmt.Errorf("Unable to PUT voxel data for key %v: %v", chunk.K, resperr)
			return
		}
		var event string
		var delta interface{}
		if op.mutate {
			event = MutateBlockEvent
			delta = MutatedBlock{&op.indexZYX, oldBlock, block.V, op.mutID}
		} else {
			event = IngestBlockEvent
			delta = Block{&op.indexZYX, block.V, op.mutID}
		}
		evt := datastore.SyncEvent{d.DataUUID(), event}
		msg := datastore.SyncMessage{event, op.version, delta}
		if err = datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("Unable to notify subscribers of event %s in %s\n", event, d.DataName())
		}
	}

	// put data -- use buffer if available
	ctx := datastore.NewVersionedCtx(d, op.version)
	if err = store.Put(ctx, chunk.K, serialization); err != nil {
		dvid.Errorf("Unable to PUT voxel data for key %v: %v\n", chunk.K, err)
		return
	}
	ready <- nil
	callback()
}

// Writes a XY image into the blocks that intersect it.  This function assumes the
// blocks have been allocated and if necessary, filled with old data.
func (d *Data) writeXYImage(v dvid.VersionID, vox *Voxels, b storage.TKeyValues) (err error) {

	// Setup concurrency in image -> block transfers.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Iterate through index space for this data using ZYX ordering.
	blockSize := d.BlockSize()
	var startingBlock int32

	for it, err := vox.NewIndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}

		ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
		ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)

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
				b[blockNum].K = NewTKey(&curIndex)

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

	ctx := datastore.NewVersionedCtx(d, v)
	evt := datastore.SyncEvent{d.DataUUID(), IngestBlockEvent}

	server.CheckChunkThrottling()
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
			serialization, err := dvid.SerializeData(block.V, d.Compression(), d.Checksum())
			preCompress += len(block.V)
			postCompress += len(serialization)
			if err != nil {
				dvid.Errorf("Unable to serialize block: %v\n", err)
				return
			}
			batch.Put(block.K, serialization)

			indexZYX, err := DecodeTKey(block.K)
			if err != nil {
				dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
				return
			}
			msg := datastore.SyncMessage{IngestBlockEvent, v, Block{indexZYX, block.V, mutID}}
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
