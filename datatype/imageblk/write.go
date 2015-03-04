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
func (v *Voxels) WriteBlock(block *storage.KeyValue, blockSize dvid.Point) error {
	return v.writeBlock(block, blockSize)
}

func (v *Voxels) writeBlock(block *storage.KeyValue, blockSize dvid.Point) error {
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
	repo     datastore.Repo
	voxels   *Voxels
	indexZYX dvid.IndexZYX
	version  dvid.VersionID
}

// PutVoxels persists voxels from a subvolume into the storage engine.
// The subvolume must be aligned to blocks of the data instance.
//
// This requirement simplifies the coding quite a bit.  Earlier versions
// of DVID allowed 2d writes, which required reading blocks, writing subsets of data
// into those blocks, and then writing the result all within a transaction. It is
// difficult to scale without requiring GETs within transactions and more complicated
// coordination when moving to distributed front-end DVIDs.
func (d *Data) PutVoxels(v dvid.VersionID, vox *Voxels, roi *ROI) error {

	// Make sure vox is block-aligned
	if !dvid.BlockAligned(vox, d.BlockSize()) {
		return fmt.Errorf("cannot store voxels in non-block aligned geometry %s -> %s", vox.StartPoint(), vox.EndPoint())
	}

	wg := new(sync.WaitGroup)
	ctx := datastore.NewVersionedContext(d, v)

	repo, err := datastore.RepoFromVersionID(v)
	if err != nil {
		return fmt.Errorf("Unable to get repo associated with version id %d\n", v)
	}

	// Only do one request at a time, although each request can start many goroutines.
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

	// Keep track of changing extents and mark repo as dirty if changed.
	var extentChanged bool
	defer func() {
		if extentChanged {
			err := datastore.SaveRepoByVersionID(v)
			if err != nil {
				dvid.Infof("Error in trying to save repo on change: %s\n", err.Error())
			}
		}
	}()

	// Track point extents
	extents := d.Extents()
	if extents.AdjustPoints(vox.StartPoint(), vox.EndPoint()) {
		extentChanged = true
	}

	// Iterate through index space for this data.
	for it, err := vox.IndexIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		i0, i1, err := it.IndexSpan()
		if err != nil {
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
			if roi != nil && roi.Iter != nil && !roi.Iter.InsideFast(curIndex) {
				wg.Done()
				continue
			}

			curIndexBytes := NewIndex(&curIndex)
			kv := &storage.KeyValue{K: ctx.ConstructKey(curIndexBytes)}
			putOp := &putOperation{repo, vox, curIndex, v}
			op := &storage.ChunkOp{putOp, wg}
			d.PutChunk(&storage.Chunk{op, kv})
		}
	}

	wg.Wait()
	return nil
}

// PutBlocks stores blocks of data in a span along X
func (d *Data) PutBlocks(v dvid.VersionID, start dvid.ChunkPoint3d, span int, data io.ReadCloser) error {
	ctx := datastore.NewVersionedContext(d, v)
	batch := batcher.NewBatch(ctx)

	repo, err := datastore.RepoFromVersionID(v)
	if err != nil {
		return err
	}

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
				return fmt.Errorf("Error reading blocks: %s\n", err.Error())
			}
		}

		if readBytes != numBlockBytes {
			return fmt.Errorf("Expected %d bytes in block read, got %d instead!  Aborting.", numBlockBytes, readBytes)
		}

		zyx := dvid.IndexZYX(chunkPt)
		blockIndex := NewIndex(&zyx)
		serialization, err := dvid.SerializeData(buf, d.Compression(), d.Checksum())
		if err != nil {
			return err
		}
		batch.Put(blockIndex, serialization)

		// Notify any subscribers that you've changed block.
		evt := datastore.SyncEvent{d.DataName(), ChangeBlockEvent}
		msg := datastore.SyncMessage{v, Block{&zyx, buf}}
		repo.NotifySubscribers(evt, msg)

		// Advance to next block
		chunkPt[0]++
		readBlocks++
		finish := (readBlocks == span)
		if finish || readBlocks%BatchSize == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("Error on batch commit, block %d: %s\n", readBlocks, err.Error())
			}
			batch = batcher.NewBatch(ctx)
		}
		if finish {
			break
		}
	}
	return nil
}

// PutChunk puts a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) PutChunk(chunk *storage.Chunk) error {
	<-server.HandlerToken
	go d.putChunk(chunk)
	return nil
}

func (d *Data) putChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op, ok := chunk.Op.(*putOperation)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}

	// Make sure our received chunk is valid.
	if chunk == nil {
		dvid.Errorf("Received nil chunk in ProcessChunk.  Ignoring chunk.\n")
		return
	}
	if chunk.K == nil {
		dvid.Errorf("Received nil chunk key in ProcessChunk.  Ignoring chunk.\n")
		return
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var blockData []byte
	var err error
	if chunk.V == nil {
		blockData = d.BackgroundBlock()
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
			return
		}
	}

	// Perform the operation.
	block := &storage.KeyValue{K: chunk.K, V: blockData}
	if err = op.voxels.WriteBlock(block, d.BlockSize()); err != nil {
		dvid.Errorf("Unable to WriteBlock() in %q: %s\n", d.DataName(), err.Error())
		return
	}
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
		return
	}

	if err := store.Put(nil, chunk.K, serialization); err != nil {
		dvid.Errorf("Unable to PUT voxel data for key %v: %s\n", chunk.K, err.Error())
		return
	}

	// Notify any subscribers that you've changed block.
	evt := datastore.SyncEvent{d.DataName(), ChangeBlockEvent}
	msg := datastore.SyncMessage{op.version, Block{&op.indexZYX, block.V}}
	op.repo.NotifySubscribers(evt, msg)
}

// Writes a XY image into the blocks that intersect it.  This function assumes the
// blocks have been allocated and if necessary, filled with old data.
func (d *Data) writeXYImage(v dvid.VersionID, vox *Voxels, b storage.KeyValues) (extentChanged bool, err error) {

	// Setup concurrency in image -> block transfers.
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	// Iterate through index space for this data using ZYX ordering.
	ctx := datastore.NewVersionedContext(d, v)
	blockSize := d.BlockSize()
	var startingBlock int32

	for it, err := vox.IndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
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
				curIndexBytes := NewIndex(&curIndex)
				b[blockNum].K = ctx.ConstructKey(curIndexBytes)

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

// writeBlocks persists blocks of voxel data asynchronously using batch writes.
func (d *Data) writeBlocks(v dvid.VersionID, b storage.KeyValues, wg1, wg2 *sync.WaitGroup) error {
	preCompress, postCompress := 0, 0
	ctx := datastore.NewVersionedContext(d, v)

	repo, err := datastore.RepoFromVersionID(v)
	if err != nil {
		return err
	}
	evt := datastore.SyncEvent{d.DataName(), ChangeBlockEvent}

	<-server.HandlerToken
	go func() {
		defer func() {
			wg1.Done()
			wg2.Done()
			dvid.Debugf("Wrote voxel blocks.  Before %s: %d bytes.  After: %d bytes\n", d.Compression(), preCompress, postCompress)
			server.HandlerToken <- 1
		}()

		batch := batcher.NewBatch(ctx)
		for i, block := range b {
			serialization, err := dvid.SerializeData(block.V, d.Compression(), d.Checksum())
			preCompress += len(block.V)
			postCompress += len(serialization)
			if err != nil {
				dvid.Errorf("Unable to serialize block: %s\n", err.Error())
				return
			}
			indexZYX, err := DecodeKey(block.K)
			if err != nil {
				dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
				return
			}
			batch.Put(indexZYX.Bytes(), serialization)

			msg := datastore.SyncMessage{v, Block{indexZYX, block.V}}
			repo.NotifySubscribers(evt, msg)

			// Check if we should commit
			if i%KVWriteSize == KVWriteSize-1 {
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on trying to write batch: %s\n", err.Error())
					return
				}
				batch = batcher.NewBatch(ctx)
			}
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Error on trying to write batch: %s\n", err.Error())
			return
		}
	}()
	return nil
}
