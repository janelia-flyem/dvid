package imageblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// ROI encapsulates a request-specific ROI check with a given scaling for voxels outside the ROI.
type ROI struct {
	Iter        *roi.Iterator
	attenuation uint8
}

// ComputeTransform determines the block coordinate and beginning + ending voxel points
// for the data corresponding to the given Block.
func (v *Voxels) ComputeTransform(block *storage.KeyValue, blockSize dvid.Point) (blockBeg, dataBeg, dataEnd dvid.Point, err error) {
	ptIndex := v.NewChunkIndex()

	var indexBytes []byte
	ctx := &storage.DataContext{}
	indexBytes, err = ctx.IndexFromKey(block.K)
	if err != nil {
		return
	}
	if indexBytes[0] != byte(keyImageBlock) {
		err = fmt.Errorf("Block key (%v) has non-VoxelBlock index", block.K)
	}
	if err = ptIndex.IndexFromBytes(indexBytes[1:]); err != nil {
		return
	}

	// Get the bounding voxel coordinates for this block.
	minBlockVoxel := ptIndex.MinPoint(blockSize)
	maxBlockVoxel := ptIndex.MaxPoint(blockSize)

	// Compute the boundary voxel coordinates for the ExtData and adjust
	// to our block bounds.
	minDataVoxel := v.StartPoint()
	maxDataVoxel := v.EndPoint()
	begVolCoord, _ := minDataVoxel.Max(minBlockVoxel)
	endVolCoord, _ := maxDataVoxel.Min(maxBlockVoxel)

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	dataBeg = begVolCoord.Sub(v.StartPoint())
	dataEnd = endVolCoord.Sub(v.StartPoint())

	// Compute block coord matching dataBeg
	blockBeg = begVolCoord.Sub(minBlockVoxel)

	return
}

// ReadBlock reads the possibly intersecting block data into the receiver Voxels.
func (v *Voxels) ReadBlock(block *storage.KeyValue, blockSize dvid.Point, attenuation uint8) error {
	if attenuation != 0 {
		return v.readScaledBlock(block, blockSize, attenuation)
	}
	return v.readBlock(block, blockSize)
}

func (v *Voxels) readScaledBlock(block *storage.KeyValue, blockSize dvid.Point, attenuation uint8) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := v.ComputeTransform(block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()
	bytesPerVoxel := int64(v.Values().BytesPerElement())
	if bytesPerVoxel != 1 {
		return fmt.Errorf("Can only scale non-ROI blocks with 1 byte voxels")
	}

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * bytesPerVoxel
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	// Get the block beginning coordinates.
	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
			dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				data[dataI] = (block.V[blockI] >> attenuation)
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
		blockZ := blockBegZ

		for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
			blockY := blockBegY
			for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
				blockI := blockZ*bY + blockY*bX + blockOffset
				dataI := int64(dataZ)*dY + int64(dataY)*dX + dataOffset
				for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
					data[dataI+x] = (block.V[blockI+x] >> attenuation)
				}
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot readScaledBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

func (v *Voxels) readBlock(block *storage.KeyValue, blockSize dvid.Point) error {
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
			copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
			blockI := bz*bY + blockBegY*bX + blockBegX*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				copy(data[dataI:dataI+bytesPerVoxel], block.V[blockI:blockI+bytesPerVoxel])
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
				blockI := blockZ*bY + blockY*bX + blockOffset
				dataI := dataZ*dY + dataY*dX + dataOffset
				copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot readBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

// Handler conversion of little to big endian for voxels larger than 1 byte.
func (v *Voxels) littleToBigEndian(data []uint8) (bigendian []uint8, err error) {
	bytesPerVoxel := v.Values().BytesPerElement()
	if bytesPerVoxel == 1 {
		return data, nil
	}
	bigendian = make([]uint8, len(data))
	switch bytesPerVoxel {
	case 2:
		for beg := 0; beg < len(data)-1; beg += 2 {
			bigendian[beg], bigendian[beg+1] = data[beg+1], data[beg]
		}
	case 4:
		for beg := 0; beg < len(data)-3; beg += 4 {
			value := binary.LittleEndian.Uint32(data[beg : beg+4])
			binary.BigEndian.PutUint32(bigendian[beg:beg+4], value)
		}
	case 8:
		for beg := 0; beg < len(data)-7; beg += 8 {
			value := binary.LittleEndian.Uint64(data[beg : beg+8])
			binary.BigEndian.PutUint64(bigendian[beg:beg+8], value)
		}
	}
	return
}

// BackgroundBlock returns a block buffer that has been preinitialized to the background value.
func (d *Data) BackgroundBlock() []byte {
	numElements := d.BlockSize().Prod()
	bytesPerElement := int64(d.Values.BytesPerElement())
	blockData := make([]byte, numElements*bytesPerElement)
	if d.Background != 0 && bytesPerElement == 1 {
		background := byte(d.Background)
		for i := range blockData {
			blockData[i] = background
		}
	}
	return blockData
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetImage(v dvid.VersionID, vox *Voxels, r *ROI) (*dvid.Image, error) {
	if err := d.GetVoxels(v, vox, r); err != nil {
		return nil, err
	}
	return vox.GetImage2d()
}

// GetVolume retrieves a n-d volume from a version node given a geometry of voxels.
func (d *Data) GetVolume(v dvid.VersionID, vox *Voxels, r *ROI) ([]byte, error) {
	if err := d.GetVoxels(v, vox, r); err != nil {
		return nil, err
	}
	return vox.Data(), nil
}

type getOperation struct {
	voxels      *Voxels
	blocksInROI map[string]bool
	attenuation uint8
}

// GetVoxels copies voxels from the storage engine to Voxels, a requested subvolume or 2d image.
func (d *Data) GetVoxels(v dvid.VersionID, vox *Voxels, r *ROI) error {
	// Only do one request at a time, although each request can start many goroutines.
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

	ctx := datastore.NewVersionedContext(d, v)

	wg := new(sync.WaitGroup)
	var it dvid.IndexIterator
	var err error
	for it, err = vox.IndexIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		blockBeg := NewIndex(indexBeg)
		blockEnd := NewIndex(indexEnd)

		// Get set of blocks in ROI if ROI provided
		var chunkOp *storage.ChunkOp
		if r != nil && r.Iter != nil {
			ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
			ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)
			begX := ptBeg.Value(0)
			endX := ptEnd.Value(0)

			blocksInROI := make(map[string]bool, (endX - begX + 1))
			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				curIndex := dvid.IndexZYX(c)
				if r.Iter.InsideFast(curIndex) {
					indexString := string(curIndex.Bytes())
					blocksInROI[indexString] = true
				}
			}
			chunkOp = &storage.ChunkOp{&getOperation{vox, blocksInROI, r.attenuation}, wg}
		} else {
			chunkOp = &storage.ChunkOp{&getOperation{vox, nil, 0}, wg}
		}

		// Send the entire range of key-value pairs to chunk processor
		err = store.ProcessRange(ctx, blockBeg, blockEnd, chunkOp, storage.ChunkProcessor(d.ReadChunk))
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %s", ctx, err.Error())
		}
	}
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

// GetBlocks returns a slice of bytes corresponding to all the blocks along a span in X
func (d *Data) GetBlocks(v dvid.VersionID, start dvid.ChunkPoint3d, span int) ([]byte, error) {
	indexBeg := dvid.IndexZYX(start)
	end := start
	end[0] += int32(span - 1)
	indexEnd := dvid.IndexZYX(end)
	voxelBlockBeg := NewIndex(&indexBeg)
	voxelBlockEnd := NewIndex(&indexEnd)

	ctx := datastore.NewVersionedContext(d, v)
	keyvalues, err := store.GetRange(ctx, voxelBlockBeg, voxelBlockEnd)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	// Save the # of keyvalues actually obtained.
	numkv := len(keyvalues)
	binary.Write(&buf, binary.LittleEndian, int32(numkv))

	// Write the block indices in XYZ little-endian format + the size of each block
	uncompress := true
	for _, kv := range keyvalues {
		block, _, err := dvid.DeserializeData(kv.V, uncompress)
		if err != nil {
			return nil, fmt.Errorf("Unable to deserialize block, %s (%v): %s", ctx, kv.K, err.Error())
		}
		_, err = buf.Write(block)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Loads blocks with old data if they exist.
func (d *Data) loadOldBlocks(v dvid.VersionID, vox *Voxels, blocks storage.KeyValues) error {
	ctx := datastore.NewVersionedContext(d, v)

	// Create a map of old blocks indexed by the index
	oldBlocks := map[string]([]byte){}

	// Iterate through index space for this data using ZYX ordering.
	blockSize := d.BlockSize()
	blockNum := 0
	for it, err := vox.IndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		begBytes := NewIndex(indexBeg)
		endBytes := NewIndex(indexEnd)

		// Get previous data.
		keyvalues, err := store.GetRange(ctx, begBytes, endBytes)
		if err != nil {
			return err
		}
		for _, kv := range keyvalues {
			indexBytes, err := ctx.IndexFromKey(kv.K)
			if err != nil {
				return err
			}
			block, _, err := dvid.DeserializeData(kv.V, true)
			if err != nil {
				return fmt.Errorf("Unable to deserialize block, %s: %s", ctx, err.Error())
			}
			oldBlocks[string(indexBytes)] = block
		}

		// Load previous data into blocks
		ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
		ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)
		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)
		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndex := dvid.IndexZYX(c)
			curIndexBytes := NewIndex(&curIndex)
			blocks[blockNum].K = ctx.ConstructKey(curIndexBytes)
			block, ok := oldBlocks[string(curIndexBytes)]
			if ok {
				copy(blocks[blockNum].V, block)
			}
			blockNum++
		}
	}
	return nil
}

// ReadChunk reads a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) ReadChunk(chunk *storage.Chunk) error {
	<-server.HandlerToken
	go d.readChunk(chunk)
	return nil
}

func (d *Data) readChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op, ok := chunk.Op.(*getOperation)
	if !ok {
		log.Fatalf("Illegal operation passed to readChunk() for data %s\n", d.DataName())
	}

	// Make sure our received chunk is valid.
	if chunk == nil {
		dvid.Errorf("Received nil chunk in readChunk.  Ignoring chunk.\n")
		return
	}
	if chunk.K == nil {
		dvid.Errorf("Received nil chunk key in readChunk.  Ignoring chunk.\n")
		return
	}

	// If there's an ROI, if outside ROI, use blank buffer or allow scaling via attenuation.
	var zeroOut bool
	var attenuation uint8
	indexZYX, err := DecodeKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error processing voxel block: %s\n", err.Error())
		return
	}
	if op.blocksInROI != nil {
		indexString := string(indexZYX.Bytes())
		_, insideROI := op.blocksInROI[indexString]
		if !insideROI {
			if op.attenuation == 0 {
				zeroOut = true
			}
			attenuation = op.attenuation
		}
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var blockData []byte
	if zeroOut || chunk.V == nil {
		blockData = d.BackgroundBlock()
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
			return
		}
	}

	// Perform the operation.
	block := &storage.KeyValue{chunk.K, blockData}
	if err = op.voxels.ReadBlock(block, d.BlockSize(), attenuation); err != nil {
		dvid.Errorf("Unable to ReadFromBlock() in %q: %s\n", d.DataName(), err.Error())
		return
	}
}
