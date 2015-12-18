package labelblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// ComputeTransform determines the block coordinate and beginning + ending voxel points
// for the data corresponding to the given Block.
func (v *Labels) ComputeTransform(block *storage.TKeyValue, blockSize dvid.Point) (blockBeg, dataBeg, dataEnd dvid.Point, err error) {
	var ptIndex *dvid.IndexZYX
	ptIndex, err = DecodeTKey(block.K)
	if err != nil {
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

// GetImage retrieves a 2d image from a version node given a geometry of labels.
func (d *Data) GetImage(v dvid.VersionID, vox *Labels, roiname dvid.InstanceName) (*dvid.Image, error) {
	r, err := imageblk.GetROI(v, roiname, vox)
	if err != nil {
		return nil, err
	}
	if err := d.GetLabels(v, vox, r); err != nil {
		return nil, err
	}
	return vox.GetImage2d()
}

// GetVolume retrieves a n-d volume from a version node given a geometry of labels.
func (d *Data) GetVolume(v dvid.VersionID, vox *Labels, roiname dvid.InstanceName) ([]byte, error) {
	r, err := imageblk.GetROI(v, roiname, vox)
	if err != nil {
		return nil, err
	}
	if err := d.GetLabels(v, vox, r); err != nil {
		return nil, err
	}
	return vox.Data(), nil
}

type getOperation struct {
	voxels      *Labels
	blocksInROI map[string]bool
	mapping     *labels.Mapping
}

// GetLabels copies labels from the storage engine to Labels, a requested subvolume or 2d image.
func (d *Data) GetLabels(v dvid.VersionID, vox *Labels, r *imageblk.ROI) error {
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	// Only do one request at a time, although each request can start many goroutines.
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

	ctx := datastore.NewVersionedCtx(d, v)

	iv := dvid.InstanceVersion{d.DataName(), v}
	mapping := labels.MergeCache.LabelMap(iv)

	wg := new(sync.WaitGroup)
	for it, err := vox.IndexIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		begTKey := NewTKey(indexBeg)
		endTKey := NewTKey(indexEnd)

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
			chunkOp = &storage.ChunkOp{&getOperation{vox, blocksInROI, mapping}, wg}
		} else {
			chunkOp = &storage.ChunkOp{&getOperation{vox, nil, mapping}, wg}
		}

		// Send the entire range of key-value pairs to chunk processor
		err = store.ProcessRange(ctx, begTKey, endTKey, chunkOp, storage.ChunkFunc(d.ReadChunk))
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
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
	store, err := storage.MutableStore()
	if err != nil {
		return nil, fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	indexBeg := dvid.IndexZYX(start)
	end := start
	end[0] += int32(span - 1)
	indexEnd := dvid.IndexZYX(end)
	begTKey := NewTKey(&indexBeg)
	endTKey := NewTKey(&indexEnd)

	ctx := datastore.NewVersionedCtx(d, v)

	iv := dvid.InstanceVersion{d.DataName(), v}
	mapping := labels.MergeCache.LabelMap(iv)

	keyvalues, err := store.GetRange(ctx, begTKey, endTKey)
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
			return nil, fmt.Errorf("Unable to deserialize block, %s (%v): %v", ctx, kv.K, err)
		}
		if mapping != nil {
			n := len(block) / 8
			for i := 0; i < n; i++ {
				orig := binary.LittleEndian.Uint64(block[i*8 : i*8+8])
				mapped, found := mapping.FinalLabel(orig)
				if !found {
					mapped = orig
				}
				binary.LittleEndian.PutUint64(block[i*8:i*8+8], mapped)
			}
		}

		_, err = buf.Write(block)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Loads blocks with old data if they exist.  Only used with ingestion so no need to worry
// about mapping changes.
func (d *Data) loadOldBlocks(v dvid.VersionID, vox *Labels, blocks storage.TKeyValues) error {
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	ctx := datastore.NewVersionedCtx(d, v)

	// Create a map of old blocks indexed by the index
	oldBlocks := map[dvid.IZYXString]([]byte){}

	// Iterate through index space for this data using ZYX ordering.
	blockSize := d.BlockSize()
	blockNum := 0
	for it, err := vox.IndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		begTKey := NewTKey(indexBeg)
		endTKey := NewTKey(indexEnd)

		// Get previous data.
		keyvalues, err := store.GetRange(ctx, begTKey, endTKey)
		if err != nil {
			return err
		}
		for _, kv := range keyvalues {
			indexZYX, err := DecodeTKey(kv.K)
			if err != nil {
				return err
			}
			block, _, err := dvid.DeserializeData(kv.V, true)
			if err != nil {
				return fmt.Errorf("Unable to deserialize block, %s: %v", ctx, err)
			}
			oldBlocks[indexZYX.ToIZYXString()] = block
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
			curTKey := NewTKey(&curIndex)
			blocks[blockNum].K = curTKey
			block, ok := oldBlocks[curIndex.ToIZYXString()]
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

	// If there's an ROI, if outside ROI, use blank buffer.
	var zeroOut bool
	indexZYX, err := DecodeTKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error processing voxel block: %s\n", err)
		return
	}
	if op.blocksInROI != nil {
		indexString := string(indexZYX.Bytes())
		_, insideROI := op.blocksInROI[indexString]
		if !insideROI {
			zeroOut = true
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
			dvid.Errorf("Unable to deserialize block in '%s': %v\n", d.DataName(), err)
			return
		}
	}

	// Perform the operation.
	block := &storage.TKeyValue{chunk.K, blockData}
	if op.mapping != nil {
		err := op.voxels.readMappedBlock(block, d.BlockSize(), op.mapping)
		if err != nil {
			dvid.Errorf("readMappedBlock, key %v in %q: %v\n", chunk.K, d.DataName(), err)
		}
	} else {
		err := op.voxels.readBlock(block, d.BlockSize())
		if err != nil {
			dvid.Errorf("readBlock, key %v in %q: %v\n", chunk.K, d.DataName(), err)
		}
	}
}

func (v *Labels) readMappedBlock(block *storage.TKeyValue, blockSize dvid.Point, m *labels.Mapping) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := v.ComputeTransform(block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * 8
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*8
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*8
		for y := int64(dataBeg.Value(1)); y <= int64(dataEnd.Value(1)); y++ {
			bI := blockI
			dI := dataI
			for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
				orig := binary.LittleEndian.Uint64(block.V[bI : bI+8])
				mapped, found := m.FinalLabel(orig)
				if found {
					binary.LittleEndian.PutUint64(data[dI:dI+8], mapped)
				} else {
					copy(data[dI:dI+8], block.V[bI:bI+8])
				}
				bI += 8
				dI += 8
			}
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*8
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*8
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			bI := blockI
			dI := dataI
			for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
				orig := binary.LittleEndian.Uint64(block.V[bI : bI+8])
				mapped, found := m.FinalLabel(orig)
				if found {
					binary.LittleEndian.PutUint64(data[dI:dI+8], mapped)
				} else {
					copy(data[dI:dI+8], block.V[bI:bI+8])
				}
				bI += 8
				dI += 8
			}
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			dataI := y*dX + int64(dataBeg.Value(1))*8
			blockI := bz*bY + blockBegY*bX + blockBegX*8
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				orig := binary.LittleEndian.Uint64(block.V[blockI : blockI+8])
				mapped, found := m.FinalLabel(orig)
				if found {
					binary.LittleEndian.PutUint64(data[dataI:dataI+8], mapped)
				} else {
					copy(data[dataI:dataI+8], block.V[blockI:blockI+8])
				}
				blockI += bX
				dataI += 8
			}
			bz++
		}

	case v.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		return fmt.Errorf("DVID currently does not support 2d in n-d space.")

	case v.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBegX * 8
		dX := int64(v.Size().Value(0)) * 8
		dY := int64(v.Size().Value(1)) * dX
		dataOffset := int64(dataBeg.Value(0)) * 8
		blockZ := blockBegZ

		for dataZ := int64(dataBeg.Value(2)); dataZ <= int64(dataEnd.Value(2)); dataZ++ {
			blockY := blockBegY
			for dataY := int64(dataBeg.Value(1)); dataY <= int64(dataEnd.Value(1)); dataY++ {
				bI := blockZ*bY + blockY*bX + blockOffset
				dI := dataZ*dY + dataY*dX + dataOffset
				for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
					orig := binary.LittleEndian.Uint64(block.V[bI : bI+8])
					mapped, found := m.FinalLabel(orig)
					if found {
						binary.LittleEndian.PutUint64(data[dI:dI+8], mapped)
					} else {
						copy(data[dI:dI+8], block.V[bI:bI+8])
					}
					bI += 8
					dI += 8
				}
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot readBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

func (v *Labels) readBlock(block *storage.TKeyValue, blockSize dvid.Point) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := v.ComputeTransform(block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * 8
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*8
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*8
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * 8
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*8
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*8
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * 8
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			dataI := y*dX + int64(dataBeg.Value(1))*8
			blockI := bz*bY + blockBegY*bX + blockBegX*8
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				copy(data[dataI:dataI+8], block.V[blockI:blockI+8])
				blockI += bX
				dataI += 8
			}
			bz++
		}

	case v.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		return fmt.Errorf("DVID currently does not support 2d in n-d space.")

	case v.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBegX * 8
		dX := int64(v.Size().Value(0)) * 8
		dY := int64(v.Size().Value(1)) * dX
		dataOffset := int64(dataBeg.Value(0)) * 8
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * 8
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
