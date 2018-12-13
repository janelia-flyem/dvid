package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

func readStreamedBlock(r io.Reader, scale uint8) (block *labels.Block, compressed []byte, bx, by, bz int32, err error) {
	hdrBytes := make([]byte, 16)
	var n int
	n, err = io.ReadFull(r, hdrBytes)
	if n == 0 || err != nil {
		return
	}
	if n != 16 {
		err = fmt.Errorf("error reading header bytes, only got %d bytes not 16", n)
		return
	}
	bx = int32(binary.LittleEndian.Uint32(hdrBytes[0:4]))
	by = int32(binary.LittleEndian.Uint32(hdrBytes[4:8]))
	bz = int32(binary.LittleEndian.Uint32(hdrBytes[8:12]))
	numBytes := int(binary.LittleEndian.Uint32(hdrBytes[12:16]))
	if numBytes == 0 {
		err = fmt.Errorf("illegal zero length block at block (%d, %d, %d) detected", bx, by, bz)
		return
	}
	bcoord := dvid.ChunkPoint3d{bx, by, bz}.ToIZYXString()
	compressed = make([]byte, numBytes)
	n, err = io.ReadFull(r, compressed)
	if n != numBytes || err != nil {
		err = fmt.Errorf("error reading %d bytes for block %s: %d actually read (%v)", numBytes, bcoord, n, err)
		return
	}

	gzipIn := bytes.NewBuffer(compressed)
	var zr *gzip.Reader
	zr, err = gzip.NewReader(gzipIn)
	if err != nil {
		err = fmt.Errorf("can't initiate gzip reader on compressed data of length %d: %v", len(compressed), err)
		return
	}
	var uncompressed []byte
	uncompressed, err = ioutil.ReadAll(zr)
	if err != nil {
		err = fmt.Errorf("can't read all %d bytes from gzipped block %s: %v", numBytes, bcoord, err)
		return
	}
	if err = zr.Close(); err != nil {
		err = fmt.Errorf("error on closing gzip on block read: %v", err)
		return
	}

	block = new(labels.Block)
	err = block.UnmarshalBinary(uncompressed)
	return
}

// ComputeTransform determines the block coordinate and beginning + ending voxel points
// for the data corresponding to the given Block.
func (v *Labels) ComputeTransform(tkey storage.TKey, blockSize dvid.Point) (blockBeg, dataBeg, dataEnd dvid.Point, err error) {
	var ptIndex *dvid.IndexZYX
	_, ptIndex, err = DecodeBlockTKey(tkey)
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
func (d *Data) GetImage(v dvid.VersionID, vox *Labels, supervoxels bool, scale uint8, roiname dvid.InstanceName) (*dvid.Image, error) {
	r, err := imageblk.GetROI(v, roiname, vox)
	if err != nil {
		return nil, err
	}
	if err := d.GetLabels(v, supervoxels, scale, vox, r); err != nil {
		return nil, err
	}
	return vox.GetImage2d()
}

// GetVolume retrieves a n-d volume from a version node given a geometry of labels.
func (d *Data) GetVolume(v dvid.VersionID, vox *Labels, supervoxels bool, scale uint8, roiname dvid.InstanceName) ([]byte, error) {
	r, err := imageblk.GetROI(v, roiname, vox)
	if err != nil {
		return nil, err
	}
	if err := d.GetLabels(v, supervoxels, scale, vox, r); err != nil {
		return nil, err
	}
	return vox.Data(), nil
}

type getOperation struct {
	version     dvid.VersionID
	voxels      *Labels
	blocksInROI map[string]bool
	mapping     *SVMap
}

// GetLabels copies labels from the storage engine to Labels, a requested subvolume or 2d image.
// If supervoxels is true, the returned labels are not mapped but are the raw supervoxels.
func (d *Data) GetLabels(v dvid.VersionID, supervoxels bool, scale uint8, vox *Labels, r *imageblk.ROI) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}
	if r != nil && scale != 0 {
		return fmt.Errorf("DVID does not currently support ROI masks on lower-scale GETs")
	}

	// Only do one request at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	var mapping *SVMap
	if !supervoxels {
		if mapping, err = getMapping(d, v); err != nil {
			return err
		}
	}
	wg := new(sync.WaitGroup)
	ctx := datastore.NewVersionedCtx(d, v)

	okv := store.(storage.BufferableOps)
	// extract buffer interface
	req, hasbuffer := okv.(storage.KeyValueRequester)
	if hasbuffer {
		okv = req.NewBuffer(ctx)
	}

	for it, err := vox.NewIndexIterator(d.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		begTKey := NewBlockTKey(scale, indexBeg)
		endTKey := NewBlockTKey(scale, indexEnd)

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
			chunkOp = &storage.ChunkOp{&getOperation{v, vox, blocksInROI, mapping}, wg}
		} else {
			chunkOp = &storage.ChunkOp{&getOperation{v, vox, nil, mapping}, wg}
		}

		if !hasbuffer {
			// Send the entire range of key-value pairs to chunk processor
			err = okv.ProcessRange(ctx, begTKey, endTKey, chunkOp, storage.ChunkFunc(d.ReadChunk))
			if err != nil {
				return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
			}
		} else {
			// Extract block list
			var tkeys []storage.TKey
			ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
			ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)
			begX := ptBeg.Value(0)
			endX := ptEnd.Value(0)

			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				tk := NewBlockTKeyByCoord(scale, c.ToIZYXString())
				tkeys = append(tkeys, tk)
			}

			err = okv.(storage.RequestBuffer).ProcessList(ctx, tkeys, chunkOp, storage.ChunkFunc(d.ReadChunk))
			if err != nil {
				return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
			}
		}
	}

	if hasbuffer {
		// submit the entire buffer to the DB
		err = okv.(storage.RequestBuffer).Flush()

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
	_, indexZYX, err := DecodeBlockTKey(chunk.K)
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
	var block labels.Block
	if zeroOut || chunk.V == nil {
		blockSize, ok := d.BlockSize().(dvid.Point3d)
		if !ok {
			dvid.Errorf("Block size for data %q is not 3d: %s\n", d.DataName(), d.BlockSize())
			return
		}
		block = *labels.MakeSolidBlock(0, blockSize)
	} else {
		var data []byte
		data, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in %q: %v\n", d.DataName(), err)
			return
		}
		if err := block.UnmarshalBinary(data); err != nil {
			dvid.Errorf("Unable to unmarshal labels Block compression in %q: %v\n", d.DataName(), err)
			return
		}
	}

	// Perform the operation.
	if op.mapping != nil && op.mapping.exists(op.version) {
		err = modifyBlockMapping(op.version, &block, op.mapping)
		if err != nil {
			dvid.Errorf("unable to modify block %s mapping: %v\n", indexZYX, err)
		}
	}
	if err = op.voxels.readBlock(chunk.K, block, d.BlockSize()); err != nil {
		dvid.Errorf("readBlock, key %v in %q: %v\n", chunk.K, d.DataName(), err)
	}
}

// overwrites labels in header with their mapped values, so converts blocks from
// supervoxels to body labels
func modifyBlockMapping(v dvid.VersionID, block *labels.Block, m *SVMap) error {
	ancestry, err := m.getAncestry(v)
	if err != nil {
		return fmt.Errorf("unable to get ancestry for version %d: %v", v, err)
	}
	m.RLock()
	for i, label := range block.Labels {
		mapped, found := m.mapLabel(label, ancestry)
		if found {
			block.Labels[i] = mapped
		}
	}
	m.RUnlock()
	return nil
}

func (v *Labels) readBlock(tkey storage.TKey, block labels.Block, blockSize dvid.Point) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := v.ComputeTransform(tkey, blockSize)
	if err != nil {
		return err
	}

	// TODO -- Refactor this function to make direct use of compressed label Block without
	// conversion into full label array.
	labels := v.Data()
	blabels, _ := block.MakeLabelVolume()

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
			copy(labels[dataI:dataI+bytes], blabels[blockI:blockI+bytes])
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*8
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*8
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * 8
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			copy(labels[dataI:dataI+bytes], blabels[blockI:blockI+bytes])
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			dataI := y*dX + int64(dataBeg.Value(1))*8
			blockI := bz*bY + blockBegY*bX + blockBegX*8
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				copy(labels[dataI:dataI+8], blabels[blockI:blockI+8])
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
				copy(labels[dataI:dataI+bytes], blabels[blockI:blockI+bytes])
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot readBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}
