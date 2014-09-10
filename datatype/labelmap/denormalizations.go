/*
	This file contains code for denormalized representations of labelmap data, e.g., indices
	for fast queries of all labels meeting given size restrictions, or sparse volume
	representations for a label.
*/

package labelmap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// GetLabelsInVolume returns a JSON list of mapped labels that intersect a volume bounded
// by the specified block coordinates.  Note that the blocks are specified using block
// coordinates, so if this data instance has 32 x 32 x 32 voxel blocks, and we specify
// min block (1,2,3) and max block (3,4,5), the subvolume in voxels will be from min voxel
// point (32, 64, 96) to max voxel point (96, 128, 160).
func (d *Data) GetLabelsInVolume(ctx storage.Context, minBlock, maxBlock dvid.ChunkPoint3d) (string, error) {
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return "{}", err
	}

	// Get the mappings for this span of keys by using just spatial indices.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	maxLabelBytes := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

	offset := 1 + dvid.IndexZYXSize + 8 // index here = s + a + b, and we want only b
	labelset := make(map[uint64]bool, 10)
	for it := dvid.NewIndexZYXIterator(minBlock, maxBlock); it.Valid(); it.NextSpan() {
		// Get keys for this span of blocks
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return "{}", err
		}
		begIndex := voxels.NewSpatialMapIndex(indexBeg, nil, 0)
		endIndex := voxels.NewSpatialMapIndex(indexEnd, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		keys, err := smalldata.KeysInRange(ctx, begIndex, endIndex)
		if err != nil {
			return "{}", err
		}

		// Add mapped labels for these keys into the set
		for _, key := range keys {
			indexBytes, err := ctx.IndexFromKey(key)
			if err != nil {
				return "{}", err
			}
			mappedLabel := binary.BigEndian.Uint64(indexBytes[offset : offset+8])
			labelset[mappedLabel] = true
		}
	}

	// Convert set to a JSON compatible list.
	numLabels := len(labelset)
	dvid.Debugf("Found %d labels that intersect subvolume with block coords %s -> %s\n", numLabels,
		minBlock, maxBlock)
	labellist := make([]uint64, numLabels, numLabels)
	i := 0
	for label, _ := range labelset {
		labellist[i] = label
		i++
	}
	m, err := json.Marshal(labellist)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// GetLabelAtPoint returns a mapped label for a given point.
func (d *Data) GetLabelAtPoint(ctx storage.Context, pt dvid.Point) (uint64, error) {
	labels, err := d.Labels.GetData()
	if err != nil {
		return 0, err
	}
	labelBytes, err := labels.GetLabelBytesAtPoint(ctx, pt)
	if err != nil {
		return 0, err
	}
	// Apply mapping.
	return d.GetLabelMapping(ctx.VersionID(), labelBytes)
}

type denormOp struct {
	source    *labels64.Data
	mapped    voxels.ExtData // Can store mapped labels into this if provided.
	dest      dvid.Data
	versionID dvid.VersionID
	mapping   map[string]uint64
}

// GetMappedImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetMappedImage(versionID dvid.VersionID, e voxels.ExtData) (*dvid.Image, error) {
	if err := d.GetMappedVoxels(versionID, e); err != nil {
		return nil, err
	}
	return e.GetImage2d()
}

// GetMappedVolume retrieves a n-d volume from a version node given a geometry of voxels.
func (d *Data) GetMappedVolume(versionID dvid.VersionID, e voxels.ExtData) ([]byte, error) {
	if err := d.GetMappedVoxels(versionID, e); err != nil {
		return nil, err
	}
	return e.Data(), nil
}

// GetMappedVoxels copies mapped labels for each voxel for a version to an ExtData, e.g.,
// a requested subvolume or 2d image.
func (d *Data) GetMappedVoxels(versionID dvid.VersionID, e voxels.ExtData) error {
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
	}
	labelData, err := d.Labels.GetData()
	if err != nil {
		dvid.Errorf("Could not get labels64 data for '%s'", d.Labels)
	}
	labelsCtx := datastore.NewVersionedContext(labelData, versionID)
	mappingCtx := datastore.NewVersionedContext(d, versionID)

	wg := new(sync.WaitGroup)
	for it, err := e.IndexIterator(labelData.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}

		// Get the mappings for this span of key-value by using just spatial indices.
		// We work with the spatial index (s), original label (a), and mapped label (b).
		maxLabelBytes := make([]byte, 8, 8)
		binary.BigEndian.PutUint64(maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		sabBeg := voxels.NewSpatialMapIndex(indexBeg, nil, 0)
		sabEnd := voxels.NewSpatialMapIndex(indexEnd, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		keys, err := smalldata.KeysInRange(mappingCtx, sabBeg, sabEnd)
		if err != nil {
			return err
		}
		numKeys := len(keys)
		if numKeys == 0 {
			continue
		}

		// Cache this layer of blocks' mappings.
		labelOffset := 1 + dvid.IndexZYXSize // index here = s + a + b
		mapping := make(map[string]uint64, numKeys)
		for _, key := range keys {
			indexBytes, err := mappingCtx.IndexFromKey(key)
			if err != nil {
				return err
			}
			label := string(indexBytes[labelOffset : labelOffset+8])
			mappedLabel := binary.BigEndian.Uint64(indexBytes[labelOffset+8 : labelOffset+16])
			mapping[label] = mappedLabel
		}

		// Send the entire range of key-value pairs to chunk mapper
		chunkOp := &storage.ChunkOp{&denormOp{labelData, e, nil, versionID, mapping}, wg}
		err = bigdata.ProcessRange(labelsCtx, indexBeg.Bytes(), indexEnd.Bytes(), chunkOp, d.MapChunk)
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %s", d.Data.DataName(), err.Error())
		}
	}
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

// Iterate through all blocks in the associated label volume, computing the spatial indices
// for bodies and the mappings for each spatial index.
func (d *Data) ProcessSpatially(uuid dvid.UUID) {
	dvid.Infof("Adding spatial information from label volume %s ...\n", d.DataName())

	versionID, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		dvid.Errorf("Illegal UUID %q with no corresponding version ID!  Aborting.", uuid)
		return
	}

	labelData, err := d.Labels.GetData()
	if err != nil {
		dvid.Errorf("Could not get labels64 data for '%s'", d.Labels)
	}

	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
		return
	}
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
		return
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	timedLog := dvid.NewTimeLog()
	wg := new(sync.WaitGroup)
	op := &denormOp{source: labelData, versionID: versionID}

	extents := labelData.Extents()
	minIndexZ := extents.MinIndex.Value(2)
	maxIndexZ := extents.MaxIndex.Value(2)

	labelsCtx := datastore.NewVersionedContext(labelData, versionID)
	for z := minIndexZ; z <= maxIndexZ; z++ {
		blockLog := dvid.NewTimeLog()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			dvid.Errorf("Error getting label mapping for block Z %d: %s\n", z, err.Error())
			return
		}

		// Process the labels chunks for this Z
		if op.mapping != nil {
			begIndex := dvid.IndexZYX(minChunkPt)
			endIndex := dvid.IndexZYX(maxChunkPt)
			chunkOp := &storage.ChunkOp{op, wg}
			err = bigdata.ProcessRange(labelsCtx, begIndex.Bytes(), endIndex.Bytes(), chunkOp, d.DenormalizeChunk)
			wg.Wait()
		} else {
			dvid.Infof("No mapping for block layer %d found!\n", z)
		}
		blockLog.Debugf("Processed all '%s' blocks for layer %d/%d",
			d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	timedLog.Infof("Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	timedLog = dvid.NewTimeLog()
	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	labelmapCtx := datastore.NewVersionedContext(d, versionID)
	go labels64.ComputeSizes(labelmapCtx, sizeCh, smalldata, wg)

	// Create a number of label-specific surface calculation jobs
	// TODO: Spawn as many surface calculators as we have handler tokens for.
	// Each surface calculator can use memory ~ XY slice x block Z so memory
	// and # of cores is important.  Might have to pass this in if there's no
	// introspection.
	size := extents.MaxPoint.Sub(extents.MinPoint)
	goroutineMB := size.Value(0) * size.Value(1) * (2*labelData.BlockSize().Value(2) + 1) / dvid.Mega
	numSurfCalculators := dvid.EstimateGoroutines(0.5, goroutineMB)
	surfaceCh := make([]chan *storage.Chunk, numSurfCalculators, numSurfCalculators)
	for i := 0; i < numSurfCalculators; i++ {
		<-server.HandlerToken
		surfaceCh[i] = make(chan *storage.Chunk, 10000)
		wg.Add(1)
		go labels64.ComputeSurface(labelmapCtx, labelData, surfaceCh[i], wg)
	}

	// Wait for results then set Updating.
	go func() {
		wg.Wait()
		timedLog.Infof("Finished processing all RLEs for labels '%s'", d.DataName())
		d.Ready = true
		if err := datastore.SaveRepo(uuid); err != nil {
			dvid.Errorf("Could not save READY state to data '%s', uuid %s: %s", d.DataName(), uuid, err.Error())
		}
	}()

	// Iterate through all mapped labels and send to size and surface processing goroutines.
	begIndex := voxels.NewLabelSpatialMapIndex(0, &dvid.MinIndexZYX)
	endIndex := voxels.NewLabelSpatialMapIndex(math.MaxUint64, &dvid.MaxIndexZYX)
	err = smalldata.ProcessRange(labelmapCtx, begIndex, endIndex, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		// Get label associated with this sparse volume.
		indexBytes, err := labelmapCtx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Unable to recover label with chunk key %v: %s\n", chunk.K, err.Error())
			return
		}
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%uint64(numSurfCalculators)] <- chunk
	})
	if err != nil {
		dvid.Errorf("Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	for i := 0; i < numSurfCalculators; i++ {
		surfaceCh[i] <- nil
	}
	timedLog.Infof("Finished reading all RLEs for labels '%s'", d.DataName())
}

// MapChunk processes a chunk of label data, storing the mapped labels64.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc).
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) MapChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.mapChunk(chunk)
}

func (d *Data) mapChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op, ok := chunk.Op.(*denormOp)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var err error
	var blockData []byte
	if chunk == nil || chunk.V == nil {
		blockData = make([]byte, op.source.BlockSize().Prod()*int64(op.source.Values().BytesPerElement()))
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in '%s': %s\n", d.Data.DataName(), err.Error())
			return
		}
	}

	// Transfer the mapped data.
	block := &voxels.Block{K: chunk.K, V: blockData}
	blockSize := op.source.BlockSize()

	blockBeg, dataBeg, dataEnd, err := voxels.ComputeTransform(op.mapped, block, blockSize)
	if err != nil {
		dvid.Errorf("Error in mapChunk(): %s\n", err.Error())
		return
	}
	data := op.mapped.Data()
	bytesPerVoxel := op.mapped.Values().BytesPerElement()

	// Compute the strides (in bytes)
	bX := blockSize.Value(0) * bytesPerVoxel
	bY := blockSize.Value(1) * bX
	dX := op.mapped.Stride()

	// Do the transfers depending on shape of the external voxels.
	switch {
	case op.mapped.DataShape().Equals(dvid.XY):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(1)*dX + dataBeg.Value(0)*bytesPerVoxel
		span := (dataEnd.Value(0) - dataBeg.Value(0) + 1)
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			b0 := blockI
			b1 := blockI + bytesPerVoxel
			d0 := dataI
			d1 := dataI + bytesPerVoxel
			for x := int32(0); x < span; x++ {
				origLabel := string(block.V[b0:b1])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Errorf("No mapping found for label %s ... using 0\n", origLabel)
					mappedLabel = 0
				}
				binary.BigEndian.PutUint64(data[d0:d1], mappedLabel)
				b0 += bytesPerVoxel
				b1 += bytesPerVoxel
				d0 += bytesPerVoxel
				d1 += bytesPerVoxel
			}
			blockI += bX
			dataI += dX
		}

	case op.mapped.DataShape().Equals(dvid.XZ):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(2)*op.mapped.Stride() + dataBeg.Value(0)*bytesPerVoxel
		span := (dataEnd.Value(0) - dataBeg.Value(0) + 1)
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			b0 := blockI
			b1 := blockI + bytesPerVoxel
			d0 := dataI
			d1 := dataI + bytesPerVoxel
			for x := int32(0); x < span; x++ {
				origLabel := string(block.V[b0:b1])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Errorf("No mapping found for label %s ... using 0\n", origLabel)
					mappedLabel = 0
				}
				binary.BigEndian.PutUint64(data[d0:d1], mappedLabel)
				b0 += bytesPerVoxel
				b1 += bytesPerVoxel
				d0 += bytesPerVoxel
				d1 += bytesPerVoxel
			}
			blockI += bY
			dataI += dX
		}

	case op.mapped.DataShape().Equals(dvid.YZ):
		bz := blockBeg.Value(2)
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			blockI := bz*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
			dataI := y*dX + dataBeg.Value(1)*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				origLabel := string(block.V[blockI : blockI+bytesPerVoxel])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Infof("No mapping found for label %s ... using 0\n", origLabel)
					mappedLabel = 0
				}
				binary.BigEndian.PutUint64(data[dataI:dataI+bytesPerVoxel], mappedLabel)
				blockI += bX
				dataI += bytesPerVoxel
			}
			bz++
		}

	case op.mapped.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		dvid.Errorf("DVID currently does not support 2d in n-d space.")

	case op.mapped.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBeg.Value(0) * bytesPerVoxel
		dX = op.mapped.Size().Value(0) * bytesPerVoxel
		dY := op.mapped.Size().Value(1) * dX
		dataOffset := dataBeg.Value(0) * bytesPerVoxel
		bytes := (dataEnd.Value(0) - dataBeg.Value(0) + 1) * bytesPerVoxel
		blockZ := blockBeg.Value(2)

		for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
			blockY := blockBeg.Value(1)
			for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
				blockI := blockZ*bY + blockY*bX + blockOffset
				dataI := dataZ*dY + dataY*dX + dataOffset
				copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
				blockY++
			}
			blockZ++
		}

	default:
		dvid.Errorf("Cannot ReadFromBlock() unsupported voxels data shape %s", op.mapped.DataShape())
	}
}

// DenormalizeChunk processes a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) DenormalizeChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.denormalizeChunk(chunk)
}

func (d *Data) denormalizeChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*denormOp)

	// Get the spatial index associated with this chunk.
	ctx := datastore.NewVersionedContext(d, op.versionID)
	indexBytes, err := ctx.IndexFromKey(chunk.K)
	if err != nil {
		dvid.Errorf("Bad chunk key %v: %s\n", chunk.K, err.Error())
		return
	}
	var zyx dvid.IndexZYX
	if err := zyx.IndexFromBytes(indexBytes); err != nil {
		dvid.Errorf("Cannot recover ZYX index from chunk key %v: %s\n", chunk.K, err.Error())
		return
	}
	zyxBytes := zyx.Bytes()

	// Setup the database
	db, err := storage.SmallDataStore()
	if err != nil {
		dvid.Infof("Error in %s.denormalizeChunk(): %s\n", d.DataName(), err.Error())
		return
	}
	smallBatcher, ok := db.(storage.KeyValueBatcher)
	if !ok {
		dvid.Infof("Database doesn't support Batch ops in %s.denormalizeChunk()", d.DataName())
		return
	}
	smallBatch := smallBatcher.NewBatch(ctx)
	defer func() {
		if err := smallBatch.Commit(); err != nil {
			dvid.Errorf("Error on batch PUT of KeySpatialMap on %s %s: %s\n", ctx, zyx, err.Error())
		}
	}()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}

	// Construct block-level mapping keys that allow quick range queries pertinent to access patterns.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	offsetSAB := 1 + dvid.IndexZYXSize
	sabIndex := make([]byte, offsetSAB+8+8) // s + a + b
	sabIndex[0] = byte(voxels.KeySpatialMap)
	copy(sabIndex[1:offsetSAB], zyxBytes)

	// Iterate through this block of labels64.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	written := make(map[string]bool, blockBytes/10)
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := zyx.MinPoint(op.source.BlockSize()).(dvid.Point3d)
	lastPt := zyx.MaxPoint(op.source.BlockSize()).(dvid.Point3d)

	var curStart dvid.Point3d
	var b, curLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				// Get the label to which the current label is mapped.
				a := blockData[start : start+8]
				start += 8

				if bytes.Compare(a, labels64.ZeroBytes()) == 0 {
					b = 0
				} else {
					b, ok = op.mapping[string(a)]
					if !ok {
						zBeg := zyx.MinPoint(op.source.BlockSize()).Value(2)
						zEnd := zyx.MaxPoint(op.source.BlockSize()).Value(2)
						slice := binary.BigEndian.Uint32(a[0:4])
						dvid.Errorf("No mapping found for %x (slice %d) in block with Z %d to %d... setting to 0\n",
							a, slice, zBeg, zEnd)
						b = 0
					}
				}

				// If we hit background or have switched label, save old run and start new one.
				if b == 0 || b != curLabel {
					// Save old run
					if curRun > 0 {
						labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					}
					// Start new one if not zero label.
					if b != 0 {
						curStart = dvid.Point3d{x, y, z}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = b
				} else {
					curRun++
				}

				// Store a KeySpatialMap key (index = s + a + b)
				if b != 0 {
					copy(sabIndex[offsetSAB:offsetSAB+8], a)
					binary.BigEndian.PutUint64(sabIndex[offsetSAB+8:offsetSAB+16], b)
					_, found := written[string(sabIndex)]
					if !found {
						smallBatch.Put(sabIndex, dvid.EmptyValue())
						written[string(sabIndex)] = true
					}
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

	// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
	labels64.StoreKeyLabelSpatialMap(op.versionID, d, smallBatcher, zyxBytes, labelRLEs)
}
