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
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type sparseOp struct {
	versionID dvid.VersionLocalID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func (d *Data) computeSurface(surfaceCh chan *storage.Chunk, db storage.OrderedKeyValueSetter,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	defer func() {
		wg.Done()
		server.HandlerToken <- 1
	}()

	// Sequentially process all the sparse volume data for each label
	var curVol dvid.SparseVol
	var curLabel uint64
	notFirst := false
	for {
		chunk := <-surfaceCh
		if chunk == nil {
			if notFirst {
				if err := d.computeAndSaveSurface(versionID, &curVol); err != nil {
					dvid.Log(dvid.Normal, "Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)
		if label != curLabel || label == 0 {
			if notFirst {
				if err := d.computeAndSaveSurface(versionID, &curVol); err != nil {
					dvid.Log(dvid.Normal, "Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			curVol.Clear()
			curVol.SetLabel(label)
		}

		if err := curVol.AddRLEs(chunk.V); err != nil {
			dvid.Log(dvid.Normal, "Error adding RLE for label %d: %s\n", label, err.Error())
			return
		}
		curLabel = label
		notFirst = true
	}
}

func (d *Data) computeAndSaveSurface(versionID dvid.VersionLocalID, vol *dvid.SparseVol) error {
	labelData, err := d.Labels.GetData()
	if err != nil {
		return err
	}

	data, err := vol.SurfaceSerialization(labelData.BlockSize().Value(2), labelData.Resolution.VoxelSize)
	if err != nil {
		return err
	}

	db, err := server.OrderedKeyValueSetter()
	if err != nil {
		return err
	}
	// Surface blobs are always stored using gzip with best compression, trading off time
	// during the store for speed during interactive GETs.
	compression, _ := dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
	serialization, err := dvid.SerializeData(data, compression, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("Unable to serialize data in surface computation: %s\n", err.Error())
	}
	return db.Put(labels.NewLabelSurfaceKey(d, versionID, vol.Label()), serialization)
}

// GetLabelsInVolume returns a JSON list of mapped labels that intersect a volume bounded
// by the specified block coordinates.  Note that the blocks are specified using block
// coordinates, so if this data instance has 32 x 32 x 32 voxel blocks, and we specify
// min block (1,2,3) and max block (3,4,5), the subvolume in voxels will be from min voxel
// point (32, 64, 96) to max voxel point (96, 128, 160).
func (d *Data) GetLabelsInVolume(uuid dvid.UUID, minBlock, maxBlock dvid.ChunkPoint3d) (string, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return "{}", err
	}
	db, err := server.OrderedKeyValueGetter()
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
		startKey := labels.NewSpatialMapKey(d, versionID, indexBeg, nil, 0)
		endKey := labels.NewSpatialMapKey(d, versionID, indexEnd, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		var keys []storage.Key
		keys, err = db.KeysInRange(startKey, endKey)
		if err != nil {
			return "{}", err
		}

		// Add mapped labels for these keys into the set
		for _, key := range keys {
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
			mappedLabel := binary.BigEndian.Uint64(indexBytes[offset : offset+8])
			labelset[mappedLabel] = true
		}
	}

	// Convert set to a JSON compatible list.
	numLabels := len(labelset)
	dvid.Log(dvid.Debug, "Found %d labels that intersect subvolume with block coords %s -> %s\n", numLabels,
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
func (d *Data) GetLabelAtPoint(uuid dvid.UUID, pt dvid.Point) (uint64, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return 0, err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return 0, err
	}

	// Compute the block key that contains the given point.
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return 0, fmt.Errorf("Can't determine block of point %s", pt)
	}
	labels, err := d.Labels.GetData()
	if err != nil {
		return 0, err
	}
	blockSize := labels.BlockSize()
	blockCoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d) // TODO -- Get rid of this cast
	key := d.DataKey(versionID, dvid.IndexZYX(blockCoord))

	// Retrieve the block of labels
	serialization, err := db.Get(key)
	if err != nil {
		return 0, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), blockCoord)
	}
	labelData, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return 0, fmt.Errorf("Unable to deserialize block %s in '%s': %s\n",
			blockCoord, d.DataName(), err.Error())
	}

	// Retrieve the particular label within the block.
	ptInBlock := coord.PointInChunk(blockSize)
	nx := blockSize.Value(0)
	nxy := nx * blockSize.Value(1)
	i := (ptInBlock.Value(0) + ptInBlock.Value(1)*nx + ptInBlock.Value(2)*nxy) * 8

	// Apply mapping.
	return d.GetLabelMapping(versionID, labelData[i:i+8])
}

// GetSparseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    # Voxels
//    uint32    # Spans
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//		  ...
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func (d *Data) GetSparseVol(uuid dvid.UUID, label uint64) ([]byte, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return nil, err
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))
	binary.Write(buf, binary.LittleEndian, byte(0))
	buf.WriteByte(byte(0))
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	// Get the start/end keys for this body's KeyLabelSpatialMap (b + s) keys.
	firstKey := labels.NewLabelSpatialMapKey(d, versionID, label, dvid.MinIndexZYX)
	lastKey := labels.NewLabelSpatialMapKey(d, versionID, label, dvid.MaxIndexZYX)

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	wg := new(sync.WaitGroup)
	op := &sparseOp{versionID: versionID, encoding: buf.Bytes()}
	err = db.ProcessRange(firstKey, lastKey, &storage.ChunkOp{op, wg}, func(chunk *storage.Chunk) {
		op := chunk.Op.(*sparseOp)
		op.numBlocks++
		op.encoding = append(op.encoding, chunk.V...)
		op.numRuns += uint32(len(chunk.V) / 16)
		chunk.Wg.Done()
	})
	if err != nil {
		return nil, err
	}
	wg.Wait()

	binary.LittleEndian.PutUint32(op.encoding[8:12], op.numRuns)

	dvid.Log(dvid.Debug, "For data '%s' label %d: found %d blocks, %d runs\n",
		d.DataName(), label, op.numBlocks, op.numRuns)
	return op.encoding, nil
}

// GetSurface returns a byte array with # voxels and float32 arrays for vertices and
// normals.
func (d *Data) GetSurface(uuid dvid.UUID, label uint64) (s []byte, found bool, err error) {
	service := server.DatastoreService()
	_, versionID, e := service.LocalIDFromUUID(uuid)
	if e != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, e.Error())
		return
	}

	// Retrieve the precomputed surface or that it's not available.
	key := labels.NewLabelSurfaceKey(d, versionID, label)

	db, e := server.OrderedKeyValueGetter()
	if e != nil {
		err = e
		return
	}
	data, e := db.Get(key)
	if e != nil {
		err = fmt.Errorf("Error in retrieving surface for key '%s': %s", key, e.Error())
		return
	}
	if data == nil {
		return
	}
	uncompress := false
	s, _, e = dvid.DeserializeData(data, uncompress)
	if e != nil {
		err = fmt.Errorf("Unable to deserialize surface for key '%s': %s\n", key, e.Error())
		return
	}
	found = true
	return
}

type denormOp struct {
	source    *labels64.Data
	mapped    voxels.ExtHandler // Can store mapped labels into this if provided.
	destID    dvid.DataLocalID
	versionID dvid.VersionLocalID
	mapping   map[string]uint64
}

// GetMappedImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetMappedImage(uuid dvid.UUID, e voxels.ExtHandler) (*dvid.Image, error) {
	if err := d.GetMappedVoxels(uuid, e); err != nil {
		return nil, err
	}
	return e.GetImage2d()
}

// GetMappedVolume retrieves a n-d volume from a version node given a geometry of voxels.
func (d *Data) GetMappedVolume(uuid dvid.UUID, e voxels.ExtHandler) ([]byte, error) {
	if err := d.GetMappedVoxels(uuid, e); err != nil {
		return nil, err
	}
	return e.Data(), nil
}

// GetMappedVoxels copies mapped labels for each voxel for a version to an ExtHandler, e.g.,
// a requested subvolume or 2d image.
func (d *Data) GetMappedVoxels(uuid dvid.UUID, e voxels.ExtHandler) error {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return fmt.Errorf("Could not determine versionID in %s.ProcessSpatially(): %s",
			d.DataInstance.DataName(), err.Error())
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return err
	}

	labelData, err := d.Labels.GetData()
	if err != nil {
		dvid.Error("Could not get labels64 data for '%s'", d.Labels)
	}

	wg := new(sync.WaitGroup)
	dataID := d.DataInstance.ID
	repoID := d.DataInstance.DsetID
	for it, err := e.IndexIterator(labelData.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		indexBegZYX, ok := indexBeg.(dvid.IndexZYX)
		if !ok {
			return fmt.Errorf("First spatial index for mapped voxels request was not dvid.IndexZYX: %s", indexBeg)
		}
		indexEndZYX, ok := indexEnd.(dvid.IndexZYX)
		if !ok {
			return fmt.Errorf("Last spatial index for mapped voxels request was not dvid.IndexZYX: %s", indexEnd)
		}

		// Get the mappings for this span of key/value by using just spatial indices.
		// We work with the spatial index (s), original label (a), and mapped label (b).
		maxLabelBytes := make([]byte, 8, 8)
		binary.BigEndian.PutUint64(maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		sabKeyBeg := labels.NewSpatialMapKey(d, versionID, indexBegZYX, nil, 0)
		sabKeyEnd := labels.NewSpatialMapKey(d, versionID, indexEndZYX, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		var keys []storage.Key
		keys, err = db.KeysInRange(sabKeyBeg, sabKeyEnd)
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
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
			label := string(indexBytes[labelOffset : labelOffset+8])
			mappedLabel := binary.BigEndian.Uint64(indexBytes[labelOffset+8 : labelOffset+16])
			mapping[label] = mappedLabel
		}

		// Send the entire range of key/value pairs to chunk mapper
		chunkOp := &storage.ChunkOp{&denormOp{labelData, e, 0, versionID, mapping}, wg}
		startKey := &datastore.DataKey{repoID, dataID, versionID, indexBeg}
		endKey := &datastore.DataKey{repoID, dataID, versionID, indexEnd}
		err = db.ProcessRange(startKey, endKey, chunkOp, d.MapChunk)
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %s", d.DataInstance.DataName(), err.Error())
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
	dvid.Log(dvid.Normal, "Adding spatial information from label volume %s ...\n", d.DataName())

	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		dvid.Error("Could not determine versionID in %s.ProcessSpatially(): %s", d.DataInstance.DataName(), err.Error())
		return
	}
	db, err := server.OrderedKeyValueDB()
	if err != nil {
		dvid.Error("Could not determine key value datastore in %s.ProcessSpatially(): %s\n", d.DataInstance.DataName(), err.Error())
		return
	}

	labelData, err := d.Labels.GetData()
	if err != nil {
		dvid.Error("Could not get labels64 data for '%s'", d.Labels)
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	startTime := time.Now()
	wg := new(sync.WaitGroup)
	op := &denormOp{labelData, nil, 0, versionID, nil}

	dataID := labelData.DataInstance()
	extents := labelData.Extents()
	minIndexZ := extents.MinIndex.(dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(dvid.IndexZYX)[2]

	for z := minIndexZ; z <= maxIndexZ; z++ {
		t := time.Now()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			dvid.Error("Error getting label mapping for block Z %d: %s\n", z, err.Error())
			return
		}

		// Process the labels chunks for this Z
		minIndex := dvid.IndexZYX(minChunkPt)
		maxIndex := dvid.IndexZYX(maxChunkPt)
		if op.mapping != nil {
			startKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, minIndex}
			endKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, maxIndex}
			chunkOp := &storage.ChunkOp{op, wg}
			err = db.ProcessRange(startKey, endKey, chunkOp, d.DenormalizeChunk)
			wg.Wait()
		} else {
			dvid.Log(dvid.Normal, "No mapping for block layer %d found!\n", z)
		}

		dvid.ElapsedTime(dvid.Debug, t, "Processed all '%s' blocks for layer %d/%d",
			d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	startTime = time.Now()
	startKey := labels.NewLabelSpatialMapKey(d, versionID, 0, dvid.MinIndexZYX)
	endKey := labels.NewLabelSpatialMapKey(d, versionID, math.MaxUint64, dvid.MaxIndexZYX)
	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go labels.ComputeSizes(d, sizeCh, db, versionID, wg)

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
		go d.computeSurface(surfaceCh[i], db, versionID, wg)
	}

	// Wait for results then set Updating.
	go func() {
		wg.Wait()
		dvid.ElapsedTime(dvid.Debug, startTime, "Finished processing all RLEs for labels '%s'", d.DataName())
		d.Ready = true
		if err := server.DatastoreService().SaveRepo(uuid); err != nil {
			dvid.Error("Could not save READY state to data '%s', uuid %s: %s", d.DataName(), uuid, err.Error())
		}
	}()

	// Iterate through all mapped labels and send to size and surface processing goroutines.
	err = db.ProcessRange(startKey, endKey, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		// Get label associated with this sparse volume.
		dataKey := chunk.K.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%uint64(numSurfCalculators)] <- chunk
	})
	if err != nil {
		dvid.Error("Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	for i := 0; i < numSurfCalculators; i++ {
		surfaceCh[i] <- nil
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Finished reading all RLEs for labels '%s'", d.DataName())
}

// MapChunk processes a chunk of label data, storing the mapped labels.  The data may be
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
			dvid.Error("Unable to deserialize block in '%s': %s\n",
				d.DataInstance.DataName(), err.Error())
			return
		}
	}

	// Transfer the mapped data.
	block := &voxels.Block{K: chunk.K, V: blockData}
	blockSize := op.source.BlockSize()

	blockBeg, dataBeg, dataEnd, err := voxels.ComputeTransform(op.mapped, block, blockSize)
	if err != nil {
		dvid.Error("Error in mapChunk(): %s\n", err.Error())
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
					dvid.Error("No mapping found for label %s ... aborting\n", origLabel)
					return
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
					dvid.Error("No mapping found for label %s ... aborting\n", origLabel)
					return
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
					dvid.Log(dvid.Normal, "No mapping found for label %s ... aborting\n", origLabel)
					return
				}
				binary.BigEndian.PutUint64(data[dataI:dataI+bytesPerVoxel], mappedLabel)
				blockI += bX
				dataI += bytesPerVoxel
			}
			bz++
		}

	case op.mapped.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtHandler in n-d space.
		dvid.Error("DVID currently does not support 2d in n-d space.")

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
		dvid.Error("Cannot ReadFromBlock() unsupported voxels data shape %s", op.mapped.DataShape())
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
	dataKey := chunk.K.(*datastore.DataKey)
	zyx := dataKey.Index.(*dvid.IndexZYX)
	zyxBytes := zyx.Bytes()

	// Setup the database
	db, err := server.OrderedKeyValueDB()
	if err != nil {
		dvid.Log(dvid.Normal, "Error in %s.denormalizeChunk(): %s\n", d.DataName(), err.Error())
		return
	}
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Log(dvid.Normal, "Database doesn't support Batch ops in %s.denormalizeChunk()", d.DataName())
		return
	}
	batch := batcher.NewBatch()
	defer func() {
		if err := batch.Commit(); err != nil {
			dvid.Log(dvid.Normal, "Error on batch PUT of KeySpatialMap on %s: %s\n",
				dataKey.Index, err.Error())
		}
	}()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}

	// Construct block-level mapping keys that allow quick range queries pertinent to access patterns.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	offsetSAB := 1 + dvid.IndexZYXSize
	sabIndex := make([]byte, offsetSAB+8+8) // s + a + b
	sabIndex[0] = byte(labels.KeySpatialMap)
	copy(sabIndex[1:offsetSAB], zyxBytes)

	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Log(dvid.Normal, "Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
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

				if bytes.Compare(a, labels.ZeroBytes()) == 0 {
					b = 0
				} else {
					b, ok = op.mapping[string(a)]
					if !ok {
						zBeg := zyx.MinPoint(op.source.BlockSize()).Value(2)
						zEnd := zyx.MaxPoint(op.source.BlockSize()).Value(2)
						slice := binary.BigEndian.Uint32(a[0:4])
						dvid.Log(dvid.Normal, "No mapping found for %x (slice %d) in block with Z %d to %d\n",
							a, slice, zBeg, zEnd)
						dvid.Log(dvid.Normal, "Aborting processing of '%s' chunk using '%s' labelmap\n",
							op.source.DataName(), d.DataName())
						return
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
						key := d.DataKey(op.versionID, dvid.IndexBytes(sabIndex))
						batch.Put(key, dvid.EmptyValue())
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
	labels.StoreKeyLabelSpatialMap(d, batcher, op.versionID, zyxBytes, labelRLEs)
}
