/*
	This file contains code for denormalized representations of label data, e.g., indices
	for fast queries of all labels meeting given size restrictions, or sparse volume
	representations for a label.
*/

package labels64

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type KeyType byte

// Label indexing is handled through a variety of key spaces that optimize
// throughput for access patterns required by our API.  For dcumentation purposes,
// consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
//   v: # of voxels for a label
const (
	// KeyInverseMap have keys of form 'b+a'
	KeyInverseMap KeyType = iota

	// KeyForwardMap have keys of form 'a+b'
	// For superpixel->body maps, this key would be superpixel+body.
	KeyForwardMap

	// KeySpatialMap have keys of form 's+a+b'
	// They are useful for composing label maps for a spatial index.
	KeySpatialMap

	// KeyLabelSpatialMap have keys of form 'b+s' and have a sparse volume
	// encoding for its value. They are useful for returning all blocks
	// intersected by a label.
	KeyLabelSpatialMap

	// KeyLabelSizes have keys of form 'v+b'.
	// They allow rapid size range queries.
	KeyLabelSizes
)

var (
	emptyValue     = []byte{}
	zeroLabelBytes = make([]byte, 8, 8)
)

func (t KeyType) String() string {
	switch t {
	case KeyInverseMap:
		return "Inverse Label Map"
	case KeyForwardMap:
		return "Forward Label Map"
	case KeySpatialMap:
		return "Spatial Index to Labels Map"
	case KeyLabelSpatialMap:
		return "Forward Label to Spatial Index Map"
	case KeyLabelSizes:
		return "Forward Label sorted by volume"
	default:
		return "Unknown Key Type"
	}
}

// NewLabelSpatialMapKey returns a datastore.DataKey that encodes a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func (d *Data) NewLabelSpatialMapKey(vID dvid.VersionLocalID, label uint64, block dvid.IndexZYX) *datastore.DataKey {
	index := make([]byte, 1+8+dvid.IndexZYXSize)
	index[0] = byte(KeyLabelSpatialMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:9+dvid.IndexZYXSize], block.Bytes())
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSizesKey returns a datastore.DataKey that encodes a "size + mapped label".
func (d *Data) NewLabelSizesKey(vID dvid.VersionLocalID, size, label uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSurfaceKey returns a datastore.DataKey that provides a surface for a given label.
func (d *Data) NewLabelSurfaceKey(vID dvid.VersionLocalID, label uint64) *datastore.DataKey {
	index := make([]byte, 8)
	binary.BigEndian.PutUint64(index, label)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

type sparseOp struct {
	versionID dvid.VersionLocalID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// Adds retrieved RLE runs to an encoding.
func (d *Data) processLabelRuns(chunk *storage.Chunk) {
	op := chunk.Op.(*sparseOp)
	op.numBlocks++
	op.encoding = append(op.encoding, chunk.V...)
	op.numRuns += uint32(len(chunk.V) / 16)
	chunk.Wg.Done()
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func (d *Data) computeSizes(sizeCh chan *storage.Chunk, db storage.OrderedKeyValueSetter,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	dvid.Log(dvid.Debug, "Storing size in voxels for all labels '%s'\n", d.DataName())

	const BATCH_SIZE = 10000
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Log(dvid.Normal, "Storage engine does not support Batch PUT.  Aborting\n")
		return
	}
	batch := batcher.NewBatch()

	defer func() {
		wg.Done()
	}()

	// Sequentially process all the sparse volume data for each label
	var curLabel, curSize uint64
	putsInBatch := 0
	notFirst := false
	for {
		chunk := <-sizeCh
		if chunk == nil {
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			batch.Put(key, emptyValue)
			if err := batch.Commit(); err != nil {
				dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
					d.DataName(), err.Error())
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)

		// Compute the size
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(chunk.V); err != nil {
			dvid.Log(dvid.Normal, "Error deserializing RLEs: %s\n", err.Error())
			return
		}
		numVoxels, _ := rles.Stats()

		// If we are a new label, store size
		if notFirst && label != curLabel {
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			curSize = 0
			batch.Put(key, emptyValue)
			putsInBatch++
			if putsInBatch%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
						d.DataName(), err.Error())
					return
				}
				batch = batcher.NewBatch()
			}
		}
		curLabel = label
		curSize += uint64(numVoxels)
		notFirst = true
	}
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

	data, err := vol.SurfaceSerialization(d.Resolution.VoxelSize)
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
	return db.Put(d.NewLabelSurfaceKey(versionID, vol.Label()), serialization)
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
// If maxSize is 0, all mapped labels are returned >= minSize.
func (d *Data) GetSizeRange(uuid dvid.UUID, minSize, maxSize uint64) (string, error) {
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
		return "{}", err
	}

	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return "{}", err
	}

	// Get the start/end keys for the size range.
	firstKey := d.NewLabelSizesKey(versionID, minSize, 0)
	var upperBound uint64
	if maxSize != 0 {
		upperBound = maxSize
	} else {
		upperBound = math.MaxUint64
	}
	lastKey := d.NewLabelSizesKey(versionID, upperBound, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return "{}", err
	}

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		dataKey := key.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		labels[i] = binary.LittleEndian.Uint64(indexBytes[9:17])
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// GetLabelAtPoint returns a mapped label for a given point.
func (d *Data) GetLabelAtPoint(uuid dvid.UUID, pt dvid.Point) (uint64, error) {
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
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
	blockSize := d.BlockSize()
	blockCoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d) // TODO -- Get rid of this cast
	key := d.DataKey(versionID, dvid.IndexZYX(blockCoord))

	// Retrieve the block of labels
	serialization, err := db.Get(key)
	if err != nil {
		return 0, fmt.Errorf("Error getting '%s' block for index %s\n",
			d.DataName(), blockCoord)
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
	return d.Properties.ByteOrder.Uint64(labelData[i : i+8]), nil
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
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
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
	firstKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MinIndexZYX)
	lastKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MaxIndexZYX)

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	wg := new(sync.WaitGroup)
	op := &sparseOp{versionID: versionID, encoding: buf.Bytes()}
	err = db.ProcessRange(firstKey, lastKey, &storage.ChunkOp{op, wg}, d.processLabelRuns)
	if err != nil {
		return nil, err
	}
	wg.Wait()

	binary.LittleEndian.PutUint32(op.encoding[8:12], op.numRuns)

	dvid.Log(dvid.Debug, "For data '%s' label %d: found %d blocks, %d runs\n",
		d.DataName(), label, op.numBlocks, op.numRuns)
	return op.encoding, nil
}

// GetSurface returns a gzipped byte array with # voxels and float32 arrays for vertices and
// normals.
func (d *Data) GetSurface(uuid dvid.UUID, label uint64) (s []byte, found bool, err error) {
	service := server.DatastoreService()
	_, versionID, e := service.LocalIDFromUUID(uuid)
	if e != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, e.Error())
		return
	}

	// Retrieve the precomputed surface or that it's not available.
	key := d.NewLabelSurfaceKey(versionID, label)

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
	source    *Data
	versionID dvid.VersionLocalID
}

// Iterate through all blocks in the associated label volume, computing the spatial indices
// for bodies and the mappings for each spatial index.
func (d *Data) ProcessSpatially(uuid dvid.UUID) {
	dvid.Log(dvid.Normal, "Adding spatial information from label volume %s ...\n", d.DataName())

	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		dvid.Log(dvid.Normal, "Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
		return
	}

	db, err := server.OrderedKeyValueDB()
	if err != nil {
		dvid.Log(dvid.Normal, "Could not determine key value datastore in %s.ProcessSpatially()\n",
			d.DataName())
		return
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	startTime := time.Now()
	wg := new(sync.WaitGroup)
	op := &denormOp{d, versionID}

	dataID := d.DataID()
	extents := d.Extents()
	minIndexZ := extents.MinIndex.(dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(dvid.IndexZYX)[2]
	for z := minIndexZ; z <= maxIndexZ; z++ {
		t := time.Now()

		minChunkPt := dvid.ChunkPoint3d{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], z}
		maxChunkPt := dvid.ChunkPoint3d{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], z}

		// Process the labels chunks for this Z
		minIndex := dvid.IndexZYX(minChunkPt)
		maxIndex := dvid.IndexZYX(maxChunkPt)
		startKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, minIndex}
		endKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, maxIndex}
		chunkOp := &storage.ChunkOp{op, wg}
		err = db.ProcessRange(startKey, endKey, chunkOp, d.DenormalizeChunk)
		wg.Wait()

		dvid.ElapsedTime(dvid.Debug, t, "Processed all '%s' blocks for layer %d/%d",
			d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	startTime = time.Now()
	startKey := d.NewLabelSpatialMapKey(versionID, 0, dvid.MinIndexZYX)
	endKey := d.NewLabelSpatialMapKey(versionID, math.MaxUint64, dvid.MaxIndexZYX)
	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go d.computeSizes(sizeCh, db, versionID, wg)

	// Create a number of label-specific surface calculation jobs
	const numSurfCalculators = 3
	var surfaceCh [numSurfCalculators]chan *storage.Chunk
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
		if err := server.DatastoreService().SaveDataset(uuid); err != nil {
			dvid.Log(dvid.Normal, "Could not save READY state to data '%s', uuid %s: %s",
				d.DataName(), uuid, err.Error())
		}
	}()

	err = db.ProcessRange(startKey, endKey, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		// Get label associated with this sparse volume.
		dataKey := chunk.K.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%numSurfCalculators] <- chunk
	})
	if err != nil {
		dvid.Log(dvid.Normal, "Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	for i := 0; i < numSurfCalculators; i++ {
		surfaceCh[i] <- nil
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Finished reading all RLEs for labels '%s'", d.DataName())
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

	// Get the spatial index associated with this chunk.
	dataKey := chunk.K.(*datastore.DataKey)
	zyx := dataKey.Index.(*dvid.IndexZYX)
	zyxBytes := zyx.Bytes()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}

	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Log(dvid.Normal, "Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := zyx.MinPoint(op.source.BlockSize()).(dvid.Point3d)
	lastPt := zyx.MaxPoint(op.source.BlockSize()).(dvid.Point3d)

	var curStart dvid.Point3d
	var voxelLabel, curLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				voxelLabel = d.Properties.ByteOrder.Uint64(blockData[start : start+8])
				start += 8

				// If we hit background or have switched label, save old run and start new one.
				if voxelLabel == 0 || voxelLabel != curLabel {
					// Save old run
					if curRun > 0 {
						labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					}
					// Start new one if not zero label.
					if voxelLabel != 0 {
						curStart = dvid.Point3d{x, y, z}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = voxelLabel
				} else {
					curRun++
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
	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeySpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
		return
	}
	batch = batcher.NewBatch()

	// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(KeyLabelSpatialMap)
	copy(bsIndex[9:9+dvid.IndexZYXSize], zyxBytes)
	for b, rles := range labelRLEs {
		binary.BigEndian.PutUint64(bsIndex[1:9], b)
		key := d.DataKey(op.versionID, dvid.IndexBytes(bsIndex))
		runsBytes, err := rles.MarshalBinary()
		if err != nil {
			dvid.Log(dvid.Normal, "Error encoding KeyLabelSpatialMap keys for mapped label %d: %s\n",
				b, err.Error())
			return
		}
		batch.Put(key, runsBytes)
	}
	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeyLabelSpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
	}
}
