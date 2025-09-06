package labelmap

import (
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// We assume that the export chunk sizes are equivalent to DVID chunk sizes (64x64x64).
// So we hardwire this and throw an error if the specs don't match.
const dvidChunkVoxelsPerDim = 64
const dvidChunkBitsPerDim = 6
const dvidChunkBitsTotal = 18

// FinalShardZ is a sentinel value indicating that all shard writers should be closed.
const FinalShardZ = -2

type exportSpec struct {
	ngVolume
	Directory string `json:"directory"`
	NumScales uint8  `json:"num_scales"`
}

type ngVolume struct {
	StoreType     string    `json:"@type"`     // must be "neuroglancer_multiscale_volume"
	VolumeType    string    `json:"type"`      // "image" or "segmentation"
	DataType      string    `json:"data_type"` // "uint8", ... "float32"
	NumChannels   int       `json:"num_channels"`
	Scales        []ngScale `json:"scales"`
	MeshDir       string    `json:"mesh"`               // optional if VolumeType == segmentation
	SkelDir       string    `json:"skeletons"`          // optional if VolumeType == segmentation
	LabelPropsDir string    `json:"segment_properties"` // optional if VolumeType == segmentation
}

type ngScale struct {
	ChunkSizes []dvid.Point3d `json:"chunk_sizes"`
	Encoding   string         `json:"encoding"`
	Key        string         `json:"key"`
	Resolution [3]float64     `json:"resolution"`
	Sharding   ngShard        `json:"sharding"`
	Size       dvid.Point3d   `json:"size"`

	chunkCoordBits    [3]uint8  // bits per dimension needed for chunk coordinates
	totChunkCoordBits uint8     // total bits needed for chunk coordinates
	gridSize          [3]uint32 // number of chunks in each dimension

	minishardMask uint64 // bit mask for minishard bits in hashed chunk ID
	shardMask     uint64 // bit mask for shard bits in hashed chunk ID
}

type ngShard struct {
	FormatType    string `json:"@type"` // should be "neuroglancer_uint64_sharded_v1"
	Hash          string `json:"hash"`
	MinishardBits uint8  `json:"minishard_bits"`
	PreshiftBits  uint8  `json:"preshift_bits"`
	ShardBits     uint8  `json:"shard_bits"`
	IndexEncoding string `json:"minishard_index_encoding"` // "raw" or "gzip"
	DataEncoding  string `json:"data_encoding"`            // "raw" or "gzip"
}

// initialize calculates and sets the volChunkBits field based on the first ChunkSize.
// It computes the number of bits required for each dimension and sums them.
// Also initializes numBits, maxBits and shard masks for Morton code calculation.
func (ng *ngScale) initialize() error {
	if ng.Sharding.FormatType != "neuroglancer_uint64_sharded_v1" {
		return fmt.Errorf("unsupported sharding format type: %s", ng.Sharding.FormatType)
	}
	if len(ng.ChunkSizes) == 0 {
		return fmt.Errorf("neuroglancer scale has no chunk sizes defined")
	}
	chunkSize := ng.ChunkSizes[0] // neuroglancer multiscale spec can have multiple chunk sizes, use the first one
	for dim := 0; dim < 3; dim++ {
		if chunkSize[dim] != dvidChunkVoxelsPerDim {
			return fmt.Errorf("neuroglancer chunk size %v for dim %d != DVID chunk size %d", chunkSize, dim, dvidChunkVoxelsPerDim)
		}
	}

	// Calculate bits needed for each dimension and total bits
	ng.totChunkCoordBits = 0
	ng.gridSize = [3]uint32{}
	for dim := 0; dim < 3; dim++ {
		chunksNeeded := ng.Size[dim] / chunkSize[dim]
		if ng.Size[dim]%chunkSize[dim] != 0 {
			chunksNeeded++
		}
		ng.gridSize[dim] = uint32(chunksNeeded)
		ng.chunkCoordBits[dim] = uint8(bits.Len32(uint32(chunksNeeded)))
		ng.totChunkCoordBits += ng.chunkCoordBits[dim]
	}

	// Calculate shard and minishard masks if sharding is enabled
	const on uint64 = 0xFFFFFFFFFFFFFFFF
	minishardBits := ng.Sharding.MinishardBits
	shardBits := ng.Sharding.ShardBits
	minishardOff := ((on >> minishardBits) << minishardBits)
	ng.minishardMask = ^minishardOff
	excessBits := 64 - shardBits - minishardBits
	ng.shardMask = (minishardOff << excessBits) >> excessBits

	return nil
}

// mortonCode computes the compressed Morton code for given block coordinate.
// From the reference implementation:
// https://github.com/google/neuroglancer/blob/master/src/datasource/precomputed/volume.md#unsharded-chunk-storage
func (ng *ngScale) mortonCode(blockCoord dvid.ChunkPoint3d) (morton_code uint64) {
	j := 0
	for i := 0; i < int(ng.totChunkCoordBits); i++ {
		for dim := 0; dim < 3; dim++ {
			if (1 << i) < ng.gridSize[dim] {
				morton_code |= ((uint64(blockCoord[dim]) >> i) & 1) << j
				j++
			}
		}
	}
	return
}

// computeShardID calculates the shard ID from block coordinates
func (ng *ngScale) computeShardID(blockX, blockY, blockZ int32) uint64 {
	blockCoord := dvid.ChunkPoint3d{blockX, blockY, blockZ}
	chunkID := ng.mortonCode(blockCoord)

	// Apply preshift
	hashedID := chunkID >> ng.Sharding.PreshiftBits

	// Apply hash (currently only identity is implemented)
	switch ng.Sharding.Hash {
	case "identity":
		// no-op
	case "murmurhash3_x86_128":
		// TODO: implement MurmurHash3 when needed
		dvid.Errorf("murmurhash3_x86_128 not yet implemented, using identity hash")
	}

	// Extract shard bits
	shard := (hashedID & ng.shardMask) >> ng.Sharding.MinishardBits
	return shard
}

type BlockData struct {
	ChunkCoord dvid.ChunkPoint3d
	Data       []byte
}

// Basic schema without metadata for initial writing
var blockSchema = arrow.NewSchema([]arrow.Field{
	{Name: "chunk_x", Type: arrow.PrimitiveTypes.Int32},
	{Name: "chunk_y", Type: arrow.PrimitiveTypes.Int32},
	{Name: "chunk_z", Type: arrow.PrimitiveTypes.Int32},
	{Name: "labels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{Name: "supervoxels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{Name: "dvid_compressed_block", Type: arrow.BinaryTypes.Binary},
}, nil)

type shardWriter struct {
	shardPath  string // Base path for shard files (without extension)
	f          *os.File
	ch         chan *BlockData // Channel to receive block data for this shard
	lastShardZ int32           // Last Z coordinate of this shard (for tracking shard boundaries)

	indexMap  map[string]uint64 // Map of chunk coordinates to Arrow record index
	mapping   *VCache           // Cache for mapping supervoxels to agglomerated labels
	version   dvid.VersionID    // Version ID for this export
	writer    *ipc.Writer       // Arrow IPC writer
	pool      memory.Allocator  // Arrow memory allocator
	recordNum uint64            // Counter for Arrow records written

	wg *sync.WaitGroup
	mu sync.Mutex // Mutex to protect indexMap and recordNum
}

// Start a goroutine to listen on the channel and write incoming block data to the shard file
func (w *shardWriter) start() {
	// Initialize Arrow writer
	w.pool = memory.NewGoAllocator()
	w.writer = ipc.NewWriter(w.f, ipc.WithSchema(blockSchema))

	go func() {
		defer func() {
			// Write the chunk index as a separate JSON file
			if err := w.writeChunkIndex(); err != nil {
				dvid.Errorf("Error writing chunk index for %s: %v", w.shardPath, err)
			}

			// close Arrow file after all data has been written
			fname := w.f.Name()
			if err := w.f.Close(); err != nil {
				dvid.Errorf("error closing shard file %s: %v", fname, err)
			}
		}()

		for block := range w.ch {
			// Record the chunk index before writing
			coordKey := fmt.Sprintf("%d_%d_%d", block.ChunkCoord[0], block.ChunkCoord[1], block.ChunkCoord[2])

			w.mu.Lock()
			if w.indexMap == nil {
				w.indexMap = make(map[string]uint64)
			}
			w.indexMap[coordKey] = w.recordNum
			currentRecord := w.recordNum
			w.recordNum++
			w.mu.Unlock()

			// Write the block data to the shard file in Arrow format
			if err := w.writeBlock(block); err != nil {
				dvid.Errorf("Error writing block %s (record %d) to shard file %s: %v", block.ChunkCoord, currentRecord, w.f.Name(), err)
			}
			dvid.Infof("Written block %s to shard file %s as record %d\n", block.ChunkCoord, w.f.Name(), currentRecord)
		}
		dvid.Infof("Shard writer for file %s finished after writing %d records\n", w.f.Name(), w.recordNum)
	}()
}

// writeBlock writes a single block's data to the shard file in Arrow format.
func (w *shardWriter) writeBlock(block *BlockData) error {
	// Parse the block to extract labels and supervoxels
	var b labels.Block
	if err := b.UnmarshalBinary(block.Data); err != nil {
		return fmt.Errorf("Failed to unmarshal block data for %s: %v\n", block.ChunkCoord, err)
	}

	// Get the list of agglomerated labels for the given version.
	mappedVersions := w.mapping.getMappedVersionsDist(w.version)
	aggloLabels := make([]uint64, len(b.Labels))
	for i, sv := range b.Labels {
		mapped, found := w.mapping.mapLabel(sv, mappedVersions)
		if found {
			aggloLabels[i] = mapped
		} else {
			aggloLabels[i] = sv // if no mapping, use supervoxel ID
		}
	}

	// Create Arrow record builders
	coordXBuilder := array.NewInt32Builder(w.pool)
	coordYBuilder := array.NewInt32Builder(w.pool)
	coordZBuilder := array.NewInt32Builder(w.pool)
	labelsBuilder := array.NewListBuilder(w.pool, arrow.PrimitiveTypes.Uint64)
	supervoxelsBuilder := array.NewListBuilder(w.pool, arrow.PrimitiveTypes.Uint64)
	compressedBuilder := array.NewBinaryBuilder(w.pool, arrow.BinaryTypes.Binary)

	defer func() {
		coordXBuilder.Release()
		coordYBuilder.Release()
		coordZBuilder.Release()
		labelsBuilder.Release()
		supervoxelsBuilder.Release()
		compressedBuilder.Release()
	}()

	// Append coordinate data
	coordXBuilder.Append(block.ChunkCoord[0])
	coordYBuilder.Append(block.ChunkCoord[1])
	coordZBuilder.Append(block.ChunkCoord[2])

	// Append labels list (agglomerated labels)
	labelsBuilder.Append(true)
	labelValues := labelsBuilder.ValueBuilder().(*array.Uint64Builder)
	for _, label := range aggloLabels {
		labelValues.Append(label)
	}

	// Append supervoxels list (original supervoxel IDs)
	supervoxelsBuilder.Append(true)
	supervoxelValues := supervoxelsBuilder.ValueBuilder().(*array.Uint64Builder)
	for _, sv := range b.Labels {
		supervoxelValues.Append(sv)
	}

	// Append compressed block data
	compressedData, err := b.MarshalBinary()
	if err != nil {
		return fmt.Errorf("Failed to marshal block data for %s: %v\n", block.ChunkCoord, err)
	}
	compressedBuilder.Append(compressedData)

	// Build arrays
	coordXArray := coordXBuilder.NewArray()
	coordYArray := coordYBuilder.NewArray()
	coordZArray := coordZBuilder.NewArray()
	labelsArray := labelsBuilder.NewArray()
	supervoxelsArray := supervoxelsBuilder.NewArray()
	compressedArray := compressedBuilder.NewArray()

	defer func() {
		coordXArray.Release()
		coordYArray.Release()
		coordZArray.Release()
		labelsArray.Release()
		supervoxelsArray.Release()
		compressedArray.Release()
	}()

	// Create record batch
	record := array.NewRecord(blockSchema, []arrow.Array{
		coordXArray, coordYArray, coordZArray,
		labelsArray, supervoxelsArray, compressedArray,
	}, 1)
	defer record.Release()

	// Write record to Arrow IPC file
	return w.writer.Write(record)
}

// writeChunkIndex writes the chunk index as a separate JSON file
func (w *shardWriter) writeChunkIndex() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize the chunk index to JSON
	indexJSON, err := json.MarshalIndent(w.indexMap, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal chunk index: %v", err)
	}

	// Write to separate JSON file with same base name
	jsonPath := w.shardPath + ".json"
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk index file %s: %v", jsonPath, err)
	}
	defer jsonFile.Close()

	if _, err := jsonFile.Write(indexJSON); err != nil {
		return fmt.Errorf("failed to write chunk index to %s: %v", jsonPath, err)
	}

	return nil
}

// Manage all shard files and channels
type shardHandler struct {
	path           string  // Path to the directory where shard files are stored
	shardDimVoxels []int32 // Size of each shard in Z direction at each scale
	writers        map[uint64]*shardWriter
	writeClosed    map[int32]bool // Track which Z slabs have been closed

	mapping *VCache        // Cache for mapping supervoxels to agglomerated labels
	version dvid.VersionID // Version ID for this export

	mu      sync.RWMutex
	chunkWG sync.WaitGroup
}

func (s *shardHandler) Initialize(ctx *datastore.VersionedCtx, exportSpec exportSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the label mapping cache for this version
	var err error
	if s.mapping, err = getMapping(ctx.Data(), ctx.VersionID()); err != nil {
		return fmt.Errorf("couldn't get label mapping for labelmap %q: %v", ctx.DataName(), err)
	}
	s.version = ctx.VersionID()

	// Initialize the scale information and shard Z sizes
	s.path = exportSpec.Directory
	s.shardDimVoxels = make([]int32, len(exportSpec.Scales))

	for level := range exportSpec.Scales {
		scale := &exportSpec.Scales[level]
		if err := scale.initialize(); err != nil {
			return fmt.Errorf("Aborting export-shards after initializing neuroglancer scale %d: %v", level, err)
		}

		// Determine max shard size in voxels based on bits allocated in sharding spec
		// to chunk size, preshift bits, and minishard bits.
		// Assumes no dimension extent is smaller than 1 shard.
		shardSideBits := int(dvidChunkBitsTotal+scale.Sharding.PreshiftBits+scale.Sharding.MinishardBits) / 3
		s.shardDimVoxels[level] = int32(1) << shardSideBits
		dvid.Infof("Exporting labelmap at scale %d with chunk size %v and shard voxels along axes = %d\n", level, scale.ChunkSizes[0], s.shardDimVoxels[level])
	}

	// Set capacity of map to # of shards in XY in scale 0
	hiresChunkSize := exportSpec.Scales[0].ChunkSizes[0]
	shardArea := s.shardDimVoxels[0] * s.shardDimVoxels[0]
	minNumShards := (hiresChunkSize[0] * hiresChunkSize[1]) / shardArea
	s.writers = make(map[uint64]*shardWriter, minNumShards)
	s.writeClosed = make(map[int32]bool)
	return nil
}

// getLastShardZ computes the last Z coordinate covered by the shard containing blockZ
func (s *shardHandler) getLastShardZ(scale uint8, blockZ int32) int32 {
	shardSize := s.shardDimVoxels[scale]
	shardStart := blockZ - (blockZ % shardSize)
	return shardStart + shardSize - 1
}

// shardOriginFromChunkCoord computes the voxel coordinate origin for the shard containing
// the given chunk coordinate
func (s *shardHandler) shardOriginFromChunkCoord(scale uint8, chunkCoord dvid.ChunkPoint3d) dvid.Point3d {
	shardVoxelsPerDim := s.shardDimVoxels[scale] // Assuming cubic shards. TODO: handle non-cubic shards if needed
	chunkOriginX := chunkCoord[0] * dvidChunkVoxelsPerDim
	chunkOriginY := chunkCoord[1] * dvidChunkVoxelsPerDim
	chunkOriginZ := chunkCoord[2] * dvidChunkVoxelsPerDim

	return dvid.Point3d{
		(chunkOriginX / shardVoxelsPerDim) * shardVoxelsPerDim,
		(chunkOriginY / shardVoxelsPerDim) * shardVoxelsPerDim,
		(chunkOriginZ / shardVoxelsPerDim) * shardVoxelsPerDim,
	}
}

func (s *shardHandler) getWriter(shardID uint64, scale uint8, chunkCoord dvid.ChunkPoint3d) (w *shardWriter, err error) {
	// First, check if writer already exists with read lock
	s.mu.RLock()
	if w, ok := s.writers[shardID]; ok {
		s.mu.RUnlock()
		return w, nil
	}
	s.mu.RUnlock()

	// Writer doesn't exist, need to create it with write lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting for the lock
	if w, ok := s.writers[shardID]; ok {
		return w, nil
	}

	// If writer does not exist for this shard ID, create a goroutine with its own block channel.
	w = &shardWriter{
		ch:         make(chan *BlockData, 100), // Buffered channel to hold block data for this shard
		lastShardZ: s.getLastShardZ(scale, chunkCoord[2]),
		mapping:    s.mapping,
		version:    s.version,
	}

	// Create scale directory (e.g., s0, s1, s2)
	scaleDir := fmt.Sprintf("s%d", scale)
	scalePath := path.Join(s.path, scaleDir)
	if err = os.MkdirAll(scalePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create scale directory %s: %v", scalePath, err)
	}

	// Calculate voxel coordinates that correspond to this shard ID
	shardOrigin := s.shardOriginFromChunkCoord(scale, chunkCoord)

	// Create shard filename using voxel coordinates
	baseName := fmt.Sprintf("%d_%d_%d", shardOrigin[0], shardOrigin[1], shardOrigin[2])
	w.shardPath = path.Join(scalePath, baseName)
	filename := w.shardPath + ".arrow"
	w.f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard file %s: %v", filename, err)
	}
	s.writers[shardID] = w
	w.start()
	dvid.Infof("Created shard writer %d -> shard file %s\n", shardID, filename)
	return w, nil
}

func (s *shardHandler) closeWriters(lastShardZ int32) {
	s.mu.RLock()
	_, alreadyClosed := s.writeClosed[lastShardZ]
	s.mu.RUnlock()
	if alreadyClosed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.writeClosed[lastShardZ] = true
	for shardID, w := range s.writers {
		if w.lastShardZ == lastShardZ || lastShardZ == FinalShardZ {
			fname := w.f.Name()
			close(w.ch)
			dvid.Infof("Closed shard id %d -> shard file %s\n", shardID, fname)
			delete(s.writers, shardID)
		}
	}
}

// ExportData dumps the label blocks to local shard files corresponding to neuroglancer precomputed
// volume specification. This is a goroutine that is called asynchronously so should provide feedback
// via log and no response to client.
func (d *Data) ExportData(ctx *datastore.VersionedCtx, spec exportSpec) error {
	var handler shardHandler
	if err := handler.Initialize(ctx, spec); err != nil {
		return fmt.Errorf("couldn't initialize shard handler for labelmap %q: %v", d.DataName(), err)
	}

	// Start the process to read blocks from storage and send them to the appropriate shard writers.
	go d.readBlocksZYX(ctx, &handler, spec)

	dvid.Infof("Beginning export of %d scale levels of labelmap %q data to %s ...\n", spec.NumScales, d.DataName(), spec.Directory)
	return nil
}

// goroutine to receive stream of block data over channel, decode, and send to correct shard writer
func (d *Data) chunkHandler(ch <-chan *storage.Chunk, handler *shardHandler, exportSpec exportSpec) {
	lastShardZ := handler.getLastShardZ(0, 0)

	var numBlocks uint64
	for c := range ch {
		// Block keys are in ZYX order, but we will compute the morton code of block coordinates
		// and shard id, using the latter to determine which worker to send the block to.
		scale, indexZYX, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			continue
		}
		if c.V == nil {
			dvid.Errorf("Nil data for label block %s in data %q\n", indexZYX, d.DataName())
			continue
		}

		// Get the neuroglancer scale configuration
		if len(exportSpec.Scales) <= int(scale) {
			dvid.Errorf("No neuroglancer scale %d defined for labelmap %q\n", scale, d.DataName())
			continue
		}

		// Track shard boundaries for resetting the shard writers.
		// Since blocks are read in ZYX order, if we cross a shard boundary in Z,
		// we can close out all shard writers because all previous shards are retired.
		// This uses fact that scale is stored in higher-order bits in the block key, so
		// we will see all blocks for a given scale before moving to the next scale.
		chunkX, chunkY, chunkZ := indexZYX.Unpack()
		if chunkZ > lastShardZ {
			handler.closeWriters(lastShardZ)
			lastShardZ = handler.getLastShardZ(scale, chunkZ)
		}

		// Uncompress the block data
		blockData, _, err := dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", indexZYX, d.DataName(), err)
			continue
		}

		chunkCoord := dvid.ChunkPoint3d{chunkX, chunkY, chunkZ}
		blockInfo := &BlockData{
			ChunkCoord: chunkCoord,
			Data:       blockData,
		}

		// Send the block data to the appropriate shard writer
		scaleStruct := &exportSpec.Scales[scale]
		shardID := scaleStruct.computeShardID(chunkX, chunkY, chunkZ)
		writer, err := handler.getWriter(shardID, scale, chunkCoord)
		if err != nil {
			dvid.Errorf("Failed to get writer for shard %d: %v", shardID, err)
			continue
		}
		writer.ch <- blockInfo
		numBlocks++
		if numBlocks%100000 == 0 {
			dvid.Infof("Exported %d blocks for labelmap %q. Most recent block is %s, scale %d\n",
				numBlocks, d.DataName(), chunkCoord, scale)
		}
	}
	dvid.Infof("Chunk handler finished sending %d blocks for labelmap %q\n", numBlocks, d.DataName())

}

// readBlocksZYX reads blocks in native ZYX key order from the labelmap data and sends them to
// appropriate shard writers.
func (d *Data) readBlocksZYX(ctx *datastore.VersionedCtx, handler *shardHandler, spec exportSpec) {
	// Read blocks from the labelmap data and send them to the blocks channel.
	// This is a long-running operation, so it should not block the main thread.
	timedLog := dvid.NewTimeLog()

	// Create a pool of workers to process blocks and send them to the appropriate shard writer.
	const workers = 100
	handler.chunkWG.Add(workers)
	chunkCh := make(chan *storage.Chunk, 1000) // Buffered channel to hold chunks read from storage.
	for i := 0; i < workers; i++ {
		go d.chunkHandler(chunkCh, handler, spec)
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("export from %q had error initializing store: %v", d.DataName(), err)
		return
	}
	begTKey := NewBlockTKeyByCoord(0, dvid.MinIndexZYX.ToIZYXString())
	endTKey := NewBlockTKeyByCoord(spec.NumScales-1, dvid.MaxIndexZYX.ToIZYXString())

	// Go through all labelmap blocks and send them to the workers.
	var numBlocks uint64
	err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(c *storage.Chunk) error {
		if c == nil {
			return fmt.Errorf("export: received nil chunk in count for data %q", d.DataName())
		}
		if c.V == nil {
			return nil
		}
		chunkCh <- c

		numBlocks++
		if numBlocks%100000 == 0 {
			scale, indexZYX, err := DecodeBlockTKey(c.K)
			if err != nil {
				dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			} else {
				timedLog.Infof("Read %d blocks. Recently at scale %d, chunk %s", numBlocks, scale, indexZYX)
			}
		}
		return nil
	})
	if err != nil {
		dvid.Errorf("export: problem during process range: %v\n", err)
	}
	timedLog.Infof("Finished reading labelmap %q %d blocks to exporting workers", d.DataName(), numBlocks)
	close(chunkCh)
	handler.chunkWG.Wait()
	timedLog.Infof("All chunk handlers finished for labelmap %q", d.DataName())
	handler.closeWriters(FinalShardZ)
}
