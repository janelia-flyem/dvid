package labelmap

import (
	"encoding/json"
	"fmt"
	"math"
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

type exportSpec struct {
	ngVolume
	Directory string `json:"directory"`
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

	chunkBits     uint8    // Number of bits in each block, e.g., 18 for 64x64x64 blocks
	numBits       [3]uint8 // required bits per dimension precomputed on init
	maxBits       uint8    // max of required bits across dimensions
	minishardMask uint64   // bit mask for minishard bits in hashed chunk ID
	shardMask     uint64   // bit mask for shard bits in hashed chunk ID
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

// initialize calculates and sets the chunkBits field based on the first ChunkSize.
// It computes the number of bits required for each dimension and sums them.
// Also initializes numBits, maxBits and shard masks for Morton code calculation.
func (ng *ngScale) initialize() error {
	if len(ng.ChunkSizes) == 0 {
		ng.chunkBits = 0
		return fmt.Errorf("neuroglancer scale has no chunk sizes defined")
	}

	if ng.Sharding.FormatType != "neuroglancer_uint64_sharded_v1" {
		return fmt.Errorf("unsupported sharding format type: %s", ng.Sharding.FormatType)
	}

	chunkSize := ng.ChunkSizes[0] // neuroglancer multiscale spec can have multiple chunk sizes, use the first one
	var totalBits uint8

	// Calculate bits needed for each dimension and find max
	for dim := 0; dim < 3; dim++ {
		if chunkSize[dim] <= 0 {
			ng.numBits[dim] = 0
			continue
		}

		// Calculate number of chunks in this dimension
		numChunks := float64(ng.Size[dim]) / float64(chunkSize[dim])
		dimBits := uint8(math.Ceil(math.Log2(numChunks)))

		ng.numBits[dim] = dimBits
		totalBits += dimBits

		if dimBits > ng.maxBits {
			ng.maxBits = dimBits
		}
	}

	ng.chunkBits = totalBits

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

// mortonCode computes the compressed Morton code for given block coordinates
// This is adapted from the ngprecomputed storage implementation
func (ng *ngScale) mortonCode(blockCoord dvid.ChunkPoint3d) uint64 {
	var coords [3]uint64
	for dim := uint8(0); dim < 3; dim++ {
		coords[dim] = uint64(blockCoord[dim])
	}

	var mortonCode uint64
	var outBit uint8
	for curBit := uint8(0); curBit < ng.maxBits; curBit++ {
		for dim := uint8(0); dim < 3; dim++ {
			if curBit < ng.numBits[dim] {
				// set mortonCode bit position outBit to value of coord[dim] curBit position
				bitVal := coords[dim] & 0x0000000000000001
				mortonCode |= (bitVal << outBit)
				outBit++
				coords[dim] = coords[dim] >> 1
			}
		}
	}
	return mortonCode
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
	f         *os.File
	ch        chan *BlockData   // Channel to receive block data for this shard
	indexMap  map[string]uint64 // Map of chunk coordinates to Arrow record index
	mapping   *VCache           // Cache for mapping supervoxels to agglomerated labels
	version   dvid.VersionID    // Version ID for this export
	wg        *sync.WaitGroup
	writer    *ipc.Writer      // Arrow IPC writer
	pool      memory.Allocator // Arrow memory allocator
	recordNum uint64           // Counter for Arrow records written
	mu        sync.Mutex       // Mutex to protect indexMap and recordNum
	shardPath string           // Base path for shard files (without extension)
}

// Start a goroutine to listen on the channel and write incoming block data to the shard file
func (w *shardWriter) start() {
	w.wg = &sync.WaitGroup{}
	w.wg.Add(1)

	// Initialize Arrow writer
	w.pool = memory.NewGoAllocator()
	w.writer = ipc.NewWriter(w.f, ipc.WithSchema(blockSchema))

	go func() {
		defer w.wg.Done()
		defer func() {
			if w.writer != nil {
				w.writer.Close()
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
		}
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

func (w *shardWriter) close() error {
	close(w.ch)
	w.wg.Wait()

	// Close Arrow writer first (this finalizes the Arrow IPC format)
	if w.writer != nil {
		w.writer.Close()
	}

	// Write the chunk index as a separate JSON file
	if err := w.writeChunkIndex(); err != nil {
		dvid.Errorf("Error writing chunk index for %s: %v", w.shardPath, err)
	}

	// close Arrow file after all data has been written
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("error closing shard file %s: %v", w.f.Name(), err)
	}
	return nil
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
	path       string  // Path to the directory where shard files are stored
	shardZSize []int32 // Size of each shard in Z direction at each scale
	writers    map[uint64]*shardWriter
	mapping    *VCache        // Cache for mapping supervoxels to agglomerated labels
	version    dvid.VersionID // Version ID for this export
	mu         sync.RWMutex
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
	s.shardZSize = make([]int32, len(exportSpec.Scales))

	for level := range exportSpec.Scales {
		scale := &exportSpec.Scales[level]
		if err := scale.initialize(); err != nil {
			return fmt.Errorf("Aborting export-shards after initializing neuroglancer scale %d: %v", level, err)
		}

		// Determine shard size in Z based on shard bits and morton code interleaving.
		// Assumes no dimension extent is smaller than 1 shard.
		shardSideBits := int(scale.Sharding.ShardBits+scale.Sharding.PreshiftBits+scale.Sharding.MinishardBits) / 3
		if shardSideBits < 0 {
			shardSideBits = 0
		}
		s.shardZSize[level] = int32(1) << shardSideBits
		dvid.Infof("Exporting labelmap at scale %d with chunk size %v and shard Z size %d\n", level, scale.ChunkSizes[0], s.shardZSize[level])
	}

	// Set capacity of map to # of shards in XY in scale 0
	hiresChunkSize := exportSpec.Scales[0].ChunkSizes[0]
	shardArea := s.shardZSize[0] * s.shardZSize[0]
	if shardArea == 0 {
		shardArea = 1 // Prevent division by zero
	}
	minNumShards := (hiresChunkSize[0] * hiresChunkSize[1]) / shardArea
	s.writers = make(map[uint64]*shardWriter, minNumShards)
	return nil
}

// getLastShardZ computes the last Z coordinate covered by the shard containing blockZ
func (sh *shardHandler) getLastShardZ(scale uint8, blockZ int32) int32 {
	shardSize := sh.shardZSize[scale]
	shardStart := blockZ - (blockZ % shardSize)
	return shardStart + shardSize - 1
}

func (s *shardHandler) getWriter(shardID uint64, scale uint8, chunkCoord dvid.ChunkPoint3d, ngScale *ngScale) (w *shardWriter, err error) {
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

	// If writer does not exist, create a file and channel for this shard.
	w = &shardWriter{
		ch:      make(chan *BlockData, 100), // Buffered channel to hold block data for this shard
		mapping: s.mapping,
		version: s.version,
	}

	// Calculate shard voxel origin
	shardOrigin := s.calculateShardOrigin(chunkCoord, ngScale)

	// Create scale directory (e.g., s0, s1, s2)
	scaleDir := fmt.Sprintf("s%d", scale)
	scalePath := path.Join(s.path, scaleDir)
	if err = os.MkdirAll(scalePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create scale directory %s: %v", scalePath, err)
	}

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

// calculateShardOrigin determines the voxel coordinate origin for the shard containing the given chunk
func (s *shardHandler) calculateShardOrigin(chunkCoord dvid.ChunkPoint3d, ngScale *ngScale) dvid.Point3d {
	if len(ngScale.ChunkSizes) == 0 {
		return dvid.Point3d{0, 0, 0}
	}
	chunkSize := ngScale.ChunkSizes[0]

	// Calculate shard size in chunks for each dimension
	// This uses the total shard bits distributed across 3 dimensions
	totalShardBits := ngScale.Sharding.ShardBits + ngScale.Sharding.MinishardBits + ngScale.Sharding.PreshiftBits
	shardBitsPerDim := totalShardBits / 3
	shardSizeInChunks := int32(1) << shardBitsPerDim

	// Calculate which shard this chunk belongs to
	shardCoordX := chunkCoord[0] / shardSizeInChunks
	shardCoordY := chunkCoord[1] / shardSizeInChunks
	shardCoordZ := chunkCoord[2] / shardSizeInChunks

	// Convert shard coordinates to voxel coordinates (origin of the shard)
	voxelOriginX := shardCoordX * shardSizeInChunks * chunkSize[0]
	voxelOriginY := shardCoordY * shardSizeInChunks * chunkSize[1]
	voxelOriginZ := shardCoordZ * shardSizeInChunks * chunkSize[2]

	return dvid.Point3d{voxelOriginX, voxelOriginY, voxelOriginZ}
}

func (s *shardHandler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for shardID, w := range s.writers {
		fname := w.f.Name()
		if err := w.close(); err != nil {
			dvid.Errorf("Error closing shard file %s: %v", w.f.Name(), err)
		}
		dvid.Infof("Closed shard id %d -> shard file %s\n", shardID, fname)
		delete(s.writers, shardID)
	}
	return nil
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

	dvid.Infof("Beginning export of labelmap %q data to %s ...\n", d.DataName(), spec.Directory)
	return nil
}

// goroutine to receive stream of block data over channel, decode, and write to shard file
func (d *Data) chunkHandler(ch <-chan *storage.Chunk, handler *shardHandler, exportSpec exportSpec) {
	lastShardZ := handler.getLastShardZ(0, 0)
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
		scaleStruct := &exportSpec.Scales[scale]

		// Track shard boundaries for resetting the shard writers.
		// Since blocks are read in ZYX order, if we cross a shard boundary in Z,
		// we can close out all shard writers because all previous shards are retired.
		// This uses fact that scale is stored in higher-order bits in the block key, so
		// we will see all blocks for a given scale before moving to the next scale.
		chunkX, chunkY, chunkZ := indexZYX.Unpack()
		if chunkZ > lastShardZ {
			handler.close()
			lastShardZ = handler.getLastShardZ(scale, chunkZ)
			dvid.Infof("Export of %s: Now processing blocks for shard starting at Z %d (block %d)\n", d.DataName(), lastShardZ, chunkZ)
		}

		// Compute the shard ID based on the chunk coordinates
		shardID := scaleStruct.computeShardID(chunkX, chunkY, chunkZ)

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
		writer, err := handler.getWriter(shardID, scale, chunkCoord, scaleStruct)
		if err != nil {
			dvid.Errorf("Failed to get writer for shard %d: %v", shardID, err)
			continue
		}
		writer.ch <- blockInfo
	}
}

// readBlocksZYX reads blocks in native ZYX key order from the labelmap data and sends them to
// appropriate shard writers.
func (d *Data) readBlocksZYX(ctx *datastore.VersionedCtx, handler *shardHandler, spec exportSpec) {
	// Read blocks from the labelmap data and send them to the blocks channel.
	// This is a long-running operation, so it should not block the main thread.
	timedLog := dvid.NewTimeLog()

	// Create a pool of workers to process blocks and send them to the appropriate shard writer.
	const workers = 100
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
	endTKey := NewBlockTKeyByCoord(0, dvid.MaxIndexZYX.ToIZYXString())

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
		if numBlocks%10000 == 0 {
			timedLog.Infof("Read up to block %d with chunk channel at %d", numBlocks, len(chunkCh))
		}
		return nil
	})
	if err != nil {
		dvid.Errorf("export: problem during process range: %v\n", err)
	}
	close(chunkCh)

	timedLog.Infof("Finished reading labelmap %q %d blocks to exporting workers", d.DataName(), numBlocks)
}
