package labelmap

import (
	"encoding/binary"
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
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/klauspost/compress/zstd"
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

// getLastShardZ computes the last shard boundary in Z direction
func (sh *shardHandler) getLastShardZ(blockZ int32) int32 {
	// Use scale 0 shard size for now - this might need refinement for multi-scale
	shardSize := sh.shardZSize[0]
	return (blockZ / shardSize) * shardSize
}

type BlockData struct {
	MortonCode     uint64   `json:"morton_code"`
	Labels         []uint64 `json:"labels"`
	Supervoxels    []uint64 `json:"supervoxels"`
	CompressedData []byte   `json:"compressed_data"`
}

// Arrow schema for each block record
var blockSchema = arrow.NewSchema([]arrow.Field{
	{Name: "morton_code", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "labels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{Name: "supervoxels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{Name: "dvid_compressed_block", Type: arrow.BinaryTypes.Binary},
}, nil)

// Manage all shard files and channels
type shardHandler struct {
	path       string  // Path to the directory where shard files are stored
	shardZSize []int32 // Size of each shard in Z direction at each scale
	writers    map[uint64]*shardWriter
	mu         sync.RWMutex
}

type shardWriter struct {
	f  *os.File
	ch chan *BlockData // Channel to receive block data for this shard
	mu sync.RWMutex
}

func (s *shardHandler) Initialize(exportSpec exportSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.path = exportSpec.Directory
	s.shardZSize = make([]int32, len(exportSpec.Scales))

	hiresChunkSize := exportSpec.Scales[0].ChunkSizes[0]
	for level := range exportSpec.Scales {
		scale := &exportSpec.Scales[level]
		if err := scale.initialize(); err != nil {
			dvid.Errorf("Aborting export-shards after initializing neuroglancer scale %d: %v", level, err)
			return
		}

		// Determine shard size in Z based on block bits and morton code interleaving.
		// Assumes no dimension extent is smaller than 1 shard.
		shardSideBits := int(scale.chunkBits+scale.Sharding.PreshiftBits+scale.Sharding.MinishardBits) / 3
		s.shardZSize[level] = int32(1) << shardSideBits

	}
	// Set capacity of map to # of shards in XY
	minNumShards := (hiresChunkSize[0] * hiresChunkSize[1]) / (s.shardZSize[0] * s.shardZSize[0])
	s.writers = make(map[uint64]*shardWriter, minNumShards)
}

func (s *shardHandler) getWriter(shardID uint64, scale int, chunkCoord dvid.ChunkPoint3d, ngScale *ngScale) (w *shardWriter, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ok bool
	if w, ok = s.writers[shardID]; ok {
		return w, nil
	}

	// If writer does not exist, create a file and channel for this shard.
	w = &shardWriter{
		ch: make(chan *BlockData, 100), // Buffered channel to hold block data for this shard
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
	filename := path.Join(scalePath, fmt.Sprintf("%d_%d_%d.arrow", shardOrigin[0], shardOrigin[1], shardOrigin[2]))
	w.f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard file %s: %v", filename, err)
	}
	s.writers[shardID] = w
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
		if err := w.f.Close(); err != nil {
			dvid.Errorf("Error closing shard file %s: %v", w.f.Name(), err)
		}
		delete(s.writers, shardID)
	}
	return nil
}

// ExportData dumps the label blocks to local shard files corresponding to neuroglancer precomputed
// volume specification. This is a goroutine that is called asynchronously so should provide feedback
// via log and no response to client.
func (d *Data) ExportData(ctx *datastore.VersionedCtx, spec exportSpec) {
	var handler shardHandler
	handler.Initialize(spec)

	// Start the process to read blocks from storage and send them to the appropriate shard writers.
	go d.readBlocksZYX(ctx, &handler, spec)

	dvid.Infof("Beginning export of labelmap %q data to %s ...\n", d.DataName(), spec.Directory)
}

// goroutine to receive stream of block data over channel, decode, and write to shard file
func (d *Data) chunkHandler(ch <-chan *storage.Chunk, handler *shardHandler, exportSpec exportSpec) {
	var lastShardZ int32
	for c := range ch {
		// Block keys are in ZYX order, but we will compute the morton code of block coordinates
		// and shard id, using the latter to determine which worker to send the block to.
		dvidScale, indexZYX, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			continue
		}

		// Skip non-scale-0 blocks for now - TODO: handle multiscale export
		if dvidScale != 0 {
			continue
		}

		// Get the neuroglancer scale configuration (use scale 0 for now)
		if len(exportSpec.Scales) == 0 {
			dvid.Errorf("No scales defined in export specification")
			continue
		}
		ngScale := &exportSpec.Scales[0]

		// Use DVID block coordinates directly as neuroglancer chunk coordinates
		chunkX, chunkY, chunkZ := indexZYX.Unpack()

		// Track shard boundaries for progress reporting
		if chunkZ > lastShardZ {
			lastShardZ = handler.getLastShardZ(chunkZ)
			dvid.Infof("Export of %s: Now processing blocks for shard starting at Z %d (block %d)\n", d.DataName(), lastShardZ, chunkZ)
		}

		// Compute the shard ID based on the chunk coordinates
		shardID := ngScale.computeShardID(chunkX, chunkY, chunkZ)

		// Uncompress the block data
		blockData, _, err := dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", indexZYX, d.DataName(), err)
			continue
		}

		// Parse the block to extract labels and supervoxels
		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			dvid.Errorf("Failed to unmarshal block data for %s: %v\n", indexZYX, err)
			continue
		}

		// Compute Morton code for this chunk
		chunkCoord := dvid.ChunkPoint3d{chunkX, chunkY, chunkZ}
		mortonCode := ngScale.mortonCode(chunkCoord)

		// Create BlockData structure for the shard writer
		blockInfo := &BlockData{
			MortonCode:     mortonCode,
			Labels:         block.Labels, // supervoxel labels
			Supervoxels:    block.Labels, // TODO: add agglomerated labels if available
			CompressedData: blockData,
		}

		// Send the block data to the appropriate shard writer
		writer, err := handler.getWriter(shardID, 0, chunkCoord, ngScale)
		if err != nil {
			dvid.Errorf("Failed to get writer for shard %d: %v", shardID, err)
			continue
		}

		// Send to the writer's channel for processing
		select {
		case writer.ch <- blockInfo:
			// Successfully queued
		default:
			dvid.Errorf("Writer channel for shard %d is full, skipping block", shardID)
		}
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

// transformBlockToArrow converts a label block to Arrow format with supervoxel/label indices
// and compressed block data
func (d *Data) transformBlockToArrow(blockData []byte, idx dvid.IndexZYX) (*flight.FlightData, error) {
	// Create Arrow memory allocator
	pool := memory.NewGoAllocator()

	// Unmarshal the block data into a labels.Block structure
	// TODO -- this does not fill in the Block indices.
	var block labels.Block
	if err := block.UnmarshalBinary(blockData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block data: %v", err)
	}

	// Separate the header information from the compressed block label data so
	// we can store both the supervoxel and aglomerated label indices.
	if len(block.Labels) == 0 {
		return nil, fmt.Errorf("block at %v has no labels", idx)
	}

	// Compress the block label data using ZSTD
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %v", err)
	}
	defer encoder.Close()
	compressedData := encoder.EncodeAll(blockData, nil)

	// Create Arrow schema for the block data
	// TODO: Consider using global blockSchema variable
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "block_coord_x", Type: arrow.PrimitiveTypes.Int32},
		{Name: "block_coord_y", Type: arrow.PrimitiveTypes.Int32},
		{Name: "block_coord_z", Type: arrow.PrimitiveTypes.Int32},
		{Name: "labels", Type: arrow.BinaryTypes.Binary},
		{Name: "compressed_data", Type: arrow.BinaryTypes.Binary},
	}, nil)

	// Create builders for each column
	coordXBuilder := array.NewInt32Builder(pool)
	coordYBuilder := array.NewInt32Builder(pool)
	coordZBuilder := array.NewInt32Builder(pool)
	labelsBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	compressedBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)

	defer func() {
		coordXBuilder.Release()
		coordYBuilder.Release()
		coordZBuilder.Release()
		labelsBuilder.Release()
		compressedBuilder.Release()
	}()

	// Convert labels slice to bytes for storage
	labelsBytes := make([]byte, len(block.Labels)*8)
	for i, label := range block.Labels {
		binary.LittleEndian.PutUint64(labelsBytes[i*8:(i+1)*8], label)
	}

	// Append data to builders
	coordXBuilder.Append(int32(idx[0]))
	coordYBuilder.Append(int32(idx[1]))
	coordZBuilder.Append(int32(idx[2]))
	labelsBuilder.Append(labelsBytes)
	compressedBuilder.Append(compressedData)

	// Build arrays
	coordXArray := coordXBuilder.NewArray()
	coordYArray := coordYBuilder.NewArray()
	coordZArray := coordZBuilder.NewArray()
	labelsArray := labelsBuilder.NewArray()
	compressedArray := compressedBuilder.NewArray()

	defer func() {
		coordXArray.Release()
		coordYArray.Release()
		coordZArray.Release()
		labelsArray.Release()
		compressedArray.Release()
	}()

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{
		coordXArray, coordYArray, coordZArray,
		labelsArray, compressedArray,
	}, 1)
	defer record.Release()

	// Convert to FlightData
	flightData := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{fmt.Sprintf("block_%d_%d_%d", idx[0], idx[1], idx[2])},
		},
	}

	// Serialize the record batch to FlightData
	var buf []byte
	// Note: In a full implementation, you'd properly serialize the Arrow record to IPC format
	// For now, we'll create a placeholder implementation
	flightData.DataBody = buf

	return flightData, nil
}
