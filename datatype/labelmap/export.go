package labelmap

import (
	"encoding/binary"
	"fmt"
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

type volSpec struct {
	VolumeExtents [3]uint32 `json:"volume_extents"` // [x, y, z] extents of the volume in voxels
	MinishardBits uint8     `json:"minishard_bits"`
	PreshiftBits  uint8     `json:"preshift_bits"`
	ShardBits     uint8     `json:"shard_bits"`
	BlockBits     uint8     `json:"block_bits"` // Number of bits in each block, e.g., 18 for 64x64x64 blocks

	// Computed masks
	minishardMask uint64
	shardMask     uint64
}

type exportSpec struct {
	volSpec
	Directory string `json:"directory"`
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
	path       string // Path to the directory where shard files are stored
	shardZSize uint32 // Size of each shard in Z direction
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

	// Determine shard size in Z based on block bits and morton code interleaving.
	// Assumes no dimension extent is smaller than 1 shard.
	shardSideBits := uint32(exportSpec.BlockBits+exportSpec.PreshiftBits+exportSpec.MinishardBits) / 3
	shardSideVoxels := uint32(1) << shardSideBits
	s.shardZSize = shardSideVoxels

	// Set capacity of map to # of shards in XY
	minNumShards := (exportSpec.VolumeExtents[0] * exportSpec.VolumeExtents[1]) / (shardSideVoxels * shardSideVoxels)
	s.writers = make(map[uint64]*shardWriter, minNumShards)
}

func (s *shardHandler) getWriter(shardID uint64) (w *shardWriter, err error) {
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
	filename := path.Join(s.path, fmt.Sprintf("shard_%d.arrow", shardID))
	w.f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard file %s: %v", filename, err)
	}
	s.writers[shardID] = w
	return w, nil
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
	go d.readBlocksZYX(ctx, &handler)

	dvid.Infof("Beginning export of labelmap %q data to %s ...\n", d.DataName(), spec.Directory)
}

// goroutine to receive stream of block data over channel, decode, and write to shard file
func (d *Data) chunkHandler(ch <-chan *storage.Chunk, handler *shardHandler) {
	var lastShardZ int32
	for c := range ch {
		// Block keys are in ZYX order, but we will compute the morton code of block coordinates
		// and shard id, using the latter to determine which worker to send the block to.
		scale, indexZYX, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
		}
		if scale != 0 { // We only want scale 0 blocks for the export for now. TODO: do multiscale export?
			continue
		}

		// Compute the shard ID based on the block coordinates.
		blockX, blockY, blockZ := indexZYX.Unpack()
		if blockZ > lastShardZ {
			// We've moved to a new shard in Z
			lastShardZ = handler.getLastShardZ(blockZ)
			dvid.Infof("Export of %s: Now processing blocks for shard starting at Z %d (block %d)\n", d.DataName(), lastShardZ, blockZ)

		}
		shardID := computeShardID(blockX, blockY, blockZ)

		// ...

		// Send the block data to the appropriate shard writer.
	}
}

// readBlocksZYX reads blocks in native ZYX key order from the labelmap data and sends them to
// appropriate shard writers.
func (d *Data) readBlocksZYX(ctx *datastore.VersionedCtx, handler *shardHandler) {
	// Read blocks from the labelmap data and send them to the blocks channel.
	// This is a long-running operation, so it should not block the main thread.
	timedLog := dvid.NewTimeLog()

	// Create a pool of workers to process blocks and send them to the appropriate shard writer.
	const workers = 100
	chunkCh := make(chan *storage.Chunk, 1000) // Buffered channel to hold chunks read from storage.
	for i := 0; i < workers; i++ {
		go d.chunkHandler(chunkCh, handler)
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("export from %q had error initializing store: %v", d.DataName(), err)
		return
	}
	begTKey := NewBlockTKeyByCoord(0, dvid.MinIndexZYX.ToIZYXString())
	endTKey := NewBlockTKeyByCoord(0, dvid.MaxIndexZYX.ToIZYXString())

	// Go through all labelmap blocks and send them to the workers.
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

// exportWorker processes blocks read from the labelmap data and sends them to the workers via Arrow Flight server.
func (d *Data) exportWorker(wg *sync.WaitGroup, chunkCh chan *storage.Chunk) {
	for c := range chunkCh {
		scale, idx, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			wg.Done()
			continue
		}
		if scale != 0 {
			dvid.Errorf("Counts had unexpected error: getting scale %d blocks\n", scale)
			wg.Done()
			continue
		}
		var data []byte
		data, _, err = dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", idx, d.DataName(), err)
			wg.Done()
			continue
		}

		// Transform block data into Arrow format
		arrowData, err := d.transformBlockToArrow(data, *idx)
		if err != nil {
			dvid.Errorf("Unable to transform block %s to Arrow format: %v\n", idx, err)
			wg.Done()
			continue
		}

		// Send the data to appropriate worker via Arrow Flight server
		if err := d.sendArrowDataToWorker(*idx, arrowData); err != nil {
			dvid.Errorf("Unable to send block %s to worker: %v\n", idx, err)
		}
		wg.Done()
	}
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
