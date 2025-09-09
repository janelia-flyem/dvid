package labelmap

import (
	"bufio"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/klauspost/compress/zstd"
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
	ChunkCoord  dvid.ChunkPoint3d
	AggloLabels []uint64
	Supervoxels []uint64
	Data        []byte
}

// Basic schema without metadata for initial writing
var blockSchema = arrow.NewSchema([]arrow.Field{
	{Name: "chunk_x", Type: arrow.PrimitiveTypes.Int32},
	{Name: "chunk_y", Type: arrow.PrimitiveTypes.Int32},
	{Name: "chunk_z", Type: arrow.PrimitiveTypes.Int32},
	{Name: "labels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{Name: "supervoxels", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
	{
		Name: "dvid_compressed_block",
		Type: arrow.BinaryTypes.Binary,
		Metadata: arrow.NewMetadata(
			[]string{"compression", "codec"},
			[]string{"true", "zstd"},
		),
	},
	{Name: "uncompressed_size", Type: arrow.PrimitiveTypes.Uint32},
}, nil)

type arrowBuilders struct {
	coordXBuilder      *array.Int32Builder
	coordYBuilder      *array.Int32Builder
	coordZBuilder      *array.Int32Builder
	labelsBuilder      *array.ListBuilder
	supervoxelsBuilder *array.ListBuilder
	compressedBuilder  *array.BinaryBuilder
	usizeBuilder       *array.Uint32Builder
}

func (ab *arrowBuilders) Release() {
	ab.coordXBuilder.Release()
	ab.coordYBuilder.Release()
	ab.coordZBuilder.Release()
	ab.labelsBuilder.Release()
	ab.supervoxelsBuilder.Release()
	ab.compressedBuilder.Release()
	ab.usizeBuilder.Release()
}

type shardWriter struct {
	shardPath string // Base path for shard files (without extension)
	f         *os.File
	ch        chan *BlockData // Channel to receive block data for this shard

	builders  arrowBuilders
	writer    *ipc.Writer      // Arrow IPC writer
	pool      memory.Allocator // Arrow memory allocator
	recordNum uint64           // Counter for Arrow records written
	zenc      *zstd.Encoder    // per-writer encoder, not shared across goroutines

	idxF    *os.File // CSV index file
	idxBuf  *bufio.Writer
	scratch []byte // reused per line

	mu sync.Mutex // Mutex to protect indexMap and recordNum
}

// Start a goroutine to listen on the channel and write incoming block data to the shard file
func (w *shardWriter) start(wg *sync.WaitGroup) error {
	// Initialize Arrow writer.
	// See ExportArchitectureAnalysis.md in labelmap package for discussion.
	base := memory.NewGoAllocator()
	tracked := memory.NewCheckedAllocator(base)
	w.pool = tracked
	w.writer = ipc.NewWriter(w.f, ipc.WithSchema(blockSchema))

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fmt.Errorf("zstd encoder create failed for %s: %v", w.shardPath, err)
	}
	w.zenc = enc

	// --- CSV index setup ---
	idxPath := w.shardPath + ".csv"
	f, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open index file %s: %w", idxPath, err)
	}
	w.idxF = f
	w.idxBuf = bufio.NewWriterSize(f, 1<<20) // 1 MiB buffer
	w.scratch = make([]byte, 0, 128)         // reusable buffer for writing index lines

	if _, err := w.idxBuf.WriteString("x,y,z,rec\n"); err != nil {
		return fmt.Errorf("write index header: %w", err)
	}

	wg.Add(1)
	go func() {
		// Create Arrow record builders
		var ab arrowBuilders
		ab.coordXBuilder = array.NewInt32Builder(w.pool)
		ab.coordYBuilder = array.NewInt32Builder(w.pool)
		ab.coordZBuilder = array.NewInt32Builder(w.pool)
		ab.labelsBuilder = array.NewListBuilder(w.pool, arrow.PrimitiveTypes.Uint64)
		ab.supervoxelsBuilder = array.NewListBuilder(w.pool, arrow.PrimitiveTypes.Uint64)
		ab.compressedBuilder = array.NewBinaryBuilder(w.pool, arrow.BinaryTypes.Binary)
		ab.usizeBuilder = array.NewUint32Builder(w.pool)

		defer func() {
			fname := w.f.Name()
			if err := w.writer.Close(); err != nil {
				dvid.Errorf("Error closing Arrow IPC writer for %s: %v", fname, err)
			}

			// flush/close CSV index
			if w.idxBuf != nil {
				if err := w.idxBuf.Flush(); err != nil {
					dvid.Errorf("Error flushing index %s: %v", idxPath, err)
				}
			}
			if w.idxF != nil {
				_ = w.idxF.Close()
			}

			// close Arrow file after all data has been written
			if err := w.f.Close(); err != nil {
				dvid.Errorf("Error closing shard file %s: %v", fname, err)
			}

			// Release zstd encoder resources
			if w.zenc != nil {
				if err := w.zenc.Close(); err != nil {
					dvid.Errorf("Shard writer for %s -- error closing zstd encoder for %s: %v",
						fname, w.shardPath, err)
				}
			}

			ab.Release()

			// Report any memory leaks
			if ca, ok := w.pool.(*memory.CheckedAllocator); ok {
				if ca.CurrentAlloc() != 0 {
					dvid.Errorf("Shard writer for %s -- Arrow allocator leak for %s: %d bytes still allocated",
						fname, w.shardPath, ca.CurrentAlloc())
				}
			}

			wg.Done()
		}()

		for block := range w.ch {
			// Write chunk index line to CSV file
			currentRecord := w.recordNum
			w.recordNum++
			w.writeIndexCSV(block.ChunkCoord[0], block.ChunkCoord[1], block.ChunkCoord[2], currentRecord)

			// Write the block data to the shard file in Arrow format
			if err := w.writeBlock(block, &ab); err != nil {
				dvid.Errorf("Error writing block %s (record %d) to shard file %s: %v", block.ChunkCoord, currentRecord, w.f.Name(), err)
			}
		}
		dvid.Infof("Shard writer for file %s finished after writing %d records\n", w.f.Name(), w.recordNum)
	}()

	return nil
}

// writeBlock writes a single block's data to the shard file in Arrow format.
func (w *shardWriter) writeBlock(block *BlockData, ab *arrowBuilders) error {

	// Append coordinate data
	ab.coordXBuilder.Append(block.ChunkCoord[0])
	ab.coordYBuilder.Append(block.ChunkCoord[1])
	ab.coordZBuilder.Append(block.ChunkCoord[2])

	// Append labels list (agglomerated labels)
	ab.labelsBuilder.Append(true)
	labelValues := ab.labelsBuilder.ValueBuilder().(*array.Uint64Builder)
	for _, label := range block.AggloLabels {
		labelValues.Append(label)
	}

	// Append supervoxels list (original supervoxel IDs)
	ab.supervoxelsBuilder.Append(true)
	supervoxelValues := ab.supervoxelsBuilder.ValueBuilder().(*array.Uint64Builder)
	for _, sv := range block.Supervoxels {
		supervoxelValues.Append(sv)
	}

	// Append uncompressed size (Uint32)
	if len(block.Data) > math.MaxUint32 {
		return fmt.Errorf("raw block too large: %d bytes", len(block.Data))
	}
	ab.usizeBuilder.Append(uint32(len(block.Data)))

	// zstd-compress the raw bytes.
	// EncodeAll emits a full zstd frame. Pre-size dst to ~half to reduce allocs.
	if w.zenc == nil {
		return fmt.Errorf("zstd encoder not initialized for shard %s", w.shardPath)
	}
	compressed := w.zenc.EncodeAll(block.Data, make([]byte, 0, len(block.Data)/2))
	ab.compressedBuilder.Append(compressed)

	// Build arrays
	coordXArray := ab.coordXBuilder.NewArray()
	coordYArray := ab.coordYBuilder.NewArray()
	coordZArray := ab.coordZBuilder.NewArray()
	labelsArray := ab.labelsBuilder.NewArray()
	supervoxelsArray := ab.supervoxelsBuilder.NewArray()
	compressedArray := ab.compressedBuilder.NewArray()
	usizeArray := ab.usizeBuilder.NewArray()

	defer func() {
		coordXArray.Release()
		coordYArray.Release()
		coordZArray.Release()
		labelsArray.Release()
		supervoxelsArray.Release()
		compressedArray.Release()
		usizeArray.Release()
	}()

	// Create record batch
	record := array.NewRecord(blockSchema, []arrow.Array{
		coordXArray,      // chunk_x
		coordYArray,      // chunk_y
		coordZArray,      // chunk_z
		labelsArray,      // labels
		supervoxelsArray, // supervoxels
		compressedArray,  // dvid_compressed_block  (zstd)
		usizeArray,       // uncompressed_size      (bytes)
	}, 1)
	defer record.Release()

	// Write record to Arrow IPC file
	return w.writer.Write(record)
}

// writeIndexCSV writes a chunk index for an Arrow file
func (w *shardWriter) writeIndexCSV(x, y, z int32, rec uint64) {
	b := w.scratch[:0]
	b = strconv.AppendInt(b, int64(x), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(y), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(z), 10)
	b = append(b, ',')
	b = strconv.AppendUint(b, rec, 10)
	b = append(b, '\n')

	if _, err := w.idxBuf.Write(b); err != nil {
		dvid.Errorf("write index CSV for %s failed: %v", w.shardPath, err)
	}
	// reuse capacity
	w.scratch = b[:0]
}

// epoch handles processing of each strip of shards.
type epoch struct {
	writers   map[uint64]*shardWriter
	writersWG sync.WaitGroup // allows wait for all shardWriters to finish
	chunkWG   sync.WaitGroup // allows wait for all chunkHandlers to finish
	mu        sync.RWMutex
}

// Manage all shard files and channels
type shardHandler struct {
	path           string // Path to the directory where shard files are stored
	scales         []ngScale
	shardDimVoxels []int32 // Size of each shard in Z direction at each scale
	shardsInStrip  int32

	mapping        *VCache // Cache for mapping supervoxels to agglomerated labels
	mappedVersions distFromRoot

	mu sync.RWMutex
}

func (sh *shardHandler) Initialize(ctx *datastore.VersionedCtx, spec exportSpec) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Get the label mapping cache for this version
	var err error
	if sh.mapping, err = getMapping(ctx.Data(), ctx.VersionID()); err != nil {
		return fmt.Errorf("couldn't get label mapping for labelmap %q: %v", ctx.DataName(), err)
	}
	sh.mappedVersions = sh.mapping.getMappedVersionsDist(ctx.VersionID())

	// Initialize the scale information and shard Z sizes
	sh.scales = spec.Scales
	sh.path = spec.Directory
	sh.shardDimVoxels = make([]int32, len(spec.Scales))

	for level := range spec.Scales {
		scale := &spec.Scales[level]
		if err := scale.initialize(); err != nil {
			return fmt.Errorf("Aborting export-shards after initializing neuroglancer scale %d: %v", level, err)
		}

		// Determine max shard size in voxels based on bits allocated in sharding spec
		// to chunk size, preshift bits, and minishard bits.
		// Assumes no dimension extent is smaller than 1 shard.
		shardSideBits := int(dvidChunkBitsTotal+scale.Sharding.PreshiftBits+scale.Sharding.MinishardBits) / 3
		sh.shardDimVoxels[level] = int32(1) << shardSideBits
		dvid.Infof("Exporting labelmap at scale %d with chunk size %v and shard voxels along axes = %d\n", level, scale.ChunkSizes[0], sh.shardDimVoxels[level])
	}

	// Calculate # of shards along a X strip since that's how we define an epoch
	volumeX := spec.Scales[0].Size[0]
	sh.shardsInStrip = volumeX / sh.shardDimVoxels[0]
	return nil
}

// shardOriginFromChunkCoord computes the voxel coordinate origin for the shard containing
// the given chunk coordinate
func (sh *shardHandler) shardOriginFromChunkCoord(scale uint8, chunkCoord dvid.ChunkPoint3d) dvid.Point3d {
	shardVoxelsPerDim := sh.shardDimVoxels[scale] // Assuming cubic shards. TODO: handle non-cubic shards if needed
	chunkOriginX := chunkCoord[0] * dvidChunkVoxelsPerDim
	chunkOriginY := chunkCoord[1] * dvidChunkVoxelsPerDim
	chunkOriginZ := chunkCoord[2] * dvidChunkVoxelsPerDim

	return dvid.Point3d{
		(chunkOriginX / shardVoxelsPerDim) * shardVoxelsPerDim,
		(chunkOriginY / shardVoxelsPerDim) * shardVoxelsPerDim,
		(chunkOriginZ / shardVoxelsPerDim) * shardVoxelsPerDim,
	}
}

func (sh *shardHandler) getWriter(shardID uint64, scale uint8, chunkCoord dvid.ChunkPoint3d, ep *epoch) (w *shardWriter, err error) {
	// First, check if writer already exists with read lock
	ep.mu.RLock()
	if w, ok := ep.writers[shardID]; ok {
		ep.mu.RUnlock()
		return w, nil
	}
	ep.mu.RUnlock()

	// Writer doesn't exist, need to create it with write lock
	ep.mu.Lock()
	defer ep.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting for the lock
	if w, ok := ep.writers[shardID]; ok {
		return w, nil
	}

	// If writer does not exist for this shard ID, create a goroutine with its own block channel.
	w = &shardWriter{
		ch: make(chan *BlockData, 100), // Buffered channel to hold block data for this shard
	}

	// Create scale directory (e.g., s0, s1, s2)
	scaleDir := fmt.Sprintf("s%d", scale)
	scalePath := path.Join(sh.path, scaleDir)
	if err = os.MkdirAll(scalePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create scale directory %s: %v", scalePath, err)
	}

	// Calculate voxel coordinates that correspond to this shard ID
	shardOrigin := sh.shardOriginFromChunkCoord(scale, chunkCoord)

	// Create shard filename using voxel coordinates
	baseName := fmt.Sprintf("%d_%d_%d", shardOrigin[0], shardOrigin[1], shardOrigin[2])
	w.shardPath = path.Join(scalePath, baseName)
	filename := w.shardPath + ".arrow"
	w.f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard file %s: %v", filename, err)
	}
	ep.writers[shardID] = w
	if err := w.start(&ep.writersWG); err != nil {
		return nil, fmt.Errorf("failed to start shard writer for %s: %v", filename, err)
	}
	dvid.Infof("Created shard writer %d -> shard file %s\n", shardID, filename)
	return w, nil
}

// startShardEpoch fires off parallel chunkHandler goroutines that will stop when
// the returned chunk channel is closed.
func (sh *shardHandler) startShardEpoch(dataname dvid.InstanceName, workers int) (chunkCh chan *storage.Chunk, ep *epoch) {
	ep = &epoch{writers: make(map[uint64]*shardWriter, sh.shardsInStrip)}
	ep.chunkWG.Add(workers)
	chunkCh = make(chan *storage.Chunk, 1000) // Buffered channel to hold chunks read from storage.
	for i := 0; i < workers; i++ {
		go sh.chunkHandler(dataname, chunkCh, ep)
	}
	return chunkCh, ep
}

// goroutine to receive stream of block data over channel, decode, and send to correct shard writer.
// Shards are closed after receiving a signal BlockData
// from the labelmap data and sends them to
// appropriate shard writers.
func (sh *shardHandler) chunkHandler(dataname dvid.InstanceName, ch <-chan *storage.Chunk, ep *epoch) {
	var numBlocks uint64
	for c := range ch {
		// Block keys are in ZYX order, but we will compute the morton code of block coordinates
		// and shard id, using the latter to determine which worker to send the block to.
		scale, indexZYX, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, dataname)
			continue
		}
		if c.V == nil {
			dvid.Errorf("Nil data for label block %s in data %q\n", indexZYX, dataname)
			continue
		}

		// Get the neuroglancer scale configuration
		if len(sh.scales) <= int(scale) {
			dvid.Errorf("No neuroglancer scale %d defined for labelmap %q\n", scale, dataname)
			continue
		}

		chunkX, chunkY, chunkZ := indexZYX.Unpack()
		chunkCoord := dvid.ChunkPoint3d{chunkX, chunkY, chunkZ}

		// Uncompress the block data
		blockData, _, err := dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", indexZYX, dataname, err)
			continue
		}

		// Parse the block to extract labels and supervoxels
		var b labels.Block
		if err := b.UnmarshalBinary(blockData); err != nil {
			dvid.Errorf("failed to unmarshal block data for %s: %v\n", chunkCoord, err)
		}

		// Get the list of agglomerated labels for the given version.
		aggloLabels := make([]uint64, len(b.Labels))
		supervoxels := make([]uint64, len(b.Labels))
		for i, sv := range b.Labels {
			supervoxels[i] = sv
			mapped, found := sh.mapping.mapLabel(sv, sh.mappedVersions)
			if found {
				aggloLabels[i] = mapped
			} else {
				aggloLabels[i] = sv // if no mapping, use supervoxel ID
			}
		}

		raw, err := b.MarshalBinary()
		if err != nil {
			dvid.Errorf("failed to marshal block data for %s: %v\n", chunkCoord, err)
		}

		blockInfo := &BlockData{
			ChunkCoord:  chunkCoord,
			AggloLabels: aggloLabels,
			Supervoxels: supervoxels,
			Data:        raw,
		}

		// Send the block data to the appropriate shard writer
		scaleStruct := &sh.scales[scale]
		shardID := scaleStruct.computeShardID(chunkX, chunkY, chunkZ)
		writer, err := sh.getWriter(shardID, scale, chunkCoord, ep)
		if err != nil {
			dvid.Errorf("Failed to get writer for shard %d: %v", shardID, err)
			continue
		}
		writer.ch <- blockInfo
		numBlocks++
		if numBlocks%100000 == 0 {
			dvid.Infof("Exported %d blocks for labelmap %q. Most recent block is %s, scale %d\n",
				numBlocks, dataname, chunkCoord, scale)
		}
	}
	ep.chunkWG.Done()
	dvid.Infof("Chunk handler finished sending %d blocks for labelmap %q\n", numBlocks, dataname)
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

// readBlocksZYX reads blocks from database in a way that limits the number of shards
// operated at a time thereby relieving memory pressure, while giving up perhaps a little
// speed due to more complex DB scan paths.  Blocks are retrieved, passed to chunkHandler
// goroutines that determine their shards, and then passed to the appropriate shardWriter.
func (d *Data) readBlocksZYX(ctx *datastore.VersionedCtx, sh *shardHandler, spec exportSpec) {
	timedLog := dvid.NewTimeLog()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("export from %q had error initializing store: %v", d.DataName(), err)
		return
	}

	var numBlocks uint64
	var scale uint8
	for scale = 0; scale < spec.NumScales; scale++ {
		// Iterate across shard volumes structured as long X-oriented strip of shard volumes
		// (e.g., 2048^3 voxels or 32^3 blocks of 64^3 voxels).
		shardDimVoxels := sh.shardDimVoxels[scale]
		shardDimChunks := shardDimVoxels / dvidChunkVoxelsPerDim
		volumeExtents := spec.Scales[scale].Size
		volChunksX := volumeExtents[0] / dvidChunkVoxelsPerDim

		var shardZ, shardY int32 // z and y voxel coordinate of shard origin
		for shardZ = 0; shardZ < volumeExtents[2]; shardZ += shardDimVoxels {
			for shardY = 0; shardY < volumeExtents[1]; shardY += shardDimVoxels {
				// Create a pool of workers to uncompress blocks and send them to the
				// appropriate shard writer for this strip of shards.
				chunkCh, ep := sh.startShardEpoch(d.DataName(), 50)

				// Read a bar of chunks that constitute shards along X.
				for chunkZ := shardZ; chunkZ < shardZ+shardDimChunks; chunkZ++ {
					for chunkY := shardY; chunkY < shardY+shardDimChunks; chunkY++ {
						// Setup keys for a strip of chunks across X
						chunkBeg := dvid.ChunkPoint3d{0, chunkY, chunkZ}
						chunkEnd := dvid.ChunkPoint3d{volChunksX, chunkY, chunkZ}
						begTKey := NewBlockTKeyByCoord(scale, chunkBeg.ToIZYXString())
						endTKey := NewBlockTKeyByCoord(scale, chunkEnd.ToIZYXString())

						// Scan the chunks across X and send them to the chunkHandlers.
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
									chunkX, chunkY, chunkZ := indexZYX.Unpack()
									timedLog.Infof("Read %d blocks. Recently at scale %d, chunk (%d,%d,%d)",
										numBlocks, scale, chunkX, chunkY, chunkZ)
								}
							}
							return nil
						})
						if err != nil {
							dvid.Errorf("export: problem during process range: %v\n", err)
						}
					}
				}

				// We've completed a strip of shards so shutdown this epoch for the strip of shards.
				// First, we close the channel to the pool of shardHandler.chunkHandler goroutines
				// and wait until they're drained and exited.
				close(chunkCh)
				go func(sy, sz int32, blocksSoFar uint64, ep *epoch) {
					ep.chunkWG.Wait()

					// Now the blocks are only within the shardHandler channels, so it's OK to close
					// those channels and let them drain.
					ep.mu.Lock()
					for _, w := range ep.writers {
						close(w.ch)
					}
					ep.writers = nil
					ep.mu.Unlock()

					timedLog.Infof("Completed strip of shards at (0, %d, %d): %s blocks read",
						sy, sz, commaUint64(blocksSoFar))
				}(shardY, shardZ, numBlocks, ep)
			}
		}
	}

	// Go through all labelmap blocks and send them to the workers.
	timedLog.Infof("Finished reading labelmap %q %d blocks to exporting workers", d.DataName(), commaUint64(numBlocks))
}

// utility to write large integers with commas
func commaUint64(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre == 0 {
		pre = 3
	}
	b.WriteString(s[:pre])
	for i := pre; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// startResourceMonitor starts a goroutine that periodically logs key runtime metrics.
// Currently unused, but could be helpful for debugging performance issues.
// Might also cause performance issues itself, so not enabled by default.
func startResourceMonitor(interval time.Duration) chan struct{} {
	stop := make(chan struct{})

	go func() {
		// Pick a few useful metrics
		names := []string{
			"/memory/classes/heap/objects:bytes",
			"/memory/classes/heap/free:bytes",
			"/memory/classes/metadata/other:bytes",
			"/gc/heap/objects:objects",
			"/gc/cycles/automatic:gc-cycles",
			"/sched/goroutines:goroutines",
		}
		samples := make([]metrics.Sample, len(names))
		for i, n := range names {
			samples[i].Name = n
		}

		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				metrics.Read(samples)
				// extract values
				m := func(name string) metrics.Sample {
					for _, s := range samples {
						if s.Name == name {
							return s
						}
					}
					return metrics.Sample{}
				}
				heapObjects := m("/memory/classes/heap/objects:bytes").Value.Uint64()
				heapFree := m("/memory/classes/heap/free:bytes").Value.Uint64()
				meta := m("/memory/classes/metadata/other:bytes").Value.Uint64()
				gcs := m("/gc/cycles/automatic:gc-cycles").Value.Uint64()
				gr := m("/sched/goroutines:goroutines").Value.Uint64()

				dvid.Infof("[resmon] goroutines=%d heap_objects=%d heap_free=%d metadata=%d gc_cycles=%d\n",
					gr, heapObjects, heapFree, meta, gcs)

			case <-stop:
				return
			}
		}
	}()
	return stop
}
