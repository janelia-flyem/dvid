package labelmap

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
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

// defaultArrowBatchSize is the default number of blocks per Arrow record batch.
// 1 preserves per-block random access for GCS range reads.
const defaultArrowBatchSize = 1

// numReadWorkers is the number of concurrent Badger reader goroutines per epoch.
// Each reader opens its own read-only transaction, enabling parallel I/O on NVMe/RAID.
const numReadWorkers = 8

// chunkRowWork describes a single X-strip range scan to be performed by a reader goroutine.
type chunkRowWork struct {
	begTKey storage.TKey
	endTKey storage.TKey
}

// countingWriter wraps an io.Writer and counts bytes written.
// Used to track byte offsets in the Arrow IPC stream.
type countingWriter struct {
	w     io.Writer
	count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}

// pendingBlock stores per-block metadata buffered until the batch is flushed
// and the byte offset/size are known.
type pendingBlock struct {
	x, y, z  int32
	batchIdx int
}

type exportSpec struct {
	ngVolume
	Directory   string         `json:"directory"`
	NumScales   uint8          `json:"num_scales"`
	BatchSize   int            `json:"batch_size"` // Blocks per Arrow record batch (0 or omitted = default)
	AnalyzeOnly bool           // When true, collect stats without writing files
	Done        chan struct{}   `json:"-"` // Closed when export completes; nil = fire-and-forget
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

	// Calculate bits needed for each dimension and total bits.
	// Use bits.Len32(chunksNeeded - 1) to match tensorstore's bit_width(grid_size - 1),
	// which computes bits needed for the maximum chunk index, not the count.
	ng.totChunkCoordBits = 0
	ng.gridSize = [3]uint32{}
	for dim := 0; dim < 3; dim++ {
		chunksNeeded := ng.Size[dim] / chunkSize[dim]
		if ng.Size[dim]%chunkSize[dim] != 0 {
			chunksNeeded++
		}
		ng.gridSize[dim] = uint32(chunksNeeded)
		if chunksNeeded <= 1 {
			ng.chunkCoordBits[dim] = 0
		} else {
			ng.chunkCoordBits[dim] = uint8(bits.Len32(uint32(chunksNeeded - 1)))
		}
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
// Matches tensorstore's EncodeCompressedZIndex: interleaves bits from each dimension,
// using chunkCoordBits[dim] to determine how many bits each dimension contributes.
func (ng *ngScale) mortonCode(blockCoord dvid.ChunkPoint3d) (morton_code uint64) {
	maxBit := ng.chunkCoordBits[0]
	if ng.chunkCoordBits[1] > maxBit {
		maxBit = ng.chunkCoordBits[1]
	}
	if ng.chunkCoordBits[2] > maxBit {
		maxBit = ng.chunkCoordBits[2]
	}
	j := 0
	for i := uint8(0); i < maxBit; i++ {
		for dim := 0; dim < 3; dim++ {
			if i < ng.chunkCoordBits[dim] {
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

// log2Histogram counts values in power-of-2 buckets.
// Bucket i holds count of values where bits.Len64(v) == i.
// Bucket 0: v==0, bucket 1: v==1, bucket 2: v in [2,3], bucket 3: v in [4,7], etc.
type log2Histogram struct {
	buckets [65]uint64 // bits.Len64 returns 0..64
}

func (h *log2Histogram) add(v uint64) {
	h.buckets[bits.Len64(v)]++
}

func (h *log2Histogram) merge(other *log2Histogram) {
	for i := range h.buckets {
		h.buckets[i] += other.buckets[i]
	}
}

// formatByteHistogram writes a histogram where values represent byte sizes.
func (h *log2Histogram) formatByteHistogram(w *bufio.Writer) {
	for i := range h.buckets {
		if h.buckets[i] == 0 {
			continue
		}
		var lo, hi uint64
		if i == 0 {
			lo, hi = 0, 0
		} else {
			lo = 1 << (i - 1)
			hi = (1 << i) - 1
		}
		fmt.Fprintf(w, "      %s - %s:  %s\n", humanBytes(lo), humanBytes(hi), commaUint64(h.buckets[i]))
	}
}

// formatCountHistogram writes a histogram where values represent counts (not bytes).
func (h *log2Histogram) formatCountHistogram(w *bufio.Writer) {
	for i := range h.buckets {
		if h.buckets[i] == 0 {
			continue
		}
		var label string
		if i == 0 {
			label = "0"
		} else if i == 1 {
			label = "1"
		} else {
			lo := uint64(1) << (i - 1)
			hi := (uint64(1) << i) - 1
			label = fmt.Sprintf("%s - %s", commaUint64(lo), commaUint64(hi))
		}
		fmt.Fprintf(w, "      %-20s %s\n", label+":", commaUint64(h.buckets[i]))
	}
}

// shardReport holds per-shard statistics collected locally by each shardWriter goroutine.
type shardReport struct {
	scale             uint8
	filename          string
	records           uint64
	totalUncompressed uint64
	totalCompressed   uint64
	minBlockUncomp    uint64
	maxBlockUncomp    uint64
	minBlockComp      uint64
	maxBlockComp      uint64
	fileSize          int64
	totalSVs          uint64 // total distinct supervoxels summed across all blocks
	minBlockSVs       uint64 // min distinct supervoxels in a single block
	maxBlockSVs       uint64 // max distinct supervoxels in a single block
	totalAgglo        uint64 // total distinct agglomerated labels summed across all blocks
	minBlockAgglo     uint64 // min distinct agglomerated labels in a single block
	maxBlockAgglo     uint64 // max distinct agglomerated labels in a single block
	histUncomp        log2Histogram
	histComp          log2Histogram
	histSVs           log2Histogram
	histAgglo         log2Histogram
}

// exportMetrics collects metrics from all shard writers and writes an export.log summary.
type exportMetrics struct {
	startTime   time.Time
	dataName    string
	uuid        string
	directory   string
	numScales   uint8
	scales      []ngScale
	analyzeOnly bool

	mu      sync.Mutex
	reports []shardReport
}

func (m *exportMetrics) reportShard(r shardReport) {
	m.mu.Lock()
	m.reports = append(m.reports, r)
	m.mu.Unlock()
}

func (m *exportMetrics) writeLog() {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime)

	logName := "export.log"
	reportTitle := "DVID Export-Shards Report"
	if m.analyzeOnly {
		logName = "analyze.log"
		reportTitle = "DVID Analyze-Shards Report"
	}
	logPath := path.Join(m.directory, logName)
	f, err := os.Create(logPath)
	if err != nil {
		dvid.Errorf("Failed to create export log %s: %v", logPath, err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "=====================================\n")
	fmt.Fprintf(w, "%s\n", reportTitle)
	fmt.Fprintf(w, "=====================================\n")
	fmt.Fprintf(w, "Data:        %s\n", m.dataName)
	fmt.Fprintf(w, "UUID:        %s\n", m.uuid)
	fmt.Fprintf(w, "Directory:   %s\n", m.directory)
	fmt.Fprintf(w, "Start:       %s\n", m.startTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "End:         %s\n", endTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Duration:    %s\n", duration.Round(time.Second))

	// Aggregate per-scale stats
	type scaleStats struct {
		chunks          uint64
		shards          uint64
		totalUncomp     uint64
		totalComp       uint64
		minBlockUncomp  uint64
		maxBlockUncomp  uint64
		minBlockComp    uint64
		maxBlockComp    uint64
		minShardRecords uint64
		maxShardRecords uint64
		totalFileSize   int64
		minFileSize     int64
		maxFileSize     int64
		totalSVs        uint64
		minBlockSVs     uint64
		maxBlockSVs     uint64
		totalAgglo      uint64
		minBlockAgglo   uint64
		maxBlockAgglo   uint64
		histUncomp      log2Histogram
		histComp        log2Histogram
		histSVs         log2Histogram
		histAgglo       log2Histogram
	}
	perScale := make(map[uint8]*scaleStats)
	for _, r := range m.reports {
		s, ok := perScale[r.scale]
		if !ok {
			s = &scaleStats{
				minBlockUncomp:  math.MaxUint64,
				minBlockComp:    math.MaxUint64,
				minShardRecords: math.MaxUint64,
				minFileSize:     math.MaxInt64,
				minBlockSVs:     math.MaxUint64,
				minBlockAgglo:   math.MaxUint64,
			}
			perScale[r.scale] = s
		}
		s.chunks += r.records
		s.shards++
		s.totalUncomp += r.totalUncompressed
		s.totalComp += r.totalCompressed
		s.totalSVs += r.totalSVs
		s.totalAgglo += r.totalAgglo
		s.histUncomp.merge(&r.histUncomp)
		s.histComp.merge(&r.histComp)
		s.histSVs.merge(&r.histSVs)
		s.histAgglo.merge(&r.histAgglo)
		if r.records > 0 {
			if r.minBlockUncomp < s.minBlockUncomp {
				s.minBlockUncomp = r.minBlockUncomp
			}
			if r.maxBlockUncomp > s.maxBlockUncomp {
				s.maxBlockUncomp = r.maxBlockUncomp
			}
			if r.minBlockComp < s.minBlockComp {
				s.minBlockComp = r.minBlockComp
			}
			if r.maxBlockComp > s.maxBlockComp {
				s.maxBlockComp = r.maxBlockComp
			}
			if r.records < s.minShardRecords {
				s.minShardRecords = r.records
			}
			if r.records > s.maxShardRecords {
				s.maxShardRecords = r.records
			}
			if r.minBlockSVs < s.minBlockSVs {
				s.minBlockSVs = r.minBlockSVs
			}
			if r.maxBlockSVs > s.maxBlockSVs {
				s.maxBlockSVs = r.maxBlockSVs
			}
			if r.minBlockAgglo < s.minBlockAgglo {
				s.minBlockAgglo = r.minBlockAgglo
			}
			if r.maxBlockAgglo > s.maxBlockAgglo {
				s.maxBlockAgglo = r.maxBlockAgglo
			}
		}
		s.totalFileSize += r.fileSize
		if r.fileSize < s.minFileSize {
			s.minFileSize = r.fileSize
		}
		if r.fileSize > s.maxFileSize {
			s.maxFileSize = r.fileSize
		}
	}

	var totalChunks, totalShards, totalUncomp, totalComp uint64
	var totalFileSize int64

	for scale := uint8(0); scale < m.numScales; scale++ {
		s, ok := perScale[scale]
		if !ok {
			continue
		}
		totalChunks += s.chunks
		totalShards += s.shards
		totalUncomp += s.totalUncomp
		totalComp += s.totalComp
		totalFileSize += s.totalFileSize

		fmt.Fprintf(w, "\n--- Scale %d ---\n", scale)
		if int(scale) < len(m.scales) {
			sc := m.scales[scale]
			fmt.Fprintf(w, "  Volume:              %s x %s x %s voxels\n",
				commaInt32(sc.Size[0]), commaInt32(sc.Size[1]), commaInt32(sc.Size[2]))
			if len(sc.ChunkSizes) > 0 {
				cs := sc.ChunkSizes[0]
				fmt.Fprintf(w, "  Chunk size:          %d x %d x %d voxels\n", cs[0], cs[1], cs[2])
			}
		}
		fmt.Fprintf(w, "  Chunks exported:     %s\n", commaUint64(s.chunks))
		fmt.Fprintf(w, "  Shard files:         %s\n", commaUint64(s.shards))

		fmt.Fprintf(w, "\n  Block sizes (uncompressed):\n")
		fmt.Fprintf(w, "    Total:   %s\n", humanBytes(s.totalUncomp))
		if s.chunks > 0 {
			fmt.Fprintf(w, "    Min:     %s\n", humanBytes(s.minBlockUncomp))
			fmt.Fprintf(w, "    Max:     %s\n", humanBytes(s.maxBlockUncomp))
			fmt.Fprintf(w, "    Mean:    %s\n", humanBytes(s.totalUncomp/s.chunks))
			fmt.Fprintf(w, "    Distribution:\n")
			s.histUncomp.formatByteHistogram(w)
		}

		if !m.analyzeOnly {
			fmt.Fprintf(w, "\n  Block sizes (compressed, zstd):\n")
			fmt.Fprintf(w, "    Total:   %s\n", humanBytes(s.totalComp))
			if s.chunks > 0 {
				fmt.Fprintf(w, "    Min:     %s\n", humanBytes(s.minBlockComp))
				fmt.Fprintf(w, "    Max:     %s\n", humanBytes(s.maxBlockComp))
				fmt.Fprintf(w, "    Mean:    %s\n", humanBytes(s.totalComp/s.chunks))
				fmt.Fprintf(w, "    Distribution:\n")
				s.histComp.formatByteHistogram(w)
			}

			if s.totalComp > 0 {
				fmt.Fprintf(w, "\n  Compression ratio:   %.2fx\n", float64(s.totalUncomp)/float64(s.totalComp))
			}
		}

		if s.chunks > 0 {
			fmt.Fprintf(w, "\n  Distinct supervoxels per block:\n")
			fmt.Fprintf(w, "    Min:     %s\n", commaUint64(s.minBlockSVs))
			fmt.Fprintf(w, "    Max:     %s\n", commaUint64(s.maxBlockSVs))
			fmt.Fprintf(w, "    Mean:    %s\n", commaUint64(s.totalSVs/s.chunks))
			fmt.Fprintf(w, "    Distribution:\n")
			s.histSVs.formatCountHistogram(w)

			fmt.Fprintf(w, "\n  Distinct agglomerated labels per block:\n")
			fmt.Fprintf(w, "    Min:     %s\n", commaUint64(s.minBlockAgglo))
			fmt.Fprintf(w, "    Max:     %s\n", commaUint64(s.maxBlockAgglo))
			fmt.Fprintf(w, "    Mean:    %s\n", commaUint64(s.totalAgglo/s.chunks))
			fmt.Fprintf(w, "    Distribution:\n")
			s.histAgglo.formatCountHistogram(w)
		}

		if s.shards > 0 {
			fmt.Fprintf(w, "\n  Records per shard file:\n")
			fmt.Fprintf(w, "    Min:     %s\n", commaUint64(s.minShardRecords))
			fmt.Fprintf(w, "    Max:     %s\n", commaUint64(s.maxShardRecords))
			fmt.Fprintf(w, "    Mean:    %s\n", commaUint64(s.chunks/s.shards))

			if !m.analyzeOnly {
				fmt.Fprintf(w, "\n  Shard file sizes (arrow):\n")
				fmt.Fprintf(w, "    Total:   %s\n", humanBytes(uint64(s.totalFileSize)))
				fmt.Fprintf(w, "    Min:     %s\n", humanBytes(uint64(s.minFileSize)))
				fmt.Fprintf(w, "    Max:     %s\n", humanBytes(uint64(s.maxFileSize)))
				fmt.Fprintf(w, "    Mean:    %s\n", humanBytes(uint64(s.totalFileSize)/s.shards))
			}
		}
	}

	fmt.Fprintf(w, "\n--- Totals ---\n")
	fmt.Fprintf(w, "  Chunks:        %s\n", commaUint64(totalChunks))
	fmt.Fprintf(w, "  Shard files:   %s\n", commaUint64(totalShards))
	fmt.Fprintf(w, "  Uncompressed:  %s\n", humanBytes(totalUncomp))
	if !m.analyzeOnly && totalComp > 0 {
		fmt.Fprintf(w, "  Compressed:    %s  (%.2fx ratio)\n", humanBytes(totalComp), float64(totalUncomp)/float64(totalComp))
	}
	if !m.analyzeOnly {
		fmt.Fprintf(w, "  File sizes:    %s\n", humanBytes(uint64(totalFileSize)))
	}
	secs := duration.Seconds()
	if secs > 0 && totalChunks > 0 {
		fmt.Fprintf(w, "  Throughput:    %s chunks/sec  (%s/sec uncompressed)\n",
			commaUint64(uint64(float64(totalChunks)/secs)),
			humanBytes(uint64(float64(totalUncomp)/secs)))
	}

	dvid.Infof("Export metrics written to %s\n", logPath)
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

	builders   arrowBuilders
	writer     *ipc.Writer      // Arrow IPC writer
	pool       memory.Allocator // Arrow memory allocator
	recordNum  uint64           // Counter for blocks written (used for logging/metrics)
	batchSize  int              // Blocks per Arrow record batch
	batchCount int              // Blocks accumulated in current Arrow batch
	zenc       *zstd.Encoder    // per-writer encoder, not shared across goroutines
	zstdBuf    []byte           // Reusable zstd output buffer

	// Reusable maps for counting distinct values per block (avoids per-block allocation)
	svSet    map[uint64]struct{}
	aggloSet map[uint64]struct{}

	idxF    *os.File // CSV index file
	idxBuf  *bufio.Writer
	scratch []byte // reused per line

	// Byte offset tracking for Arrow IPC stream
	cw         *countingWriter // wraps w.f to count bytes written
	schemaSize int64           // byte size of the Arrow schema message
	batchIdx   int             // per-block index within current batch (0-based)
	pending    []pendingBlock  // blocks buffered for current batch (written to CSV on flush)

	mu sync.Mutex

	// Metrics tracking (local to this goroutine, no contention)
	scale          uint8
	metrics        *exportMetrics
	analyzeOnly    bool
	totalUncomp    uint64
	totalComp      uint64
	minBlockUncomp uint64
	maxBlockUncomp uint64
	minBlockComp   uint64
	maxBlockComp   uint64
	totalSVs       uint64
	minBlockSVs    uint64
	maxBlockSVs    uint64
	totalAgglo     uint64
	minBlockAgglo  uint64
	maxBlockAgglo  uint64
	histUncomp     log2Histogram
	histComp       log2Histogram
	histSVs        log2Histogram
	histAgglo      log2Histogram
}

// analyzeBlock collects per-block metrics without compression or file I/O.
func (w *shardWriter) analyzeBlock(block *BlockData) {
	usize := uint64(len(block.Data))
	w.totalUncomp += usize
	if usize < w.minBlockUncomp {
		w.minBlockUncomp = usize
	}
	if usize > w.maxBlockUncomp {
		w.maxBlockUncomp = usize
	}
	w.histUncomp.add(usize)

	svSet := make(map[uint64]struct{}, len(block.Supervoxels))
	for _, sv := range block.Supervoxels {
		svSet[sv] = struct{}{}
	}
	numSVs := uint64(len(svSet))
	w.totalSVs += numSVs
	if numSVs < w.minBlockSVs {
		w.minBlockSVs = numSVs
	}
	if numSVs > w.maxBlockSVs {
		w.maxBlockSVs = numSVs
	}
	w.histSVs.add(numSVs)

	aggloSet := make(map[uint64]struct{}, len(block.AggloLabels))
	for _, label := range block.AggloLabels {
		aggloSet[label] = struct{}{}
	}
	numAgglo := uint64(len(aggloSet))
	w.totalAgglo += numAgglo
	if numAgglo < w.minBlockAgglo {
		w.minBlockAgglo = numAgglo
	}
	if numAgglo > w.maxBlockAgglo {
		w.maxBlockAgglo = numAgglo
	}
	w.histAgglo.add(numAgglo)
}

// Start a goroutine to listen on the channel and write incoming block data to the shard file
func (w *shardWriter) start(wg *sync.WaitGroup) error {
	if w.analyzeOnly {
		wg.Add(1)
		go func() {
			defer func() {
				dvid.Infof("Analyze writer finished after processing %d blocks\n", w.recordNum)
				if w.metrics != nil {
					w.metrics.reportShard(shardReport{
						scale:             w.scale,
						records:           w.recordNum,
						totalUncompressed: w.totalUncomp,
						minBlockUncomp:    w.minBlockUncomp,
						maxBlockUncomp:    w.maxBlockUncomp,
						totalSVs:          w.totalSVs,
						minBlockSVs:       w.minBlockSVs,
						maxBlockSVs:       w.maxBlockSVs,
						totalAgglo:        w.totalAgglo,
						minBlockAgglo:     w.minBlockAgglo,
						maxBlockAgglo:     w.maxBlockAgglo,
						histUncomp:        w.histUncomp,
						histSVs:           w.histSVs,
						histAgglo:         w.histAgglo,
					})
				}
				wg.Done()
			}()
			for block := range w.ch {
				w.recordNum++
				w.analyzeBlock(block)
			}
		}()
		return nil
	}

	// Initialize Arrow writer with byte counting for offset tracking.
	// See ExportArchitectureAnalysis.md in labelmap package for discussion.
	base := memory.NewGoAllocator()
	tracked := memory.NewCheckedAllocator(base)
	w.pool = tracked
	w.cw = &countingWriter{w: w.f}
	w.writer = ipc.NewWriter(w.cw, ipc.WithSchema(blockSchema))
	w.schemaSize = w.cw.count
	w.pending = make([]pendingBlock, 0, w.batchSize)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fmt.Errorf("zstd encoder create failed for %s: %v", w.shardPath, err)
	}
	w.zenc = enc
	w.svSet = make(map[uint64]struct{}, 256)
	w.aggloSet = make(map[uint64]struct{}, 256)

	// --- CSV index setup ---
	idxPath := w.shardPath + ".csv"
	f, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open index file %s: %w", idxPath, err)
	}
	w.idxF = f
	w.idxBuf = bufio.NewWriterSize(f, 1<<20) // 1 MiB buffer
	w.scratch = make([]byte, 0, 128)         // reusable buffer for writing index lines

	header := fmt.Sprintf("# schema_size=%d\nx,y,z,offset,size,batch_idx\n", w.schemaSize)
	if _, err := w.idxBuf.WriteString(header); err != nil {
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

			dvid.Infof("Shard writer for file %s finished after writing %d records\n", fname, w.recordNum)

			// Report metrics for this shard (after file is closed so size is accurate)
			if w.metrics != nil {
				var fsize int64
				if fi, err := os.Stat(fname); err == nil {
					fsize = fi.Size()
				}
				w.metrics.reportShard(shardReport{
					scale:             w.scale,
					filename:          fname,
					records:           w.recordNum,
					totalUncompressed: w.totalUncomp,
					totalCompressed:   w.totalComp,
					minBlockUncomp:    w.minBlockUncomp,
					maxBlockUncomp:    w.maxBlockUncomp,
					minBlockComp:      w.minBlockComp,
					maxBlockComp:      w.maxBlockComp,
					fileSize:          fsize,
					totalSVs:          w.totalSVs,
					minBlockSVs:       w.minBlockSVs,
					maxBlockSVs:       w.maxBlockSVs,
					totalAgglo:        w.totalAgglo,
					minBlockAgglo:     w.minBlockAgglo,
					maxBlockAgglo:     w.maxBlockAgglo,
					histUncomp:        w.histUncomp,
					histComp:          w.histComp,
					histSVs:           w.histSVs,
					histAgglo:         w.histAgglo,
				})
			}

			wg.Done()
		}()

		for block := range w.ch {
			w.recordNum++

			// Buffer this block's metadata (written to CSV on batch flush when offset/size are known)
			w.pending = append(w.pending, pendingBlock{
				x: block.ChunkCoord[0], y: block.ChunkCoord[1], z: block.ChunkCoord[2],
				batchIdx: w.batchIdx,
			})
			w.batchIdx++

			// Accumulate block data into Arrow builders
			if err := w.writeBlock(block, &ab); err != nil {
				dvid.Errorf("Error writing block %s to shard file %s: %v", block.ChunkCoord, w.f.Name(), err)
			}
		}
		// Flush any remaining blocks in the batch
		if w.batchCount > 0 {
			if err := w.flushBatch(&ab); err != nil {
				dvid.Errorf("Error flushing final batch to shard file %s: %v", w.f.Name(), err)
			}
		}
	}()

	return nil
}

// writeBlock accumulates a single block's data into Arrow builders. When the batch
// reaches arrowBatchSize, it flushes the batch to the Arrow IPC file. This amortizes
// Arrow record construction and IPC framing overhead across many blocks.
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

	// zstd-compress the raw bytes, reusing the output buffer.
	if w.zenc == nil {
		return fmt.Errorf("zstd encoder not initialized for shard %s", w.shardPath)
	}
	w.zstdBuf = w.zenc.EncodeAll(block.Data, w.zstdBuf[:0])
	ab.compressedBuilder.Append(w.zstdBuf)

	// Update local metrics (no contention — single goroutine per shardWriter)
	usize := uint64(len(block.Data))
	csize := uint64(len(w.zstdBuf))
	w.totalUncomp += usize
	w.totalComp += csize
	if usize < w.minBlockUncomp {
		w.minBlockUncomp = usize
	}
	if usize > w.maxBlockUncomp {
		w.maxBlockUncomp = usize
	}
	if csize < w.minBlockComp {
		w.minBlockComp = csize
	}
	if csize > w.maxBlockComp {
		w.maxBlockComp = csize
	}
	w.histUncomp.add(usize)
	w.histComp.add(csize)

	// Count distinct supervoxels and agglomerated labels using reusable maps
	clear(w.svSet)
	for _, sv := range block.Supervoxels {
		w.svSet[sv] = struct{}{}
	}
	numSVs := uint64(len(w.svSet))
	w.totalSVs += numSVs
	if numSVs < w.minBlockSVs {
		w.minBlockSVs = numSVs
	}
	if numSVs > w.maxBlockSVs {
		w.maxBlockSVs = numSVs
	}
	w.histSVs.add(numSVs)

	clear(w.aggloSet)
	for _, label := range block.AggloLabels {
		w.aggloSet[label] = struct{}{}
	}
	numAgglo := uint64(len(w.aggloSet))
	w.totalAgglo += numAgglo
	if numAgglo < w.minBlockAgglo {
		w.minBlockAgglo = numAgglo
	}
	if numAgglo > w.maxBlockAgglo {
		w.maxBlockAgglo = numAgglo
	}
	w.histAgglo.add(numAgglo)

	w.batchCount++
	if w.batchCount >= w.batchSize {
		return w.flushBatch(ab)
	}
	return nil
}

// flushBatch builds Arrow arrays from the accumulated builders, writes a record batch
// to the IPC file, records byte offsets, writes pending rows to the offsets CSV,
// and resets batch state.
func (w *shardWriter) flushBatch(ab *arrowBuilders) error {
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

	record := array.NewRecord(blockSchema, []arrow.Array{
		coordXArray,      // chunk_x
		coordYArray,      // chunk_y
		coordZArray,      // chunk_z
		labelsArray,      // labels
		supervoxelsArray, // supervoxels
		compressedArray,  // dvid_compressed_block  (zstd)
		usizeArray,       // uncompressed_size      (bytes)
	}, int64(w.batchCount))
	defer record.Release()

	// Snapshot offset before write
	batchStart := w.cw.count
	if err := w.writer.Write(record); err != nil {
		return err
	}
	batchSize := w.cw.count - batchStart

	// Write pending rows to CSV now that offset/size are known
	for _, pb := range w.pending {
		w.writeIndexCSV(pb.x, pb.y, pb.z, batchStart, batchSize, pb.batchIdx)
	}

	// Reset batch state
	w.batchCount = 0
	w.batchIdx = 0
	w.pending = w.pending[:0]
	return nil
}

// writeIndexCSV writes a chunk index line: x,y,z,offset,size,batch_idx
func (w *shardWriter) writeIndexCSV(x, y, z int32, offset, size int64, batchIdx int) {
	b := w.scratch[:0]
	b = strconv.AppendInt(b, int64(x), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(y), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(z), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, offset, 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, size, 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(batchIdx), 10)
	b = append(b, '\n')

	if _, err := w.idxBuf.Write(b); err != nil {
		dvid.Errorf("write index CSV for %s failed: %v", w.shardPath, err)
	}
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
	batchSize      int // Blocks per Arrow record batch

	mapping        *VCache // Cache for mapping supervoxels to agglomerated labels
	mappedVersions distFromRoot

	metrics     *exportMetrics
	analyzeOnly bool

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
	sh.batchSize = spec.BatchSize
	if sh.batchSize <= 0 {
		sh.batchSize = defaultArrowBatchSize
	}
	sh.shardDimVoxels = make([]int32, len(spec.Scales))

	for level := range spec.Scales {
		scale := &spec.Scales[level]
		if err := scale.initialize(); err != nil {
			return fmt.Errorf("Aborting export-shards after initializing neuroglancer scale %d: %v", level, err)
		}

		// Compute per-dimension shard extent using the same approach as tensorstore's
		// CompressedMortonBitIterator + GetShardChunkHierarchy. Walk the compressed
		// Morton code bit-by-bit, consuming (preshift + minishard) bits in interleaved
		// X,Y,Z order, then compute the shard cell shape from the bits consumed per dim.
		nonShardBits := int(scale.Sharding.PreshiftBits + scale.Sharding.MinishardBits)
		totalZBits := int(scale.totChunkCoordBits)
		if nonShardBits > totalZBits {
			nonShardBits = totalZBits
		}
		curBit := [3]int{0, 0, 0}
		dimI := 0
		for i := 0; i < nonShardBits; i++ {
			// Skip dimensions that have exhausted their bits
			for curBit[dimI] == int(scale.chunkCoordBits[dimI]) {
				dimI = (dimI + 1) % 3
			}
			curBit[dimI]++
			dimI = (dimI + 1) % 3
		}
		// Shard extent in each dimension = min(gridSize, 1 << curBit) * chunkVoxels
		var maxShardDimVoxels int32
		for dim := 0; dim < 3; dim++ {
			shardChunks := int32(1) << curBit[dim]
			if shardChunks > int32(scale.gridSize[dim]) {
				shardChunks = int32(scale.gridSize[dim])
			}
			dimVoxels := shardChunks * dvidChunkVoxelsPerDim
			if dimVoxels > maxShardDimVoxels {
				maxShardDimVoxels = dimVoxels
			}
		}
		sh.shardDimVoxels[level] = maxShardDimVoxels
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
		ch:             make(chan *BlockData, 100), // Buffered channel to hold block data for this shard
		scale:          scale,
		batchSize:      sh.batchSize,
		metrics:        sh.metrics,
		analyzeOnly:    sh.analyzeOnly,
		minBlockUncomp: math.MaxUint64,
		minBlockComp:   math.MaxUint64,
		minBlockSVs:    math.MaxUint64,
		minBlockAgglo:  math.MaxUint64,
	}

	if !sh.analyzeOnly {
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
	}
	ep.writers[shardID] = w
	if err := w.start(&ep.writersWG); err != nil {
		return nil, fmt.Errorf("failed to start shard writer for shard %d: %v", shardID, err)
	}
	if sh.analyzeOnly {
		dvid.Infof("Created analyze writer for shard %d\n", shardID)
	} else {
		dvid.Infof("Created shard writer %d -> shard file %s\n", shardID, w.shardPath+".arrow")
	}
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

		// Parse the compressed block header directly to extract labels, avoiding the
		// UnmarshalBinary/MarshalBinary roundtrip which copies into an 8-byte-aligned
		// buffer purely for AliasByteToUint64. We only need to read label values here
		// and binary.LittleEndian.Uint64 doesn't require alignment.
		// Header layout (see compressed.go:1815-1838):
		//   [gx:u32][gy:u32][gz:u32][numLabels:u32][labels: numLabels × u64]...
		if len(blockData) < 16 {
			dvid.Errorf("block %s in data %q too short: %d bytes\n", chunkCoord, dataname, len(blockData))
			continue
		}
		numLabels := binary.LittleEndian.Uint32(blockData[12:16])
		labelsEnd := uint32(16 + numLabels*8)
		if uint32(len(blockData)) < labelsEnd {
			dvid.Errorf("block %s in data %q truncated: need %d bytes for %d labels, have %d\n",
				chunkCoord, dataname, labelsEnd, numLabels, len(blockData))
			continue
		}
		aggloLabels := make([]uint64, numLabels)
		supervoxels := make([]uint64, numLabels)
		allZero := true
		for i := uint32(0); i < numLabels; i++ {
			off := 16 + i*8
			sv := binary.LittleEndian.Uint64(blockData[off : off+8])
			supervoxels[i] = sv
			if mapped, found := sh.mapping.mapLabel(sv, sh.mappedVersions); found {
				aggloLabels[i] = mapped
			} else {
				aggloLabels[i] = sv
			}
			if aggloLabels[i] != 0 {
				allZero = false
			}
		}
		if allZero {
			continue
		}

		blockInfo := &BlockData{
			ChunkCoord:  chunkCoord,
			AggloLabels: aggloLabels,
			Supervoxels: supervoxels,
			Data:        blockData, // pass decompressed bytes directly, no copy
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
}

// ExportData dumps the label blocks to local shard files corresponding to neuroglancer precomputed
// volume specification. This is a goroutine that is called asynchronously so should provide feedback
// via log and no response to client.
func (d *Data) ExportData(ctx *datastore.VersionedCtx, spec exportSpec) error {
	versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())

	m := &exportMetrics{
		startTime:   time.Now(),
		dataName:    string(d.DataName()),
		uuid:        string(versionuuid),
		directory:   spec.Directory,
		numScales:   spec.NumScales,
		scales:      spec.Scales,
		analyzeOnly: spec.AnalyzeOnly,
	}

	var handler shardHandler
	handler.metrics = m
	handler.analyzeOnly = spec.AnalyzeOnly
	if err := handler.Initialize(ctx, spec); err != nil {
		return fmt.Errorf("couldn't initialize shard handler for labelmap %q: %v", d.DataName(), err)
	}

	// Start the process to read blocks from storage and send them to the appropriate shard writers.
	go d.readBlocksZYX(ctx, &handler, spec)

	action := "export"
	if spec.AnalyzeOnly {
		action = "analyze"
	}
	dvid.Infof("Beginning %s of %d scale levels of labelmap %q data to %s ...\n", action, spec.NumScales, d.DataName(), spec.Directory)
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

	var epochsWG sync.WaitGroup // tracks all epoch cleanup goroutines
	var numBlocks atomic.Uint64
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
			shardChunkZ := shardZ / dvidChunkVoxelsPerDim // Convert shard voxel origin to chunk coordinates.
			for shardY = 0; shardY < volumeExtents[1]; shardY += shardDimVoxels {
				// Convert from shard origin to chunk coordinates.
				shardChunkY := shardY / dvidChunkVoxelsPerDim

				// Create a pool of workers to uncompress blocks and send them to the
				// appropriate shard writer for this strip of shards.
				chunkCh, ep := sh.startShardEpoch(d.DataName(), 50)

				// Build work items for all chunk rows in this strip.
				rowCh := make(chan chunkRowWork, shardDimChunks*shardDimChunks)
				for chunkZ := shardChunkZ; chunkZ < shardChunkZ+shardDimChunks; chunkZ++ {
					for chunkY := shardChunkY; chunkY < shardChunkY+shardDimChunks; chunkY++ {
						chunkBeg := dvid.ChunkPoint3d{0, chunkY, chunkZ}
						chunkEnd := dvid.ChunkPoint3d{volChunksX, chunkY, chunkZ}
						rowCh <- chunkRowWork{
							begTKey: NewBlockTKeyByCoord(scale, chunkBeg.ToIZYXString()),
							endTKey: NewBlockTKeyByCoord(scale, chunkEnd.ToIZYXString()),
						}
					}
				}
				close(rowCh)

				// Launch concurrent readers, each pulling row work and scanning Badger.
				epochStart := time.Now()
				var readersWG sync.WaitGroup
				readersWG.Add(numReadWorkers)
				for r := 0; r < numReadWorkers; r++ {
					go func() {
						defer readersWG.Done()
						for row := range rowCh {
							if err := store.ProcessRange(ctx, row.begTKey, row.endTKey, nil, func(c *storage.Chunk) error {
								if c == nil {
									return fmt.Errorf("export: received nil chunk in count for data %q", d.DataName())
								}
								if c.V == nil {
									return nil
								}
								chunkCh <- c

								n := numBlocks.Add(1)
								if n%100000 == 0 {
									s, indexZYX, err := DecodeBlockTKey(c.K)
									if err != nil {
										dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
									} else {
										cx, cy, cz := indexZYX.Unpack()
										timedLog.Infof("Read %s blocks. Recently at scale %d, chunk (%d,%d,%d)",
											commaUint64(n), s, cx, cy, cz)
									}
								}
								return nil
							}); err != nil {
								dvid.Errorf("export: problem during process range: %v\n", err)
							}
						}
					}()
				}

				// Wait for all readers to finish, then close chunkCh to signal chunkHandlers.
				readersWG.Wait()
				readDone := time.Since(epochStart)
				close(chunkCh)

				epochsWG.Add(1)
				go func(sy, sz int32, blocksSoFar uint64, ep *epoch, epochStart time.Time, readDone time.Duration) {
					ep.chunkWG.Wait()
					chunkDone := time.Since(epochStart)

					// Now the blocks are only within the shardHandler channels, so it's OK to close
					// those channels and let them drain.
					ep.mu.Lock()
					for _, w := range ep.writers {
						close(w.ch)
					}
					ep.writers = nil
					ep.mu.Unlock()

					// Wait for all shard writers to finish before marking epoch as done.
					ep.writersWG.Wait()
					writeDone := time.Since(epochStart)

					if blocksSoFar > 0 {
						timedLog.Infof("Epoch (0, %d, %d): %s blocks — read %v, decompress+route %v, write %v",
							sy, sz, commaUint64(blocksSoFar),
							readDone.Round(time.Millisecond),
							(chunkDone - readDone).Round(time.Millisecond),
							(writeDone - chunkDone).Round(time.Millisecond))
					}
					epochsWG.Done()
				}(shardY, shardZ, numBlocks.Load(), ep, epochStart, readDone)
			}
		}
	}

	// Go through all labelmap blocks and send them to the workers.
	timedLog.Infof("Finished reading labelmap %q %s blocks to exporting workers", d.DataName(), commaUint64(numBlocks.Load()))

	// Wait for all epoch cleanup goroutines (and their shard writers) to complete.
	epochsWG.Wait()

	// Write the export metrics log.
	if sh.metrics != nil {
		sh.metrics.writeLog()
	}

	// Signal completion if a Done channel was provided.
	if spec.Done != nil {
		close(spec.Done)
	}
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

func commaInt32(n int32) string {
	return commaUint64(uint64(n))
}

func humanBytes(b uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", b)
	}
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
