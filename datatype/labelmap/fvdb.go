// fVDB export functionality for labelmap datatype.
// This file implements the IndexGrid export for use with the fVDB library.

package labelmap

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/nanovdb"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// denseBlockSize is the size of a fully dense 64³ block of uint64 labels
const denseBlockSize = 64 * 64 * 64 * 8 // 2,097,152 bytes

// fvdbExportStats holds statistics collected during fVDB export.
type fvdbExportStats struct {
	// Voxel statistics
	totalVoxels int64
	minCoord    [3]int32
	maxCoord    [3]int32
	coordsSet   bool // true once we've seen at least one voxel

	// Block data statistics
	// "Storage" = data as stored (typically gzip compressed)
	// "Serialized" = after decompression but still in DVID's native block format
	//                (label dictionary + sub-block encoding)
	totalStorageBytes    int64            // compressed/stored size
	totalSerializedBytes int64            // DVID native format size (after gzip decompression)
	blockSerializedSizes []int            // serialized sizes for histogram
	compressionFormats   map[string]int   // count of blocks by compression format

	// Labels per block statistics
	labelsPerBlock []int // number of unique labels in each block's dictionary

	// Unique labels across all processed blocks
	uniqueLabels map[uint64]struct{}
}

func newFvdbExportStats() *fvdbExportStats {
	return &fvdbExportStats{
		minCoord:           [3]int32{math.MaxInt32, math.MaxInt32, math.MaxInt32},
		maxCoord:           [3]int32{math.MinInt32, math.MinInt32, math.MinInt32},
		uniqueLabels:       make(map[uint64]struct{}),
		compressionFormats: make(map[string]int),
	}
}

// grayscaleSource holds configuration for fetching grayscale data alongside segmentation.
type grayscaleSource struct {
	instanceName string                   // Name of the grayscale data instance
	store        storage.OrderedKeyValueDB // Store for grayscale data
	ctx          *datastore.VersionedCtx  // Context for grayscale data
	values       map[nanovdb.Coord]uint8  // Collected grayscale values keyed by coord
	blocksFound  int                      // Number of grayscale blocks successfully fetched
}

// newGrayscaleSource creates a grayscale source for the given instance name.
func newGrayscaleSource(uuid dvid.UUID, instanceName string) (*grayscaleSource, error) {
	// Look up the grayscale data instance
	data, err := datastore.GetDataByUUIDName(uuid, dvid.InstanceName(instanceName))
	if err != nil {
		return nil, fmt.Errorf("grayscale instance %q not found: %w", instanceName, err)
	}

	// Verify it's a uint8blk type
	typeName := data.TypeName()
	if typeName != "uint8blk" {
		return nil, fmt.Errorf("grayscale instance %q is type %q, expected uint8blk", instanceName, typeName)
	}

	// Get the store
	store, err := datastore.GetOrderedKeyValueDB(data)
	if err != nil {
		return nil, fmt.Errorf("failed to get store for grayscale instance %q: %w", instanceName, err)
	}

	// Create versioned context
	_, versionID, err := datastore.MatchingUUID(string(uuid))
	if err != nil {
		return nil, fmt.Errorf("failed to get version ID for UUID %s: %w", uuid, err)
	}
	ctx := datastore.NewVersionedCtx(data, versionID)

	return &grayscaleSource{
		instanceName: instanceName,
		store:        store,
		ctx:          ctx,
		values:       make(map[nanovdb.Coord]uint8),
	}, nil
}

// getBlock fetches a grayscale block by its coordinate string.
func (gs *grayscaleSource) getBlock(izyx dvid.IZYXString) ([]byte, error) {
	// Create the TKey for this block (same format as imageblk)
	tk := storage.NewTKey(keyImageBlock, []byte(izyx))
	data, err := gs.store.Get(gs.ctx, tk)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil // Block doesn't exist
	}
	// Decompress the block
	blockData, _, err := dvid.DeserializeData(data, true)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize grayscale block: %w", err)
	}
	gs.blocksFound++
	return blockData, nil
}

// Key class for imageblk blocks (must match imageblk/keys.go)
const keyImageBlock = 23

func (s *fvdbExportStats) updateBoundingBox(x, y, z int32) {
	if !s.coordsSet {
		s.minCoord = [3]int32{x, y, z}
		s.maxCoord = [3]int32{x, y, z}
		s.coordsSet = true
		return
	}
	if x < s.minCoord[0] {
		s.minCoord[0] = x
	}
	if y < s.minCoord[1] {
		s.minCoord[1] = y
	}
	if z < s.minCoord[2] {
		s.minCoord[2] = z
	}
	if x > s.maxCoord[0] {
		s.maxCoord[0] = x
	}
	if y > s.maxCoord[1] {
		s.maxCoord[1] = y
	}
	if z > s.maxCoord[2] {
		s.maxCoord[2] = z
	}
}

func (s *fvdbExportStats) addBlockStats(storageSize, serializedSize int, compressionFormat string) {
	s.totalStorageBytes += int64(storageSize)
	s.totalSerializedBytes += int64(serializedSize)
	s.blockSerializedSizes = append(s.blockSerializedSizes, serializedSize)
	s.compressionFormats[compressionFormat]++
}

func (s *fvdbExportStats) addBlockLabelCount(numLabels int) {
	s.labelsPerBlock = append(s.labelsPerBlock, numLabels)
}

func (s *fvdbExportStats) addLabel(label uint64) {
	s.uniqueLabels[label] = struct{}{}
}

// printSummary logs a comprehensive summary of the export statistics.
func (s *fvdbExportStats) printSummary(label uint64, outputBytes int) {
	dvid.Infof("=== fVDB Export Statistics for label %d ===\n", label)

	// Voxel statistics
	dvid.Infof("Voxels: %d total\n", s.totalVoxels)
	if s.coordsSet {
		extentX := s.maxCoord[0] - s.minCoord[0] + 1
		extentY := s.maxCoord[1] - s.minCoord[1] + 1
		extentZ := s.maxCoord[2] - s.minCoord[2] + 1
		boundingBoxVol := int64(extentX) * int64(extentY) * int64(extentZ)
		fillRatio := float64(s.totalVoxels) / float64(boundingBoxVol) * 100
		dvid.Infof("Bounding box: (%d, %d, %d) to (%d, %d, %d)\n",
			s.minCoord[0], s.minCoord[1], s.minCoord[2],
			s.maxCoord[0], s.maxCoord[1], s.maxCoord[2])
		dvid.Infof("Extent: %d x %d x %d = %d voxels (%.2f%% fill ratio)\n",
			extentX, extentY, extentZ, boundingBoxVol, fillRatio)
	}

	// Block data statistics with terminology explanation
	numBlocks := len(s.blockSerializedSizes)
	dvid.Infof("Blocks processed: %d\n", numBlocks)

	// Compression formats used
	if len(s.compressionFormats) > 0 {
		dvid.Infof("Compression formats:\n")
		for format, count := range s.compressionFormats {
			dvid.Infof("  %s: %d blocks\n", format, count)
		}
	}

	// Storage vs serialized vs dense comparison
	// Storage = as stored on disk (compressed)
	// Serialized = DVID native format (label dictionary + sub-block encoding)
	// Dense = hypothetical 64³ × uint64 per block
	totalDenseBytes := int64(numBlocks) * denseBlockSize
	dvid.Infof("Block data sizes:\n")
	dvid.Infof("  Storage (on-disk compressed): %s\n", formatBytes(s.totalStorageBytes))
	dvid.Infof("  Serialized (DVID native format): %s\n", formatBytes(s.totalSerializedBytes))
	dvid.Infof("  Dense (64³ × uint64 per block): %s\n", formatBytes(totalDenseBytes))

	if s.totalStorageBytes > 0 {
		storageToSerializedRatio := float64(s.totalSerializedBytes) / float64(s.totalStorageBytes)
		dvid.Infof("  Compression ratio (serialized/storage): %.2fx\n", storageToSerializedRatio)
	}
	if s.totalSerializedBytes > 0 {
		denseToSerializedRatio := float64(totalDenseBytes) / float64(s.totalSerializedBytes)
		dvid.Infof("  DVID efficiency (dense/serialized): %.2fx\n", denseToSerializedRatio)
	}

	// Block serialized size histogram
	if len(s.blockSerializedSizes) > 0 {
		s.printBlockSizeHistogram()
	}

	// Labels per block histogram
	if len(s.labelsPerBlock) > 0 {
		s.printLabelsPerBlockHistogram()
	}

	// Unique labels across all blocks
	dvid.Infof("Unique labels in processed blocks: %d\n", len(s.uniqueLabels))

	// Output file statistics
	dvid.Infof("Output IndexGrid: %s\n", formatBytes(int64(outputBytes)))
	if s.totalVoxels > 0 {
		bytesPerVoxel := float64(outputBytes) / float64(s.totalVoxels)
		dvid.Infof("IndexGrid efficiency: %.2f bytes/voxel\n", bytesPerVoxel)
	}

	dvid.Infof("=== End fVDB Export Statistics ===\n")
}

// printBlockSizeHistogram prints a histogram of block serialized sizes.
func (s *fvdbExportStats) printBlockSizeHistogram() {
	if len(s.blockSerializedSizes) == 0 {
		return
	}

	// Sort to find min/max/median
	sorted := make([]int, len(s.blockSerializedSizes))
	copy(sorted, s.blockSerializedSizes)
	sort.Ints(sorted)

	minSize := sorted[0]
	maxSize := sorted[len(sorted)-1]
	medianSize := sorted[len(sorted)/2]

	// Calculate average
	var sum int64
	for _, size := range sorted {
		sum += int64(size)
	}
	avgSize := float64(sum) / float64(len(sorted))

	dvid.Infof("Serialized block sizes: min=%s, max=%s, median=%s, avg=%s\n",
		formatBytes(int64(minSize)), formatBytes(int64(maxSize)),
		formatBytes(int64(medianSize)), formatBytes(int64(avgSize)))

	// Create histogram with power-of-2 buckets
	// Buckets: <1KB, 1-2KB, 2-4KB, 4-8KB, 8-16KB, 16-32KB, 32-64KB, 64-128KB, 128-256KB, 256KB+
	buckets := []struct {
		label string
		min   int
		max   int
		count int
	}{
		{"<1KB", 0, 1024, 0},
		{"1-2KB", 1024, 2048, 0},
		{"2-4KB", 2048, 4096, 0},
		{"4-8KB", 4096, 8192, 0},
		{"8-16KB", 8192, 16384, 0},
		{"16-32KB", 16384, 32768, 0},
		{"32-64KB", 32768, 65536, 0},
		{"64-128KB", 65536, 131072, 0},
		{"128-256KB", 131072, 262144, 0},
		{"256KB+", 262144, math.MaxInt32, 0},
	}

	for _, size := range s.blockSerializedSizes {
		for i := range buckets {
			if size >= buckets[i].min && size < buckets[i].max {
				buckets[i].count++
				break
			}
		}
	}

	// Print non-empty buckets
	dvid.Infof("Serialized block size distribution:\n")
	for _, b := range buckets {
		if b.count > 0 {
			pct := float64(b.count) / float64(len(s.blockSerializedSizes)) * 100
			dvid.Infof("  %s: %d blocks (%.1f%%)\n", b.label, b.count, pct)
		}
	}
}

// printLabelsPerBlockHistogram prints a histogram of labels per block.
func (s *fvdbExportStats) printLabelsPerBlockHistogram() {
	if len(s.labelsPerBlock) == 0 {
		return
	}

	// Sort to find min/max/median
	sorted := make([]int, len(s.labelsPerBlock))
	copy(sorted, s.labelsPerBlock)
	sort.Ints(sorted)

	minLabels := sorted[0]
	maxLabels := sorted[len(sorted)-1]
	medianLabels := sorted[len(sorted)/2]

	// Calculate average
	var sum int64
	for _, count := range sorted {
		sum += int64(count)
	}
	avgLabels := float64(sum) / float64(len(sorted))

	dvid.Infof("Labels per block: min=%d, max=%d, median=%d, avg=%.1f\n",
		minLabels, maxLabels, medianLabels, avgLabels)

	// Create histogram buckets: 1, 2-5, 6-10, 11-20, 21-50, 51-100, 100+
	buckets := []struct {
		label string
		min   int
		max   int
		count int
	}{
		{"1", 1, 2, 0},
		{"2-5", 2, 6, 0},
		{"6-10", 6, 11, 0},
		{"11-20", 11, 21, 0},
		{"21-50", 21, 51, 0},
		{"51-100", 51, 101, 0},
		{"100+", 101, math.MaxInt32, 0},
	}

	for _, count := range s.labelsPerBlock {
		for i := range buckets {
			if count >= buckets[i].min && count < buckets[i].max {
				buckets[i].count++
				break
			}
		}
	}

	// Print non-empty buckets
	dvid.Infof("Labels per block distribution:\n")
	for _, b := range buckets {
		if b.count > 0 {
			pct := float64(b.count) / float64(len(s.labelsPerBlock)) * 100
			dvid.Infof("  %s labels: %d blocks (%.1f%%)\n", b.label, b.count, pct)
		}
	}
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// handleFVDB handles the fvdb endpoint for exporting label topology as NanoVDB IndexGrid.
//
// GET <api URL>/node/<UUID>/<data name>/fvdb/<label>
//
// Query parameters:
//   - supervoxels=true: interpret label as supervoxel ID
//   - scale=N: use scale level N (default 0)
//   - name=string: grid name in the output file (default: "label_<id>")
//
// Returns: application/octet-stream with NanoVDB binary data
func (d *Data) handleFVDB(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/fvdb/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label ID to follow 'fvdb' command")
		return
	}

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "fvdb endpoint only supports GET requests")
		return
	}

	// Parse label ID
	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, "invalid label ID: %v", err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be exported")
		return
	}

	// Parse query parameters
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	// Get optional grid name
	gridName := queryStrings.Get("name")
	if gridName == "" {
		gridName = fmt.Sprintf("label_%d", label)
	}

	timedLog := dvid.NewTimeLog()

	// Get the label index to find which blocks contain this label
	bounds := dvid.Bounds{} // No bounds restriction for now
	labelBlockMeta, exists, err := d.constrainLabelIndex(ctx, label, scale, bounds, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if !exists {
		dvid.Infof("GET fvdb on label %d was not found.\n", label)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Export the label topology as IndexGrid (no stats or grayscale for HTTP endpoint)
	data, _, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName, nil, nil)
	if err != nil {
		server.BadRequest(w, r, "failed to export label %d: %v", label, err)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.nvdb\"", gridName))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))

	if _, err := w.Write(data); err != nil {
		dvid.Errorf("failed to write fvdb response: %v", err)
		return
	}

	timedLog.Infof("HTTP GET fvdb for label %d: exported %d bytes, %d voxels in %d blocks",
		label, len(data), countVoxelsInMeta(labelBlockMeta), len(labelBlockMeta.sortedBlocks))
}

// exportLabelToIndexGrid creates a NanoVDB IndexGrid from the label's voxels.
// If stats is non-nil, it will be populated with export statistics.
// If gs is non-nil, grayscale values will be collected for active voxels.
// Returns the IndexGrid bytes and optionally grayscale values (sorted by voxel index).
func (d *Data) exportLabelToIndexGrid(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, gridName string, stats *fvdbExportStats, gs *grayscaleSource) ([]byte, []byte, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get store: %w", err)
	}

	// Create the IndexGrid builder
	builder := nanovdb.NewIndexGridBuilder(gridName)

	// Get voxel size from data properties (default to 1.0 if not set)
	voxelSize := nanovdb.Vec3d{1.0, 1.0, 1.0}
	if len(d.Properties.VoxelSize) >= 3 {
		voxelSize = nanovdb.Vec3d{
			float64(d.Properties.VoxelSize[0]),
			float64(d.Properties.VoxelSize[1]),
			float64(d.Properties.VoxelSize[2]),
		}
	}
	builder.SetVoxelSize(voxelSize)
	builder.SetGridClass(nanovdb.GridClassIndexGrid)

	// Get block size
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		return nil, nil, fmt.Errorf("labelmap %q does not have 3D block size", d.DataName())
	}

	// Progress tracking
	totalBlocks := len(blockMeta.sortedBlocks)
	progressInterval := totalBlocks / 10 // Log every ~10%
	if progressInterval < 100 {
		progressInterval = 100 // But at least every 100 blocks
	}
	if progressInterval > 1000 {
		progressInterval = 1000 // And at most every 1000 blocks
	}
	var totalVoxels int64

	// Process each block that contains this label
	for i, izyx := range blockMeta.sortedBlocks {
		tk := NewBlockTKeyByCoord(blockMeta.scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get block %s: %w", izyx, err)
		}
		if data == nil {
			dvid.Infof("expected block %s to have data but found none", izyx)
			continue
		}

		storageSize := len(data)

		// Decompress and parse the block
		blockData, compressionFormat, err := dvid.DeserializeData(data, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to deserialize block %s: %w", izyx, err)
		}

		serializedSize := len(blockData)

		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal block %s: %w", izyx, err)
		}

		// Track block stats
		if stats != nil {
			stats.addBlockStats(storageSize, serializedSize, compressionFormat.String())
			stats.addBlockLabelCount(len(block.Labels))
		}

		// Fetch grayscale block if needed
		var grayscaleBlock []byte
		if gs != nil {
			grayscaleBlock, err = gs.getBlock(izyx)
			if err != nil {
				dvid.Errorf("failed to get grayscale block %s: %v\n", izyx, err)
				// Continue without grayscale for this block
			}
		}

		// Get block origin in voxel coordinates
		// DVID block coordinates are in units of blocks, need to convert to voxels
		blockCoord, err := izyx.IndexZYX()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse block coord %s: %w", izyx, err)
		}
		blockOriginX := int32(blockCoord[0]) * blockSize[0]
		blockOriginY := int32(blockCoord[1]) * blockSize[1]
		blockOriginZ := int32(blockCoord[2]) * blockSize[2]

		// Extract voxels belonging to this label's supervoxels from the block
		voxels := extractLabelVoxelsWithStats(&block, blockMeta.supervoxels, blockOriginX, blockOriginY, blockOriginZ, blockSize, stats, grayscaleBlock, gs)
		builder.AddVoxels(voxels)
		totalVoxels += int64(len(voxels))

		// Update stats
		if stats != nil {
			stats.totalVoxels = totalVoxels
		}

		// Log progress periodically
		if (i+1)%progressInterval == 0 || i+1 == totalBlocks {
			pct := float64(i+1) / float64(totalBlocks) * 100
			dvid.Infof("fVDB export progress: %d/%d blocks (%.1f%%), %d voxels so far\n",
				i+1, totalBlocks, pct, totalVoxels)
		}
	}

	// Build the IndexGrid
	grid := builder.Build()
	if grid == nil {
		return nil, nil, fmt.Errorf("failed to build IndexGrid (no voxels found)")
	}

	// Serialize to NanoVDB format
	writer := nanovdb.NewWriter()
	if _, err := writer.WriteIndexGrid(grid); err != nil {
		return nil, nil, fmt.Errorf("failed to serialize IndexGrid: %w", err)
	}

	// If grayscale was requested, output values in sorted voxel order
	var grayscaleData []byte
	if gs != nil {
		if gs.blocksFound == 0 {
			dvid.Errorf("Grayscale export failed: no grayscale blocks found for instance %q (checked %d block locations)\n",
				gs.instanceName, totalBlocks)
		} else if len(gs.values) == 0 {
			dvid.Errorf("Grayscale export failed: found %d grayscale blocks but no values matched active voxels\n",
				gs.blocksFound)
		} else {
			sortedVoxels := builder.GetSortedVoxels()
			grayscaleData = make([]byte, len(sortedVoxels))
			for i, coord := range sortedVoxels {
				if val, ok := gs.values[coord]; ok {
					grayscaleData[i] = val
				}
				// If not found, defaults to 0
			}
			dvid.Infof("Grayscale: found %d/%d blocks, collected %d values for %d voxels\n",
				gs.blocksFound, totalBlocks, len(gs.values), len(sortedVoxels))
		}
	}

	return writer.Bytes(), grayscaleData, nil
}

// extractLabelVoxelsWithStats extracts all voxels belonging to the given supervoxels from a block.
// Returns the voxel coordinates in absolute (not block-relative) coordinates.
// If stats is non-nil, updates bounding box and unique labels.
// If grayscaleBlock is non-nil, collects grayscale values for active voxels into gs.values.
func extractLabelVoxelsWithStats(block *labels.Block, supervoxels labels.Set, originX, originY, originZ int32, blockSize dvid.Point3d, stats *fvdbExportStats, grayscaleBlock []byte, gs *grayscaleSource) []nanovdb.Coord {
	// Get the full label volume from the compressed block
	labelData, _ := block.MakeLabelVolume()
	if len(labelData) == 0 {
		return nil
	}

	// Iterate through all voxels in the block
	var voxels []nanovdb.Coord
	nx, ny, nz := int32(blockSize[0]), int32(blockSize[1]), int32(blockSize[2])

	labelIdx := 0
	for z := int32(0); z < nz; z++ {
		for y := int32(0); y < ny; y++ {
			for x := int32(0); x < nx; x++ {
				// Read the label at this voxel (uint64, little-endian)
				if labelIdx+8 > len(labelData) {
					break
				}
				label := uint64(labelData[labelIdx]) |
					uint64(labelData[labelIdx+1])<<8 |
					uint64(labelData[labelIdx+2])<<16 |
					uint64(labelData[labelIdx+3])<<24 |
					uint64(labelData[labelIdx+4])<<32 |
					uint64(labelData[labelIdx+5])<<40 |
					uint64(labelData[labelIdx+6])<<48 |
					uint64(labelData[labelIdx+7])<<56
				labelIdx += 8

				// Track unique labels in the block (for stats)
				if stats != nil && label != 0 {
					stats.addLabel(label)
				}

				// Check if this voxel belongs to our label
				if label != 0 {
					if _, found := supervoxels[label]; found {
						vx := originX + x
						vy := originY + y
						vz := originZ + z
						coord := nanovdb.Coord{X: vx, Y: vy, Z: vz}
						voxels = append(voxels, coord)

						// Update bounding box
						if stats != nil {
							stats.updateBoundingBox(vx, vy, vz)
						}

						// Collect grayscale value if available
						if gs != nil && grayscaleBlock != nil {
							// Grayscale block layout: uint8 values in Z, Y, X order
							gsIdx := int(z)*int(ny)*int(nx) + int(y)*int(nx) + int(x)
							if gsIdx < len(grayscaleBlock) {
								gs.values[coord] = grayscaleBlock[gsIdx]
							}
						}
					}
				}
			}
		}
	}

	return voxels
}

// countVoxelsInMeta estimates the voxel count from block metadata.
// This is an approximation since we don't have exact counts without reading blocks.
func countVoxelsInMeta(blockMeta *labelBlockMetadata) int {
	// Just return number of blocks * some factor as rough estimate
	// The actual count will come from processing
	return len(blockMeta.sortedBlocks) * 1000 // rough estimate
}

// exportLabelToFVDB exports a label's topology to a NanoVDB IndexGrid file.
// This is called asynchronously from the RPC "fvdb" command.
//
// Usage: dvid node <UUID> <data name> fvdb <label> <file path> [grayscale=<instance>]
// If grayscale is specified, a sidecar .bin file with uint8 values will be created.
func (d *Data) exportLabelToFVDB(ctx *datastore.VersionedCtx, label uint64, outPath string, grayscaleInstance string) {
	timedLog := dvid.NewTimeLog()

	if grayscaleInstance != "" {
		dvid.Infof("Starting fVDB export of label %d for data %q to %s (with grayscale from %q)\n",
			label, d.DataName(), outPath, grayscaleInstance)
	} else {
		dvid.Infof("Starting fVDB export of label %d for data %q to %s\n", label, d.DataName(), outPath)
	}

	// Get the label index to find which blocks contain this label
	bounds := dvid.Bounds{} // No bounds restriction
	scale := uint8(0)       // Use highest resolution
	isSupervoxel := false   // Interpret as agglomerated label

	labelBlockMeta, exists, err := d.constrainLabelIndex(ctx, label, scale, bounds, isSupervoxel)
	if err != nil {
		dvid.Errorf("fVDB export failed for label %d: %v\n", label, err)
		return
	}
	if !exists {
		dvid.Errorf("fVDB export failed: label %d not found\n", label)
		return
	}

	dvid.Infof("fVDB export: label %d spans %d blocks\n", label, len(labelBlockMeta.sortedBlocks))

	// Create grid name from label
	gridName := fmt.Sprintf("label_%d", label)

	// Create stats collector for RPC command
	stats := newFvdbExportStats()

	// Create grayscale source if specified
	var gs *grayscaleSource
	if grayscaleInstance != "" {
		gs, err = newGrayscaleSource(ctx.VersionUUID(), grayscaleInstance)
		if err != nil {
			dvid.Errorf("fVDB export failed: %v\n", err)
			return
		}
		dvid.Infof("fVDB export: using grayscale from instance %q\n", grayscaleInstance)
	}

	// Export the label topology as IndexGrid
	data, grayscaleData, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName, stats, gs)
	if err != nil {
		dvid.Errorf("fVDB export failed for label %d: %v\n", label, err)
		return
	}

	// Write IndexGrid to file
	f, err := os.Create(outPath)
	if err != nil {
		dvid.Errorf("fVDB export failed: cannot create file %s: %v\n", outPath, err)
		return
	}
	defer f.Close()

	n, err := f.Write(data)
	if err != nil {
		dvid.Errorf("fVDB export failed: error writing to %s: %v\n", outPath, err)
		return
	}

	timedLog.Infof("fVDB export complete: label %d -> %s (%d bytes, %d blocks)",
		label, outPath, n, len(labelBlockMeta.sortedBlocks))

	// Write grayscale sidecar file if we have grayscale data
	if len(grayscaleData) > 0 {
		// Generate grayscale filename: replace .nvdb with -<instance>.bin
		ext := filepath.Ext(outPath)
		basePath := strings.TrimSuffix(outPath, ext)
		grayscalePath := fmt.Sprintf("%s-%s.bin", basePath, grayscaleInstance)

		gf, err := os.Create(grayscalePath)
		if err != nil {
			dvid.Errorf("fVDB export: failed to create grayscale file %s: %v\n", grayscalePath, err)
		} else {
			defer gf.Close()
			gn, err := gf.Write(grayscaleData)
			if err != nil {
				dvid.Errorf("fVDB export: failed to write grayscale file %s: %v\n", grayscalePath, err)
			} else {
				dvid.Infof("fVDB export: wrote grayscale sidecar %s (%d bytes, %d voxels)\n",
					grayscalePath, gn, len(grayscaleData))
			}
		}
	}

	// Print comprehensive statistics
	stats.printSummary(label, n)
}
