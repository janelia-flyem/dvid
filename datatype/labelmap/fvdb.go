// fVDB export functionality for labelmap datatype.
// This file implements the IndexGrid export for use with the fVDB library.

package labelmap

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/nanovdb"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// fvdbExportStats holds statistics collected during fVDB export.
type fvdbExportStats struct {
	// Voxel statistics
	totalVoxels int64
	minCoord    [3]int32
	maxCoord    [3]int32
	coordsSet   bool // true once we've seen at least one voxel

	// Block data statistics
	totalCompressedBytes   int64
	totalUncompressedBytes int64
	blockSizes             []int // uncompressed sizes for histogram

	// Unique labels in processed blocks
	uniqueLabels map[uint64]struct{}
}

func newFvdbExportStats() *fvdbExportStats {
	return &fvdbExportStats{
		minCoord:     [3]int32{math.MaxInt32, math.MaxInt32, math.MaxInt32},
		maxCoord:     [3]int32{math.MinInt32, math.MinInt32, math.MinInt32},
		uniqueLabels: make(map[uint64]struct{}),
	}
}

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

func (s *fvdbExportStats) addBlockStats(compressedSize, uncompressedSize int) {
	s.totalCompressedBytes += int64(compressedSize)
	s.totalUncompressedBytes += int64(uncompressedSize)
	s.blockSizes = append(s.blockSizes, uncompressedSize)
}

func (s *fvdbExportStats) addLabel(label uint64) {
	s.uniqueLabels[label] = struct{}{}
}

// printSummary logs a comprehensive summary of the export statistics.
func (s *fvdbExportStats) printSummary(label uint64, outputBytes int) {
	dvid.Infof("=== fVDB Export Statistics for label %d ===", label)

	// Voxel statistics
	dvid.Infof("Voxels: %d total", s.totalVoxels)
	if s.coordsSet {
		extentX := s.maxCoord[0] - s.minCoord[0] + 1
		extentY := s.maxCoord[1] - s.minCoord[1] + 1
		extentZ := s.maxCoord[2] - s.minCoord[2] + 1
		boundingBoxVol := int64(extentX) * int64(extentY) * int64(extentZ)
		fillRatio := float64(s.totalVoxels) / float64(boundingBoxVol) * 100
		dvid.Infof("Bounding box: (%d, %d, %d) to (%d, %d, %d)",
			s.minCoord[0], s.minCoord[1], s.minCoord[2],
			s.maxCoord[0], s.maxCoord[1], s.maxCoord[2])
		dvid.Infof("Extent: %d x %d x %d = %d voxels (%.2f%% fill ratio)",
			extentX, extentY, extentZ, boundingBoxVol, fillRatio)
	}

	// Block data statistics
	dvid.Infof("Segmentation data read: %s compressed, %s uncompressed",
		formatBytes(s.totalCompressedBytes), formatBytes(s.totalUncompressedBytes))
	if s.totalCompressedBytes > 0 {
		compressionRatio := float64(s.totalUncompressedBytes) / float64(s.totalCompressedBytes)
		dvid.Infof("Compression ratio: %.2fx", compressionRatio)
	}

	// Block size histogram
	if len(s.blockSizes) > 0 {
		s.printBlockSizeHistogram()
	}

	// Unique labels
	dvid.Infof("Unique labels in processed blocks: %d", len(s.uniqueLabels))

	// Output file statistics
	dvid.Infof("Output IndexGrid: %s", formatBytes(int64(outputBytes)))
	if s.totalVoxels > 0 {
		bytesPerVoxel := float64(outputBytes) / float64(s.totalVoxels)
		dvid.Infof("IndexGrid efficiency: %.2f bytes/voxel", bytesPerVoxel)
	}

	dvid.Infof("=== End fVDB Export Statistics ===")
}

// printBlockSizeHistogram prints a histogram of block sizes.
func (s *fvdbExportStats) printBlockSizeHistogram() {
	if len(s.blockSizes) == 0 {
		return
	}

	// Sort to find min/max/median
	sorted := make([]int, len(s.blockSizes))
	copy(sorted, s.blockSizes)
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

	dvid.Infof("Block sizes: min=%s, max=%s, median=%s, avg=%s",
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

	for _, size := range s.blockSizes {
		for i := range buckets {
			if size >= buckets[i].min && size < buckets[i].max {
				buckets[i].count++
				break
			}
		}
	}

	// Print non-empty buckets
	dvid.Infof("Block size distribution:")
	for _, b := range buckets {
		if b.count > 0 {
			pct := float64(b.count) / float64(len(s.blockSizes)) * 100
			dvid.Infof("  %s: %d blocks (%.1f%%)", b.label, b.count, pct)
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

	// Export the label topology as IndexGrid (no stats for HTTP endpoint)
	data, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName, nil)
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
func (d *Data) exportLabelToIndexGrid(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, gridName string, stats *fvdbExportStats) ([]byte, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("failed to get store: %w", err)
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
		return nil, fmt.Errorf("labelmap %q does not have 3D block size", d.DataName())
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
			return nil, fmt.Errorf("failed to get block %s: %w", izyx, err)
		}
		if data == nil {
			dvid.Infof("expected block %s to have data but found none", izyx)
			continue
		}

		compressedSize := len(data)

		// Decompress and parse the block
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize block %s: %w", izyx, err)
		}

		uncompressedSize := len(blockData)

		// Track block stats
		if stats != nil {
			stats.addBlockStats(compressedSize, uncompressedSize)
		}

		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal block %s: %w", izyx, err)
		}

		// Get block origin in voxel coordinates
		// DVID block coordinates are in units of blocks, need to convert to voxels
		blockCoord, err := izyx.IndexZYX()
		if err != nil {
			return nil, fmt.Errorf("failed to parse block coord %s: %w", izyx, err)
		}
		blockOriginX := int32(blockCoord[0]) * blockSize[0]
		blockOriginY := int32(blockCoord[1]) * blockSize[1]
		blockOriginZ := int32(blockCoord[2]) * blockSize[2]

		// Extract voxels belonging to this label's supervoxels from the block
		voxels := extractLabelVoxelsWithStats(&block, blockMeta.supervoxels, blockOriginX, blockOriginY, blockOriginZ, blockSize, stats)
		builder.AddVoxels(voxels)
		totalVoxels += int64(len(voxels))

		// Update stats
		if stats != nil {
			stats.totalVoxels = totalVoxels
		}

		// Log progress periodically
		if (i+1)%progressInterval == 0 || i+1 == totalBlocks {
			pct := float64(i+1) / float64(totalBlocks) * 100
			dvid.Infof("fVDB export progress: %d/%d blocks (%.1f%%), %d voxels so far",
				i+1, totalBlocks, pct, totalVoxels)
		}
	}

	// Build the IndexGrid
	grid := builder.Build()
	if grid == nil {
		return nil, fmt.Errorf("failed to build IndexGrid (no voxels found)")
	}

	// Serialize to NanoVDB format
	writer := nanovdb.NewWriter()
	if _, err := writer.WriteIndexGrid(grid); err != nil {
		return nil, fmt.Errorf("failed to serialize IndexGrid: %w", err)
	}

	return writer.Bytes(), nil
}

// extractLabelVoxelsWithStats extracts all voxels belonging to the given supervoxels from a block.
// Returns the voxel coordinates in absolute (not block-relative) coordinates.
// If stats is non-nil, updates bounding box and unique labels.
func extractLabelVoxelsWithStats(block *labels.Block, supervoxels labels.Set, originX, originY, originZ int32, blockSize dvid.Point3d, stats *fvdbExportStats) []nanovdb.Coord {
	// Get the full label volume from the compressed block
	labelData, _ := block.MakeLabelVolume()
	if len(labelData) == 0 {
		return nil
	}

	// Iterate through all voxels in the block
	var voxels []nanovdb.Coord
	nx, ny, nz := int32(blockSize[0]), int32(blockSize[1]), int32(blockSize[2])

	idx := 0
	for z := int32(0); z < nz; z++ {
		for y := int32(0); y < ny; y++ {
			for x := int32(0); x < nx; x++ {
				// Read the label at this voxel (uint64, little-endian)
				if idx+8 > len(labelData) {
					break
				}
				label := uint64(labelData[idx]) |
					uint64(labelData[idx+1])<<8 |
					uint64(labelData[idx+2])<<16 |
					uint64(labelData[idx+3])<<24 |
					uint64(labelData[idx+4])<<32 |
					uint64(labelData[idx+5])<<40 |
					uint64(labelData[idx+6])<<48 |
					uint64(labelData[idx+7])<<56
				idx += 8

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
						voxels = append(voxels, nanovdb.Coord{
							X: vx,
							Y: vy,
							Z: vz,
						})
						// Update bounding box
						if stats != nil {
							stats.updateBoundingBox(vx, vy, vz)
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
// Usage: dvid node <UUID> <data name> fvdb <label> <file path>
func (d *Data) exportLabelToFVDB(ctx *datastore.VersionedCtx, label uint64, outPath string) {
	timedLog := dvid.NewTimeLog()

	dvid.Infof("Starting fVDB export of label %d for data %q to %s\n", label, d.DataName(), outPath)

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

	// Export the label topology as IndexGrid
	data, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName, stats)
	if err != nil {
		dvid.Errorf("fVDB export failed for label %d: %v\n", label, err)
		return
	}

	// Write to file
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

	// Print comprehensive statistics
	stats.printSummary(label, n)
}
