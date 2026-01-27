// fVDB export functionality for labelmap datatype.
// This file implements the IndexGrid export for use with the fVDB library.

package labelmap

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/nanovdb"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

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

	// Export the label topology as IndexGrid
	data, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName)
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
func (d *Data) exportLabelToIndexGrid(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, gridName string) ([]byte, error) {
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

	// Process each block that contains this label
	for _, izyx := range blockMeta.sortedBlocks {
		tk := NewBlockTKeyByCoord(blockMeta.scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return nil, fmt.Errorf("failed to get block %s: %w", izyx, err)
		}
		if data == nil {
			dvid.Infof("expected block %s to have data but found none", izyx)
			continue
		}

		// Decompress and parse the block
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize block %s: %w", izyx, err)
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
		voxels := extractLabelVoxels(&block, blockMeta.supervoxels, blockOriginX, blockOriginY, blockOriginZ, blockSize)
		builder.AddVoxels(voxels)
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

// extractLabelVoxels extracts all voxels belonging to the given supervoxels from a block.
// Returns the voxel coordinates in absolute (not block-relative) coordinates.
func extractLabelVoxels(block *labels.Block, supervoxels labels.Set, originX, originY, originZ int32, blockSize dvid.Point3d) []nanovdb.Coord {
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

				// Check if this voxel belongs to our label
				if label != 0 {
					if _, found := supervoxels[label]; found {
						voxels = append(voxels, nanovdb.Coord{
							X: originX + x,
							Y: originY + y,
							Z: originZ + z,
						})
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

	// Export the label topology as IndexGrid
	data, err := d.exportLabelToIndexGrid(ctx, labelBlockMeta, gridName)
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
}
