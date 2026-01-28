// generate_test_grids.go generates test .nvdb files using the dvid nanovdb package.
//
// Usage: go run generate_test_grids.go [output_dir]
//
// Produces test .nvdb files and a manifest (test_grids.txt) with expected voxel counts.
// Supports both OnIndex (topology-only) and Int64 (value) grids.
package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/datatype/common/nanovdb"
)

type indexTestCase struct {
	name          string
	filename      string
	voxels        []nanovdb.Coord
	expectedCount int
}

type int64TestCase struct {
	name          string
	filename      string
	voxels        []nanovdb.Coord
	values        []int64
	expectedCount int
	checkValues   []int64 // values to verify exist
}

func main() {
	outDir := "."
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	voxelSize := nanovdb.Vec3d{X: 1.0, Y: 1.0, Z: 1.0}

	// OnIndex (topology-only) test cases
	indexCases := []indexTestCase{
		{
			name:     "single_voxel",
			filename: "single_voxel.nvdb",
			voxels:   []nanovdb.Coord{{X: 0, Y: 0, Z: 0}},
		},
		{
			name:     "small_grid",
			filename: "small_grid.nvdb",
			voxels: []nanovdb.Coord{
				{X: 0, Y: 0, Z: 0},
				{X: 1, Y: 1, Z: 1},
				{X: 10, Y: 10, Z: 10},
			},
		},
		{
			name:     "multi_leaf",
			filename: "multi_leaf.nvdb",
			voxels:   generateCube(0, 0, 0, 10), // 10x10x10 = 1000 voxels
		},
		{
			name:     "large_grid",
			filename: "large_grid.nvdb",
			voxels:   generateCube(0, 0, 0, 64), // 64x64x64 = 262144 voxels
		},
	}

	// Int64 value grid test cases
	int64Cases := []int64TestCase{
		{
			name:     "int64_single",
			filename: "int64_single.nvdb",
			voxels:   []nanovdb.Coord{{X: 0, Y: 0, Z: 0}},
			values:   []int64{42},
			checkValues: []int64{42},
		},
		{
			name:     "int64_small",
			filename: "int64_small.nvdb",
			voxels: []nanovdb.Coord{
				{X: 0, Y: 0, Z: 0},
				{X: 1, Y: 1, Z: 1},
				{X: 10, Y: 10, Z: 10},
			},
			values:      []int64{100, 200, 300},
			checkValues: []int64{100, 200, 300},
		},
		{
			name:        "int64_cube",
			filename:    "int64_cube.nvdb",
			voxels:      generateCube(0, 0, 0, 10),
			values:      generateLabelValues(10 * 10 * 10), // sequential labels
			checkValues: []int64{1, 500, 1000},
		},
		{
			name:     "int64_maxval",
			filename: "int64_maxval.nvdb",
			voxels: []nanovdb.Coord{
				{X: 0, Y: 0, Z: 0},
				{X: 1, Y: 0, Z: 0},
			},
			values:      []int64{0, math.MaxInt64}, // test edge case
			checkValues: []int64{0, math.MaxInt64},
		},
	}

	// Set expected counts
	for i := range indexCases {
		indexCases[i].expectedCount = len(indexCases[i].voxels)
	}
	for i := range int64Cases {
		int64Cases[i].expectedCount = len(int64Cases[i].voxels)
	}

	// Write manifest
	manifestPath := filepath.Join(outDir, "test_grids.txt")
	manifest, err := os.Create(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR creating manifest: %v\n", err)
		os.Exit(1)
	}
	defer manifest.Close()

	// Generate OnIndex grids
	for _, tc := range indexCases {
		path := filepath.Join(outDir, tc.filename)
		fmt.Printf("Generating %s (OnIndex, %d voxels)...\n", tc.filename, tc.expectedCount)

		if err := nanovdb.WriteIndexGridToFile(path, tc.name, tc.voxels, voxelSize); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR writing %s: %v\n", tc.filename, err)
			os.Exit(1)
		}

		// Format: filename count [type]
		fmt.Fprintf(manifest, "%s %d OnIndex\n", tc.filename, tc.expectedCount)
	}

	// Generate Int64 grids
	for _, tc := range int64Cases {
		path := filepath.Join(outDir, tc.filename)
		fmt.Printf("Generating %s (Int64, %d voxels)...\n", tc.filename, tc.expectedCount)

		if err := nanovdb.WriteInt64GridToFile(path, tc.name, tc.voxels, tc.values, voxelSize); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR writing %s: %v\n", tc.filename, err)
			os.Exit(1)
		}

		// Format: filename count type [check_values]
		checkStr := ""
		if len(tc.checkValues) > 0 {
			strs := make([]string, len(tc.checkValues))
			for i, v := range tc.checkValues {
				strs[i] = fmt.Sprintf("%d", v)
			}
			checkStr = " " + strings.Join(strs, ",")
		}
		fmt.Fprintf(manifest, "%s %d Int64%s\n", tc.filename, tc.expectedCount, checkStr)
	}

	fmt.Printf("Manifest written to %s\n", manifestPath)
}

// generateCube generates all voxel coordinates for an NxNxN cube starting at (ox, oy, oz).
func generateCube(ox, oy, oz, n int) []nanovdb.Coord {
	coords := make([]nanovdb.Coord, 0, n*n*n)
	for z := 0; z < n; z++ {
		for y := 0; y < n; y++ {
			for x := 0; x < n; x++ {
				coords = append(coords, nanovdb.Coord{
					X: int32(ox + x),
					Y: int32(oy + y),
					Z: int32(oz + z),
				})
			}
		}
	}
	return coords
}

// generateLabelValues generates sequential label values starting from 1.
func generateLabelValues(count int) []int64 {
	values := make([]int64, count)
	for i := range values {
		values[i] = int64(i + 1)
	}
	return values
}
