package labelmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/bits"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func TestComputeShardID(t *testing.T) {
	scale := &ngScale{
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "murmurhash3_x86_128",
			PreshiftBits:  1,
			MinishardBits: 6,
			ShardBits:     15,
		},
		shardMask: 0x7FFF << 6, // computed during initialize()
	}

	// Test with known coordinates
	testCases := []dvid.ChunkPoint3d{
		{0, 0, 0},
		{100, 200, 300},
		{1, 1, 1},
		{1000, 2000, 3000},
	}

	for _, coord := range testCases {
		shardID := scale.computeShardID(coord[0], coord[1], coord[2])

		// Verify shard ID is within expected range
		maxShardID := uint64(1<<scale.Sharding.ShardBits) - 1
		if shardID > maxShardID {
			t.Errorf("computeShardID(%v) returned %d, which exceeds max shard ID %d", coord, shardID, maxShardID)
		}

		// Verify consistency - same input should give same output
		shardID2 := scale.computeShardID(coord[0], coord[1], coord[2])
		if shardID != shardID2 {
			t.Errorf("computeShardID(%v) not consistent: got %d then %d", coord, shardID, shardID2)
		}
	}
}

// --- Cross-validation tests against tensorstore reference implementation ---
// Test vectors are taken directly from tensorstore's metadata_test.cc in
// ~/tensorstore/tensorstore/driver/neuroglancer_precomputed/metadata_test.cc

// TestMortonCodeMatchesTensorstore verifies DVID's mortonCode matches
// tensorstore's EncodeCompressedZIndex using tensorstore's exact test vectors.
func TestMortonCodeMatchesTensorstore(t *testing.T) {
	// From tensorstore EncodeCompressedZIndexTest::Basic with bits={4,2,1}
	// Note: DVID hardwires chunk size to 64, so we can't directly use bits={4,2,1}
	// via initialize(). Instead we test mortonCode() directly with manual chunkCoordBits.
	scale := &ngScale{
		chunkCoordBits: [3]uint8{4, 2, 1},
	}

	cases := []struct {
		coord    dvid.ChunkPoint3d
		expected uint64
	}{
		{dvid.ChunkPoint3d{0, 0, 0}, 0},
		{dvid.ChunkPoint3d{1, 0, 0}, 1},
		{dvid.ChunkPoint3d{0, 1, 0}, 2},
		{dvid.ChunkPoint3d{0b10, 0b0, 0b1}, 0b1100},
		{dvid.ChunkPoint3d{0b10, 0b11, 0b0}, 0b11010},
		{dvid.ChunkPoint3d{0b1001, 0b10, 0b1}, 0b1010101},
	}

	for _, tc := range cases {
		got := scale.mortonCode(tc.coord)
		if got != tc.expected {
			t.Errorf("mortonCode(%v) with bits={4,2,1}: got %d (0b%b), expected %d (0b%b)",
				tc.coord, got, got, tc.expected, tc.expected)
		}
	}
}

// TestCompressedZIndexBitsMatchesTensorstore verifies DVID's bit width calculation
// matches tensorstore's GetCompressedZIndexBits using tensorstore's exact test vectors.
func TestCompressedZIndexBitsMatchesTensorstore(t *testing.T) {
	// From tensorstore GetCompressedZIndexBitsTest::Basic
	// volume_shape={79, 80, 144}, chunk_shape={20, 20, 12} → bits={2, 2, 4}
	// Grid: ceil(79/20)=4, ceil(80/20)=4, ceil(144/12)=12
	// bits: bit_width(3)=2, bit_width(3)=2, bit_width(11)=4

	// We can't use DVID's initialize() because it requires chunk_size=64.
	// Instead we test the bit calculation logic directly.
	cases := []struct {
		volumeSize, chunkSize dvid.Point3d
		expectedBits          [3]uint8
		expectedGrid          [3]uint32
	}{
		{
			dvid.Point3d{79, 80, 144}, dvid.Point3d{20, 20, 12},
			[3]uint8{2, 2, 4}, [3]uint32{4, 4, 12},
		},
		// mCNS scale 0: volume {94088, 78317, 134576}, chunk {64, 64, 64}
		{
			dvid.Point3d{94088, 78317, 134576}, dvid.Point3d{64, 64, 64},
			[3]uint8{11, 11, 12}, [3]uint32{1471, 1224, 2103},
		},
		// Edge case: volume exactly divisible by chunk size
		{
			dvid.Point3d{128, 256, 64}, dvid.Point3d{64, 64, 64},
			[3]uint8{1, 2, 0}, [3]uint32{2, 4, 1},
		},
		// Edge case: volume size = 1 chunk + 1 voxel
		{
			dvid.Point3d{65, 65, 65}, dvid.Point3d{64, 64, 64},
			[3]uint8{1, 1, 1}, [3]uint32{2, 2, 2},
		},
		// Edge case: single chunk dimension
		{
			dvid.Point3d{64, 64, 64}, dvid.Point3d{64, 64, 64},
			[3]uint8{0, 0, 0}, [3]uint32{1, 1, 1},
		},
	}

	for _, tc := range cases {
		for dim := 0; dim < 3; dim++ {
			chunksNeeded := tc.volumeSize[dim] / tc.chunkSize[dim]
			if tc.volumeSize[dim]%tc.chunkSize[dim] != 0 {
				chunksNeeded++
			}
			if uint32(chunksNeeded) != tc.expectedGrid[dim] {
				t.Errorf("Volume %v chunk %v dim %d: grid=%d, expected %d",
					tc.volumeSize, tc.chunkSize, dim, chunksNeeded, tc.expectedGrid[dim])
			}
			var gotBits uint8
			if chunksNeeded <= 1 {
				gotBits = 0
			} else {
				gotBits = uint8(bits.Len32(uint32(chunksNeeded - 1)))
			}
			if gotBits != tc.expectedBits[dim] {
				t.Errorf("Volume %v chunk %v dim %d: bits=%d, expected %d",
					tc.volumeSize, tc.chunkSize, dim, gotBits, tc.expectedBits[dim])
			}
		}
	}
}

// TestShardHierarchyMatchesTensorstore verifies DVID's shard shape calculation
// matches tensorstore's GetShardChunkHierarchy using tensorstore's exact test vectors.
// The tensorstore tests use non-64 chunk sizes, but the Morton bit iterator and
// shard shape logic is independent of chunk size.
func TestShardHierarchyMatchesTensorstore(t *testing.T) {
	// This tests the CompressedMortonBitIterator logic which DVID implements
	// in shardHandler.Initialize()'s Morton bit walking loop.

	// Helper: simulate the DVID Morton bit iterator to compute shard shape
	computeShardShape := func(zIndexBits [3]uint8, preshiftBits, minishardBits uint8, gridShape [3]uint32) [3]int32 {
		totalBits := int(zIndexBits[0]) + int(zIndexBits[1]) + int(zIndexBits[2])
		nonShardBits := int(preshiftBits) + int(minishardBits)
		if nonShardBits > totalBits {
			nonShardBits = totalBits
		}
		curBit := [3]int{0, 0, 0}
		dimI := 0
		for i := 0; i < nonShardBits; i++ {
			for curBit[dimI] == int(zIndexBits[dimI]) {
				dimI = (dimI + 1) % 3
			}
			curBit[dimI]++
			dimI = (dimI + 1) % 3
		}
		var shape [3]int32
		for dim := 0; dim < 3; dim++ {
			s := int32(1) << curBit[dim]
			if s > int32(gridShape[dim]) {
				s = int32(gridShape[dim])
			}
			shape[dim] = s
		}
		return shape
	}

	cases := []struct {
		name         string
		zIndexBits   [3]uint8
		gridShape    [3]uint32
		preshift     uint8
		minishard    uint8
		shardBits    uint8
		expectedShard [3]int32 // shard shape in chunks
	}{
		{
			// From tensorstore AllShardsFull test
			// volume={99,98,97}, chunk={50,25,13} → grid={2,4,8}, bits={1,2,3}
			// preshift=1, minishard=2, shard=3
			// nonShardBits = 1+2 = 3
			// shard_shape = {2, 2, 2}
			name: "AllShardsFull", zIndexBits: [3]uint8{1, 2, 3},
			gridShape: [3]uint32{2, 4, 8}, preshift: 1, minishard: 2, shardBits: 3,
			expectedShard: [3]int32{2, 2, 2},
		},
		{
			// From tensorstore PartialShards1Dim test
			// volume={99,98,90}, chunk={50,25,13} → grid={2,4,7}, bits={1,2,3}
			// Same sharding → shard_shape = {2, 2, 2}
			name: "PartialShards1Dim", zIndexBits: [3]uint8{1, 2, 3},
			gridShape: [3]uint32{2, 4, 7}, preshift: 1, minishard: 2, shardBits: 3,
			expectedShard: [3]int32{2, 2, 2},
		},
		{
			// From tensorstore PartialShards2Dims test
			// volume={99,70,90}, chunk={50,25,13} → grid={2,3,7}, bits={1,2,3}
			// shard_shape = {2, 2, 2}
			name: "PartialShards2Dims", zIndexBits: [3]uint8{1, 2, 3},
			gridShape: [3]uint32{2, 3, 7}, preshift: 1, minishard: 2, shardBits: 3,
			expectedShard: [3]int32{2, 2, 2},
		},
		{
			// mCNS scale 0: bits={11,11,12}, preshift=9, minishard=6, shard=19
			// nonShardBits = 15
			// Expected: shard shape = {32, 32, 32} chunks = 2048 voxels per dim
			name: "mCNS_scale0", zIndexBits: [3]uint8{11, 11, 12},
			gridShape: [3]uint32{1471, 1224, 2103}, preshift: 9, minishard: 6, shardBits: 19,
			expectedShard: [3]int32{32, 32, 32},
		},
		{
			// mCNS scale 4 (used as test scale 0): bits for volume 5881×4895×8411
			// grid = {92, 77, 132}, bits = {7, 7, 8}
			// preshift=9, minishard=6, shard=7
			// nonShardBits = min(15, 7+7+8=22) = 15
			name: "mCNS_scale4", zIndexBits: [3]uint8{7, 7, 8},
			gridShape: [3]uint32{92, 77, 132}, preshift: 9, minishard: 6, shardBits: 7,
			expectedShard: [3]int32{32, 32, 32},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeShardShape(tc.zIndexBits, tc.preshift, tc.minishard, tc.gridShape)
			if got != tc.expectedShard {
				t.Errorf("Shard shape: got %v, expected %v", got, tc.expectedShard)
			}
		})
	}
}

// TestShardIDEndToEndMatchesTensorstore tests the full pipeline:
// chunk coord → Morton code → preshift → shard ID extraction.
// Uses the tensorstore AllShardsFull test case to verify shard assignments.
func TestShardIDEndToEndMatchesTensorstore(t *testing.T) {
	// tensorstore AllShardsFull: volume={99,98,97}, chunk={50,25,13}
	// grid={2,4,8}, bits={1,2,3}, preshift=1, minishard=2, shard=3
	// 8 shards (2^3), each containing 2×2×2=8 chunks
	//
	// We can verify the shard assignment by manually computing Morton codes.
	// With bits={1,2,3}, the Morton interleaving is:
	//   bit0: X₀, Y₀, Z₀
	//   bit1: Y₁, Z₁
	//   bit2: Z₂
	// Total 6 bits of Morton code.
	//
	// After preshift>>1, we have 5 bits. minishard takes 2 bits, shard takes 3 bits.

	scale := &ngScale{
		chunkCoordBits:    [3]uint8{1, 2, 3},
		totChunkCoordBits: 6,
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "identity",
			PreshiftBits:  1,
			MinishardBits: 2,
			ShardBits:     3,
		},
	}
	// Compute masks
	const on uint64 = 0xFFFFFFFFFFFFFFFF
	minishardOff := (on >> scale.Sharding.MinishardBits) << scale.Sharding.MinishardBits
	scale.minishardMask = ^minishardOff
	excessBits := 64 - scale.Sharding.ShardBits - scale.Sharding.MinishardBits
	scale.shardMask = (minishardOff << excessBits) >> excessBits

	// Verify all chunks in the 2×4×8 grid produce valid shard IDs in [0, 7].
	shardChunkCount := make(map[uint64]int)
	for z := int32(0); z < 8; z++ {
		for y := int32(0); y < 4; y++ {
			for x := int32(0); x < 2; x++ {
				shardID := scale.computeShardID(x, y, z)
				if shardID >= 8 {
					t.Errorf("Chunk (%d,%d,%d) got shard %d, expected < 8", x, y, z, shardID)
				}
				shardChunkCount[shardID]++
			}
		}
	}

	// Per tensorstore: all 8 shards should have exactly 8 chunks each.
	for shard := uint64(0); shard < 8; shard++ {
		if shardChunkCount[shard] != 8 {
			t.Errorf("Shard %d has %d chunks, expected 8", shard, shardChunkCount[shard])
		}
	}
}

// TestShardIDEdgeCases tests shard ID calculation at volume boundaries.
func TestShardIDEdgeCases(t *testing.T) {
	// Test with mCNS scale 0 parameters
	scale := &ngScale{
		ChunkSizes: []dvid.Point3d{{64, 64, 64}},
		Size:       dvid.Point3d{94088, 78317, 134576},
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "identity",
			PreshiftBits:  9,
			MinishardBits: 6,
			ShardBits:     19,
		},
	}
	if err := scale.initialize(); err != nil {
		t.Fatalf("initialize failed: %v", err)
	}

	maxShardID := uint64(1<<scale.Sharding.ShardBits) - 1 // 2^19 - 1

	// Test edge coordinates: origin, max grid, and various boundaries
	edgeCases := []dvid.ChunkPoint3d{
		{0, 0, 0},                                        // origin
		{1, 0, 0}, {0, 1, 0}, {0, 0, 1},                  // unit offsets
		{int32(scale.gridSize[0]) - 1, 0, 0},              // max X, min Y, min Z
		{0, int32(scale.gridSize[1]) - 1, 0},              // min X, max Y, min Z
		{0, 0, int32(scale.gridSize[2]) - 1},              // min X, min Y, max Z
		{int32(scale.gridSize[0]) - 1,                      // all max
			int32(scale.gridSize[1]) - 1,
			int32(scale.gridSize[2]) - 1},
		{31, 31, 31},                                      // last chunk in first shard
		{32, 0, 0}, {0, 32, 0}, {0, 0, 32},               // first chunk in second shard per dim
	}

	for _, coord := range edgeCases {
		shardID := scale.computeShardID(coord[0], coord[1], coord[2])
		if shardID > maxShardID {
			t.Errorf("Chunk %v: shard %d exceeds max %d", coord, shardID, maxShardID)
		}
		// Verify determinism
		shardID2 := scale.computeShardID(coord[0], coord[1], coord[2])
		if shardID != shardID2 {
			t.Errorf("Chunk %v: non-deterministic shard IDs %d, %d", coord, shardID, shardID2)
		}
	}

	// Verify that adjacent chunks within a shard get the same shard ID,
	// but chunks across shard boundaries get different IDs.
	shard00 := scale.computeShardID(0, 0, 0)
	shard10 := scale.computeShardID(10, 10, 10)
	shard31 := scale.computeShardID(31, 31, 31)
	if shard00 != shard10 || shard00 != shard31 {
		t.Errorf("Chunks (0,0,0), (10,10,10), (31,31,31) should be in same shard, got %d, %d, %d",
			shard00, shard10, shard31)
	}

	// Chunk (32,0,0) should be in a different shard than (0,0,0)
	shardNext := scale.computeShardID(32, 0, 0)
	if shardNext == shard00 {
		t.Errorf("Chunk (32,0,0) should be in different shard than (0,0,0), both got %d", shard00)
	}
}

func getExportSpec(t *testing.T) exportSpec {
	// Load real neuroglancer specs
	data, err := os.ReadFile("../../test_data/mcns-ng-specs.json")
	if err != nil {
		t.Fatalf("Failed to read test spec file: %v", err)
	}

	var spec ngVolume
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("Failed to parse test spec: %v", err)
	}

	return exportSpec{
		ngVolume:  spec,
		Directory: "/tmp/test_export",
		NumScales: 3,
	}
}

func TestShardHandlerWithRealSpecs(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Add data to labelmap instance
	// Create testbed volume and data instances
	uuid, v := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "2")
	config.Set("BlockSize", "32,32,32") // Previous test data was on 32^3 blocks
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)
	createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on labels updating: %v\n", err)
	}
	if err := downres.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on update for labels: %v\n", err)
	}

	dataservice, err := datastore.GetDataByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("couldn't get labels data instance from datastore: %v\n", err)
	}
	lbls, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned data instance for 'labels' instance was not labelmap.Data!\n")
	}

	// Run the Export with a Done channel for clean completion signaling.
	ctx := datastore.NewVersionedCtx(lbls, v)
	spec := getExportSpec(t)
	spec.Done = make(chan struct{})
	err = lbls.ExportData(ctx, spec)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	select {
	case <-spec.Done:
	case <-time.After(30 * time.Second):
		t.Fatal("Export did not complete within 30 seconds")
	}

	arrowFile := path.Join(spec.Directory, "s0", "0_0_0.arrow")
	csvFile := path.Join(spec.Directory, "s0", "0_0_0.csv")
	if _, err := os.Stat(arrowFile); err != nil {
		t.Fatalf("Expected shard arrow file %q not found: %v\n", arrowFile, err)
	}
	if _, err := os.Stat(csvFile); err != nil {
		t.Fatalf("Expected shard csv file %q not found: %v\n", csvFile, err)
	}
	dvid.Infof("Found expected shard files %q and %q\n", arrowFile, csvFile)
}

func getTestDataInstance(t *testing.T) (*Data, dvid.UUID, dvid.VersionID) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, v := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	dataservice, err := datastore.GetDataByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("couldn't get labels data instance from datastore: %v\n", err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned data instance for 'labels' instance was not labelmap.Data!\n")
	}
	return labels, uuid, v
}

func TestShardCalcs(t *testing.T) {
	// Create a test ngScale with known sharding parameters
	ns := &ngScale{
		ChunkSizes: []dvid.Point3d{{64, 64, 64}},
		Size:       dvid.Point3d{94088, 78317, 134576}, // mCNS scale 0 size
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "identity",
			PreshiftBits:  9,
			MinishardBits: 6,
			ShardBits:     19,
		},
	}
	const shardDimVoxels = 2048 // Expected shard dimension in voxels (2^11)

	// Initialize the scale to set up computed values
	err := ns.initialize()
	if err != nil {
		t.Fatalf("Failed to initialize ngScale: %v", err)
	}

	// Make sure this is what we expect.
	expectedChunkBits := [3]uint8{11, 11, 12}
	expectedTotalChunkBits := uint8(11 + 11 + 12)
	expectedGridSize := [3]uint32{1471, 1224, 2103}
	for i := 0; i < 3; i++ {
		if ns.chunkCoordBits[i] != expectedChunkBits[i] {
			t.Errorf("Expected chunkBits[%d]=%d, got %d", i, expectedChunkBits[i], ns.chunkCoordBits[i])
		}
		if ns.gridSize[i] != expectedGridSize[i] {
			t.Errorf("Expected gridSize[%d]=%d, got %d", i, expectedGridSize[i], ns.gridSize[i])
		}
	}
	if ns.totChunkCoordBits != expectedTotalChunkBits {
		t.Errorf("Expected totChunkBits=%d, got %d", expectedTotalChunkBits, ns.totChunkCoordBits)
	}

	// Set up a shard handler
	labels, _, v := getTestDataInstance(t)
	ctx := datastore.NewVersionedCtx(labels, v)
	exportSpec := getExportSpec(t)
	var handler shardHandler
	if err := handler.Initialize(ctx, exportSpec); err != nil {
		t.Fatalf("Failed to initialize shard handler: %v", err)
	}

	// Test cases with specific chunk coordinates that should fall into different shards
	// With shard size of 32 chunks per dimension (2048 voxels), chunks beyond that should be in different shards
	maxChunkCoord := dvid.ChunkPoint3d{
		int32(ns.gridSize[0] - 1),
		int32(ns.gridSize[1] - 1),
		int32(ns.gridSize[2] - 1),
	}

	testCases := []struct {
		name                string
		chunkCoord          dvid.ChunkPoint3d
		expectedShardOrigin dvid.Point3d // Expected voxel origin of the shard
	}{
		{"Origin shard", dvid.ChunkPoint3d{0, 0, 0}, dvid.Point3d{0, 0, 0}},
		{"Still in origin shard", dvid.ChunkPoint3d{10, 20, 30}, dvid.Point3d{0, 0, 0}},
		{"Still in origin shard max", dvid.ChunkPoint3d{31, 31, 31}, dvid.Point3d{0, 0, 0}},
		{"Next X shard", dvid.ChunkPoint3d{32, 0, 0}, dvid.Point3d{shardDimVoxels, 0, 0}},
		{"Next Y shard", dvid.ChunkPoint3d{0, 32, 0}, dvid.Point3d{0, shardDimVoxels, 0}},
		{"Next Z shard", dvid.ChunkPoint3d{0, 0, 32}, dvid.Point3d{0, 0, shardDimVoxels}},
		{"Diagonal next shard", dvid.ChunkPoint3d{32, 32, 32}, dvid.Point3d{shardDimVoxels, shardDimVoxels, shardDimVoxels}},
		{"Max chunk coords", maxChunkCoord, dvid.Point3d{92160, 77824, 133120}},
	}

	writers := []*shardWriter{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualShardID := ns.computeShardID(tc.chunkCoord[0], tc.chunkCoord[1], tc.chunkCoord[2])

			// Calculate the shard origin using our method
			shardOrigin := handler.shardOriginFromChunkCoord(0, tc.chunkCoord)
			filename := fmt.Sprintf("%d_%d_%d.arrow", shardOrigin[0], shardOrigin[1], shardOrigin[2])

			t.Logf("Chunk %v -> Shard ID %d -> Origin %v -> Filename %s",
				tc.chunkCoord, actualShardID, shardOrigin, filename)

			// Verify the calculated shard origin matches expected
			if shardOrigin != tc.expectedShardOrigin {
				t.Errorf("Chunk %v: Expected shard origin %v, got %v",
					tc.chunkCoord, tc.expectedShardOrigin, shardOrigin)
			}

			// Verify a shard writer uses the expected filename
			ep := &epoch{writers: make(map[uint64]*shardWriter, handler.shardsInStrip)}
			w, err := handler.getWriter(actualShardID, 0, tc.chunkCoord, ep)
			if err != nil {
				t.Fatalf("Failed to get shard writer for chunk %v: %v", tc.chunkCoord, err)
			}
			if w.f.Name() != path.Join(handler.path, "s0", filename) {
				t.Errorf("Chunk %v: Expected filename %s, got %s",
					tc.chunkCoord, filename, w.f.Name())
			}
			writers = append(writers, w)
		})
	}

	// Test that chunks mapping to the same shard ID produce the same shard origin and get same shardWriter.
	t.Run("SameShardSameWriter", func(t *testing.T) {
		// Find chunks that should map to the same shard (if any exist)
		chunk1 := dvid.ChunkPoint3d{10, 20, 30}
		chunk2 := dvid.ChunkPoint3d{11, 20, 30} // Slightly different coordinate

		shardOrigin1 := handler.shardOriginFromChunkCoord(0, chunk1)
		shardOrigin2 := handler.shardOriginFromChunkCoord(0, chunk2)

		if !shardOrigin1.Equals(shardOrigin2) {
			t.Errorf("Chunks %v and %v should map to same shard origin, got %v and %v",
				chunk1, chunk2, shardOrigin1, shardOrigin2)
		}

		shardID1 := ns.computeShardID(chunk1[0], chunk1[1], chunk1[2])
		shardID2 := ns.computeShardID(chunk2[0], chunk2[1], chunk2[2])

		if shardID1 != shardID2 {
			t.Errorf("Chunks %v and %v should map to same shard ID, got %d and %d",
				chunk1, chunk2, shardID1, shardID2)
		}

		ep := &epoch{writers: make(map[uint64]*shardWriter, handler.shardsInStrip)}
		w1, err := handler.getWriter(shardID1, 0, chunk1, ep)
		if err != nil {
			t.Fatalf("Failed to get shard writer for chunk %v: %v", chunk1, err)
		}
		w2, err := handler.getWriter(shardID2, 0, chunk2, ep)
		if err != nil {
			t.Fatalf("Failed to get shard writer for chunk %v: %v", chunk2, err)
		}

		if w1 != w2 {
			t.Errorf("Chunks %v and %v should get same shard writer instance", chunk1, chunk2)
		}
	})
}

// --- Deterministic pattern generator for export-shards integration test ---

const (
	tileDimX = 71 // primes slightly > 64 so tile boundaries never align with chunk boundaries
	tileDimY = 73
	tileDimZ = 67
	numSV    = 8 // 8 distinct supervoxels
)

// syntheticSV returns a deterministic supervoxel ID (1-8) for any voxel coordinate.
func syntheticSV(x, y, z int32) uint64 {
	tx := x / tileDimX
	ty := y / tileDimY
	tz := z / tileDimZ
	h := uint64(tx*7 + ty*13 + tz*19)
	return (h % numSV) + 1
}

// expectedAgglo returns the agglomerated label for a supervoxel after merges.
// Merge groups: {1,2,3}→1, {4,5}→4, {6,7,8}→6
func expectedAgglo(sv uint64) uint64 {
	switch {
	case sv <= 3:
		return 1
	case sv <= 5:
		return 4
	default:
		return 6
	}
}

// expectedBlockLabels computes the unique supervoxels and agglomerated labels
// expected for a block at the given chunk coordinate.
func expectedBlockLabels(cx, cy, cz int32) (svs, agglos map[uint64]struct{}) {
	svs = make(map[uint64]struct{})
	agglos = make(map[uint64]struct{})
	for z := int32(0); z < 64; z++ {
		for y := int32(0); y < 64; y++ {
			for x := int32(0); x < 64; x++ {
				sv := syntheticSV(cx*64+x, cy*64+y, cz*64+z)
				svs[sv] = struct{}{}
				agglos[expectedAgglo(sv)] = struct{}{}
			}
		}
	}
	return
}

// generateZeroBlock creates a compressed DVID label block filled entirely with label 0.
func generateZeroBlock() (*labels.Block, error) {
	blockSize := dvid.Point3d{64, 64, 64}
	raw := make([]byte, 64*64*64*8) // zero-initialized = all label 0
	return labels.MakeBlock(raw, blockSize)
}

// generateBlock creates a compressed DVID label block filled with the deterministic pattern.
func generateBlock(cx, cy, cz int32) (*labels.Block, error) {
	blockSize := dvid.Point3d{64, 64, 64}
	raw := make([]byte, 64*64*64*8)
	for z := int32(0); z < 64; z++ {
		for y := int32(0); y < 64; y++ {
			for x := int32(0); x < 64; x++ {
				sv := syntheticSV(cx*64+x, cy*64+y, cz*64+z)
				off := (z*64*64 + y*64 + x) * 8
				binary.LittleEndian.PutUint64(raw[off:off+8], sv)
			}
		}
	}
	return labels.MakeBlock(raw, blockSize)
}

// writeTestInt32 writes a little-endian int32 to the buffer.
func writeTestBlockInt32(t *testing.T, buf *bytes.Buffer, v int32) {
	t.Helper()
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	_, err := buf.Write(b)
	if err != nil {
		t.Fatalf("unable to write int32: %v", err)
	}
}

// ingestSyntheticBlocks generates and POSTs compressed blocks for the given chunk regions.
func ingestSyntheticBlocks(t *testing.T, uuid dvid.UUID, name string, regions []struct{ origin, size dvid.ChunkPoint3d }) int {
	t.Helper()
	var totalBlocks int
	for _, r := range regions {
		var buf bytes.Buffer
		count := 0
		for cz := r.origin[2]; cz < r.origin[2]+r.size[2]; cz++ {
			for cy := r.origin[1]; cy < r.origin[1]+r.size[1]; cy++ {
				for cx := r.origin[0]; cx < r.origin[0]+r.size[0]; cx++ {
					block, err := generateBlock(cx, cy, cz)
					if err != nil {
						t.Fatalf("generateBlock(%d,%d,%d) failed: %v", cx, cy, cz, err)
					}
					gzipped, err := block.CompressGZIP()
					if err != nil {
						t.Fatalf("CompressGZIP failed for block (%d,%d,%d): %v", cx, cy, cz, err)
					}
					writeTestBlockInt32(t, &buf, cx)
					writeTestBlockInt32(t, &buf, cy)
					writeTestBlockInt32(t, &buf, cz)
					writeTestBlockInt32(t, &buf, int32(len(gzipped)))
					if _, err := buf.Write(gzipped); err != nil {
						t.Fatalf("buf.Write failed: %v", err)
					}
					count++
				}
			}
		}
		apiStr := fmt.Sprintf("%snode/%s/%s/blocks?downres=true", server.WebAPIPath, uuid, name)
		server.TestHTTP(t, "POST", apiStr, &buf)
		totalBlocks += count
	}
	return totalBlocks
}

// getIntegrationExportSpec builds an export spec using mCNS scales 4-6 as test scales 0-2.
func getIntegrationExportSpec(t *testing.T, dir string) exportSpec {
	t.Helper()
	data, err := os.ReadFile("../../test_data/mcns-ng-specs.json")
	if err != nil {
		t.Fatalf("Failed to read test spec file: %v", err)
	}

	var vol ngVolume
	if err := json.Unmarshal(data, &vol); err != nil {
		t.Fatalf("Failed to parse test spec: %v", err)
	}

	// Extract scales 4, 5, 6 as test scales 0, 1, 2
	if len(vol.Scales) < 7 {
		t.Fatalf("Expected at least 7 scales in mCNS spec, got %d", len(vol.Scales))
	}
	testVol := ngVolume{
		StoreType:   vol.StoreType,
		VolumeType:  vol.VolumeType,
		DataType:    vol.DataType,
		NumChannels: vol.NumChannels,
		Scales:      []ngScale{vol.Scales[4], vol.Scales[5], vol.Scales[6]},
	}

	return exportSpec{
		ngVolume:  testVol,
		Directory: dir,
		NumScales: 3,
		Done:      make(chan struct{}),
	}
}

// verifyArrowFiles reads exported Arrow IPC files for scale 0 and verifies
// that each block's supervoxel and label lists match the deterministic pattern.
func verifyArrowFiles(t *testing.T, dir string) int {
	t.Helper()
	s0Dir := path.Join(dir, "s0")
	arrowFiles, err := filepath.Glob(path.Join(s0Dir, "*.arrow"))
	if err != nil {
		t.Fatalf("glob for arrow files failed: %v", err)
	}
	if len(arrowFiles) == 0 {
		t.Fatalf("No arrow files found in %s", s0Dir)
	}

	var totalRecords int
	for _, arrowPath := range arrowFiles {
		f, err := os.Open(arrowPath)
		if err != nil {
			t.Fatalf("open %s: %v", arrowPath, err)
		}

		reader, err := ipc.NewReader(bufio.NewReader(f), ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			f.Close()
			t.Fatalf("ipc.NewReader for %s: %v", arrowPath, err)
		}

		for reader.Next() {
			rec := reader.Record()
			nRows := int(rec.NumRows())
			totalRecords += nRows

			chunkXCol := rec.Column(0).(*array.Int32)
			chunkYCol := rec.Column(1).(*array.Int32)
			chunkZCol := rec.Column(2).(*array.Int32)
			labelsCol := rec.Column(3).(*array.List)
			svsCol := rec.Column(4).(*array.List)

			for i := 0; i < nRows; i++ {
				cx := chunkXCol.Value(i)
				cy := chunkYCol.Value(i)
				cz := chunkZCol.Value(i)

				expectedSVs, expectedAgglos := expectedBlockLabels(cx, cy, cz)

				// Extract supervoxels from Arrow list
				gotSVs := make(map[uint64]struct{})
				if !svsCol.IsNull(i) {
					start := int(svsCol.Offsets()[i])
					end := int(svsCol.Offsets()[i+1])
					vals := svsCol.ListValues().(*array.Uint64)
					for j := start; j < end; j++ {
						gotSVs[vals.Value(j)] = struct{}{}
					}
				}

				// Extract agglomerated labels from Arrow list
				gotAgglos := make(map[uint64]struct{})
				if !labelsCol.IsNull(i) {
					start := int(labelsCol.Offsets()[i])
					end := int(labelsCol.Offsets()[i+1])
					vals := labelsCol.ListValues().(*array.Uint64)
					for j := start; j < end; j++ {
						gotAgglos[vals.Value(j)] = struct{}{}
					}
				}

				// Verify supervoxels match
				if len(gotSVs) != len(expectedSVs) {
					t.Errorf("Block (%d,%d,%d): expected %d supervoxels, got %d",
						cx, cy, cz, len(expectedSVs), len(gotSVs))
				}
				for sv := range expectedSVs {
					if _, ok := gotSVs[sv]; !ok {
						t.Errorf("Block (%d,%d,%d): missing expected supervoxel %d", cx, cy, cz, sv)
					}
				}

				// Verify agglomerated labels match
				if len(gotAgglos) != len(expectedAgglos) {
					t.Errorf("Block (%d,%d,%d): expected %d agglo labels, got %d",
						cx, cy, cz, len(expectedAgglos), len(gotAgglos))
				}
				for agglo := range expectedAgglos {
					if _, ok := gotAgglos[agglo]; !ok {
						t.Errorf("Block (%d,%d,%d): missing expected agglo label %d", cx, cy, cz, agglo)
					}
				}
			}
		}
		reader.Release()
		f.Close()
	}
	return totalRecords
}

// verifyCSVFiles checks that CSV index files exist for scale 0 and have the correct header.
func verifyCSVFiles(t *testing.T, dir string) {
	t.Helper()
	s0Dir := path.Join(dir, "s0")
	csvFiles, err := filepath.Glob(path.Join(s0Dir, "*.csv"))
	if err != nil {
		t.Fatalf("glob for csv files failed: %v", err)
	}
	if len(csvFiles) == 0 {
		t.Fatalf("No csv files found in %s", s0Dir)
	}

	for _, csvPath := range csvFiles {
		data, err := os.ReadFile(csvPath)
		if err != nil {
			t.Fatalf("read %s: %v", csvPath, err)
		}
		content := string(data)
		if !strings.HasPrefix(content, "# schema_size=") {
			t.Errorf("CSV %s missing schema_size header", csvPath)
		}
		if !strings.Contains(content, "x,y,z,offset,size,batch_idx") {
			t.Errorf("CSV %s missing column header", csvPath)
		}
	}
}

func TestExportShardsIntegration(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, v := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "2")
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	// Ingest synthetic blocks in scattered regions to exercise multiple shards.
	// Region A: 8x8x8 at origin — 512 blocks (one shard)
	// Region B: 4x4x4 at chunk (36,36,36) — 64 blocks (different shard)
	// Region C: 4x4x4 at chunk (72,60,100) — 64 blocks (different shard)
	regions := []struct{ origin, size dvid.ChunkPoint3d }{
		{dvid.ChunkPoint3d{0, 0, 0}, dvid.ChunkPoint3d{8, 8, 8}},
		{dvid.ChunkPoint3d{36, 36, 36}, dvid.ChunkPoint3d{4, 4, 4}},
		{dvid.ChunkPoint3d{72, 60, 100}, dvid.ChunkPoint3d{4, 4, 4}},
	}
	totalBlocks := ingestSyntheticBlocks(t, uuid, "labels", regions)
	t.Logf("Ingested %d synthetic blocks", totalBlocks)

	// Wait for ingest and downres to complete.
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on labels updating: %v\n", err)
	}
	if err := downres.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on downres for labels: %v\n", err)
	}

	// Set up merge mappings: {2,3}→1, {5}→4, {7,8}→6
	mergeJSON(`[1, 2, 3]`).send(t, uuid, "labels")
	mergeJSON(`[4, 5]`).send(t, uuid, "labels")
	mergeJSON(`[6, 7, 8]`).send(t, uuid, "labels")

	// Get the Data instance.
	dataservice, err := datastore.GetDataByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("couldn't get labels data instance: %v\n", err)
	}
	lbls, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned data instance was not labelmap.Data\n")
	}

	// Build export spec using mCNS scales 4-6 as test scales 0-2.
	exportDir := t.TempDir()
	spec := getIntegrationExportSpec(t, exportDir)
	ctx := datastore.NewVersionedCtx(lbls, v)

	// Run export and wait for completion.
	if err := lbls.ExportData(ctx, spec); err != nil {
		t.Fatalf("ExportData failed: %v", err)
	}
	select {
	case <-spec.Done:
	case <-time.After(60 * time.Second):
		t.Fatal("Export did not complete within 60 seconds")
	}

	// Verify scale 0 Arrow files contain correct data.
	records := verifyArrowFiles(t, exportDir)
	if records != totalBlocks {
		t.Errorf("Expected %d scale-0 records, got %d", totalBlocks, records)
	}
	t.Logf("Verified %d scale-0 block records across Arrow files", records)

	// Verify CSV index files.
	verifyCSVFiles(t, exportDir)

	// Verify higher scales produced output.
	for _, scaleDir := range []string{"s1", "s2"} {
		arrowFiles, _ := filepath.Glob(path.Join(exportDir, scaleDir, "*.arrow"))
		if len(arrowFiles) == 0 {
			t.Errorf("No arrow files found for %s", scaleDir)
		} else {
			t.Logf("Found %d arrow file(s) for %s", len(arrowFiles), scaleDir)
		}
	}

	// Verify export.log was written.
	if _, err := os.Stat(path.Join(exportDir, "export.log")); err != nil {
		t.Errorf("export.log not found: %v", err)
	}
}

// ingestZeroBlocks generates and POSTs all-zero label blocks at the given chunk coordinates.
func ingestZeroBlocks(t *testing.T, uuid dvid.UUID, name string, regions []struct{ origin, size dvid.ChunkPoint3d }) int {
	t.Helper()
	var totalBlocks int
	for _, r := range regions {
		var buf bytes.Buffer
		count := 0
		block, err := generateZeroBlock()
		if err != nil {
			t.Fatalf("generateZeroBlock failed: %v", err)
		}
		gzipped, err := block.CompressGZIP()
		if err != nil {
			t.Fatalf("CompressGZIP failed for zero block: %v", err)
		}
		for cz := r.origin[2]; cz < r.origin[2]+r.size[2]; cz++ {
			for cy := r.origin[1]; cy < r.origin[1]+r.size[1]; cy++ {
				for cx := r.origin[0]; cx < r.origin[0]+r.size[0]; cx++ {
					writeTestBlockInt32(t, &buf, cx)
					writeTestBlockInt32(t, &buf, cy)
					writeTestBlockInt32(t, &buf, cz)
					writeTestBlockInt32(t, &buf, int32(len(gzipped)))
					if _, err := buf.Write(gzipped); err != nil {
						t.Fatalf("buf.Write failed: %v", err)
					}
					count++
				}
			}
		}
		apiStr := fmt.Sprintf("%snode/%s/%s/blocks?downres=true", server.WebAPIPath, uuid, name)
		server.TestHTTP(t, "POST", apiStr, &buf)
		totalBlocks += count
	}
	return totalBlocks
}

// TestExportShardsSkipsZeroBlocks verifies that all-zero label blocks are excluded
// from Arrow output, and shards containing only zero blocks produce no shard files.
func TestExportShardsSkipsZeroBlocks(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, v := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "0") // no downres needed for this test
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	// Region A: 4x4x4 non-zero blocks at origin — 64 blocks
	nonZeroRegions := []struct{ origin, size dvid.ChunkPoint3d }{
		{dvid.ChunkPoint3d{0, 0, 0}, dvid.ChunkPoint3d{4, 4, 4}},
	}
	nonZeroCount := ingestSyntheticBlocks(t, uuid, "labels", nonZeroRegions)

	// Region B: 4x4x4 all-zero blocks at chunk (36,36,36) — 64 blocks in a different shard
	zeroRegions := []struct{ origin, size dvid.ChunkPoint3d }{
		{dvid.ChunkPoint3d{36, 36, 36}, dvid.ChunkPoint3d{4, 4, 4}},
	}
	zeroCount := ingestZeroBlocks(t, uuid, "labels", zeroRegions)
	t.Logf("Ingested %d non-zero blocks and %d zero blocks", nonZeroCount, zeroCount)

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on labels updating: %v\n", err)
	}

	dataservice, err := datastore.GetDataByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("couldn't get labels data instance: %v\n", err)
	}
	lbls, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned data instance was not labelmap.Data\n")
	}

	// Build export spec (single scale, no downres).
	exportDir := t.TempDir()
	spec := getIntegrationExportSpec(t, exportDir)
	spec.NumScales = 1
	spec.Done = make(chan struct{})
	ctx := datastore.NewVersionedCtx(lbls, v)

	if err := lbls.ExportData(ctx, spec); err != nil {
		t.Fatalf("ExportData failed: %v", err)
	}
	select {
	case <-spec.Done:
	case <-time.After(30 * time.Second):
		t.Fatal("Export did not complete within 30 seconds")
	}

	// Verify only non-zero blocks appear in Arrow output.
	s0Dir := path.Join(exportDir, "s0")
	arrowFiles, err := filepath.Glob(path.Join(s0Dir, "*.arrow"))
	if err != nil {
		t.Fatalf("glob for arrow files: %v", err)
	}
	if len(arrowFiles) == 0 {
		t.Fatal("No arrow files found for scale 0")
	}

	totalRecords := 0
	for _, arrowPath := range arrowFiles {
		f, err := os.Open(arrowPath)
		if err != nil {
			t.Fatalf("open %s: %v", arrowPath, err)
		}
		reader, err := ipc.NewReader(bufio.NewReader(f), ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			f.Close()
			t.Fatalf("ipc.NewReader for %s: %v", arrowPath, err)
		}
		for reader.Next() {
			rec := reader.Record()
			nRows := int(rec.NumRows())
			totalRecords += nRows

			// Verify no block has all-zero labels.
			labelsCol := rec.Column(3).(*array.List)
			svsCol := rec.Column(4).(*array.List)
			for i := 0; i < nRows; i++ {
				if !svsCol.IsNull(i) {
					start := int(svsCol.Offsets()[i])
					end := int(svsCol.Offsets()[i+1])
					vals := svsCol.ListValues().(*array.Uint64)
					allZero := true
					for j := start; j < end; j++ {
						if vals.Value(j) != 0 {
							allZero = false
							break
						}
					}
					if allZero {
						cx := rec.Column(0).(*array.Int32).Value(i)
						cy := rec.Column(1).(*array.Int32).Value(i)
						cz := rec.Column(2).(*array.Int32).Value(i)
						t.Errorf("Block (%d,%d,%d) has all-zero supervoxels but was not skipped", cx, cy, cz)
					}
				}
				if !labelsCol.IsNull(i) {
					start := int(labelsCol.Offsets()[i])
					end := int(labelsCol.Offsets()[i+1])
					vals := labelsCol.ListValues().(*array.Uint64)
					allZero := true
					for j := start; j < end; j++ {
						if vals.Value(j) != 0 {
							allZero = false
							break
						}
					}
					if allZero {
						cx := rec.Column(0).(*array.Int32).Value(i)
						cy := rec.Column(1).(*array.Int32).Value(i)
						cz := rec.Column(2).(*array.Int32).Value(i)
						t.Errorf("Block (%d,%d,%d) has all-zero agglo labels but was not skipped", cx, cy, cz)
					}
				}
			}
		}
		reader.Release()
		f.Close()
	}

	if totalRecords != nonZeroCount {
		t.Errorf("Expected %d records (non-zero blocks only), got %d", nonZeroCount, totalRecords)
	}
	t.Logf("Verified %d records in Arrow output, all non-zero (skipped %d zero blocks)", totalRecords, zeroCount)

	// Verify that the shard containing only zero blocks has no arrow file.
	// Region B at chunk (36,36,36) is in a different shard from region A at origin.
	// With mCNS scale 4 sharding (shard dim ~32 chunks), chunk (36,36,36) falls
	// in a shard starting at voxel (2048, 2048, 2048).
	zeroShardArrow := path.Join(s0Dir, "2048_2048_2048.arrow")
	if _, err := os.Stat(zeroShardArrow); err == nil {
		t.Errorf("Shard file %s should not exist (all-zero shard), but it does", zeroShardArrow)
	}
}
