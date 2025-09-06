package labelmap

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

func TestMortonCode(t *testing.T) {
	scale := &ngScale{
		volChunkBits: 10, // 2^10 = 1024 chunks per dimension
		numBits:   [3]uint8{10, 10, 10},
		maxBits:   30,
	}

	testCases := []struct {
		coord    dvid.ChunkPoint3d
		expected uint64
	}{
		{dvid.ChunkPoint3d{0, 0, 0}, 0},
		{dvid.ChunkPoint3d{1, 0, 0}, 1},
		{dvid.ChunkPoint3d{0, 1, 0}, 2},
		{dvid.ChunkPoint3d{0, 0, 1}, 4},
		{dvid.ChunkPoint3d{1, 1, 0}, 3},
		{dvid.ChunkPoint3d{1, 0, 1}, 5},
		{dvid.ChunkPoint3d{0, 1, 1}, 6},
		{dvid.ChunkPoint3d{1, 1, 1}, 7},
	}

	for _, tc := range testCases {
		result := scale.mortonCode(tc.coord)
		if result != tc.expected {
			t.Errorf("mortonCode(%v) = %d, want %d", tc.coord, result, tc.expected)
		}
	}
}

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

func TestNgScaleInitialize(t *testing.T) {
	scale := &ngScale{
		ChunkSizes: []dvid.Point3d{{64, 64, 64}},
		Size:       dvid.Point3d{1024, 1024, 1024}, // Total volume size
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "murmurhash3_x86_128",
			PreshiftBits:  1,
			MinishardBits: 6,
			ShardBits:     15,
		},
	}

	err := scale.initialize()
	if err != nil {
		t.Fatalf("initialize() failed: %v", err)
	}

	// Verify computed values
	if scale.volChunkBits == 0 {
		t.Error("volChunkBits should be set after initialize")
	}

	if scale.shardMask == 0 {
		t.Error("shardMask should be set after initialize")
	}

	// Verify chunk bits calculation
	// With size 1024 and chunk size 64, we have 1024/64 = 16 chunks per dimension
	// 16 chunks requires 4 bits (log2(16) = 4), so total = 4*3 = 12 bits
	expectedVolChunkBits := uint8(12)
	if scale.volChunkBits != expectedVolChunkBits {
		t.Errorf("Expected volChunkBits %d, got %d", expectedVolChunkBits, scale.volChunkBits)
	}
}


// Test variables similar to labelmap_test.go
var (
	exportLabelsT datastore.TypeService
	exportTestMu  sync.Mutex
)

// Test helper functions similar to labelmap_test.go
func initExportTestRepo(t *testing.T) (dvid.UUID, dvid.VersionID) {
	exportTestMu.Lock()
	defer exportTestMu.Unlock()
	if exportLabelsT == nil {
		var err error
		exportLabelsT, err = datastore.TypeServiceByName("labelmap")
		if err != nil {
			t.Fatalf("Can't get labelmap type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func newExportDataInstance(uuid dvid.UUID, t *testing.T, name dvid.InstanceName) *Data {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, exportLabelsT, name, config)
	if err != nil {
		t.Fatalf("Unable to create labelmap instance %q: %v\n", name, err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast labels data service into Data\n")
	}
	return labels
}

func TestShardHandlerWithRealSpecs(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Load the real neuroglancer specs
	data, err := os.ReadFile("../../test_data/mcns-ng-specs.json")
	if err != nil {
		t.Fatalf("Failed to read test spec file: %v", err)
	}

	var spec ngVolume
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("Failed to parse test spec: %v", err)
	}

	// Convert to export spec with directory
	exportSpec := exportSpec{
		ngVolume:  spec,
		Directory: "/tmp/test_export",
	}

	// Create DVID test repo and labelmap instance
	uuid, versionID := initExportTestRepo(t)
	lbls := newExportDataInstance(uuid, t, "test_labelmap")
	labelsCtx := datastore.NewVersionedCtx(lbls, versionID)

	// Create shard handler and test initialization
	handler := &shardHandler{}
	err = handler.Initialize(labelsCtx, exportSpec)
	if err != nil {
		t.Fatalf("ShardHandler initialization failed: %v", err)
	}

	// Verify shard Z sizes were calculated for all scales
	expectedScales := len(spec.Scales)
	if len(handler.shardZSize) != expectedScales {
		t.Errorf("Expected %d shard Z sizes, got %d", expectedScales, len(handler.shardZSize))
	}

	// Log the calculated shard Z sizes for each scale to verify they're reasonable
	for i, size := range handler.shardZSize {
		scale := &exportSpec.Scales[i]
		totalBits := int(scale.Sharding.ShardBits + scale.Sharding.PreshiftBits + scale.Sharding.MinishardBits)
		shardSideBits := totalBits / 3
		expectedSize := int32(1) << shardSideBits
		
		if shardSideBits < 0 {
			expectedSize = 1 // Due to our bounds checking
		}
		
		t.Logf("Scale %d: ShardBits=%d, PreshiftBits=%d, MinishardBits=%d → TotalBits=%d → ShardSideBits=%d → ShardZSize=%d (expected=%d)",
			i, scale.Sharding.ShardBits, scale.Sharding.PreshiftBits, scale.Sharding.MinishardBits,
			totalBits, shardSideBits, size, expectedSize)
			
		if size != expectedSize {
			t.Errorf("Scale %d: Expected shard Z size %d, got %d", i, expectedSize, size)
		}
		
		if size <= 0 {
			t.Errorf("Scale %d: Invalid shard Z size %d (should be positive)", i, size)
		}
	}
}

func TestShardFilenameMapping(t *testing.T) {
	// Create a test ngScale with known sharding parameters
	// Set up a large volume so we get meaningful chunk bits
	// For a volume of 1M x 1M x 1M voxels with 64^3 chunks, we get ~16K chunks per dimension
	// log2(16384) = 14 bits per dimension, so volChunkBits = 14*3 = 42
	scale := &ngScale{
		ChunkSizes: []dvid.Point3d{{64, 64, 64}},
		Size:       dvid.Point3d{1048576, 1048576, 1048576}, // 1M x 1M x 1M voxels
		Sharding: ngShard{
			FormatType:    "neuroglancer_uint64_sharded_v1",
			Hash:          "identity",
			PreshiftBits:  9,
			MinishardBits: 6,
			ShardBits:     15,
		},
	}
	
	// Initialize the scale to set up computed values
	err := scale.initialize()
	if err != nil {
		t.Fatalf("Failed to initialize scale: %v", err)
	}
	
	// Log the actual volume chunk bits calculated  
	t.Logf("Calculated volChunkBits: %d", scale.volChunkBits)
	
	handler := &shardHandler{}
	
	// Calculate expected shard size
	// Calculate neuroglancer shard size with proper voxel bits per chunk (not volume addressing bits)
	voxelBitsPerChunk := 18 // log2(64^3) = 18 bits per 64x64x64 chunk
	totalLowerBits := int(scale.Sharding.PreshiftBits + scale.Sharding.MinishardBits) + voxelBitsPerChunk
	bitsPerDim := totalLowerBits / 3
	shardSizeInVoxels := int32(1) << bitsPerDim  // 2^11 = 2048 voxels per dimension
	shardSizeInChunks := shardSizeInVoxels / 64  // 2048/64 = 32 chunks per dimension
	
	t.Logf("PreshiftBits=%d, MinishardBits=%d, VoxelBitsPerChunk=%d", 
		scale.Sharding.PreshiftBits, scale.Sharding.MinishardBits, voxelBitsPerChunk)
	t.Logf("Total lower bits=%d, bits per dim=%d", totalLowerBits, bitsPerDim)  
	t.Logf("Shard size: %d voxels per dim = %d chunks per dim", shardSizeInVoxels, shardSizeInChunks)
	
	// Test cases with specific chunk coordinates that should fall into different shards
	// With shard size of 32 chunks per dimension (2048 voxels), chunks beyond that should be in different shards
	testCases := []struct {
		name       string
		chunkCoord dvid.ChunkPoint3d
		expectedShardOrigin dvid.Point3d // Expected voxel origin of the shard
	}{
		{"Origin shard", dvid.ChunkPoint3d{0, 0, 0}, dvid.Point3d{0, 0, 0}},
		{"Still in origin shard", dvid.ChunkPoint3d{10, 20, 30}, dvid.Point3d{0, 0, 0}},
		{"Still in origin shard max", dvid.ChunkPoint3d{31, 31, 31}, dvid.Point3d{0, 0, 0}},
		{"Next X shard", dvid.ChunkPoint3d{32, 0, 0}, dvid.Point3d{shardSizeInVoxels, 0, 0}},
		{"Next Y shard", dvid.ChunkPoint3d{0, 32, 0}, dvid.Point3d{0, shardSizeInVoxels, 0}}, 
		{"Next Z shard", dvid.ChunkPoint3d{0, 0, 32}, dvid.Point3d{0, 0, shardSizeInVoxels}},
		{"Diagonal next shard", dvid.ChunkPoint3d{32, 32, 32}, dvid.Point3d{shardSizeInVoxels, shardSizeInVoxels, shardSizeInVoxels}},
		{"Large coords shard", dvid.ChunkPoint3d{100, 200, 300}, dvid.Point3d{
			(100 / shardSizeInChunks) * shardSizeInVoxels,
			(200 / shardSizeInChunks) * shardSizeInVoxels,
			(300 / shardSizeInChunks) * shardSizeInVoxels,
		}},
	}
	
	// Track filename to shard ID mapping to verify uniqueness
	filenameToShardID := make(map[string]uint64)
	shardIDToFilename := make(map[uint64]string)
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate shard ID using the scale's method
			actualShardID := scale.computeShardID(tc.chunkCoord[0], tc.chunkCoord[1], tc.chunkCoord[2])
			
			// Calculate the shard origin using our method
			shardOrigin := handler.calculateShardOriginFromID(actualShardID, tc.chunkCoord, scale)
			filename := fmt.Sprintf("%d_%d_%d.arrow", shardOrigin[0], shardOrigin[1], shardOrigin[2])
			
			t.Logf("Chunk %v -> Shard ID %d -> Origin %v -> Filename %s", 
				tc.chunkCoord, actualShardID, shardOrigin, filename)
				
			// Verify the calculated shard origin matches expected
			if shardOrigin != tc.expectedShardOrigin {
				t.Errorf("Chunk %v: Expected shard origin %v, got %v", 
					tc.chunkCoord, tc.expectedShardOrigin, shardOrigin)
			}
			
			// Verify that the same shard ID always produces the same filename
			if existingFilename, exists := shardIDToFilename[actualShardID]; exists {
				if existingFilename != filename {
					t.Errorf("Shard ID %d produced different filenames: %s vs %s", actualShardID, existingFilename, filename)
				}
			} else {
				shardIDToFilename[actualShardID] = filename
			}
			
			// Verify that different shard IDs produce different filenames (filename uniqueness)
			if existingShardID, exists := filenameToShardID[filename]; exists {
				if existingShardID != actualShardID {
					t.Errorf("Filename %s maps to multiple shard IDs: %d and %d", filename, existingShardID, actualShardID)
				}
			} else {
				filenameToShardID[filename] = actualShardID
			}
		})
	}
	
	// Test that chunks mapping to the same shard ID produce the same filename
	t.Run("SameShardSameFilename", func(t *testing.T) {
		// Find chunks that should map to the same shard (if any exist)
		chunk1 := dvid.ChunkPoint3d{10, 20, 30}
		chunk2 := dvid.ChunkPoint3d{11, 20, 30} // Slightly different coordinate
		
		shardID1 := scale.computeShardID(chunk1[0], chunk1[1], chunk1[2])
		shardID2 := scale.computeShardID(chunk2[0], chunk2[1], chunk2[2])
		
		shardOrigin1 := handler.calculateShardOriginFromID(shardID1, chunk1, scale)
		shardOrigin2 := handler.calculateShardOriginFromID(shardID2, chunk2, scale)
		
		filename1 := fmt.Sprintf("%d_%d_%d.arrow", shardOrigin1[0], shardOrigin1[1], shardOrigin1[2])
		filename2 := fmt.Sprintf("%d_%d_%d.arrow", shardOrigin2[0], shardOrigin2[1], shardOrigin2[2])
		
		if shardID1 == shardID2 {
			// If they map to the same shard, filenames must be identical
			if filename1 != filename2 {
				t.Errorf("Chunks with same shard ID %d produced different filenames: %s vs %s", shardID1, filename1, filename2)
			}
			t.Logf("Chunks %v and %v both map to shard %d -> filename %s", chunk1, chunk2, shardID1, filename1)
		} else {
			// If they map to different shards, filenames must be different
			if filename1 == filename2 {
				t.Errorf("Chunks with different shard IDs (%d, %d) produced same filename: %s", shardID1, shardID2, filename1)
			}
			t.Logf("Chunk %v -> shard %d -> filename %s", chunk1, shardID1, filename1)
			t.Logf("Chunk %v -> shard %d -> filename %s", chunk2, shardID2, filename2)
		}
	})
}

func TestShardFilenameConsistency(t *testing.T) {
	// Test with the real neuroglancer specs to ensure consistency
	data, err := os.ReadFile("../../test_data/mcns-ng-specs.json")
	if err != nil {
		t.Fatalf("Failed to read test spec file: %v", err)
	}

	var spec ngVolume
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("Failed to parse test spec: %v", err)
	}

	handler := &shardHandler{}
	
	// Test each scale in the spec
	for scaleIdx, scale := range spec.Scales {
		t.Run(fmt.Sprintf("Scale%d", scaleIdx), func(t *testing.T) {
			err := scale.initialize()
			if err != nil {
				t.Fatalf("Failed to initialize scale %d: %v", scaleIdx, err)
			}
			
			// Test a variety of chunk coordinates
			testChunks := []dvid.ChunkPoint3d{
				{0, 0, 0},
				{1, 0, 0},
				{0, 1, 0},
				{0, 0, 1},
				{5, 10, 15},
				{100, 200, 300},
			}
			
			filenameMap := make(map[uint64]string)
			
			for _, chunk := range testChunks {
				shardID := scale.computeShardID(chunk[0], chunk[1], chunk[2])
				shardOrigin := handler.calculateShardOriginFromID(shardID, chunk, &scale)
				filename := fmt.Sprintf("%d_%d_%d.arrow", shardOrigin[0], shardOrigin[1], shardOrigin[2])
				
				// Verify consistency: same shard ID should always produce same filename
				if existingFilename, exists := filenameMap[shardID]; exists {
					if existingFilename != filename {
						t.Errorf("Scale %d: Shard ID %d produced inconsistent filenames: %s vs %s", 
							scaleIdx, shardID, existingFilename, filename)
					}
				} else {
					filenameMap[shardID] = filename
				}
				
				t.Logf("Scale %d: Chunk %v -> Shard ID %d -> Filename %s", 
					scaleIdx, chunk, shardID, filename)
			}
		})
	}
}
