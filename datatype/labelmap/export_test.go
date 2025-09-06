package labelmap

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

func TestMortonCode(t *testing.T) {
	scale := &ngScale{
		chunkBits: 10, // 2^10 = 1024 chunks per dimension
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
	if scale.chunkBits == 0 {
		t.Error("chunkBits should be set after initialize")
	}

	if scale.shardMask == 0 {
		t.Error("shardMask should be set after initialize")
	}

	// Verify chunk bits calculation
	// With size 1024 and chunk size 64, we have 1024/64 = 16 chunks per dimension
	// 16 chunks requires 4 bits (log2(16) = 4), so total = 4*3 = 12 bits
	expectedChunkBits := uint8(12)
	if scale.chunkBits != expectedChunkBits {
		t.Errorf("Expected chunkBits %d, got %d", expectedChunkBits, scale.chunkBits)
	}
}

func TestCalculateShardOrigin(t *testing.T) {
	handler := &shardHandler{}
	scale := &ngScale{
		ChunkSizes: []dvid.Point3d{{64, 64, 64}},
		chunkBits:  6, // 2^6 = 64
	}

	testCases := []struct {
		chunkCoord dvid.ChunkPoint3d
		expected   dvid.Point3d
	}{
		{dvid.ChunkPoint3d{0, 0, 0}, dvid.Point3d{0, 0, 0}},
		{dvid.ChunkPoint3d{1, 1, 1}, dvid.Point3d{64, 64, 64}},
		{dvid.ChunkPoint3d{2, 0, 1}, dvid.Point3d{128, 0, 64}},
		{dvid.ChunkPoint3d{-1, -1, -1}, dvid.Point3d{-64, -64, -64}},
	}

	for _, tc := range testCases {
		result := handler.calculateShardOrigin(tc.chunkCoord, scale)
		if result != tc.expected {
			t.Errorf("calculateShardOrigin(%v) = %v, want %v", tc.chunkCoord, result, tc.expected)
		}
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
