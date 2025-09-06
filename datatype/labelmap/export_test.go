package labelmap

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
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
	time.Sleep(1 * time.Second)

	dataservice, err := datastore.GetDataByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("couldn't get labels data instance from datastore: %v\n", err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned data instance for 'labels' instance was not labelmap.Data!\n")
	}

	// Run the Export
	ctx := datastore.NewVersionedCtx(labels, v)
	exportSpec := getExportSpec(t)
	err = labels.ExportData(ctx, exportSpec)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Periodically check for presence of 0_0_0.arrow and 0_0_0.json files
	// in /tmp/test_export/s0. Give up after half second.
	arrowFile := path.Join(exportSpec.Directory, "s0", "0_0_0.arrow")
	jsonFile := path.Join(exportSpec.Directory, "s0", "0_0_0.json")
	foundArrow := false
	foundJSON := false
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(arrowFile); err == nil {
			foundArrow = true
			if _, err := os.Stat(jsonFile); err == nil {
				foundJSON = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !foundArrow {
		t.Fatalf("After half second, did not find expected shard arrow file %q\n", arrowFile)
	}
	if !foundJSON {
		t.Fatalf("After half second, did not find expected shard json file %q\n", jsonFile)
	}
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
			w, err := handler.getWriter(actualShardID, 0, tc.chunkCoord)
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

		w1, err := handler.getWriter(shardID1, 0, chunk1)
		if err != nil {
			t.Fatalf("Failed to get shard writer for chunk %v: %v", chunk1, err)
		}
		w2, err := handler.getWriter(shardID2, 0, chunk2)
		if err != nil {
			t.Fatalf("Failed to get shard writer for chunk %v: %v", chunk2, err)
		}

		if w1 != w2 {
			t.Errorf("Chunks %v and %v should get same shard writer instance", chunk1, chunk2)
		}
	})
}
