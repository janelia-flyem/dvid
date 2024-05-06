package labels

import (
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
)

func TestIndexOps(t *testing.T) {
	var idx Index
	idx.Blocks = make(map[uint64]*proto.SVCount)

	block1 := EncodeBlockIndex(1, 1, 1)
	x, y, z := DecodeBlockIndex(block1)
	if x != 1 || y != 1 || z != 1 {
		t.Errorf("bad block index encoding\n")
	}
	svc := new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		23:      100,
		1001:    899,
		11890:   357,
		8473291: 20000,
	}
	idx.Blocks[block1] = svc
	if idx.NumVoxels() != 21356 {
		t.Errorf("bad NumVoxels(), get %d, expected 21356\n", idx.NumVoxels())
	}
	expected := Set{
		23:      struct{}{},
		1001:    struct{}{},
		11890:   struct{}{},
		8473291: struct{}{},
	}
	supervoxels := idx.GetSupervoxels()
	if len(supervoxels) != 4 || !reflect.DeepEqual(supervoxels, expected) {
		t.Errorf("expected %v, got %v\n", expected, supervoxels)
	}

	block2 := EncodeBlockIndex(10, 24837, 890)
	x, y, z = DecodeBlockIndex(block2)
	if x != 10 || y != 24837 || z != 890 {
		t.Errorf("bad block index encoding\n")
	}
	izyx := BlockIndexToIZYXString(block2)
	chunkPt, err := izyx.ToChunkPoint3d()
	if err != nil {
		t.Error(err)
	}
	if chunkPt[0] != 10 || chunkPt[1] != 24837 || chunkPt[2] != 890 {
		t.Error("bad block index to IZYXString")
	}
	svc = new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		23:    11,
		87:    289,
		382:   400,
		1001:  1000,
		3829:  10000,
		9584:  15000,
		29284: 3819,
	}
	idx.Blocks[block2] = svc

	block3 := EncodeBlockIndex(87, 283, 3855)
	x, y, z = DecodeBlockIndex(block3)
	if x != 87 || y != 283 || z != 3855 {
		t.Errorf("bad block index encoding\n")
	}
	svc = new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		23:     14,
		673:    2389,
		1001:   5000,
		8763:   25463,
		26029:  63560,
		356983: 486927,
	}
	idx.Blocks[block3] = svc
	idx.Label = 199

	sidx, err := idx.LimitToSupervoxel(673)
	if err != nil {
		t.Error(err)
	}
	limitCounts := map[uint64]uint64{
		673: 2389,
	}
	if !reflect.DeepEqual(limitCounts, sidx.GetSupervoxelCounts()) {
		t.Errorf("after cleave, remain index has incorrect counts:\nExpected %v\nGot %v\n", limitCounts, sidx.GetSupervoxelCounts())
	}
	if len(sidx.Blocks) != 1 {
		t.Errorf("expected supervoxel limits count to be 1, got %d\n", len(sidx.Blocks))
	}
	_, found := sidx.Blocks[block3]
	if !found {
		t.Errorf("didn't find expected block after LimitToSupervoxel()\n")
	}

	origCounts := idx.GetSupervoxelCounts()
	var totalVoxels uint64
	for _, count := range origCounts {
		totalVoxels += count
	}
	if totalVoxels != 635228 {
		t.Errorf("expected 635228 total voxels, got %d voxels in index supervoxel counts\n", totalVoxels)
	}

	cleavedSize, remainSize, cleaveIdx := idx.Cleave(200, []uint64{1001, 26029, 3829}, dvid.MutInfo{})
	expectedCleavedSize := uint64(6899 + 63560 + 10000)
	if cleavedSize != expectedCleavedSize {
		t.Errorf("expected cleaved size to be %d, got %d\n", expectedCleavedSize, cleavedSize)
	}
	if remainSize != totalVoxels-cleavedSize {
		t.Errorf("expected remaining size to be %d, got %d\n", totalVoxels-cleavedSize, remainSize)
	}
	supervoxels = idx.GetSupervoxels()
	mainBodySupervoxels := Set{
		23:      struct{}{},
		11890:   struct{}{},
		8473291: struct{}{},
		87:      struct{}{},
		382:     struct{}{},
		9584:    struct{}{},
		29284:   struct{}{},
		673:     struct{}{},
		8763:    struct{}{},
		356983:  struct{}{},
	}
	if !reflect.DeepEqual(supervoxels, mainBodySupervoxels) {
		t.Errorf("after cleave, remain index is weird.  Expected %v, got %v\n", mainBodySupervoxels, supervoxels)
	}
	cleaveSupervoxels := Set{
		26029: struct{}{},
		3829:  struct{}{},
		1001:  struct{}{},
	}
	supervoxels = cleaveIdx.GetSupervoxels()
	if !reflect.DeepEqual(supervoxels, cleaveSupervoxels) {
		t.Errorf("after cleave, the cleaved index is weird.  Expected %v, got %v\n", cleaveSupervoxels, supervoxels)
	}

	mainCounts := map[uint64]uint64{
		23:      125,
		11890:   357,
		8473291: 20000,
		87:      289,
		382:     400,
		9584:    15000,
		29284:   3819,
		673:     2389,
		8763:    25463,
		356983:  486927,
	}
	cleaveCounts := map[uint64]uint64{
		1001:  6899,
		3829:  10000,
		26029: 63560,
	}
	if idx.GetSupervoxelCount(23) != 125 {
		t.Error("bad count")
	}
	if idx.GetSupervoxelCount(9584) != 15000 {
		t.Error("bad count")
	}
	if cleaveIdx.GetSupervoxelCount(1001) != 6899 {
		t.Error("bad count")
	}
	if !reflect.DeepEqual(mainCounts, idx.GetSupervoxelCounts()) {
		t.Errorf("after cleave, remain index has incorrect counts:\nExpected %v\nGot %v\n", mainCounts, idx.GetSupervoxelCounts())
	}
	if !reflect.DeepEqual(cleaveCounts, cleaveIdx.GetSupervoxelCounts()) {
		t.Errorf("after cleave, remain index has incorrect counts:\nExpected %v\nGot %v\n", cleaveCounts, cleaveIdx.GetSupervoxelCounts())
	}
	if err := idx.Add(cleaveIdx, dvid.MutInfo{}); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(origCounts, idx.GetSupervoxelCounts()) {
		t.Errorf("after add, recombined index has incorrect counts:\nExpected %v\nGot %v\n", origCounts, idx.GetSupervoxelCounts())
	}

	// test GetProcessedBlockIndices()

	blockIndices := idx.GetBlockIndices()
	expectedBlocks := map[dvid.IZYXString]struct{}{
		BlockIndexToIZYXString(block1): struct{}{},
		BlockIndexToIZYXString(block2): struct{}{},
		BlockIndexToIZYXString(block3): struct{}{},
	}
	gotBlocks := make(map[dvid.IZYXString]struct{}, 3)
	for _, zyx := range blockIndices {
		gotBlocks[zyx] = struct{}{}
	}
	if !reflect.DeepEqual(expectedBlocks, gotBlocks) {
		t.Errorf("after add, recombined index has incorrect blocks:\nExpected %v\nGot %v\n", expectedBlocks, gotBlocks)
	}
	var totVoxels uint64
	for _, count := range mainCounts {
		totVoxels += count
	}
	for _, count := range cleaveCounts {
		totVoxels += count
	}
	if idx.NumVoxels() != totVoxels {
		t.Errorf("expected %d total voxels, got %d\n", totVoxels, idx.NumVoxels())
	}

	var bounds dvid.Bounds
	bounds.Block = new(dvid.OptionalBounds)
	bounds.Block.SetMinX(2)
	bounds.Block.SetMinY(2)
	bounds.Block.SetMaxZ(3800)
	blockIndices, err = idx.GetProcessedBlockIndices(0, bounds, 0)
	if err != nil {
		t.Error(err)
	}
	expectedBlocks = map[dvid.IZYXString]struct{}{
		BlockIndexToIZYXString(block2): struct{}{},
	}
	gotBlocks = make(map[dvid.IZYXString]struct{}, 3)
	for _, zyx := range blockIndices {
		gotBlocks[zyx] = struct{}{}
	}
	if !reflect.DeepEqual(expectedBlocks, gotBlocks) {
		t.Errorf("bounded recombined index has incorrect blocks:\nExpected %v\nGot %v\n", expectedBlocks, gotBlocks)
	}

	// test ModifyBlocks
	var sc SupervoxelChanges
	if err := idx.ModifyBlocks(23, sc); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(origCounts, idx.GetSupervoxelCounts()) {
		t.Errorf("after nil modify blocks, full index has incorrect counts:\nExpected %v\nGot %v\n", origCounts, idx.GetSupervoxelCounts())
	}
	sc = make(SupervoxelChanges)
	sc[23] = make(map[dvid.IZYXString]int32)
	sc[23][dvid.ChunkPoint3d{87, 283, 3855}.ToIZYXString()] = -5
	sc[23][dvid.ChunkPoint3d{1, 1, 1}.ToIZYXString()] = 8
	if err := idx.ModifyBlocks(23, sc); err != nil {
		t.Error(err)
	}
	testCounts := origCounts
	testCounts[23] += 3
	if !reflect.DeepEqual(testCounts, idx.GetSupervoxelCounts()) {
		t.Errorf("after modify blocks, full index has incorrect counts:\nExpected %v\nGot %v\n", testCounts, idx.GetSupervoxelCounts())
	}

	sc[23][dvid.ChunkPoint3d{87, 283, 3855}.ToIZYXString()] = -50
	if err := idx.ModifyBlocks(23, sc); err == nil {
		t.Errorf("expected error in subtracting too many voxels in block, but got none!\n")
	}
}

func TestCleaveIndex(t *testing.T) {
	var idx Index
	idx.Blocks = make(map[uint64]*proto.SVCount)

	block1 := EncodeBlockIndex(1, 1, 1)
	svc := new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		23:      100,
		1001:    899,
		11890:   357,
		8473291: 20000,
	}
	idx.Blocks[block1] = svc

	block2 := EncodeBlockIndex(10, 24837, 890)
	svc = new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		87:   289,
		382:  400,
		1001: 1000,
		3829: 10000,
	}
	idx.Blocks[block2] = svc

	block3 := EncodeBlockIndex(87, 283, 3855)
	svc = new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		673:  2389,
		1001: 5000,
		8763: 25463,
	}
	idx.Blocks[block3] = svc
	idx.Label = 199

	block4 := EncodeBlockIndex(2, 1, 1)
	svc = new(proto.SVCount)
	svc.Counts = map[uint64]uint32{
		23:      111,
		41234:   7,
		8473291: 7381,
	}
	idx.Blocks[block4] = svc

	cleavedSize, remainSize, cleaveIdx := idx.Cleave(200, []uint64{87, 382, 1001, 3829, 673, 8763}, dvid.MutInfo{})
	expectedTotalSize := uint64(73396)
	if cleavedSize+remainSize != expectedTotalSize {
		t.Errorf("cleaved %d + remain %d voxels != %d total voxels\n", cleavedSize, remainSize, expectedTotalSize)
	}
	if cleavedSize != 45440 {
		t.Errorf("cleaved voxels %d != 45440\n", cleavedSize)
	}
	supervoxels := idx.GetSupervoxels()
	mainBodySupervoxels := Set{
		23:      struct{}{},
		11890:   struct{}{},
		41234:   struct{}{},
		8473291: struct{}{},
	}
	if !reflect.DeepEqual(supervoxels, mainBodySupervoxels) {
		t.Errorf("after cleave, remain index is weird.  Expected %v, got %v\n", mainBodySupervoxels, supervoxels)
	}
	if len(idx.Blocks) != 2 {
		t.Fatalf("after cleave, expected remain index to have 2 blocks, got %d\n", len(idx.Blocks))
	}
	if len(cleaveIdx.Blocks) != 3 {
		t.Errorf("after cleave, expected cleaved index to have 3 blocks, got %d\n", len(cleaveIdx.Blocks))
	}
	cleaveSupervoxels := Set{
		87:   struct{}{},
		382:  struct{}{},
		673:  struct{}{},
		1001: struct{}{},
		3829: struct{}{},
		8763: struct{}{},
	}
	supervoxels = cleaveIdx.GetSupervoxels()
	if !reflect.DeepEqual(supervoxels, cleaveSupervoxels) {
		t.Errorf("after cleave, the cleaved index is weird.  Expected %v, got %v\n", cleaveSupervoxels, supervoxels)
	}

	remainCounts1 := map[uint64]uint32{
		23:      100,
		11890:   357,
		8473291: 20000,
	}
	svc, found := idx.Blocks[block1]
	if !found || svc == nil {
		t.Fatalf("expected remain index to have block %s\n", BlockIndexToIZYXString(block1))
	}
	if !reflect.DeepEqual(svc.Counts, remainCounts1) {
		t.Errorf("expected remain index to be %v, got %v\n", remainCounts1, svc.Counts)
	}
	remainCounts4 := map[uint64]uint32{
		23:      111,
		41234:   7,
		8473291: 7381,
	}
	svc, found = idx.Blocks[block4]
	if !found || svc == nil {
		t.Fatalf("expected remain index to have block %s\n", BlockIndexToIZYXString(block4))
	}
	if !reflect.DeepEqual(svc.Counts, remainCounts4) {
		t.Errorf("expected remain index to be %v, got %v\n", remainCounts4, svc.Counts)
	}

	cleaveCounts1 := map[uint64]uint32{
		1001: 899,
	}
	svc, found = cleaveIdx.Blocks[block1]
	if !found || svc == nil {
		t.Fatalf("expected cleave index to have block %s\n", BlockIndexToIZYXString(block1))
	}
	if !reflect.DeepEqual(svc.Counts, cleaveCounts1) {
		t.Errorf("expected cleave index for %s to be %v, got %v\n", BlockIndexToIZYXString(block1), cleaveCounts1, svc.Counts)
	}
	cleaveCounts2 := map[uint64]uint32{
		87:   289,
		382:  400,
		1001: 1000,
		3829: 10000,
	}
	svc, found = cleaveIdx.Blocks[block2]
	if !found || svc == nil {
		t.Fatalf("expected cleave index to have block %s\n", BlockIndexToIZYXString(block2))
	}
	if !reflect.DeepEqual(svc.Counts, cleaveCounts2) {
		t.Errorf("expected cleave index for %s to be %v, got %v\n", BlockIndexToIZYXString(block2), cleaveCounts2, svc.Counts)
	}
	cleaveCounts3 := map[uint64]uint32{
		673:  2389,
		1001: 5000,
		8763: 25463,
	}
	svc, found = cleaveIdx.Blocks[block3]
	if !found || svc == nil {
		t.Fatalf("expected cleave index to have block %s\n", BlockIndexToIZYXString(block3))
	}
	if !reflect.DeepEqual(svc.Counts, cleaveCounts3) {
		t.Errorf("expected cleave index for %s to be %v, got %v\n", BlockIndexToIZYXString(block3), cleaveCounts3, svc.Counts)
	}
}
