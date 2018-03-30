package labels

import (
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datatype/common/proto"
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
		87:    289,
		382:   400,
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
		673:    2389,
		8763:   25463,
		26029:  63560,
		356983: 486927,
	}
	idx.Blocks[block3] = svc
	idx.Label = 199

	cleaveIdx := idx.Cleave(200, []uint64{1001, 26029, 3829})
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
}
