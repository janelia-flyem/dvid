package labels64

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

func TestBaseAddMerge(t *testing.T) {
	tuples1 := MergeTuples{
		{20, 3, 5, 7},
		{30, 1, 6, 13, 19},
		{40, 2, 18},
	}
	tuples1.addMerge(98, 20)
	if len(tuples1[0]) != 5 {
		t.Errorf("Expected MergeTuples.addMerge() to add: %v\n", tuples1)
	}
}

// A single label block within the volume
type testBody struct {
	label        uint64
	offset, size dvid.Point3d
}

// A slice of bytes representing 3d label volume
type testVolume struct {
	data []byte
	size dvid.Point3d
}

func newTestVolume(nx, ny, nz int32) *testVolume {
	return &testVolume{
		data: make([]byte, nx*ny*nz*8),
		size: dvid.Point3d{nx, ny, nz},
	}
}

// Sets voxels in body to given label
func (v *testVolume) add(body testBody) {
	nx := v.size[0]
	nxy := nx * v.size[1]
	for z := body.offset[2]; z < body.offset[2]+body.size[2]; z++ {
		for y := body.offset[1]; y < body.offset[1]+body.size[1]; y++ {
			i := (z*nxy + y*nx + body.offset[0]) * 8
			for x := int32(0); x < body.size[0]; x++ {
				binary.LittleEndian.PutUint64(v.data[i:i+8], body.label)
				i += 8
			}
		}
	}
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func (v *testVolume) get(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	v.data = server.TestHTTP(t, "GET", apiStr, nil)
}

func (v *testVolume) equals(v2 *testVolume) bool {
	if !v.size.Equals(v2.size) {
		return false
	}
	if len(v.data) != len(v2.data) {
		return false
	}
	for i, value := range v.data {
		if value != v2.data[i] {
			return false
		}
	}
	return true
}

// Returns true if all voxels in test volume has given label.
func (v *testVolume) isLabel(label uint64, body *testBody) bool {
	var offset, size dvid.Point3d
	if body == nil {
		offset = dvid.Point3d{0, 0, 0}
		size = v.size
	} else {
		offset = body.offset
		size = body.size
	}
	nx := v.size[0]
	nxy := nx * v.size[1]
	for z := offset[2]; z < offset[2]+size[2]; z++ {
		for y := offset[1]; y < offset[1]+size[1]; y++ {
			i := (z*nxy + y*nx + offset[0]) * 8
			for x := int32(0); x < size[0]; x++ {
				curLabel := binary.LittleEndian.Uint64(v.data[i : i+8])
				if curLabel != label {
					return false
				}
				i += 8
			}
		}
	}
	return true
}

// Returns true if any voxel in test volume has given label.
func (v *testVolume) hasLabel(label uint64, body *testBody) bool {
	var offset, size dvid.Point3d
	if body == nil {
		offset = dvid.Point3d{0, 0, 0}
		size = v.size
	} else {
		offset = body.offset
		size = body.size
	}
	nx := v.size[0]
	nxy := nx * v.size[1]
	for z := offset[2]; z < offset[2]+size[2]; z++ {
		for y := offset[1]; y < offset[1]+size[1]; y++ {
			i := (z*nxy + y*nx + offset[0]) * 8
			for x := int32(0); x < size[0]; x++ {
				curLabel := binary.LittleEndian.Uint64(v.data[i : i+8])
				if curLabel == label {
					return true
				}
				i += 8
			}
		}
	}
	return false
}

type mergeJSON string

func (mjson mergeJSON) send(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer([]byte(mjson)))
}

var (
	body1 = testBody{
		label:  1,
		offset: dvid.Point3d{10, 40, 10},
		size:   dvid.Point3d{20, 20, 80},
	}
	body2 = testBody{
		label:  2,
		offset: dvid.Point3d{30, 20, 40},
		size:   dvid.Point3d{50, 50, 20},
	}
	body2a = testBody{
		label:  2,
		offset: dvid.Point3d{40, 40, 10},
		size:   dvid.Point3d{20, 20, 30},
	}
	body3 = testBody{
		label:  3,
		offset: dvid.Point3d{40, 40, 10},
		size:   dvid.Point3d{20, 20, 30},
	}
	body4 = testBody{
		label:  4,
		offset: dvid.Point3d{75, 40, 60},
		size:   dvid.Point3d{20, 20, 30},
	}
)

func createLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	// Setup test label blocks that are non-intersecting.
	volume := newTestVolume(100, 100, 100)
	volume.add(body1)
	volume.add(body2)
	if !volume.isLabel(2, &body2) {
		t.Errorf("Label 2 was incorrectly written in createLabelTestVolume!")
	}
	volume.add(body3)
	volume.add(body4)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

func TestMergeLabels(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create testbed labels64 volume
	repo, _ := initTestRepo()
	labelsName := "mylabels"
	uuid := repo.RootUUID()
	server.CreateTestInstance(t, uuid, "labels64", labelsName)
	vol := createLabelTestVolume(t, uuid, labelsName)

	// TODO -- Remove this hack in favor of whatever will be the method
	// for discerning denormalizations are not yet complete.
	time.Sleep(10 * time.Second)

	expected := newTestVolume(100, 100, 100)
	expected.add(body1)
	expected.add(body2)
	expected.add(body2a)
	expected.add(body4)

	if !vol.isLabel(2, &body2) {
		t.Errorf("Label 2 was incorrectly written!")
	}

	// Test merge 1
	testMerge := mergeJSON(`
		[ [2, 3] ]
	`)
	testMerge.send(t, uuid, labelsName)

	// Make sure changes are correct after completion
	retrieved := newTestVolume(100, 100, 100)
	retrieved.get(t, uuid, labelsName)
	if len(retrieved.data) != 8*100*100*100 {
		t.Errorf("Retrieved labels64 volume is incorrect size\n")
	}
	if !retrieved.isLabel(2, &body2) {
		t.Errorf("Expected label 2 original voxels to remain.  Instead some were removed.\n")
	}
	if retrieved.hasLabel(3, nil) {
		t.Errorf("Found label 3 when all label 3 should have been merged into label 2!\n")
	}
	if !retrieved.isLabel(2, &body3) {
		t.Errorf("Incomplete merging.  Label 2 should have taken over full extent of label 3\n")
	}
	if !retrieved.equals(expected) {
		t.Errorf("Merged label volume not equal to expected merged volume\n")
	}
}

func TestSplitLabel(t *testing.T) {
	// Create testbed labels64 volume

	// Split part of a label

	// Make sure changes are correct after completion
}
