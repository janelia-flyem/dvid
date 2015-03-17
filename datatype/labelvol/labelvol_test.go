// Tests sparsevol variants and merge/split.
// Test body data is at end of file.

package labelvol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	labelsT datastore.TypeService
	testMu  sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if labelsT == nil {
		var err error
		labelsT, err = datastore.TypeServiceByName("labelblk")
		if err != nil {
			log.Fatalf("Can't get labelblk type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// A single label block within the volume
type testBody struct {
	label        uint64
	offset, size dvid.Point3d
	blockSpans   dvid.Spans
	voxelSpans   dvid.Spans
}

// Makes sure the coarse sparse volume encoding matches the body.
func (b testBody) checkCoarse(t *testing.T, encoding []byte) {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Errorf("Error in decoding coarse sparse volume: %s\n", err.Error())
		return
	}

	// Check those spans match the body voxels.
	if !reflect.DeepEqual(spans, b.blockSpans) {
		t.Errorf("Expected spans for label %d:\n%s\nGot spans:\n%s\n", b.label, b.blockSpans, spans)
	}
}

// Makes sure the sparse volume encoding matches the actual body voxels.
func (b testBody) checkSparseVol(t *testing.T, encoding []byte, bounds dvid.Bounds) {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Errorf("Error in decoding sparse volume: %s\n", err.Error())
		return
	}

	// Create potentially bounded spans
	expected := dvid.Spans{}
	if bounds.IsSet() {
		for _, span := range b.voxelSpans {
			if bounds.OutsideY(span[1]) || bounds.OutsideZ(span[0]) {
				continue
			}
			expected = append(expected, span)
		}
	} else {
		expected = b.voxelSpans
	}

	// Check those spans match the body voxels.
	if !reflect.DeepEqual(spans, expected) {
		t.Errorf("Expected spans for label %d:\n%s\nGot spans:\n%s\n", b.label, expected, spans)
	}
}

func checkSpans(t *testing.T, encoding []byte, minx, maxx int32) {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Errorf("Error in decoding coarse sparse volume: %s\n", err.Error())
		return
	}
	for _, span := range spans {
		if span[2] < minx {
			t.Errorf("Found span violating min x %d: %s\n", minx, span)
			return
		}
		if span[3] > maxx {
			t.Errorf("Found span violating max x %d: %s\n", maxx, span)
		}
	}
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

// Sets voxels in body to given label or if 0, the label of the passed testBody
func (v *testVolume) add(body testBody, label uint64) {
	if label == 0 {
		label = body.label
	}
	nx := v.size[0]
	nxy := nx * v.size[1]
	for z := body.offset[2]; z < body.offset[2]+body.size[2]; z++ {
		for y := body.offset[1]; y < body.offset[1]+body.size[1]; y++ {
			i := (z*nxy + y*nx + body.offset[0]) * 8
			for x := int32(0); x < body.size[0]; x++ {
				binary.LittleEndian.PutUint64(v.data[i:i+8], label)
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

func createLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	// Setup test label blocks that are non-intersecting.
	volume := newTestVolume(128, 128, 128)
	volume.add(body1, 0)
	volume.add(body2, 0)
	if !volume.isLabel(2, &body2) {
		t.Errorf("Label 2 was incorrectly written in createLabelTestVolume!")
	}
	volume.add(body3, 0)
	volume.add(body4, 0)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

func TestSparseVolumes(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create testbed volume and data instances
	repo, _ := initTestRepo()
	uuid := repo.RootUUID()
	var config dvid.Config
	config.Set("sync", "bodies")
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	config.Clear()
	config.Set("sync", "labels")
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	// Populte the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	// TODO -- Remove this hack in favor of whatever will be the method
	// for discerning denormalizations are not yet complete.
	time.Sleep(10 * time.Second)

	for _, label := range []uint64{1, 3, 4} {
		// Get the coarse sparse volumes for each label and make sure they are correct.
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol-coarse/%d", server.WebAPIPath, uuid, "bodies", label)
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkCoarse(t, encoding)
	}

	for _, label := range []uint64{1, 2, 3, 4} {
		// Check full sparse volumes
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkSparseVol(t, encoding, dvid.Bounds{})

		// Check Y/Z restriction
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d?miny=30&maxy=50&minz=20&maxz=40&exact=true", server.WebAPIPath, uuid, "bodies", label)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		var bound dvid.Bounds
		bound.SetMinY(30)
		bound.SetMaxY(50)
		bound.SetMinZ(20)
		bound.SetMaxZ(40)
		bodies[label-1].checkSparseVol(t, encoding, bound)

		// Check X restriction
		minx := int32(20)
		maxx := int32(47)
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d?minx=%d&maxx=%d&exact=true", server.WebAPIPath, uuid, "bodies", label, minx, maxx)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		checkSpans(t, encoding, minx, maxx)
	}
}

func TestMergeLabels(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create testbed volume and data instances
	repo, _ := initTestRepo()
	uuid := repo.RootUUID()
	var config dvid.Config
	config.Set("sync", "bodies")
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	config.Clear()
	config.Set("sync", "labels")
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	vol := createLabelTestVolume(t, uuid, "labels")

	// TODO -- Remove this hack in favor of whatever will be the method
	// for discerning denormalizations are not yet complete.
	time.Sleep(10 * time.Second)

	// Make sure max label is consistent
	reqStr := fmt.Sprintf("%snode/%s/%s/maxlabel", server.WebAPIPath, uuid, "bodies")
	r := server.TestHTTP(t, "GET", reqStr, nil)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get maxlabel from server.  Instead got: %v\n", jsonVal)
	}
	maxlabel, ok := jsonVal["maxlabel"]
	if !ok {
		t.Errorf("The maxlabel query did not yield max label.  Instead got: %v\n", jsonVal)
	}
	if maxlabel != 4 {
		t.Errorf("Expected max label to be 4, instead got %d\n", maxlabel)
	}

	expected := newTestVolume(128, 128, 128)
	expected.add(body1, 0)
	expected.add(body2, 0)
	expected.add(body3, 2)
	expected.add(body4, 0)

	if !vol.isLabel(2, &body2) {
		t.Errorf("Label 2 was incorrectly written!")
	}

	// Test merge 1
	testMerge := mergeJSON(`[2, 3]`)
	testMerge.send(t, uuid, "bodies")

	// Make sure changes are correct after completion
	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved labelvol volume is incorrect size\n")
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
	tests.UseStore()
	defer tests.CloseStore()

	// Create testbed volume and data instances
	repo, _ := initTestRepo()
	uuid := repo.RootUUID()
	var config dvid.Config
	config.Set("sync", "bodies")
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	config.Clear()
	config.Set("sync", "labels")
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	// Store body 4 in as labels
	labels := newTestVolume(128, 128, 128)
	labels.add(body4, 0)
	labels.put(t, uuid, "labels")

	// TODO -- Remove this hack in favor of whatever will be the method
	// for discerning denormalizations are not yet complete.
	time.Sleep(10 * time.Second)

	// Create the sparsevol encoding for body 4a
	numspans := len(bodysplit.voxelSpans)
	rles := make(dvid.RLEs, numspans, numspans)
	for i, span := range bodysplit.voxelSpans {
		start := dvid.Point3d{span[2], span[1], span[0]}
		length := span[3] - span[2] + 1
		rles[i] = dvid.NewRLE(start, length)
	}

	// Create the sparse volume binary
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))         // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))          // dimension of run (X = 0)
	buf.WriteByte(byte(0))                                   // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0))        // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(numspans)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %s\n", err.Error())
	}
	buf.Write(rleBytes)

	// Submit the split sparsevol for body 4a
	reqStr := fmt.Sprintf("%snode/%s/%s/split/%d", server.WebAPIPath, uuid, "bodies", 4)
	r := server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}
	newlabel, ok := jsonVal["label"]
	if !ok {
		t.Errorf("The split request did not yield label value.  Instead got: %v\n", jsonVal)
	}
	if newlabel != 5 {
		t.Errorf("Expected split label to be 5, instead got %d\n", newlabel)
	}

	// Make sure labels are correct

	// Make sure new body 5 is what we sent
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 5)
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	bodysplit.checkSparseVol(t, encoding, dvid.Bounds{})

	// Make sure sparsevol for original body 4 is correct
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 4)
	encoding = server.TestHTTP(t, "GET", reqStr, nil)
	bodyleft.checkSparseVol(t, encoding, dvid.Bounds{})
}

var (
	bodies = []testBody{
		{
			label:  1,
			offset: dvid.Point3d{10, 40, 10},
			size:   dvid.Point3d{20, 20, 80},
			blockSpans: []dvid.Span{
				dvid.Span{0, 1, 0, 0},
				dvid.Span{1, 1, 0, 0},
				dvid.Span{2, 1, 0, 0},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{10, 40, 10, 29}, dvid.Span{10, 41, 10, 29}, dvid.Span{10, 42, 10, 29}, dvid.Span{10, 43, 10, 29}, dvid.Span{10, 44, 10, 29},
				dvid.Span{10, 45, 10, 29}, dvid.Span{10, 46, 10, 29}, dvid.Span{10, 47, 10, 29}, dvid.Span{10, 48, 10, 29}, dvid.Span{10, 49, 10, 29},
				dvid.Span{10, 50, 10, 29}, dvid.Span{10, 51, 10, 29}, dvid.Span{10, 52, 10, 29}, dvid.Span{10, 53, 10, 29}, dvid.Span{10, 54, 10, 29},
				dvid.Span{10, 55, 10, 29}, dvid.Span{10, 56, 10, 29}, dvid.Span{10, 57, 10, 29}, dvid.Span{10, 58, 10, 29}, dvid.Span{10, 59, 10, 29},
				dvid.Span{11, 40, 10, 29}, dvid.Span{11, 41, 10, 29}, dvid.Span{11, 42, 10, 29}, dvid.Span{11, 43, 10, 29}, dvid.Span{11, 44, 10, 29},
				dvid.Span{11, 45, 10, 29}, dvid.Span{11, 46, 10, 29}, dvid.Span{11, 47, 10, 29}, dvid.Span{11, 48, 10, 29}, dvid.Span{11, 49, 10, 29},
				dvid.Span{11, 50, 10, 29}, dvid.Span{11, 51, 10, 29}, dvid.Span{11, 52, 10, 29}, dvid.Span{11, 53, 10, 29}, dvid.Span{11, 54, 10, 29},
				dvid.Span{11, 55, 10, 29}, dvid.Span{11, 56, 10, 29}, dvid.Span{11, 57, 10, 29}, dvid.Span{11, 58, 10, 29}, dvid.Span{11, 59, 10, 29},
				dvid.Span{12, 40, 10, 29}, dvid.Span{12, 41, 10, 29}, dvid.Span{12, 42, 10, 29}, dvid.Span{12, 43, 10, 29}, dvid.Span{12, 44, 10, 29},
				dvid.Span{12, 45, 10, 29}, dvid.Span{12, 46, 10, 29}, dvid.Span{12, 47, 10, 29}, dvid.Span{12, 48, 10, 29}, dvid.Span{12, 49, 10, 29},
				dvid.Span{12, 50, 10, 29}, dvid.Span{12, 51, 10, 29}, dvid.Span{12, 52, 10, 29}, dvid.Span{12, 53, 10, 29}, dvid.Span{12, 54, 10, 29},
				dvid.Span{12, 55, 10, 29}, dvid.Span{12, 56, 10, 29}, dvid.Span{12, 57, 10, 29}, dvid.Span{12, 58, 10, 29}, dvid.Span{12, 59, 10, 29},
				dvid.Span{13, 40, 10, 29}, dvid.Span{13, 41, 10, 29}, dvid.Span{13, 42, 10, 29}, dvid.Span{13, 43, 10, 29}, dvid.Span{13, 44, 10, 29},
				dvid.Span{13, 45, 10, 29}, dvid.Span{13, 46, 10, 29}, dvid.Span{13, 47, 10, 29}, dvid.Span{13, 48, 10, 29}, dvid.Span{13, 49, 10, 29},
				dvid.Span{13, 50, 10, 29}, dvid.Span{13, 51, 10, 29}, dvid.Span{13, 52, 10, 29}, dvid.Span{13, 53, 10, 29}, dvid.Span{13, 54, 10, 29},
				dvid.Span{13, 55, 10, 29}, dvid.Span{13, 56, 10, 29}, dvid.Span{13, 57, 10, 29}, dvid.Span{13, 58, 10, 29}, dvid.Span{13, 59, 10, 29},
				dvid.Span{14, 40, 10, 29}, dvid.Span{14, 41, 10, 29}, dvid.Span{14, 42, 10, 29}, dvid.Span{14, 43, 10, 29}, dvid.Span{14, 44, 10, 29},
				dvid.Span{14, 45, 10, 29}, dvid.Span{14, 46, 10, 29}, dvid.Span{14, 47, 10, 29}, dvid.Span{14, 48, 10, 29}, dvid.Span{14, 49, 10, 29},
				dvid.Span{14, 50, 10, 29}, dvid.Span{14, 51, 10, 29}, dvid.Span{14, 52, 10, 29}, dvid.Span{14, 53, 10, 29}, dvid.Span{14, 54, 10, 29},
				dvid.Span{14, 55, 10, 29}, dvid.Span{14, 56, 10, 29}, dvid.Span{14, 57, 10, 29}, dvid.Span{14, 58, 10, 29}, dvid.Span{14, 59, 10, 29},
				dvid.Span{15, 40, 10, 29}, dvid.Span{15, 41, 10, 29}, dvid.Span{15, 42, 10, 29}, dvid.Span{15, 43, 10, 29}, dvid.Span{15, 44, 10, 29},
				dvid.Span{15, 45, 10, 29}, dvid.Span{15, 46, 10, 29}, dvid.Span{15, 47, 10, 29}, dvid.Span{15, 48, 10, 29}, dvid.Span{15, 49, 10, 29},
				dvid.Span{15, 50, 10, 29}, dvid.Span{15, 51, 10, 29}, dvid.Span{15, 52, 10, 29}, dvid.Span{15, 53, 10, 29}, dvid.Span{15, 54, 10, 29},
				dvid.Span{15, 55, 10, 29}, dvid.Span{15, 56, 10, 29}, dvid.Span{15, 57, 10, 29}, dvid.Span{15, 58, 10, 29}, dvid.Span{15, 59, 10, 29},
				dvid.Span{16, 40, 10, 29}, dvid.Span{16, 41, 10, 29}, dvid.Span{16, 42, 10, 29}, dvid.Span{16, 43, 10, 29}, dvid.Span{16, 44, 10, 29},
				dvid.Span{16, 45, 10, 29}, dvid.Span{16, 46, 10, 29}, dvid.Span{16, 47, 10, 29}, dvid.Span{16, 48, 10, 29}, dvid.Span{16, 49, 10, 29},
				dvid.Span{16, 50, 10, 29}, dvid.Span{16, 51, 10, 29}, dvid.Span{16, 52, 10, 29}, dvid.Span{16, 53, 10, 29}, dvid.Span{16, 54, 10, 29},
				dvid.Span{16, 55, 10, 29}, dvid.Span{16, 56, 10, 29}, dvid.Span{16, 57, 10, 29}, dvid.Span{16, 58, 10, 29}, dvid.Span{16, 59, 10, 29},
				dvid.Span{17, 40, 10, 29}, dvid.Span{17, 41, 10, 29}, dvid.Span{17, 42, 10, 29}, dvid.Span{17, 43, 10, 29}, dvid.Span{17, 44, 10, 29},
				dvid.Span{17, 45, 10, 29}, dvid.Span{17, 46, 10, 29}, dvid.Span{17, 47, 10, 29}, dvid.Span{17, 48, 10, 29}, dvid.Span{17, 49, 10, 29},
				dvid.Span{17, 50, 10, 29}, dvid.Span{17, 51, 10, 29}, dvid.Span{17, 52, 10, 29}, dvid.Span{17, 53, 10, 29}, dvid.Span{17, 54, 10, 29},
				dvid.Span{17, 55, 10, 29}, dvid.Span{17, 56, 10, 29}, dvid.Span{17, 57, 10, 29}, dvid.Span{17, 58, 10, 29}, dvid.Span{17, 59, 10, 29},
				dvid.Span{18, 40, 10, 29}, dvid.Span{18, 41, 10, 29}, dvid.Span{18, 42, 10, 29}, dvid.Span{18, 43, 10, 29}, dvid.Span{18, 44, 10, 29},
				dvid.Span{18, 45, 10, 29}, dvid.Span{18, 46, 10, 29}, dvid.Span{18, 47, 10, 29}, dvid.Span{18, 48, 10, 29}, dvid.Span{18, 49, 10, 29},
				dvid.Span{18, 50, 10, 29}, dvid.Span{18, 51, 10, 29}, dvid.Span{18, 52, 10, 29}, dvid.Span{18, 53, 10, 29}, dvid.Span{18, 54, 10, 29},
				dvid.Span{18, 55, 10, 29}, dvid.Span{18, 56, 10, 29}, dvid.Span{18, 57, 10, 29}, dvid.Span{18, 58, 10, 29}, dvid.Span{18, 59, 10, 29},
				dvid.Span{19, 40, 10, 29}, dvid.Span{19, 41, 10, 29}, dvid.Span{19, 42, 10, 29}, dvid.Span{19, 43, 10, 29}, dvid.Span{19, 44, 10, 29},
				dvid.Span{19, 45, 10, 29}, dvid.Span{19, 46, 10, 29}, dvid.Span{19, 47, 10, 29}, dvid.Span{19, 48, 10, 29}, dvid.Span{19, 49, 10, 29},
				dvid.Span{19, 50, 10, 29}, dvid.Span{19, 51, 10, 29}, dvid.Span{19, 52, 10, 29}, dvid.Span{19, 53, 10, 29}, dvid.Span{19, 54, 10, 29},
				dvid.Span{19, 55, 10, 29}, dvid.Span{19, 56, 10, 29}, dvid.Span{19, 57, 10, 29}, dvid.Span{19, 58, 10, 29}, dvid.Span{19, 59, 10, 29},
				dvid.Span{20, 40, 10, 29}, dvid.Span{20, 41, 10, 29}, dvid.Span{20, 42, 10, 29}, dvid.Span{20, 43, 10, 29}, dvid.Span{20, 44, 10, 29},
				dvid.Span{20, 45, 10, 29}, dvid.Span{20, 46, 10, 29}, dvid.Span{20, 47, 10, 29}, dvid.Span{20, 48, 10, 29}, dvid.Span{20, 49, 10, 29},
				dvid.Span{20, 50, 10, 29}, dvid.Span{20, 51, 10, 29}, dvid.Span{20, 52, 10, 29}, dvid.Span{20, 53, 10, 29}, dvid.Span{20, 54, 10, 29},
				dvid.Span{20, 55, 10, 29}, dvid.Span{20, 56, 10, 29}, dvid.Span{20, 57, 10, 29}, dvid.Span{20, 58, 10, 29}, dvid.Span{20, 59, 10, 29},
				dvid.Span{21, 40, 10, 29}, dvid.Span{21, 41, 10, 29}, dvid.Span{21, 42, 10, 29}, dvid.Span{21, 43, 10, 29}, dvid.Span{21, 44, 10, 29},
				dvid.Span{21, 45, 10, 29}, dvid.Span{21, 46, 10, 29}, dvid.Span{21, 47, 10, 29}, dvid.Span{21, 48, 10, 29}, dvid.Span{21, 49, 10, 29},
				dvid.Span{21, 50, 10, 29}, dvid.Span{21, 51, 10, 29}, dvid.Span{21, 52, 10, 29}, dvid.Span{21, 53, 10, 29}, dvid.Span{21, 54, 10, 29},
				dvid.Span{21, 55, 10, 29}, dvid.Span{21, 56, 10, 29}, dvid.Span{21, 57, 10, 29}, dvid.Span{21, 58, 10, 29}, dvid.Span{21, 59, 10, 29},
				dvid.Span{22, 40, 10, 29}, dvid.Span{22, 41, 10, 29}, dvid.Span{22, 42, 10, 29}, dvid.Span{22, 43, 10, 29}, dvid.Span{22, 44, 10, 29},
				dvid.Span{22, 45, 10, 29}, dvid.Span{22, 46, 10, 29}, dvid.Span{22, 47, 10, 29}, dvid.Span{22, 48, 10, 29}, dvid.Span{22, 49, 10, 29},
				dvid.Span{22, 50, 10, 29}, dvid.Span{22, 51, 10, 29}, dvid.Span{22, 52, 10, 29}, dvid.Span{22, 53, 10, 29}, dvid.Span{22, 54, 10, 29},
				dvid.Span{22, 55, 10, 29}, dvid.Span{22, 56, 10, 29}, dvid.Span{22, 57, 10, 29}, dvid.Span{22, 58, 10, 29}, dvid.Span{22, 59, 10, 29},
				dvid.Span{23, 40, 10, 29}, dvid.Span{23, 41, 10, 29}, dvid.Span{23, 42, 10, 29}, dvid.Span{23, 43, 10, 29}, dvid.Span{23, 44, 10, 29},
				dvid.Span{23, 45, 10, 29}, dvid.Span{23, 46, 10, 29}, dvid.Span{23, 47, 10, 29}, dvid.Span{23, 48, 10, 29}, dvid.Span{23, 49, 10, 29},
				dvid.Span{23, 50, 10, 29}, dvid.Span{23, 51, 10, 29}, dvid.Span{23, 52, 10, 29}, dvid.Span{23, 53, 10, 29}, dvid.Span{23, 54, 10, 29},
				dvid.Span{23, 55, 10, 29}, dvid.Span{23, 56, 10, 29}, dvid.Span{23, 57, 10, 29}, dvid.Span{23, 58, 10, 29}, dvid.Span{23, 59, 10, 29},
				dvid.Span{24, 40, 10, 29}, dvid.Span{24, 41, 10, 29}, dvid.Span{24, 42, 10, 29}, dvid.Span{24, 43, 10, 29}, dvid.Span{24, 44, 10, 29},
				dvid.Span{24, 45, 10, 29}, dvid.Span{24, 46, 10, 29}, dvid.Span{24, 47, 10, 29}, dvid.Span{24, 48, 10, 29}, dvid.Span{24, 49, 10, 29},
				dvid.Span{24, 50, 10, 29}, dvid.Span{24, 51, 10, 29}, dvid.Span{24, 52, 10, 29}, dvid.Span{24, 53, 10, 29}, dvid.Span{24, 54, 10, 29},
				dvid.Span{24, 55, 10, 29}, dvid.Span{24, 56, 10, 29}, dvid.Span{24, 57, 10, 29}, dvid.Span{24, 58, 10, 29}, dvid.Span{24, 59, 10, 29},
				dvid.Span{25, 40, 10, 29}, dvid.Span{25, 41, 10, 29}, dvid.Span{25, 42, 10, 29}, dvid.Span{25, 43, 10, 29}, dvid.Span{25, 44, 10, 29},
				dvid.Span{25, 45, 10, 29}, dvid.Span{25, 46, 10, 29}, dvid.Span{25, 47, 10, 29}, dvid.Span{25, 48, 10, 29}, dvid.Span{25, 49, 10, 29},
				dvid.Span{25, 50, 10, 29}, dvid.Span{25, 51, 10, 29}, dvid.Span{25, 52, 10, 29}, dvid.Span{25, 53, 10, 29}, dvid.Span{25, 54, 10, 29},
				dvid.Span{25, 55, 10, 29}, dvid.Span{25, 56, 10, 29}, dvid.Span{25, 57, 10, 29}, dvid.Span{25, 58, 10, 29}, dvid.Span{25, 59, 10, 29},
				dvid.Span{26, 40, 10, 29}, dvid.Span{26, 41, 10, 29}, dvid.Span{26, 42, 10, 29}, dvid.Span{26, 43, 10, 29}, dvid.Span{26, 44, 10, 29},
				dvid.Span{26, 45, 10, 29}, dvid.Span{26, 46, 10, 29}, dvid.Span{26, 47, 10, 29}, dvid.Span{26, 48, 10, 29}, dvid.Span{26, 49, 10, 29},
				dvid.Span{26, 50, 10, 29}, dvid.Span{26, 51, 10, 29}, dvid.Span{26, 52, 10, 29}, dvid.Span{26, 53, 10, 29}, dvid.Span{26, 54, 10, 29},
				dvid.Span{26, 55, 10, 29}, dvid.Span{26, 56, 10, 29}, dvid.Span{26, 57, 10, 29}, dvid.Span{26, 58, 10, 29}, dvid.Span{26, 59, 10, 29},
				dvid.Span{27, 40, 10, 29}, dvid.Span{27, 41, 10, 29}, dvid.Span{27, 42, 10, 29}, dvid.Span{27, 43, 10, 29}, dvid.Span{27, 44, 10, 29},
				dvid.Span{27, 45, 10, 29}, dvid.Span{27, 46, 10, 29}, dvid.Span{27, 47, 10, 29}, dvid.Span{27, 48, 10, 29}, dvid.Span{27, 49, 10, 29},
				dvid.Span{27, 50, 10, 29}, dvid.Span{27, 51, 10, 29}, dvid.Span{27, 52, 10, 29}, dvid.Span{27, 53, 10, 29}, dvid.Span{27, 54, 10, 29},
				dvid.Span{27, 55, 10, 29}, dvid.Span{27, 56, 10, 29}, dvid.Span{27, 57, 10, 29}, dvid.Span{27, 58, 10, 29}, dvid.Span{27, 59, 10, 29},
				dvid.Span{28, 40, 10, 29}, dvid.Span{28, 41, 10, 29}, dvid.Span{28, 42, 10, 29}, dvid.Span{28, 43, 10, 29}, dvid.Span{28, 44, 10, 29},
				dvid.Span{28, 45, 10, 29}, dvid.Span{28, 46, 10, 29}, dvid.Span{28, 47, 10, 29}, dvid.Span{28, 48, 10, 29}, dvid.Span{28, 49, 10, 29},
				dvid.Span{28, 50, 10, 29}, dvid.Span{28, 51, 10, 29}, dvid.Span{28, 52, 10, 29}, dvid.Span{28, 53, 10, 29}, dvid.Span{28, 54, 10, 29},
				dvid.Span{28, 55, 10, 29}, dvid.Span{28, 56, 10, 29}, dvid.Span{28, 57, 10, 29}, dvid.Span{28, 58, 10, 29}, dvid.Span{28, 59, 10, 29},
				dvid.Span{29, 40, 10, 29}, dvid.Span{29, 41, 10, 29}, dvid.Span{29, 42, 10, 29}, dvid.Span{29, 43, 10, 29}, dvid.Span{29, 44, 10, 29},
				dvid.Span{29, 45, 10, 29}, dvid.Span{29, 46, 10, 29}, dvid.Span{29, 47, 10, 29}, dvid.Span{29, 48, 10, 29}, dvid.Span{29, 49, 10, 29},
				dvid.Span{29, 50, 10, 29}, dvid.Span{29, 51, 10, 29}, dvid.Span{29, 52, 10, 29}, dvid.Span{29, 53, 10, 29}, dvid.Span{29, 54, 10, 29},
				dvid.Span{29, 55, 10, 29}, dvid.Span{29, 56, 10, 29}, dvid.Span{29, 57, 10, 29}, dvid.Span{29, 58, 10, 29}, dvid.Span{29, 59, 10, 29},
				dvid.Span{30, 40, 10, 29}, dvid.Span{30, 41, 10, 29}, dvid.Span{30, 42, 10, 29}, dvid.Span{30, 43, 10, 29}, dvid.Span{30, 44, 10, 29},
				dvid.Span{30, 45, 10, 29}, dvid.Span{30, 46, 10, 29}, dvid.Span{30, 47, 10, 29}, dvid.Span{30, 48, 10, 29}, dvid.Span{30, 49, 10, 29},
				dvid.Span{30, 50, 10, 29}, dvid.Span{30, 51, 10, 29}, dvid.Span{30, 52, 10, 29}, dvid.Span{30, 53, 10, 29}, dvid.Span{30, 54, 10, 29},
				dvid.Span{30, 55, 10, 29}, dvid.Span{30, 56, 10, 29}, dvid.Span{30, 57, 10, 29}, dvid.Span{30, 58, 10, 29}, dvid.Span{30, 59, 10, 29},
				dvid.Span{31, 40, 10, 29}, dvid.Span{31, 41, 10, 29}, dvid.Span{31, 42, 10, 29}, dvid.Span{31, 43, 10, 29}, dvid.Span{31, 44, 10, 29},
				dvid.Span{31, 45, 10, 29}, dvid.Span{31, 46, 10, 29}, dvid.Span{31, 47, 10, 29}, dvid.Span{31, 48, 10, 29}, dvid.Span{31, 49, 10, 29},
				dvid.Span{31, 50, 10, 29}, dvid.Span{31, 51, 10, 29}, dvid.Span{31, 52, 10, 29}, dvid.Span{31, 53, 10, 29}, dvid.Span{31, 54, 10, 29},
				dvid.Span{31, 55, 10, 29}, dvid.Span{31, 56, 10, 29}, dvid.Span{31, 57, 10, 29}, dvid.Span{31, 58, 10, 29}, dvid.Span{31, 59, 10, 29},
				dvid.Span{32, 40, 10, 29}, dvid.Span{32, 41, 10, 29}, dvid.Span{32, 42, 10, 29}, dvid.Span{32, 43, 10, 29}, dvid.Span{32, 44, 10, 29},
				dvid.Span{32, 45, 10, 29}, dvid.Span{32, 46, 10, 29}, dvid.Span{32, 47, 10, 29}, dvid.Span{32, 48, 10, 29}, dvid.Span{32, 49, 10, 29},
				dvid.Span{32, 50, 10, 29}, dvid.Span{32, 51, 10, 29}, dvid.Span{32, 52, 10, 29}, dvid.Span{32, 53, 10, 29}, dvid.Span{32, 54, 10, 29},
				dvid.Span{32, 55, 10, 29}, dvid.Span{32, 56, 10, 29}, dvid.Span{32, 57, 10, 29}, dvid.Span{32, 58, 10, 29}, dvid.Span{32, 59, 10, 29},
				dvid.Span{33, 40, 10, 29}, dvid.Span{33, 41, 10, 29}, dvid.Span{33, 42, 10, 29}, dvid.Span{33, 43, 10, 29}, dvid.Span{33, 44, 10, 29},
				dvid.Span{33, 45, 10, 29}, dvid.Span{33, 46, 10, 29}, dvid.Span{33, 47, 10, 29}, dvid.Span{33, 48, 10, 29}, dvid.Span{33, 49, 10, 29},
				dvid.Span{33, 50, 10, 29}, dvid.Span{33, 51, 10, 29}, dvid.Span{33, 52, 10, 29}, dvid.Span{33, 53, 10, 29}, dvid.Span{33, 54, 10, 29},
				dvid.Span{33, 55, 10, 29}, dvid.Span{33, 56, 10, 29}, dvid.Span{33, 57, 10, 29}, dvid.Span{33, 58, 10, 29}, dvid.Span{33, 59, 10, 29},
				dvid.Span{34, 40, 10, 29}, dvid.Span{34, 41, 10, 29}, dvid.Span{34, 42, 10, 29}, dvid.Span{34, 43, 10, 29}, dvid.Span{34, 44, 10, 29},
				dvid.Span{34, 45, 10, 29}, dvid.Span{34, 46, 10, 29}, dvid.Span{34, 47, 10, 29}, dvid.Span{34, 48, 10, 29}, dvid.Span{34, 49, 10, 29},
				dvid.Span{34, 50, 10, 29}, dvid.Span{34, 51, 10, 29}, dvid.Span{34, 52, 10, 29}, dvid.Span{34, 53, 10, 29}, dvid.Span{34, 54, 10, 29},
				dvid.Span{34, 55, 10, 29}, dvid.Span{34, 56, 10, 29}, dvid.Span{34, 57, 10, 29}, dvid.Span{34, 58, 10, 29}, dvid.Span{34, 59, 10, 29},
				dvid.Span{35, 40, 10, 29}, dvid.Span{35, 41, 10, 29}, dvid.Span{35, 42, 10, 29}, dvid.Span{35, 43, 10, 29}, dvid.Span{35, 44, 10, 29},
				dvid.Span{35, 45, 10, 29}, dvid.Span{35, 46, 10, 29}, dvid.Span{35, 47, 10, 29}, dvid.Span{35, 48, 10, 29}, dvid.Span{35, 49, 10, 29},
				dvid.Span{35, 50, 10, 29}, dvid.Span{35, 51, 10, 29}, dvid.Span{35, 52, 10, 29}, dvid.Span{35, 53, 10, 29}, dvid.Span{35, 54, 10, 29},
				dvid.Span{35, 55, 10, 29}, dvid.Span{35, 56, 10, 29}, dvid.Span{35, 57, 10, 29}, dvid.Span{35, 58, 10, 29}, dvid.Span{35, 59, 10, 29},
				dvid.Span{36, 40, 10, 29}, dvid.Span{36, 41, 10, 29}, dvid.Span{36, 42, 10, 29}, dvid.Span{36, 43, 10, 29}, dvid.Span{36, 44, 10, 29},
				dvid.Span{36, 45, 10, 29}, dvid.Span{36, 46, 10, 29}, dvid.Span{36, 47, 10, 29}, dvid.Span{36, 48, 10, 29}, dvid.Span{36, 49, 10, 29},
				dvid.Span{36, 50, 10, 29}, dvid.Span{36, 51, 10, 29}, dvid.Span{36, 52, 10, 29}, dvid.Span{36, 53, 10, 29}, dvid.Span{36, 54, 10, 29},
				dvid.Span{36, 55, 10, 29}, dvid.Span{36, 56, 10, 29}, dvid.Span{36, 57, 10, 29}, dvid.Span{36, 58, 10, 29}, dvid.Span{36, 59, 10, 29},
				dvid.Span{37, 40, 10, 29}, dvid.Span{37, 41, 10, 29}, dvid.Span{37, 42, 10, 29}, dvid.Span{37, 43, 10, 29}, dvid.Span{37, 44, 10, 29},
				dvid.Span{37, 45, 10, 29}, dvid.Span{37, 46, 10, 29}, dvid.Span{37, 47, 10, 29}, dvid.Span{37, 48, 10, 29}, dvid.Span{37, 49, 10, 29},
				dvid.Span{37, 50, 10, 29}, dvid.Span{37, 51, 10, 29}, dvid.Span{37, 52, 10, 29}, dvid.Span{37, 53, 10, 29}, dvid.Span{37, 54, 10, 29},
				dvid.Span{37, 55, 10, 29}, dvid.Span{37, 56, 10, 29}, dvid.Span{37, 57, 10, 29}, dvid.Span{37, 58, 10, 29}, dvid.Span{37, 59, 10, 29},
				dvid.Span{38, 40, 10, 29}, dvid.Span{38, 41, 10, 29}, dvid.Span{38, 42, 10, 29}, dvid.Span{38, 43, 10, 29}, dvid.Span{38, 44, 10, 29},
				dvid.Span{38, 45, 10, 29}, dvid.Span{38, 46, 10, 29}, dvid.Span{38, 47, 10, 29}, dvid.Span{38, 48, 10, 29}, dvid.Span{38, 49, 10, 29},
				dvid.Span{38, 50, 10, 29}, dvid.Span{38, 51, 10, 29}, dvid.Span{38, 52, 10, 29}, dvid.Span{38, 53, 10, 29}, dvid.Span{38, 54, 10, 29},
				dvid.Span{38, 55, 10, 29}, dvid.Span{38, 56, 10, 29}, dvid.Span{38, 57, 10, 29}, dvid.Span{38, 58, 10, 29}, dvid.Span{38, 59, 10, 29},
				dvid.Span{39, 40, 10, 29}, dvid.Span{39, 41, 10, 29}, dvid.Span{39, 42, 10, 29}, dvid.Span{39, 43, 10, 29}, dvid.Span{39, 44, 10, 29},
				dvid.Span{39, 45, 10, 29}, dvid.Span{39, 46, 10, 29}, dvid.Span{39, 47, 10, 29}, dvid.Span{39, 48, 10, 29}, dvid.Span{39, 49, 10, 29},
				dvid.Span{39, 50, 10, 29}, dvid.Span{39, 51, 10, 29}, dvid.Span{39, 52, 10, 29}, dvid.Span{39, 53, 10, 29}, dvid.Span{39, 54, 10, 29},
				dvid.Span{39, 55, 10, 29}, dvid.Span{39, 56, 10, 29}, dvid.Span{39, 57, 10, 29}, dvid.Span{39, 58, 10, 29}, dvid.Span{39, 59, 10, 29},
				dvid.Span{40, 40, 10, 29}, dvid.Span{40, 41, 10, 29}, dvid.Span{40, 42, 10, 29}, dvid.Span{40, 43, 10, 29}, dvid.Span{40, 44, 10, 29},
				dvid.Span{40, 45, 10, 29}, dvid.Span{40, 46, 10, 29}, dvid.Span{40, 47, 10, 29}, dvid.Span{40, 48, 10, 29}, dvid.Span{40, 49, 10, 29},
				dvid.Span{40, 50, 10, 29}, dvid.Span{40, 51, 10, 29}, dvid.Span{40, 52, 10, 29}, dvid.Span{40, 53, 10, 29}, dvid.Span{40, 54, 10, 29},
				dvid.Span{40, 55, 10, 29}, dvid.Span{40, 56, 10, 29}, dvid.Span{40, 57, 10, 29}, dvid.Span{40, 58, 10, 29}, dvid.Span{40, 59, 10, 29},
				dvid.Span{41, 40, 10, 29}, dvid.Span{41, 41, 10, 29}, dvid.Span{41, 42, 10, 29}, dvid.Span{41, 43, 10, 29}, dvid.Span{41, 44, 10, 29},
				dvid.Span{41, 45, 10, 29}, dvid.Span{41, 46, 10, 29}, dvid.Span{41, 47, 10, 29}, dvid.Span{41, 48, 10, 29}, dvid.Span{41, 49, 10, 29},
				dvid.Span{41, 50, 10, 29}, dvid.Span{41, 51, 10, 29}, dvid.Span{41, 52, 10, 29}, dvid.Span{41, 53, 10, 29}, dvid.Span{41, 54, 10, 29},
				dvid.Span{41, 55, 10, 29}, dvid.Span{41, 56, 10, 29}, dvid.Span{41, 57, 10, 29}, dvid.Span{41, 58, 10, 29}, dvid.Span{41, 59, 10, 29},
				dvid.Span{42, 40, 10, 29}, dvid.Span{42, 41, 10, 29}, dvid.Span{42, 42, 10, 29}, dvid.Span{42, 43, 10, 29}, dvid.Span{42, 44, 10, 29},
				dvid.Span{42, 45, 10, 29}, dvid.Span{42, 46, 10, 29}, dvid.Span{42, 47, 10, 29}, dvid.Span{42, 48, 10, 29}, dvid.Span{42, 49, 10, 29},
				dvid.Span{42, 50, 10, 29}, dvid.Span{42, 51, 10, 29}, dvid.Span{42, 52, 10, 29}, dvid.Span{42, 53, 10, 29}, dvid.Span{42, 54, 10, 29},
				dvid.Span{42, 55, 10, 29}, dvid.Span{42, 56, 10, 29}, dvid.Span{42, 57, 10, 29}, dvid.Span{42, 58, 10, 29}, dvid.Span{42, 59, 10, 29},
				dvid.Span{43, 40, 10, 29}, dvid.Span{43, 41, 10, 29}, dvid.Span{43, 42, 10, 29}, dvid.Span{43, 43, 10, 29}, dvid.Span{43, 44, 10, 29},
				dvid.Span{43, 45, 10, 29}, dvid.Span{43, 46, 10, 29}, dvid.Span{43, 47, 10, 29}, dvid.Span{43, 48, 10, 29}, dvid.Span{43, 49, 10, 29},
				dvid.Span{43, 50, 10, 29}, dvid.Span{43, 51, 10, 29}, dvid.Span{43, 52, 10, 29}, dvid.Span{43, 53, 10, 29}, dvid.Span{43, 54, 10, 29},
				dvid.Span{43, 55, 10, 29}, dvid.Span{43, 56, 10, 29}, dvid.Span{43, 57, 10, 29}, dvid.Span{43, 58, 10, 29}, dvid.Span{43, 59, 10, 29},
				dvid.Span{44, 40, 10, 29}, dvid.Span{44, 41, 10, 29}, dvid.Span{44, 42, 10, 29}, dvid.Span{44, 43, 10, 29}, dvid.Span{44, 44, 10, 29},
				dvid.Span{44, 45, 10, 29}, dvid.Span{44, 46, 10, 29}, dvid.Span{44, 47, 10, 29}, dvid.Span{44, 48, 10, 29}, dvid.Span{44, 49, 10, 29},
				dvid.Span{44, 50, 10, 29}, dvid.Span{44, 51, 10, 29}, dvid.Span{44, 52, 10, 29}, dvid.Span{44, 53, 10, 29}, dvid.Span{44, 54, 10, 29},
				dvid.Span{44, 55, 10, 29}, dvid.Span{44, 56, 10, 29}, dvid.Span{44, 57, 10, 29}, dvid.Span{44, 58, 10, 29}, dvid.Span{44, 59, 10, 29},
				dvid.Span{45, 40, 10, 29}, dvid.Span{45, 41, 10, 29}, dvid.Span{45, 42, 10, 29}, dvid.Span{45, 43, 10, 29}, dvid.Span{45, 44, 10, 29},
				dvid.Span{45, 45, 10, 29}, dvid.Span{45, 46, 10, 29}, dvid.Span{45, 47, 10, 29}, dvid.Span{45, 48, 10, 29}, dvid.Span{45, 49, 10, 29},
				dvid.Span{45, 50, 10, 29}, dvid.Span{45, 51, 10, 29}, dvid.Span{45, 52, 10, 29}, dvid.Span{45, 53, 10, 29}, dvid.Span{45, 54, 10, 29},
				dvid.Span{45, 55, 10, 29}, dvid.Span{45, 56, 10, 29}, dvid.Span{45, 57, 10, 29}, dvid.Span{45, 58, 10, 29}, dvid.Span{45, 59, 10, 29},
				dvid.Span{46, 40, 10, 29}, dvid.Span{46, 41, 10, 29}, dvid.Span{46, 42, 10, 29}, dvid.Span{46, 43, 10, 29}, dvid.Span{46, 44, 10, 29},
				dvid.Span{46, 45, 10, 29}, dvid.Span{46, 46, 10, 29}, dvid.Span{46, 47, 10, 29}, dvid.Span{46, 48, 10, 29}, dvid.Span{46, 49, 10, 29},
				dvid.Span{46, 50, 10, 29}, dvid.Span{46, 51, 10, 29}, dvid.Span{46, 52, 10, 29}, dvid.Span{46, 53, 10, 29}, dvid.Span{46, 54, 10, 29},
				dvid.Span{46, 55, 10, 29}, dvid.Span{46, 56, 10, 29}, dvid.Span{46, 57, 10, 29}, dvid.Span{46, 58, 10, 29}, dvid.Span{46, 59, 10, 29},
				dvid.Span{47, 40, 10, 29}, dvid.Span{47, 41, 10, 29}, dvid.Span{47, 42, 10, 29}, dvid.Span{47, 43, 10, 29}, dvid.Span{47, 44, 10, 29},
				dvid.Span{47, 45, 10, 29}, dvid.Span{47, 46, 10, 29}, dvid.Span{47, 47, 10, 29}, dvid.Span{47, 48, 10, 29}, dvid.Span{47, 49, 10, 29},
				dvid.Span{47, 50, 10, 29}, dvid.Span{47, 51, 10, 29}, dvid.Span{47, 52, 10, 29}, dvid.Span{47, 53, 10, 29}, dvid.Span{47, 54, 10, 29},
				dvid.Span{47, 55, 10, 29}, dvid.Span{47, 56, 10, 29}, dvid.Span{47, 57, 10, 29}, dvid.Span{47, 58, 10, 29}, dvid.Span{47, 59, 10, 29},
				dvid.Span{48, 40, 10, 29}, dvid.Span{48, 41, 10, 29}, dvid.Span{48, 42, 10, 29}, dvid.Span{48, 43, 10, 29}, dvid.Span{48, 44, 10, 29},
				dvid.Span{48, 45, 10, 29}, dvid.Span{48, 46, 10, 29}, dvid.Span{48, 47, 10, 29}, dvid.Span{48, 48, 10, 29}, dvid.Span{48, 49, 10, 29},
				dvid.Span{48, 50, 10, 29}, dvid.Span{48, 51, 10, 29}, dvid.Span{48, 52, 10, 29}, dvid.Span{48, 53, 10, 29}, dvid.Span{48, 54, 10, 29},
				dvid.Span{48, 55, 10, 29}, dvid.Span{48, 56, 10, 29}, dvid.Span{48, 57, 10, 29}, dvid.Span{48, 58, 10, 29}, dvid.Span{48, 59, 10, 29},
				dvid.Span{49, 40, 10, 29}, dvid.Span{49, 41, 10, 29}, dvid.Span{49, 42, 10, 29}, dvid.Span{49, 43, 10, 29}, dvid.Span{49, 44, 10, 29},
				dvid.Span{49, 45, 10, 29}, dvid.Span{49, 46, 10, 29}, dvid.Span{49, 47, 10, 29}, dvid.Span{49, 48, 10, 29}, dvid.Span{49, 49, 10, 29},
				dvid.Span{49, 50, 10, 29}, dvid.Span{49, 51, 10, 29}, dvid.Span{49, 52, 10, 29}, dvid.Span{49, 53, 10, 29}, dvid.Span{49, 54, 10, 29},
				dvid.Span{49, 55, 10, 29}, dvid.Span{49, 56, 10, 29}, dvid.Span{49, 57, 10, 29}, dvid.Span{49, 58, 10, 29}, dvid.Span{49, 59, 10, 29},
				dvid.Span{50, 40, 10, 29}, dvid.Span{50, 41, 10, 29}, dvid.Span{50, 42, 10, 29}, dvid.Span{50, 43, 10, 29}, dvid.Span{50, 44, 10, 29},
				dvid.Span{50, 45, 10, 29}, dvid.Span{50, 46, 10, 29}, dvid.Span{50, 47, 10, 29}, dvid.Span{50, 48, 10, 29}, dvid.Span{50, 49, 10, 29},
				dvid.Span{50, 50, 10, 29}, dvid.Span{50, 51, 10, 29}, dvid.Span{50, 52, 10, 29}, dvid.Span{50, 53, 10, 29}, dvid.Span{50, 54, 10, 29},
				dvid.Span{50, 55, 10, 29}, dvid.Span{50, 56, 10, 29}, dvid.Span{50, 57, 10, 29}, dvid.Span{50, 58, 10, 29}, dvid.Span{50, 59, 10, 29},
				dvid.Span{51, 40, 10, 29}, dvid.Span{51, 41, 10, 29}, dvid.Span{51, 42, 10, 29}, dvid.Span{51, 43, 10, 29}, dvid.Span{51, 44, 10, 29},
				dvid.Span{51, 45, 10, 29}, dvid.Span{51, 46, 10, 29}, dvid.Span{51, 47, 10, 29}, dvid.Span{51, 48, 10, 29}, dvid.Span{51, 49, 10, 29},
				dvid.Span{51, 50, 10, 29}, dvid.Span{51, 51, 10, 29}, dvid.Span{51, 52, 10, 29}, dvid.Span{51, 53, 10, 29}, dvid.Span{51, 54, 10, 29},
				dvid.Span{51, 55, 10, 29}, dvid.Span{51, 56, 10, 29}, dvid.Span{51, 57, 10, 29}, dvid.Span{51, 58, 10, 29}, dvid.Span{51, 59, 10, 29},
				dvid.Span{52, 40, 10, 29}, dvid.Span{52, 41, 10, 29}, dvid.Span{52, 42, 10, 29}, dvid.Span{52, 43, 10, 29}, dvid.Span{52, 44, 10, 29},
				dvid.Span{52, 45, 10, 29}, dvid.Span{52, 46, 10, 29}, dvid.Span{52, 47, 10, 29}, dvid.Span{52, 48, 10, 29}, dvid.Span{52, 49, 10, 29},
				dvid.Span{52, 50, 10, 29}, dvid.Span{52, 51, 10, 29}, dvid.Span{52, 52, 10, 29}, dvid.Span{52, 53, 10, 29}, dvid.Span{52, 54, 10, 29},
				dvid.Span{52, 55, 10, 29}, dvid.Span{52, 56, 10, 29}, dvid.Span{52, 57, 10, 29}, dvid.Span{52, 58, 10, 29}, dvid.Span{52, 59, 10, 29},
				dvid.Span{53, 40, 10, 29}, dvid.Span{53, 41, 10, 29}, dvid.Span{53, 42, 10, 29}, dvid.Span{53, 43, 10, 29}, dvid.Span{53, 44, 10, 29},
				dvid.Span{53, 45, 10, 29}, dvid.Span{53, 46, 10, 29}, dvid.Span{53, 47, 10, 29}, dvid.Span{53, 48, 10, 29}, dvid.Span{53, 49, 10, 29},
				dvid.Span{53, 50, 10, 29}, dvid.Span{53, 51, 10, 29}, dvid.Span{53, 52, 10, 29}, dvid.Span{53, 53, 10, 29}, dvid.Span{53, 54, 10, 29},
				dvid.Span{53, 55, 10, 29}, dvid.Span{53, 56, 10, 29}, dvid.Span{53, 57, 10, 29}, dvid.Span{53, 58, 10, 29}, dvid.Span{53, 59, 10, 29},
				dvid.Span{54, 40, 10, 29}, dvid.Span{54, 41, 10, 29}, dvid.Span{54, 42, 10, 29}, dvid.Span{54, 43, 10, 29}, dvid.Span{54, 44, 10, 29},
				dvid.Span{54, 45, 10, 29}, dvid.Span{54, 46, 10, 29}, dvid.Span{54, 47, 10, 29}, dvid.Span{54, 48, 10, 29}, dvid.Span{54, 49, 10, 29},
				dvid.Span{54, 50, 10, 29}, dvid.Span{54, 51, 10, 29}, dvid.Span{54, 52, 10, 29}, dvid.Span{54, 53, 10, 29}, dvid.Span{54, 54, 10, 29},
				dvid.Span{54, 55, 10, 29}, dvid.Span{54, 56, 10, 29}, dvid.Span{54, 57, 10, 29}, dvid.Span{54, 58, 10, 29}, dvid.Span{54, 59, 10, 29},
				dvid.Span{55, 40, 10, 29}, dvid.Span{55, 41, 10, 29}, dvid.Span{55, 42, 10, 29}, dvid.Span{55, 43, 10, 29}, dvid.Span{55, 44, 10, 29},
				dvid.Span{55, 45, 10, 29}, dvid.Span{55, 46, 10, 29}, dvid.Span{55, 47, 10, 29}, dvid.Span{55, 48, 10, 29}, dvid.Span{55, 49, 10, 29},
				dvid.Span{55, 50, 10, 29}, dvid.Span{55, 51, 10, 29}, dvid.Span{55, 52, 10, 29}, dvid.Span{55, 53, 10, 29}, dvid.Span{55, 54, 10, 29},
				dvid.Span{55, 55, 10, 29}, dvid.Span{55, 56, 10, 29}, dvid.Span{55, 57, 10, 29}, dvid.Span{55, 58, 10, 29}, dvid.Span{55, 59, 10, 29},
				dvid.Span{56, 40, 10, 29}, dvid.Span{56, 41, 10, 29}, dvid.Span{56, 42, 10, 29}, dvid.Span{56, 43, 10, 29}, dvid.Span{56, 44, 10, 29},
				dvid.Span{56, 45, 10, 29}, dvid.Span{56, 46, 10, 29}, dvid.Span{56, 47, 10, 29}, dvid.Span{56, 48, 10, 29}, dvid.Span{56, 49, 10, 29},
				dvid.Span{56, 50, 10, 29}, dvid.Span{56, 51, 10, 29}, dvid.Span{56, 52, 10, 29}, dvid.Span{56, 53, 10, 29}, dvid.Span{56, 54, 10, 29},
				dvid.Span{56, 55, 10, 29}, dvid.Span{56, 56, 10, 29}, dvid.Span{56, 57, 10, 29}, dvid.Span{56, 58, 10, 29}, dvid.Span{56, 59, 10, 29},
				dvid.Span{57, 40, 10, 29}, dvid.Span{57, 41, 10, 29}, dvid.Span{57, 42, 10, 29}, dvid.Span{57, 43, 10, 29}, dvid.Span{57, 44, 10, 29},
				dvid.Span{57, 45, 10, 29}, dvid.Span{57, 46, 10, 29}, dvid.Span{57, 47, 10, 29}, dvid.Span{57, 48, 10, 29}, dvid.Span{57, 49, 10, 29},
				dvid.Span{57, 50, 10, 29}, dvid.Span{57, 51, 10, 29}, dvid.Span{57, 52, 10, 29}, dvid.Span{57, 53, 10, 29}, dvid.Span{57, 54, 10, 29},
				dvid.Span{57, 55, 10, 29}, dvid.Span{57, 56, 10, 29}, dvid.Span{57, 57, 10, 29}, dvid.Span{57, 58, 10, 29}, dvid.Span{57, 59, 10, 29},
				dvid.Span{58, 40, 10, 29}, dvid.Span{58, 41, 10, 29}, dvid.Span{58, 42, 10, 29}, dvid.Span{58, 43, 10, 29}, dvid.Span{58, 44, 10, 29},
				dvid.Span{58, 45, 10, 29}, dvid.Span{58, 46, 10, 29}, dvid.Span{58, 47, 10, 29}, dvid.Span{58, 48, 10, 29}, dvid.Span{58, 49, 10, 29},
				dvid.Span{58, 50, 10, 29}, dvid.Span{58, 51, 10, 29}, dvid.Span{58, 52, 10, 29}, dvid.Span{58, 53, 10, 29}, dvid.Span{58, 54, 10, 29},
				dvid.Span{58, 55, 10, 29}, dvid.Span{58, 56, 10, 29}, dvid.Span{58, 57, 10, 29}, dvid.Span{58, 58, 10, 29}, dvid.Span{58, 59, 10, 29},
				dvid.Span{59, 40, 10, 29}, dvid.Span{59, 41, 10, 29}, dvid.Span{59, 42, 10, 29}, dvid.Span{59, 43, 10, 29}, dvid.Span{59, 44, 10, 29},
				dvid.Span{59, 45, 10, 29}, dvid.Span{59, 46, 10, 29}, dvid.Span{59, 47, 10, 29}, dvid.Span{59, 48, 10, 29}, dvid.Span{59, 49, 10, 29},
				dvid.Span{59, 50, 10, 29}, dvid.Span{59, 51, 10, 29}, dvid.Span{59, 52, 10, 29}, dvid.Span{59, 53, 10, 29}, dvid.Span{59, 54, 10, 29},
				dvid.Span{59, 55, 10, 29}, dvid.Span{59, 56, 10, 29}, dvid.Span{59, 57, 10, 29}, dvid.Span{59, 58, 10, 29}, dvid.Span{59, 59, 10, 29},
				dvid.Span{60, 40, 10, 29}, dvid.Span{60, 41, 10, 29}, dvid.Span{60, 42, 10, 29}, dvid.Span{60, 43, 10, 29}, dvid.Span{60, 44, 10, 29},
				dvid.Span{60, 45, 10, 29}, dvid.Span{60, 46, 10, 29}, dvid.Span{60, 47, 10, 29}, dvid.Span{60, 48, 10, 29}, dvid.Span{60, 49, 10, 29},
				dvid.Span{60, 50, 10, 29}, dvid.Span{60, 51, 10, 29}, dvid.Span{60, 52, 10, 29}, dvid.Span{60, 53, 10, 29}, dvid.Span{60, 54, 10, 29},
				dvid.Span{60, 55, 10, 29}, dvid.Span{60, 56, 10, 29}, dvid.Span{60, 57, 10, 29}, dvid.Span{60, 58, 10, 29}, dvid.Span{60, 59, 10, 29},
				dvid.Span{61, 40, 10, 29}, dvid.Span{61, 41, 10, 29}, dvid.Span{61, 42, 10, 29}, dvid.Span{61, 43, 10, 29}, dvid.Span{61, 44, 10, 29},
				dvid.Span{61, 45, 10, 29}, dvid.Span{61, 46, 10, 29}, dvid.Span{61, 47, 10, 29}, dvid.Span{61, 48, 10, 29}, dvid.Span{61, 49, 10, 29},
				dvid.Span{61, 50, 10, 29}, dvid.Span{61, 51, 10, 29}, dvid.Span{61, 52, 10, 29}, dvid.Span{61, 53, 10, 29}, dvid.Span{61, 54, 10, 29},
				dvid.Span{61, 55, 10, 29}, dvid.Span{61, 56, 10, 29}, dvid.Span{61, 57, 10, 29}, dvid.Span{61, 58, 10, 29}, dvid.Span{61, 59, 10, 29},
				dvid.Span{62, 40, 10, 29}, dvid.Span{62, 41, 10, 29}, dvid.Span{62, 42, 10, 29}, dvid.Span{62, 43, 10, 29}, dvid.Span{62, 44, 10, 29},
				dvid.Span{62, 45, 10, 29}, dvid.Span{62, 46, 10, 29}, dvid.Span{62, 47, 10, 29}, dvid.Span{62, 48, 10, 29}, dvid.Span{62, 49, 10, 29},
				dvid.Span{62, 50, 10, 29}, dvid.Span{62, 51, 10, 29}, dvid.Span{62, 52, 10, 29}, dvid.Span{62, 53, 10, 29}, dvid.Span{62, 54, 10, 29},
				dvid.Span{62, 55, 10, 29}, dvid.Span{62, 56, 10, 29}, dvid.Span{62, 57, 10, 29}, dvid.Span{62, 58, 10, 29}, dvid.Span{62, 59, 10, 29},
				dvid.Span{63, 40, 10, 29}, dvid.Span{63, 41, 10, 29}, dvid.Span{63, 42, 10, 29}, dvid.Span{63, 43, 10, 29}, dvid.Span{63, 44, 10, 29},
				dvid.Span{63, 45, 10, 29}, dvid.Span{63, 46, 10, 29}, dvid.Span{63, 47, 10, 29}, dvid.Span{63, 48, 10, 29}, dvid.Span{63, 49, 10, 29},
				dvid.Span{63, 50, 10, 29}, dvid.Span{63, 51, 10, 29}, dvid.Span{63, 52, 10, 29}, dvid.Span{63, 53, 10, 29}, dvid.Span{63, 54, 10, 29},
				dvid.Span{63, 55, 10, 29}, dvid.Span{63, 56, 10, 29}, dvid.Span{63, 57, 10, 29}, dvid.Span{63, 58, 10, 29}, dvid.Span{63, 59, 10, 29},
				dvid.Span{64, 40, 10, 29}, dvid.Span{64, 41, 10, 29}, dvid.Span{64, 42, 10, 29}, dvid.Span{64, 43, 10, 29}, dvid.Span{64, 44, 10, 29},
				dvid.Span{64, 45, 10, 29}, dvid.Span{64, 46, 10, 29}, dvid.Span{64, 47, 10, 29}, dvid.Span{64, 48, 10, 29}, dvid.Span{64, 49, 10, 29},
				dvid.Span{64, 50, 10, 29}, dvid.Span{64, 51, 10, 29}, dvid.Span{64, 52, 10, 29}, dvid.Span{64, 53, 10, 29}, dvid.Span{64, 54, 10, 29},
				dvid.Span{64, 55, 10, 29}, dvid.Span{64, 56, 10, 29}, dvid.Span{64, 57, 10, 29}, dvid.Span{64, 58, 10, 29}, dvid.Span{64, 59, 10, 29},
				dvid.Span{65, 40, 10, 29}, dvid.Span{65, 41, 10, 29}, dvid.Span{65, 42, 10, 29}, dvid.Span{65, 43, 10, 29}, dvid.Span{65, 44, 10, 29},
				dvid.Span{65, 45, 10, 29}, dvid.Span{65, 46, 10, 29}, dvid.Span{65, 47, 10, 29}, dvid.Span{65, 48, 10, 29}, dvid.Span{65, 49, 10, 29},
				dvid.Span{65, 50, 10, 29}, dvid.Span{65, 51, 10, 29}, dvid.Span{65, 52, 10, 29}, dvid.Span{65, 53, 10, 29}, dvid.Span{65, 54, 10, 29},
				dvid.Span{65, 55, 10, 29}, dvid.Span{65, 56, 10, 29}, dvid.Span{65, 57, 10, 29}, dvid.Span{65, 58, 10, 29}, dvid.Span{65, 59, 10, 29},
				dvid.Span{66, 40, 10, 29}, dvid.Span{66, 41, 10, 29}, dvid.Span{66, 42, 10, 29}, dvid.Span{66, 43, 10, 29}, dvid.Span{66, 44, 10, 29},
				dvid.Span{66, 45, 10, 29}, dvid.Span{66, 46, 10, 29}, dvid.Span{66, 47, 10, 29}, dvid.Span{66, 48, 10, 29}, dvid.Span{66, 49, 10, 29},
				dvid.Span{66, 50, 10, 29}, dvid.Span{66, 51, 10, 29}, dvid.Span{66, 52, 10, 29}, dvid.Span{66, 53, 10, 29}, dvid.Span{66, 54, 10, 29},
				dvid.Span{66, 55, 10, 29}, dvid.Span{66, 56, 10, 29}, dvid.Span{66, 57, 10, 29}, dvid.Span{66, 58, 10, 29}, dvid.Span{66, 59, 10, 29},
				dvid.Span{67, 40, 10, 29}, dvid.Span{67, 41, 10, 29}, dvid.Span{67, 42, 10, 29}, dvid.Span{67, 43, 10, 29}, dvid.Span{67, 44, 10, 29},
				dvid.Span{67, 45, 10, 29}, dvid.Span{67, 46, 10, 29}, dvid.Span{67, 47, 10, 29}, dvid.Span{67, 48, 10, 29}, dvid.Span{67, 49, 10, 29},
				dvid.Span{67, 50, 10, 29}, dvid.Span{67, 51, 10, 29}, dvid.Span{67, 52, 10, 29}, dvid.Span{67, 53, 10, 29}, dvid.Span{67, 54, 10, 29},
				dvid.Span{67, 55, 10, 29}, dvid.Span{67, 56, 10, 29}, dvid.Span{67, 57, 10, 29}, dvid.Span{67, 58, 10, 29}, dvid.Span{67, 59, 10, 29},
				dvid.Span{68, 40, 10, 29}, dvid.Span{68, 41, 10, 29}, dvid.Span{68, 42, 10, 29}, dvid.Span{68, 43, 10, 29}, dvid.Span{68, 44, 10, 29},
				dvid.Span{68, 45, 10, 29}, dvid.Span{68, 46, 10, 29}, dvid.Span{68, 47, 10, 29}, dvid.Span{68, 48, 10, 29}, dvid.Span{68, 49, 10, 29},
				dvid.Span{68, 50, 10, 29}, dvid.Span{68, 51, 10, 29}, dvid.Span{68, 52, 10, 29}, dvid.Span{68, 53, 10, 29}, dvid.Span{68, 54, 10, 29},
				dvid.Span{68, 55, 10, 29}, dvid.Span{68, 56, 10, 29}, dvid.Span{68, 57, 10, 29}, dvid.Span{68, 58, 10, 29}, dvid.Span{68, 59, 10, 29},
				dvid.Span{69, 40, 10, 29}, dvid.Span{69, 41, 10, 29}, dvid.Span{69, 42, 10, 29}, dvid.Span{69, 43, 10, 29}, dvid.Span{69, 44, 10, 29},
				dvid.Span{69, 45, 10, 29}, dvid.Span{69, 46, 10, 29}, dvid.Span{69, 47, 10, 29}, dvid.Span{69, 48, 10, 29}, dvid.Span{69, 49, 10, 29},
				dvid.Span{69, 50, 10, 29}, dvid.Span{69, 51, 10, 29}, dvid.Span{69, 52, 10, 29}, dvid.Span{69, 53, 10, 29}, dvid.Span{69, 54, 10, 29},
				dvid.Span{69, 55, 10, 29}, dvid.Span{69, 56, 10, 29}, dvid.Span{69, 57, 10, 29}, dvid.Span{69, 58, 10, 29}, dvid.Span{69, 59, 10, 29},
				dvid.Span{70, 40, 10, 29}, dvid.Span{70, 41, 10, 29}, dvid.Span{70, 42, 10, 29}, dvid.Span{70, 43, 10, 29}, dvid.Span{70, 44, 10, 29},
				dvid.Span{70, 45, 10, 29}, dvid.Span{70, 46, 10, 29}, dvid.Span{70, 47, 10, 29}, dvid.Span{70, 48, 10, 29}, dvid.Span{70, 49, 10, 29},
				dvid.Span{70, 50, 10, 29}, dvid.Span{70, 51, 10, 29}, dvid.Span{70, 52, 10, 29}, dvid.Span{70, 53, 10, 29}, dvid.Span{70, 54, 10, 29},
				dvid.Span{70, 55, 10, 29}, dvid.Span{70, 56, 10, 29}, dvid.Span{70, 57, 10, 29}, dvid.Span{70, 58, 10, 29}, dvid.Span{70, 59, 10, 29},
				dvid.Span{71, 40, 10, 29}, dvid.Span{71, 41, 10, 29}, dvid.Span{71, 42, 10, 29}, dvid.Span{71, 43, 10, 29}, dvid.Span{71, 44, 10, 29},
				dvid.Span{71, 45, 10, 29}, dvid.Span{71, 46, 10, 29}, dvid.Span{71, 47, 10, 29}, dvid.Span{71, 48, 10, 29}, dvid.Span{71, 49, 10, 29},
				dvid.Span{71, 50, 10, 29}, dvid.Span{71, 51, 10, 29}, dvid.Span{71, 52, 10, 29}, dvid.Span{71, 53, 10, 29}, dvid.Span{71, 54, 10, 29},
				dvid.Span{71, 55, 10, 29}, dvid.Span{71, 56, 10, 29}, dvid.Span{71, 57, 10, 29}, dvid.Span{71, 58, 10, 29}, dvid.Span{71, 59, 10, 29},
				dvid.Span{72, 40, 10, 29}, dvid.Span{72, 41, 10, 29}, dvid.Span{72, 42, 10, 29}, dvid.Span{72, 43, 10, 29}, dvid.Span{72, 44, 10, 29},
				dvid.Span{72, 45, 10, 29}, dvid.Span{72, 46, 10, 29}, dvid.Span{72, 47, 10, 29}, dvid.Span{72, 48, 10, 29}, dvid.Span{72, 49, 10, 29},
				dvid.Span{72, 50, 10, 29}, dvid.Span{72, 51, 10, 29}, dvid.Span{72, 52, 10, 29}, dvid.Span{72, 53, 10, 29}, dvid.Span{72, 54, 10, 29},
				dvid.Span{72, 55, 10, 29}, dvid.Span{72, 56, 10, 29}, dvid.Span{72, 57, 10, 29}, dvid.Span{72, 58, 10, 29}, dvid.Span{72, 59, 10, 29},
				dvid.Span{73, 40, 10, 29}, dvid.Span{73, 41, 10, 29}, dvid.Span{73, 42, 10, 29}, dvid.Span{73, 43, 10, 29}, dvid.Span{73, 44, 10, 29},
				dvid.Span{73, 45, 10, 29}, dvid.Span{73, 46, 10, 29}, dvid.Span{73, 47, 10, 29}, dvid.Span{73, 48, 10, 29}, dvid.Span{73, 49, 10, 29},
				dvid.Span{73, 50, 10, 29}, dvid.Span{73, 51, 10, 29}, dvid.Span{73, 52, 10, 29}, dvid.Span{73, 53, 10, 29}, dvid.Span{73, 54, 10, 29},
				dvid.Span{73, 55, 10, 29}, dvid.Span{73, 56, 10, 29}, dvid.Span{73, 57, 10, 29}, dvid.Span{73, 58, 10, 29}, dvid.Span{73, 59, 10, 29},
				dvid.Span{74, 40, 10, 29}, dvid.Span{74, 41, 10, 29}, dvid.Span{74, 42, 10, 29}, dvid.Span{74, 43, 10, 29}, dvid.Span{74, 44, 10, 29},
				dvid.Span{74, 45, 10, 29}, dvid.Span{74, 46, 10, 29}, dvid.Span{74, 47, 10, 29}, dvid.Span{74, 48, 10, 29}, dvid.Span{74, 49, 10, 29},
				dvid.Span{74, 50, 10, 29}, dvid.Span{74, 51, 10, 29}, dvid.Span{74, 52, 10, 29}, dvid.Span{74, 53, 10, 29}, dvid.Span{74, 54, 10, 29},
				dvid.Span{74, 55, 10, 29}, dvid.Span{74, 56, 10, 29}, dvid.Span{74, 57, 10, 29}, dvid.Span{74, 58, 10, 29}, dvid.Span{74, 59, 10, 29},
				dvid.Span{75, 40, 10, 29}, dvid.Span{75, 41, 10, 29}, dvid.Span{75, 42, 10, 29}, dvid.Span{75, 43, 10, 29}, dvid.Span{75, 44, 10, 29},
				dvid.Span{75, 45, 10, 29}, dvid.Span{75, 46, 10, 29}, dvid.Span{75, 47, 10, 29}, dvid.Span{75, 48, 10, 29}, dvid.Span{75, 49, 10, 29},
				dvid.Span{75, 50, 10, 29}, dvid.Span{75, 51, 10, 29}, dvid.Span{75, 52, 10, 29}, dvid.Span{75, 53, 10, 29}, dvid.Span{75, 54, 10, 29},
				dvid.Span{75, 55, 10, 29}, dvid.Span{75, 56, 10, 29}, dvid.Span{75, 57, 10, 29}, dvid.Span{75, 58, 10, 29}, dvid.Span{75, 59, 10, 29},
				dvid.Span{76, 40, 10, 29}, dvid.Span{76, 41, 10, 29}, dvid.Span{76, 42, 10, 29}, dvid.Span{76, 43, 10, 29}, dvid.Span{76, 44, 10, 29},
				dvid.Span{76, 45, 10, 29}, dvid.Span{76, 46, 10, 29}, dvid.Span{76, 47, 10, 29}, dvid.Span{76, 48, 10, 29}, dvid.Span{76, 49, 10, 29},
				dvid.Span{76, 50, 10, 29}, dvid.Span{76, 51, 10, 29}, dvid.Span{76, 52, 10, 29}, dvid.Span{76, 53, 10, 29}, dvid.Span{76, 54, 10, 29},
				dvid.Span{76, 55, 10, 29}, dvid.Span{76, 56, 10, 29}, dvid.Span{76, 57, 10, 29}, dvid.Span{76, 58, 10, 29}, dvid.Span{76, 59, 10, 29},
				dvid.Span{77, 40, 10, 29}, dvid.Span{77, 41, 10, 29}, dvid.Span{77, 42, 10, 29}, dvid.Span{77, 43, 10, 29}, dvid.Span{77, 44, 10, 29},
				dvid.Span{77, 45, 10, 29}, dvid.Span{77, 46, 10, 29}, dvid.Span{77, 47, 10, 29}, dvid.Span{77, 48, 10, 29}, dvid.Span{77, 49, 10, 29},
				dvid.Span{77, 50, 10, 29}, dvid.Span{77, 51, 10, 29}, dvid.Span{77, 52, 10, 29}, dvid.Span{77, 53, 10, 29}, dvid.Span{77, 54, 10, 29},
				dvid.Span{77, 55, 10, 29}, dvid.Span{77, 56, 10, 29}, dvid.Span{77, 57, 10, 29}, dvid.Span{77, 58, 10, 29}, dvid.Span{77, 59, 10, 29},
				dvid.Span{78, 40, 10, 29}, dvid.Span{78, 41, 10, 29}, dvid.Span{78, 42, 10, 29}, dvid.Span{78, 43, 10, 29}, dvid.Span{78, 44, 10, 29},
				dvid.Span{78, 45, 10, 29}, dvid.Span{78, 46, 10, 29}, dvid.Span{78, 47, 10, 29}, dvid.Span{78, 48, 10, 29}, dvid.Span{78, 49, 10, 29},
				dvid.Span{78, 50, 10, 29}, dvid.Span{78, 51, 10, 29}, dvid.Span{78, 52, 10, 29}, dvid.Span{78, 53, 10, 29}, dvid.Span{78, 54, 10, 29},
				dvid.Span{78, 55, 10, 29}, dvid.Span{78, 56, 10, 29}, dvid.Span{78, 57, 10, 29}, dvid.Span{78, 58, 10, 29}, dvid.Span{78, 59, 10, 29},
				dvid.Span{79, 40, 10, 29}, dvid.Span{79, 41, 10, 29}, dvid.Span{79, 42, 10, 29}, dvid.Span{79, 43, 10, 29}, dvid.Span{79, 44, 10, 29},
				dvid.Span{79, 45, 10, 29}, dvid.Span{79, 46, 10, 29}, dvid.Span{79, 47, 10, 29}, dvid.Span{79, 48, 10, 29}, dvid.Span{79, 49, 10, 29},
				dvid.Span{79, 50, 10, 29}, dvid.Span{79, 51, 10, 29}, dvid.Span{79, 52, 10, 29}, dvid.Span{79, 53, 10, 29}, dvid.Span{79, 54, 10, 29},
				dvid.Span{79, 55, 10, 29}, dvid.Span{79, 56, 10, 29}, dvid.Span{79, 57, 10, 29}, dvid.Span{79, 58, 10, 29}, dvid.Span{79, 59, 10, 29},
				dvid.Span{80, 40, 10, 29}, dvid.Span{80, 41, 10, 29}, dvid.Span{80, 42, 10, 29}, dvid.Span{80, 43, 10, 29}, dvid.Span{80, 44, 10, 29},
				dvid.Span{80, 45, 10, 29}, dvid.Span{80, 46, 10, 29}, dvid.Span{80, 47, 10, 29}, dvid.Span{80, 48, 10, 29}, dvid.Span{80, 49, 10, 29},
				dvid.Span{80, 50, 10, 29}, dvid.Span{80, 51, 10, 29}, dvid.Span{80, 52, 10, 29}, dvid.Span{80, 53, 10, 29}, dvid.Span{80, 54, 10, 29},
				dvid.Span{80, 55, 10, 29}, dvid.Span{80, 56, 10, 29}, dvid.Span{80, 57, 10, 29}, dvid.Span{80, 58, 10, 29}, dvid.Span{80, 59, 10, 29},
				dvid.Span{81, 40, 10, 29}, dvid.Span{81, 41, 10, 29}, dvid.Span{81, 42, 10, 29}, dvid.Span{81, 43, 10, 29}, dvid.Span{81, 44, 10, 29},
				dvid.Span{81, 45, 10, 29}, dvid.Span{81, 46, 10, 29}, dvid.Span{81, 47, 10, 29}, dvid.Span{81, 48, 10, 29}, dvid.Span{81, 49, 10, 29},
				dvid.Span{81, 50, 10, 29}, dvid.Span{81, 51, 10, 29}, dvid.Span{81, 52, 10, 29}, dvid.Span{81, 53, 10, 29}, dvid.Span{81, 54, 10, 29},
				dvid.Span{81, 55, 10, 29}, dvid.Span{81, 56, 10, 29}, dvid.Span{81, 57, 10, 29}, dvid.Span{81, 58, 10, 29}, dvid.Span{81, 59, 10, 29},
				dvid.Span{82, 40, 10, 29}, dvid.Span{82, 41, 10, 29}, dvid.Span{82, 42, 10, 29}, dvid.Span{82, 43, 10, 29}, dvid.Span{82, 44, 10, 29},
				dvid.Span{82, 45, 10, 29}, dvid.Span{82, 46, 10, 29}, dvid.Span{82, 47, 10, 29}, dvid.Span{82, 48, 10, 29}, dvid.Span{82, 49, 10, 29},
				dvid.Span{82, 50, 10, 29}, dvid.Span{82, 51, 10, 29}, dvid.Span{82, 52, 10, 29}, dvid.Span{82, 53, 10, 29}, dvid.Span{82, 54, 10, 29},
				dvid.Span{82, 55, 10, 29}, dvid.Span{82, 56, 10, 29}, dvid.Span{82, 57, 10, 29}, dvid.Span{82, 58, 10, 29}, dvid.Span{82, 59, 10, 29},
				dvid.Span{83, 40, 10, 29}, dvid.Span{83, 41, 10, 29}, dvid.Span{83, 42, 10, 29}, dvid.Span{83, 43, 10, 29}, dvid.Span{83, 44, 10, 29},
				dvid.Span{83, 45, 10, 29}, dvid.Span{83, 46, 10, 29}, dvid.Span{83, 47, 10, 29}, dvid.Span{83, 48, 10, 29}, dvid.Span{83, 49, 10, 29},
				dvid.Span{83, 50, 10, 29}, dvid.Span{83, 51, 10, 29}, dvid.Span{83, 52, 10, 29}, dvid.Span{83, 53, 10, 29}, dvid.Span{83, 54, 10, 29},
				dvid.Span{83, 55, 10, 29}, dvid.Span{83, 56, 10, 29}, dvid.Span{83, 57, 10, 29}, dvid.Span{83, 58, 10, 29}, dvid.Span{83, 59, 10, 29},
				dvid.Span{84, 40, 10, 29}, dvid.Span{84, 41, 10, 29}, dvid.Span{84, 42, 10, 29}, dvid.Span{84, 43, 10, 29}, dvid.Span{84, 44, 10, 29},
				dvid.Span{84, 45, 10, 29}, dvid.Span{84, 46, 10, 29}, dvid.Span{84, 47, 10, 29}, dvid.Span{84, 48, 10, 29}, dvid.Span{84, 49, 10, 29},
				dvid.Span{84, 50, 10, 29}, dvid.Span{84, 51, 10, 29}, dvid.Span{84, 52, 10, 29}, dvid.Span{84, 53, 10, 29}, dvid.Span{84, 54, 10, 29},
				dvid.Span{84, 55, 10, 29}, dvid.Span{84, 56, 10, 29}, dvid.Span{84, 57, 10, 29}, dvid.Span{84, 58, 10, 29}, dvid.Span{84, 59, 10, 29},
				dvid.Span{85, 40, 10, 29}, dvid.Span{85, 41, 10, 29}, dvid.Span{85, 42, 10, 29}, dvid.Span{85, 43, 10, 29}, dvid.Span{85, 44, 10, 29},
				dvid.Span{85, 45, 10, 29}, dvid.Span{85, 46, 10, 29}, dvid.Span{85, 47, 10, 29}, dvid.Span{85, 48, 10, 29}, dvid.Span{85, 49, 10, 29},
				dvid.Span{85, 50, 10, 29}, dvid.Span{85, 51, 10, 29}, dvid.Span{85, 52, 10, 29}, dvid.Span{85, 53, 10, 29}, dvid.Span{85, 54, 10, 29},
				dvid.Span{85, 55, 10, 29}, dvid.Span{85, 56, 10, 29}, dvid.Span{85, 57, 10, 29}, dvid.Span{85, 58, 10, 29}, dvid.Span{85, 59, 10, 29},
				dvid.Span{86, 40, 10, 29}, dvid.Span{86, 41, 10, 29}, dvid.Span{86, 42, 10, 29}, dvid.Span{86, 43, 10, 29}, dvid.Span{86, 44, 10, 29},
				dvid.Span{86, 45, 10, 29}, dvid.Span{86, 46, 10, 29}, dvid.Span{86, 47, 10, 29}, dvid.Span{86, 48, 10, 29}, dvid.Span{86, 49, 10, 29},
				dvid.Span{86, 50, 10, 29}, dvid.Span{86, 51, 10, 29}, dvid.Span{86, 52, 10, 29}, dvid.Span{86, 53, 10, 29}, dvid.Span{86, 54, 10, 29},
				dvid.Span{86, 55, 10, 29}, dvid.Span{86, 56, 10, 29}, dvid.Span{86, 57, 10, 29}, dvid.Span{86, 58, 10, 29}, dvid.Span{86, 59, 10, 29},
				dvid.Span{87, 40, 10, 29}, dvid.Span{87, 41, 10, 29}, dvid.Span{87, 42, 10, 29}, dvid.Span{87, 43, 10, 29}, dvid.Span{87, 44, 10, 29},
				dvid.Span{87, 45, 10, 29}, dvid.Span{87, 46, 10, 29}, dvid.Span{87, 47, 10, 29}, dvid.Span{87, 48, 10, 29}, dvid.Span{87, 49, 10, 29},
				dvid.Span{87, 50, 10, 29}, dvid.Span{87, 51, 10, 29}, dvid.Span{87, 52, 10, 29}, dvid.Span{87, 53, 10, 29}, dvid.Span{87, 54, 10, 29},
				dvid.Span{87, 55, 10, 29}, dvid.Span{87, 56, 10, 29}, dvid.Span{87, 57, 10, 29}, dvid.Span{87, 58, 10, 29}, dvid.Span{87, 59, 10, 29},
				dvid.Span{88, 40, 10, 29}, dvid.Span{88, 41, 10, 29}, dvid.Span{88, 42, 10, 29}, dvid.Span{88, 43, 10, 29}, dvid.Span{88, 44, 10, 29},
				dvid.Span{88, 45, 10, 29}, dvid.Span{88, 46, 10, 29}, dvid.Span{88, 47, 10, 29}, dvid.Span{88, 48, 10, 29}, dvid.Span{88, 49, 10, 29},
				dvid.Span{88, 50, 10, 29}, dvid.Span{88, 51, 10, 29}, dvid.Span{88, 52, 10, 29}, dvid.Span{88, 53, 10, 29}, dvid.Span{88, 54, 10, 29},
				dvid.Span{88, 55, 10, 29}, dvid.Span{88, 56, 10, 29}, dvid.Span{88, 57, 10, 29}, dvid.Span{88, 58, 10, 29}, dvid.Span{88, 59, 10, 29},
				dvid.Span{89, 40, 10, 29}, dvid.Span{89, 41, 10, 29}, dvid.Span{89, 42, 10, 29}, dvid.Span{89, 43, 10, 29}, dvid.Span{89, 44, 10, 29},
				dvid.Span{89, 45, 10, 29}, dvid.Span{89, 46, 10, 29}, dvid.Span{89, 47, 10, 29}, dvid.Span{89, 48, 10, 29}, dvid.Span{89, 49, 10, 29},
				dvid.Span{89, 50, 10, 29}, dvid.Span{89, 51, 10, 29}, dvid.Span{89, 52, 10, 29}, dvid.Span{89, 53, 10, 29}, dvid.Span{89, 54, 10, 29},
				dvid.Span{89, 55, 10, 29}, dvid.Span{89, 56, 10, 29}, dvid.Span{89, 57, 10, 29}, dvid.Span{89, 58, 10, 29}, dvid.Span{89, 59, 10, 29},
			},
		}, {
			label:  2,
			offset: dvid.Point3d{30, 20, 40},
			size:   dvid.Point3d{50, 50, 20},
			blockSpans: []dvid.Span{
				dvid.Span{1, 0, 0, 2},
				dvid.Span{1, 1, 0, 2},
				dvid.Span{1, 2, 0, 2},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{40, 20, 30, 31}, dvid.Span{40, 21, 30, 31}, dvid.Span{40, 22, 30, 31}, dvid.Span{40, 23, 30, 31}, dvid.Span{40, 24, 30, 31},
				dvid.Span{40, 25, 30, 31}, dvid.Span{40, 26, 30, 31}, dvid.Span{40, 27, 30, 31}, dvid.Span{40, 28, 30, 31}, dvid.Span{40, 29, 30, 31},
				dvid.Span{40, 30, 30, 31}, dvid.Span{40, 31, 30, 31}, dvid.Span{41, 20, 30, 31}, dvid.Span{41, 21, 30, 31}, dvid.Span{41, 22, 30, 31},
				dvid.Span{41, 23, 30, 31}, dvid.Span{41, 24, 30, 31}, dvid.Span{41, 25, 30, 31}, dvid.Span{41, 26, 30, 31}, dvid.Span{41, 27, 30, 31},
				dvid.Span{41, 28, 30, 31}, dvid.Span{41, 29, 30, 31}, dvid.Span{41, 30, 30, 31}, dvid.Span{41, 31, 30, 31}, dvid.Span{42, 20, 30, 31},
				dvid.Span{42, 21, 30, 31}, dvid.Span{42, 22, 30, 31}, dvid.Span{42, 23, 30, 31}, dvid.Span{42, 24, 30, 31}, dvid.Span{42, 25, 30, 31},
				dvid.Span{42, 26, 30, 31}, dvid.Span{42, 27, 30, 31}, dvid.Span{42, 28, 30, 31}, dvid.Span{42, 29, 30, 31}, dvid.Span{42, 30, 30, 31},
				dvid.Span{42, 31, 30, 31}, dvid.Span{43, 20, 30, 31}, dvid.Span{43, 21, 30, 31}, dvid.Span{43, 22, 30, 31}, dvid.Span{43, 23, 30, 31},
				dvid.Span{43, 24, 30, 31}, dvid.Span{43, 25, 30, 31}, dvid.Span{43, 26, 30, 31}, dvid.Span{43, 27, 30, 31}, dvid.Span{43, 28, 30, 31},
				dvid.Span{43, 29, 30, 31}, dvid.Span{43, 30, 30, 31}, dvid.Span{43, 31, 30, 31}, dvid.Span{44, 20, 30, 31}, dvid.Span{44, 21, 30, 31},
				dvid.Span{44, 22, 30, 31}, dvid.Span{44, 23, 30, 31}, dvid.Span{44, 24, 30, 31}, dvid.Span{44, 25, 30, 31}, dvid.Span{44, 26, 30, 31},
				dvid.Span{44, 27, 30, 31}, dvid.Span{44, 28, 30, 31}, dvid.Span{44, 29, 30, 31}, dvid.Span{44, 30, 30, 31}, dvid.Span{44, 31, 30, 31},
				dvid.Span{45, 20, 30, 31}, dvid.Span{45, 21, 30, 31}, dvid.Span{45, 22, 30, 31}, dvid.Span{45, 23, 30, 31}, dvid.Span{45, 24, 30, 31},
				dvid.Span{45, 25, 30, 31}, dvid.Span{45, 26, 30, 31}, dvid.Span{45, 27, 30, 31}, dvid.Span{45, 28, 30, 31}, dvid.Span{45, 29, 30, 31},
				dvid.Span{45, 30, 30, 31}, dvid.Span{45, 31, 30, 31}, dvid.Span{46, 20, 30, 31}, dvid.Span{46, 21, 30, 31}, dvid.Span{46, 22, 30, 31},
				dvid.Span{46, 23, 30, 31}, dvid.Span{46, 24, 30, 31}, dvid.Span{46, 25, 30, 31}, dvid.Span{46, 26, 30, 31}, dvid.Span{46, 27, 30, 31},
				dvid.Span{46, 28, 30, 31}, dvid.Span{46, 29, 30, 31}, dvid.Span{46, 30, 30, 31}, dvid.Span{46, 31, 30, 31}, dvid.Span{47, 20, 30, 31},
				dvid.Span{47, 21, 30, 31}, dvid.Span{47, 22, 30, 31}, dvid.Span{47, 23, 30, 31}, dvid.Span{47, 24, 30, 31}, dvid.Span{47, 25, 30, 31},
				dvid.Span{47, 26, 30, 31}, dvid.Span{47, 27, 30, 31}, dvid.Span{47, 28, 30, 31}, dvid.Span{47, 29, 30, 31}, dvid.Span{47, 30, 30, 31},
				dvid.Span{47, 31, 30, 31}, dvid.Span{48, 20, 30, 31}, dvid.Span{48, 21, 30, 31}, dvid.Span{48, 22, 30, 31}, dvid.Span{48, 23, 30, 31},
				dvid.Span{48, 24, 30, 31}, dvid.Span{48, 25, 30, 31}, dvid.Span{48, 26, 30, 31}, dvid.Span{48, 27, 30, 31}, dvid.Span{48, 28, 30, 31},
				dvid.Span{48, 29, 30, 31}, dvid.Span{48, 30, 30, 31}, dvid.Span{48, 31, 30, 31}, dvid.Span{49, 20, 30, 31}, dvid.Span{49, 21, 30, 31},
				dvid.Span{49, 22, 30, 31}, dvid.Span{49, 23, 30, 31}, dvid.Span{49, 24, 30, 31}, dvid.Span{49, 25, 30, 31}, dvid.Span{49, 26, 30, 31},
				dvid.Span{49, 27, 30, 31}, dvid.Span{49, 28, 30, 31}, dvid.Span{49, 29, 30, 31}, dvid.Span{49, 30, 30, 31}, dvid.Span{49, 31, 30, 31},
				dvid.Span{50, 20, 30, 31}, dvid.Span{50, 21, 30, 31}, dvid.Span{50, 22, 30, 31}, dvid.Span{50, 23, 30, 31}, dvid.Span{50, 24, 30, 31},
				dvid.Span{50, 25, 30, 31}, dvid.Span{50, 26, 30, 31}, dvid.Span{50, 27, 30, 31}, dvid.Span{50, 28, 30, 31}, dvid.Span{50, 29, 30, 31},
				dvid.Span{50, 30, 30, 31}, dvid.Span{50, 31, 30, 31}, dvid.Span{51, 20, 30, 31}, dvid.Span{51, 21, 30, 31}, dvid.Span{51, 22, 30, 31},
				dvid.Span{51, 23, 30, 31}, dvid.Span{51, 24, 30, 31}, dvid.Span{51, 25, 30, 31}, dvid.Span{51, 26, 30, 31}, dvid.Span{51, 27, 30, 31},
				dvid.Span{51, 28, 30, 31}, dvid.Span{51, 29, 30, 31}, dvid.Span{51, 30, 30, 31}, dvid.Span{51, 31, 30, 31}, dvid.Span{52, 20, 30, 31},
				dvid.Span{52, 21, 30, 31}, dvid.Span{52, 22, 30, 31}, dvid.Span{52, 23, 30, 31}, dvid.Span{52, 24, 30, 31}, dvid.Span{52, 25, 30, 31},
				dvid.Span{52, 26, 30, 31}, dvid.Span{52, 27, 30, 31}, dvid.Span{52, 28, 30, 31}, dvid.Span{52, 29, 30, 31}, dvid.Span{52, 30, 30, 31},
				dvid.Span{52, 31, 30, 31}, dvid.Span{53, 20, 30, 31}, dvid.Span{53, 21, 30, 31}, dvid.Span{53, 22, 30, 31}, dvid.Span{53, 23, 30, 31},
				dvid.Span{53, 24, 30, 31}, dvid.Span{53, 25, 30, 31}, dvid.Span{53, 26, 30, 31}, dvid.Span{53, 27, 30, 31}, dvid.Span{53, 28, 30, 31},
				dvid.Span{53, 29, 30, 31}, dvid.Span{53, 30, 30, 31}, dvid.Span{53, 31, 30, 31}, dvid.Span{54, 20, 30, 31}, dvid.Span{54, 21, 30, 31},
				dvid.Span{54, 22, 30, 31}, dvid.Span{54, 23, 30, 31}, dvid.Span{54, 24, 30, 31}, dvid.Span{54, 25, 30, 31}, dvid.Span{54, 26, 30, 31},
				dvid.Span{54, 27, 30, 31}, dvid.Span{54, 28, 30, 31}, dvid.Span{54, 29, 30, 31}, dvid.Span{54, 30, 30, 31}, dvid.Span{54, 31, 30, 31},
				dvid.Span{55, 20, 30, 31}, dvid.Span{55, 21, 30, 31}, dvid.Span{55, 22, 30, 31}, dvid.Span{55, 23, 30, 31}, dvid.Span{55, 24, 30, 31},
				dvid.Span{55, 25, 30, 31}, dvid.Span{55, 26, 30, 31}, dvid.Span{55, 27, 30, 31}, dvid.Span{55, 28, 30, 31}, dvid.Span{55, 29, 30, 31},
				dvid.Span{55, 30, 30, 31}, dvid.Span{55, 31, 30, 31}, dvid.Span{56, 20, 30, 31}, dvid.Span{56, 21, 30, 31}, dvid.Span{56, 22, 30, 31},
				dvid.Span{56, 23, 30, 31}, dvid.Span{56, 24, 30, 31}, dvid.Span{56, 25, 30, 31}, dvid.Span{56, 26, 30, 31}, dvid.Span{56, 27, 30, 31},
				dvid.Span{56, 28, 30, 31}, dvid.Span{56, 29, 30, 31}, dvid.Span{56, 30, 30, 31}, dvid.Span{56, 31, 30, 31}, dvid.Span{57, 20, 30, 31},
				dvid.Span{57, 21, 30, 31}, dvid.Span{57, 22, 30, 31}, dvid.Span{57, 23, 30, 31}, dvid.Span{57, 24, 30, 31}, dvid.Span{57, 25, 30, 31},
				dvid.Span{57, 26, 30, 31}, dvid.Span{57, 27, 30, 31}, dvid.Span{57, 28, 30, 31}, dvid.Span{57, 29, 30, 31}, dvid.Span{57, 30, 30, 31},
				dvid.Span{57, 31, 30, 31}, dvid.Span{58, 20, 30, 31}, dvid.Span{58, 21, 30, 31}, dvid.Span{58, 22, 30, 31}, dvid.Span{58, 23, 30, 31},
				dvid.Span{58, 24, 30, 31}, dvid.Span{58, 25, 30, 31}, dvid.Span{58, 26, 30, 31}, dvid.Span{58, 27, 30, 31}, dvid.Span{58, 28, 30, 31},
				dvid.Span{58, 29, 30, 31}, dvid.Span{58, 30, 30, 31}, dvid.Span{58, 31, 30, 31}, dvid.Span{59, 20, 30, 31}, dvid.Span{59, 21, 30, 31},
				dvid.Span{59, 22, 30, 31}, dvid.Span{59, 23, 30, 31}, dvid.Span{59, 24, 30, 31}, dvid.Span{59, 25, 30, 31}, dvid.Span{59, 26, 30, 31},
				dvid.Span{59, 27, 30, 31}, dvid.Span{59, 28, 30, 31}, dvid.Span{59, 29, 30, 31}, dvid.Span{59, 30, 30, 31}, dvid.Span{59, 31, 30, 31},
				dvid.Span{40, 20, 32, 63}, dvid.Span{40, 21, 32, 63}, dvid.Span{40, 22, 32, 63}, dvid.Span{40, 23, 32, 63}, dvid.Span{40, 24, 32, 63},
				dvid.Span{40, 25, 32, 63}, dvid.Span{40, 26, 32, 63}, dvid.Span{40, 27, 32, 63}, dvid.Span{40, 28, 32, 63}, dvid.Span{40, 29, 32, 63},
				dvid.Span{40, 30, 32, 63}, dvid.Span{40, 31, 32, 63}, dvid.Span{41, 20, 32, 63}, dvid.Span{41, 21, 32, 63}, dvid.Span{41, 22, 32, 63},
				dvid.Span{41, 23, 32, 63}, dvid.Span{41, 24, 32, 63}, dvid.Span{41, 25, 32, 63}, dvid.Span{41, 26, 32, 63}, dvid.Span{41, 27, 32, 63},
				dvid.Span{41, 28, 32, 63}, dvid.Span{41, 29, 32, 63}, dvid.Span{41, 30, 32, 63}, dvid.Span{41, 31, 32, 63}, dvid.Span{42, 20, 32, 63},
				dvid.Span{42, 21, 32, 63}, dvid.Span{42, 22, 32, 63}, dvid.Span{42, 23, 32, 63}, dvid.Span{42, 24, 32, 63}, dvid.Span{42, 25, 32, 63},
				dvid.Span{42, 26, 32, 63}, dvid.Span{42, 27, 32, 63}, dvid.Span{42, 28, 32, 63}, dvid.Span{42, 29, 32, 63}, dvid.Span{42, 30, 32, 63},
				dvid.Span{42, 31, 32, 63}, dvid.Span{43, 20, 32, 63}, dvid.Span{43, 21, 32, 63}, dvid.Span{43, 22, 32, 63}, dvid.Span{43, 23, 32, 63},
				dvid.Span{43, 24, 32, 63}, dvid.Span{43, 25, 32, 63}, dvid.Span{43, 26, 32, 63}, dvid.Span{43, 27, 32, 63}, dvid.Span{43, 28, 32, 63},
				dvid.Span{43, 29, 32, 63}, dvid.Span{43, 30, 32, 63}, dvid.Span{43, 31, 32, 63}, dvid.Span{44, 20, 32, 63}, dvid.Span{44, 21, 32, 63},
				dvid.Span{44, 22, 32, 63}, dvid.Span{44, 23, 32, 63}, dvid.Span{44, 24, 32, 63}, dvid.Span{44, 25, 32, 63}, dvid.Span{44, 26, 32, 63},
				dvid.Span{44, 27, 32, 63}, dvid.Span{44, 28, 32, 63}, dvid.Span{44, 29, 32, 63}, dvid.Span{44, 30, 32, 63}, dvid.Span{44, 31, 32, 63},
				dvid.Span{45, 20, 32, 63}, dvid.Span{45, 21, 32, 63}, dvid.Span{45, 22, 32, 63}, dvid.Span{45, 23, 32, 63}, dvid.Span{45, 24, 32, 63},
				dvid.Span{45, 25, 32, 63}, dvid.Span{45, 26, 32, 63}, dvid.Span{45, 27, 32, 63}, dvid.Span{45, 28, 32, 63}, dvid.Span{45, 29, 32, 63},
				dvid.Span{45, 30, 32, 63}, dvid.Span{45, 31, 32, 63}, dvid.Span{46, 20, 32, 63}, dvid.Span{46, 21, 32, 63}, dvid.Span{46, 22, 32, 63},
				dvid.Span{46, 23, 32, 63}, dvid.Span{46, 24, 32, 63}, dvid.Span{46, 25, 32, 63}, dvid.Span{46, 26, 32, 63}, dvid.Span{46, 27, 32, 63},
				dvid.Span{46, 28, 32, 63}, dvid.Span{46, 29, 32, 63}, dvid.Span{46, 30, 32, 63}, dvid.Span{46, 31, 32, 63}, dvid.Span{47, 20, 32, 63},
				dvid.Span{47, 21, 32, 63}, dvid.Span{47, 22, 32, 63}, dvid.Span{47, 23, 32, 63}, dvid.Span{47, 24, 32, 63}, dvid.Span{47, 25, 32, 63},
				dvid.Span{47, 26, 32, 63}, dvid.Span{47, 27, 32, 63}, dvid.Span{47, 28, 32, 63}, dvid.Span{47, 29, 32, 63}, dvid.Span{47, 30, 32, 63},
				dvid.Span{47, 31, 32, 63}, dvid.Span{48, 20, 32, 63}, dvid.Span{48, 21, 32, 63}, dvid.Span{48, 22, 32, 63}, dvid.Span{48, 23, 32, 63},
				dvid.Span{48, 24, 32, 63}, dvid.Span{48, 25, 32, 63}, dvid.Span{48, 26, 32, 63}, dvid.Span{48, 27, 32, 63}, dvid.Span{48, 28, 32, 63},
				dvid.Span{48, 29, 32, 63}, dvid.Span{48, 30, 32, 63}, dvid.Span{48, 31, 32, 63}, dvid.Span{49, 20, 32, 63}, dvid.Span{49, 21, 32, 63},
				dvid.Span{49, 22, 32, 63}, dvid.Span{49, 23, 32, 63}, dvid.Span{49, 24, 32, 63}, dvid.Span{49, 25, 32, 63}, dvid.Span{49, 26, 32, 63},
				dvid.Span{49, 27, 32, 63}, dvid.Span{49, 28, 32, 63}, dvid.Span{49, 29, 32, 63}, dvid.Span{49, 30, 32, 63}, dvid.Span{49, 31, 32, 63},
				dvid.Span{50, 20, 32, 63}, dvid.Span{50, 21, 32, 63}, dvid.Span{50, 22, 32, 63}, dvid.Span{50, 23, 32, 63}, dvid.Span{50, 24, 32, 63},
				dvid.Span{50, 25, 32, 63}, dvid.Span{50, 26, 32, 63}, dvid.Span{50, 27, 32, 63}, dvid.Span{50, 28, 32, 63}, dvid.Span{50, 29, 32, 63},
				dvid.Span{50, 30, 32, 63}, dvid.Span{50, 31, 32, 63}, dvid.Span{51, 20, 32, 63}, dvid.Span{51, 21, 32, 63}, dvid.Span{51, 22, 32, 63},
				dvid.Span{51, 23, 32, 63}, dvid.Span{51, 24, 32, 63}, dvid.Span{51, 25, 32, 63}, dvid.Span{51, 26, 32, 63}, dvid.Span{51, 27, 32, 63},
				dvid.Span{51, 28, 32, 63}, dvid.Span{51, 29, 32, 63}, dvid.Span{51, 30, 32, 63}, dvid.Span{51, 31, 32, 63}, dvid.Span{52, 20, 32, 63},
				dvid.Span{52, 21, 32, 63}, dvid.Span{52, 22, 32, 63}, dvid.Span{52, 23, 32, 63}, dvid.Span{52, 24, 32, 63}, dvid.Span{52, 25, 32, 63},
				dvid.Span{52, 26, 32, 63}, dvid.Span{52, 27, 32, 63}, dvid.Span{52, 28, 32, 63}, dvid.Span{52, 29, 32, 63}, dvid.Span{52, 30, 32, 63},
				dvid.Span{52, 31, 32, 63}, dvid.Span{53, 20, 32, 63}, dvid.Span{53, 21, 32, 63}, dvid.Span{53, 22, 32, 63}, dvid.Span{53, 23, 32, 63},
				dvid.Span{53, 24, 32, 63}, dvid.Span{53, 25, 32, 63}, dvid.Span{53, 26, 32, 63}, dvid.Span{53, 27, 32, 63}, dvid.Span{53, 28, 32, 63},
				dvid.Span{53, 29, 32, 63}, dvid.Span{53, 30, 32, 63}, dvid.Span{53, 31, 32, 63}, dvid.Span{54, 20, 32, 63}, dvid.Span{54, 21, 32, 63},
				dvid.Span{54, 22, 32, 63}, dvid.Span{54, 23, 32, 63}, dvid.Span{54, 24, 32, 63}, dvid.Span{54, 25, 32, 63}, dvid.Span{54, 26, 32, 63},
				dvid.Span{54, 27, 32, 63}, dvid.Span{54, 28, 32, 63}, dvid.Span{54, 29, 32, 63}, dvid.Span{54, 30, 32, 63}, dvid.Span{54, 31, 32, 63},
				dvid.Span{55, 20, 32, 63}, dvid.Span{55, 21, 32, 63}, dvid.Span{55, 22, 32, 63}, dvid.Span{55, 23, 32, 63}, dvid.Span{55, 24, 32, 63},
				dvid.Span{55, 25, 32, 63}, dvid.Span{55, 26, 32, 63}, dvid.Span{55, 27, 32, 63}, dvid.Span{55, 28, 32, 63}, dvid.Span{55, 29, 32, 63},
				dvid.Span{55, 30, 32, 63}, dvid.Span{55, 31, 32, 63}, dvid.Span{56, 20, 32, 63}, dvid.Span{56, 21, 32, 63}, dvid.Span{56, 22, 32, 63},
				dvid.Span{56, 23, 32, 63}, dvid.Span{56, 24, 32, 63}, dvid.Span{56, 25, 32, 63}, dvid.Span{56, 26, 32, 63}, dvid.Span{56, 27, 32, 63},
				dvid.Span{56, 28, 32, 63}, dvid.Span{56, 29, 32, 63}, dvid.Span{56, 30, 32, 63}, dvid.Span{56, 31, 32, 63}, dvid.Span{57, 20, 32, 63},
				dvid.Span{57, 21, 32, 63}, dvid.Span{57, 22, 32, 63}, dvid.Span{57, 23, 32, 63}, dvid.Span{57, 24, 32, 63}, dvid.Span{57, 25, 32, 63},
				dvid.Span{57, 26, 32, 63}, dvid.Span{57, 27, 32, 63}, dvid.Span{57, 28, 32, 63}, dvid.Span{57, 29, 32, 63}, dvid.Span{57, 30, 32, 63},
				dvid.Span{57, 31, 32, 63}, dvid.Span{58, 20, 32, 63}, dvid.Span{58, 21, 32, 63}, dvid.Span{58, 22, 32, 63}, dvid.Span{58, 23, 32, 63},
				dvid.Span{58, 24, 32, 63}, dvid.Span{58, 25, 32, 63}, dvid.Span{58, 26, 32, 63}, dvid.Span{58, 27, 32, 63}, dvid.Span{58, 28, 32, 63},
				dvid.Span{58, 29, 32, 63}, dvid.Span{58, 30, 32, 63}, dvid.Span{58, 31, 32, 63}, dvid.Span{59, 20, 32, 63}, dvid.Span{59, 21, 32, 63},
				dvid.Span{59, 22, 32, 63}, dvid.Span{59, 23, 32, 63}, dvid.Span{59, 24, 32, 63}, dvid.Span{59, 25, 32, 63}, dvid.Span{59, 26, 32, 63},
				dvid.Span{59, 27, 32, 63}, dvid.Span{59, 28, 32, 63}, dvid.Span{59, 29, 32, 63}, dvid.Span{59, 30, 32, 63}, dvid.Span{59, 31, 32, 63},
				dvid.Span{40, 20, 64, 79}, dvid.Span{40, 21, 64, 79}, dvid.Span{40, 22, 64, 79}, dvid.Span{40, 23, 64, 79}, dvid.Span{40, 24, 64, 79},
				dvid.Span{40, 25, 64, 79}, dvid.Span{40, 26, 64, 79}, dvid.Span{40, 27, 64, 79}, dvid.Span{40, 28, 64, 79}, dvid.Span{40, 29, 64, 79},
				dvid.Span{40, 30, 64, 79}, dvid.Span{40, 31, 64, 79}, dvid.Span{41, 20, 64, 79}, dvid.Span{41, 21, 64, 79}, dvid.Span{41, 22, 64, 79},
				dvid.Span{41, 23, 64, 79}, dvid.Span{41, 24, 64, 79}, dvid.Span{41, 25, 64, 79}, dvid.Span{41, 26, 64, 79}, dvid.Span{41, 27, 64, 79},
				dvid.Span{41, 28, 64, 79}, dvid.Span{41, 29, 64, 79}, dvid.Span{41, 30, 64, 79}, dvid.Span{41, 31, 64, 79}, dvid.Span{42, 20, 64, 79},
				dvid.Span{42, 21, 64, 79}, dvid.Span{42, 22, 64, 79}, dvid.Span{42, 23, 64, 79}, dvid.Span{42, 24, 64, 79}, dvid.Span{42, 25, 64, 79},
				dvid.Span{42, 26, 64, 79}, dvid.Span{42, 27, 64, 79}, dvid.Span{42, 28, 64, 79}, dvid.Span{42, 29, 64, 79}, dvid.Span{42, 30, 64, 79},
				dvid.Span{42, 31, 64, 79}, dvid.Span{43, 20, 64, 79}, dvid.Span{43, 21, 64, 79}, dvid.Span{43, 22, 64, 79}, dvid.Span{43, 23, 64, 79},
				dvid.Span{43, 24, 64, 79}, dvid.Span{43, 25, 64, 79}, dvid.Span{43, 26, 64, 79}, dvid.Span{43, 27, 64, 79}, dvid.Span{43, 28, 64, 79},
				dvid.Span{43, 29, 64, 79}, dvid.Span{43, 30, 64, 79}, dvid.Span{43, 31, 64, 79}, dvid.Span{44, 20, 64, 79}, dvid.Span{44, 21, 64, 79},
				dvid.Span{44, 22, 64, 79}, dvid.Span{44, 23, 64, 79}, dvid.Span{44, 24, 64, 79}, dvid.Span{44, 25, 64, 79}, dvid.Span{44, 26, 64, 79},
				dvid.Span{44, 27, 64, 79}, dvid.Span{44, 28, 64, 79}, dvid.Span{44, 29, 64, 79}, dvid.Span{44, 30, 64, 79}, dvid.Span{44, 31, 64, 79},
				dvid.Span{45, 20, 64, 79}, dvid.Span{45, 21, 64, 79}, dvid.Span{45, 22, 64, 79}, dvid.Span{45, 23, 64, 79}, dvid.Span{45, 24, 64, 79},
				dvid.Span{45, 25, 64, 79}, dvid.Span{45, 26, 64, 79}, dvid.Span{45, 27, 64, 79}, dvid.Span{45, 28, 64, 79}, dvid.Span{45, 29, 64, 79},
				dvid.Span{45, 30, 64, 79}, dvid.Span{45, 31, 64, 79}, dvid.Span{46, 20, 64, 79}, dvid.Span{46, 21, 64, 79}, dvid.Span{46, 22, 64, 79},
				dvid.Span{46, 23, 64, 79}, dvid.Span{46, 24, 64, 79}, dvid.Span{46, 25, 64, 79}, dvid.Span{46, 26, 64, 79}, dvid.Span{46, 27, 64, 79},
				dvid.Span{46, 28, 64, 79}, dvid.Span{46, 29, 64, 79}, dvid.Span{46, 30, 64, 79}, dvid.Span{46, 31, 64, 79}, dvid.Span{47, 20, 64, 79},
				dvid.Span{47, 21, 64, 79}, dvid.Span{47, 22, 64, 79}, dvid.Span{47, 23, 64, 79}, dvid.Span{47, 24, 64, 79}, dvid.Span{47, 25, 64, 79},
				dvid.Span{47, 26, 64, 79}, dvid.Span{47, 27, 64, 79}, dvid.Span{47, 28, 64, 79}, dvid.Span{47, 29, 64, 79}, dvid.Span{47, 30, 64, 79},
				dvid.Span{47, 31, 64, 79}, dvid.Span{48, 20, 64, 79}, dvid.Span{48, 21, 64, 79}, dvid.Span{48, 22, 64, 79}, dvid.Span{48, 23, 64, 79},
				dvid.Span{48, 24, 64, 79}, dvid.Span{48, 25, 64, 79}, dvid.Span{48, 26, 64, 79}, dvid.Span{48, 27, 64, 79}, dvid.Span{48, 28, 64, 79},
				dvid.Span{48, 29, 64, 79}, dvid.Span{48, 30, 64, 79}, dvid.Span{48, 31, 64, 79}, dvid.Span{49, 20, 64, 79}, dvid.Span{49, 21, 64, 79},
				dvid.Span{49, 22, 64, 79}, dvid.Span{49, 23, 64, 79}, dvid.Span{49, 24, 64, 79}, dvid.Span{49, 25, 64, 79}, dvid.Span{49, 26, 64, 79},
				dvid.Span{49, 27, 64, 79}, dvid.Span{49, 28, 64, 79}, dvid.Span{49, 29, 64, 79}, dvid.Span{49, 30, 64, 79}, dvid.Span{49, 31, 64, 79},
				dvid.Span{50, 20, 64, 79}, dvid.Span{50, 21, 64, 79}, dvid.Span{50, 22, 64, 79}, dvid.Span{50, 23, 64, 79}, dvid.Span{50, 24, 64, 79},
				dvid.Span{50, 25, 64, 79}, dvid.Span{50, 26, 64, 79}, dvid.Span{50, 27, 64, 79}, dvid.Span{50, 28, 64, 79}, dvid.Span{50, 29, 64, 79},
				dvid.Span{50, 30, 64, 79}, dvid.Span{50, 31, 64, 79}, dvid.Span{51, 20, 64, 79}, dvid.Span{51, 21, 64, 79}, dvid.Span{51, 22, 64, 79},
				dvid.Span{51, 23, 64, 79}, dvid.Span{51, 24, 64, 79}, dvid.Span{51, 25, 64, 79}, dvid.Span{51, 26, 64, 79}, dvid.Span{51, 27, 64, 79},
				dvid.Span{51, 28, 64, 79}, dvid.Span{51, 29, 64, 79}, dvid.Span{51, 30, 64, 79}, dvid.Span{51, 31, 64, 79}, dvid.Span{52, 20, 64, 79},
				dvid.Span{52, 21, 64, 79}, dvid.Span{52, 22, 64, 79}, dvid.Span{52, 23, 64, 79}, dvid.Span{52, 24, 64, 79}, dvid.Span{52, 25, 64, 79},
				dvid.Span{52, 26, 64, 79}, dvid.Span{52, 27, 64, 79}, dvid.Span{52, 28, 64, 79}, dvid.Span{52, 29, 64, 79}, dvid.Span{52, 30, 64, 79},
				dvid.Span{52, 31, 64, 79}, dvid.Span{53, 20, 64, 79}, dvid.Span{53, 21, 64, 79}, dvid.Span{53, 22, 64, 79}, dvid.Span{53, 23, 64, 79},
				dvid.Span{53, 24, 64, 79}, dvid.Span{53, 25, 64, 79}, dvid.Span{53, 26, 64, 79}, dvid.Span{53, 27, 64, 79}, dvid.Span{53, 28, 64, 79},
				dvid.Span{53, 29, 64, 79}, dvid.Span{53, 30, 64, 79}, dvid.Span{53, 31, 64, 79}, dvid.Span{54, 20, 64, 79}, dvid.Span{54, 21, 64, 79},
				dvid.Span{54, 22, 64, 79}, dvid.Span{54, 23, 64, 79}, dvid.Span{54, 24, 64, 79}, dvid.Span{54, 25, 64, 79}, dvid.Span{54, 26, 64, 79},
				dvid.Span{54, 27, 64, 79}, dvid.Span{54, 28, 64, 79}, dvid.Span{54, 29, 64, 79}, dvid.Span{54, 30, 64, 79}, dvid.Span{54, 31, 64, 79},
				dvid.Span{55, 20, 64, 79}, dvid.Span{55, 21, 64, 79}, dvid.Span{55, 22, 64, 79}, dvid.Span{55, 23, 64, 79}, dvid.Span{55, 24, 64, 79},
				dvid.Span{55, 25, 64, 79}, dvid.Span{55, 26, 64, 79}, dvid.Span{55, 27, 64, 79}, dvid.Span{55, 28, 64, 79}, dvid.Span{55, 29, 64, 79},
				dvid.Span{55, 30, 64, 79}, dvid.Span{55, 31, 64, 79}, dvid.Span{56, 20, 64, 79}, dvid.Span{56, 21, 64, 79}, dvid.Span{56, 22, 64, 79},
				dvid.Span{56, 23, 64, 79}, dvid.Span{56, 24, 64, 79}, dvid.Span{56, 25, 64, 79}, dvid.Span{56, 26, 64, 79}, dvid.Span{56, 27, 64, 79},
				dvid.Span{56, 28, 64, 79}, dvid.Span{56, 29, 64, 79}, dvid.Span{56, 30, 64, 79}, dvid.Span{56, 31, 64, 79}, dvid.Span{57, 20, 64, 79},
				dvid.Span{57, 21, 64, 79}, dvid.Span{57, 22, 64, 79}, dvid.Span{57, 23, 64, 79}, dvid.Span{57, 24, 64, 79}, dvid.Span{57, 25, 64, 79},
				dvid.Span{57, 26, 64, 79}, dvid.Span{57, 27, 64, 79}, dvid.Span{57, 28, 64, 79}, dvid.Span{57, 29, 64, 79}, dvid.Span{57, 30, 64, 79},
				dvid.Span{57, 31, 64, 79}, dvid.Span{58, 20, 64, 79}, dvid.Span{58, 21, 64, 79}, dvid.Span{58, 22, 64, 79}, dvid.Span{58, 23, 64, 79},
				dvid.Span{58, 24, 64, 79}, dvid.Span{58, 25, 64, 79}, dvid.Span{58, 26, 64, 79}, dvid.Span{58, 27, 64, 79}, dvid.Span{58, 28, 64, 79},
				dvid.Span{58, 29, 64, 79}, dvid.Span{58, 30, 64, 79}, dvid.Span{58, 31, 64, 79}, dvid.Span{59, 20, 64, 79}, dvid.Span{59, 21, 64, 79},
				dvid.Span{59, 22, 64, 79}, dvid.Span{59, 23, 64, 79}, dvid.Span{59, 24, 64, 79}, dvid.Span{59, 25, 64, 79}, dvid.Span{59, 26, 64, 79},
				dvid.Span{59, 27, 64, 79}, dvid.Span{59, 28, 64, 79}, dvid.Span{59, 29, 64, 79}, dvid.Span{59, 30, 64, 79}, dvid.Span{59, 31, 64, 79},
				dvid.Span{40, 32, 30, 31}, dvid.Span{40, 33, 30, 31}, dvid.Span{40, 34, 30, 31}, dvid.Span{40, 35, 30, 31}, dvid.Span{40, 36, 30, 31},
				dvid.Span{40, 37, 30, 31}, dvid.Span{40, 38, 30, 31}, dvid.Span{40, 39, 30, 31}, dvid.Span{40, 40, 30, 31}, dvid.Span{40, 41, 30, 31},
				dvid.Span{40, 42, 30, 31}, dvid.Span{40, 43, 30, 31}, dvid.Span{40, 44, 30, 31}, dvid.Span{40, 45, 30, 31}, dvid.Span{40, 46, 30, 31},
				dvid.Span{40, 47, 30, 31}, dvid.Span{40, 48, 30, 31}, dvid.Span{40, 49, 30, 31}, dvid.Span{40, 50, 30, 31}, dvid.Span{40, 51, 30, 31},
				dvid.Span{40, 52, 30, 31}, dvid.Span{40, 53, 30, 31}, dvid.Span{40, 54, 30, 31}, dvid.Span{40, 55, 30, 31}, dvid.Span{40, 56, 30, 31},
				dvid.Span{40, 57, 30, 31}, dvid.Span{40, 58, 30, 31}, dvid.Span{40, 59, 30, 31}, dvid.Span{40, 60, 30, 31}, dvid.Span{40, 61, 30, 31},
				dvid.Span{40, 62, 30, 31}, dvid.Span{40, 63, 30, 31}, dvid.Span{41, 32, 30, 31}, dvid.Span{41, 33, 30, 31}, dvid.Span{41, 34, 30, 31},
				dvid.Span{41, 35, 30, 31}, dvid.Span{41, 36, 30, 31}, dvid.Span{41, 37, 30, 31}, dvid.Span{41, 38, 30, 31}, dvid.Span{41, 39, 30, 31},
				dvid.Span{41, 40, 30, 31}, dvid.Span{41, 41, 30, 31}, dvid.Span{41, 42, 30, 31}, dvid.Span{41, 43, 30, 31}, dvid.Span{41, 44, 30, 31},
				dvid.Span{41, 45, 30, 31}, dvid.Span{41, 46, 30, 31}, dvid.Span{41, 47, 30, 31}, dvid.Span{41, 48, 30, 31}, dvid.Span{41, 49, 30, 31},
				dvid.Span{41, 50, 30, 31}, dvid.Span{41, 51, 30, 31}, dvid.Span{41, 52, 30, 31}, dvid.Span{41, 53, 30, 31}, dvid.Span{41, 54, 30, 31},
				dvid.Span{41, 55, 30, 31}, dvid.Span{41, 56, 30, 31}, dvid.Span{41, 57, 30, 31}, dvid.Span{41, 58, 30, 31}, dvid.Span{41, 59, 30, 31},
				dvid.Span{41, 60, 30, 31}, dvid.Span{41, 61, 30, 31}, dvid.Span{41, 62, 30, 31}, dvid.Span{41, 63, 30, 31}, dvid.Span{42, 32, 30, 31},
				dvid.Span{42, 33, 30, 31}, dvid.Span{42, 34, 30, 31}, dvid.Span{42, 35, 30, 31}, dvid.Span{42, 36, 30, 31}, dvid.Span{42, 37, 30, 31},
				dvid.Span{42, 38, 30, 31}, dvid.Span{42, 39, 30, 31}, dvid.Span{42, 40, 30, 31}, dvid.Span{42, 41, 30, 31}, dvid.Span{42, 42, 30, 31},
				dvid.Span{42, 43, 30, 31}, dvid.Span{42, 44, 30, 31}, dvid.Span{42, 45, 30, 31}, dvid.Span{42, 46, 30, 31}, dvid.Span{42, 47, 30, 31},
				dvid.Span{42, 48, 30, 31}, dvid.Span{42, 49, 30, 31}, dvid.Span{42, 50, 30, 31}, dvid.Span{42, 51, 30, 31}, dvid.Span{42, 52, 30, 31},
				dvid.Span{42, 53, 30, 31}, dvid.Span{42, 54, 30, 31}, dvid.Span{42, 55, 30, 31}, dvid.Span{42, 56, 30, 31}, dvid.Span{42, 57, 30, 31},
				dvid.Span{42, 58, 30, 31}, dvid.Span{42, 59, 30, 31}, dvid.Span{42, 60, 30, 31}, dvid.Span{42, 61, 30, 31}, dvid.Span{42, 62, 30, 31},
				dvid.Span{42, 63, 30, 31}, dvid.Span{43, 32, 30, 31}, dvid.Span{43, 33, 30, 31}, dvid.Span{43, 34, 30, 31}, dvid.Span{43, 35, 30, 31},
				dvid.Span{43, 36, 30, 31}, dvid.Span{43, 37, 30, 31}, dvid.Span{43, 38, 30, 31}, dvid.Span{43, 39, 30, 31}, dvid.Span{43, 40, 30, 31},
				dvid.Span{43, 41, 30, 31}, dvid.Span{43, 42, 30, 31}, dvid.Span{43, 43, 30, 31}, dvid.Span{43, 44, 30, 31}, dvid.Span{43, 45, 30, 31},
				dvid.Span{43, 46, 30, 31}, dvid.Span{43, 47, 30, 31}, dvid.Span{43, 48, 30, 31}, dvid.Span{43, 49, 30, 31}, dvid.Span{43, 50, 30, 31},
				dvid.Span{43, 51, 30, 31}, dvid.Span{43, 52, 30, 31}, dvid.Span{43, 53, 30, 31}, dvid.Span{43, 54, 30, 31}, dvid.Span{43, 55, 30, 31},
				dvid.Span{43, 56, 30, 31}, dvid.Span{43, 57, 30, 31}, dvid.Span{43, 58, 30, 31}, dvid.Span{43, 59, 30, 31}, dvid.Span{43, 60, 30, 31},
				dvid.Span{43, 61, 30, 31}, dvid.Span{43, 62, 30, 31}, dvid.Span{43, 63, 30, 31}, dvid.Span{44, 32, 30, 31}, dvid.Span{44, 33, 30, 31},
				dvid.Span{44, 34, 30, 31}, dvid.Span{44, 35, 30, 31}, dvid.Span{44, 36, 30, 31}, dvid.Span{44, 37, 30, 31}, dvid.Span{44, 38, 30, 31},
				dvid.Span{44, 39, 30, 31}, dvid.Span{44, 40, 30, 31}, dvid.Span{44, 41, 30, 31}, dvid.Span{44, 42, 30, 31}, dvid.Span{44, 43, 30, 31},
				dvid.Span{44, 44, 30, 31}, dvid.Span{44, 45, 30, 31}, dvid.Span{44, 46, 30, 31}, dvid.Span{44, 47, 30, 31}, dvid.Span{44, 48, 30, 31},
				dvid.Span{44, 49, 30, 31}, dvid.Span{44, 50, 30, 31}, dvid.Span{44, 51, 30, 31}, dvid.Span{44, 52, 30, 31}, dvid.Span{44, 53, 30, 31},
				dvid.Span{44, 54, 30, 31}, dvid.Span{44, 55, 30, 31}, dvid.Span{44, 56, 30, 31}, dvid.Span{44, 57, 30, 31}, dvid.Span{44, 58, 30, 31},
				dvid.Span{44, 59, 30, 31}, dvid.Span{44, 60, 30, 31}, dvid.Span{44, 61, 30, 31}, dvid.Span{44, 62, 30, 31}, dvid.Span{44, 63, 30, 31},
				dvid.Span{45, 32, 30, 31}, dvid.Span{45, 33, 30, 31}, dvid.Span{45, 34, 30, 31}, dvid.Span{45, 35, 30, 31}, dvid.Span{45, 36, 30, 31},
				dvid.Span{45, 37, 30, 31}, dvid.Span{45, 38, 30, 31}, dvid.Span{45, 39, 30, 31}, dvid.Span{45, 40, 30, 31}, dvid.Span{45, 41, 30, 31},
				dvid.Span{45, 42, 30, 31}, dvid.Span{45, 43, 30, 31}, dvid.Span{45, 44, 30, 31}, dvid.Span{45, 45, 30, 31}, dvid.Span{45, 46, 30, 31},
				dvid.Span{45, 47, 30, 31}, dvid.Span{45, 48, 30, 31}, dvid.Span{45, 49, 30, 31}, dvid.Span{45, 50, 30, 31}, dvid.Span{45, 51, 30, 31},
				dvid.Span{45, 52, 30, 31}, dvid.Span{45, 53, 30, 31}, dvid.Span{45, 54, 30, 31}, dvid.Span{45, 55, 30, 31}, dvid.Span{45, 56, 30, 31},
				dvid.Span{45, 57, 30, 31}, dvid.Span{45, 58, 30, 31}, dvid.Span{45, 59, 30, 31}, dvid.Span{45, 60, 30, 31}, dvid.Span{45, 61, 30, 31},
				dvid.Span{45, 62, 30, 31}, dvid.Span{45, 63, 30, 31}, dvid.Span{46, 32, 30, 31}, dvid.Span{46, 33, 30, 31}, dvid.Span{46, 34, 30, 31},
				dvid.Span{46, 35, 30, 31}, dvid.Span{46, 36, 30, 31}, dvid.Span{46, 37, 30, 31}, dvid.Span{46, 38, 30, 31}, dvid.Span{46, 39, 30, 31},
				dvid.Span{46, 40, 30, 31}, dvid.Span{46, 41, 30, 31}, dvid.Span{46, 42, 30, 31}, dvid.Span{46, 43, 30, 31}, dvid.Span{46, 44, 30, 31},
				dvid.Span{46, 45, 30, 31}, dvid.Span{46, 46, 30, 31}, dvid.Span{46, 47, 30, 31}, dvid.Span{46, 48, 30, 31}, dvid.Span{46, 49, 30, 31},
				dvid.Span{46, 50, 30, 31}, dvid.Span{46, 51, 30, 31}, dvid.Span{46, 52, 30, 31}, dvid.Span{46, 53, 30, 31}, dvid.Span{46, 54, 30, 31},
				dvid.Span{46, 55, 30, 31}, dvid.Span{46, 56, 30, 31}, dvid.Span{46, 57, 30, 31}, dvid.Span{46, 58, 30, 31}, dvid.Span{46, 59, 30, 31},
				dvid.Span{46, 60, 30, 31}, dvid.Span{46, 61, 30, 31}, dvid.Span{46, 62, 30, 31}, dvid.Span{46, 63, 30, 31}, dvid.Span{47, 32, 30, 31},
				dvid.Span{47, 33, 30, 31}, dvid.Span{47, 34, 30, 31}, dvid.Span{47, 35, 30, 31}, dvid.Span{47, 36, 30, 31}, dvid.Span{47, 37, 30, 31},
				dvid.Span{47, 38, 30, 31}, dvid.Span{47, 39, 30, 31}, dvid.Span{47, 40, 30, 31}, dvid.Span{47, 41, 30, 31}, dvid.Span{47, 42, 30, 31},
				dvid.Span{47, 43, 30, 31}, dvid.Span{47, 44, 30, 31}, dvid.Span{47, 45, 30, 31}, dvid.Span{47, 46, 30, 31}, dvid.Span{47, 47, 30, 31},
				dvid.Span{47, 48, 30, 31}, dvid.Span{47, 49, 30, 31}, dvid.Span{47, 50, 30, 31}, dvid.Span{47, 51, 30, 31}, dvid.Span{47, 52, 30, 31},
				dvid.Span{47, 53, 30, 31}, dvid.Span{47, 54, 30, 31}, dvid.Span{47, 55, 30, 31}, dvid.Span{47, 56, 30, 31}, dvid.Span{47, 57, 30, 31},
				dvid.Span{47, 58, 30, 31}, dvid.Span{47, 59, 30, 31}, dvid.Span{47, 60, 30, 31}, dvid.Span{47, 61, 30, 31}, dvid.Span{47, 62, 30, 31},
				dvid.Span{47, 63, 30, 31}, dvid.Span{48, 32, 30, 31}, dvid.Span{48, 33, 30, 31}, dvid.Span{48, 34, 30, 31}, dvid.Span{48, 35, 30, 31},
				dvid.Span{48, 36, 30, 31}, dvid.Span{48, 37, 30, 31}, dvid.Span{48, 38, 30, 31}, dvid.Span{48, 39, 30, 31}, dvid.Span{48, 40, 30, 31},
				dvid.Span{48, 41, 30, 31}, dvid.Span{48, 42, 30, 31}, dvid.Span{48, 43, 30, 31}, dvid.Span{48, 44, 30, 31}, dvid.Span{48, 45, 30, 31},
				dvid.Span{48, 46, 30, 31}, dvid.Span{48, 47, 30, 31}, dvid.Span{48, 48, 30, 31}, dvid.Span{48, 49, 30, 31}, dvid.Span{48, 50, 30, 31},
				dvid.Span{48, 51, 30, 31}, dvid.Span{48, 52, 30, 31}, dvid.Span{48, 53, 30, 31}, dvid.Span{48, 54, 30, 31}, dvid.Span{48, 55, 30, 31},
				dvid.Span{48, 56, 30, 31}, dvid.Span{48, 57, 30, 31}, dvid.Span{48, 58, 30, 31}, dvid.Span{48, 59, 30, 31}, dvid.Span{48, 60, 30, 31},
				dvid.Span{48, 61, 30, 31}, dvid.Span{48, 62, 30, 31}, dvid.Span{48, 63, 30, 31}, dvid.Span{49, 32, 30, 31}, dvid.Span{49, 33, 30, 31},
				dvid.Span{49, 34, 30, 31}, dvid.Span{49, 35, 30, 31}, dvid.Span{49, 36, 30, 31}, dvid.Span{49, 37, 30, 31}, dvid.Span{49, 38, 30, 31},
				dvid.Span{49, 39, 30, 31}, dvid.Span{49, 40, 30, 31}, dvid.Span{49, 41, 30, 31}, dvid.Span{49, 42, 30, 31}, dvid.Span{49, 43, 30, 31},
				dvid.Span{49, 44, 30, 31}, dvid.Span{49, 45, 30, 31}, dvid.Span{49, 46, 30, 31}, dvid.Span{49, 47, 30, 31}, dvid.Span{49, 48, 30, 31},
				dvid.Span{49, 49, 30, 31}, dvid.Span{49, 50, 30, 31}, dvid.Span{49, 51, 30, 31}, dvid.Span{49, 52, 30, 31}, dvid.Span{49, 53, 30, 31},
				dvid.Span{49, 54, 30, 31}, dvid.Span{49, 55, 30, 31}, dvid.Span{49, 56, 30, 31}, dvid.Span{49, 57, 30, 31}, dvid.Span{49, 58, 30, 31},
				dvid.Span{49, 59, 30, 31}, dvid.Span{49, 60, 30, 31}, dvid.Span{49, 61, 30, 31}, dvid.Span{49, 62, 30, 31}, dvid.Span{49, 63, 30, 31},
				dvid.Span{50, 32, 30, 31}, dvid.Span{50, 33, 30, 31}, dvid.Span{50, 34, 30, 31}, dvid.Span{50, 35, 30, 31}, dvid.Span{50, 36, 30, 31},
				dvid.Span{50, 37, 30, 31}, dvid.Span{50, 38, 30, 31}, dvid.Span{50, 39, 30, 31}, dvid.Span{50, 40, 30, 31}, dvid.Span{50, 41, 30, 31},
				dvid.Span{50, 42, 30, 31}, dvid.Span{50, 43, 30, 31}, dvid.Span{50, 44, 30, 31}, dvid.Span{50, 45, 30, 31}, dvid.Span{50, 46, 30, 31},
				dvid.Span{50, 47, 30, 31}, dvid.Span{50, 48, 30, 31}, dvid.Span{50, 49, 30, 31}, dvid.Span{50, 50, 30, 31}, dvid.Span{50, 51, 30, 31},
				dvid.Span{50, 52, 30, 31}, dvid.Span{50, 53, 30, 31}, dvid.Span{50, 54, 30, 31}, dvid.Span{50, 55, 30, 31}, dvid.Span{50, 56, 30, 31},
				dvid.Span{50, 57, 30, 31}, dvid.Span{50, 58, 30, 31}, dvid.Span{50, 59, 30, 31}, dvid.Span{50, 60, 30, 31}, dvid.Span{50, 61, 30, 31},
				dvid.Span{50, 62, 30, 31}, dvid.Span{50, 63, 30, 31}, dvid.Span{51, 32, 30, 31}, dvid.Span{51, 33, 30, 31}, dvid.Span{51, 34, 30, 31},
				dvid.Span{51, 35, 30, 31}, dvid.Span{51, 36, 30, 31}, dvid.Span{51, 37, 30, 31}, dvid.Span{51, 38, 30, 31}, dvid.Span{51, 39, 30, 31},
				dvid.Span{51, 40, 30, 31}, dvid.Span{51, 41, 30, 31}, dvid.Span{51, 42, 30, 31}, dvid.Span{51, 43, 30, 31}, dvid.Span{51, 44, 30, 31},
				dvid.Span{51, 45, 30, 31}, dvid.Span{51, 46, 30, 31}, dvid.Span{51, 47, 30, 31}, dvid.Span{51, 48, 30, 31}, dvid.Span{51, 49, 30, 31},
				dvid.Span{51, 50, 30, 31}, dvid.Span{51, 51, 30, 31}, dvid.Span{51, 52, 30, 31}, dvid.Span{51, 53, 30, 31}, dvid.Span{51, 54, 30, 31},
				dvid.Span{51, 55, 30, 31}, dvid.Span{51, 56, 30, 31}, dvid.Span{51, 57, 30, 31}, dvid.Span{51, 58, 30, 31}, dvid.Span{51, 59, 30, 31},
				dvid.Span{51, 60, 30, 31}, dvid.Span{51, 61, 30, 31}, dvid.Span{51, 62, 30, 31}, dvid.Span{51, 63, 30, 31}, dvid.Span{52, 32, 30, 31},
				dvid.Span{52, 33, 30, 31}, dvid.Span{52, 34, 30, 31}, dvid.Span{52, 35, 30, 31}, dvid.Span{52, 36, 30, 31}, dvid.Span{52, 37, 30, 31},
				dvid.Span{52, 38, 30, 31}, dvid.Span{52, 39, 30, 31}, dvid.Span{52, 40, 30, 31}, dvid.Span{52, 41, 30, 31}, dvid.Span{52, 42, 30, 31},
				dvid.Span{52, 43, 30, 31}, dvid.Span{52, 44, 30, 31}, dvid.Span{52, 45, 30, 31}, dvid.Span{52, 46, 30, 31}, dvid.Span{52, 47, 30, 31},
				dvid.Span{52, 48, 30, 31}, dvid.Span{52, 49, 30, 31}, dvid.Span{52, 50, 30, 31}, dvid.Span{52, 51, 30, 31}, dvid.Span{52, 52, 30, 31},
				dvid.Span{52, 53, 30, 31}, dvid.Span{52, 54, 30, 31}, dvid.Span{52, 55, 30, 31}, dvid.Span{52, 56, 30, 31}, dvid.Span{52, 57, 30, 31},
				dvid.Span{52, 58, 30, 31}, dvid.Span{52, 59, 30, 31}, dvid.Span{52, 60, 30, 31}, dvid.Span{52, 61, 30, 31}, dvid.Span{52, 62, 30, 31},
				dvid.Span{52, 63, 30, 31}, dvid.Span{53, 32, 30, 31}, dvid.Span{53, 33, 30, 31}, dvid.Span{53, 34, 30, 31}, dvid.Span{53, 35, 30, 31},
				dvid.Span{53, 36, 30, 31}, dvid.Span{53, 37, 30, 31}, dvid.Span{53, 38, 30, 31}, dvid.Span{53, 39, 30, 31}, dvid.Span{53, 40, 30, 31},
				dvid.Span{53, 41, 30, 31}, dvid.Span{53, 42, 30, 31}, dvid.Span{53, 43, 30, 31}, dvid.Span{53, 44, 30, 31}, dvid.Span{53, 45, 30, 31},
				dvid.Span{53, 46, 30, 31}, dvid.Span{53, 47, 30, 31}, dvid.Span{53, 48, 30, 31}, dvid.Span{53, 49, 30, 31}, dvid.Span{53, 50, 30, 31},
				dvid.Span{53, 51, 30, 31}, dvid.Span{53, 52, 30, 31}, dvid.Span{53, 53, 30, 31}, dvid.Span{53, 54, 30, 31}, dvid.Span{53, 55, 30, 31},
				dvid.Span{53, 56, 30, 31}, dvid.Span{53, 57, 30, 31}, dvid.Span{53, 58, 30, 31}, dvid.Span{53, 59, 30, 31}, dvid.Span{53, 60, 30, 31},
				dvid.Span{53, 61, 30, 31}, dvid.Span{53, 62, 30, 31}, dvid.Span{53, 63, 30, 31}, dvid.Span{54, 32, 30, 31}, dvid.Span{54, 33, 30, 31},
				dvid.Span{54, 34, 30, 31}, dvid.Span{54, 35, 30, 31}, dvid.Span{54, 36, 30, 31}, dvid.Span{54, 37, 30, 31}, dvid.Span{54, 38, 30, 31},
				dvid.Span{54, 39, 30, 31}, dvid.Span{54, 40, 30, 31}, dvid.Span{54, 41, 30, 31}, dvid.Span{54, 42, 30, 31}, dvid.Span{54, 43, 30, 31},
				dvid.Span{54, 44, 30, 31}, dvid.Span{54, 45, 30, 31}, dvid.Span{54, 46, 30, 31}, dvid.Span{54, 47, 30, 31}, dvid.Span{54, 48, 30, 31},
				dvid.Span{54, 49, 30, 31}, dvid.Span{54, 50, 30, 31}, dvid.Span{54, 51, 30, 31}, dvid.Span{54, 52, 30, 31}, dvid.Span{54, 53, 30, 31},
				dvid.Span{54, 54, 30, 31}, dvid.Span{54, 55, 30, 31}, dvid.Span{54, 56, 30, 31}, dvid.Span{54, 57, 30, 31}, dvid.Span{54, 58, 30, 31},
				dvid.Span{54, 59, 30, 31}, dvid.Span{54, 60, 30, 31}, dvid.Span{54, 61, 30, 31}, dvid.Span{54, 62, 30, 31}, dvid.Span{54, 63, 30, 31},
				dvid.Span{55, 32, 30, 31}, dvid.Span{55, 33, 30, 31}, dvid.Span{55, 34, 30, 31}, dvid.Span{55, 35, 30, 31}, dvid.Span{55, 36, 30, 31},
				dvid.Span{55, 37, 30, 31}, dvid.Span{55, 38, 30, 31}, dvid.Span{55, 39, 30, 31}, dvid.Span{55, 40, 30, 31}, dvid.Span{55, 41, 30, 31},
				dvid.Span{55, 42, 30, 31}, dvid.Span{55, 43, 30, 31}, dvid.Span{55, 44, 30, 31}, dvid.Span{55, 45, 30, 31}, dvid.Span{55, 46, 30, 31},
				dvid.Span{55, 47, 30, 31}, dvid.Span{55, 48, 30, 31}, dvid.Span{55, 49, 30, 31}, dvid.Span{55, 50, 30, 31}, dvid.Span{55, 51, 30, 31},
				dvid.Span{55, 52, 30, 31}, dvid.Span{55, 53, 30, 31}, dvid.Span{55, 54, 30, 31}, dvid.Span{55, 55, 30, 31}, dvid.Span{55, 56, 30, 31},
				dvid.Span{55, 57, 30, 31}, dvid.Span{55, 58, 30, 31}, dvid.Span{55, 59, 30, 31}, dvid.Span{55, 60, 30, 31}, dvid.Span{55, 61, 30, 31},
				dvid.Span{55, 62, 30, 31}, dvid.Span{55, 63, 30, 31}, dvid.Span{56, 32, 30, 31}, dvid.Span{56, 33, 30, 31}, dvid.Span{56, 34, 30, 31},
				dvid.Span{56, 35, 30, 31}, dvid.Span{56, 36, 30, 31}, dvid.Span{56, 37, 30, 31}, dvid.Span{56, 38, 30, 31}, dvid.Span{56, 39, 30, 31},
				dvid.Span{56, 40, 30, 31}, dvid.Span{56, 41, 30, 31}, dvid.Span{56, 42, 30, 31}, dvid.Span{56, 43, 30, 31}, dvid.Span{56, 44, 30, 31},
				dvid.Span{56, 45, 30, 31}, dvid.Span{56, 46, 30, 31}, dvid.Span{56, 47, 30, 31}, dvid.Span{56, 48, 30, 31}, dvid.Span{56, 49, 30, 31},
				dvid.Span{56, 50, 30, 31}, dvid.Span{56, 51, 30, 31}, dvid.Span{56, 52, 30, 31}, dvid.Span{56, 53, 30, 31}, dvid.Span{56, 54, 30, 31},
				dvid.Span{56, 55, 30, 31}, dvid.Span{56, 56, 30, 31}, dvid.Span{56, 57, 30, 31}, dvid.Span{56, 58, 30, 31}, dvid.Span{56, 59, 30, 31},
				dvid.Span{56, 60, 30, 31}, dvid.Span{56, 61, 30, 31}, dvid.Span{56, 62, 30, 31}, dvid.Span{56, 63, 30, 31}, dvid.Span{57, 32, 30, 31},
				dvid.Span{57, 33, 30, 31}, dvid.Span{57, 34, 30, 31}, dvid.Span{57, 35, 30, 31}, dvid.Span{57, 36, 30, 31}, dvid.Span{57, 37, 30, 31},
				dvid.Span{57, 38, 30, 31}, dvid.Span{57, 39, 30, 31}, dvid.Span{57, 40, 30, 31}, dvid.Span{57, 41, 30, 31}, dvid.Span{57, 42, 30, 31},
				dvid.Span{57, 43, 30, 31}, dvid.Span{57, 44, 30, 31}, dvid.Span{57, 45, 30, 31}, dvid.Span{57, 46, 30, 31}, dvid.Span{57, 47, 30, 31},
				dvid.Span{57, 48, 30, 31}, dvid.Span{57, 49, 30, 31}, dvid.Span{57, 50, 30, 31}, dvid.Span{57, 51, 30, 31}, dvid.Span{57, 52, 30, 31},
				dvid.Span{57, 53, 30, 31}, dvid.Span{57, 54, 30, 31}, dvid.Span{57, 55, 30, 31}, dvid.Span{57, 56, 30, 31}, dvid.Span{57, 57, 30, 31},
				dvid.Span{57, 58, 30, 31}, dvid.Span{57, 59, 30, 31}, dvid.Span{57, 60, 30, 31}, dvid.Span{57, 61, 30, 31}, dvid.Span{57, 62, 30, 31},
				dvid.Span{57, 63, 30, 31}, dvid.Span{58, 32, 30, 31}, dvid.Span{58, 33, 30, 31}, dvid.Span{58, 34, 30, 31}, dvid.Span{58, 35, 30, 31},
				dvid.Span{58, 36, 30, 31}, dvid.Span{58, 37, 30, 31}, dvid.Span{58, 38, 30, 31}, dvid.Span{58, 39, 30, 31}, dvid.Span{58, 40, 30, 31},
				dvid.Span{58, 41, 30, 31}, dvid.Span{58, 42, 30, 31}, dvid.Span{58, 43, 30, 31}, dvid.Span{58, 44, 30, 31}, dvid.Span{58, 45, 30, 31},
				dvid.Span{58, 46, 30, 31}, dvid.Span{58, 47, 30, 31}, dvid.Span{58, 48, 30, 31}, dvid.Span{58, 49, 30, 31}, dvid.Span{58, 50, 30, 31},
				dvid.Span{58, 51, 30, 31}, dvid.Span{58, 52, 30, 31}, dvid.Span{58, 53, 30, 31}, dvid.Span{58, 54, 30, 31}, dvid.Span{58, 55, 30, 31},
				dvid.Span{58, 56, 30, 31}, dvid.Span{58, 57, 30, 31}, dvid.Span{58, 58, 30, 31}, dvid.Span{58, 59, 30, 31}, dvid.Span{58, 60, 30, 31},
				dvid.Span{58, 61, 30, 31}, dvid.Span{58, 62, 30, 31}, dvid.Span{58, 63, 30, 31}, dvid.Span{59, 32, 30, 31}, dvid.Span{59, 33, 30, 31},
				dvid.Span{59, 34, 30, 31}, dvid.Span{59, 35, 30, 31}, dvid.Span{59, 36, 30, 31}, dvid.Span{59, 37, 30, 31}, dvid.Span{59, 38, 30, 31},
				dvid.Span{59, 39, 30, 31}, dvid.Span{59, 40, 30, 31}, dvid.Span{59, 41, 30, 31}, dvid.Span{59, 42, 30, 31}, dvid.Span{59, 43, 30, 31},
				dvid.Span{59, 44, 30, 31}, dvid.Span{59, 45, 30, 31}, dvid.Span{59, 46, 30, 31}, dvid.Span{59, 47, 30, 31}, dvid.Span{59, 48, 30, 31},
				dvid.Span{59, 49, 30, 31}, dvid.Span{59, 50, 30, 31}, dvid.Span{59, 51, 30, 31}, dvid.Span{59, 52, 30, 31}, dvid.Span{59, 53, 30, 31},
				dvid.Span{59, 54, 30, 31}, dvid.Span{59, 55, 30, 31}, dvid.Span{59, 56, 30, 31}, dvid.Span{59, 57, 30, 31}, dvid.Span{59, 58, 30, 31},
				dvid.Span{59, 59, 30, 31}, dvid.Span{59, 60, 30, 31}, dvid.Span{59, 61, 30, 31}, dvid.Span{59, 62, 30, 31}, dvid.Span{59, 63, 30, 31},
				dvid.Span{40, 32, 32, 63}, dvid.Span{40, 33, 32, 63}, dvid.Span{40, 34, 32, 63}, dvid.Span{40, 35, 32, 63}, dvid.Span{40, 36, 32, 63},
				dvid.Span{40, 37, 32, 63}, dvid.Span{40, 38, 32, 63}, dvid.Span{40, 39, 32, 63}, dvid.Span{40, 40, 32, 63}, dvid.Span{40, 41, 32, 63},
				dvid.Span{40, 42, 32, 63}, dvid.Span{40, 43, 32, 63}, dvid.Span{40, 44, 32, 63}, dvid.Span{40, 45, 32, 63}, dvid.Span{40, 46, 32, 63},
				dvid.Span{40, 47, 32, 63}, dvid.Span{40, 48, 32, 63}, dvid.Span{40, 49, 32, 63}, dvid.Span{40, 50, 32, 63}, dvid.Span{40, 51, 32, 63},
				dvid.Span{40, 52, 32, 63}, dvid.Span{40, 53, 32, 63}, dvid.Span{40, 54, 32, 63}, dvid.Span{40, 55, 32, 63}, dvid.Span{40, 56, 32, 63},
				dvid.Span{40, 57, 32, 63}, dvid.Span{40, 58, 32, 63}, dvid.Span{40, 59, 32, 63}, dvid.Span{40, 60, 32, 63}, dvid.Span{40, 61, 32, 63},
				dvid.Span{40, 62, 32, 63}, dvid.Span{40, 63, 32, 63}, dvid.Span{41, 32, 32, 63}, dvid.Span{41, 33, 32, 63}, dvid.Span{41, 34, 32, 63},
				dvid.Span{41, 35, 32, 63}, dvid.Span{41, 36, 32, 63}, dvid.Span{41, 37, 32, 63}, dvid.Span{41, 38, 32, 63}, dvid.Span{41, 39, 32, 63},
				dvid.Span{41, 40, 32, 63}, dvid.Span{41, 41, 32, 63}, dvid.Span{41, 42, 32, 63}, dvid.Span{41, 43, 32, 63}, dvid.Span{41, 44, 32, 63},
				dvid.Span{41, 45, 32, 63}, dvid.Span{41, 46, 32, 63}, dvid.Span{41, 47, 32, 63}, dvid.Span{41, 48, 32, 63}, dvid.Span{41, 49, 32, 63},
				dvid.Span{41, 50, 32, 63}, dvid.Span{41, 51, 32, 63}, dvid.Span{41, 52, 32, 63}, dvid.Span{41, 53, 32, 63}, dvid.Span{41, 54, 32, 63},
				dvid.Span{41, 55, 32, 63}, dvid.Span{41, 56, 32, 63}, dvid.Span{41, 57, 32, 63}, dvid.Span{41, 58, 32, 63}, dvid.Span{41, 59, 32, 63},
				dvid.Span{41, 60, 32, 63}, dvid.Span{41, 61, 32, 63}, dvid.Span{41, 62, 32, 63}, dvid.Span{41, 63, 32, 63}, dvid.Span{42, 32, 32, 63},
				dvid.Span{42, 33, 32, 63}, dvid.Span{42, 34, 32, 63}, dvid.Span{42, 35, 32, 63}, dvid.Span{42, 36, 32, 63}, dvid.Span{42, 37, 32, 63},
				dvid.Span{42, 38, 32, 63}, dvid.Span{42, 39, 32, 63}, dvid.Span{42, 40, 32, 63}, dvid.Span{42, 41, 32, 63}, dvid.Span{42, 42, 32, 63},
				dvid.Span{42, 43, 32, 63}, dvid.Span{42, 44, 32, 63}, dvid.Span{42, 45, 32, 63}, dvid.Span{42, 46, 32, 63}, dvid.Span{42, 47, 32, 63},
				dvid.Span{42, 48, 32, 63}, dvid.Span{42, 49, 32, 63}, dvid.Span{42, 50, 32, 63}, dvid.Span{42, 51, 32, 63}, dvid.Span{42, 52, 32, 63},
				dvid.Span{42, 53, 32, 63}, dvid.Span{42, 54, 32, 63}, dvid.Span{42, 55, 32, 63}, dvid.Span{42, 56, 32, 63}, dvid.Span{42, 57, 32, 63},
				dvid.Span{42, 58, 32, 63}, dvid.Span{42, 59, 32, 63}, dvid.Span{42, 60, 32, 63}, dvid.Span{42, 61, 32, 63}, dvid.Span{42, 62, 32, 63},
				dvid.Span{42, 63, 32, 63}, dvid.Span{43, 32, 32, 63}, dvid.Span{43, 33, 32, 63}, dvid.Span{43, 34, 32, 63}, dvid.Span{43, 35, 32, 63},
				dvid.Span{43, 36, 32, 63}, dvid.Span{43, 37, 32, 63}, dvid.Span{43, 38, 32, 63}, dvid.Span{43, 39, 32, 63}, dvid.Span{43, 40, 32, 63},
				dvid.Span{43, 41, 32, 63}, dvid.Span{43, 42, 32, 63}, dvid.Span{43, 43, 32, 63}, dvid.Span{43, 44, 32, 63}, dvid.Span{43, 45, 32, 63},
				dvid.Span{43, 46, 32, 63}, dvid.Span{43, 47, 32, 63}, dvid.Span{43, 48, 32, 63}, dvid.Span{43, 49, 32, 63}, dvid.Span{43, 50, 32, 63},
				dvid.Span{43, 51, 32, 63}, dvid.Span{43, 52, 32, 63}, dvid.Span{43, 53, 32, 63}, dvid.Span{43, 54, 32, 63}, dvid.Span{43, 55, 32, 63},
				dvid.Span{43, 56, 32, 63}, dvid.Span{43, 57, 32, 63}, dvid.Span{43, 58, 32, 63}, dvid.Span{43, 59, 32, 63}, dvid.Span{43, 60, 32, 63},
				dvid.Span{43, 61, 32, 63}, dvid.Span{43, 62, 32, 63}, dvid.Span{43, 63, 32, 63}, dvid.Span{44, 32, 32, 63}, dvid.Span{44, 33, 32, 63},
				dvid.Span{44, 34, 32, 63}, dvid.Span{44, 35, 32, 63}, dvid.Span{44, 36, 32, 63}, dvid.Span{44, 37, 32, 63}, dvid.Span{44, 38, 32, 63},
				dvid.Span{44, 39, 32, 63}, dvid.Span{44, 40, 32, 63}, dvid.Span{44, 41, 32, 63}, dvid.Span{44, 42, 32, 63}, dvid.Span{44, 43, 32, 63},
				dvid.Span{44, 44, 32, 63}, dvid.Span{44, 45, 32, 63}, dvid.Span{44, 46, 32, 63}, dvid.Span{44, 47, 32, 63}, dvid.Span{44, 48, 32, 63},
				dvid.Span{44, 49, 32, 63}, dvid.Span{44, 50, 32, 63}, dvid.Span{44, 51, 32, 63}, dvid.Span{44, 52, 32, 63}, dvid.Span{44, 53, 32, 63},
				dvid.Span{44, 54, 32, 63}, dvid.Span{44, 55, 32, 63}, dvid.Span{44, 56, 32, 63}, dvid.Span{44, 57, 32, 63}, dvid.Span{44, 58, 32, 63},
				dvid.Span{44, 59, 32, 63}, dvid.Span{44, 60, 32, 63}, dvid.Span{44, 61, 32, 63}, dvid.Span{44, 62, 32, 63}, dvid.Span{44, 63, 32, 63},
				dvid.Span{45, 32, 32, 63}, dvid.Span{45, 33, 32, 63}, dvid.Span{45, 34, 32, 63}, dvid.Span{45, 35, 32, 63}, dvid.Span{45, 36, 32, 63},
				dvid.Span{45, 37, 32, 63}, dvid.Span{45, 38, 32, 63}, dvid.Span{45, 39, 32, 63}, dvid.Span{45, 40, 32, 63}, dvid.Span{45, 41, 32, 63},
				dvid.Span{45, 42, 32, 63}, dvid.Span{45, 43, 32, 63}, dvid.Span{45, 44, 32, 63}, dvid.Span{45, 45, 32, 63}, dvid.Span{45, 46, 32, 63},
				dvid.Span{45, 47, 32, 63}, dvid.Span{45, 48, 32, 63}, dvid.Span{45, 49, 32, 63}, dvid.Span{45, 50, 32, 63}, dvid.Span{45, 51, 32, 63},
				dvid.Span{45, 52, 32, 63}, dvid.Span{45, 53, 32, 63}, dvid.Span{45, 54, 32, 63}, dvid.Span{45, 55, 32, 63}, dvid.Span{45, 56, 32, 63},
				dvid.Span{45, 57, 32, 63}, dvid.Span{45, 58, 32, 63}, dvid.Span{45, 59, 32, 63}, dvid.Span{45, 60, 32, 63}, dvid.Span{45, 61, 32, 63},
				dvid.Span{45, 62, 32, 63}, dvid.Span{45, 63, 32, 63}, dvid.Span{46, 32, 32, 63}, dvid.Span{46, 33, 32, 63}, dvid.Span{46, 34, 32, 63},
				dvid.Span{46, 35, 32, 63}, dvid.Span{46, 36, 32, 63}, dvid.Span{46, 37, 32, 63}, dvid.Span{46, 38, 32, 63}, dvid.Span{46, 39, 32, 63},
				dvid.Span{46, 40, 32, 63}, dvid.Span{46, 41, 32, 63}, dvid.Span{46, 42, 32, 63}, dvid.Span{46, 43, 32, 63}, dvid.Span{46, 44, 32, 63},
				dvid.Span{46, 45, 32, 63}, dvid.Span{46, 46, 32, 63}, dvid.Span{46, 47, 32, 63}, dvid.Span{46, 48, 32, 63}, dvid.Span{46, 49, 32, 63},
				dvid.Span{46, 50, 32, 63}, dvid.Span{46, 51, 32, 63}, dvid.Span{46, 52, 32, 63}, dvid.Span{46, 53, 32, 63}, dvid.Span{46, 54, 32, 63},
				dvid.Span{46, 55, 32, 63}, dvid.Span{46, 56, 32, 63}, dvid.Span{46, 57, 32, 63}, dvid.Span{46, 58, 32, 63}, dvid.Span{46, 59, 32, 63},
				dvid.Span{46, 60, 32, 63}, dvid.Span{46, 61, 32, 63}, dvid.Span{46, 62, 32, 63}, dvid.Span{46, 63, 32, 63}, dvid.Span{47, 32, 32, 63},
				dvid.Span{47, 33, 32, 63}, dvid.Span{47, 34, 32, 63}, dvid.Span{47, 35, 32, 63}, dvid.Span{47, 36, 32, 63}, dvid.Span{47, 37, 32, 63},
				dvid.Span{47, 38, 32, 63}, dvid.Span{47, 39, 32, 63}, dvid.Span{47, 40, 32, 63}, dvid.Span{47, 41, 32, 63}, dvid.Span{47, 42, 32, 63},
				dvid.Span{47, 43, 32, 63}, dvid.Span{47, 44, 32, 63}, dvid.Span{47, 45, 32, 63}, dvid.Span{47, 46, 32, 63}, dvid.Span{47, 47, 32, 63},
				dvid.Span{47, 48, 32, 63}, dvid.Span{47, 49, 32, 63}, dvid.Span{47, 50, 32, 63}, dvid.Span{47, 51, 32, 63}, dvid.Span{47, 52, 32, 63},
				dvid.Span{47, 53, 32, 63}, dvid.Span{47, 54, 32, 63}, dvid.Span{47, 55, 32, 63}, dvid.Span{47, 56, 32, 63}, dvid.Span{47, 57, 32, 63},
				dvid.Span{47, 58, 32, 63}, dvid.Span{47, 59, 32, 63}, dvid.Span{47, 60, 32, 63}, dvid.Span{47, 61, 32, 63}, dvid.Span{47, 62, 32, 63},
				dvid.Span{47, 63, 32, 63}, dvid.Span{48, 32, 32, 63}, dvid.Span{48, 33, 32, 63}, dvid.Span{48, 34, 32, 63}, dvid.Span{48, 35, 32, 63},
				dvid.Span{48, 36, 32, 63}, dvid.Span{48, 37, 32, 63}, dvid.Span{48, 38, 32, 63}, dvid.Span{48, 39, 32, 63}, dvid.Span{48, 40, 32, 63},
				dvid.Span{48, 41, 32, 63}, dvid.Span{48, 42, 32, 63}, dvid.Span{48, 43, 32, 63}, dvid.Span{48, 44, 32, 63}, dvid.Span{48, 45, 32, 63},
				dvid.Span{48, 46, 32, 63}, dvid.Span{48, 47, 32, 63}, dvid.Span{48, 48, 32, 63}, dvid.Span{48, 49, 32, 63}, dvid.Span{48, 50, 32, 63},
				dvid.Span{48, 51, 32, 63}, dvid.Span{48, 52, 32, 63}, dvid.Span{48, 53, 32, 63}, dvid.Span{48, 54, 32, 63}, dvid.Span{48, 55, 32, 63},
				dvid.Span{48, 56, 32, 63}, dvid.Span{48, 57, 32, 63}, dvid.Span{48, 58, 32, 63}, dvid.Span{48, 59, 32, 63}, dvid.Span{48, 60, 32, 63},
				dvid.Span{48, 61, 32, 63}, dvid.Span{48, 62, 32, 63}, dvid.Span{48, 63, 32, 63}, dvid.Span{49, 32, 32, 63}, dvid.Span{49, 33, 32, 63},
				dvid.Span{49, 34, 32, 63}, dvid.Span{49, 35, 32, 63}, dvid.Span{49, 36, 32, 63}, dvid.Span{49, 37, 32, 63}, dvid.Span{49, 38, 32, 63},
				dvid.Span{49, 39, 32, 63}, dvid.Span{49, 40, 32, 63}, dvid.Span{49, 41, 32, 63}, dvid.Span{49, 42, 32, 63}, dvid.Span{49, 43, 32, 63},
				dvid.Span{49, 44, 32, 63}, dvid.Span{49, 45, 32, 63}, dvid.Span{49, 46, 32, 63}, dvid.Span{49, 47, 32, 63}, dvid.Span{49, 48, 32, 63},
				dvid.Span{49, 49, 32, 63}, dvid.Span{49, 50, 32, 63}, dvid.Span{49, 51, 32, 63}, dvid.Span{49, 52, 32, 63}, dvid.Span{49, 53, 32, 63},
				dvid.Span{49, 54, 32, 63}, dvid.Span{49, 55, 32, 63}, dvid.Span{49, 56, 32, 63}, dvid.Span{49, 57, 32, 63}, dvid.Span{49, 58, 32, 63},
				dvid.Span{49, 59, 32, 63}, dvid.Span{49, 60, 32, 63}, dvid.Span{49, 61, 32, 63}, dvid.Span{49, 62, 32, 63}, dvid.Span{49, 63, 32, 63},
				dvid.Span{50, 32, 32, 63}, dvid.Span{50, 33, 32, 63}, dvid.Span{50, 34, 32, 63}, dvid.Span{50, 35, 32, 63}, dvid.Span{50, 36, 32, 63},
				dvid.Span{50, 37, 32, 63}, dvid.Span{50, 38, 32, 63}, dvid.Span{50, 39, 32, 63}, dvid.Span{50, 40, 32, 63}, dvid.Span{50, 41, 32, 63},
				dvid.Span{50, 42, 32, 63}, dvid.Span{50, 43, 32, 63}, dvid.Span{50, 44, 32, 63}, dvid.Span{50, 45, 32, 63}, dvid.Span{50, 46, 32, 63},
				dvid.Span{50, 47, 32, 63}, dvid.Span{50, 48, 32, 63}, dvid.Span{50, 49, 32, 63}, dvid.Span{50, 50, 32, 63}, dvid.Span{50, 51, 32, 63},
				dvid.Span{50, 52, 32, 63}, dvid.Span{50, 53, 32, 63}, dvid.Span{50, 54, 32, 63}, dvid.Span{50, 55, 32, 63}, dvid.Span{50, 56, 32, 63},
				dvid.Span{50, 57, 32, 63}, dvid.Span{50, 58, 32, 63}, dvid.Span{50, 59, 32, 63}, dvid.Span{50, 60, 32, 63}, dvid.Span{50, 61, 32, 63},
				dvid.Span{50, 62, 32, 63}, dvid.Span{50, 63, 32, 63}, dvid.Span{51, 32, 32, 63}, dvid.Span{51, 33, 32, 63}, dvid.Span{51, 34, 32, 63},
				dvid.Span{51, 35, 32, 63}, dvid.Span{51, 36, 32, 63}, dvid.Span{51, 37, 32, 63}, dvid.Span{51, 38, 32, 63}, dvid.Span{51, 39, 32, 63},
				dvid.Span{51, 40, 32, 63}, dvid.Span{51, 41, 32, 63}, dvid.Span{51, 42, 32, 63}, dvid.Span{51, 43, 32, 63}, dvid.Span{51, 44, 32, 63},
				dvid.Span{51, 45, 32, 63}, dvid.Span{51, 46, 32, 63}, dvid.Span{51, 47, 32, 63}, dvid.Span{51, 48, 32, 63}, dvid.Span{51, 49, 32, 63},
				dvid.Span{51, 50, 32, 63}, dvid.Span{51, 51, 32, 63}, dvid.Span{51, 52, 32, 63}, dvid.Span{51, 53, 32, 63}, dvid.Span{51, 54, 32, 63},
				dvid.Span{51, 55, 32, 63}, dvid.Span{51, 56, 32, 63}, dvid.Span{51, 57, 32, 63}, dvid.Span{51, 58, 32, 63}, dvid.Span{51, 59, 32, 63},
				dvid.Span{51, 60, 32, 63}, dvid.Span{51, 61, 32, 63}, dvid.Span{51, 62, 32, 63}, dvid.Span{51, 63, 32, 63}, dvid.Span{52, 32, 32, 63},
				dvid.Span{52, 33, 32, 63}, dvid.Span{52, 34, 32, 63}, dvid.Span{52, 35, 32, 63}, dvid.Span{52, 36, 32, 63}, dvid.Span{52, 37, 32, 63},
				dvid.Span{52, 38, 32, 63}, dvid.Span{52, 39, 32, 63}, dvid.Span{52, 40, 32, 63}, dvid.Span{52, 41, 32, 63}, dvid.Span{52, 42, 32, 63},
				dvid.Span{52, 43, 32, 63}, dvid.Span{52, 44, 32, 63}, dvid.Span{52, 45, 32, 63}, dvid.Span{52, 46, 32, 63}, dvid.Span{52, 47, 32, 63},
				dvid.Span{52, 48, 32, 63}, dvid.Span{52, 49, 32, 63}, dvid.Span{52, 50, 32, 63}, dvid.Span{52, 51, 32, 63}, dvid.Span{52, 52, 32, 63},
				dvid.Span{52, 53, 32, 63}, dvid.Span{52, 54, 32, 63}, dvid.Span{52, 55, 32, 63}, dvid.Span{52, 56, 32, 63}, dvid.Span{52, 57, 32, 63},
				dvid.Span{52, 58, 32, 63}, dvid.Span{52, 59, 32, 63}, dvid.Span{52, 60, 32, 63}, dvid.Span{52, 61, 32, 63}, dvid.Span{52, 62, 32, 63},
				dvid.Span{52, 63, 32, 63}, dvid.Span{53, 32, 32, 63}, dvid.Span{53, 33, 32, 63}, dvid.Span{53, 34, 32, 63}, dvid.Span{53, 35, 32, 63},
				dvid.Span{53, 36, 32, 63}, dvid.Span{53, 37, 32, 63}, dvid.Span{53, 38, 32, 63}, dvid.Span{53, 39, 32, 63}, dvid.Span{53, 40, 32, 63},
				dvid.Span{53, 41, 32, 63}, dvid.Span{53, 42, 32, 63}, dvid.Span{53, 43, 32, 63}, dvid.Span{53, 44, 32, 63}, dvid.Span{53, 45, 32, 63},
				dvid.Span{53, 46, 32, 63}, dvid.Span{53, 47, 32, 63}, dvid.Span{53, 48, 32, 63}, dvid.Span{53, 49, 32, 63}, dvid.Span{53, 50, 32, 63},
				dvid.Span{53, 51, 32, 63}, dvid.Span{53, 52, 32, 63}, dvid.Span{53, 53, 32, 63}, dvid.Span{53, 54, 32, 63}, dvid.Span{53, 55, 32, 63},
				dvid.Span{53, 56, 32, 63}, dvid.Span{53, 57, 32, 63}, dvid.Span{53, 58, 32, 63}, dvid.Span{53, 59, 32, 63}, dvid.Span{53, 60, 32, 63},
				dvid.Span{53, 61, 32, 63}, dvid.Span{53, 62, 32, 63}, dvid.Span{53, 63, 32, 63}, dvid.Span{54, 32, 32, 63}, dvid.Span{54, 33, 32, 63},
				dvid.Span{54, 34, 32, 63}, dvid.Span{54, 35, 32, 63}, dvid.Span{54, 36, 32, 63}, dvid.Span{54, 37, 32, 63}, dvid.Span{54, 38, 32, 63},
				dvid.Span{54, 39, 32, 63}, dvid.Span{54, 40, 32, 63}, dvid.Span{54, 41, 32, 63}, dvid.Span{54, 42, 32, 63}, dvid.Span{54, 43, 32, 63},
				dvid.Span{54, 44, 32, 63}, dvid.Span{54, 45, 32, 63}, dvid.Span{54, 46, 32, 63}, dvid.Span{54, 47, 32, 63}, dvid.Span{54, 48, 32, 63},
				dvid.Span{54, 49, 32, 63}, dvid.Span{54, 50, 32, 63}, dvid.Span{54, 51, 32, 63}, dvid.Span{54, 52, 32, 63}, dvid.Span{54, 53, 32, 63},
				dvid.Span{54, 54, 32, 63}, dvid.Span{54, 55, 32, 63}, dvid.Span{54, 56, 32, 63}, dvid.Span{54, 57, 32, 63}, dvid.Span{54, 58, 32, 63},
				dvid.Span{54, 59, 32, 63}, dvid.Span{54, 60, 32, 63}, dvid.Span{54, 61, 32, 63}, dvid.Span{54, 62, 32, 63}, dvid.Span{54, 63, 32, 63},
				dvid.Span{55, 32, 32, 63}, dvid.Span{55, 33, 32, 63}, dvid.Span{55, 34, 32, 63}, dvid.Span{55, 35, 32, 63}, dvid.Span{55, 36, 32, 63},
				dvid.Span{55, 37, 32, 63}, dvid.Span{55, 38, 32, 63}, dvid.Span{55, 39, 32, 63}, dvid.Span{55, 40, 32, 63}, dvid.Span{55, 41, 32, 63},
				dvid.Span{55, 42, 32, 63}, dvid.Span{55, 43, 32, 63}, dvid.Span{55, 44, 32, 63}, dvid.Span{55, 45, 32, 63}, dvid.Span{55, 46, 32, 63},
				dvid.Span{55, 47, 32, 63}, dvid.Span{55, 48, 32, 63}, dvid.Span{55, 49, 32, 63}, dvid.Span{55, 50, 32, 63}, dvid.Span{55, 51, 32, 63},
				dvid.Span{55, 52, 32, 63}, dvid.Span{55, 53, 32, 63}, dvid.Span{55, 54, 32, 63}, dvid.Span{55, 55, 32, 63}, dvid.Span{55, 56, 32, 63},
				dvid.Span{55, 57, 32, 63}, dvid.Span{55, 58, 32, 63}, dvid.Span{55, 59, 32, 63}, dvid.Span{55, 60, 32, 63}, dvid.Span{55, 61, 32, 63},
				dvid.Span{55, 62, 32, 63}, dvid.Span{55, 63, 32, 63}, dvid.Span{56, 32, 32, 63}, dvid.Span{56, 33, 32, 63}, dvid.Span{56, 34, 32, 63},
				dvid.Span{56, 35, 32, 63}, dvid.Span{56, 36, 32, 63}, dvid.Span{56, 37, 32, 63}, dvid.Span{56, 38, 32, 63}, dvid.Span{56, 39, 32, 63},
				dvid.Span{56, 40, 32, 63}, dvid.Span{56, 41, 32, 63}, dvid.Span{56, 42, 32, 63}, dvid.Span{56, 43, 32, 63}, dvid.Span{56, 44, 32, 63},
				dvid.Span{56, 45, 32, 63}, dvid.Span{56, 46, 32, 63}, dvid.Span{56, 47, 32, 63}, dvid.Span{56, 48, 32, 63}, dvid.Span{56, 49, 32, 63},
				dvid.Span{56, 50, 32, 63}, dvid.Span{56, 51, 32, 63}, dvid.Span{56, 52, 32, 63}, dvid.Span{56, 53, 32, 63}, dvid.Span{56, 54, 32, 63},
				dvid.Span{56, 55, 32, 63}, dvid.Span{56, 56, 32, 63}, dvid.Span{56, 57, 32, 63}, dvid.Span{56, 58, 32, 63}, dvid.Span{56, 59, 32, 63},
				dvid.Span{56, 60, 32, 63}, dvid.Span{56, 61, 32, 63}, dvid.Span{56, 62, 32, 63}, dvid.Span{56, 63, 32, 63}, dvid.Span{57, 32, 32, 63},
				dvid.Span{57, 33, 32, 63}, dvid.Span{57, 34, 32, 63}, dvid.Span{57, 35, 32, 63}, dvid.Span{57, 36, 32, 63}, dvid.Span{57, 37, 32, 63},
				dvid.Span{57, 38, 32, 63}, dvid.Span{57, 39, 32, 63}, dvid.Span{57, 40, 32, 63}, dvid.Span{57, 41, 32, 63}, dvid.Span{57, 42, 32, 63},
				dvid.Span{57, 43, 32, 63}, dvid.Span{57, 44, 32, 63}, dvid.Span{57, 45, 32, 63}, dvid.Span{57, 46, 32, 63}, dvid.Span{57, 47, 32, 63},
				dvid.Span{57, 48, 32, 63}, dvid.Span{57, 49, 32, 63}, dvid.Span{57, 50, 32, 63}, dvid.Span{57, 51, 32, 63}, dvid.Span{57, 52, 32, 63},
				dvid.Span{57, 53, 32, 63}, dvid.Span{57, 54, 32, 63}, dvid.Span{57, 55, 32, 63}, dvid.Span{57, 56, 32, 63}, dvid.Span{57, 57, 32, 63},
				dvid.Span{57, 58, 32, 63}, dvid.Span{57, 59, 32, 63}, dvid.Span{57, 60, 32, 63}, dvid.Span{57, 61, 32, 63}, dvid.Span{57, 62, 32, 63},
				dvid.Span{57, 63, 32, 63}, dvid.Span{58, 32, 32, 63}, dvid.Span{58, 33, 32, 63}, dvid.Span{58, 34, 32, 63}, dvid.Span{58, 35, 32, 63},
				dvid.Span{58, 36, 32, 63}, dvid.Span{58, 37, 32, 63}, dvid.Span{58, 38, 32, 63}, dvid.Span{58, 39, 32, 63}, dvid.Span{58, 40, 32, 63},
				dvid.Span{58, 41, 32, 63}, dvid.Span{58, 42, 32, 63}, dvid.Span{58, 43, 32, 63}, dvid.Span{58, 44, 32, 63}, dvid.Span{58, 45, 32, 63},
				dvid.Span{58, 46, 32, 63}, dvid.Span{58, 47, 32, 63}, dvid.Span{58, 48, 32, 63}, dvid.Span{58, 49, 32, 63}, dvid.Span{58, 50, 32, 63},
				dvid.Span{58, 51, 32, 63}, dvid.Span{58, 52, 32, 63}, dvid.Span{58, 53, 32, 63}, dvid.Span{58, 54, 32, 63}, dvid.Span{58, 55, 32, 63},
				dvid.Span{58, 56, 32, 63}, dvid.Span{58, 57, 32, 63}, dvid.Span{58, 58, 32, 63}, dvid.Span{58, 59, 32, 63}, dvid.Span{58, 60, 32, 63},
				dvid.Span{58, 61, 32, 63}, dvid.Span{58, 62, 32, 63}, dvid.Span{58, 63, 32, 63}, dvid.Span{59, 32, 32, 63}, dvid.Span{59, 33, 32, 63},
				dvid.Span{59, 34, 32, 63}, dvid.Span{59, 35, 32, 63}, dvid.Span{59, 36, 32, 63}, dvid.Span{59, 37, 32, 63}, dvid.Span{59, 38, 32, 63},
				dvid.Span{59, 39, 32, 63}, dvid.Span{59, 40, 32, 63}, dvid.Span{59, 41, 32, 63}, dvid.Span{59, 42, 32, 63}, dvid.Span{59, 43, 32, 63},
				dvid.Span{59, 44, 32, 63}, dvid.Span{59, 45, 32, 63}, dvid.Span{59, 46, 32, 63}, dvid.Span{59, 47, 32, 63}, dvid.Span{59, 48, 32, 63},
				dvid.Span{59, 49, 32, 63}, dvid.Span{59, 50, 32, 63}, dvid.Span{59, 51, 32, 63}, dvid.Span{59, 52, 32, 63}, dvid.Span{59, 53, 32, 63},
				dvid.Span{59, 54, 32, 63}, dvid.Span{59, 55, 32, 63}, dvid.Span{59, 56, 32, 63}, dvid.Span{59, 57, 32, 63}, dvid.Span{59, 58, 32, 63},
				dvid.Span{59, 59, 32, 63}, dvid.Span{59, 60, 32, 63}, dvid.Span{59, 61, 32, 63}, dvid.Span{59, 62, 32, 63}, dvid.Span{59, 63, 32, 63},
				dvid.Span{40, 32, 64, 79}, dvid.Span{40, 33, 64, 79}, dvid.Span{40, 34, 64, 79}, dvid.Span{40, 35, 64, 79}, dvid.Span{40, 36, 64, 79},
				dvid.Span{40, 37, 64, 79}, dvid.Span{40, 38, 64, 79}, dvid.Span{40, 39, 64, 79}, dvid.Span{40, 40, 64, 79}, dvid.Span{40, 41, 64, 79},
				dvid.Span{40, 42, 64, 79}, dvid.Span{40, 43, 64, 79}, dvid.Span{40, 44, 64, 79}, dvid.Span{40, 45, 64, 79}, dvid.Span{40, 46, 64, 79},
				dvid.Span{40, 47, 64, 79}, dvid.Span{40, 48, 64, 79}, dvid.Span{40, 49, 64, 79}, dvid.Span{40, 50, 64, 79}, dvid.Span{40, 51, 64, 79},
				dvid.Span{40, 52, 64, 79}, dvid.Span{40, 53, 64, 79}, dvid.Span{40, 54, 64, 79}, dvid.Span{40, 55, 64, 79}, dvid.Span{40, 56, 64, 79},
				dvid.Span{40, 57, 64, 79}, dvid.Span{40, 58, 64, 79}, dvid.Span{40, 59, 64, 79}, dvid.Span{40, 60, 64, 79}, dvid.Span{40, 61, 64, 79},
				dvid.Span{40, 62, 64, 79}, dvid.Span{40, 63, 64, 79}, dvid.Span{41, 32, 64, 79}, dvid.Span{41, 33, 64, 79}, dvid.Span{41, 34, 64, 79},
				dvid.Span{41, 35, 64, 79}, dvid.Span{41, 36, 64, 79}, dvid.Span{41, 37, 64, 79}, dvid.Span{41, 38, 64, 79}, dvid.Span{41, 39, 64, 79},
				dvid.Span{41, 40, 64, 79}, dvid.Span{41, 41, 64, 79}, dvid.Span{41, 42, 64, 79}, dvid.Span{41, 43, 64, 79}, dvid.Span{41, 44, 64, 79},
				dvid.Span{41, 45, 64, 79}, dvid.Span{41, 46, 64, 79}, dvid.Span{41, 47, 64, 79}, dvid.Span{41, 48, 64, 79}, dvid.Span{41, 49, 64, 79},
				dvid.Span{41, 50, 64, 79}, dvid.Span{41, 51, 64, 79}, dvid.Span{41, 52, 64, 79}, dvid.Span{41, 53, 64, 79}, dvid.Span{41, 54, 64, 79},
				dvid.Span{41, 55, 64, 79}, dvid.Span{41, 56, 64, 79}, dvid.Span{41, 57, 64, 79}, dvid.Span{41, 58, 64, 79}, dvid.Span{41, 59, 64, 79},
				dvid.Span{41, 60, 64, 79}, dvid.Span{41, 61, 64, 79}, dvid.Span{41, 62, 64, 79}, dvid.Span{41, 63, 64, 79}, dvid.Span{42, 32, 64, 79},
				dvid.Span{42, 33, 64, 79}, dvid.Span{42, 34, 64, 79}, dvid.Span{42, 35, 64, 79}, dvid.Span{42, 36, 64, 79}, dvid.Span{42, 37, 64, 79},
				dvid.Span{42, 38, 64, 79}, dvid.Span{42, 39, 64, 79}, dvid.Span{42, 40, 64, 79}, dvid.Span{42, 41, 64, 79}, dvid.Span{42, 42, 64, 79},
				dvid.Span{42, 43, 64, 79}, dvid.Span{42, 44, 64, 79}, dvid.Span{42, 45, 64, 79}, dvid.Span{42, 46, 64, 79}, dvid.Span{42, 47, 64, 79},
				dvid.Span{42, 48, 64, 79}, dvid.Span{42, 49, 64, 79}, dvid.Span{42, 50, 64, 79}, dvid.Span{42, 51, 64, 79}, dvid.Span{42, 52, 64, 79},
				dvid.Span{42, 53, 64, 79}, dvid.Span{42, 54, 64, 79}, dvid.Span{42, 55, 64, 79}, dvid.Span{42, 56, 64, 79}, dvid.Span{42, 57, 64, 79},
				dvid.Span{42, 58, 64, 79}, dvid.Span{42, 59, 64, 79}, dvid.Span{42, 60, 64, 79}, dvid.Span{42, 61, 64, 79}, dvid.Span{42, 62, 64, 79},
				dvid.Span{42, 63, 64, 79}, dvid.Span{43, 32, 64, 79}, dvid.Span{43, 33, 64, 79}, dvid.Span{43, 34, 64, 79}, dvid.Span{43, 35, 64, 79},
				dvid.Span{43, 36, 64, 79}, dvid.Span{43, 37, 64, 79}, dvid.Span{43, 38, 64, 79}, dvid.Span{43, 39, 64, 79}, dvid.Span{43, 40, 64, 79},
				dvid.Span{43, 41, 64, 79}, dvid.Span{43, 42, 64, 79}, dvid.Span{43, 43, 64, 79}, dvid.Span{43, 44, 64, 79}, dvid.Span{43, 45, 64, 79},
				dvid.Span{43, 46, 64, 79}, dvid.Span{43, 47, 64, 79}, dvid.Span{43, 48, 64, 79}, dvid.Span{43, 49, 64, 79}, dvid.Span{43, 50, 64, 79},
				dvid.Span{43, 51, 64, 79}, dvid.Span{43, 52, 64, 79}, dvid.Span{43, 53, 64, 79}, dvid.Span{43, 54, 64, 79}, dvid.Span{43, 55, 64, 79},
				dvid.Span{43, 56, 64, 79}, dvid.Span{43, 57, 64, 79}, dvid.Span{43, 58, 64, 79}, dvid.Span{43, 59, 64, 79}, dvid.Span{43, 60, 64, 79},
				dvid.Span{43, 61, 64, 79}, dvid.Span{43, 62, 64, 79}, dvid.Span{43, 63, 64, 79}, dvid.Span{44, 32, 64, 79}, dvid.Span{44, 33, 64, 79},
				dvid.Span{44, 34, 64, 79}, dvid.Span{44, 35, 64, 79}, dvid.Span{44, 36, 64, 79}, dvid.Span{44, 37, 64, 79}, dvid.Span{44, 38, 64, 79},
				dvid.Span{44, 39, 64, 79}, dvid.Span{44, 40, 64, 79}, dvid.Span{44, 41, 64, 79}, dvid.Span{44, 42, 64, 79}, dvid.Span{44, 43, 64, 79},
				dvid.Span{44, 44, 64, 79}, dvid.Span{44, 45, 64, 79}, dvid.Span{44, 46, 64, 79}, dvid.Span{44, 47, 64, 79}, dvid.Span{44, 48, 64, 79},
				dvid.Span{44, 49, 64, 79}, dvid.Span{44, 50, 64, 79}, dvid.Span{44, 51, 64, 79}, dvid.Span{44, 52, 64, 79}, dvid.Span{44, 53, 64, 79},
				dvid.Span{44, 54, 64, 79}, dvid.Span{44, 55, 64, 79}, dvid.Span{44, 56, 64, 79}, dvid.Span{44, 57, 64, 79}, dvid.Span{44, 58, 64, 79},
				dvid.Span{44, 59, 64, 79}, dvid.Span{44, 60, 64, 79}, dvid.Span{44, 61, 64, 79}, dvid.Span{44, 62, 64, 79}, dvid.Span{44, 63, 64, 79},
				dvid.Span{45, 32, 64, 79}, dvid.Span{45, 33, 64, 79}, dvid.Span{45, 34, 64, 79}, dvid.Span{45, 35, 64, 79}, dvid.Span{45, 36, 64, 79},
				dvid.Span{45, 37, 64, 79}, dvid.Span{45, 38, 64, 79}, dvid.Span{45, 39, 64, 79}, dvid.Span{45, 40, 64, 79}, dvid.Span{45, 41, 64, 79},
				dvid.Span{45, 42, 64, 79}, dvid.Span{45, 43, 64, 79}, dvid.Span{45, 44, 64, 79}, dvid.Span{45, 45, 64, 79}, dvid.Span{45, 46, 64, 79},
				dvid.Span{45, 47, 64, 79}, dvid.Span{45, 48, 64, 79}, dvid.Span{45, 49, 64, 79}, dvid.Span{45, 50, 64, 79}, dvid.Span{45, 51, 64, 79},
				dvid.Span{45, 52, 64, 79}, dvid.Span{45, 53, 64, 79}, dvid.Span{45, 54, 64, 79}, dvid.Span{45, 55, 64, 79}, dvid.Span{45, 56, 64, 79},
				dvid.Span{45, 57, 64, 79}, dvid.Span{45, 58, 64, 79}, dvid.Span{45, 59, 64, 79}, dvid.Span{45, 60, 64, 79}, dvid.Span{45, 61, 64, 79},
				dvid.Span{45, 62, 64, 79}, dvid.Span{45, 63, 64, 79}, dvid.Span{46, 32, 64, 79}, dvid.Span{46, 33, 64, 79}, dvid.Span{46, 34, 64, 79},
				dvid.Span{46, 35, 64, 79}, dvid.Span{46, 36, 64, 79}, dvid.Span{46, 37, 64, 79}, dvid.Span{46, 38, 64, 79}, dvid.Span{46, 39, 64, 79},
				dvid.Span{46, 40, 64, 79}, dvid.Span{46, 41, 64, 79}, dvid.Span{46, 42, 64, 79}, dvid.Span{46, 43, 64, 79}, dvid.Span{46, 44, 64, 79},
				dvid.Span{46, 45, 64, 79}, dvid.Span{46, 46, 64, 79}, dvid.Span{46, 47, 64, 79}, dvid.Span{46, 48, 64, 79}, dvid.Span{46, 49, 64, 79},
				dvid.Span{46, 50, 64, 79}, dvid.Span{46, 51, 64, 79}, dvid.Span{46, 52, 64, 79}, dvid.Span{46, 53, 64, 79}, dvid.Span{46, 54, 64, 79},
				dvid.Span{46, 55, 64, 79}, dvid.Span{46, 56, 64, 79}, dvid.Span{46, 57, 64, 79}, dvid.Span{46, 58, 64, 79}, dvid.Span{46, 59, 64, 79},
				dvid.Span{46, 60, 64, 79}, dvid.Span{46, 61, 64, 79}, dvid.Span{46, 62, 64, 79}, dvid.Span{46, 63, 64, 79}, dvid.Span{47, 32, 64, 79},
				dvid.Span{47, 33, 64, 79}, dvid.Span{47, 34, 64, 79}, dvid.Span{47, 35, 64, 79}, dvid.Span{47, 36, 64, 79}, dvid.Span{47, 37, 64, 79},
				dvid.Span{47, 38, 64, 79}, dvid.Span{47, 39, 64, 79}, dvid.Span{47, 40, 64, 79}, dvid.Span{47, 41, 64, 79}, dvid.Span{47, 42, 64, 79},
				dvid.Span{47, 43, 64, 79}, dvid.Span{47, 44, 64, 79}, dvid.Span{47, 45, 64, 79}, dvid.Span{47, 46, 64, 79}, dvid.Span{47, 47, 64, 79},
				dvid.Span{47, 48, 64, 79}, dvid.Span{47, 49, 64, 79}, dvid.Span{47, 50, 64, 79}, dvid.Span{47, 51, 64, 79}, dvid.Span{47, 52, 64, 79},
				dvid.Span{47, 53, 64, 79}, dvid.Span{47, 54, 64, 79}, dvid.Span{47, 55, 64, 79}, dvid.Span{47, 56, 64, 79}, dvid.Span{47, 57, 64, 79},
				dvid.Span{47, 58, 64, 79}, dvid.Span{47, 59, 64, 79}, dvid.Span{47, 60, 64, 79}, dvid.Span{47, 61, 64, 79}, dvid.Span{47, 62, 64, 79},
				dvid.Span{47, 63, 64, 79}, dvid.Span{48, 32, 64, 79}, dvid.Span{48, 33, 64, 79}, dvid.Span{48, 34, 64, 79}, dvid.Span{48, 35, 64, 79},
				dvid.Span{48, 36, 64, 79}, dvid.Span{48, 37, 64, 79}, dvid.Span{48, 38, 64, 79}, dvid.Span{48, 39, 64, 79}, dvid.Span{48, 40, 64, 79},
				dvid.Span{48, 41, 64, 79}, dvid.Span{48, 42, 64, 79}, dvid.Span{48, 43, 64, 79}, dvid.Span{48, 44, 64, 79}, dvid.Span{48, 45, 64, 79},
				dvid.Span{48, 46, 64, 79}, dvid.Span{48, 47, 64, 79}, dvid.Span{48, 48, 64, 79}, dvid.Span{48, 49, 64, 79}, dvid.Span{48, 50, 64, 79},
				dvid.Span{48, 51, 64, 79}, dvid.Span{48, 52, 64, 79}, dvid.Span{48, 53, 64, 79}, dvid.Span{48, 54, 64, 79}, dvid.Span{48, 55, 64, 79},
				dvid.Span{48, 56, 64, 79}, dvid.Span{48, 57, 64, 79}, dvid.Span{48, 58, 64, 79}, dvid.Span{48, 59, 64, 79}, dvid.Span{48, 60, 64, 79},
				dvid.Span{48, 61, 64, 79}, dvid.Span{48, 62, 64, 79}, dvid.Span{48, 63, 64, 79}, dvid.Span{49, 32, 64, 79}, dvid.Span{49, 33, 64, 79},
				dvid.Span{49, 34, 64, 79}, dvid.Span{49, 35, 64, 79}, dvid.Span{49, 36, 64, 79}, dvid.Span{49, 37, 64, 79}, dvid.Span{49, 38, 64, 79},
				dvid.Span{49, 39, 64, 79}, dvid.Span{49, 40, 64, 79}, dvid.Span{49, 41, 64, 79}, dvid.Span{49, 42, 64, 79}, dvid.Span{49, 43, 64, 79},
				dvid.Span{49, 44, 64, 79}, dvid.Span{49, 45, 64, 79}, dvid.Span{49, 46, 64, 79}, dvid.Span{49, 47, 64, 79}, dvid.Span{49, 48, 64, 79},
				dvid.Span{49, 49, 64, 79}, dvid.Span{49, 50, 64, 79}, dvid.Span{49, 51, 64, 79}, dvid.Span{49, 52, 64, 79}, dvid.Span{49, 53, 64, 79},
				dvid.Span{49, 54, 64, 79}, dvid.Span{49, 55, 64, 79}, dvid.Span{49, 56, 64, 79}, dvid.Span{49, 57, 64, 79}, dvid.Span{49, 58, 64, 79},
				dvid.Span{49, 59, 64, 79}, dvid.Span{49, 60, 64, 79}, dvid.Span{49, 61, 64, 79}, dvid.Span{49, 62, 64, 79}, dvid.Span{49, 63, 64, 79},
				dvid.Span{50, 32, 64, 79}, dvid.Span{50, 33, 64, 79}, dvid.Span{50, 34, 64, 79}, dvid.Span{50, 35, 64, 79}, dvid.Span{50, 36, 64, 79},
				dvid.Span{50, 37, 64, 79}, dvid.Span{50, 38, 64, 79}, dvid.Span{50, 39, 64, 79}, dvid.Span{50, 40, 64, 79}, dvid.Span{50, 41, 64, 79},
				dvid.Span{50, 42, 64, 79}, dvid.Span{50, 43, 64, 79}, dvid.Span{50, 44, 64, 79}, dvid.Span{50, 45, 64, 79}, dvid.Span{50, 46, 64, 79},
				dvid.Span{50, 47, 64, 79}, dvid.Span{50, 48, 64, 79}, dvid.Span{50, 49, 64, 79}, dvid.Span{50, 50, 64, 79}, dvid.Span{50, 51, 64, 79},
				dvid.Span{50, 52, 64, 79}, dvid.Span{50, 53, 64, 79}, dvid.Span{50, 54, 64, 79}, dvid.Span{50, 55, 64, 79}, dvid.Span{50, 56, 64, 79},
				dvid.Span{50, 57, 64, 79}, dvid.Span{50, 58, 64, 79}, dvid.Span{50, 59, 64, 79}, dvid.Span{50, 60, 64, 79}, dvid.Span{50, 61, 64, 79},
				dvid.Span{50, 62, 64, 79}, dvid.Span{50, 63, 64, 79}, dvid.Span{51, 32, 64, 79}, dvid.Span{51, 33, 64, 79}, dvid.Span{51, 34, 64, 79},
				dvid.Span{51, 35, 64, 79}, dvid.Span{51, 36, 64, 79}, dvid.Span{51, 37, 64, 79}, dvid.Span{51, 38, 64, 79}, dvid.Span{51, 39, 64, 79},
				dvid.Span{51, 40, 64, 79}, dvid.Span{51, 41, 64, 79}, dvid.Span{51, 42, 64, 79}, dvid.Span{51, 43, 64, 79}, dvid.Span{51, 44, 64, 79},
				dvid.Span{51, 45, 64, 79}, dvid.Span{51, 46, 64, 79}, dvid.Span{51, 47, 64, 79}, dvid.Span{51, 48, 64, 79}, dvid.Span{51, 49, 64, 79},
				dvid.Span{51, 50, 64, 79}, dvid.Span{51, 51, 64, 79}, dvid.Span{51, 52, 64, 79}, dvid.Span{51, 53, 64, 79}, dvid.Span{51, 54, 64, 79},
				dvid.Span{51, 55, 64, 79}, dvid.Span{51, 56, 64, 79}, dvid.Span{51, 57, 64, 79}, dvid.Span{51, 58, 64, 79}, dvid.Span{51, 59, 64, 79},
				dvid.Span{51, 60, 64, 79}, dvid.Span{51, 61, 64, 79}, dvid.Span{51, 62, 64, 79}, dvid.Span{51, 63, 64, 79}, dvid.Span{52, 32, 64, 79},
				dvid.Span{52, 33, 64, 79}, dvid.Span{52, 34, 64, 79}, dvid.Span{52, 35, 64, 79}, dvid.Span{52, 36, 64, 79}, dvid.Span{52, 37, 64, 79},
				dvid.Span{52, 38, 64, 79}, dvid.Span{52, 39, 64, 79}, dvid.Span{52, 40, 64, 79}, dvid.Span{52, 41, 64, 79}, dvid.Span{52, 42, 64, 79},
				dvid.Span{52, 43, 64, 79}, dvid.Span{52, 44, 64, 79}, dvid.Span{52, 45, 64, 79}, dvid.Span{52, 46, 64, 79}, dvid.Span{52, 47, 64, 79},
				dvid.Span{52, 48, 64, 79}, dvid.Span{52, 49, 64, 79}, dvid.Span{52, 50, 64, 79}, dvid.Span{52, 51, 64, 79}, dvid.Span{52, 52, 64, 79},
				dvid.Span{52, 53, 64, 79}, dvid.Span{52, 54, 64, 79}, dvid.Span{52, 55, 64, 79}, dvid.Span{52, 56, 64, 79}, dvid.Span{52, 57, 64, 79},
				dvid.Span{52, 58, 64, 79}, dvid.Span{52, 59, 64, 79}, dvid.Span{52, 60, 64, 79}, dvid.Span{52, 61, 64, 79}, dvid.Span{52, 62, 64, 79},
				dvid.Span{52, 63, 64, 79}, dvid.Span{53, 32, 64, 79}, dvid.Span{53, 33, 64, 79}, dvid.Span{53, 34, 64, 79}, dvid.Span{53, 35, 64, 79},
				dvid.Span{53, 36, 64, 79}, dvid.Span{53, 37, 64, 79}, dvid.Span{53, 38, 64, 79}, dvid.Span{53, 39, 64, 79}, dvid.Span{53, 40, 64, 79},
				dvid.Span{53, 41, 64, 79}, dvid.Span{53, 42, 64, 79}, dvid.Span{53, 43, 64, 79}, dvid.Span{53, 44, 64, 79}, dvid.Span{53, 45, 64, 79},
				dvid.Span{53, 46, 64, 79}, dvid.Span{53, 47, 64, 79}, dvid.Span{53, 48, 64, 79}, dvid.Span{53, 49, 64, 79}, dvid.Span{53, 50, 64, 79},
				dvid.Span{53, 51, 64, 79}, dvid.Span{53, 52, 64, 79}, dvid.Span{53, 53, 64, 79}, dvid.Span{53, 54, 64, 79}, dvid.Span{53, 55, 64, 79},
				dvid.Span{53, 56, 64, 79}, dvid.Span{53, 57, 64, 79}, dvid.Span{53, 58, 64, 79}, dvid.Span{53, 59, 64, 79}, dvid.Span{53, 60, 64, 79},
				dvid.Span{53, 61, 64, 79}, dvid.Span{53, 62, 64, 79}, dvid.Span{53, 63, 64, 79}, dvid.Span{54, 32, 64, 79}, dvid.Span{54, 33, 64, 79},
				dvid.Span{54, 34, 64, 79}, dvid.Span{54, 35, 64, 79}, dvid.Span{54, 36, 64, 79}, dvid.Span{54, 37, 64, 79}, dvid.Span{54, 38, 64, 79},
				dvid.Span{54, 39, 64, 79}, dvid.Span{54, 40, 64, 79}, dvid.Span{54, 41, 64, 79}, dvid.Span{54, 42, 64, 79}, dvid.Span{54, 43, 64, 79},
				dvid.Span{54, 44, 64, 79}, dvid.Span{54, 45, 64, 79}, dvid.Span{54, 46, 64, 79}, dvid.Span{54, 47, 64, 79}, dvid.Span{54, 48, 64, 79},
				dvid.Span{54, 49, 64, 79}, dvid.Span{54, 50, 64, 79}, dvid.Span{54, 51, 64, 79}, dvid.Span{54, 52, 64, 79}, dvid.Span{54, 53, 64, 79},
				dvid.Span{54, 54, 64, 79}, dvid.Span{54, 55, 64, 79}, dvid.Span{54, 56, 64, 79}, dvid.Span{54, 57, 64, 79}, dvid.Span{54, 58, 64, 79},
				dvid.Span{54, 59, 64, 79}, dvid.Span{54, 60, 64, 79}, dvid.Span{54, 61, 64, 79}, dvid.Span{54, 62, 64, 79}, dvid.Span{54, 63, 64, 79},
				dvid.Span{55, 32, 64, 79}, dvid.Span{55, 33, 64, 79}, dvid.Span{55, 34, 64, 79}, dvid.Span{55, 35, 64, 79}, dvid.Span{55, 36, 64, 79},
				dvid.Span{55, 37, 64, 79}, dvid.Span{55, 38, 64, 79}, dvid.Span{55, 39, 64, 79}, dvid.Span{55, 40, 64, 79}, dvid.Span{55, 41, 64, 79},
				dvid.Span{55, 42, 64, 79}, dvid.Span{55, 43, 64, 79}, dvid.Span{55, 44, 64, 79}, dvid.Span{55, 45, 64, 79}, dvid.Span{55, 46, 64, 79},
				dvid.Span{55, 47, 64, 79}, dvid.Span{55, 48, 64, 79}, dvid.Span{55, 49, 64, 79}, dvid.Span{55, 50, 64, 79}, dvid.Span{55, 51, 64, 79},
				dvid.Span{55, 52, 64, 79}, dvid.Span{55, 53, 64, 79}, dvid.Span{55, 54, 64, 79}, dvid.Span{55, 55, 64, 79}, dvid.Span{55, 56, 64, 79},
				dvid.Span{55, 57, 64, 79}, dvid.Span{55, 58, 64, 79}, dvid.Span{55, 59, 64, 79}, dvid.Span{55, 60, 64, 79}, dvid.Span{55, 61, 64, 79},
				dvid.Span{55, 62, 64, 79}, dvid.Span{55, 63, 64, 79}, dvid.Span{56, 32, 64, 79}, dvid.Span{56, 33, 64, 79}, dvid.Span{56, 34, 64, 79},
				dvid.Span{56, 35, 64, 79}, dvid.Span{56, 36, 64, 79}, dvid.Span{56, 37, 64, 79}, dvid.Span{56, 38, 64, 79}, dvid.Span{56, 39, 64, 79},
				dvid.Span{56, 40, 64, 79}, dvid.Span{56, 41, 64, 79}, dvid.Span{56, 42, 64, 79}, dvid.Span{56, 43, 64, 79}, dvid.Span{56, 44, 64, 79},
				dvid.Span{56, 45, 64, 79}, dvid.Span{56, 46, 64, 79}, dvid.Span{56, 47, 64, 79}, dvid.Span{56, 48, 64, 79}, dvid.Span{56, 49, 64, 79},
				dvid.Span{56, 50, 64, 79}, dvid.Span{56, 51, 64, 79}, dvid.Span{56, 52, 64, 79}, dvid.Span{56, 53, 64, 79}, dvid.Span{56, 54, 64, 79},
				dvid.Span{56, 55, 64, 79}, dvid.Span{56, 56, 64, 79}, dvid.Span{56, 57, 64, 79}, dvid.Span{56, 58, 64, 79}, dvid.Span{56, 59, 64, 79},
				dvid.Span{56, 60, 64, 79}, dvid.Span{56, 61, 64, 79}, dvid.Span{56, 62, 64, 79}, dvid.Span{56, 63, 64, 79}, dvid.Span{57, 32, 64, 79},
				dvid.Span{57, 33, 64, 79}, dvid.Span{57, 34, 64, 79}, dvid.Span{57, 35, 64, 79}, dvid.Span{57, 36, 64, 79}, dvid.Span{57, 37, 64, 79},
				dvid.Span{57, 38, 64, 79}, dvid.Span{57, 39, 64, 79}, dvid.Span{57, 40, 64, 79}, dvid.Span{57, 41, 64, 79}, dvid.Span{57, 42, 64, 79},
				dvid.Span{57, 43, 64, 79}, dvid.Span{57, 44, 64, 79}, dvid.Span{57, 45, 64, 79}, dvid.Span{57, 46, 64, 79}, dvid.Span{57, 47, 64, 79},
				dvid.Span{57, 48, 64, 79}, dvid.Span{57, 49, 64, 79}, dvid.Span{57, 50, 64, 79}, dvid.Span{57, 51, 64, 79}, dvid.Span{57, 52, 64, 79},
				dvid.Span{57, 53, 64, 79}, dvid.Span{57, 54, 64, 79}, dvid.Span{57, 55, 64, 79}, dvid.Span{57, 56, 64, 79}, dvid.Span{57, 57, 64, 79},
				dvid.Span{57, 58, 64, 79}, dvid.Span{57, 59, 64, 79}, dvid.Span{57, 60, 64, 79}, dvid.Span{57, 61, 64, 79}, dvid.Span{57, 62, 64, 79},
				dvid.Span{57, 63, 64, 79}, dvid.Span{58, 32, 64, 79}, dvid.Span{58, 33, 64, 79}, dvid.Span{58, 34, 64, 79}, dvid.Span{58, 35, 64, 79},
				dvid.Span{58, 36, 64, 79}, dvid.Span{58, 37, 64, 79}, dvid.Span{58, 38, 64, 79}, dvid.Span{58, 39, 64, 79}, dvid.Span{58, 40, 64, 79},
				dvid.Span{58, 41, 64, 79}, dvid.Span{58, 42, 64, 79}, dvid.Span{58, 43, 64, 79}, dvid.Span{58, 44, 64, 79}, dvid.Span{58, 45, 64, 79},
				dvid.Span{58, 46, 64, 79}, dvid.Span{58, 47, 64, 79}, dvid.Span{58, 48, 64, 79}, dvid.Span{58, 49, 64, 79}, dvid.Span{58, 50, 64, 79},
				dvid.Span{58, 51, 64, 79}, dvid.Span{58, 52, 64, 79}, dvid.Span{58, 53, 64, 79}, dvid.Span{58, 54, 64, 79}, dvid.Span{58, 55, 64, 79},
				dvid.Span{58, 56, 64, 79}, dvid.Span{58, 57, 64, 79}, dvid.Span{58, 58, 64, 79}, dvid.Span{58, 59, 64, 79}, dvid.Span{58, 60, 64, 79},
				dvid.Span{58, 61, 64, 79}, dvid.Span{58, 62, 64, 79}, dvid.Span{58, 63, 64, 79}, dvid.Span{59, 32, 64, 79}, dvid.Span{59, 33, 64, 79},
				dvid.Span{59, 34, 64, 79}, dvid.Span{59, 35, 64, 79}, dvid.Span{59, 36, 64, 79}, dvid.Span{59, 37, 64, 79}, dvid.Span{59, 38, 64, 79},
				dvid.Span{59, 39, 64, 79}, dvid.Span{59, 40, 64, 79}, dvid.Span{59, 41, 64, 79}, dvid.Span{59, 42, 64, 79}, dvid.Span{59, 43, 64, 79},
				dvid.Span{59, 44, 64, 79}, dvid.Span{59, 45, 64, 79}, dvid.Span{59, 46, 64, 79}, dvid.Span{59, 47, 64, 79}, dvid.Span{59, 48, 64, 79},
				dvid.Span{59, 49, 64, 79}, dvid.Span{59, 50, 64, 79}, dvid.Span{59, 51, 64, 79}, dvid.Span{59, 52, 64, 79}, dvid.Span{59, 53, 64, 79},
				dvid.Span{59, 54, 64, 79}, dvid.Span{59, 55, 64, 79}, dvid.Span{59, 56, 64, 79}, dvid.Span{59, 57, 64, 79}, dvid.Span{59, 58, 64, 79},
				dvid.Span{59, 59, 64, 79}, dvid.Span{59, 60, 64, 79}, dvid.Span{59, 61, 64, 79}, dvid.Span{59, 62, 64, 79}, dvid.Span{59, 63, 64, 79},
				dvid.Span{40, 64, 30, 31}, dvid.Span{40, 65, 30, 31}, dvid.Span{40, 66, 30, 31}, dvid.Span{40, 67, 30, 31}, dvid.Span{40, 68, 30, 31},
				dvid.Span{40, 69, 30, 31}, dvid.Span{41, 64, 30, 31}, dvid.Span{41, 65, 30, 31}, dvid.Span{41, 66, 30, 31}, dvid.Span{41, 67, 30, 31},
				dvid.Span{41, 68, 30, 31}, dvid.Span{41, 69, 30, 31}, dvid.Span{42, 64, 30, 31}, dvid.Span{42, 65, 30, 31}, dvid.Span{42, 66, 30, 31},
				dvid.Span{42, 67, 30, 31}, dvid.Span{42, 68, 30, 31}, dvid.Span{42, 69, 30, 31}, dvid.Span{43, 64, 30, 31}, dvid.Span{43, 65, 30, 31},
				dvid.Span{43, 66, 30, 31}, dvid.Span{43, 67, 30, 31}, dvid.Span{43, 68, 30, 31}, dvid.Span{43, 69, 30, 31}, dvid.Span{44, 64, 30, 31},
				dvid.Span{44, 65, 30, 31}, dvid.Span{44, 66, 30, 31}, dvid.Span{44, 67, 30, 31}, dvid.Span{44, 68, 30, 31}, dvid.Span{44, 69, 30, 31},
				dvid.Span{45, 64, 30, 31}, dvid.Span{45, 65, 30, 31}, dvid.Span{45, 66, 30, 31}, dvid.Span{45, 67, 30, 31}, dvid.Span{45, 68, 30, 31},
				dvid.Span{45, 69, 30, 31}, dvid.Span{46, 64, 30, 31}, dvid.Span{46, 65, 30, 31}, dvid.Span{46, 66, 30, 31}, dvid.Span{46, 67, 30, 31},
				dvid.Span{46, 68, 30, 31}, dvid.Span{46, 69, 30, 31}, dvid.Span{47, 64, 30, 31}, dvid.Span{47, 65, 30, 31}, dvid.Span{47, 66, 30, 31},
				dvid.Span{47, 67, 30, 31}, dvid.Span{47, 68, 30, 31}, dvid.Span{47, 69, 30, 31}, dvid.Span{48, 64, 30, 31}, dvid.Span{48, 65, 30, 31},
				dvid.Span{48, 66, 30, 31}, dvid.Span{48, 67, 30, 31}, dvid.Span{48, 68, 30, 31}, dvid.Span{48, 69, 30, 31}, dvid.Span{49, 64, 30, 31},
				dvid.Span{49, 65, 30, 31}, dvid.Span{49, 66, 30, 31}, dvid.Span{49, 67, 30, 31}, dvid.Span{49, 68, 30, 31}, dvid.Span{49, 69, 30, 31},
				dvid.Span{50, 64, 30, 31}, dvid.Span{50, 65, 30, 31}, dvid.Span{50, 66, 30, 31}, dvid.Span{50, 67, 30, 31}, dvid.Span{50, 68, 30, 31},
				dvid.Span{50, 69, 30, 31}, dvid.Span{51, 64, 30, 31}, dvid.Span{51, 65, 30, 31}, dvid.Span{51, 66, 30, 31}, dvid.Span{51, 67, 30, 31},
				dvid.Span{51, 68, 30, 31}, dvid.Span{51, 69, 30, 31}, dvid.Span{52, 64, 30, 31}, dvid.Span{52, 65, 30, 31}, dvid.Span{52, 66, 30, 31},
				dvid.Span{52, 67, 30, 31}, dvid.Span{52, 68, 30, 31}, dvid.Span{52, 69, 30, 31}, dvid.Span{53, 64, 30, 31}, dvid.Span{53, 65, 30, 31},
				dvid.Span{53, 66, 30, 31}, dvid.Span{53, 67, 30, 31}, dvid.Span{53, 68, 30, 31}, dvid.Span{53, 69, 30, 31}, dvid.Span{54, 64, 30, 31},
				dvid.Span{54, 65, 30, 31}, dvid.Span{54, 66, 30, 31}, dvid.Span{54, 67, 30, 31}, dvid.Span{54, 68, 30, 31}, dvid.Span{54, 69, 30, 31},
				dvid.Span{55, 64, 30, 31}, dvid.Span{55, 65, 30, 31}, dvid.Span{55, 66, 30, 31}, dvid.Span{55, 67, 30, 31}, dvid.Span{55, 68, 30, 31},
				dvid.Span{55, 69, 30, 31}, dvid.Span{56, 64, 30, 31}, dvid.Span{56, 65, 30, 31}, dvid.Span{56, 66, 30, 31}, dvid.Span{56, 67, 30, 31},
				dvid.Span{56, 68, 30, 31}, dvid.Span{56, 69, 30, 31}, dvid.Span{57, 64, 30, 31}, dvid.Span{57, 65, 30, 31}, dvid.Span{57, 66, 30, 31},
				dvid.Span{57, 67, 30, 31}, dvid.Span{57, 68, 30, 31}, dvid.Span{57, 69, 30, 31}, dvid.Span{58, 64, 30, 31}, dvid.Span{58, 65, 30, 31},
				dvid.Span{58, 66, 30, 31}, dvid.Span{58, 67, 30, 31}, dvid.Span{58, 68, 30, 31}, dvid.Span{58, 69, 30, 31}, dvid.Span{59, 64, 30, 31},
				dvid.Span{59, 65, 30, 31}, dvid.Span{59, 66, 30, 31}, dvid.Span{59, 67, 30, 31}, dvid.Span{59, 68, 30, 31}, dvid.Span{59, 69, 30, 31},
				dvid.Span{40, 64, 32, 63}, dvid.Span{40, 65, 32, 63}, dvid.Span{40, 66, 32, 63}, dvid.Span{40, 67, 32, 63}, dvid.Span{40, 68, 32, 63},
				dvid.Span{40, 69, 32, 63}, dvid.Span{41, 64, 32, 63}, dvid.Span{41, 65, 32, 63}, dvid.Span{41, 66, 32, 63}, dvid.Span{41, 67, 32, 63},
				dvid.Span{41, 68, 32, 63}, dvid.Span{41, 69, 32, 63}, dvid.Span{42, 64, 32, 63}, dvid.Span{42, 65, 32, 63}, dvid.Span{42, 66, 32, 63},
				dvid.Span{42, 67, 32, 63}, dvid.Span{42, 68, 32, 63}, dvid.Span{42, 69, 32, 63}, dvid.Span{43, 64, 32, 63}, dvid.Span{43, 65, 32, 63},
				dvid.Span{43, 66, 32, 63}, dvid.Span{43, 67, 32, 63}, dvid.Span{43, 68, 32, 63}, dvid.Span{43, 69, 32, 63}, dvid.Span{44, 64, 32, 63},
				dvid.Span{44, 65, 32, 63}, dvid.Span{44, 66, 32, 63}, dvid.Span{44, 67, 32, 63}, dvid.Span{44, 68, 32, 63}, dvid.Span{44, 69, 32, 63},
				dvid.Span{45, 64, 32, 63}, dvid.Span{45, 65, 32, 63}, dvid.Span{45, 66, 32, 63}, dvid.Span{45, 67, 32, 63}, dvid.Span{45, 68, 32, 63},
				dvid.Span{45, 69, 32, 63}, dvid.Span{46, 64, 32, 63}, dvid.Span{46, 65, 32, 63}, dvid.Span{46, 66, 32, 63}, dvid.Span{46, 67, 32, 63},
				dvid.Span{46, 68, 32, 63}, dvid.Span{46, 69, 32, 63}, dvid.Span{47, 64, 32, 63}, dvid.Span{47, 65, 32, 63}, dvid.Span{47, 66, 32, 63},
				dvid.Span{47, 67, 32, 63}, dvid.Span{47, 68, 32, 63}, dvid.Span{47, 69, 32, 63}, dvid.Span{48, 64, 32, 63}, dvid.Span{48, 65, 32, 63},
				dvid.Span{48, 66, 32, 63}, dvid.Span{48, 67, 32, 63}, dvid.Span{48, 68, 32, 63}, dvid.Span{48, 69, 32, 63}, dvid.Span{49, 64, 32, 63},
				dvid.Span{49, 65, 32, 63}, dvid.Span{49, 66, 32, 63}, dvid.Span{49, 67, 32, 63}, dvid.Span{49, 68, 32, 63}, dvid.Span{49, 69, 32, 63},
				dvid.Span{50, 64, 32, 63}, dvid.Span{50, 65, 32, 63}, dvid.Span{50, 66, 32, 63}, dvid.Span{50, 67, 32, 63}, dvid.Span{50, 68, 32, 63},
				dvid.Span{50, 69, 32, 63}, dvid.Span{51, 64, 32, 63}, dvid.Span{51, 65, 32, 63}, dvid.Span{51, 66, 32, 63}, dvid.Span{51, 67, 32, 63},
				dvid.Span{51, 68, 32, 63}, dvid.Span{51, 69, 32, 63}, dvid.Span{52, 64, 32, 63}, dvid.Span{52, 65, 32, 63}, dvid.Span{52, 66, 32, 63},
				dvid.Span{52, 67, 32, 63}, dvid.Span{52, 68, 32, 63}, dvid.Span{52, 69, 32, 63}, dvid.Span{53, 64, 32, 63}, dvid.Span{53, 65, 32, 63},
				dvid.Span{53, 66, 32, 63}, dvid.Span{53, 67, 32, 63}, dvid.Span{53, 68, 32, 63}, dvid.Span{53, 69, 32, 63}, dvid.Span{54, 64, 32, 63},
				dvid.Span{54, 65, 32, 63}, dvid.Span{54, 66, 32, 63}, dvid.Span{54, 67, 32, 63}, dvid.Span{54, 68, 32, 63}, dvid.Span{54, 69, 32, 63},
				dvid.Span{55, 64, 32, 63}, dvid.Span{55, 65, 32, 63}, dvid.Span{55, 66, 32, 63}, dvid.Span{55, 67, 32, 63}, dvid.Span{55, 68, 32, 63},
				dvid.Span{55, 69, 32, 63}, dvid.Span{56, 64, 32, 63}, dvid.Span{56, 65, 32, 63}, dvid.Span{56, 66, 32, 63}, dvid.Span{56, 67, 32, 63},
				dvid.Span{56, 68, 32, 63}, dvid.Span{56, 69, 32, 63}, dvid.Span{57, 64, 32, 63}, dvid.Span{57, 65, 32, 63}, dvid.Span{57, 66, 32, 63},
				dvid.Span{57, 67, 32, 63}, dvid.Span{57, 68, 32, 63}, dvid.Span{57, 69, 32, 63}, dvid.Span{58, 64, 32, 63}, dvid.Span{58, 65, 32, 63},
				dvid.Span{58, 66, 32, 63}, dvid.Span{58, 67, 32, 63}, dvid.Span{58, 68, 32, 63}, dvid.Span{58, 69, 32, 63}, dvid.Span{59, 64, 32, 63},
				dvid.Span{59, 65, 32, 63}, dvid.Span{59, 66, 32, 63}, dvid.Span{59, 67, 32, 63}, dvid.Span{59, 68, 32, 63}, dvid.Span{59, 69, 32, 63},
				dvid.Span{40, 64, 64, 79}, dvid.Span{40, 65, 64, 79}, dvid.Span{40, 66, 64, 79}, dvid.Span{40, 67, 64, 79}, dvid.Span{40, 68, 64, 79},
				dvid.Span{40, 69, 64, 79}, dvid.Span{41, 64, 64, 79}, dvid.Span{41, 65, 64, 79}, dvid.Span{41, 66, 64, 79}, dvid.Span{41, 67, 64, 79},
				dvid.Span{41, 68, 64, 79}, dvid.Span{41, 69, 64, 79}, dvid.Span{42, 64, 64, 79}, dvid.Span{42, 65, 64, 79}, dvid.Span{42, 66, 64, 79},
				dvid.Span{42, 67, 64, 79}, dvid.Span{42, 68, 64, 79}, dvid.Span{42, 69, 64, 79}, dvid.Span{43, 64, 64, 79}, dvid.Span{43, 65, 64, 79},
				dvid.Span{43, 66, 64, 79}, dvid.Span{43, 67, 64, 79}, dvid.Span{43, 68, 64, 79}, dvid.Span{43, 69, 64, 79}, dvid.Span{44, 64, 64, 79},
				dvid.Span{44, 65, 64, 79}, dvid.Span{44, 66, 64, 79}, dvid.Span{44, 67, 64, 79}, dvid.Span{44, 68, 64, 79}, dvid.Span{44, 69, 64, 79},
				dvid.Span{45, 64, 64, 79}, dvid.Span{45, 65, 64, 79}, dvid.Span{45, 66, 64, 79}, dvid.Span{45, 67, 64, 79}, dvid.Span{45, 68, 64, 79},
				dvid.Span{45, 69, 64, 79}, dvid.Span{46, 64, 64, 79}, dvid.Span{46, 65, 64, 79}, dvid.Span{46, 66, 64, 79}, dvid.Span{46, 67, 64, 79},
				dvid.Span{46, 68, 64, 79}, dvid.Span{46, 69, 64, 79}, dvid.Span{47, 64, 64, 79}, dvid.Span{47, 65, 64, 79}, dvid.Span{47, 66, 64, 79},
				dvid.Span{47, 67, 64, 79}, dvid.Span{47, 68, 64, 79}, dvid.Span{47, 69, 64, 79}, dvid.Span{48, 64, 64, 79}, dvid.Span{48, 65, 64, 79},
				dvid.Span{48, 66, 64, 79}, dvid.Span{48, 67, 64, 79}, dvid.Span{48, 68, 64, 79}, dvid.Span{48, 69, 64, 79}, dvid.Span{49, 64, 64, 79},
				dvid.Span{49, 65, 64, 79}, dvid.Span{49, 66, 64, 79}, dvid.Span{49, 67, 64, 79}, dvid.Span{49, 68, 64, 79}, dvid.Span{49, 69, 64, 79},
				dvid.Span{50, 64, 64, 79}, dvid.Span{50, 65, 64, 79}, dvid.Span{50, 66, 64, 79}, dvid.Span{50, 67, 64, 79}, dvid.Span{50, 68, 64, 79},
				dvid.Span{50, 69, 64, 79}, dvid.Span{51, 64, 64, 79}, dvid.Span{51, 65, 64, 79}, dvid.Span{51, 66, 64, 79}, dvid.Span{51, 67, 64, 79},
				dvid.Span{51, 68, 64, 79}, dvid.Span{51, 69, 64, 79}, dvid.Span{52, 64, 64, 79}, dvid.Span{52, 65, 64, 79}, dvid.Span{52, 66, 64, 79},
				dvid.Span{52, 67, 64, 79}, dvid.Span{52, 68, 64, 79}, dvid.Span{52, 69, 64, 79}, dvid.Span{53, 64, 64, 79}, dvid.Span{53, 65, 64, 79},
				dvid.Span{53, 66, 64, 79}, dvid.Span{53, 67, 64, 79}, dvid.Span{53, 68, 64, 79}, dvid.Span{53, 69, 64, 79}, dvid.Span{54, 64, 64, 79},
				dvid.Span{54, 65, 64, 79}, dvid.Span{54, 66, 64, 79}, dvid.Span{54, 67, 64, 79}, dvid.Span{54, 68, 64, 79}, dvid.Span{54, 69, 64, 79},
				dvid.Span{55, 64, 64, 79}, dvid.Span{55, 65, 64, 79}, dvid.Span{55, 66, 64, 79}, dvid.Span{55, 67, 64, 79}, dvid.Span{55, 68, 64, 79},
				dvid.Span{55, 69, 64, 79}, dvid.Span{56, 64, 64, 79}, dvid.Span{56, 65, 64, 79}, dvid.Span{56, 66, 64, 79}, dvid.Span{56, 67, 64, 79},
				dvid.Span{56, 68, 64, 79}, dvid.Span{56, 69, 64, 79}, dvid.Span{57, 64, 64, 79}, dvid.Span{57, 65, 64, 79}, dvid.Span{57, 66, 64, 79},
				dvid.Span{57, 67, 64, 79}, dvid.Span{57, 68, 64, 79}, dvid.Span{57, 69, 64, 79}, dvid.Span{58, 64, 64, 79}, dvid.Span{58, 65, 64, 79},
				dvid.Span{58, 66, 64, 79}, dvid.Span{58, 67, 64, 79}, dvid.Span{58, 68, 64, 79}, dvid.Span{58, 69, 64, 79}, dvid.Span{59, 64, 64, 79},
				dvid.Span{59, 65, 64, 79}, dvid.Span{59, 66, 64, 79}, dvid.Span{59, 67, 64, 79}, dvid.Span{59, 68, 64, 79}, dvid.Span{59, 69, 64, 79},
			},
		}, {
			label:  3,
			offset: dvid.Point3d{40, 40, 10},
			size:   dvid.Point3d{20, 20, 30},
			blockSpans: []dvid.Span{
				dvid.Span{0, 1, 1, 1},
				dvid.Span{1, 1, 1, 1},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{10, 40, 40, 59}, dvid.Span{10, 41, 40, 59}, dvid.Span{10, 42, 40, 59}, dvid.Span{10, 43, 40, 59}, dvid.Span{10, 44, 40, 59},
				dvid.Span{10, 45, 40, 59}, dvid.Span{10, 46, 40, 59}, dvid.Span{10, 47, 40, 59}, dvid.Span{10, 48, 40, 59}, dvid.Span{10, 49, 40, 59},
				dvid.Span{10, 50, 40, 59}, dvid.Span{10, 51, 40, 59}, dvid.Span{10, 52, 40, 59}, dvid.Span{10, 53, 40, 59}, dvid.Span{10, 54, 40, 59},
				dvid.Span{10, 55, 40, 59}, dvid.Span{10, 56, 40, 59}, dvid.Span{10, 57, 40, 59}, dvid.Span{10, 58, 40, 59}, dvid.Span{10, 59, 40, 59},
				dvid.Span{11, 40, 40, 59}, dvid.Span{11, 41, 40, 59}, dvid.Span{11, 42, 40, 59}, dvid.Span{11, 43, 40, 59}, dvid.Span{11, 44, 40, 59},
				dvid.Span{11, 45, 40, 59}, dvid.Span{11, 46, 40, 59}, dvid.Span{11, 47, 40, 59}, dvid.Span{11, 48, 40, 59}, dvid.Span{11, 49, 40, 59},
				dvid.Span{11, 50, 40, 59}, dvid.Span{11, 51, 40, 59}, dvid.Span{11, 52, 40, 59}, dvid.Span{11, 53, 40, 59}, dvid.Span{11, 54, 40, 59},
				dvid.Span{11, 55, 40, 59}, dvid.Span{11, 56, 40, 59}, dvid.Span{11, 57, 40, 59}, dvid.Span{11, 58, 40, 59}, dvid.Span{11, 59, 40, 59},
				dvid.Span{12, 40, 40, 59}, dvid.Span{12, 41, 40, 59}, dvid.Span{12, 42, 40, 59}, dvid.Span{12, 43, 40, 59}, dvid.Span{12, 44, 40, 59},
				dvid.Span{12, 45, 40, 59}, dvid.Span{12, 46, 40, 59}, dvid.Span{12, 47, 40, 59}, dvid.Span{12, 48, 40, 59}, dvid.Span{12, 49, 40, 59},
				dvid.Span{12, 50, 40, 59}, dvid.Span{12, 51, 40, 59}, dvid.Span{12, 52, 40, 59}, dvid.Span{12, 53, 40, 59}, dvid.Span{12, 54, 40, 59},
				dvid.Span{12, 55, 40, 59}, dvid.Span{12, 56, 40, 59}, dvid.Span{12, 57, 40, 59}, dvid.Span{12, 58, 40, 59}, dvid.Span{12, 59, 40, 59},
				dvid.Span{13, 40, 40, 59}, dvid.Span{13, 41, 40, 59}, dvid.Span{13, 42, 40, 59}, dvid.Span{13, 43, 40, 59}, dvid.Span{13, 44, 40, 59},
				dvid.Span{13, 45, 40, 59}, dvid.Span{13, 46, 40, 59}, dvid.Span{13, 47, 40, 59}, dvid.Span{13, 48, 40, 59}, dvid.Span{13, 49, 40, 59},
				dvid.Span{13, 50, 40, 59}, dvid.Span{13, 51, 40, 59}, dvid.Span{13, 52, 40, 59}, dvid.Span{13, 53, 40, 59}, dvid.Span{13, 54, 40, 59},
				dvid.Span{13, 55, 40, 59}, dvid.Span{13, 56, 40, 59}, dvid.Span{13, 57, 40, 59}, dvid.Span{13, 58, 40, 59}, dvid.Span{13, 59, 40, 59},
				dvid.Span{14, 40, 40, 59}, dvid.Span{14, 41, 40, 59}, dvid.Span{14, 42, 40, 59}, dvid.Span{14, 43, 40, 59}, dvid.Span{14, 44, 40, 59},
				dvid.Span{14, 45, 40, 59}, dvid.Span{14, 46, 40, 59}, dvid.Span{14, 47, 40, 59}, dvid.Span{14, 48, 40, 59}, dvid.Span{14, 49, 40, 59},
				dvid.Span{14, 50, 40, 59}, dvid.Span{14, 51, 40, 59}, dvid.Span{14, 52, 40, 59}, dvid.Span{14, 53, 40, 59}, dvid.Span{14, 54, 40, 59},
				dvid.Span{14, 55, 40, 59}, dvid.Span{14, 56, 40, 59}, dvid.Span{14, 57, 40, 59}, dvid.Span{14, 58, 40, 59}, dvid.Span{14, 59, 40, 59},
				dvid.Span{15, 40, 40, 59}, dvid.Span{15, 41, 40, 59}, dvid.Span{15, 42, 40, 59}, dvid.Span{15, 43, 40, 59}, dvid.Span{15, 44, 40, 59},
				dvid.Span{15, 45, 40, 59}, dvid.Span{15, 46, 40, 59}, dvid.Span{15, 47, 40, 59}, dvid.Span{15, 48, 40, 59}, dvid.Span{15, 49, 40, 59},
				dvid.Span{15, 50, 40, 59}, dvid.Span{15, 51, 40, 59}, dvid.Span{15, 52, 40, 59}, dvid.Span{15, 53, 40, 59}, dvid.Span{15, 54, 40, 59},
				dvid.Span{15, 55, 40, 59}, dvid.Span{15, 56, 40, 59}, dvid.Span{15, 57, 40, 59}, dvid.Span{15, 58, 40, 59}, dvid.Span{15, 59, 40, 59},
				dvid.Span{16, 40, 40, 59}, dvid.Span{16, 41, 40, 59}, dvid.Span{16, 42, 40, 59}, dvid.Span{16, 43, 40, 59}, dvid.Span{16, 44, 40, 59},
				dvid.Span{16, 45, 40, 59}, dvid.Span{16, 46, 40, 59}, dvid.Span{16, 47, 40, 59}, dvid.Span{16, 48, 40, 59}, dvid.Span{16, 49, 40, 59},
				dvid.Span{16, 50, 40, 59}, dvid.Span{16, 51, 40, 59}, dvid.Span{16, 52, 40, 59}, dvid.Span{16, 53, 40, 59}, dvid.Span{16, 54, 40, 59},
				dvid.Span{16, 55, 40, 59}, dvid.Span{16, 56, 40, 59}, dvid.Span{16, 57, 40, 59}, dvid.Span{16, 58, 40, 59}, dvid.Span{16, 59, 40, 59},
				dvid.Span{17, 40, 40, 59}, dvid.Span{17, 41, 40, 59}, dvid.Span{17, 42, 40, 59}, dvid.Span{17, 43, 40, 59}, dvid.Span{17, 44, 40, 59},
				dvid.Span{17, 45, 40, 59}, dvid.Span{17, 46, 40, 59}, dvid.Span{17, 47, 40, 59}, dvid.Span{17, 48, 40, 59}, dvid.Span{17, 49, 40, 59},
				dvid.Span{17, 50, 40, 59}, dvid.Span{17, 51, 40, 59}, dvid.Span{17, 52, 40, 59}, dvid.Span{17, 53, 40, 59}, dvid.Span{17, 54, 40, 59},
				dvid.Span{17, 55, 40, 59}, dvid.Span{17, 56, 40, 59}, dvid.Span{17, 57, 40, 59}, dvid.Span{17, 58, 40, 59}, dvid.Span{17, 59, 40, 59},
				dvid.Span{18, 40, 40, 59}, dvid.Span{18, 41, 40, 59}, dvid.Span{18, 42, 40, 59}, dvid.Span{18, 43, 40, 59}, dvid.Span{18, 44, 40, 59},
				dvid.Span{18, 45, 40, 59}, dvid.Span{18, 46, 40, 59}, dvid.Span{18, 47, 40, 59}, dvid.Span{18, 48, 40, 59}, dvid.Span{18, 49, 40, 59},
				dvid.Span{18, 50, 40, 59}, dvid.Span{18, 51, 40, 59}, dvid.Span{18, 52, 40, 59}, dvid.Span{18, 53, 40, 59}, dvid.Span{18, 54, 40, 59},
				dvid.Span{18, 55, 40, 59}, dvid.Span{18, 56, 40, 59}, dvid.Span{18, 57, 40, 59}, dvid.Span{18, 58, 40, 59}, dvid.Span{18, 59, 40, 59},
				dvid.Span{19, 40, 40, 59}, dvid.Span{19, 41, 40, 59}, dvid.Span{19, 42, 40, 59}, dvid.Span{19, 43, 40, 59}, dvid.Span{19, 44, 40, 59},
				dvid.Span{19, 45, 40, 59}, dvid.Span{19, 46, 40, 59}, dvid.Span{19, 47, 40, 59}, dvid.Span{19, 48, 40, 59}, dvid.Span{19, 49, 40, 59},
				dvid.Span{19, 50, 40, 59}, dvid.Span{19, 51, 40, 59}, dvid.Span{19, 52, 40, 59}, dvid.Span{19, 53, 40, 59}, dvid.Span{19, 54, 40, 59},
				dvid.Span{19, 55, 40, 59}, dvid.Span{19, 56, 40, 59}, dvid.Span{19, 57, 40, 59}, dvid.Span{19, 58, 40, 59}, dvid.Span{19, 59, 40, 59},
				dvid.Span{20, 40, 40, 59}, dvid.Span{20, 41, 40, 59}, dvid.Span{20, 42, 40, 59}, dvid.Span{20, 43, 40, 59}, dvid.Span{20, 44, 40, 59},
				dvid.Span{20, 45, 40, 59}, dvid.Span{20, 46, 40, 59}, dvid.Span{20, 47, 40, 59}, dvid.Span{20, 48, 40, 59}, dvid.Span{20, 49, 40, 59},
				dvid.Span{20, 50, 40, 59}, dvid.Span{20, 51, 40, 59}, dvid.Span{20, 52, 40, 59}, dvid.Span{20, 53, 40, 59}, dvid.Span{20, 54, 40, 59},
				dvid.Span{20, 55, 40, 59}, dvid.Span{20, 56, 40, 59}, dvid.Span{20, 57, 40, 59}, dvid.Span{20, 58, 40, 59}, dvid.Span{20, 59, 40, 59},
				dvid.Span{21, 40, 40, 59}, dvid.Span{21, 41, 40, 59}, dvid.Span{21, 42, 40, 59}, dvid.Span{21, 43, 40, 59}, dvid.Span{21, 44, 40, 59},
				dvid.Span{21, 45, 40, 59}, dvid.Span{21, 46, 40, 59}, dvid.Span{21, 47, 40, 59}, dvid.Span{21, 48, 40, 59}, dvid.Span{21, 49, 40, 59},
				dvid.Span{21, 50, 40, 59}, dvid.Span{21, 51, 40, 59}, dvid.Span{21, 52, 40, 59}, dvid.Span{21, 53, 40, 59}, dvid.Span{21, 54, 40, 59},
				dvid.Span{21, 55, 40, 59}, dvid.Span{21, 56, 40, 59}, dvid.Span{21, 57, 40, 59}, dvid.Span{21, 58, 40, 59}, dvid.Span{21, 59, 40, 59},
				dvid.Span{22, 40, 40, 59}, dvid.Span{22, 41, 40, 59}, dvid.Span{22, 42, 40, 59}, dvid.Span{22, 43, 40, 59}, dvid.Span{22, 44, 40, 59},
				dvid.Span{22, 45, 40, 59}, dvid.Span{22, 46, 40, 59}, dvid.Span{22, 47, 40, 59}, dvid.Span{22, 48, 40, 59}, dvid.Span{22, 49, 40, 59},
				dvid.Span{22, 50, 40, 59}, dvid.Span{22, 51, 40, 59}, dvid.Span{22, 52, 40, 59}, dvid.Span{22, 53, 40, 59}, dvid.Span{22, 54, 40, 59},
				dvid.Span{22, 55, 40, 59}, dvid.Span{22, 56, 40, 59}, dvid.Span{22, 57, 40, 59}, dvid.Span{22, 58, 40, 59}, dvid.Span{22, 59, 40, 59},
				dvid.Span{23, 40, 40, 59}, dvid.Span{23, 41, 40, 59}, dvid.Span{23, 42, 40, 59}, dvid.Span{23, 43, 40, 59}, dvid.Span{23, 44, 40, 59},
				dvid.Span{23, 45, 40, 59}, dvid.Span{23, 46, 40, 59}, dvid.Span{23, 47, 40, 59}, dvid.Span{23, 48, 40, 59}, dvid.Span{23, 49, 40, 59},
				dvid.Span{23, 50, 40, 59}, dvid.Span{23, 51, 40, 59}, dvid.Span{23, 52, 40, 59}, dvid.Span{23, 53, 40, 59}, dvid.Span{23, 54, 40, 59},
				dvid.Span{23, 55, 40, 59}, dvid.Span{23, 56, 40, 59}, dvid.Span{23, 57, 40, 59}, dvid.Span{23, 58, 40, 59}, dvid.Span{23, 59, 40, 59},
				dvid.Span{24, 40, 40, 59}, dvid.Span{24, 41, 40, 59}, dvid.Span{24, 42, 40, 59}, dvid.Span{24, 43, 40, 59}, dvid.Span{24, 44, 40, 59},
				dvid.Span{24, 45, 40, 59}, dvid.Span{24, 46, 40, 59}, dvid.Span{24, 47, 40, 59}, dvid.Span{24, 48, 40, 59}, dvid.Span{24, 49, 40, 59},
				dvid.Span{24, 50, 40, 59}, dvid.Span{24, 51, 40, 59}, dvid.Span{24, 52, 40, 59}, dvid.Span{24, 53, 40, 59}, dvid.Span{24, 54, 40, 59},
				dvid.Span{24, 55, 40, 59}, dvid.Span{24, 56, 40, 59}, dvid.Span{24, 57, 40, 59}, dvid.Span{24, 58, 40, 59}, dvid.Span{24, 59, 40, 59},
				dvid.Span{25, 40, 40, 59}, dvid.Span{25, 41, 40, 59}, dvid.Span{25, 42, 40, 59}, dvid.Span{25, 43, 40, 59}, dvid.Span{25, 44, 40, 59},
				dvid.Span{25, 45, 40, 59}, dvid.Span{25, 46, 40, 59}, dvid.Span{25, 47, 40, 59}, dvid.Span{25, 48, 40, 59}, dvid.Span{25, 49, 40, 59},
				dvid.Span{25, 50, 40, 59}, dvid.Span{25, 51, 40, 59}, dvid.Span{25, 52, 40, 59}, dvid.Span{25, 53, 40, 59}, dvid.Span{25, 54, 40, 59},
				dvid.Span{25, 55, 40, 59}, dvid.Span{25, 56, 40, 59}, dvid.Span{25, 57, 40, 59}, dvid.Span{25, 58, 40, 59}, dvid.Span{25, 59, 40, 59},
				dvid.Span{26, 40, 40, 59}, dvid.Span{26, 41, 40, 59}, dvid.Span{26, 42, 40, 59}, dvid.Span{26, 43, 40, 59}, dvid.Span{26, 44, 40, 59},
				dvid.Span{26, 45, 40, 59}, dvid.Span{26, 46, 40, 59}, dvid.Span{26, 47, 40, 59}, dvid.Span{26, 48, 40, 59}, dvid.Span{26, 49, 40, 59},
				dvid.Span{26, 50, 40, 59}, dvid.Span{26, 51, 40, 59}, dvid.Span{26, 52, 40, 59}, dvid.Span{26, 53, 40, 59}, dvid.Span{26, 54, 40, 59},
				dvid.Span{26, 55, 40, 59}, dvid.Span{26, 56, 40, 59}, dvid.Span{26, 57, 40, 59}, dvid.Span{26, 58, 40, 59}, dvid.Span{26, 59, 40, 59},
				dvid.Span{27, 40, 40, 59}, dvid.Span{27, 41, 40, 59}, dvid.Span{27, 42, 40, 59}, dvid.Span{27, 43, 40, 59}, dvid.Span{27, 44, 40, 59},
				dvid.Span{27, 45, 40, 59}, dvid.Span{27, 46, 40, 59}, dvid.Span{27, 47, 40, 59}, dvid.Span{27, 48, 40, 59}, dvid.Span{27, 49, 40, 59},
				dvid.Span{27, 50, 40, 59}, dvid.Span{27, 51, 40, 59}, dvid.Span{27, 52, 40, 59}, dvid.Span{27, 53, 40, 59}, dvid.Span{27, 54, 40, 59},
				dvid.Span{27, 55, 40, 59}, dvid.Span{27, 56, 40, 59}, dvid.Span{27, 57, 40, 59}, dvid.Span{27, 58, 40, 59}, dvid.Span{27, 59, 40, 59},
				dvid.Span{28, 40, 40, 59}, dvid.Span{28, 41, 40, 59}, dvid.Span{28, 42, 40, 59}, dvid.Span{28, 43, 40, 59}, dvid.Span{28, 44, 40, 59},
				dvid.Span{28, 45, 40, 59}, dvid.Span{28, 46, 40, 59}, dvid.Span{28, 47, 40, 59}, dvid.Span{28, 48, 40, 59}, dvid.Span{28, 49, 40, 59},
				dvid.Span{28, 50, 40, 59}, dvid.Span{28, 51, 40, 59}, dvid.Span{28, 52, 40, 59}, dvid.Span{28, 53, 40, 59}, dvid.Span{28, 54, 40, 59},
				dvid.Span{28, 55, 40, 59}, dvid.Span{28, 56, 40, 59}, dvid.Span{28, 57, 40, 59}, dvid.Span{28, 58, 40, 59}, dvid.Span{28, 59, 40, 59},
				dvid.Span{29, 40, 40, 59}, dvid.Span{29, 41, 40, 59}, dvid.Span{29, 42, 40, 59}, dvid.Span{29, 43, 40, 59}, dvid.Span{29, 44, 40, 59},
				dvid.Span{29, 45, 40, 59}, dvid.Span{29, 46, 40, 59}, dvid.Span{29, 47, 40, 59}, dvid.Span{29, 48, 40, 59}, dvid.Span{29, 49, 40, 59},
				dvid.Span{29, 50, 40, 59}, dvid.Span{29, 51, 40, 59}, dvid.Span{29, 52, 40, 59}, dvid.Span{29, 53, 40, 59}, dvid.Span{29, 54, 40, 59},
				dvid.Span{29, 55, 40, 59}, dvid.Span{29, 56, 40, 59}, dvid.Span{29, 57, 40, 59}, dvid.Span{29, 58, 40, 59}, dvid.Span{29, 59, 40, 59},
				dvid.Span{30, 40, 40, 59}, dvid.Span{30, 41, 40, 59}, dvid.Span{30, 42, 40, 59}, dvid.Span{30, 43, 40, 59}, dvid.Span{30, 44, 40, 59},
				dvid.Span{30, 45, 40, 59}, dvid.Span{30, 46, 40, 59}, dvid.Span{30, 47, 40, 59}, dvid.Span{30, 48, 40, 59}, dvid.Span{30, 49, 40, 59},
				dvid.Span{30, 50, 40, 59}, dvid.Span{30, 51, 40, 59}, dvid.Span{30, 52, 40, 59}, dvid.Span{30, 53, 40, 59}, dvid.Span{30, 54, 40, 59},
				dvid.Span{30, 55, 40, 59}, dvid.Span{30, 56, 40, 59}, dvid.Span{30, 57, 40, 59}, dvid.Span{30, 58, 40, 59}, dvid.Span{30, 59, 40, 59},
				dvid.Span{31, 40, 40, 59}, dvid.Span{31, 41, 40, 59}, dvid.Span{31, 42, 40, 59}, dvid.Span{31, 43, 40, 59}, dvid.Span{31, 44, 40, 59},
				dvid.Span{31, 45, 40, 59}, dvid.Span{31, 46, 40, 59}, dvid.Span{31, 47, 40, 59}, dvid.Span{31, 48, 40, 59}, dvid.Span{31, 49, 40, 59},
				dvid.Span{31, 50, 40, 59}, dvid.Span{31, 51, 40, 59}, dvid.Span{31, 52, 40, 59}, dvid.Span{31, 53, 40, 59}, dvid.Span{31, 54, 40, 59},
				dvid.Span{31, 55, 40, 59}, dvid.Span{31, 56, 40, 59}, dvid.Span{31, 57, 40, 59}, dvid.Span{31, 58, 40, 59}, dvid.Span{31, 59, 40, 59},
				dvid.Span{32, 40, 40, 59}, dvid.Span{32, 41, 40, 59}, dvid.Span{32, 42, 40, 59}, dvid.Span{32, 43, 40, 59}, dvid.Span{32, 44, 40, 59},
				dvid.Span{32, 45, 40, 59}, dvid.Span{32, 46, 40, 59}, dvid.Span{32, 47, 40, 59}, dvid.Span{32, 48, 40, 59}, dvid.Span{32, 49, 40, 59},
				dvid.Span{32, 50, 40, 59}, dvid.Span{32, 51, 40, 59}, dvid.Span{32, 52, 40, 59}, dvid.Span{32, 53, 40, 59}, dvid.Span{32, 54, 40, 59},
				dvid.Span{32, 55, 40, 59}, dvid.Span{32, 56, 40, 59}, dvid.Span{32, 57, 40, 59}, dvid.Span{32, 58, 40, 59}, dvid.Span{32, 59, 40, 59},
				dvid.Span{33, 40, 40, 59}, dvid.Span{33, 41, 40, 59}, dvid.Span{33, 42, 40, 59}, dvid.Span{33, 43, 40, 59}, dvid.Span{33, 44, 40, 59},
				dvid.Span{33, 45, 40, 59}, dvid.Span{33, 46, 40, 59}, dvid.Span{33, 47, 40, 59}, dvid.Span{33, 48, 40, 59}, dvid.Span{33, 49, 40, 59},
				dvid.Span{33, 50, 40, 59}, dvid.Span{33, 51, 40, 59}, dvid.Span{33, 52, 40, 59}, dvid.Span{33, 53, 40, 59}, dvid.Span{33, 54, 40, 59},
				dvid.Span{33, 55, 40, 59}, dvid.Span{33, 56, 40, 59}, dvid.Span{33, 57, 40, 59}, dvid.Span{33, 58, 40, 59}, dvid.Span{33, 59, 40, 59},
				dvid.Span{34, 40, 40, 59}, dvid.Span{34, 41, 40, 59}, dvid.Span{34, 42, 40, 59}, dvid.Span{34, 43, 40, 59}, dvid.Span{34, 44, 40, 59},
				dvid.Span{34, 45, 40, 59}, dvid.Span{34, 46, 40, 59}, dvid.Span{34, 47, 40, 59}, dvid.Span{34, 48, 40, 59}, dvid.Span{34, 49, 40, 59},
				dvid.Span{34, 50, 40, 59}, dvid.Span{34, 51, 40, 59}, dvid.Span{34, 52, 40, 59}, dvid.Span{34, 53, 40, 59}, dvid.Span{34, 54, 40, 59},
				dvid.Span{34, 55, 40, 59}, dvid.Span{34, 56, 40, 59}, dvid.Span{34, 57, 40, 59}, dvid.Span{34, 58, 40, 59}, dvid.Span{34, 59, 40, 59},
				dvid.Span{35, 40, 40, 59}, dvid.Span{35, 41, 40, 59}, dvid.Span{35, 42, 40, 59}, dvid.Span{35, 43, 40, 59}, dvid.Span{35, 44, 40, 59},
				dvid.Span{35, 45, 40, 59}, dvid.Span{35, 46, 40, 59}, dvid.Span{35, 47, 40, 59}, dvid.Span{35, 48, 40, 59}, dvid.Span{35, 49, 40, 59},
				dvid.Span{35, 50, 40, 59}, dvid.Span{35, 51, 40, 59}, dvid.Span{35, 52, 40, 59}, dvid.Span{35, 53, 40, 59}, dvid.Span{35, 54, 40, 59},
				dvid.Span{35, 55, 40, 59}, dvid.Span{35, 56, 40, 59}, dvid.Span{35, 57, 40, 59}, dvid.Span{35, 58, 40, 59}, dvid.Span{35, 59, 40, 59},
				dvid.Span{36, 40, 40, 59}, dvid.Span{36, 41, 40, 59}, dvid.Span{36, 42, 40, 59}, dvid.Span{36, 43, 40, 59}, dvid.Span{36, 44, 40, 59},
				dvid.Span{36, 45, 40, 59}, dvid.Span{36, 46, 40, 59}, dvid.Span{36, 47, 40, 59}, dvid.Span{36, 48, 40, 59}, dvid.Span{36, 49, 40, 59},
				dvid.Span{36, 50, 40, 59}, dvid.Span{36, 51, 40, 59}, dvid.Span{36, 52, 40, 59}, dvid.Span{36, 53, 40, 59}, dvid.Span{36, 54, 40, 59},
				dvid.Span{36, 55, 40, 59}, dvid.Span{36, 56, 40, 59}, dvid.Span{36, 57, 40, 59}, dvid.Span{36, 58, 40, 59}, dvid.Span{36, 59, 40, 59},
				dvid.Span{37, 40, 40, 59}, dvid.Span{37, 41, 40, 59}, dvid.Span{37, 42, 40, 59}, dvid.Span{37, 43, 40, 59}, dvid.Span{37, 44, 40, 59},
				dvid.Span{37, 45, 40, 59}, dvid.Span{37, 46, 40, 59}, dvid.Span{37, 47, 40, 59}, dvid.Span{37, 48, 40, 59}, dvid.Span{37, 49, 40, 59},
				dvid.Span{37, 50, 40, 59}, dvid.Span{37, 51, 40, 59}, dvid.Span{37, 52, 40, 59}, dvid.Span{37, 53, 40, 59}, dvid.Span{37, 54, 40, 59},
				dvid.Span{37, 55, 40, 59}, dvid.Span{37, 56, 40, 59}, dvid.Span{37, 57, 40, 59}, dvid.Span{37, 58, 40, 59}, dvid.Span{37, 59, 40, 59},
				dvid.Span{38, 40, 40, 59}, dvid.Span{38, 41, 40, 59}, dvid.Span{38, 42, 40, 59}, dvid.Span{38, 43, 40, 59}, dvid.Span{38, 44, 40, 59},
				dvid.Span{38, 45, 40, 59}, dvid.Span{38, 46, 40, 59}, dvid.Span{38, 47, 40, 59}, dvid.Span{38, 48, 40, 59}, dvid.Span{38, 49, 40, 59},
				dvid.Span{38, 50, 40, 59}, dvid.Span{38, 51, 40, 59}, dvid.Span{38, 52, 40, 59}, dvid.Span{38, 53, 40, 59}, dvid.Span{38, 54, 40, 59},
				dvid.Span{38, 55, 40, 59}, dvid.Span{38, 56, 40, 59}, dvid.Span{38, 57, 40, 59}, dvid.Span{38, 58, 40, 59}, dvid.Span{38, 59, 40, 59},
				dvid.Span{39, 40, 40, 59}, dvid.Span{39, 41, 40, 59}, dvid.Span{39, 42, 40, 59}, dvid.Span{39, 43, 40, 59}, dvid.Span{39, 44, 40, 59},
				dvid.Span{39, 45, 40, 59}, dvid.Span{39, 46, 40, 59}, dvid.Span{39, 47, 40, 59}, dvid.Span{39, 48, 40, 59}, dvid.Span{39, 49, 40, 59},
				dvid.Span{39, 50, 40, 59}, dvid.Span{39, 51, 40, 59}, dvid.Span{39, 52, 40, 59}, dvid.Span{39, 53, 40, 59}, dvid.Span{39, 54, 40, 59},
				dvid.Span{39, 55, 40, 59}, dvid.Span{39, 56, 40, 59}, dvid.Span{39, 57, 40, 59}, dvid.Span{39, 58, 40, 59}, dvid.Span{39, 59, 40, 59},
			},
		}, {
			label:  4,
			offset: dvid.Point3d{75, 40, 60},
			size:   dvid.Point3d{20, 20, 30},
			blockSpans: []dvid.Span{
				dvid.Span{1, 1, 2, 2},
				dvid.Span{2, 1, 2, 2},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{60, 40, 75, 94}, dvid.Span{60, 41, 75, 94}, dvid.Span{60, 42, 75, 94}, dvid.Span{60, 43, 75, 94}, dvid.Span{60, 44, 75, 94},
				dvid.Span{60, 45, 75, 94}, dvid.Span{60, 46, 75, 94}, dvid.Span{60, 47, 75, 94}, dvid.Span{60, 48, 75, 94}, dvid.Span{60, 49, 75, 94},
				dvid.Span{60, 50, 75, 94}, dvid.Span{60, 51, 75, 94}, dvid.Span{60, 52, 75, 94}, dvid.Span{60, 53, 75, 94}, dvid.Span{60, 54, 75, 94},
				dvid.Span{60, 55, 75, 94}, dvid.Span{60, 56, 75, 94}, dvid.Span{60, 57, 75, 94}, dvid.Span{60, 58, 75, 94}, dvid.Span{60, 59, 75, 94},
				dvid.Span{61, 40, 75, 94}, dvid.Span{61, 41, 75, 94}, dvid.Span{61, 42, 75, 94}, dvid.Span{61, 43, 75, 94}, dvid.Span{61, 44, 75, 94},
				dvid.Span{61, 45, 75, 94}, dvid.Span{61, 46, 75, 94}, dvid.Span{61, 47, 75, 94}, dvid.Span{61, 48, 75, 94}, dvid.Span{61, 49, 75, 94},
				dvid.Span{61, 50, 75, 94}, dvid.Span{61, 51, 75, 94}, dvid.Span{61, 52, 75, 94}, dvid.Span{61, 53, 75, 94}, dvid.Span{61, 54, 75, 94},
				dvid.Span{61, 55, 75, 94}, dvid.Span{61, 56, 75, 94}, dvid.Span{61, 57, 75, 94}, dvid.Span{61, 58, 75, 94}, dvid.Span{61, 59, 75, 94},
				dvid.Span{62, 40, 75, 94}, dvid.Span{62, 41, 75, 94}, dvid.Span{62, 42, 75, 94}, dvid.Span{62, 43, 75, 94}, dvid.Span{62, 44, 75, 94},
				dvid.Span{62, 45, 75, 94}, dvid.Span{62, 46, 75, 94}, dvid.Span{62, 47, 75, 94}, dvid.Span{62, 48, 75, 94}, dvid.Span{62, 49, 75, 94},
				dvid.Span{62, 50, 75, 94}, dvid.Span{62, 51, 75, 94}, dvid.Span{62, 52, 75, 94}, dvid.Span{62, 53, 75, 94}, dvid.Span{62, 54, 75, 94},
				dvid.Span{62, 55, 75, 94}, dvid.Span{62, 56, 75, 94}, dvid.Span{62, 57, 75, 94}, dvid.Span{62, 58, 75, 94}, dvid.Span{62, 59, 75, 94},
				dvid.Span{63, 40, 75, 94}, dvid.Span{63, 41, 75, 94}, dvid.Span{63, 42, 75, 94}, dvid.Span{63, 43, 75, 94}, dvid.Span{63, 44, 75, 94},
				dvid.Span{63, 45, 75, 94}, dvid.Span{63, 46, 75, 94}, dvid.Span{63, 47, 75, 94}, dvid.Span{63, 48, 75, 94}, dvid.Span{63, 49, 75, 94},
				dvid.Span{63, 50, 75, 94}, dvid.Span{63, 51, 75, 94}, dvid.Span{63, 52, 75, 94}, dvid.Span{63, 53, 75, 94}, dvid.Span{63, 54, 75, 94},
				dvid.Span{63, 55, 75, 94}, dvid.Span{63, 56, 75, 94}, dvid.Span{63, 57, 75, 94}, dvid.Span{63, 58, 75, 94}, dvid.Span{63, 59, 75, 94},
				dvid.Span{64, 40, 75, 94}, dvid.Span{64, 41, 75, 94}, dvid.Span{64, 42, 75, 94}, dvid.Span{64, 43, 75, 94}, dvid.Span{64, 44, 75, 94},
				dvid.Span{64, 45, 75, 94}, dvid.Span{64, 46, 75, 94}, dvid.Span{64, 47, 75, 94}, dvid.Span{64, 48, 75, 94}, dvid.Span{64, 49, 75, 94},
				dvid.Span{64, 50, 75, 94}, dvid.Span{64, 51, 75, 94}, dvid.Span{64, 52, 75, 94}, dvid.Span{64, 53, 75, 94}, dvid.Span{64, 54, 75, 94},
				dvid.Span{64, 55, 75, 94}, dvid.Span{64, 56, 75, 94}, dvid.Span{64, 57, 75, 94}, dvid.Span{64, 58, 75, 94}, dvid.Span{64, 59, 75, 94},
				dvid.Span{65, 40, 75, 94}, dvid.Span{65, 41, 75, 94}, dvid.Span{65, 42, 75, 94}, dvid.Span{65, 43, 75, 94}, dvid.Span{65, 44, 75, 94},
				dvid.Span{65, 45, 75, 94}, dvid.Span{65, 46, 75, 94}, dvid.Span{65, 47, 75, 94}, dvid.Span{65, 48, 75, 94}, dvid.Span{65, 49, 75, 94},
				dvid.Span{65, 50, 75, 94}, dvid.Span{65, 51, 75, 94}, dvid.Span{65, 52, 75, 94}, dvid.Span{65, 53, 75, 94}, dvid.Span{65, 54, 75, 94},
				dvid.Span{65, 55, 75, 94}, dvid.Span{65, 56, 75, 94}, dvid.Span{65, 57, 75, 94}, dvid.Span{65, 58, 75, 94}, dvid.Span{65, 59, 75, 94},
				dvid.Span{66, 40, 75, 94}, dvid.Span{66, 41, 75, 94}, dvid.Span{66, 42, 75, 94}, dvid.Span{66, 43, 75, 94}, dvid.Span{66, 44, 75, 94},
				dvid.Span{66, 45, 75, 94}, dvid.Span{66, 46, 75, 94}, dvid.Span{66, 47, 75, 94}, dvid.Span{66, 48, 75, 94}, dvid.Span{66, 49, 75, 94},
				dvid.Span{66, 50, 75, 94}, dvid.Span{66, 51, 75, 94}, dvid.Span{66, 52, 75, 94}, dvid.Span{66, 53, 75, 94}, dvid.Span{66, 54, 75, 94},
				dvid.Span{66, 55, 75, 94}, dvid.Span{66, 56, 75, 94}, dvid.Span{66, 57, 75, 94}, dvid.Span{66, 58, 75, 94}, dvid.Span{66, 59, 75, 94},
				dvid.Span{67, 40, 75, 94}, dvid.Span{67, 41, 75, 94}, dvid.Span{67, 42, 75, 94}, dvid.Span{67, 43, 75, 94}, dvid.Span{67, 44, 75, 94},
				dvid.Span{67, 45, 75, 94}, dvid.Span{67, 46, 75, 94}, dvid.Span{67, 47, 75, 94}, dvid.Span{67, 48, 75, 94}, dvid.Span{67, 49, 75, 94},
				dvid.Span{67, 50, 75, 94}, dvid.Span{67, 51, 75, 94}, dvid.Span{67, 52, 75, 94}, dvid.Span{67, 53, 75, 94}, dvid.Span{67, 54, 75, 94},
				dvid.Span{67, 55, 75, 94}, dvid.Span{67, 56, 75, 94}, dvid.Span{67, 57, 75, 94}, dvid.Span{67, 58, 75, 94}, dvid.Span{67, 59, 75, 94},
				dvid.Span{68, 40, 75, 94}, dvid.Span{68, 41, 75, 94}, dvid.Span{68, 42, 75, 94}, dvid.Span{68, 43, 75, 94}, dvid.Span{68, 44, 75, 94},
				dvid.Span{68, 45, 75, 94}, dvid.Span{68, 46, 75, 94}, dvid.Span{68, 47, 75, 94}, dvid.Span{68, 48, 75, 94}, dvid.Span{68, 49, 75, 94},
				dvid.Span{68, 50, 75, 94}, dvid.Span{68, 51, 75, 94}, dvid.Span{68, 52, 75, 94}, dvid.Span{68, 53, 75, 94}, dvid.Span{68, 54, 75, 94},
				dvid.Span{68, 55, 75, 94}, dvid.Span{68, 56, 75, 94}, dvid.Span{68, 57, 75, 94}, dvid.Span{68, 58, 75, 94}, dvid.Span{68, 59, 75, 94},
				dvid.Span{69, 40, 75, 94}, dvid.Span{69, 41, 75, 94}, dvid.Span{69, 42, 75, 94}, dvid.Span{69, 43, 75, 94}, dvid.Span{69, 44, 75, 94},
				dvid.Span{69, 45, 75, 94}, dvid.Span{69, 46, 75, 94}, dvid.Span{69, 47, 75, 94}, dvid.Span{69, 48, 75, 94}, dvid.Span{69, 49, 75, 94},
				dvid.Span{69, 50, 75, 94}, dvid.Span{69, 51, 75, 94}, dvid.Span{69, 52, 75, 94}, dvid.Span{69, 53, 75, 94}, dvid.Span{69, 54, 75, 94},
				dvid.Span{69, 55, 75, 94}, dvid.Span{69, 56, 75, 94}, dvid.Span{69, 57, 75, 94}, dvid.Span{69, 58, 75, 94}, dvid.Span{69, 59, 75, 94},
				dvid.Span{70, 40, 75, 94}, dvid.Span{70, 41, 75, 94}, dvid.Span{70, 42, 75, 94}, dvid.Span{70, 43, 75, 94}, dvid.Span{70, 44, 75, 94},
				dvid.Span{70, 45, 75, 94}, dvid.Span{70, 46, 75, 94}, dvid.Span{70, 47, 75, 94}, dvid.Span{70, 48, 75, 94}, dvid.Span{70, 49, 75, 94},
				dvid.Span{70, 50, 75, 94}, dvid.Span{70, 51, 75, 94}, dvid.Span{70, 52, 75, 94}, dvid.Span{70, 53, 75, 94}, dvid.Span{70, 54, 75, 94},
				dvid.Span{70, 55, 75, 94}, dvid.Span{70, 56, 75, 94}, dvid.Span{70, 57, 75, 94}, dvid.Span{70, 58, 75, 94}, dvid.Span{70, 59, 75, 94},
				dvid.Span{71, 40, 75, 94}, dvid.Span{71, 41, 75, 94}, dvid.Span{71, 42, 75, 94}, dvid.Span{71, 43, 75, 94}, dvid.Span{71, 44, 75, 94},
				dvid.Span{71, 45, 75, 94}, dvid.Span{71, 46, 75, 94}, dvid.Span{71, 47, 75, 94}, dvid.Span{71, 48, 75, 94}, dvid.Span{71, 49, 75, 94},
				dvid.Span{71, 50, 75, 94}, dvid.Span{71, 51, 75, 94}, dvid.Span{71, 52, 75, 94}, dvid.Span{71, 53, 75, 94}, dvid.Span{71, 54, 75, 94},
				dvid.Span{71, 55, 75, 94}, dvid.Span{71, 56, 75, 94}, dvid.Span{71, 57, 75, 94}, dvid.Span{71, 58, 75, 94}, dvid.Span{71, 59, 75, 94},
				dvid.Span{72, 40, 75, 94}, dvid.Span{72, 41, 75, 94}, dvid.Span{72, 42, 75, 94}, dvid.Span{72, 43, 75, 94}, dvid.Span{72, 44, 75, 94},
				dvid.Span{72, 45, 75, 94}, dvid.Span{72, 46, 75, 94}, dvid.Span{72, 47, 75, 94}, dvid.Span{72, 48, 75, 94}, dvid.Span{72, 49, 75, 94},
				dvid.Span{72, 50, 75, 94}, dvid.Span{72, 51, 75, 94}, dvid.Span{72, 52, 75, 94}, dvid.Span{72, 53, 75, 94}, dvid.Span{72, 54, 75, 94},
				dvid.Span{72, 55, 75, 94}, dvid.Span{72, 56, 75, 94}, dvid.Span{72, 57, 75, 94}, dvid.Span{72, 58, 75, 94}, dvid.Span{72, 59, 75, 94},
				dvid.Span{73, 40, 75, 94}, dvid.Span{73, 41, 75, 94}, dvid.Span{73, 42, 75, 94}, dvid.Span{73, 43, 75, 94}, dvid.Span{73, 44, 75, 94},
				dvid.Span{73, 45, 75, 94}, dvid.Span{73, 46, 75, 94}, dvid.Span{73, 47, 75, 94}, dvid.Span{73, 48, 75, 94}, dvid.Span{73, 49, 75, 94},
				dvid.Span{73, 50, 75, 94}, dvid.Span{73, 51, 75, 94}, dvid.Span{73, 52, 75, 94}, dvid.Span{73, 53, 75, 94}, dvid.Span{73, 54, 75, 94},
				dvid.Span{73, 55, 75, 94}, dvid.Span{73, 56, 75, 94}, dvid.Span{73, 57, 75, 94}, dvid.Span{73, 58, 75, 94}, dvid.Span{73, 59, 75, 94},
				dvid.Span{74, 40, 75, 94}, dvid.Span{74, 41, 75, 94}, dvid.Span{74, 42, 75, 94}, dvid.Span{74, 43, 75, 94}, dvid.Span{74, 44, 75, 94},
				dvid.Span{74, 45, 75, 94}, dvid.Span{74, 46, 75, 94}, dvid.Span{74, 47, 75, 94}, dvid.Span{74, 48, 75, 94}, dvid.Span{74, 49, 75, 94},
				dvid.Span{74, 50, 75, 94}, dvid.Span{74, 51, 75, 94}, dvid.Span{74, 52, 75, 94}, dvid.Span{74, 53, 75, 94}, dvid.Span{74, 54, 75, 94},
				dvid.Span{74, 55, 75, 94}, dvid.Span{74, 56, 75, 94}, dvid.Span{74, 57, 75, 94}, dvid.Span{74, 58, 75, 94}, dvid.Span{74, 59, 75, 94},
				dvid.Span{75, 40, 75, 94}, dvid.Span{75, 41, 75, 94}, dvid.Span{75, 42, 75, 94}, dvid.Span{75, 43, 75, 94}, dvid.Span{75, 44, 75, 94},
				dvid.Span{75, 45, 75, 94}, dvid.Span{75, 46, 75, 94}, dvid.Span{75, 47, 75, 94}, dvid.Span{75, 48, 75, 94}, dvid.Span{75, 49, 75, 94},
				dvid.Span{75, 50, 75, 94}, dvid.Span{75, 51, 75, 94}, dvid.Span{75, 52, 75, 94}, dvid.Span{75, 53, 75, 94}, dvid.Span{75, 54, 75, 94},
				dvid.Span{75, 55, 75, 94}, dvid.Span{75, 56, 75, 94}, dvid.Span{75, 57, 75, 94}, dvid.Span{75, 58, 75, 94}, dvid.Span{75, 59, 75, 94},
				dvid.Span{76, 40, 75, 94}, dvid.Span{76, 41, 75, 94}, dvid.Span{76, 42, 75, 94}, dvid.Span{76, 43, 75, 94}, dvid.Span{76, 44, 75, 94},
				dvid.Span{76, 45, 75, 94}, dvid.Span{76, 46, 75, 94}, dvid.Span{76, 47, 75, 94}, dvid.Span{76, 48, 75, 94}, dvid.Span{76, 49, 75, 94},
				dvid.Span{76, 50, 75, 94}, dvid.Span{76, 51, 75, 94}, dvid.Span{76, 52, 75, 94}, dvid.Span{76, 53, 75, 94}, dvid.Span{76, 54, 75, 94},
				dvid.Span{76, 55, 75, 94}, dvid.Span{76, 56, 75, 94}, dvid.Span{76, 57, 75, 94}, dvid.Span{76, 58, 75, 94}, dvid.Span{76, 59, 75, 94},
				dvid.Span{77, 40, 75, 94}, dvid.Span{77, 41, 75, 94}, dvid.Span{77, 42, 75, 94}, dvid.Span{77, 43, 75, 94}, dvid.Span{77, 44, 75, 94},
				dvid.Span{77, 45, 75, 94}, dvid.Span{77, 46, 75, 94}, dvid.Span{77, 47, 75, 94}, dvid.Span{77, 48, 75, 94}, dvid.Span{77, 49, 75, 94},
				dvid.Span{77, 50, 75, 94}, dvid.Span{77, 51, 75, 94}, dvid.Span{77, 52, 75, 94}, dvid.Span{77, 53, 75, 94}, dvid.Span{77, 54, 75, 94},
				dvid.Span{77, 55, 75, 94}, dvid.Span{77, 56, 75, 94}, dvid.Span{77, 57, 75, 94}, dvid.Span{77, 58, 75, 94}, dvid.Span{77, 59, 75, 94},
				dvid.Span{78, 40, 75, 94}, dvid.Span{78, 41, 75, 94}, dvid.Span{78, 42, 75, 94}, dvid.Span{78, 43, 75, 94}, dvid.Span{78, 44, 75, 94},
				dvid.Span{78, 45, 75, 94}, dvid.Span{78, 46, 75, 94}, dvid.Span{78, 47, 75, 94}, dvid.Span{78, 48, 75, 94}, dvid.Span{78, 49, 75, 94},
				dvid.Span{78, 50, 75, 94}, dvid.Span{78, 51, 75, 94}, dvid.Span{78, 52, 75, 94}, dvid.Span{78, 53, 75, 94}, dvid.Span{78, 54, 75, 94},
				dvid.Span{78, 55, 75, 94}, dvid.Span{78, 56, 75, 94}, dvid.Span{78, 57, 75, 94}, dvid.Span{78, 58, 75, 94}, dvid.Span{78, 59, 75, 94},
				dvid.Span{79, 40, 75, 94}, dvid.Span{79, 41, 75, 94}, dvid.Span{79, 42, 75, 94}, dvid.Span{79, 43, 75, 94}, dvid.Span{79, 44, 75, 94},
				dvid.Span{79, 45, 75, 94}, dvid.Span{79, 46, 75, 94}, dvid.Span{79, 47, 75, 94}, dvid.Span{79, 48, 75, 94}, dvid.Span{79, 49, 75, 94},
				dvid.Span{79, 50, 75, 94}, dvid.Span{79, 51, 75, 94}, dvid.Span{79, 52, 75, 94}, dvid.Span{79, 53, 75, 94}, dvid.Span{79, 54, 75, 94},
				dvid.Span{79, 55, 75, 94}, dvid.Span{79, 56, 75, 94}, dvid.Span{79, 57, 75, 94}, dvid.Span{79, 58, 75, 94}, dvid.Span{79, 59, 75, 94},
				dvid.Span{80, 40, 75, 94}, dvid.Span{80, 41, 75, 94}, dvid.Span{80, 42, 75, 94}, dvid.Span{80, 43, 75, 94}, dvid.Span{80, 44, 75, 94},
				dvid.Span{80, 45, 75, 94}, dvid.Span{80, 46, 75, 94}, dvid.Span{80, 47, 75, 94}, dvid.Span{80, 48, 75, 94}, dvid.Span{80, 49, 75, 94},
				dvid.Span{80, 50, 75, 94}, dvid.Span{80, 51, 75, 94}, dvid.Span{80, 52, 75, 94}, dvid.Span{80, 53, 75, 94}, dvid.Span{80, 54, 75, 94},
				dvid.Span{80, 55, 75, 94}, dvid.Span{80, 56, 75, 94}, dvid.Span{80, 57, 75, 94}, dvid.Span{80, 58, 75, 94}, dvid.Span{80, 59, 75, 94},
				dvid.Span{81, 40, 75, 94}, dvid.Span{81, 41, 75, 94}, dvid.Span{81, 42, 75, 94}, dvid.Span{81, 43, 75, 94}, dvid.Span{81, 44, 75, 94},
				dvid.Span{81, 45, 75, 94}, dvid.Span{81, 46, 75, 94}, dvid.Span{81, 47, 75, 94}, dvid.Span{81, 48, 75, 94}, dvid.Span{81, 49, 75, 94},
				dvid.Span{81, 50, 75, 94}, dvid.Span{81, 51, 75, 94}, dvid.Span{81, 52, 75, 94}, dvid.Span{81, 53, 75, 94}, dvid.Span{81, 54, 75, 94},
				dvid.Span{81, 55, 75, 94}, dvid.Span{81, 56, 75, 94}, dvid.Span{81, 57, 75, 94}, dvid.Span{81, 58, 75, 94}, dvid.Span{81, 59, 75, 94},
				dvid.Span{82, 40, 75, 94}, dvid.Span{82, 41, 75, 94}, dvid.Span{82, 42, 75, 94}, dvid.Span{82, 43, 75, 94}, dvid.Span{82, 44, 75, 94},
				dvid.Span{82, 45, 75, 94}, dvid.Span{82, 46, 75, 94}, dvid.Span{82, 47, 75, 94}, dvid.Span{82, 48, 75, 94}, dvid.Span{82, 49, 75, 94},
				dvid.Span{82, 50, 75, 94}, dvid.Span{82, 51, 75, 94}, dvid.Span{82, 52, 75, 94}, dvid.Span{82, 53, 75, 94}, dvid.Span{82, 54, 75, 94},
				dvid.Span{82, 55, 75, 94}, dvid.Span{82, 56, 75, 94}, dvid.Span{82, 57, 75, 94}, dvid.Span{82, 58, 75, 94}, dvid.Span{82, 59, 75, 94},
				dvid.Span{83, 40, 75, 94}, dvid.Span{83, 41, 75, 94}, dvid.Span{83, 42, 75, 94}, dvid.Span{83, 43, 75, 94}, dvid.Span{83, 44, 75, 94},
				dvid.Span{83, 45, 75, 94}, dvid.Span{83, 46, 75, 94}, dvid.Span{83, 47, 75, 94}, dvid.Span{83, 48, 75, 94}, dvid.Span{83, 49, 75, 94},
				dvid.Span{83, 50, 75, 94}, dvid.Span{83, 51, 75, 94}, dvid.Span{83, 52, 75, 94}, dvid.Span{83, 53, 75, 94}, dvid.Span{83, 54, 75, 94},
				dvid.Span{83, 55, 75, 94}, dvid.Span{83, 56, 75, 94}, dvid.Span{83, 57, 75, 94}, dvid.Span{83, 58, 75, 94}, dvid.Span{83, 59, 75, 94},
				dvid.Span{84, 40, 75, 94}, dvid.Span{84, 41, 75, 94}, dvid.Span{84, 42, 75, 94}, dvid.Span{84, 43, 75, 94}, dvid.Span{84, 44, 75, 94},
				dvid.Span{84, 45, 75, 94}, dvid.Span{84, 46, 75, 94}, dvid.Span{84, 47, 75, 94}, dvid.Span{84, 48, 75, 94}, dvid.Span{84, 49, 75, 94},
				dvid.Span{84, 50, 75, 94}, dvid.Span{84, 51, 75, 94}, dvid.Span{84, 52, 75, 94}, dvid.Span{84, 53, 75, 94}, dvid.Span{84, 54, 75, 94},
				dvid.Span{84, 55, 75, 94}, dvid.Span{84, 56, 75, 94}, dvid.Span{84, 57, 75, 94}, dvid.Span{84, 58, 75, 94}, dvid.Span{84, 59, 75, 94},
				dvid.Span{85, 40, 75, 94}, dvid.Span{85, 41, 75, 94}, dvid.Span{85, 42, 75, 94}, dvid.Span{85, 43, 75, 94}, dvid.Span{85, 44, 75, 94},
				dvid.Span{85, 45, 75, 94}, dvid.Span{85, 46, 75, 94}, dvid.Span{85, 47, 75, 94}, dvid.Span{85, 48, 75, 94}, dvid.Span{85, 49, 75, 94},
				dvid.Span{85, 50, 75, 94}, dvid.Span{85, 51, 75, 94}, dvid.Span{85, 52, 75, 94}, dvid.Span{85, 53, 75, 94}, dvid.Span{85, 54, 75, 94},
				dvid.Span{85, 55, 75, 94}, dvid.Span{85, 56, 75, 94}, dvid.Span{85, 57, 75, 94}, dvid.Span{85, 58, 75, 94}, dvid.Span{85, 59, 75, 94},
				dvid.Span{86, 40, 75, 94}, dvid.Span{86, 41, 75, 94}, dvid.Span{86, 42, 75, 94}, dvid.Span{86, 43, 75, 94}, dvid.Span{86, 44, 75, 94},
				dvid.Span{86, 45, 75, 94}, dvid.Span{86, 46, 75, 94}, dvid.Span{86, 47, 75, 94}, dvid.Span{86, 48, 75, 94}, dvid.Span{86, 49, 75, 94},
				dvid.Span{86, 50, 75, 94}, dvid.Span{86, 51, 75, 94}, dvid.Span{86, 52, 75, 94}, dvid.Span{86, 53, 75, 94}, dvid.Span{86, 54, 75, 94},
				dvid.Span{86, 55, 75, 94}, dvid.Span{86, 56, 75, 94}, dvid.Span{86, 57, 75, 94}, dvid.Span{86, 58, 75, 94}, dvid.Span{86, 59, 75, 94},
				dvid.Span{87, 40, 75, 94}, dvid.Span{87, 41, 75, 94}, dvid.Span{87, 42, 75, 94}, dvid.Span{87, 43, 75, 94}, dvid.Span{87, 44, 75, 94},
				dvid.Span{87, 45, 75, 94}, dvid.Span{87, 46, 75, 94}, dvid.Span{87, 47, 75, 94}, dvid.Span{87, 48, 75, 94}, dvid.Span{87, 49, 75, 94},
				dvid.Span{87, 50, 75, 94}, dvid.Span{87, 51, 75, 94}, dvid.Span{87, 52, 75, 94}, dvid.Span{87, 53, 75, 94}, dvid.Span{87, 54, 75, 94},
				dvid.Span{87, 55, 75, 94}, dvid.Span{87, 56, 75, 94}, dvid.Span{87, 57, 75, 94}, dvid.Span{87, 58, 75, 94}, dvid.Span{87, 59, 75, 94},
				dvid.Span{88, 40, 75, 94}, dvid.Span{88, 41, 75, 94}, dvid.Span{88, 42, 75, 94}, dvid.Span{88, 43, 75, 94}, dvid.Span{88, 44, 75, 94},
				dvid.Span{88, 45, 75, 94}, dvid.Span{88, 46, 75, 94}, dvid.Span{88, 47, 75, 94}, dvid.Span{88, 48, 75, 94}, dvid.Span{88, 49, 75, 94},
				dvid.Span{88, 50, 75, 94}, dvid.Span{88, 51, 75, 94}, dvid.Span{88, 52, 75, 94}, dvid.Span{88, 53, 75, 94}, dvid.Span{88, 54, 75, 94},
				dvid.Span{88, 55, 75, 94}, dvid.Span{88, 56, 75, 94}, dvid.Span{88, 57, 75, 94}, dvid.Span{88, 58, 75, 94}, dvid.Span{88, 59, 75, 94},
				dvid.Span{89, 40, 75, 94}, dvid.Span{89, 41, 75, 94}, dvid.Span{89, 42, 75, 94}, dvid.Span{89, 43, 75, 94}, dvid.Span{89, 44, 75, 94},
				dvid.Span{89, 45, 75, 94}, dvid.Span{89, 46, 75, 94}, dvid.Span{89, 47, 75, 94}, dvid.Span{89, 48, 75, 94}, dvid.Span{89, 49, 75, 94},
				dvid.Span{89, 50, 75, 94}, dvid.Span{89, 51, 75, 94}, dvid.Span{89, 52, 75, 94}, dvid.Span{89, 53, 75, 94}, dvid.Span{89, 54, 75, 94},
				dvid.Span{89, 55, 75, 94}, dvid.Span{89, 56, 75, 94}, dvid.Span{89, 57, 75, 94}, dvid.Span{89, 58, 75, 94}, dvid.Span{89, 59, 75, 94},
			},
		}, {
			label:  4,
			offset: dvid.Point3d{75, 40, 60},
			size:   dvid.Point3d{20, 20, 21},
			blockSpans: []dvid.Span{
				dvid.Span{1, 1, 2, 2},
				dvid.Span{2, 1, 2, 2},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{60, 40, 75, 94}, dvid.Span{60, 41, 75, 94}, dvid.Span{60, 42, 75, 94}, dvid.Span{60, 43, 75, 94}, dvid.Span{60, 44, 75, 94},
				dvid.Span{60, 45, 75, 94}, dvid.Span{60, 46, 75, 94}, dvid.Span{60, 47, 75, 94}, dvid.Span{60, 48, 75, 94}, dvid.Span{60, 49, 75, 94},
				dvid.Span{60, 50, 75, 94}, dvid.Span{60, 51, 75, 94}, dvid.Span{60, 52, 75, 94}, dvid.Span{60, 53, 75, 94}, dvid.Span{60, 54, 75, 94},
				dvid.Span{60, 55, 75, 94}, dvid.Span{60, 56, 75, 94}, dvid.Span{60, 57, 75, 94}, dvid.Span{60, 58, 75, 94}, dvid.Span{60, 59, 75, 94},
				dvid.Span{61, 40, 75, 94}, dvid.Span{61, 41, 75, 94}, dvid.Span{61, 42, 75, 94}, dvid.Span{61, 43, 75, 94}, dvid.Span{61, 44, 75, 94},
				dvid.Span{61, 45, 75, 94}, dvid.Span{61, 46, 75, 94}, dvid.Span{61, 47, 75, 94}, dvid.Span{61, 48, 75, 94}, dvid.Span{61, 49, 75, 94},
				dvid.Span{61, 50, 75, 94}, dvid.Span{61, 51, 75, 94}, dvid.Span{61, 52, 75, 94}, dvid.Span{61, 53, 75, 94}, dvid.Span{61, 54, 75, 94},
				dvid.Span{61, 55, 75, 94}, dvid.Span{61, 56, 75, 94}, dvid.Span{61, 57, 75, 94}, dvid.Span{61, 58, 75, 94}, dvid.Span{61, 59, 75, 94},
				dvid.Span{62, 40, 75, 94}, dvid.Span{62, 41, 75, 94}, dvid.Span{62, 42, 75, 94}, dvid.Span{62, 43, 75, 94}, dvid.Span{62, 44, 75, 94},
				dvid.Span{62, 45, 75, 94}, dvid.Span{62, 46, 75, 94}, dvid.Span{62, 47, 75, 94}, dvid.Span{62, 48, 75, 94}, dvid.Span{62, 49, 75, 94},
				dvid.Span{62, 50, 75, 94}, dvid.Span{62, 51, 75, 94}, dvid.Span{62, 52, 75, 94}, dvid.Span{62, 53, 75, 94}, dvid.Span{62, 54, 75, 94},
				dvid.Span{62, 55, 75, 94}, dvid.Span{62, 56, 75, 94}, dvid.Span{62, 57, 75, 94}, dvid.Span{62, 58, 75, 94}, dvid.Span{62, 59, 75, 94},
				dvid.Span{63, 40, 75, 94}, dvid.Span{63, 41, 75, 94}, dvid.Span{63, 42, 75, 94}, dvid.Span{63, 43, 75, 94}, dvid.Span{63, 44, 75, 94},
				dvid.Span{63, 45, 75, 94}, dvid.Span{63, 46, 75, 94}, dvid.Span{63, 47, 75, 94}, dvid.Span{63, 48, 75, 94}, dvid.Span{63, 49, 75, 94},
				dvid.Span{63, 50, 75, 94}, dvid.Span{63, 51, 75, 94}, dvid.Span{63, 52, 75, 94}, dvid.Span{63, 53, 75, 94}, dvid.Span{63, 54, 75, 94},
				dvid.Span{63, 55, 75, 94}, dvid.Span{63, 56, 75, 94}, dvid.Span{63, 57, 75, 94}, dvid.Span{63, 58, 75, 94}, dvid.Span{63, 59, 75, 94},
				dvid.Span{64, 40, 75, 94}, dvid.Span{64, 41, 75, 94}, dvid.Span{64, 42, 75, 94}, dvid.Span{64, 43, 75, 94}, dvid.Span{64, 44, 75, 94},
				dvid.Span{64, 45, 75, 94}, dvid.Span{64, 46, 75, 94}, dvid.Span{64, 47, 75, 94}, dvid.Span{64, 48, 75, 94}, dvid.Span{64, 49, 75, 94},
				dvid.Span{64, 50, 75, 94}, dvid.Span{64, 51, 75, 94}, dvid.Span{64, 52, 75, 94}, dvid.Span{64, 53, 75, 94}, dvid.Span{64, 54, 75, 94},
				dvid.Span{64, 55, 75, 94}, dvid.Span{64, 56, 75, 94}, dvid.Span{64, 57, 75, 94}, dvid.Span{64, 58, 75, 94}, dvid.Span{64, 59, 75, 94},
				dvid.Span{65, 40, 75, 94}, dvid.Span{65, 41, 75, 94}, dvid.Span{65, 42, 75, 94}, dvid.Span{65, 43, 75, 94}, dvid.Span{65, 44, 75, 94},
				dvid.Span{65, 45, 75, 94}, dvid.Span{65, 46, 75, 94}, dvid.Span{65, 47, 75, 94}, dvid.Span{65, 48, 75, 94}, dvid.Span{65, 49, 75, 94},
				dvid.Span{65, 50, 75, 94}, dvid.Span{65, 51, 75, 94}, dvid.Span{65, 52, 75, 94}, dvid.Span{65, 53, 75, 94}, dvid.Span{65, 54, 75, 94},
				dvid.Span{65, 55, 75, 94}, dvid.Span{65, 56, 75, 94}, dvid.Span{65, 57, 75, 94}, dvid.Span{65, 58, 75, 94}, dvid.Span{65, 59, 75, 94},
				dvid.Span{66, 40, 75, 94}, dvid.Span{66, 41, 75, 94}, dvid.Span{66, 42, 75, 94}, dvid.Span{66, 43, 75, 94}, dvid.Span{66, 44, 75, 94},
				dvid.Span{66, 45, 75, 94}, dvid.Span{66, 46, 75, 94}, dvid.Span{66, 47, 75, 94}, dvid.Span{66, 48, 75, 94}, dvid.Span{66, 49, 75, 94},
				dvid.Span{66, 50, 75, 94}, dvid.Span{66, 51, 75, 94}, dvid.Span{66, 52, 75, 94}, dvid.Span{66, 53, 75, 94}, dvid.Span{66, 54, 75, 94},
				dvid.Span{66, 55, 75, 94}, dvid.Span{66, 56, 75, 94}, dvid.Span{66, 57, 75, 94}, dvid.Span{66, 58, 75, 94}, dvid.Span{66, 59, 75, 94},
				dvid.Span{67, 40, 75, 94}, dvid.Span{67, 41, 75, 94}, dvid.Span{67, 42, 75, 94}, dvid.Span{67, 43, 75, 94}, dvid.Span{67, 44, 75, 94},
				dvid.Span{67, 45, 75, 94}, dvid.Span{67, 46, 75, 94}, dvid.Span{67, 47, 75, 94}, dvid.Span{67, 48, 75, 94}, dvid.Span{67, 49, 75, 94},
				dvid.Span{67, 50, 75, 94}, dvid.Span{67, 51, 75, 94}, dvid.Span{67, 52, 75, 94}, dvid.Span{67, 53, 75, 94}, dvid.Span{67, 54, 75, 94},
				dvid.Span{67, 55, 75, 94}, dvid.Span{67, 56, 75, 94}, dvid.Span{67, 57, 75, 94}, dvid.Span{67, 58, 75, 94}, dvid.Span{67, 59, 75, 94},
				dvid.Span{68, 40, 75, 94}, dvid.Span{68, 41, 75, 94}, dvid.Span{68, 42, 75, 94}, dvid.Span{68, 43, 75, 94}, dvid.Span{68, 44, 75, 94},
				dvid.Span{68, 45, 75, 94}, dvid.Span{68, 46, 75, 94}, dvid.Span{68, 47, 75, 94}, dvid.Span{68, 48, 75, 94}, dvid.Span{68, 49, 75, 94},
				dvid.Span{68, 50, 75, 94}, dvid.Span{68, 51, 75, 94}, dvid.Span{68, 52, 75, 94}, dvid.Span{68, 53, 75, 94}, dvid.Span{68, 54, 75, 94},
				dvid.Span{68, 55, 75, 94}, dvid.Span{68, 56, 75, 94}, dvid.Span{68, 57, 75, 94}, dvid.Span{68, 58, 75, 94}, dvid.Span{68, 59, 75, 94},
				dvid.Span{69, 40, 75, 94}, dvid.Span{69, 41, 75, 94}, dvid.Span{69, 42, 75, 94}, dvid.Span{69, 43, 75, 94}, dvid.Span{69, 44, 75, 94},
				dvid.Span{69, 45, 75, 94}, dvid.Span{69, 46, 75, 94}, dvid.Span{69, 47, 75, 94}, dvid.Span{69, 48, 75, 94}, dvid.Span{69, 49, 75, 94},
				dvid.Span{69, 50, 75, 94}, dvid.Span{69, 51, 75, 94}, dvid.Span{69, 52, 75, 94}, dvid.Span{69, 53, 75, 94}, dvid.Span{69, 54, 75, 94},
				dvid.Span{69, 55, 75, 94}, dvid.Span{69, 56, 75, 94}, dvid.Span{69, 57, 75, 94}, dvid.Span{69, 58, 75, 94}, dvid.Span{69, 59, 75, 94},
				dvid.Span{70, 40, 75, 94}, dvid.Span{70, 41, 75, 94}, dvid.Span{70, 42, 75, 94}, dvid.Span{70, 43, 75, 94}, dvid.Span{70, 44, 75, 94},
				dvid.Span{70, 45, 75, 94}, dvid.Span{70, 46, 75, 94}, dvid.Span{70, 47, 75, 94}, dvid.Span{70, 48, 75, 94}, dvid.Span{70, 49, 75, 94},
				dvid.Span{70, 50, 75, 94}, dvid.Span{70, 51, 75, 94}, dvid.Span{70, 52, 75, 94}, dvid.Span{70, 53, 75, 94}, dvid.Span{70, 54, 75, 94},
				dvid.Span{70, 55, 75, 94}, dvid.Span{70, 56, 75, 94}, dvid.Span{70, 57, 75, 94}, dvid.Span{70, 58, 75, 94}, dvid.Span{70, 59, 75, 94},
				dvid.Span{71, 40, 75, 94}, dvid.Span{71, 41, 75, 94}, dvid.Span{71, 42, 75, 94}, dvid.Span{71, 43, 75, 94}, dvid.Span{71, 44, 75, 94},
				dvid.Span{71, 45, 75, 94}, dvid.Span{71, 46, 75, 94}, dvid.Span{71, 47, 75, 94}, dvid.Span{71, 48, 75, 94}, dvid.Span{71, 49, 75, 94},
				dvid.Span{71, 50, 75, 94}, dvid.Span{71, 51, 75, 94}, dvid.Span{71, 52, 75, 94}, dvid.Span{71, 53, 75, 94}, dvid.Span{71, 54, 75, 94},
				dvid.Span{71, 55, 75, 94}, dvid.Span{71, 56, 75, 94}, dvid.Span{71, 57, 75, 94}, dvid.Span{71, 58, 75, 94}, dvid.Span{71, 59, 75, 94},
				dvid.Span{72, 40, 75, 94}, dvid.Span{72, 41, 75, 94}, dvid.Span{72, 42, 75, 94}, dvid.Span{72, 43, 75, 94}, dvid.Span{72, 44, 75, 94},
				dvid.Span{72, 45, 75, 94}, dvid.Span{72, 46, 75, 94}, dvid.Span{72, 47, 75, 94}, dvid.Span{72, 48, 75, 94}, dvid.Span{72, 49, 75, 94},
				dvid.Span{72, 50, 75, 94}, dvid.Span{72, 51, 75, 94}, dvid.Span{72, 52, 75, 94}, dvid.Span{72, 53, 75, 94}, dvid.Span{72, 54, 75, 94},
				dvid.Span{72, 55, 75, 94}, dvid.Span{72, 56, 75, 94}, dvid.Span{72, 57, 75, 94}, dvid.Span{72, 58, 75, 94}, dvid.Span{72, 59, 75, 94},
				dvid.Span{73, 40, 75, 94}, dvid.Span{73, 41, 75, 94}, dvid.Span{73, 42, 75, 94}, dvid.Span{73, 43, 75, 94}, dvid.Span{73, 44, 75, 94},
				dvid.Span{73, 45, 75, 94}, dvid.Span{73, 46, 75, 94}, dvid.Span{73, 47, 75, 94}, dvid.Span{73, 48, 75, 94}, dvid.Span{73, 49, 75, 94},
				dvid.Span{73, 50, 75, 94}, dvid.Span{73, 51, 75, 94}, dvid.Span{73, 52, 75, 94}, dvid.Span{73, 53, 75, 94}, dvid.Span{73, 54, 75, 94},
				dvid.Span{73, 55, 75, 94}, dvid.Span{73, 56, 75, 94}, dvid.Span{73, 57, 75, 94}, dvid.Span{73, 58, 75, 94}, dvid.Span{73, 59, 75, 94},
				dvid.Span{74, 40, 75, 94}, dvid.Span{74, 41, 75, 94}, dvid.Span{74, 42, 75, 94}, dvid.Span{74, 43, 75, 94}, dvid.Span{74, 44, 75, 94},
				dvid.Span{74, 45, 75, 94}, dvid.Span{74, 46, 75, 94}, dvid.Span{74, 47, 75, 94}, dvid.Span{74, 48, 75, 94}, dvid.Span{74, 49, 75, 94},
				dvid.Span{74, 50, 75, 94}, dvid.Span{74, 51, 75, 94}, dvid.Span{74, 52, 75, 94}, dvid.Span{74, 53, 75, 94}, dvid.Span{74, 54, 75, 94},
				dvid.Span{74, 55, 75, 94}, dvid.Span{74, 56, 75, 94}, dvid.Span{74, 57, 75, 94}, dvid.Span{74, 58, 75, 94}, dvid.Span{74, 59, 75, 94},
				dvid.Span{75, 40, 75, 94}, dvid.Span{75, 41, 75, 94}, dvid.Span{75, 42, 75, 94}, dvid.Span{75, 43, 75, 94}, dvid.Span{75, 44, 75, 94},
				dvid.Span{75, 45, 75, 94}, dvid.Span{75, 46, 75, 94}, dvid.Span{75, 47, 75, 94}, dvid.Span{75, 48, 75, 94}, dvid.Span{75, 49, 75, 94},
				dvid.Span{75, 50, 75, 94}, dvid.Span{75, 51, 75, 94}, dvid.Span{75, 52, 75, 94}, dvid.Span{75, 53, 75, 94}, dvid.Span{75, 54, 75, 94},
				dvid.Span{75, 55, 75, 94}, dvid.Span{75, 56, 75, 94}, dvid.Span{75, 57, 75, 94}, dvid.Span{75, 58, 75, 94}, dvid.Span{75, 59, 75, 94},
				dvid.Span{76, 40, 75, 94}, dvid.Span{76, 41, 75, 94}, dvid.Span{76, 42, 75, 94}, dvid.Span{76, 43, 75, 94}, dvid.Span{76, 44, 75, 94},
				dvid.Span{76, 45, 75, 94}, dvid.Span{76, 46, 75, 94}, dvid.Span{76, 47, 75, 94}, dvid.Span{76, 48, 75, 94}, dvid.Span{76, 49, 75, 94},
				dvid.Span{76, 50, 75, 94}, dvid.Span{76, 51, 75, 94}, dvid.Span{76, 52, 75, 94}, dvid.Span{76, 53, 75, 94}, dvid.Span{76, 54, 75, 94},
				dvid.Span{76, 55, 75, 94}, dvid.Span{76, 56, 75, 94}, dvid.Span{76, 57, 75, 94}, dvid.Span{76, 58, 75, 94}, dvid.Span{76, 59, 75, 94},
				dvid.Span{77, 40, 75, 94}, dvid.Span{77, 41, 75, 94}, dvid.Span{77, 42, 75, 94}, dvid.Span{77, 43, 75, 94}, dvid.Span{77, 44, 75, 94},
				dvid.Span{77, 45, 75, 94}, dvid.Span{77, 46, 75, 94}, dvid.Span{77, 47, 75, 94}, dvid.Span{77, 48, 75, 94}, dvid.Span{77, 49, 75, 94},
				dvid.Span{77, 50, 75, 94}, dvid.Span{77, 51, 75, 94}, dvid.Span{77, 52, 75, 94}, dvid.Span{77, 53, 75, 94}, dvid.Span{77, 54, 75, 94},
				dvid.Span{77, 55, 75, 94}, dvid.Span{77, 56, 75, 94}, dvid.Span{77, 57, 75, 94}, dvid.Span{77, 58, 75, 94}, dvid.Span{77, 59, 75, 94},
				dvid.Span{78, 40, 75, 94}, dvid.Span{78, 41, 75, 94}, dvid.Span{78, 42, 75, 94}, dvid.Span{78, 43, 75, 94}, dvid.Span{78, 44, 75, 94},
				dvid.Span{78, 45, 75, 94}, dvid.Span{78, 46, 75, 94}, dvid.Span{78, 47, 75, 94}, dvid.Span{78, 48, 75, 94}, dvid.Span{78, 49, 75, 94},
				dvid.Span{78, 50, 75, 94}, dvid.Span{78, 51, 75, 94}, dvid.Span{78, 52, 75, 94}, dvid.Span{78, 53, 75, 94}, dvid.Span{78, 54, 75, 94},
				dvid.Span{78, 55, 75, 94}, dvid.Span{78, 56, 75, 94}, dvid.Span{78, 57, 75, 94}, dvid.Span{78, 58, 75, 94}, dvid.Span{78, 59, 75, 94},
				dvid.Span{79, 40, 75, 94}, dvid.Span{79, 41, 75, 94}, dvid.Span{79, 42, 75, 94}, dvid.Span{79, 43, 75, 94}, dvid.Span{79, 44, 75, 94},
				dvid.Span{79, 45, 75, 94}, dvid.Span{79, 46, 75, 94}, dvid.Span{79, 47, 75, 94}, dvid.Span{79, 48, 75, 94}, dvid.Span{79, 49, 75, 94},
				dvid.Span{79, 50, 75, 94}, dvid.Span{79, 51, 75, 94}, dvid.Span{79, 52, 75, 94}, dvid.Span{79, 53, 75, 94}, dvid.Span{79, 54, 75, 94},
				dvid.Span{79, 55, 75, 94}, dvid.Span{79, 56, 75, 94}, dvid.Span{79, 57, 75, 94}, dvid.Span{79, 58, 75, 94}, dvid.Span{79, 59, 75, 94},
				dvid.Span{80, 40, 75, 80}, dvid.Span{80, 40, 87, 89}, dvid.Span{80, 40, 93, 94},
			},
		}, {
			label:  5,
			offset: dvid.Point3d{75, 40, 80},
			size:   dvid.Point3d{20, 20, 10},
			blockSpans: []dvid.Span{
				dvid.Span{2, 1, 2, 2},
			},
			voxelSpans: []dvid.Span{
				dvid.Span{80, 40, 81, 86}, dvid.Span{80, 40, 90, 92}, // These first 2 test splits interleaved in one span.
				dvid.Span{80, 41, 75, 94}, dvid.Span{80, 42, 75, 94}, dvid.Span{80, 43, 75, 94}, dvid.Span{80, 44, 75, 94},
				dvid.Span{80, 45, 75, 94}, dvid.Span{80, 46, 75, 94}, dvid.Span{80, 47, 75, 94}, dvid.Span{80, 48, 75, 94}, dvid.Span{80, 49, 75, 94},
				dvid.Span{80, 50, 75, 94}, dvid.Span{80, 51, 75, 94}, dvid.Span{80, 52, 75, 94}, dvid.Span{80, 53, 75, 94}, dvid.Span{80, 54, 75, 94},
				dvid.Span{80, 55, 75, 94}, dvid.Span{80, 56, 75, 94}, dvid.Span{80, 57, 75, 94}, dvid.Span{80, 58, 75, 94}, dvid.Span{80, 59, 75, 94},
				dvid.Span{81, 40, 75, 94}, dvid.Span{81, 41, 75, 94}, dvid.Span{81, 42, 75, 94}, dvid.Span{81, 43, 75, 94}, dvid.Span{81, 44, 75, 94},
				dvid.Span{81, 45, 75, 94}, dvid.Span{81, 46, 75, 94}, dvid.Span{81, 47, 75, 94}, dvid.Span{81, 48, 75, 94}, dvid.Span{81, 49, 75, 94},
				dvid.Span{81, 50, 75, 94}, dvid.Span{81, 51, 75, 94}, dvid.Span{81, 52, 75, 94}, dvid.Span{81, 53, 75, 94}, dvid.Span{81, 54, 75, 94},
				dvid.Span{81, 55, 75, 94}, dvid.Span{81, 56, 75, 94}, dvid.Span{81, 57, 75, 94}, dvid.Span{81, 58, 75, 94}, dvid.Span{81, 59, 75, 94},
				dvid.Span{82, 40, 75, 94}, dvid.Span{82, 41, 75, 94}, dvid.Span{82, 42, 75, 94}, dvid.Span{82, 43, 75, 94}, dvid.Span{82, 44, 75, 94},
				dvid.Span{82, 45, 75, 94}, dvid.Span{82, 46, 75, 94}, dvid.Span{82, 47, 75, 94}, dvid.Span{82, 48, 75, 94}, dvid.Span{82, 49, 75, 94},
				dvid.Span{82, 50, 75, 94}, dvid.Span{82, 51, 75, 94}, dvid.Span{82, 52, 75, 94}, dvid.Span{82, 53, 75, 94}, dvid.Span{82, 54, 75, 94},
				dvid.Span{82, 55, 75, 94}, dvid.Span{82, 56, 75, 94}, dvid.Span{82, 57, 75, 94}, dvid.Span{82, 58, 75, 94}, dvid.Span{82, 59, 75, 94},
				dvid.Span{83, 40, 75, 94}, dvid.Span{83, 41, 75, 94}, dvid.Span{83, 42, 75, 94}, dvid.Span{83, 43, 75, 94}, dvid.Span{83, 44, 75, 94},
				dvid.Span{83, 45, 75, 94}, dvid.Span{83, 46, 75, 94}, dvid.Span{83, 47, 75, 94}, dvid.Span{83, 48, 75, 94}, dvid.Span{83, 49, 75, 94},
				dvid.Span{83, 50, 75, 94}, dvid.Span{83, 51, 75, 94}, dvid.Span{83, 52, 75, 94}, dvid.Span{83, 53, 75, 94}, dvid.Span{83, 54, 75, 94},
				dvid.Span{83, 55, 75, 94}, dvid.Span{83, 56, 75, 94}, dvid.Span{83, 57, 75, 94}, dvid.Span{83, 58, 75, 94}, dvid.Span{83, 59, 75, 94},
				dvid.Span{84, 40, 75, 94}, dvid.Span{84, 41, 75, 94}, dvid.Span{84, 42, 75, 94}, dvid.Span{84, 43, 75, 94}, dvid.Span{84, 44, 75, 94},
				dvid.Span{84, 45, 75, 94}, dvid.Span{84, 46, 75, 94}, dvid.Span{84, 47, 75, 94}, dvid.Span{84, 48, 75, 94}, dvid.Span{84, 49, 75, 94},
				dvid.Span{84, 50, 75, 94}, dvid.Span{84, 51, 75, 94}, dvid.Span{84, 52, 75, 94}, dvid.Span{84, 53, 75, 94}, dvid.Span{84, 54, 75, 94},
				dvid.Span{84, 55, 75, 94}, dvid.Span{84, 56, 75, 94}, dvid.Span{84, 57, 75, 94}, dvid.Span{84, 58, 75, 94}, dvid.Span{84, 59, 75, 94},
				dvid.Span{85, 40, 75, 94}, dvid.Span{85, 41, 75, 94}, dvid.Span{85, 42, 75, 94}, dvid.Span{85, 43, 75, 94}, dvid.Span{85, 44, 75, 94},
				dvid.Span{85, 45, 75, 94}, dvid.Span{85, 46, 75, 94}, dvid.Span{85, 47, 75, 94}, dvid.Span{85, 48, 75, 94}, dvid.Span{85, 49, 75, 94},
				dvid.Span{85, 50, 75, 94}, dvid.Span{85, 51, 75, 94}, dvid.Span{85, 52, 75, 94}, dvid.Span{85, 53, 75, 94}, dvid.Span{85, 54, 75, 94},
				dvid.Span{85, 55, 75, 94}, dvid.Span{85, 56, 75, 94}, dvid.Span{85, 57, 75, 94}, dvid.Span{85, 58, 75, 94}, dvid.Span{85, 59, 75, 94},
				dvid.Span{86, 40, 75, 94}, dvid.Span{86, 41, 75, 94}, dvid.Span{86, 42, 75, 94}, dvid.Span{86, 43, 75, 94}, dvid.Span{86, 44, 75, 94},
				dvid.Span{86, 45, 75, 94}, dvid.Span{86, 46, 75, 94}, dvid.Span{86, 47, 75, 94}, dvid.Span{86, 48, 75, 94}, dvid.Span{86, 49, 75, 94},
				dvid.Span{86, 50, 75, 94}, dvid.Span{86, 51, 75, 94}, dvid.Span{86, 52, 75, 94}, dvid.Span{86, 53, 75, 94}, dvid.Span{86, 54, 75, 94},
				dvid.Span{86, 55, 75, 94}, dvid.Span{86, 56, 75, 94}, dvid.Span{86, 57, 75, 94}, dvid.Span{86, 58, 75, 94}, dvid.Span{86, 59, 75, 94},
				dvid.Span{87, 40, 75, 94}, dvid.Span{87, 41, 75, 94}, dvid.Span{87, 42, 75, 94}, dvid.Span{87, 43, 75, 94}, dvid.Span{87, 44, 75, 94},
				dvid.Span{87, 45, 75, 94}, dvid.Span{87, 46, 75, 94}, dvid.Span{87, 47, 75, 94}, dvid.Span{87, 48, 75, 94}, dvid.Span{87, 49, 75, 94},
				dvid.Span{87, 50, 75, 94}, dvid.Span{87, 51, 75, 94}, dvid.Span{87, 52, 75, 94}, dvid.Span{87, 53, 75, 94}, dvid.Span{87, 54, 75, 94},
				dvid.Span{87, 55, 75, 94}, dvid.Span{87, 56, 75, 94}, dvid.Span{87, 57, 75, 94}, dvid.Span{87, 58, 75, 94}, dvid.Span{87, 59, 75, 94},
				dvid.Span{88, 40, 75, 94}, dvid.Span{88, 41, 75, 94}, dvid.Span{88, 42, 75, 94}, dvid.Span{88, 43, 75, 94}, dvid.Span{88, 44, 75, 94},
				dvid.Span{88, 45, 75, 94}, dvid.Span{88, 46, 75, 94}, dvid.Span{88, 47, 75, 94}, dvid.Span{88, 48, 75, 94}, dvid.Span{88, 49, 75, 94},
				dvid.Span{88, 50, 75, 94}, dvid.Span{88, 51, 75, 94}, dvid.Span{88, 52, 75, 94}, dvid.Span{88, 53, 75, 94}, dvid.Span{88, 54, 75, 94},
				dvid.Span{88, 55, 75, 94}, dvid.Span{88, 56, 75, 94}, dvid.Span{88, 57, 75, 94}, dvid.Span{88, 58, 75, 94}, dvid.Span{88, 59, 75, 94},
				dvid.Span{89, 40, 75, 94}, dvid.Span{89, 41, 75, 94}, dvid.Span{89, 42, 75, 94}, dvid.Span{89, 43, 75, 94}, dvid.Span{89, 44, 75, 94},
				dvid.Span{89, 45, 75, 94}, dvid.Span{89, 46, 75, 94}, dvid.Span{89, 47, 75, 94}, dvid.Span{89, 48, 75, 94}, dvid.Span{89, 49, 75, 94},
				dvid.Span{89, 50, 75, 94}, dvid.Span{89, 51, 75, 94}, dvid.Span{89, 52, 75, 94}, dvid.Span{89, 53, 75, 94}, dvid.Span{89, 54, 75, 94},
				dvid.Span{89, 55, 75, 94}, dvid.Span{89, 56, 75, 94}, dvid.Span{89, 57, 75, 94}, dvid.Span{89, 58, 75, 94}, dvid.Span{89, 59, 75, 94},
			},
		},
	}
	body1     = bodies[0]
	body2     = bodies[1]
	body3     = bodies[2]
	body4     = bodies[3]
	bodyleft  = bodies[4]
	bodysplit = bodies[5]
)
