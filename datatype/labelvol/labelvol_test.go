// Tests sparsevol variants and merge/split.
// Test body data is at end of file.
// TODO: Remove hardwiring of tests to assume block size = 32, including data.

package labelvol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

var (
	labelsT   datastore.TypeService
	labelvolT datastore.TypeService
	testMu    sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	var err error
	if labelsT == nil {
		labelsT, err = datastore.TypeServiceByName("labelblk")
		if err != nil {
			log.Fatalf("Can't get labelblk type: %v\n", err)
		}
	}
	if labelvolT == nil {
		labelvolT, err = datastore.TypeServiceByName("labelvol")
		if err != nil {
			log.Fatalf("Can't get labelvol type: %v\n", err)
		}
	}
	return datastore.NewTestRepo()
}

// A single label block within the volume
type testBody struct {
	label        uint64
	offset, size dvid.Point3d
	blockSpans   dvid.Spans
	voxelSpans   dvid.Spans
}

var emptyBody = testBody{
	label:      0,
	offset:     dvid.Point3d{},
	size:       dvid.Point3d{},
	blockSpans: dvid.Spans{},
	voxelSpans: dvid.Spans{},
}

// Makes sure the coarse sparse volume encoding matches the body.
func (b testBody) checkCoarse(t *testing.T, encoding []byte) {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Errorf("Error in decoding coarse sparse volume: %v\n", err)
		return
	}

	// Check those spans match the body voxels.
	if !reflect.DeepEqual(spans, b.blockSpans) {
		t.Errorf("Expected spans for label %d:\n%s\nGot spans:\n%s\n", b.label, b.blockSpans, spans)
	}
}

// Makes sure the sparse volume encoding matches the actual body voxels.
func (b testBody) checkSparseVol(t *testing.T, encoding []byte, bounds dvid.OptionalBounds) {
	if len(encoding) < 12 {
		t.Fatalf("Bad encoded sparsevol received.  Only %d bytes\n", len(encoding))
	}

	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Fatalf("Error in decoding sparse volume: %v\n", err)
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
	gotNorm := spans.Normalize()
	expectNorm := expected.Normalize()
	if !reflect.DeepEqual(gotNorm, expectNorm) {
		for _, got := range gotNorm {
			bad := true
			for _, expect := range expectNorm {
				if reflect.DeepEqual(got, expect) {
					bad = false
				}
			}
			if bad {
				fmt.Printf("Got unexpected span: %s\n", got)
			}
		}
		for _, expect := range expectNorm {
			bad := true
			for _, got := range gotNorm {
				if reflect.DeepEqual(got, expect) {
					bad = false
				}
			}
			if bad {
				fmt.Printf("Never got expected span: %s\n", expect)
			}
		}
		t.Errorf("Expected spans for label %d:\n%s\nGot spans:\n%s\nAfter Norm:%s\n", b.label, expectNorm, spans, gotNorm)
	}
}

// Sees if the given block span has any of this test body label in it.
func (b testBody) isDeleted(t *testing.T, encoding []byte, bspan dvid.Span) bool {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Fatalf("Error in decoding sparse volume: %v\n", err)
		return false
	}

	// Iterate true spans to see if any are in the blocks given.
	for _, span := range spans {
		bx0 := span[2] / 32
		bx1 := span[3] / 32
		by := span[1] / 32
		bz := span[0] / 32

		within_x := (bx0 >= bspan[2] && bx0 <= bspan[3]) || (bx1 >= bspan[2] && bx1 <= bspan[3])
		if bz == bspan[0] && by == bspan[1] && within_x {
			return false
		}
	}
	return true
}

func checkSpans(t *testing.T, encoding []byte, minx, maxx int32) {
	// Get to the  # spans and RLE in encoding
	spansEncoding := encoding[8:]
	var spans dvid.Spans
	if err := spans.UnmarshalBinary(spansEncoding); err != nil {
		t.Errorf("Error in decoding coarse sparse volume: %v\n", err)
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

// Sets voxels in body to given label.
func (v *testVolume) addBody(body testBody, label uint64) {
	nx := v.size[0]
	nxy := nx * v.size[1]
	for _, span := range body.voxelSpans {
		z, y, x0, x1 := span.Unpack()
		p := (z*nxy + y*nx) * 8
		for i := p + x0*8; i <= p+x1*8; i += 8 {
			binary.LittleEndian.PutUint64(v.data[i:i+8], label)
		}
	}
}

// Add a label to a subvolume.
func (v *testVolume) addSubvol(origin, size dvid.Point3d, label uint64) {
	nx := v.size[0]
	nxy := nx * v.size[1]
	spanBytes := size[0] * 8
	buf := make([]byte, spanBytes)
	for x := int32(0); x < size[0]; x++ {
		binary.LittleEndian.PutUint64(buf[x*8:x*8+8], label)
	}
	for z := origin[2]; z < origin[2]+size[2]; z++ {
		for y := origin[1]; y < origin[1]+size[1]; y++ {
			i := (z*nxy + y*nx + origin[0]) * 8
			copy(v.data[i:i+spanBytes], buf)
		}
	}
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func (v *testVolume) putMutable(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func (v *testVolume) get(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	v.data = server.TestHTTP(t, "GET", apiStr, nil)
}

func (v *testVolume) getVoxel(pt dvid.Point3d) uint64 {
	nx := v.size[0]
	nxy := nx * v.size[1]
	i := (pt[2]*nxy + pt[1]*nx + pt[0]) * 8
	return binary.LittleEndian.Uint64(v.data[i : i+8])
}

func (v *testVolume) verifyLabel(t *testing.T, expected uint64, x, y, z int32) {
	pt := dvid.Point3d{x, y, z}
	label := v.getVoxel(pt)
	if label != expected {
		t.Errorf("Expected label %d at %s but got %d instead\n", expected, pt, label)
	}
}

func (v *testVolume) equals(v2 *testVolume) error {
	if !v.size.Equals(v2.size) {
		return fmt.Errorf("volume sizes are not equal")
	}
	if len(v.data) != len(v2.data) {
		return fmt.Errorf("data lengths are not equal")
	}
	for i, value := range v.data {
		if value != v2.data[i] {
			return fmt.Errorf("For element %d, found value %d != %d\n", i, value, v2.data[i])
		}
	}
	return nil
}

// Returns true if all voxels in test volume for given body has label.
func (v *testVolume) isLabel(label uint64, body *testBody) bool {
	nx := v.size[0]
	nxy := nx * v.size[1]
	for _, span := range body.voxelSpans {
		z, y, x0, x1 := span.Unpack()
		p := (z*nxy + y*nx) * 8
		for i := p + x0*8; i <= p+x1*8; i += 8 {
			curLabel := binary.LittleEndian.Uint64(v.data[i : i+8])
			if curLabel != label {
				return false
			}
		}
	}
	return true
}

// Returns true if any voxel in test volume has given label.
func (v *testVolume) hasLabel(label uint64, body *testBody) bool {
	nx := v.size[0]
	nxy := nx * v.size[1]
	for _, span := range body.voxelSpans {
		z, y, x0, x1 := span.Unpack()
		p := (z*nxy + y*nx) * 8
		for i := p + x0*8; i <= p+x1*8; i += 8 {
			curLabel := binary.LittleEndian.Uint64(v.data[i : i+8])
			if curLabel == label {
				return true
			}
		}
	}
	return false
}

type mergeJSON string

func (mjson mergeJSON) send(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(string(mjson)))
}

func createLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	// Setup test label blocks that are non-intersecting.
	volume := newTestVolume(128, 128, 128)
	volume.addBody(body1, 1)
	volume.addBody(body2, 2)
	volume.addBody(body3, 3)
	volume.addBody(body4, 4)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

func createLabelTest2Volume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	// Setup test label blocks that are non-intersecting.
	volume := newTestVolume(128, 128, 128)
	volume.addBody(body6, 6)
	volume.addBody(body7, 7)

	// Send data over HTTP to populate a data instance using mutable flag
	volume.putMutable(t, uuid, name)
	return volume
}

func TestBadSyncBlockSize(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies2", config)
	config.Set("BlockSize", "128,128,128")
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	url := fmt.Sprintf("%snode/%s/bodies/sync", server.WebAPIPath, uuid)
	msg := `{"sync": "labels"}`
	server.TestBadHTTP(t, "POST", url, strings.NewReader(msg))

	server.CreateTestSync(t, uuid, "bodies2", "labels")
}

func TestSparseVolumes(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Populate the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	badReqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/0", server.WebAPIPath, uuid, "bodies")
	server.TestBadHTTP(t, "GET", badReqStr, nil)

	for _, label := range []uint64{1, 3, 4} {
		// Get the coarse sparse volumes for each label and make sure they are correct.
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol-coarse/%d", server.WebAPIPath, uuid, "bodies", label)
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkCoarse(t, encoding)
	}

	for _, label := range []uint64{1, 2, 3, 4} {
		// Check fast HEAD requests
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		resp := server.TestHTTPResponse(t, "HEAD", reqStr, nil)
		if resp.Code != http.StatusOK {
			t.Errorf("HEAD on %s did not return OK.  Status = %d\n", reqStr, resp.Code)
		}

		// Check full sparse volumes
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})

		// Check with lz4 compression
		compressed := server.TestHTTP(t, "GET", reqStr+"?compression=lz4", nil)
		if err := lz4.Uncompress(compressed, encoding); err != nil {
			t.Fatalf("error uncompressing lz4: %v\n", err)
		}
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})

		// Check with gzip compression
		compressed = server.TestHTTP(t, "GET", reqStr+"?compression=gzip", nil)
		b := bytes.NewBuffer(compressed)
		var err error
		r, err := gzip.NewReader(b)
		if err != nil {
			t.Fatalf("error creating gzip reader: %v\n", err)
		}
		var buffer bytes.Buffer
		_, err = io.Copy(&buffer, r)
		if err != nil {
			t.Fatalf("error copying gzip data: %v\n", err)
		}
		err = r.Close()
		if err != nil {
			t.Fatalf("error closing gzip: %v\n", err)
		}
		encoding = buffer.Bytes()
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})

		// Check Y/Z restriction
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d?miny=30&maxy=50&minz=20&maxz=40", server.WebAPIPath, uuid, "bodies", label)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		var bound dvid.OptionalBounds
		bound.SetMinY(30)
		bound.SetMaxY(50)
		bound.SetMinZ(20)
		bound.SetMaxZ(40)
		bodies[label-1].checkSparseVol(t, encoding, bound)

		// Check X restriction
		minx := int32(20)
		maxx := int32(47)
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d?minx=%d&maxx=%d", server.WebAPIPath, uuid, "bodies", label, minx, maxx)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		checkSpans(t, encoding, minx, maxx)
	}

	// Make sure non-existent bodies return proper HEAD responses.
	headReq := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 10)
	resp := server.TestHTTPResponse(t, "HEAD", headReq, nil)
	if resp.Code != http.StatusNoContent {
		t.Errorf("HEAD on %s did not return 204 (No Content).  Status = %d\n", headReq, resp.Code)
	}

	// Commit this node and create branch for deletion testing.
	if err := datastore.Commit(uuid, "base segmentation", nil); err != nil {
		t.Errorf("Unable to lock root node %s: %v\n", uuid, err)
	}

	uuid2, err := datastore.NewVersion(uuid, "deletion test", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid, err)
	}

	// Delete an area
	delReq := fmt.Sprintf("%snode/%s/%s/area/2/96_64_32/0_32_32", server.WebAPIPath, uuid2, "bodies")
	server.TestHTTP(t, "DELETE", delReq, nil)

	// Read the area to make sure they are gone.
	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/2", server.WebAPIPath, uuid2, "bodies")
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	body2cropped.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	delReq = fmt.Sprintf("%snode/%s/%s/area/2/95_64_32/0_32_32", server.WebAPIPath, uuid2, "bodies")
	server.TestBadHTTP(t, "DELETE", delReq, nil)

	delReq = fmt.Sprintf("%snode/%s/%s/area/2/96_64_32/0_32_31", server.WebAPIPath, uuid2, "bodies")
	server.TestBadHTTP(t, "DELETE", delReq, nil)

	/*
		// Make sure those blocks are still in the root uuid.
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 2)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		if bodies[1].isDeleted(t, encoding, dvid.Span{1, 1, 0, 1}) {
			t.Errorf("Expected RLEs to be presented in label 2 root undeleted blocks.  Failed.\n")
		}
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 1)
		encoding = server.TestHTTP(t, "GET", reqStr, nil)
		if bodies[0].isDeleted(t, encoding, dvid.Span{1, 1, 0, 1}) {
			t.Errorf("Expected RLEs to be presented in label 1 root undeleted blocks.  Failed.\n")
		}
	*/
}

func TestMergeLabels(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	expected := createLabelTestVolume(t, uuid, "labels")
	expected.addBody(body3, 2)

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

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

	// Test merge of 3 into 2
	testMerge := mergeJSON(`[2, 3]`)
	testMerge.send(t, uuid, "bodies")

	// Make sure label 3 sparsevol has been removed.
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 3)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	// Make sure label changes are correct after completion
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved labelvol volume is incorrect size\n")
	}
	if !retrieved.isLabel(2, &body2) {
		t.Errorf("Expected label 2 original voxels to remain.  Instead some were removed.\n")
	}
	if retrieved.hasLabel(3, &body3) {
		t.Errorf("Found label 3 when all label 3 should have been merged into label 2!\n")
	}
	if !retrieved.isLabel(2, &body3) {
		t.Errorf("Incomplete merging.  Label 2 should have taken over full extent of label 3\n")
	}
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Merged label volume: %v\n", err)
	}
}

func TestSplitLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post label volume and setup expected volume after split.
	expected := createLabelTestVolume(t, uuid, "labels")
	expected.addBody(bodysplit, 5)

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Make sure sparsevol for original body 4 is correct
	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 4)
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	fmt.Printf("Checking original body 4 is correct\n")
	body4.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	// Create the sparsevol encoding for split area
	numspans := len(bodysplit.voxelSpans)
	rles := make(dvid.RLEs, numspans, numspans)
	for i, span := range bodysplit.voxelSpans {
		start := dvid.Point3d{span[2], span[1], span[0]}
		length := span[3] - span[2] + 1
		rles[i] = dvid.NewRLE(start, length)
	}

	// Create the split sparse volume binary
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))         // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))          // dimension of run (X = 0)
	buf.WriteByte(byte(0))                                   // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0))        // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(numspans)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Verify the max label is 4
	reqStr = fmt.Sprintf("%snode/%s/%s/maxlabel", server.WebAPIPath, uuid, "bodies")
	jsonStr := server.TestHTTP(t, "GET", reqStr, nil)
	expectedJSON := `{"maxlabel": 4}`
	if string(jsonStr) != expectedJSON {
		t.Errorf("Expected this JSON returned from maxlabel:\n%s\nGot:\n%s\n", expectedJSON, string(jsonStr))
	}

	// Submit the split sparsevol for body 4a
	reqStr = fmt.Sprintf("%snode/%s/%s/split/%d", server.WebAPIPath, uuid, "bodies", 4)
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

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved post-split volume is incorrect size\n")
	}
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Split label volume not equal to expected volume: %v\n", err)
	}

	// Make sure new body 5 is what we sent
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 5)
	encoding = server.TestHTTP(t, "GET", reqStr, nil)
	bodysplit.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	// Make sure sparsevol for original body 4 is correct
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 4)
	encoding = server.TestHTTP(t, "GET", reqStr, nil)
	bodyleft.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	// Do a merge of two after the split
	testMerge := mergeJSON(`[4, 5]`)
	testMerge.send(t, uuid, "bodies")

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on bodies update: %v\n", err)
	}

	// Make sure we wind up with original body 4
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/4", server.WebAPIPath, uuid, "bodies")
	encoding = server.TestHTTP(t, "GET", reqStr, nil)
	body4.checkSparseVol(t, encoding, dvid.OptionalBounds{})
}

// Same as TestSplitLabel but now designate the actual split label
func TestSplitGivenLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post label volume and setup expected volume after split.
	expected := createLabelTestVolume(t, uuid, "labels")
	expected.addBody(bodyleft, 4)
	expected.addBody(bodysplit, 23)

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Create the sparsevol encoding for split area
	numspans := len(bodysplit.voxelSpans)
	rles := make(dvid.RLEs, numspans, numspans)
	for i, span := range bodysplit.voxelSpans {
		start := dvid.Point3d{span[2], span[1], span[0]}
		length := span[3] - span[2] + 1
		rles[i] = dvid.NewRLE(start, length)
	}

	// Create the split sparse volume binary
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))         // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))          // dimension of run (X = 0)
	buf.WriteByte(byte(0))                                   // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0))        // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(numspans)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Submit the split sparsevol for body 4a
	reqStr := fmt.Sprintf("%snode/%s/%s/split/%d?splitlabel=23", server.WebAPIPath, uuid, "bodies", 4)
	r := server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}
	newlabel, ok := jsonVal["label"]
	if !ok {
		t.Errorf("The split request did not yield label value.  Instead got: %v\n", jsonVal)
	}
	if newlabel != 23 {
		t.Errorf("Expected split label to be assigned label 23, instead got %d\n", newlabel)
	}
}

func TestSplitCoarseLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post label volume and setup expected volume after split of block coords (2, 1, 1) and (3, 1, 2)
	expected := createLabelTestVolume(t, uuid, "labels")
	fromLabel := uint64(4)
	toLabel := uint64(5)
	nx := expected.size[0]
	nxy := nx * expected.size[1]
	var x, y, z int32
	for z = 0; z < 128; z++ {
		bz := z / DefaultBlockSize
		if bz != 1 && bz != 2 {
			continue
		}
		for y = 0; y < 128; y++ {
			by := y / DefaultBlockSize
			if by != 1 {
				continue
			}
			for x = 0; x < 128; x++ {
				bx := x / DefaultBlockSize
				if (bz == 1 && bx == 2) || (bz == 2 && bx == 2) {
					i := (z*nxy + y*nx + x) * 8
					label := binary.LittleEndian.Uint64(expected.data[i : i+8])
					if label == fromLabel {
						binary.LittleEndian.PutUint64(expected.data[i:i+8], toLabel)
					}
				}
			}
		}
	}

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Make sure sparsevol for original body 4 is correct
	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 4)
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	fmt.Printf("Checking original body 4 is correct\n")
	body4.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	// Create the encoding for split area in block coordinates.
	rles := dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{2, 1, 1}, 1),
		dvid.NewRLE(dvid.Point3d{2, 1, 2}, 1),
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(2)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Submit the coarse split
	reqStr = fmt.Sprintf("%snode/%s/%s/split-coarse/%d", server.WebAPIPath, uuid, "bodies", 4)
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

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	// Make sure labels are correct
	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved post-split volume is incorrect size\n")
	}
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Split label volume not equal to expected volume: %v\n", err)
	}
}

func TestSplitCoarseGivenLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post label volume and setup expected volume after split of block coords (2, 1, 1) and (3, 1, 2)
	expected := createLabelTestVolume(t, uuid, "labels")
	fromLabel := uint64(4)
	toLabel := uint64(8127)
	nx := expected.size[0]
	nxy := nx * expected.size[1]
	var x, y, z int32
	for z = 0; z < 128; z++ {
		bz := z / DefaultBlockSize
		if bz != 1 && bz != 2 {
			continue
		}
		for y = 0; y < 128; y++ {
			by := y / DefaultBlockSize
			if by != 1 {
				continue
			}
			for x = 0; x < 128; x++ {
				bx := x / DefaultBlockSize
				if (bz == 1 && bx == 2) || (bz == 2 && bx == 2) {
					i := (z*nxy + y*nx + x) * 8
					label := binary.LittleEndian.Uint64(expected.data[i : i+8])
					if label == fromLabel {
						binary.LittleEndian.PutUint64(expected.data[i:i+8], toLabel)
					}
				}
			}
		}
	}

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Create the encoding for split area in block coordinates.
	rles := dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{2, 1, 1}, 1),
		dvid.NewRLE(dvid.Point3d{2, 1, 2}, 1),
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(2)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Submit the coarse split
	reqStr := fmt.Sprintf("%snode/%s/%s/split-coarse/%d?splitlabel=8127", server.WebAPIPath, uuid, "bodies", 4)
	r := server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}
	newlabel, ok := jsonVal["label"]
	if !ok {
		t.Errorf("The split request did not yield label value.  Instead got: %v\n", jsonVal)
	}
	if newlabel != 8127 {
		t.Errorf("Expected split label to be 8127, instead got %d\n", newlabel)
	}

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	// Make sure labels are correct
	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved post-split volume is incorrect size\n")
	}
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Split label volume not equal to expected volume: %v\n", err)
	}
}

func TestMergeSplitLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post standard label 1-4 volume
	expected := createLabelTestVolume(t, uuid, "labels")

	// Get expected volume if we add label 3 to 4.
	expected.addBody(body3, 4)

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Test merge of 3 into 4
	testMerge := mergeJSON(`[4, 3]`)
	testMerge.send(t, uuid, "bodies")

	// Make sure label changes are correct after completion
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	// Make sure label 3 sparsevol has been removed.
	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 3)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved labelvol volume is incorrect size\n")
	}
	if !retrieved.isLabel(2, &body2) {
		t.Errorf("Expected label 2 original voxels to remain.  Instead some were removed.\n")
	}
	if retrieved.hasLabel(3, &body3) {
		t.Errorf("Found label 3 when all label 3 should have been merged into label 4!\n")
	}
	if !retrieved.isLabel(4, &body3) {
		t.Errorf("Incomplete merging.  Label 4 should have taken over full extent of label 3\n")
	}
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Merged label volume not equal to expected merged volume: %v\n", err)
	}

	// Create the sparsevol encoding for split area of 4
	numspans := len(bodysplit.voxelSpans)
	rles := make(dvid.RLEs, numspans, numspans)
	for i, span := range bodysplit.voxelSpans {
		start := dvid.Point3d{span[2], span[1], span[0]}
		length := span[3] - span[2] + 1
		rles[i] = dvid.NewRLE(start, length)
	}

	// Create the split sparse volume binary
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))         // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))          // dimension of run (X = 0)
	buf.WriteByte(byte(0))                                   // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0))        // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(numspans)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Verify the max label is 4
	reqStr = fmt.Sprintf("%snode/%s/%s/maxlabel", server.WebAPIPath, uuid, "bodies")
	jsonStr := server.TestHTTP(t, "GET", reqStr, nil)
	expectedJSON := `{"maxlabel": 4}`
	if string(jsonStr) != expectedJSON {
		t.Errorf("Expected this JSON returned from maxlabel:\n%s\nGot:\n%s\n", expectedJSON, string(jsonStr))
	}

	// Submit the split sparsevol for body 4a (-> 5)
	reqStr = fmt.Sprintf("%snode/%s/%s/split/4", server.WebAPIPath, uuid, "bodies")
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

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of bodies -> labels: %v\n", err)
	}

	retrieved = newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	if len(retrieved.data) != 8*128*128*128 {
		t.Errorf("Retrieved post-split volume is incorrect size\n")
	}
	expected.addBody(bodysplit, 5)
	if err := retrieved.equals(expected); err != nil {
		t.Errorf("Split label volume not equal to expected volume: %v\n", err)
	}

	// Make sure new body 5 is what we sent
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 5)
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	bodysplit.checkSparseVol(t, encoding, dvid.OptionalBounds{})
}

func TestMultiscaleMergeSplit(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Add multiscale
	server.CreateTestInstance(t, uuid, "labelblk", "labels_1", config) // 64 x 64 x 64
	server.CreateTestSync(t, uuid, "labels_1", "labels")
	server.CreateTestInstance(t, uuid, "labelblk", "labels_2", config) // 32 x 32 x 32
	server.CreateTestSync(t, uuid, "labels_2", "labels_1")

	// Create an easily interpreted label volume with a couple of labels.
	volume := newTestVolume(128, 128, 128)
	volume.addSubvol(dvid.Point3d{40, 40, 40}, dvid.Point3d{40, 40, 40}, 1)
	volume.addSubvol(dvid.Point3d{40, 40, 80}, dvid.Point3d{40, 40, 40}, 2)
	volume.addSubvol(dvid.Point3d{80, 40, 40}, dvid.Point3d{40, 40, 40}, 13)
	volume.addSubvol(dvid.Point3d{40, 80, 40}, dvid.Point3d{40, 40, 40}, 209)
	volume.addSubvol(dvid.Point3d{80, 80, 40}, dvid.Point3d{40, 40, 40}, 311)
	volume.put(t, uuid, "labels")

	// Verify initial ingest for hi-res
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on update for labels: %v\n", err)
	}
	hires := newTestVolume(128, 128, 128)
	hires.get(t, uuid, "labels")
	hires.verifyLabel(t, 1, 45, 45, 45)
	hires.verifyLabel(t, 2, 50, 50, 100)
	hires.verifyLabel(t, 13, 100, 60, 60)
	hires.verifyLabel(t, 209, 55, 100, 55)
	hires.verifyLabel(t, 311, 81, 81, 41)

	// Check the first downres: 64^3
	if err := datastore.BlockOnUpdating(uuid, "labels_1"); err != nil {
		t.Fatalf("Error blocking on update for labels_1: %v\n", err)
	}
	downres1 := newTestVolume(64, 64, 64)
	downres1.get(t, uuid, "labels_1")
	downres1.verifyLabel(t, 1, 30, 30, 30)
	downres1.verifyLabel(t, 2, 21, 21, 45)
	downres1.verifyLabel(t, 13, 45, 21, 36)
	downres1.verifyLabel(t, 209, 21, 50, 35)
	downres1.verifyLabel(t, 311, 45, 55, 35)
	expected1 := newTestVolume(64, 64, 64)
	expected1.addSubvol(dvid.Point3d{20, 20, 20}, dvid.Point3d{20, 20, 20}, 1)
	expected1.addSubvol(dvid.Point3d{20, 20, 40}, dvid.Point3d{20, 20, 20}, 2)
	expected1.addSubvol(dvid.Point3d{40, 20, 20}, dvid.Point3d{20, 20, 20}, 13)
	expected1.addSubvol(dvid.Point3d{20, 40, 20}, dvid.Point3d{20, 20, 20}, 209)
	expected1.addSubvol(dvid.Point3d{40, 40, 20}, dvid.Point3d{20, 20, 20}, 311)
	if err := downres1.equals(expected1); err != nil {
		t.Errorf("1st downres 'labels_1' isn't what is expected: %v\n", err)
	}

	// Check the second downres to voxel: 32^3
	if err := datastore.BlockOnUpdating(uuid, "labels_2"); err != nil {
		t.Fatalf("Error blocking on update for labels_2: %v\n", err)
	}
	expected2 := newTestVolume(32, 32, 32)
	expected2.addSubvol(dvid.Point3d{10, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	expected2.addSubvol(dvid.Point3d{10, 10, 20}, dvid.Point3d{10, 10, 10}, 2)
	expected2.addSubvol(dvid.Point3d{20, 10, 10}, dvid.Point3d{10, 10, 10}, 13)
	expected2.addSubvol(dvid.Point3d{10, 20, 10}, dvid.Point3d{10, 10, 10}, 209)
	expected2.addSubvol(dvid.Point3d{20, 20, 10}, dvid.Point3d{10, 10, 10}, 311)
	downres2 := newTestVolume(32, 32, 32)
	downres2.get(t, uuid, "labels_2")
	if err := downres2.equals(expected2); err != nil {
		t.Errorf("2nd downres 'labels_2' isn't what is expected: %v\n", err)
	}

	// Test merge of 2 and 13 into 1
	testMerge := mergeJSON(`[1, 2, 13]`)
	testMerge.send(t, uuid, "bodies")

	// Make sure labels 2 and 13 sparsevol has been removed.
	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/2", server.WebAPIPath, uuid, "bodies")
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/13", server.WebAPIPath, uuid, "bodies")
	server.TestBadHTTP(t, "GET", reqStr, nil)

	// Make sure label changes are correct after completion of merge
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of merge bodies -> labels: %v\n", err)
	}
	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, uuid, "labels")
	merged := newTestVolume(128, 128, 128)
	merged.addSubvol(dvid.Point3d{40, 40, 40}, dvid.Point3d{40, 40, 40}, 1)
	merged.addSubvol(dvid.Point3d{40, 40, 80}, dvid.Point3d{40, 40, 40}, 1)
	merged.addSubvol(dvid.Point3d{80, 40, 40}, dvid.Point3d{40, 40, 40}, 1)
	merged.addSubvol(dvid.Point3d{40, 80, 40}, dvid.Point3d{40, 40, 40}, 209)
	merged.addSubvol(dvid.Point3d{80, 80, 40}, dvid.Point3d{40, 40, 40}, 311)
	if err := retrieved.equals(merged); err != nil {
		t.Errorf("Merged label volume not equal to expected merged volume: %v\n", err)
	}

	if err := datastore.BlockOnUpdating(uuid, "labels_1"); err != nil {
		t.Fatalf("Error blocking on sync of merge bodies -> labels: %v\n", err)
	}
	retrieved1 := newTestVolume(64, 64, 64)
	retrieved1.get(t, uuid, "labels_1")
	merged1 := newTestVolume(64, 64, 64)
	merged1.addSubvol(dvid.Point3d{20, 20, 20}, dvid.Point3d{20, 20, 20}, 1)
	merged1.addSubvol(dvid.Point3d{20, 20, 40}, dvid.Point3d{20, 20, 20}, 1)
	merged1.addSubvol(dvid.Point3d{40, 20, 20}, dvid.Point3d{20, 20, 20}, 1)
	merged1.addSubvol(dvid.Point3d{20, 40, 20}, dvid.Point3d{20, 20, 20}, 209)
	merged1.addSubvol(dvid.Point3d{40, 40, 20}, dvid.Point3d{20, 20, 20}, 311)
	if err := retrieved1.equals(merged1); err != nil {
		t.Errorf("Merged label volume downres #1 not equal to expected merged volume: %v\n", err)
	}

	if err := datastore.BlockOnUpdating(uuid, "labels_2"); err != nil {
		t.Fatalf("Error blocking on sync of merge bodies -> labels: %v\n", err)
	}
	retrieved2 := newTestVolume(32, 32, 32)
	retrieved2.get(t, uuid, "labels_2")
	merged2 := newTestVolume(32, 32, 32)
	merged2.addSubvol(dvid.Point3d{10, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	merged2.addSubvol(dvid.Point3d{10, 10, 20}, dvid.Point3d{10, 10, 10}, 1)
	merged2.addSubvol(dvid.Point3d{20, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	merged2.addSubvol(dvid.Point3d{10, 20, 10}, dvid.Point3d{10, 10, 10}, 209)
	merged2.addSubvol(dvid.Point3d{20, 20, 10}, dvid.Point3d{10, 10, 10}, 311)
	if err := retrieved2.equals(merged2); err != nil {
		t.Errorf("Merged label volume downres #2 not equal to expected merged volume: %v\n", err)
	}

	// Create the sparsevol encoding for split area that used to be body 13
	numspans := 40 * 40
	rles := make(dvid.RLEs, numspans, numspans)
	length := int32(40)
	i := 0
	for z := int32(40); z < 80; z++ {
		for y := int32(40); y < 80; y++ {
			start := dvid.Point3d{80, y, z}
			rles[i] = dvid.NewRLE(start, length)
			i++
		}
	}

	// Create the split sparse volume binary
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))            // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))             // dimension of run (X = 0)
	buf.WriteByte(byte(0))                                      // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(numspans*40)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(numspans))    // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Submit the split sparsevol and assign to label 28
	reqStr = fmt.Sprintf("%snode/%s/%s/split/1?splitlabel=28", server.WebAPIPath, uuid, "bodies")
	r := server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}

	// Test all the multiscales for correct split volume.

	// Make sure label changes are correct after completion of merge
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of merge bodies -> labels: %v\n", err)
	}
	retrieved.get(t, uuid, "labels")
	split := newTestVolume(128, 128, 128)
	split.addSubvol(dvid.Point3d{40, 40, 40}, dvid.Point3d{40, 40, 40}, 1)
	split.addSubvol(dvid.Point3d{40, 40, 80}, dvid.Point3d{40, 40, 40}, 1)
	split.addSubvol(dvid.Point3d{80, 40, 40}, dvid.Point3d{40, 40, 40}, 28)
	split.addSubvol(dvid.Point3d{40, 80, 40}, dvid.Point3d{40, 40, 40}, 209)
	split.addSubvol(dvid.Point3d{80, 80, 40}, dvid.Point3d{40, 40, 40}, 311)
	if err := retrieved.equals(split); err != nil {
		t.Errorf("Split label volume not equal to expected split volume: %v\n", err)
	}

	if err := datastore.BlockOnUpdating(uuid, "labels_1"); err != nil {
		t.Fatalf("Error blocking on sync of split bodies -> labels: %v\n", err)
	}
	retrieved1.get(t, uuid, "labels_1")
	split1 := newTestVolume(64, 64, 64)
	split1.addSubvol(dvid.Point3d{20, 20, 20}, dvid.Point3d{20, 20, 20}, 1)
	split1.addSubvol(dvid.Point3d{20, 20, 40}, dvid.Point3d{20, 20, 20}, 1)
	split1.addSubvol(dvid.Point3d{40, 20, 20}, dvid.Point3d{20, 20, 20}, 28)
	split1.addSubvol(dvid.Point3d{20, 40, 20}, dvid.Point3d{20, 20, 20}, 209)
	split1.addSubvol(dvid.Point3d{40, 40, 20}, dvid.Point3d{20, 20, 20}, 311)
	if err := retrieved1.equals(split1); err != nil {
		t.Errorf("Split label volume downres #1 not equal to expected split volume: %v\n", err)
	}

	if err := datastore.BlockOnUpdating(uuid, "labels_2"); err != nil {
		t.Fatalf("Error blocking on sync of merge bodies -> labels: %v\n", err)
	}
	retrieved2.get(t, uuid, "labels_2")
	split2 := newTestVolume(32, 32, 32)
	split2.addSubvol(dvid.Point3d{10, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	split2.addSubvol(dvid.Point3d{10, 10, 20}, dvid.Point3d{10, 10, 10}, 1)
	split2.addSubvol(dvid.Point3d{20, 10, 10}, dvid.Point3d{10, 10, 10}, 28)
	split2.addSubvol(dvid.Point3d{10, 20, 10}, dvid.Point3d{10, 10, 10}, 209)
	split2.addSubvol(dvid.Point3d{20, 20, 10}, dvid.Point3d{10, 10, 10}, 311)
	if err := retrieved2.equals(split2); err != nil {
		t.Errorf("Split label volume downres #2 not equal to expected split volume: %v\n", err)
	}
}

// Test that mutable labelblk POST will accurately remove prior bodies.
func TestMutableLabelblkPOST(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Post labels 1-4
	createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Make sure we have labels 1-4 sparsevol
	for _, label := range []uint64{1, 2, 3, 4} {
		// Check fast HEAD requests
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		resp := server.TestHTTPResponse(t, "HEAD", reqStr, nil)
		if resp.Code != http.StatusOK {
			t.Errorf("HEAD on %s did not return OK.  Status = %d\n", reqStr, resp.Code)
		}

		// Check full sparse volumes
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})
	}

	// Post labels 6-7
	createLabelTest2Volume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Make sure that labels 1-4 have no more sparse vol.
	for _, label := range []uint64{1, 2, 3, 4} {
		// Check full sparse volumes aren't retrievable anymore
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		server.TestBadHTTP(t, "GET", reqStr, nil)

		// Make sure non-existent bodies return proper HEAD responses.
		resp := server.TestHTTPResponse(t, "HEAD", reqStr, nil)
		if resp.Code != http.StatusNoContent {
			t.Errorf("HEAD on %s did not return 204 (No Content).  Status = %d\n", reqStr, resp.Code)
		}
	}

	// Make sure labels 6-7 are available as sparse vol.
	for _, label := range []uint64{6, 7} {
		// Check fast HEAD requests
		reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		resp := server.TestHTTPResponse(t, "HEAD", reqStr, nil)
		if resp.Code != http.StatusOK {
			t.Errorf("HEAD on %s did not return OK.  Status = %d\n", reqStr, resp.Code)
		}

		// Check full sparse volumes
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})
	}
}

func TestResyncLabel(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	createLabelTestVolume(t, uuid, "labels")
	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	reqStr := fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 1)
	encoding := server.TestHTTP(t, "GET", reqStr, nil)
	body1.checkSparseVol(t, encoding, dvid.OptionalBounds{})

	// Delete the labelvol but leave the labelblk.
	d, err := GetByUUIDName(uuid, "bodies")
	if err != nil {
		t.Fatalf("Unable to get labelvol data pointer: %v\n", err)
	}
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		t.Fatalf("data %q had error initializing store: %v\n", d.DataName(), err)
	}
	startPt := dvid.ChunkPoint3d{0, 0, 0}
	endPt := dvid.ChunkPoint3d{10000, 10000, 10000}
	begTKey := NewTKey(1, startPt.ToIZYXString())
	endTKey := NewTKey(1, endPt.ToIZYXString())
	v, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		t.Fatalf("bad version from UUID %s: %v\n", uuid, err)
	}
	ctx := datastore.NewVersionedCtx(d, v)
	if err := store.DeleteRange(ctx, begTKey, endTKey); err != nil {
		t.Fatalf("can't delete labelvol blocks for data %q: %v\n", d.DataName(), err)
	}

	// We should have bad labelvol at this point.
	reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", 1)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	// Create the encoding for resync area in block coordinates.
	rles := dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{0, 1, 0}, 1), // actually a superset of body 1 blocks
		dvid.NewRLE(dvid.Point3d{0, 1, 1}, 1),
		dvid.NewRLE(dvid.Point3d{0, 1, 2}, 1),
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(3)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)

	// Submit the resync
	reqStr = fmt.Sprintf("%snode/%s/%s/resync/%d", server.WebAPIPath, uuid, "bodies", 1)
	server.TestHTTP(t, "POST", reqStr, buf)

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on resync of labels: %v\n", err)
	}

	// Make sure sparsevol for all bodies are correct (no inadvertent deletions)
	for _, label := range []uint64{1, 2, 3, 4} {
		// Check fast HEAD requests
		reqStr = fmt.Sprintf("%snode/%s/%s/sparsevol/%d", server.WebAPIPath, uuid, "bodies", label)
		resp := server.TestHTTPResponse(t, "HEAD", reqStr, nil)
		if resp.Code != http.StatusOK {
			t.Errorf("HEAD on %s did not return OK.  Status = %d\n", reqStr, resp.Code)
		}

		// Check full sparse volumes
		encoding := server.TestHTTP(t, "GET", reqStr, nil)
		bodies[label-1].checkSparseVol(t, encoding, dvid.OptionalBounds{})
	}
}

var (
	body1 = testBody{
		label:  1,
		offset: dvid.Point3d{10, 40, 10},
		size:   dvid.Point3d{20, 20, 80},
		blockSpans: []dvid.Span{
			{0, 1, 0, 0},
			{1, 1, 0, 0},
			{2, 1, 0, 0},
		},
		voxelSpans: []dvid.Span{
			{10, 40, 10, 29}, {10, 41, 10, 29}, {10, 42, 10, 29}, {10, 43, 10, 29}, {10, 44, 10, 29},
			{10, 45, 10, 29}, {10, 46, 10, 29}, {10, 47, 10, 29}, {10, 48, 10, 29}, {10, 49, 10, 29},
			{10, 50, 10, 29}, {10, 51, 10, 29}, {10, 52, 10, 29}, {10, 53, 10, 29}, {10, 54, 10, 29},
			{10, 55, 10, 29}, {10, 56, 10, 29}, {10, 57, 10, 29}, {10, 58, 10, 29}, {10, 59, 10, 29},
			{11, 40, 10, 29}, {11, 41, 10, 29}, {11, 42, 10, 29}, {11, 43, 10, 29}, {11, 44, 10, 29},
			{11, 45, 10, 29}, {11, 46, 10, 29}, {11, 47, 10, 29}, {11, 48, 10, 29}, {11, 49, 10, 29},
			{11, 50, 10, 29}, {11, 51, 10, 29}, {11, 52, 10, 29}, {11, 53, 10, 29}, {11, 54, 10, 29},
			{11, 55, 10, 29}, {11, 56, 10, 29}, {11, 57, 10, 29}, {11, 58, 10, 29}, {11, 59, 10, 29},
			{12, 40, 10, 29}, {12, 41, 10, 29}, {12, 42, 10, 29}, {12, 43, 10, 29}, {12, 44, 10, 29},
			{12, 45, 10, 29}, {12, 46, 10, 29}, {12, 47, 10, 29}, {12, 48, 10, 29}, {12, 49, 10, 29},
			{12, 50, 10, 29}, {12, 51, 10, 29}, {12, 52, 10, 29}, {12, 53, 10, 29}, {12, 54, 10, 29},
			{12, 55, 10, 29}, {12, 56, 10, 29}, {12, 57, 10, 29}, {12, 58, 10, 29}, {12, 59, 10, 29},
			{13, 40, 10, 29}, {13, 41, 10, 29}, {13, 42, 10, 29}, {13, 43, 10, 29}, {13, 44, 10, 29},
			{13, 45, 10, 29}, {13, 46, 10, 29}, {13, 47, 10, 29}, {13, 48, 10, 29}, {13, 49, 10, 29},
			{13, 50, 10, 29}, {13, 51, 10, 29}, {13, 52, 10, 29}, {13, 53, 10, 29}, {13, 54, 10, 29},
			{13, 55, 10, 29}, {13, 56, 10, 29}, {13, 57, 10, 29}, {13, 58, 10, 29}, {13, 59, 10, 29},
			{14, 40, 10, 29}, {14, 41, 10, 29}, {14, 42, 10, 29}, {14, 43, 10, 29}, {14, 44, 10, 29},
			{14, 45, 10, 29}, {14, 46, 10, 29}, {14, 47, 10, 29}, {14, 48, 10, 29}, {14, 49, 10, 29},
			{14, 50, 10, 29}, {14, 51, 10, 29}, {14, 52, 10, 29}, {14, 53, 10, 29}, {14, 54, 10, 29},
			{14, 55, 10, 29}, {14, 56, 10, 29}, {14, 57, 10, 29}, {14, 58, 10, 29}, {14, 59, 10, 29},
			{15, 40, 10, 29}, {15, 41, 10, 29}, {15, 42, 10, 29}, {15, 43, 10, 29}, {15, 44, 10, 29},
			{15, 45, 10, 29}, {15, 46, 10, 29}, {15, 47, 10, 29}, {15, 48, 10, 29}, {15, 49, 10, 29},
			{15, 50, 10, 29}, {15, 51, 10, 29}, {15, 52, 10, 29}, {15, 53, 10, 29}, {15, 54, 10, 29},
			{15, 55, 10, 29}, {15, 56, 10, 29}, {15, 57, 10, 29}, {15, 58, 10, 29}, {15, 59, 10, 29},
			{16, 40, 10, 29}, {16, 41, 10, 29}, {16, 42, 10, 29}, {16, 43, 10, 29}, {16, 44, 10, 29},
			{16, 45, 10, 29}, {16, 46, 10, 29}, {16, 47, 10, 29}, {16, 48, 10, 29}, {16, 49, 10, 29},
			{16, 50, 10, 29}, {16, 51, 10, 29}, {16, 52, 10, 29}, {16, 53, 10, 29}, {16, 54, 10, 29},
			{16, 55, 10, 29}, {16, 56, 10, 29}, {16, 57, 10, 29}, {16, 58, 10, 29}, {16, 59, 10, 29},
			{17, 40, 10, 29}, {17, 41, 10, 29}, {17, 42, 10, 29}, {17, 43, 10, 29}, {17, 44, 10, 29},
			{17, 45, 10, 29}, {17, 46, 10, 29}, {17, 47, 10, 29}, {17, 48, 10, 29}, {17, 49, 10, 29},
			{17, 50, 10, 29}, {17, 51, 10, 29}, {17, 52, 10, 29}, {17, 53, 10, 29}, {17, 54, 10, 29},
			{17, 55, 10, 29}, {17, 56, 10, 29}, {17, 57, 10, 29}, {17, 58, 10, 29}, {17, 59, 10, 29},
			{18, 40, 10, 29}, {18, 41, 10, 29}, {18, 42, 10, 29}, {18, 43, 10, 29}, {18, 44, 10, 29},
			{18, 45, 10, 29}, {18, 46, 10, 29}, {18, 47, 10, 29}, {18, 48, 10, 29}, {18, 49, 10, 29},
			{18, 50, 10, 29}, {18, 51, 10, 29}, {18, 52, 10, 29}, {18, 53, 10, 29}, {18, 54, 10, 29},
			{18, 55, 10, 29}, {18, 56, 10, 29}, {18, 57, 10, 29}, {18, 58, 10, 29}, {18, 59, 10, 29},
			{19, 40, 10, 29}, {19, 41, 10, 29}, {19, 42, 10, 29}, {19, 43, 10, 29}, {19, 44, 10, 29},
			{19, 45, 10, 29}, {19, 46, 10, 29}, {19, 47, 10, 29}, {19, 48, 10, 29}, {19, 49, 10, 29},
			{19, 50, 10, 29}, {19, 51, 10, 29}, {19, 52, 10, 29}, {19, 53, 10, 29}, {19, 54, 10, 29},
			{19, 55, 10, 29}, {19, 56, 10, 29}, {19, 57, 10, 29}, {19, 58, 10, 29}, {19, 59, 10, 29},
			{20, 40, 10, 29}, {20, 41, 10, 29}, {20, 42, 10, 29}, {20, 43, 10, 29}, {20, 44, 10, 29},
			{20, 45, 10, 29}, {20, 46, 10, 29}, {20, 47, 10, 29}, {20, 48, 10, 29}, {20, 49, 10, 29},
			{20, 50, 10, 29}, {20, 51, 10, 29}, {20, 52, 10, 29}, {20, 53, 10, 29}, {20, 54, 10, 29},
			{20, 55, 10, 29}, {20, 56, 10, 29}, {20, 57, 10, 29}, {20, 58, 10, 29}, {20, 59, 10, 29},
			{21, 40, 10, 29}, {21, 41, 10, 29}, {21, 42, 10, 29}, {21, 43, 10, 29}, {21, 44, 10, 29},
			{21, 45, 10, 29}, {21, 46, 10, 29}, {21, 47, 10, 29}, {21, 48, 10, 29}, {21, 49, 10, 29},
			{21, 50, 10, 29}, {21, 51, 10, 29}, {21, 52, 10, 29}, {21, 53, 10, 29}, {21, 54, 10, 29},
			{21, 55, 10, 29}, {21, 56, 10, 29}, {21, 57, 10, 29}, {21, 58, 10, 29}, {21, 59, 10, 29},
			{22, 40, 10, 29}, {22, 41, 10, 29}, {22, 42, 10, 29}, {22, 43, 10, 29}, {22, 44, 10, 29},
			{22, 45, 10, 29}, {22, 46, 10, 29}, {22, 47, 10, 29}, {22, 48, 10, 29}, {22, 49, 10, 29},
			{22, 50, 10, 29}, {22, 51, 10, 29}, {22, 52, 10, 29}, {22, 53, 10, 29}, {22, 54, 10, 29},
			{22, 55, 10, 29}, {22, 56, 10, 29}, {22, 57, 10, 29}, {22, 58, 10, 29}, {22, 59, 10, 29},
			{23, 40, 10, 29}, {23, 41, 10, 29}, {23, 42, 10, 29}, {23, 43, 10, 29}, {23, 44, 10, 29},
			{23, 45, 10, 29}, {23, 46, 10, 29}, {23, 47, 10, 29}, {23, 48, 10, 29}, {23, 49, 10, 29},
			{23, 50, 10, 29}, {23, 51, 10, 29}, {23, 52, 10, 29}, {23, 53, 10, 29}, {23, 54, 10, 29},
			{23, 55, 10, 29}, {23, 56, 10, 29}, {23, 57, 10, 29}, {23, 58, 10, 29}, {23, 59, 10, 29},
			{24, 40, 10, 29}, {24, 41, 10, 29}, {24, 42, 10, 29}, {24, 43, 10, 29}, {24, 44, 10, 29},
			{24, 45, 10, 29}, {24, 46, 10, 29}, {24, 47, 10, 29}, {24, 48, 10, 29}, {24, 49, 10, 29},
			{24, 50, 10, 29}, {24, 51, 10, 29}, {24, 52, 10, 29}, {24, 53, 10, 29}, {24, 54, 10, 29},
			{24, 55, 10, 29}, {24, 56, 10, 29}, {24, 57, 10, 29}, {24, 58, 10, 29}, {24, 59, 10, 29},
			{25, 40, 10, 29}, {25, 41, 10, 29}, {25, 42, 10, 29}, {25, 43, 10, 29}, {25, 44, 10, 29},
			{25, 45, 10, 29}, {25, 46, 10, 29}, {25, 47, 10, 29}, {25, 48, 10, 29}, {25, 49, 10, 29},
			{25, 50, 10, 29}, {25, 51, 10, 29}, {25, 52, 10, 29}, {25, 53, 10, 29}, {25, 54, 10, 29},
			{25, 55, 10, 29}, {25, 56, 10, 29}, {25, 57, 10, 29}, {25, 58, 10, 29}, {25, 59, 10, 29},
			{26, 40, 10, 29}, {26, 41, 10, 29}, {26, 42, 10, 29}, {26, 43, 10, 29}, {26, 44, 10, 29},
			{26, 45, 10, 29}, {26, 46, 10, 29}, {26, 47, 10, 29}, {26, 48, 10, 29}, {26, 49, 10, 29},
			{26, 50, 10, 29}, {26, 51, 10, 29}, {26, 52, 10, 29}, {26, 53, 10, 29}, {26, 54, 10, 29},
			{26, 55, 10, 29}, {26, 56, 10, 29}, {26, 57, 10, 29}, {26, 58, 10, 29}, {26, 59, 10, 29},
			{27, 40, 10, 29}, {27, 41, 10, 29}, {27, 42, 10, 29}, {27, 43, 10, 29}, {27, 44, 10, 29},
			{27, 45, 10, 29}, {27, 46, 10, 29}, {27, 47, 10, 29}, {27, 48, 10, 29}, {27, 49, 10, 29},
			{27, 50, 10, 29}, {27, 51, 10, 29}, {27, 52, 10, 29}, {27, 53, 10, 29}, {27, 54, 10, 29},
			{27, 55, 10, 29}, {27, 56, 10, 29}, {27, 57, 10, 29}, {27, 58, 10, 29}, {27, 59, 10, 29},
			{28, 40, 10, 29}, {28, 41, 10, 29}, {28, 42, 10, 29}, {28, 43, 10, 29}, {28, 44, 10, 29},
			{28, 45, 10, 29}, {28, 46, 10, 29}, {28, 47, 10, 29}, {28, 48, 10, 29}, {28, 49, 10, 29},
			{28, 50, 10, 29}, {28, 51, 10, 29}, {28, 52, 10, 29}, {28, 53, 10, 29}, {28, 54, 10, 29},
			{28, 55, 10, 29}, {28, 56, 10, 29}, {28, 57, 10, 29}, {28, 58, 10, 29}, {28, 59, 10, 29},
			{29, 40, 10, 29}, {29, 41, 10, 29}, {29, 42, 10, 29}, {29, 43, 10, 29}, {29, 44, 10, 29},
			{29, 45, 10, 29}, {29, 46, 10, 29}, {29, 47, 10, 29}, {29, 48, 10, 29}, {29, 49, 10, 29},
			{29, 50, 10, 29}, {29, 51, 10, 29}, {29, 52, 10, 29}, {29, 53, 10, 29}, {29, 54, 10, 29},
			{29, 55, 10, 29}, {29, 56, 10, 29}, {29, 57, 10, 29}, {29, 58, 10, 29}, {29, 59, 10, 29},
			{30, 40, 10, 29}, {30, 41, 10, 29}, {30, 42, 10, 29}, {30, 43, 10, 29}, {30, 44, 10, 29},
			{30, 45, 10, 29}, {30, 46, 10, 29}, {30, 47, 10, 29}, {30, 48, 10, 29}, {30, 49, 10, 29},
			{30, 50, 10, 29}, {30, 51, 10, 29}, {30, 52, 10, 29}, {30, 53, 10, 29}, {30, 54, 10, 29},
			{30, 55, 10, 29}, {30, 56, 10, 29}, {30, 57, 10, 29}, {30, 58, 10, 29}, {30, 59, 10, 29},
			{31, 40, 10, 29}, {31, 41, 10, 29}, {31, 42, 10, 29}, {31, 43, 10, 29}, {31, 44, 10, 29},
			{31, 45, 10, 29}, {31, 46, 10, 29}, {31, 47, 10, 29}, {31, 48, 10, 29}, {31, 49, 10, 29},
			{31, 50, 10, 29}, {31, 51, 10, 29}, {31, 52, 10, 29}, {31, 53, 10, 29}, {31, 54, 10, 29},
			{31, 55, 10, 29}, {31, 56, 10, 29}, {31, 57, 10, 29}, {31, 58, 10, 29}, {31, 59, 10, 29},
			{32, 40, 10, 29}, {32, 41, 10, 29}, {32, 42, 10, 29}, {32, 43, 10, 29}, {32, 44, 10, 29},
			{32, 45, 10, 29}, {32, 46, 10, 29}, {32, 47, 10, 29}, {32, 48, 10, 29}, {32, 49, 10, 29},
			{32, 50, 10, 29}, {32, 51, 10, 29}, {32, 52, 10, 29}, {32, 53, 10, 29}, {32, 54, 10, 29},
			{32, 55, 10, 29}, {32, 56, 10, 29}, {32, 57, 10, 29}, {32, 58, 10, 29}, {32, 59, 10, 29},
			{33, 40, 10, 29}, {33, 41, 10, 29}, {33, 42, 10, 29}, {33, 43, 10, 29}, {33, 44, 10, 29},
			{33, 45, 10, 29}, {33, 46, 10, 29}, {33, 47, 10, 29}, {33, 48, 10, 29}, {33, 49, 10, 29},
			{33, 50, 10, 29}, {33, 51, 10, 29}, {33, 52, 10, 29}, {33, 53, 10, 29}, {33, 54, 10, 29},
			{33, 55, 10, 29}, {33, 56, 10, 29}, {33, 57, 10, 29}, {33, 58, 10, 29}, {33, 59, 10, 29},
			{34, 40, 10, 29}, {34, 41, 10, 29}, {34, 42, 10, 29}, {34, 43, 10, 29}, {34, 44, 10, 29},
			{34, 45, 10, 29}, {34, 46, 10, 29}, {34, 47, 10, 29}, {34, 48, 10, 29}, {34, 49, 10, 29},
			{34, 50, 10, 29}, {34, 51, 10, 29}, {34, 52, 10, 29}, {34, 53, 10, 29}, {34, 54, 10, 29},
			{34, 55, 10, 29}, {34, 56, 10, 29}, {34, 57, 10, 29}, {34, 58, 10, 29}, {34, 59, 10, 29},
			{35, 40, 10, 29}, {35, 41, 10, 29}, {35, 42, 10, 29}, {35, 43, 10, 29}, {35, 44, 10, 29},
			{35, 45, 10, 29}, {35, 46, 10, 29}, {35, 47, 10, 29}, {35, 48, 10, 29}, {35, 49, 10, 29},
			{35, 50, 10, 29}, {35, 51, 10, 29}, {35, 52, 10, 29}, {35, 53, 10, 29}, {35, 54, 10, 29},
			{35, 55, 10, 29}, {35, 56, 10, 29}, {35, 57, 10, 29}, {35, 58, 10, 29}, {35, 59, 10, 29},
			{36, 40, 10, 29}, {36, 41, 10, 29}, {36, 42, 10, 29}, {36, 43, 10, 29}, {36, 44, 10, 29},
			{36, 45, 10, 29}, {36, 46, 10, 29}, {36, 47, 10, 29}, {36, 48, 10, 29}, {36, 49, 10, 29},
			{36, 50, 10, 29}, {36, 51, 10, 29}, {36, 52, 10, 29}, {36, 53, 10, 29}, {36, 54, 10, 29},
			{36, 55, 10, 29}, {36, 56, 10, 29}, {36, 57, 10, 29}, {36, 58, 10, 29}, {36, 59, 10, 29},
			{37, 40, 10, 29}, {37, 41, 10, 29}, {37, 42, 10, 29}, {37, 43, 10, 29}, {37, 44, 10, 29},
			{37, 45, 10, 29}, {37, 46, 10, 29}, {37, 47, 10, 29}, {37, 48, 10, 29}, {37, 49, 10, 29},
			{37, 50, 10, 29}, {37, 51, 10, 29}, {37, 52, 10, 29}, {37, 53, 10, 29}, {37, 54, 10, 29},
			{37, 55, 10, 29}, {37, 56, 10, 29}, {37, 57, 10, 29}, {37, 58, 10, 29}, {37, 59, 10, 29},
			{38, 40, 10, 29}, {38, 41, 10, 29}, {38, 42, 10, 29}, {38, 43, 10, 29}, {38, 44, 10, 29},
			{38, 45, 10, 29}, {38, 46, 10, 29}, {38, 47, 10, 29}, {38, 48, 10, 29}, {38, 49, 10, 29},
			{38, 50, 10, 29}, {38, 51, 10, 29}, {38, 52, 10, 29}, {38, 53, 10, 29}, {38, 54, 10, 29},
			{38, 55, 10, 29}, {38, 56, 10, 29}, {38, 57, 10, 29}, {38, 58, 10, 29}, {38, 59, 10, 29},
			{39, 40, 10, 29}, {39, 41, 10, 29}, {39, 42, 10, 29}, {39, 43, 10, 29}, {39, 44, 10, 29},
			{39, 45, 10, 29}, {39, 46, 10, 29}, {39, 47, 10, 29}, {39, 48, 10, 29}, {39, 49, 10, 29},
			{39, 50, 10, 29}, {39, 51, 10, 29}, {39, 52, 10, 29}, {39, 53, 10, 29}, {39, 54, 10, 29},
			{39, 55, 10, 29}, {39, 56, 10, 29}, {39, 57, 10, 29}, {39, 58, 10, 29}, {39, 59, 10, 29},
			{40, 40, 10, 29}, {40, 41, 10, 29}, {40, 42, 10, 29}, {40, 43, 10, 29}, {40, 44, 10, 29},
			{40, 45, 10, 29}, {40, 46, 10, 29}, {40, 47, 10, 29}, {40, 48, 10, 29}, {40, 49, 10, 29},
			{40, 50, 10, 29}, {40, 51, 10, 29}, {40, 52, 10, 29}, {40, 53, 10, 29}, {40, 54, 10, 29},
			{40, 55, 10, 29}, {40, 56, 10, 29}, {40, 57, 10, 29}, {40, 58, 10, 29}, {40, 59, 10, 29},
			{41, 40, 10, 29}, {41, 41, 10, 29}, {41, 42, 10, 29}, {41, 43, 10, 29}, {41, 44, 10, 29},
			{41, 45, 10, 29}, {41, 46, 10, 29}, {41, 47, 10, 29}, {41, 48, 10, 29}, {41, 49, 10, 29},
			{41, 50, 10, 29}, {41, 51, 10, 29}, {41, 52, 10, 29}, {41, 53, 10, 29}, {41, 54, 10, 29},
			{41, 55, 10, 29}, {41, 56, 10, 29}, {41, 57, 10, 29}, {41, 58, 10, 29}, {41, 59, 10, 29},
			{42, 40, 10, 29}, {42, 41, 10, 29}, {42, 42, 10, 29}, {42, 43, 10, 29}, {42, 44, 10, 29},
			{42, 45, 10, 29}, {42, 46, 10, 29}, {42, 47, 10, 29}, {42, 48, 10, 29}, {42, 49, 10, 29},
			{42, 50, 10, 29}, {42, 51, 10, 29}, {42, 52, 10, 29}, {42, 53, 10, 29}, {42, 54, 10, 29},
			{42, 55, 10, 29}, {42, 56, 10, 29}, {42, 57, 10, 29}, {42, 58, 10, 29}, {42, 59, 10, 29},
			{43, 40, 10, 29}, {43, 41, 10, 29}, {43, 42, 10, 29}, {43, 43, 10, 29}, {43, 44, 10, 29},
			{43, 45, 10, 29}, {43, 46, 10, 29}, {43, 47, 10, 29}, {43, 48, 10, 29}, {43, 49, 10, 29},
			{43, 50, 10, 29}, {43, 51, 10, 29}, {43, 52, 10, 29}, {43, 53, 10, 29}, {43, 54, 10, 29},
			{43, 55, 10, 29}, {43, 56, 10, 29}, {43, 57, 10, 29}, {43, 58, 10, 29}, {43, 59, 10, 29},
			{44, 40, 10, 29}, {44, 41, 10, 29}, {44, 42, 10, 29}, {44, 43, 10, 29}, {44, 44, 10, 29},
			{44, 45, 10, 29}, {44, 46, 10, 29}, {44, 47, 10, 29}, {44, 48, 10, 29}, {44, 49, 10, 29},
			{44, 50, 10, 29}, {44, 51, 10, 29}, {44, 52, 10, 29}, {44, 53, 10, 29}, {44, 54, 10, 29},
			{44, 55, 10, 29}, {44, 56, 10, 29}, {44, 57, 10, 29}, {44, 58, 10, 29}, {44, 59, 10, 29},
			{45, 40, 10, 29}, {45, 41, 10, 29}, {45, 42, 10, 29}, {45, 43, 10, 29}, {45, 44, 10, 29},
			{45, 45, 10, 29}, {45, 46, 10, 29}, {45, 47, 10, 29}, {45, 48, 10, 29}, {45, 49, 10, 29},
			{45, 50, 10, 29}, {45, 51, 10, 29}, {45, 52, 10, 29}, {45, 53, 10, 29}, {45, 54, 10, 29},
			{45, 55, 10, 29}, {45, 56, 10, 29}, {45, 57, 10, 29}, {45, 58, 10, 29}, {45, 59, 10, 29},
			{46, 40, 10, 29}, {46, 41, 10, 29}, {46, 42, 10, 29}, {46, 43, 10, 29}, {46, 44, 10, 29},
			{46, 45, 10, 29}, {46, 46, 10, 29}, {46, 47, 10, 29}, {46, 48, 10, 29}, {46, 49, 10, 29},
			{46, 50, 10, 29}, {46, 51, 10, 29}, {46, 52, 10, 29}, {46, 53, 10, 29}, {46, 54, 10, 29},
			{46, 55, 10, 29}, {46, 56, 10, 29}, {46, 57, 10, 29}, {46, 58, 10, 29}, {46, 59, 10, 29},
			{47, 40, 10, 29}, {47, 41, 10, 29}, {47, 42, 10, 29}, {47, 43, 10, 29}, {47, 44, 10, 29},
			{47, 45, 10, 29}, {47, 46, 10, 29}, {47, 47, 10, 29}, {47, 48, 10, 29}, {47, 49, 10, 29},
			{47, 50, 10, 29}, {47, 51, 10, 29}, {47, 52, 10, 29}, {47, 53, 10, 29}, {47, 54, 10, 29},
			{47, 55, 10, 29}, {47, 56, 10, 29}, {47, 57, 10, 29}, {47, 58, 10, 29}, {47, 59, 10, 29},
			{48, 40, 10, 29}, {48, 41, 10, 29}, {48, 42, 10, 29}, {48, 43, 10, 29}, {48, 44, 10, 29},
			{48, 45, 10, 29}, {48, 46, 10, 29}, {48, 47, 10, 29}, {48, 48, 10, 29}, {48, 49, 10, 29},
			{48, 50, 10, 29}, {48, 51, 10, 29}, {48, 52, 10, 29}, {48, 53, 10, 29}, {48, 54, 10, 29},
			{48, 55, 10, 29}, {48, 56, 10, 29}, {48, 57, 10, 29}, {48, 58, 10, 29}, {48, 59, 10, 29},
			{49, 40, 10, 29}, {49, 41, 10, 29}, {49, 42, 10, 29}, {49, 43, 10, 29}, {49, 44, 10, 29},
			{49, 45, 10, 29}, {49, 46, 10, 29}, {49, 47, 10, 29}, {49, 48, 10, 29}, {49, 49, 10, 29},
			{49, 50, 10, 29}, {49, 51, 10, 29}, {49, 52, 10, 29}, {49, 53, 10, 29}, {49, 54, 10, 29},
			{49, 55, 10, 29}, {49, 56, 10, 29}, {49, 57, 10, 29}, {49, 58, 10, 29}, {49, 59, 10, 29},
			{50, 40, 10, 29}, {50, 41, 10, 29}, {50, 42, 10, 29}, {50, 43, 10, 29}, {50, 44, 10, 29},
			{50, 45, 10, 29}, {50, 46, 10, 29}, {50, 47, 10, 29}, {50, 48, 10, 29}, {50, 49, 10, 29},
			{50, 50, 10, 29}, {50, 51, 10, 29}, {50, 52, 10, 29}, {50, 53, 10, 29}, {50, 54, 10, 29},
			{50, 55, 10, 29}, {50, 56, 10, 29}, {50, 57, 10, 29}, {50, 58, 10, 29}, {50, 59, 10, 29},
			{51, 40, 10, 29}, {51, 41, 10, 29}, {51, 42, 10, 29}, {51, 43, 10, 29}, {51, 44, 10, 29},
			{51, 45, 10, 29}, {51, 46, 10, 29}, {51, 47, 10, 29}, {51, 48, 10, 29}, {51, 49, 10, 29},
			{51, 50, 10, 29}, {51, 51, 10, 29}, {51, 52, 10, 29}, {51, 53, 10, 29}, {51, 54, 10, 29},
			{51, 55, 10, 29}, {51, 56, 10, 29}, {51, 57, 10, 29}, {51, 58, 10, 29}, {51, 59, 10, 29},
			{52, 40, 10, 29}, {52, 41, 10, 29}, {52, 42, 10, 29}, {52, 43, 10, 29}, {52, 44, 10, 29},
			{52, 45, 10, 29}, {52, 46, 10, 29}, {52, 47, 10, 29}, {52, 48, 10, 29}, {52, 49, 10, 29},
			{52, 50, 10, 29}, {52, 51, 10, 29}, {52, 52, 10, 29}, {52, 53, 10, 29}, {52, 54, 10, 29},
			{52, 55, 10, 29}, {52, 56, 10, 29}, {52, 57, 10, 29}, {52, 58, 10, 29}, {52, 59, 10, 29},
			{53, 40, 10, 29}, {53, 41, 10, 29}, {53, 42, 10, 29}, {53, 43, 10, 29}, {53, 44, 10, 29},
			{53, 45, 10, 29}, {53, 46, 10, 29}, {53, 47, 10, 29}, {53, 48, 10, 29}, {53, 49, 10, 29},
			{53, 50, 10, 29}, {53, 51, 10, 29}, {53, 52, 10, 29}, {53, 53, 10, 29}, {53, 54, 10, 29},
			{53, 55, 10, 29}, {53, 56, 10, 29}, {53, 57, 10, 29}, {53, 58, 10, 29}, {53, 59, 10, 29},
			{54, 40, 10, 29}, {54, 41, 10, 29}, {54, 42, 10, 29}, {54, 43, 10, 29}, {54, 44, 10, 29},
			{54, 45, 10, 29}, {54, 46, 10, 29}, {54, 47, 10, 29}, {54, 48, 10, 29}, {54, 49, 10, 29},
			{54, 50, 10, 29}, {54, 51, 10, 29}, {54, 52, 10, 29}, {54, 53, 10, 29}, {54, 54, 10, 29},
			{54, 55, 10, 29}, {54, 56, 10, 29}, {54, 57, 10, 29}, {54, 58, 10, 29}, {54, 59, 10, 29},
			{55, 40, 10, 29}, {55, 41, 10, 29}, {55, 42, 10, 29}, {55, 43, 10, 29}, {55, 44, 10, 29},
			{55, 45, 10, 29}, {55, 46, 10, 29}, {55, 47, 10, 29}, {55, 48, 10, 29}, {55, 49, 10, 29},
			{55, 50, 10, 29}, {55, 51, 10, 29}, {55, 52, 10, 29}, {55, 53, 10, 29}, {55, 54, 10, 29},
			{55, 55, 10, 29}, {55, 56, 10, 29}, {55, 57, 10, 29}, {55, 58, 10, 29}, {55, 59, 10, 29},
			{56, 40, 10, 29}, {56, 41, 10, 29}, {56, 42, 10, 29}, {56, 43, 10, 29}, {56, 44, 10, 29},
			{56, 45, 10, 29}, {56, 46, 10, 29}, {56, 47, 10, 29}, {56, 48, 10, 29}, {56, 49, 10, 29},
			{56, 50, 10, 29}, {56, 51, 10, 29}, {56, 52, 10, 29}, {56, 53, 10, 29}, {56, 54, 10, 29},
			{56, 55, 10, 29}, {56, 56, 10, 29}, {56, 57, 10, 29}, {56, 58, 10, 29}, {56, 59, 10, 29},
			{57, 40, 10, 29}, {57, 41, 10, 29}, {57, 42, 10, 29}, {57, 43, 10, 29}, {57, 44, 10, 29},
			{57, 45, 10, 29}, {57, 46, 10, 29}, {57, 47, 10, 29}, {57, 48, 10, 29}, {57, 49, 10, 29},
			{57, 50, 10, 29}, {57, 51, 10, 29}, {57, 52, 10, 29}, {57, 53, 10, 29}, {57, 54, 10, 29},
			{57, 55, 10, 29}, {57, 56, 10, 29}, {57, 57, 10, 29}, {57, 58, 10, 29}, {57, 59, 10, 29},
			{58, 40, 10, 29}, {58, 41, 10, 29}, {58, 42, 10, 29}, {58, 43, 10, 29}, {58, 44, 10, 29},
			{58, 45, 10, 29}, {58, 46, 10, 29}, {58, 47, 10, 29}, {58, 48, 10, 29}, {58, 49, 10, 29},
			{58, 50, 10, 29}, {58, 51, 10, 29}, {58, 52, 10, 29}, {58, 53, 10, 29}, {58, 54, 10, 29},
			{58, 55, 10, 29}, {58, 56, 10, 29}, {58, 57, 10, 29}, {58, 58, 10, 29}, {58, 59, 10, 29},
			{59, 40, 10, 29}, {59, 41, 10, 29}, {59, 42, 10, 29}, {59, 43, 10, 29}, {59, 44, 10, 29},
			{59, 45, 10, 29}, {59, 46, 10, 29}, {59, 47, 10, 29}, {59, 48, 10, 29}, {59, 49, 10, 29},
			{59, 50, 10, 29}, {59, 51, 10, 29}, {59, 52, 10, 29}, {59, 53, 10, 29}, {59, 54, 10, 29},
			{59, 55, 10, 29}, {59, 56, 10, 29}, {59, 57, 10, 29}, {59, 58, 10, 29}, {59, 59, 10, 29},
			{60, 40, 10, 29}, {60, 41, 10, 29}, {60, 42, 10, 29}, {60, 43, 10, 29}, {60, 44, 10, 29},
			{60, 45, 10, 29}, {60, 46, 10, 29}, {60, 47, 10, 29}, {60, 48, 10, 29}, {60, 49, 10, 29},
			{60, 50, 10, 29}, {60, 51, 10, 29}, {60, 52, 10, 29}, {60, 53, 10, 29}, {60, 54, 10, 29},
			{60, 55, 10, 29}, {60, 56, 10, 29}, {60, 57, 10, 29}, {60, 58, 10, 29}, {60, 59, 10, 29},
			{61, 40, 10, 29}, {61, 41, 10, 29}, {61, 42, 10, 29}, {61, 43, 10, 29}, {61, 44, 10, 29},
			{61, 45, 10, 29}, {61, 46, 10, 29}, {61, 47, 10, 29}, {61, 48, 10, 29}, {61, 49, 10, 29},
			{61, 50, 10, 29}, {61, 51, 10, 29}, {61, 52, 10, 29}, {61, 53, 10, 29}, {61, 54, 10, 29},
			{61, 55, 10, 29}, {61, 56, 10, 29}, {61, 57, 10, 29}, {61, 58, 10, 29}, {61, 59, 10, 29},
			{62, 40, 10, 29}, {62, 41, 10, 29}, {62, 42, 10, 29}, {62, 43, 10, 29}, {62, 44, 10, 29},
			{62, 45, 10, 29}, {62, 46, 10, 29}, {62, 47, 10, 29}, {62, 48, 10, 29}, {62, 49, 10, 29},
			{62, 50, 10, 29}, {62, 51, 10, 29}, {62, 52, 10, 29}, {62, 53, 10, 29}, {62, 54, 10, 29},
			{62, 55, 10, 29}, {62, 56, 10, 29}, {62, 57, 10, 29}, {62, 58, 10, 29}, {62, 59, 10, 29},
			{63, 40, 10, 29}, {63, 41, 10, 29}, {63, 42, 10, 29}, {63, 43, 10, 29}, {63, 44, 10, 29},
			{63, 45, 10, 29}, {63, 46, 10, 29}, {63, 47, 10, 29}, {63, 48, 10, 29}, {63, 49, 10, 29},
			{63, 50, 10, 29}, {63, 51, 10, 29}, {63, 52, 10, 29}, {63, 53, 10, 29}, {63, 54, 10, 29},
			{63, 55, 10, 29}, {63, 56, 10, 29}, {63, 57, 10, 29}, {63, 58, 10, 29}, {63, 59, 10, 29},
			{64, 40, 10, 29}, {64, 41, 10, 29}, {64, 42, 10, 29}, {64, 43, 10, 29}, {64, 44, 10, 29},
			{64, 45, 10, 29}, {64, 46, 10, 29}, {64, 47, 10, 29}, {64, 48, 10, 29}, {64, 49, 10, 29},
			{64, 50, 10, 29}, {64, 51, 10, 29}, {64, 52, 10, 29}, {64, 53, 10, 29}, {64, 54, 10, 29},
			{64, 55, 10, 29}, {64, 56, 10, 29}, {64, 57, 10, 29}, {64, 58, 10, 29}, {64, 59, 10, 29},
			{65, 40, 10, 29}, {65, 41, 10, 29}, {65, 42, 10, 29}, {65, 43, 10, 29}, {65, 44, 10, 29},
			{65, 45, 10, 29}, {65, 46, 10, 29}, {65, 47, 10, 29}, {65, 48, 10, 29}, {65, 49, 10, 29},
			{65, 50, 10, 29}, {65, 51, 10, 29}, {65, 52, 10, 29}, {65, 53, 10, 29}, {65, 54, 10, 29},
			{65, 55, 10, 29}, {65, 56, 10, 29}, {65, 57, 10, 29}, {65, 58, 10, 29}, {65, 59, 10, 29},
			{66, 40, 10, 29}, {66, 41, 10, 29}, {66, 42, 10, 29}, {66, 43, 10, 29}, {66, 44, 10, 29},
			{66, 45, 10, 29}, {66, 46, 10, 29}, {66, 47, 10, 29}, {66, 48, 10, 29}, {66, 49, 10, 29},
			{66, 50, 10, 29}, {66, 51, 10, 29}, {66, 52, 10, 29}, {66, 53, 10, 29}, {66, 54, 10, 29},
			{66, 55, 10, 29}, {66, 56, 10, 29}, {66, 57, 10, 29}, {66, 58, 10, 29}, {66, 59, 10, 29},
			{67, 40, 10, 29}, {67, 41, 10, 29}, {67, 42, 10, 29}, {67, 43, 10, 29}, {67, 44, 10, 29},
			{67, 45, 10, 29}, {67, 46, 10, 29}, {67, 47, 10, 29}, {67, 48, 10, 29}, {67, 49, 10, 29},
			{67, 50, 10, 29}, {67, 51, 10, 29}, {67, 52, 10, 29}, {67, 53, 10, 29}, {67, 54, 10, 29},
			{67, 55, 10, 29}, {67, 56, 10, 29}, {67, 57, 10, 29}, {67, 58, 10, 29}, {67, 59, 10, 29},
			{68, 40, 10, 29}, {68, 41, 10, 29}, {68, 42, 10, 29}, {68, 43, 10, 29}, {68, 44, 10, 29},
			{68, 45, 10, 29}, {68, 46, 10, 29}, {68, 47, 10, 29}, {68, 48, 10, 29}, {68, 49, 10, 29},
			{68, 50, 10, 29}, {68, 51, 10, 29}, {68, 52, 10, 29}, {68, 53, 10, 29}, {68, 54, 10, 29},
			{68, 55, 10, 29}, {68, 56, 10, 29}, {68, 57, 10, 29}, {68, 58, 10, 29}, {68, 59, 10, 29},
			{69, 40, 10, 29}, {69, 41, 10, 29}, {69, 42, 10, 29}, {69, 43, 10, 29}, {69, 44, 10, 29},
			{69, 45, 10, 29}, {69, 46, 10, 29}, {69, 47, 10, 29}, {69, 48, 10, 29}, {69, 49, 10, 29},
			{69, 50, 10, 29}, {69, 51, 10, 29}, {69, 52, 10, 29}, {69, 53, 10, 29}, {69, 54, 10, 29},
			{69, 55, 10, 29}, {69, 56, 10, 29}, {69, 57, 10, 29}, {69, 58, 10, 29}, {69, 59, 10, 29},
			{70, 40, 10, 29}, {70, 41, 10, 29}, {70, 42, 10, 29}, {70, 43, 10, 29}, {70, 44, 10, 29},
			{70, 45, 10, 29}, {70, 46, 10, 29}, {70, 47, 10, 29}, {70, 48, 10, 29}, {70, 49, 10, 29},
			{70, 50, 10, 29}, {70, 51, 10, 29}, {70, 52, 10, 29}, {70, 53, 10, 29}, {70, 54, 10, 29},
			{70, 55, 10, 29}, {70, 56, 10, 29}, {70, 57, 10, 29}, {70, 58, 10, 29}, {70, 59, 10, 29},
			{71, 40, 10, 29}, {71, 41, 10, 29}, {71, 42, 10, 29}, {71, 43, 10, 29}, {71, 44, 10, 29},
			{71, 45, 10, 29}, {71, 46, 10, 29}, {71, 47, 10, 29}, {71, 48, 10, 29}, {71, 49, 10, 29},
			{71, 50, 10, 29}, {71, 51, 10, 29}, {71, 52, 10, 29}, {71, 53, 10, 29}, {71, 54, 10, 29},
			{71, 55, 10, 29}, {71, 56, 10, 29}, {71, 57, 10, 29}, {71, 58, 10, 29}, {71, 59, 10, 29},
			{72, 40, 10, 29}, {72, 41, 10, 29}, {72, 42, 10, 29}, {72, 43, 10, 29}, {72, 44, 10, 29},
			{72, 45, 10, 29}, {72, 46, 10, 29}, {72, 47, 10, 29}, {72, 48, 10, 29}, {72, 49, 10, 29},
			{72, 50, 10, 29}, {72, 51, 10, 29}, {72, 52, 10, 29}, {72, 53, 10, 29}, {72, 54, 10, 29},
			{72, 55, 10, 29}, {72, 56, 10, 29}, {72, 57, 10, 29}, {72, 58, 10, 29}, {72, 59, 10, 29},
			{73, 40, 10, 29}, {73, 41, 10, 29}, {73, 42, 10, 29}, {73, 43, 10, 29}, {73, 44, 10, 29},
			{73, 45, 10, 29}, {73, 46, 10, 29}, {73, 47, 10, 29}, {73, 48, 10, 29}, {73, 49, 10, 29},
			{73, 50, 10, 29}, {73, 51, 10, 29}, {73, 52, 10, 29}, {73, 53, 10, 29}, {73, 54, 10, 29},
			{73, 55, 10, 29}, {73, 56, 10, 29}, {73, 57, 10, 29}, {73, 58, 10, 29}, {73, 59, 10, 29},
			{74, 40, 10, 29}, {74, 41, 10, 29}, {74, 42, 10, 29}, {74, 43, 10, 29}, {74, 44, 10, 29},
			{74, 45, 10, 29}, {74, 46, 10, 29}, {74, 47, 10, 29}, {74, 48, 10, 29}, {74, 49, 10, 29},
			{74, 50, 10, 29}, {74, 51, 10, 29}, {74, 52, 10, 29}, {74, 53, 10, 29}, {74, 54, 10, 29},
			{74, 55, 10, 29}, {74, 56, 10, 29}, {74, 57, 10, 29}, {74, 58, 10, 29}, {74, 59, 10, 29},
			{75, 40, 10, 29}, {75, 41, 10, 29}, {75, 42, 10, 29}, {75, 43, 10, 29}, {75, 44, 10, 29},
			{75, 45, 10, 29}, {75, 46, 10, 29}, {75, 47, 10, 29}, {75, 48, 10, 29}, {75, 49, 10, 29},
			{75, 50, 10, 29}, {75, 51, 10, 29}, {75, 52, 10, 29}, {75, 53, 10, 29}, {75, 54, 10, 29},
			{75, 55, 10, 29}, {75, 56, 10, 29}, {75, 57, 10, 29}, {75, 58, 10, 29}, {75, 59, 10, 29},
			{76, 40, 10, 29}, {76, 41, 10, 29}, {76, 42, 10, 29}, {76, 43, 10, 29}, {76, 44, 10, 29},
			{76, 45, 10, 29}, {76, 46, 10, 29}, {76, 47, 10, 29}, {76, 48, 10, 29}, {76, 49, 10, 29},
			{76, 50, 10, 29}, {76, 51, 10, 29}, {76, 52, 10, 29}, {76, 53, 10, 29}, {76, 54, 10, 29},
			{76, 55, 10, 29}, {76, 56, 10, 29}, {76, 57, 10, 29}, {76, 58, 10, 29}, {76, 59, 10, 29},
			{77, 40, 10, 29}, {77, 41, 10, 29}, {77, 42, 10, 29}, {77, 43, 10, 29}, {77, 44, 10, 29},
			{77, 45, 10, 29}, {77, 46, 10, 29}, {77, 47, 10, 29}, {77, 48, 10, 29}, {77, 49, 10, 29},
			{77, 50, 10, 29}, {77, 51, 10, 29}, {77, 52, 10, 29}, {77, 53, 10, 29}, {77, 54, 10, 29},
			{77, 55, 10, 29}, {77, 56, 10, 29}, {77, 57, 10, 29}, {77, 58, 10, 29}, {77, 59, 10, 29},
			{78, 40, 10, 29}, {78, 41, 10, 29}, {78, 42, 10, 29}, {78, 43, 10, 29}, {78, 44, 10, 29},
			{78, 45, 10, 29}, {78, 46, 10, 29}, {78, 47, 10, 29}, {78, 48, 10, 29}, {78, 49, 10, 29},
			{78, 50, 10, 29}, {78, 51, 10, 29}, {78, 52, 10, 29}, {78, 53, 10, 29}, {78, 54, 10, 29},
			{78, 55, 10, 29}, {78, 56, 10, 29}, {78, 57, 10, 29}, {78, 58, 10, 29}, {78, 59, 10, 29},
			{79, 40, 10, 29}, {79, 41, 10, 29}, {79, 42, 10, 29}, {79, 43, 10, 29}, {79, 44, 10, 29},
			{79, 45, 10, 29}, {79, 46, 10, 29}, {79, 47, 10, 29}, {79, 48, 10, 29}, {79, 49, 10, 29},
			{79, 50, 10, 29}, {79, 51, 10, 29}, {79, 52, 10, 29}, {79, 53, 10, 29}, {79, 54, 10, 29},
			{79, 55, 10, 29}, {79, 56, 10, 29}, {79, 57, 10, 29}, {79, 58, 10, 29}, {79, 59, 10, 29},
			{80, 40, 10, 29}, {80, 41, 10, 29}, {80, 42, 10, 29}, {80, 43, 10, 29}, {80, 44, 10, 29},
			{80, 45, 10, 29}, {80, 46, 10, 29}, {80, 47, 10, 29}, {80, 48, 10, 29}, {80, 49, 10, 29},
			{80, 50, 10, 29}, {80, 51, 10, 29}, {80, 52, 10, 29}, {80, 53, 10, 29}, {80, 54, 10, 29},
			{80, 55, 10, 29}, {80, 56, 10, 29}, {80, 57, 10, 29}, {80, 58, 10, 29}, {80, 59, 10, 29},
			{81, 40, 10, 29}, {81, 41, 10, 29}, {81, 42, 10, 29}, {81, 43, 10, 29}, {81, 44, 10, 29},
			{81, 45, 10, 29}, {81, 46, 10, 29}, {81, 47, 10, 29}, {81, 48, 10, 29}, {81, 49, 10, 29},
			{81, 50, 10, 29}, {81, 51, 10, 29}, {81, 52, 10, 29}, {81, 53, 10, 29}, {81, 54, 10, 29},
			{81, 55, 10, 29}, {81, 56, 10, 29}, {81, 57, 10, 29}, {81, 58, 10, 29}, {81, 59, 10, 29},
			{82, 40, 10, 29}, {82, 41, 10, 29}, {82, 42, 10, 29}, {82, 43, 10, 29}, {82, 44, 10, 29},
			{82, 45, 10, 29}, {82, 46, 10, 29}, {82, 47, 10, 29}, {82, 48, 10, 29}, {82, 49, 10, 29},
			{82, 50, 10, 29}, {82, 51, 10, 29}, {82, 52, 10, 29}, {82, 53, 10, 29}, {82, 54, 10, 29},
			{82, 55, 10, 29}, {82, 56, 10, 29}, {82, 57, 10, 29}, {82, 58, 10, 29}, {82, 59, 10, 29},
			{83, 40, 10, 29}, {83, 41, 10, 29}, {83, 42, 10, 29}, {83, 43, 10, 29}, {83, 44, 10, 29},
			{83, 45, 10, 29}, {83, 46, 10, 29}, {83, 47, 10, 29}, {83, 48, 10, 29}, {83, 49, 10, 29},
			{83, 50, 10, 29}, {83, 51, 10, 29}, {83, 52, 10, 29}, {83, 53, 10, 29}, {83, 54, 10, 29},
			{83, 55, 10, 29}, {83, 56, 10, 29}, {83, 57, 10, 29}, {83, 58, 10, 29}, {83, 59, 10, 29},
			{84, 40, 10, 29}, {84, 41, 10, 29}, {84, 42, 10, 29}, {84, 43, 10, 29}, {84, 44, 10, 29},
			{84, 45, 10, 29}, {84, 46, 10, 29}, {84, 47, 10, 29}, {84, 48, 10, 29}, {84, 49, 10, 29},
			{84, 50, 10, 29}, {84, 51, 10, 29}, {84, 52, 10, 29}, {84, 53, 10, 29}, {84, 54, 10, 29},
			{84, 55, 10, 29}, {84, 56, 10, 29}, {84, 57, 10, 29}, {84, 58, 10, 29}, {84, 59, 10, 29},
			{85, 40, 10, 29}, {85, 41, 10, 29}, {85, 42, 10, 29}, {85, 43, 10, 29}, {85, 44, 10, 29},
			{85, 45, 10, 29}, {85, 46, 10, 29}, {85, 47, 10, 29}, {85, 48, 10, 29}, {85, 49, 10, 29},
			{85, 50, 10, 29}, {85, 51, 10, 29}, {85, 52, 10, 29}, {85, 53, 10, 29}, {85, 54, 10, 29},
			{85, 55, 10, 29}, {85, 56, 10, 29}, {85, 57, 10, 29}, {85, 58, 10, 29}, {85, 59, 10, 29},
			{86, 40, 10, 29}, {86, 41, 10, 29}, {86, 42, 10, 29}, {86, 43, 10, 29}, {86, 44, 10, 29},
			{86, 45, 10, 29}, {86, 46, 10, 29}, {86, 47, 10, 29}, {86, 48, 10, 29}, {86, 49, 10, 29},
			{86, 50, 10, 29}, {86, 51, 10, 29}, {86, 52, 10, 29}, {86, 53, 10, 29}, {86, 54, 10, 29},
			{86, 55, 10, 29}, {86, 56, 10, 29}, {86, 57, 10, 29}, {86, 58, 10, 29}, {86, 59, 10, 29},
			{87, 40, 10, 29}, {87, 41, 10, 29}, {87, 42, 10, 29}, {87, 43, 10, 29}, {87, 44, 10, 29},
			{87, 45, 10, 29}, {87, 46, 10, 29}, {87, 47, 10, 29}, {87, 48, 10, 29}, {87, 49, 10, 29},
			{87, 50, 10, 29}, {87, 51, 10, 29}, {87, 52, 10, 29}, {87, 53, 10, 29}, {87, 54, 10, 29},
			{87, 55, 10, 29}, {87, 56, 10, 29}, {87, 57, 10, 29}, {87, 58, 10, 29}, {87, 59, 10, 29},
			{88, 40, 10, 29}, {88, 41, 10, 29}, {88, 42, 10, 29}, {88, 43, 10, 29}, {88, 44, 10, 29},
			{88, 45, 10, 29}, {88, 46, 10, 29}, {88, 47, 10, 29}, {88, 48, 10, 29}, {88, 49, 10, 29},
			{88, 50, 10, 29}, {88, 51, 10, 29}, {88, 52, 10, 29}, {88, 53, 10, 29}, {88, 54, 10, 29},
			{88, 55, 10, 29}, {88, 56, 10, 29}, {88, 57, 10, 29}, {88, 58, 10, 29}, {88, 59, 10, 29},
			{89, 40, 10, 29}, {89, 41, 10, 29}, {89, 42, 10, 29}, {89, 43, 10, 29}, {89, 44, 10, 29},
			{89, 45, 10, 29}, {89, 46, 10, 29}, {89, 47, 10, 29}, {89, 48, 10, 29}, {89, 49, 10, 29},
			{89, 50, 10, 29}, {89, 51, 10, 29}, {89, 52, 10, 29}, {89, 53, 10, 29}, {89, 54, 10, 29},
			{89, 55, 10, 29}, {89, 56, 10, 29}, {89, 57, 10, 29}, {89, 58, 10, 29}, {89, 59, 10, 29},
		},
	}
	body2 = testBody{
		label:  2,
		offset: dvid.Point3d{30, 20, 40},
		size:   dvid.Point3d{50, 50, 20},
		blockSpans: []dvid.Span{
			{1, 0, 0, 2},
			{1, 1, 0, 2},
			{1, 2, 0, 2},
		},
		voxelSpans: []dvid.Span{
			{40, 20, 30, 31}, {40, 21, 30, 31}, {40, 22, 30, 31}, {40, 23, 30, 31}, {40, 24, 30, 31},
			{40, 25, 30, 31}, {40, 26, 30, 31}, {40, 27, 30, 31}, {40, 28, 30, 31}, {40, 29, 30, 31},
			{40, 30, 30, 31}, {40, 31, 30, 31}, {41, 20, 30, 31}, {41, 21, 30, 31}, {41, 22, 30, 31},
			{41, 23, 30, 31}, {41, 24, 30, 31}, {41, 25, 30, 31}, {41, 26, 30, 31}, {41, 27, 30, 31},
			{41, 28, 30, 31}, {41, 29, 30, 31}, {41, 30, 30, 31}, {41, 31, 30, 31}, {42, 20, 30, 31},
			{42, 21, 30, 31}, {42, 22, 30, 31}, {42, 23, 30, 31}, {42, 24, 30, 31}, {42, 25, 30, 31},
			{42, 26, 30, 31}, {42, 27, 30, 31}, {42, 28, 30, 31}, {42, 29, 30, 31}, {42, 30, 30, 31},
			{42, 31, 30, 31}, {43, 20, 30, 31}, {43, 21, 30, 31}, {43, 22, 30, 31}, {43, 23, 30, 31},
			{43, 24, 30, 31}, {43, 25, 30, 31}, {43, 26, 30, 31}, {43, 27, 30, 31}, {43, 28, 30, 31},
			{43, 29, 30, 31}, {43, 30, 30, 31}, {43, 31, 30, 31}, {44, 20, 30, 31}, {44, 21, 30, 31},
			{44, 22, 30, 31}, {44, 23, 30, 31}, {44, 24, 30, 31}, {44, 25, 30, 31}, {44, 26, 30, 31},
			{44, 27, 30, 31}, {44, 28, 30, 31}, {44, 29, 30, 31}, {44, 30, 30, 31}, {44, 31, 30, 31},
			{45, 20, 30, 31}, {45, 21, 30, 31}, {45, 22, 30, 31}, {45, 23, 30, 31}, {45, 24, 30, 31},
			{45, 25, 30, 31}, {45, 26, 30, 31}, {45, 27, 30, 31}, {45, 28, 30, 31}, {45, 29, 30, 31},
			{45, 30, 30, 31}, {45, 31, 30, 31}, {46, 20, 30, 31}, {46, 21, 30, 31}, {46, 22, 30, 31},
			{46, 23, 30, 31}, {46, 24, 30, 31}, {46, 25, 30, 31}, {46, 26, 30, 31}, {46, 27, 30, 31},
			{46, 28, 30, 31}, {46, 29, 30, 31}, {46, 30, 30, 31}, {46, 31, 30, 31}, {47, 20, 30, 31},
			{47, 21, 30, 31}, {47, 22, 30, 31}, {47, 23, 30, 31}, {47, 24, 30, 31}, {47, 25, 30, 31},
			{47, 26, 30, 31}, {47, 27, 30, 31}, {47, 28, 30, 31}, {47, 29, 30, 31}, {47, 30, 30, 31},
			{47, 31, 30, 31}, {48, 20, 30, 31}, {48, 21, 30, 31}, {48, 22, 30, 31}, {48, 23, 30, 31},
			{48, 24, 30, 31}, {48, 25, 30, 31}, {48, 26, 30, 31}, {48, 27, 30, 31}, {48, 28, 30, 31},
			{48, 29, 30, 31}, {48, 30, 30, 31}, {48, 31, 30, 31}, {49, 20, 30, 31}, {49, 21, 30, 31},
			{49, 22, 30, 31}, {49, 23, 30, 31}, {49, 24, 30, 31}, {49, 25, 30, 31}, {49, 26, 30, 31},
			{49, 27, 30, 31}, {49, 28, 30, 31}, {49, 29, 30, 31}, {49, 30, 30, 31}, {49, 31, 30, 31},
			{50, 20, 30, 31}, {50, 21, 30, 31}, {50, 22, 30, 31}, {50, 23, 30, 31}, {50, 24, 30, 31},
			{50, 25, 30, 31}, {50, 26, 30, 31}, {50, 27, 30, 31}, {50, 28, 30, 31}, {50, 29, 30, 31},
			{50, 30, 30, 31}, {50, 31, 30, 31}, {51, 20, 30, 31}, {51, 21, 30, 31}, {51, 22, 30, 31},
			{51, 23, 30, 31}, {51, 24, 30, 31}, {51, 25, 30, 31}, {51, 26, 30, 31}, {51, 27, 30, 31},
			{51, 28, 30, 31}, {51, 29, 30, 31}, {51, 30, 30, 31}, {51, 31, 30, 31}, {52, 20, 30, 31},
			{52, 21, 30, 31}, {52, 22, 30, 31}, {52, 23, 30, 31}, {52, 24, 30, 31}, {52, 25, 30, 31},
			{52, 26, 30, 31}, {52, 27, 30, 31}, {52, 28, 30, 31}, {52, 29, 30, 31}, {52, 30, 30, 31},
			{52, 31, 30, 31}, {53, 20, 30, 31}, {53, 21, 30, 31}, {53, 22, 30, 31}, {53, 23, 30, 31},
			{53, 24, 30, 31}, {53, 25, 30, 31}, {53, 26, 30, 31}, {53, 27, 30, 31}, {53, 28, 30, 31},
			{53, 29, 30, 31}, {53, 30, 30, 31}, {53, 31, 30, 31}, {54, 20, 30, 31}, {54, 21, 30, 31},
			{54, 22, 30, 31}, {54, 23, 30, 31}, {54, 24, 30, 31}, {54, 25, 30, 31}, {54, 26, 30, 31},
			{54, 27, 30, 31}, {54, 28, 30, 31}, {54, 29, 30, 31}, {54, 30, 30, 31}, {54, 31, 30, 31},
			{55, 20, 30, 31}, {55, 21, 30, 31}, {55, 22, 30, 31}, {55, 23, 30, 31}, {55, 24, 30, 31},
			{55, 25, 30, 31}, {55, 26, 30, 31}, {55, 27, 30, 31}, {55, 28, 30, 31}, {55, 29, 30, 31},
			{55, 30, 30, 31}, {55, 31, 30, 31}, {56, 20, 30, 31}, {56, 21, 30, 31}, {56, 22, 30, 31},
			{56, 23, 30, 31}, {56, 24, 30, 31}, {56, 25, 30, 31}, {56, 26, 30, 31}, {56, 27, 30, 31},
			{56, 28, 30, 31}, {56, 29, 30, 31}, {56, 30, 30, 31}, {56, 31, 30, 31}, {57, 20, 30, 31},
			{57, 21, 30, 31}, {57, 22, 30, 31}, {57, 23, 30, 31}, {57, 24, 30, 31}, {57, 25, 30, 31},
			{57, 26, 30, 31}, {57, 27, 30, 31}, {57, 28, 30, 31}, {57, 29, 30, 31}, {57, 30, 30, 31},
			{57, 31, 30, 31}, {58, 20, 30, 31}, {58, 21, 30, 31}, {58, 22, 30, 31}, {58, 23, 30, 31},
			{58, 24, 30, 31}, {58, 25, 30, 31}, {58, 26, 30, 31}, {58, 27, 30, 31}, {58, 28, 30, 31},
			{58, 29, 30, 31}, {58, 30, 30, 31}, {58, 31, 30, 31}, {59, 20, 30, 31}, {59, 21, 30, 31},
			{59, 22, 30, 31}, {59, 23, 30, 31}, {59, 24, 30, 31}, {59, 25, 30, 31}, {59, 26, 30, 31},
			{59, 27, 30, 31}, {59, 28, 30, 31}, {59, 29, 30, 31}, {59, 30, 30, 31}, {59, 31, 30, 31},
			{40, 20, 32, 63}, {40, 21, 32, 63}, {40, 22, 32, 63}, {40, 23, 32, 63}, {40, 24, 32, 63},
			{40, 25, 32, 63}, {40, 26, 32, 63}, {40, 27, 32, 63}, {40, 28, 32, 63}, {40, 29, 32, 63},
			{40, 30, 32, 63}, {40, 31, 32, 63}, {41, 20, 32, 63}, {41, 21, 32, 63}, {41, 22, 32, 63},
			{41, 23, 32, 63}, {41, 24, 32, 63}, {41, 25, 32, 63}, {41, 26, 32, 63}, {41, 27, 32, 63},
			{41, 28, 32, 63}, {41, 29, 32, 63}, {41, 30, 32, 63}, {41, 31, 32, 63}, {42, 20, 32, 63},
			{42, 21, 32, 63}, {42, 22, 32, 63}, {42, 23, 32, 63}, {42, 24, 32, 63}, {42, 25, 32, 63},
			{42, 26, 32, 63}, {42, 27, 32, 63}, {42, 28, 32, 63}, {42, 29, 32, 63}, {42, 30, 32, 63},
			{42, 31, 32, 63}, {43, 20, 32, 63}, {43, 21, 32, 63}, {43, 22, 32, 63}, {43, 23, 32, 63},
			{43, 24, 32, 63}, {43, 25, 32, 63}, {43, 26, 32, 63}, {43, 27, 32, 63}, {43, 28, 32, 63},
			{43, 29, 32, 63}, {43, 30, 32, 63}, {43, 31, 32, 63}, {44, 20, 32, 63}, {44, 21, 32, 63},
			{44, 22, 32, 63}, {44, 23, 32, 63}, {44, 24, 32, 63}, {44, 25, 32, 63}, {44, 26, 32, 63},
			{44, 27, 32, 63}, {44, 28, 32, 63}, {44, 29, 32, 63}, {44, 30, 32, 63}, {44, 31, 32, 63},
			{45, 20, 32, 63}, {45, 21, 32, 63}, {45, 22, 32, 63}, {45, 23, 32, 63}, {45, 24, 32, 63},
			{45, 25, 32, 63}, {45, 26, 32, 63}, {45, 27, 32, 63}, {45, 28, 32, 63}, {45, 29, 32, 63},
			{45, 30, 32, 63}, {45, 31, 32, 63}, {46, 20, 32, 63}, {46, 21, 32, 63}, {46, 22, 32, 63},
			{46, 23, 32, 63}, {46, 24, 32, 63}, {46, 25, 32, 63}, {46, 26, 32, 63}, {46, 27, 32, 63},
			{46, 28, 32, 63}, {46, 29, 32, 63}, {46, 30, 32, 63}, {46, 31, 32, 63}, {47, 20, 32, 63},
			{47, 21, 32, 63}, {47, 22, 32, 63}, {47, 23, 32, 63}, {47, 24, 32, 63}, {47, 25, 32, 63},
			{47, 26, 32, 63}, {47, 27, 32, 63}, {47, 28, 32, 63}, {47, 29, 32, 63}, {47, 30, 32, 63},
			{47, 31, 32, 63}, {48, 20, 32, 63}, {48, 21, 32, 63}, {48, 22, 32, 63}, {48, 23, 32, 63},
			{48, 24, 32, 63}, {48, 25, 32, 63}, {48, 26, 32, 63}, {48, 27, 32, 63}, {48, 28, 32, 63},
			{48, 29, 32, 63}, {48, 30, 32, 63}, {48, 31, 32, 63}, {49, 20, 32, 63}, {49, 21, 32, 63},
			{49, 22, 32, 63}, {49, 23, 32, 63}, {49, 24, 32, 63}, {49, 25, 32, 63}, {49, 26, 32, 63},
			{49, 27, 32, 63}, {49, 28, 32, 63}, {49, 29, 32, 63}, {49, 30, 32, 63}, {49, 31, 32, 63},
			{50, 20, 32, 63}, {50, 21, 32, 63}, {50, 22, 32, 63}, {50, 23, 32, 63}, {50, 24, 32, 63},
			{50, 25, 32, 63}, {50, 26, 32, 63}, {50, 27, 32, 63}, {50, 28, 32, 63}, {50, 29, 32, 63},
			{50, 30, 32, 63}, {50, 31, 32, 63}, {51, 20, 32, 63}, {51, 21, 32, 63}, {51, 22, 32, 63},
			{51, 23, 32, 63}, {51, 24, 32, 63}, {51, 25, 32, 63}, {51, 26, 32, 63}, {51, 27, 32, 63},
			{51, 28, 32, 63}, {51, 29, 32, 63}, {51, 30, 32, 63}, {51, 31, 32, 63}, {52, 20, 32, 63},
			{52, 21, 32, 63}, {52, 22, 32, 63}, {52, 23, 32, 63}, {52, 24, 32, 63}, {52, 25, 32, 63},
			{52, 26, 32, 63}, {52, 27, 32, 63}, {52, 28, 32, 63}, {52, 29, 32, 63}, {52, 30, 32, 63},
			{52, 31, 32, 63}, {53, 20, 32, 63}, {53, 21, 32, 63}, {53, 22, 32, 63}, {53, 23, 32, 63},
			{53, 24, 32, 63}, {53, 25, 32, 63}, {53, 26, 32, 63}, {53, 27, 32, 63}, {53, 28, 32, 63},
			{53, 29, 32, 63}, {53, 30, 32, 63}, {53, 31, 32, 63}, {54, 20, 32, 63}, {54, 21, 32, 63},
			{54, 22, 32, 63}, {54, 23, 32, 63}, {54, 24, 32, 63}, {54, 25, 32, 63}, {54, 26, 32, 63},
			{54, 27, 32, 63}, {54, 28, 32, 63}, {54, 29, 32, 63}, {54, 30, 32, 63}, {54, 31, 32, 63},
			{55, 20, 32, 63}, {55, 21, 32, 63}, {55, 22, 32, 63}, {55, 23, 32, 63}, {55, 24, 32, 63},
			{55, 25, 32, 63}, {55, 26, 32, 63}, {55, 27, 32, 63}, {55, 28, 32, 63}, {55, 29, 32, 63},
			{55, 30, 32, 63}, {55, 31, 32, 63}, {56, 20, 32, 63}, {56, 21, 32, 63}, {56, 22, 32, 63},
			{56, 23, 32, 63}, {56, 24, 32, 63}, {56, 25, 32, 63}, {56, 26, 32, 63}, {56, 27, 32, 63},
			{56, 28, 32, 63}, {56, 29, 32, 63}, {56, 30, 32, 63}, {56, 31, 32, 63}, {57, 20, 32, 63},
			{57, 21, 32, 63}, {57, 22, 32, 63}, {57, 23, 32, 63}, {57, 24, 32, 63}, {57, 25, 32, 63},
			{57, 26, 32, 63}, {57, 27, 32, 63}, {57, 28, 32, 63}, {57, 29, 32, 63}, {57, 30, 32, 63},
			{57, 31, 32, 63}, {58, 20, 32, 63}, {58, 21, 32, 63}, {58, 22, 32, 63}, {58, 23, 32, 63},
			{58, 24, 32, 63}, {58, 25, 32, 63}, {58, 26, 32, 63}, {58, 27, 32, 63}, {58, 28, 32, 63},
			{58, 29, 32, 63}, {58, 30, 32, 63}, {58, 31, 32, 63}, {59, 20, 32, 63}, {59, 21, 32, 63},
			{59, 22, 32, 63}, {59, 23, 32, 63}, {59, 24, 32, 63}, {59, 25, 32, 63}, {59, 26, 32, 63},
			{59, 27, 32, 63}, {59, 28, 32, 63}, {59, 29, 32, 63}, {59, 30, 32, 63}, {59, 31, 32, 63},
			{40, 20, 64, 79}, {40, 21, 64, 79}, {40, 22, 64, 79}, {40, 23, 64, 79}, {40, 24, 64, 79},
			{40, 25, 64, 79}, {40, 26, 64, 79}, {40, 27, 64, 79}, {40, 28, 64, 79}, {40, 29, 64, 79},
			{40, 30, 64, 79}, {40, 31, 64, 79}, {41, 20, 64, 79}, {41, 21, 64, 79}, {41, 22, 64, 79},
			{41, 23, 64, 79}, {41, 24, 64, 79}, {41, 25, 64, 79}, {41, 26, 64, 79}, {41, 27, 64, 79},
			{41, 28, 64, 79}, {41, 29, 64, 79}, {41, 30, 64, 79}, {41, 31, 64, 79}, {42, 20, 64, 79},
			{42, 21, 64, 79}, {42, 22, 64, 79}, {42, 23, 64, 79}, {42, 24, 64, 79}, {42, 25, 64, 79},
			{42, 26, 64, 79}, {42, 27, 64, 79}, {42, 28, 64, 79}, {42, 29, 64, 79}, {42, 30, 64, 79},
			{42, 31, 64, 79}, {43, 20, 64, 79}, {43, 21, 64, 79}, {43, 22, 64, 79}, {43, 23, 64, 79},
			{43, 24, 64, 79}, {43, 25, 64, 79}, {43, 26, 64, 79}, {43, 27, 64, 79}, {43, 28, 64, 79},
			{43, 29, 64, 79}, {43, 30, 64, 79}, {43, 31, 64, 79}, {44, 20, 64, 79}, {44, 21, 64, 79},
			{44, 22, 64, 79}, {44, 23, 64, 79}, {44, 24, 64, 79}, {44, 25, 64, 79}, {44, 26, 64, 79},
			{44, 27, 64, 79}, {44, 28, 64, 79}, {44, 29, 64, 79}, {44, 30, 64, 79}, {44, 31, 64, 79},
			{45, 20, 64, 79}, {45, 21, 64, 79}, {45, 22, 64, 79}, {45, 23, 64, 79}, {45, 24, 64, 79},
			{45, 25, 64, 79}, {45, 26, 64, 79}, {45, 27, 64, 79}, {45, 28, 64, 79}, {45, 29, 64, 79},
			{45, 30, 64, 79}, {45, 31, 64, 79}, {46, 20, 64, 79}, {46, 21, 64, 79}, {46, 22, 64, 79},
			{46, 23, 64, 79}, {46, 24, 64, 79}, {46, 25, 64, 79}, {46, 26, 64, 79}, {46, 27, 64, 79},
			{46, 28, 64, 79}, {46, 29, 64, 79}, {46, 30, 64, 79}, {46, 31, 64, 79}, {47, 20, 64, 79},
			{47, 21, 64, 79}, {47, 22, 64, 79}, {47, 23, 64, 79}, {47, 24, 64, 79}, {47, 25, 64, 79},
			{47, 26, 64, 79}, {47, 27, 64, 79}, {47, 28, 64, 79}, {47, 29, 64, 79}, {47, 30, 64, 79},
			{47, 31, 64, 79}, {48, 20, 64, 79}, {48, 21, 64, 79}, {48, 22, 64, 79}, {48, 23, 64, 79},
			{48, 24, 64, 79}, {48, 25, 64, 79}, {48, 26, 64, 79}, {48, 27, 64, 79}, {48, 28, 64, 79},
			{48, 29, 64, 79}, {48, 30, 64, 79}, {48, 31, 64, 79}, {49, 20, 64, 79}, {49, 21, 64, 79},
			{49, 22, 64, 79}, {49, 23, 64, 79}, {49, 24, 64, 79}, {49, 25, 64, 79}, {49, 26, 64, 79},
			{49, 27, 64, 79}, {49, 28, 64, 79}, {49, 29, 64, 79}, {49, 30, 64, 79}, {49, 31, 64, 79},
			{50, 20, 64, 79}, {50, 21, 64, 79}, {50, 22, 64, 79}, {50, 23, 64, 79}, {50, 24, 64, 79},
			{50, 25, 64, 79}, {50, 26, 64, 79}, {50, 27, 64, 79}, {50, 28, 64, 79}, {50, 29, 64, 79},
			{50, 30, 64, 79}, {50, 31, 64, 79}, {51, 20, 64, 79}, {51, 21, 64, 79}, {51, 22, 64, 79},
			{51, 23, 64, 79}, {51, 24, 64, 79}, {51, 25, 64, 79}, {51, 26, 64, 79}, {51, 27, 64, 79},
			{51, 28, 64, 79}, {51, 29, 64, 79}, {51, 30, 64, 79}, {51, 31, 64, 79}, {52, 20, 64, 79},
			{52, 21, 64, 79}, {52, 22, 64, 79}, {52, 23, 64, 79}, {52, 24, 64, 79}, {52, 25, 64, 79},
			{52, 26, 64, 79}, {52, 27, 64, 79}, {52, 28, 64, 79}, {52, 29, 64, 79}, {52, 30, 64, 79},
			{52, 31, 64, 79}, {53, 20, 64, 79}, {53, 21, 64, 79}, {53, 22, 64, 79}, {53, 23, 64, 79},
			{53, 24, 64, 79}, {53, 25, 64, 79}, {53, 26, 64, 79}, {53, 27, 64, 79}, {53, 28, 64, 79},
			{53, 29, 64, 79}, {53, 30, 64, 79}, {53, 31, 64, 79}, {54, 20, 64, 79}, {54, 21, 64, 79},
			{54, 22, 64, 79}, {54, 23, 64, 79}, {54, 24, 64, 79}, {54, 25, 64, 79}, {54, 26, 64, 79},
			{54, 27, 64, 79}, {54, 28, 64, 79}, {54, 29, 64, 79}, {54, 30, 64, 79}, {54, 31, 64, 79},
			{55, 20, 64, 79}, {55, 21, 64, 79}, {55, 22, 64, 79}, {55, 23, 64, 79}, {55, 24, 64, 79},
			{55, 25, 64, 79}, {55, 26, 64, 79}, {55, 27, 64, 79}, {55, 28, 64, 79}, {55, 29, 64, 79},
			{55, 30, 64, 79}, {55, 31, 64, 79}, {56, 20, 64, 79}, {56, 21, 64, 79}, {56, 22, 64, 79},
			{56, 23, 64, 79}, {56, 24, 64, 79}, {56, 25, 64, 79}, {56, 26, 64, 79}, {56, 27, 64, 79},
			{56, 28, 64, 79}, {56, 29, 64, 79}, {56, 30, 64, 79}, {56, 31, 64, 79}, {57, 20, 64, 79},
			{57, 21, 64, 79}, {57, 22, 64, 79}, {57, 23, 64, 79}, {57, 24, 64, 79}, {57, 25, 64, 79},
			{57, 26, 64, 79}, {57, 27, 64, 79}, {57, 28, 64, 79}, {57, 29, 64, 79}, {57, 30, 64, 79},
			{57, 31, 64, 79}, {58, 20, 64, 79}, {58, 21, 64, 79}, {58, 22, 64, 79}, {58, 23, 64, 79},
			{58, 24, 64, 79}, {58, 25, 64, 79}, {58, 26, 64, 79}, {58, 27, 64, 79}, {58, 28, 64, 79},
			{58, 29, 64, 79}, {58, 30, 64, 79}, {58, 31, 64, 79}, {59, 20, 64, 79}, {59, 21, 64, 79},
			{59, 22, 64, 79}, {59, 23, 64, 79}, {59, 24, 64, 79}, {59, 25, 64, 79}, {59, 26, 64, 79},
			{59, 27, 64, 79}, {59, 28, 64, 79}, {59, 29, 64, 79}, {59, 30, 64, 79}, {59, 31, 64, 79},
			{40, 32, 30, 31}, {40, 33, 30, 31}, {40, 34, 30, 31}, {40, 35, 30, 31}, {40, 36, 30, 31},
			{40, 37, 30, 31}, {40, 38, 30, 31}, {40, 39, 30, 31}, {40, 40, 30, 31}, {40, 41, 30, 31},
			{40, 42, 30, 31}, {40, 43, 30, 31}, {40, 44, 30, 31}, {40, 45, 30, 31}, {40, 46, 30, 31},
			{40, 47, 30, 31}, {40, 48, 30, 31}, {40, 49, 30, 31}, {40, 50, 30, 31}, {40, 51, 30, 31},
			{40, 52, 30, 31}, {40, 53, 30, 31}, {40, 54, 30, 31}, {40, 55, 30, 31}, {40, 56, 30, 31},
			{40, 57, 30, 31}, {40, 58, 30, 31}, {40, 59, 30, 31}, {40, 60, 30, 31}, {40, 61, 30, 31},
			{40, 62, 30, 31}, {40, 63, 30, 31}, {41, 32, 30, 31}, {41, 33, 30, 31}, {41, 34, 30, 31},
			{41, 35, 30, 31}, {41, 36, 30, 31}, {41, 37, 30, 31}, {41, 38, 30, 31}, {41, 39, 30, 31},
			{41, 40, 30, 31}, {41, 41, 30, 31}, {41, 42, 30, 31}, {41, 43, 30, 31}, {41, 44, 30, 31},
			{41, 45, 30, 31}, {41, 46, 30, 31}, {41, 47, 30, 31}, {41, 48, 30, 31}, {41, 49, 30, 31},
			{41, 50, 30, 31}, {41, 51, 30, 31}, {41, 52, 30, 31}, {41, 53, 30, 31}, {41, 54, 30, 31},
			{41, 55, 30, 31}, {41, 56, 30, 31}, {41, 57, 30, 31}, {41, 58, 30, 31}, {41, 59, 30, 31},
			{41, 60, 30, 31}, {41, 61, 30, 31}, {41, 62, 30, 31}, {41, 63, 30, 31}, {42, 32, 30, 31},
			{42, 33, 30, 31}, {42, 34, 30, 31}, {42, 35, 30, 31}, {42, 36, 30, 31}, {42, 37, 30, 31},
			{42, 38, 30, 31}, {42, 39, 30, 31}, {42, 40, 30, 31}, {42, 41, 30, 31}, {42, 42, 30, 31},
			{42, 43, 30, 31}, {42, 44, 30, 31}, {42, 45, 30, 31}, {42, 46, 30, 31}, {42, 47, 30, 31},
			{42, 48, 30, 31}, {42, 49, 30, 31}, {42, 50, 30, 31}, {42, 51, 30, 31}, {42, 52, 30, 31},
			{42, 53, 30, 31}, {42, 54, 30, 31}, {42, 55, 30, 31}, {42, 56, 30, 31}, {42, 57, 30, 31},
			{42, 58, 30, 31}, {42, 59, 30, 31}, {42, 60, 30, 31}, {42, 61, 30, 31}, {42, 62, 30, 31},
			{42, 63, 30, 31}, {43, 32, 30, 31}, {43, 33, 30, 31}, {43, 34, 30, 31}, {43, 35, 30, 31},
			{43, 36, 30, 31}, {43, 37, 30, 31}, {43, 38, 30, 31}, {43, 39, 30, 31}, {43, 40, 30, 31},
			{43, 41, 30, 31}, {43, 42, 30, 31}, {43, 43, 30, 31}, {43, 44, 30, 31}, {43, 45, 30, 31},
			{43, 46, 30, 31}, {43, 47, 30, 31}, {43, 48, 30, 31}, {43, 49, 30, 31}, {43, 50, 30, 31},
			{43, 51, 30, 31}, {43, 52, 30, 31}, {43, 53, 30, 31}, {43, 54, 30, 31}, {43, 55, 30, 31},
			{43, 56, 30, 31}, {43, 57, 30, 31}, {43, 58, 30, 31}, {43, 59, 30, 31}, {43, 60, 30, 31},
			{43, 61, 30, 31}, {43, 62, 30, 31}, {43, 63, 30, 31}, {44, 32, 30, 31}, {44, 33, 30, 31},
			{44, 34, 30, 31}, {44, 35, 30, 31}, {44, 36, 30, 31}, {44, 37, 30, 31}, {44, 38, 30, 31},
			{44, 39, 30, 31}, {44, 40, 30, 31}, {44, 41, 30, 31}, {44, 42, 30, 31}, {44, 43, 30, 31},
			{44, 44, 30, 31}, {44, 45, 30, 31}, {44, 46, 30, 31}, {44, 47, 30, 31}, {44, 48, 30, 31},
			{44, 49, 30, 31}, {44, 50, 30, 31}, {44, 51, 30, 31}, {44, 52, 30, 31}, {44, 53, 30, 31},
			{44, 54, 30, 31}, {44, 55, 30, 31}, {44, 56, 30, 31}, {44, 57, 30, 31}, {44, 58, 30, 31},
			{44, 59, 30, 31}, {44, 60, 30, 31}, {44, 61, 30, 31}, {44, 62, 30, 31}, {44, 63, 30, 31},
			{45, 32, 30, 31}, {45, 33, 30, 31}, {45, 34, 30, 31}, {45, 35, 30, 31}, {45, 36, 30, 31},
			{45, 37, 30, 31}, {45, 38, 30, 31}, {45, 39, 30, 31}, {45, 40, 30, 31}, {45, 41, 30, 31},
			{45, 42, 30, 31}, {45, 43, 30, 31}, {45, 44, 30, 31}, {45, 45, 30, 31}, {45, 46, 30, 31},
			{45, 47, 30, 31}, {45, 48, 30, 31}, {45, 49, 30, 31}, {45, 50, 30, 31}, {45, 51, 30, 31},
			{45, 52, 30, 31}, {45, 53, 30, 31}, {45, 54, 30, 31}, {45, 55, 30, 31}, {45, 56, 30, 31},
			{45, 57, 30, 31}, {45, 58, 30, 31}, {45, 59, 30, 31}, {45, 60, 30, 31}, {45, 61, 30, 31},
			{45, 62, 30, 31}, {45, 63, 30, 31}, {46, 32, 30, 31}, {46, 33, 30, 31}, {46, 34, 30, 31},
			{46, 35, 30, 31}, {46, 36, 30, 31}, {46, 37, 30, 31}, {46, 38, 30, 31}, {46, 39, 30, 31},
			{46, 40, 30, 31}, {46, 41, 30, 31}, {46, 42, 30, 31}, {46, 43, 30, 31}, {46, 44, 30, 31},
			{46, 45, 30, 31}, {46, 46, 30, 31}, {46, 47, 30, 31}, {46, 48, 30, 31}, {46, 49, 30, 31},
			{46, 50, 30, 31}, {46, 51, 30, 31}, {46, 52, 30, 31}, {46, 53, 30, 31}, {46, 54, 30, 31},
			{46, 55, 30, 31}, {46, 56, 30, 31}, {46, 57, 30, 31}, {46, 58, 30, 31}, {46, 59, 30, 31},
			{46, 60, 30, 31}, {46, 61, 30, 31}, {46, 62, 30, 31}, {46, 63, 30, 31}, {47, 32, 30, 31},
			{47, 33, 30, 31}, {47, 34, 30, 31}, {47, 35, 30, 31}, {47, 36, 30, 31}, {47, 37, 30, 31},
			{47, 38, 30, 31}, {47, 39, 30, 31}, {47, 40, 30, 31}, {47, 41, 30, 31}, {47, 42, 30, 31},
			{47, 43, 30, 31}, {47, 44, 30, 31}, {47, 45, 30, 31}, {47, 46, 30, 31}, {47, 47, 30, 31},
			{47, 48, 30, 31}, {47, 49, 30, 31}, {47, 50, 30, 31}, {47, 51, 30, 31}, {47, 52, 30, 31},
			{47, 53, 30, 31}, {47, 54, 30, 31}, {47, 55, 30, 31}, {47, 56, 30, 31}, {47, 57, 30, 31},
			{47, 58, 30, 31}, {47, 59, 30, 31}, {47, 60, 30, 31}, {47, 61, 30, 31}, {47, 62, 30, 31},
			{47, 63, 30, 31}, {48, 32, 30, 31}, {48, 33, 30, 31}, {48, 34, 30, 31}, {48, 35, 30, 31},
			{48, 36, 30, 31}, {48, 37, 30, 31}, {48, 38, 30, 31}, {48, 39, 30, 31}, {48, 40, 30, 31},
			{48, 41, 30, 31}, {48, 42, 30, 31}, {48, 43, 30, 31}, {48, 44, 30, 31}, {48, 45, 30, 31},
			{48, 46, 30, 31}, {48, 47, 30, 31}, {48, 48, 30, 31}, {48, 49, 30, 31}, {48, 50, 30, 31},
			{48, 51, 30, 31}, {48, 52, 30, 31}, {48, 53, 30, 31}, {48, 54, 30, 31}, {48, 55, 30, 31},
			{48, 56, 30, 31}, {48, 57, 30, 31}, {48, 58, 30, 31}, {48, 59, 30, 31}, {48, 60, 30, 31},
			{48, 61, 30, 31}, {48, 62, 30, 31}, {48, 63, 30, 31}, {49, 32, 30, 31}, {49, 33, 30, 31},
			{49, 34, 30, 31}, {49, 35, 30, 31}, {49, 36, 30, 31}, {49, 37, 30, 31}, {49, 38, 30, 31},
			{49, 39, 30, 31}, {49, 40, 30, 31}, {49, 41, 30, 31}, {49, 42, 30, 31}, {49, 43, 30, 31},
			{49, 44, 30, 31}, {49, 45, 30, 31}, {49, 46, 30, 31}, {49, 47, 30, 31}, {49, 48, 30, 31},
			{49, 49, 30, 31}, {49, 50, 30, 31}, {49, 51, 30, 31}, {49, 52, 30, 31}, {49, 53, 30, 31},
			{49, 54, 30, 31}, {49, 55, 30, 31}, {49, 56, 30, 31}, {49, 57, 30, 31}, {49, 58, 30, 31},
			{49, 59, 30, 31}, {49, 60, 30, 31}, {49, 61, 30, 31}, {49, 62, 30, 31}, {49, 63, 30, 31},
			{50, 32, 30, 31}, {50, 33, 30, 31}, {50, 34, 30, 31}, {50, 35, 30, 31}, {50, 36, 30, 31},
			{50, 37, 30, 31}, {50, 38, 30, 31}, {50, 39, 30, 31}, {50, 40, 30, 31}, {50, 41, 30, 31},
			{50, 42, 30, 31}, {50, 43, 30, 31}, {50, 44, 30, 31}, {50, 45, 30, 31}, {50, 46, 30, 31},
			{50, 47, 30, 31}, {50, 48, 30, 31}, {50, 49, 30, 31}, {50, 50, 30, 31}, {50, 51, 30, 31},
			{50, 52, 30, 31}, {50, 53, 30, 31}, {50, 54, 30, 31}, {50, 55, 30, 31}, {50, 56, 30, 31},
			{50, 57, 30, 31}, {50, 58, 30, 31}, {50, 59, 30, 31}, {50, 60, 30, 31}, {50, 61, 30, 31},
			{50, 62, 30, 31}, {50, 63, 30, 31}, {51, 32, 30, 31}, {51, 33, 30, 31}, {51, 34, 30, 31},
			{51, 35, 30, 31}, {51, 36, 30, 31}, {51, 37, 30, 31}, {51, 38, 30, 31}, {51, 39, 30, 31},
			{51, 40, 30, 31}, {51, 41, 30, 31}, {51, 42, 30, 31}, {51, 43, 30, 31}, {51, 44, 30, 31},
			{51, 45, 30, 31}, {51, 46, 30, 31}, {51, 47, 30, 31}, {51, 48, 30, 31}, {51, 49, 30, 31},
			{51, 50, 30, 31}, {51, 51, 30, 31}, {51, 52, 30, 31}, {51, 53, 30, 31}, {51, 54, 30, 31},
			{51, 55, 30, 31}, {51, 56, 30, 31}, {51, 57, 30, 31}, {51, 58, 30, 31}, {51, 59, 30, 31},
			{51, 60, 30, 31}, {51, 61, 30, 31}, {51, 62, 30, 31}, {51, 63, 30, 31}, {52, 32, 30, 31},
			{52, 33, 30, 31}, {52, 34, 30, 31}, {52, 35, 30, 31}, {52, 36, 30, 31}, {52, 37, 30, 31},
			{52, 38, 30, 31}, {52, 39, 30, 31}, {52, 40, 30, 31}, {52, 41, 30, 31}, {52, 42, 30, 31},
			{52, 43, 30, 31}, {52, 44, 30, 31}, {52, 45, 30, 31}, {52, 46, 30, 31}, {52, 47, 30, 31},
			{52, 48, 30, 31}, {52, 49, 30, 31}, {52, 50, 30, 31}, {52, 51, 30, 31}, {52, 52, 30, 31},
			{52, 53, 30, 31}, {52, 54, 30, 31}, {52, 55, 30, 31}, {52, 56, 30, 31}, {52, 57, 30, 31},
			{52, 58, 30, 31}, {52, 59, 30, 31}, {52, 60, 30, 31}, {52, 61, 30, 31}, {52, 62, 30, 31},
			{52, 63, 30, 31}, {53, 32, 30, 31}, {53, 33, 30, 31}, {53, 34, 30, 31}, {53, 35, 30, 31},
			{53, 36, 30, 31}, {53, 37, 30, 31}, {53, 38, 30, 31}, {53, 39, 30, 31}, {53, 40, 30, 31},
			{53, 41, 30, 31}, {53, 42, 30, 31}, {53, 43, 30, 31}, {53, 44, 30, 31}, {53, 45, 30, 31},
			{53, 46, 30, 31}, {53, 47, 30, 31}, {53, 48, 30, 31}, {53, 49, 30, 31}, {53, 50, 30, 31},
			{53, 51, 30, 31}, {53, 52, 30, 31}, {53, 53, 30, 31}, {53, 54, 30, 31}, {53, 55, 30, 31},
			{53, 56, 30, 31}, {53, 57, 30, 31}, {53, 58, 30, 31}, {53, 59, 30, 31}, {53, 60, 30, 31},
			{53, 61, 30, 31}, {53, 62, 30, 31}, {53, 63, 30, 31}, {54, 32, 30, 31}, {54, 33, 30, 31},
			{54, 34, 30, 31}, {54, 35, 30, 31}, {54, 36, 30, 31}, {54, 37, 30, 31}, {54, 38, 30, 31},
			{54, 39, 30, 31}, {54, 40, 30, 31}, {54, 41, 30, 31}, {54, 42, 30, 31}, {54, 43, 30, 31},
			{54, 44, 30, 31}, {54, 45, 30, 31}, {54, 46, 30, 31}, {54, 47, 30, 31}, {54, 48, 30, 31},
			{54, 49, 30, 31}, {54, 50, 30, 31}, {54, 51, 30, 31}, {54, 52, 30, 31}, {54, 53, 30, 31},
			{54, 54, 30, 31}, {54, 55, 30, 31}, {54, 56, 30, 31}, {54, 57, 30, 31}, {54, 58, 30, 31},
			{54, 59, 30, 31}, {54, 60, 30, 31}, {54, 61, 30, 31}, {54, 62, 30, 31}, {54, 63, 30, 31},
			{55, 32, 30, 31}, {55, 33, 30, 31}, {55, 34, 30, 31}, {55, 35, 30, 31}, {55, 36, 30, 31},
			{55, 37, 30, 31}, {55, 38, 30, 31}, {55, 39, 30, 31}, {55, 40, 30, 31}, {55, 41, 30, 31},
			{55, 42, 30, 31}, {55, 43, 30, 31}, {55, 44, 30, 31}, {55, 45, 30, 31}, {55, 46, 30, 31},
			{55, 47, 30, 31}, {55, 48, 30, 31}, {55, 49, 30, 31}, {55, 50, 30, 31}, {55, 51, 30, 31},
			{55, 52, 30, 31}, {55, 53, 30, 31}, {55, 54, 30, 31}, {55, 55, 30, 31}, {55, 56, 30, 31},
			{55, 57, 30, 31}, {55, 58, 30, 31}, {55, 59, 30, 31}, {55, 60, 30, 31}, {55, 61, 30, 31},
			{55, 62, 30, 31}, {55, 63, 30, 31}, {56, 32, 30, 31}, {56, 33, 30, 31}, {56, 34, 30, 31},
			{56, 35, 30, 31}, {56, 36, 30, 31}, {56, 37, 30, 31}, {56, 38, 30, 31}, {56, 39, 30, 31},
			{56, 40, 30, 31}, {56, 41, 30, 31}, {56, 42, 30, 31}, {56, 43, 30, 31}, {56, 44, 30, 31},
			{56, 45, 30, 31}, {56, 46, 30, 31}, {56, 47, 30, 31}, {56, 48, 30, 31}, {56, 49, 30, 31},
			{56, 50, 30, 31}, {56, 51, 30, 31}, {56, 52, 30, 31}, {56, 53, 30, 31}, {56, 54, 30, 31},
			{56, 55, 30, 31}, {56, 56, 30, 31}, {56, 57, 30, 31}, {56, 58, 30, 31}, {56, 59, 30, 31},
			{56, 60, 30, 31}, {56, 61, 30, 31}, {56, 62, 30, 31}, {56, 63, 30, 31}, {57, 32, 30, 31},
			{57, 33, 30, 31}, {57, 34, 30, 31}, {57, 35, 30, 31}, {57, 36, 30, 31}, {57, 37, 30, 31},
			{57, 38, 30, 31}, {57, 39, 30, 31}, {57, 40, 30, 31}, {57, 41, 30, 31}, {57, 42, 30, 31},
			{57, 43, 30, 31}, {57, 44, 30, 31}, {57, 45, 30, 31}, {57, 46, 30, 31}, {57, 47, 30, 31},
			{57, 48, 30, 31}, {57, 49, 30, 31}, {57, 50, 30, 31}, {57, 51, 30, 31}, {57, 52, 30, 31},
			{57, 53, 30, 31}, {57, 54, 30, 31}, {57, 55, 30, 31}, {57, 56, 30, 31}, {57, 57, 30, 31},
			{57, 58, 30, 31}, {57, 59, 30, 31}, {57, 60, 30, 31}, {57, 61, 30, 31}, {57, 62, 30, 31},
			{57, 63, 30, 31}, {58, 32, 30, 31}, {58, 33, 30, 31}, {58, 34, 30, 31}, {58, 35, 30, 31},
			{58, 36, 30, 31}, {58, 37, 30, 31}, {58, 38, 30, 31}, {58, 39, 30, 31}, {58, 40, 30, 31},
			{58, 41, 30, 31}, {58, 42, 30, 31}, {58, 43, 30, 31}, {58, 44, 30, 31}, {58, 45, 30, 31},
			{58, 46, 30, 31}, {58, 47, 30, 31}, {58, 48, 30, 31}, {58, 49, 30, 31}, {58, 50, 30, 31},
			{58, 51, 30, 31}, {58, 52, 30, 31}, {58, 53, 30, 31}, {58, 54, 30, 31}, {58, 55, 30, 31},
			{58, 56, 30, 31}, {58, 57, 30, 31}, {58, 58, 30, 31}, {58, 59, 30, 31}, {58, 60, 30, 31},
			{58, 61, 30, 31}, {58, 62, 30, 31}, {58, 63, 30, 31}, {59, 32, 30, 31}, {59, 33, 30, 31},
			{59, 34, 30, 31}, {59, 35, 30, 31}, {59, 36, 30, 31}, {59, 37, 30, 31}, {59, 38, 30, 31},
			{59, 39, 30, 31}, {59, 40, 30, 31}, {59, 41, 30, 31}, {59, 42, 30, 31}, {59, 43, 30, 31},
			{59, 44, 30, 31}, {59, 45, 30, 31}, {59, 46, 30, 31}, {59, 47, 30, 31}, {59, 48, 30, 31},
			{59, 49, 30, 31}, {59, 50, 30, 31}, {59, 51, 30, 31}, {59, 52, 30, 31}, {59, 53, 30, 31},
			{59, 54, 30, 31}, {59, 55, 30, 31}, {59, 56, 30, 31}, {59, 57, 30, 31}, {59, 58, 30, 31},
			{59, 59, 30, 31}, {59, 60, 30, 31}, {59, 61, 30, 31}, {59, 62, 30, 31}, {59, 63, 30, 31},
			{40, 32, 32, 63}, {40, 33, 32, 63}, {40, 34, 32, 63}, {40, 35, 32, 63}, {40, 36, 32, 63},
			{40, 37, 32, 63}, {40, 38, 32, 63}, {40, 39, 32, 63}, {40, 40, 32, 63}, {40, 41, 32, 63},
			{40, 42, 32, 63}, {40, 43, 32, 63}, {40, 44, 32, 63}, {40, 45, 32, 63}, {40, 46, 32, 63},
			{40, 47, 32, 63}, {40, 48, 32, 63}, {40, 49, 32, 63}, {40, 50, 32, 63}, {40, 51, 32, 63},
			{40, 52, 32, 63}, {40, 53, 32, 63}, {40, 54, 32, 63}, {40, 55, 32, 63}, {40, 56, 32, 63},
			{40, 57, 32, 63}, {40, 58, 32, 63}, {40, 59, 32, 63}, {40, 60, 32, 63}, {40, 61, 32, 63},
			{40, 62, 32, 63}, {40, 63, 32, 63}, {41, 32, 32, 63}, {41, 33, 32, 63}, {41, 34, 32, 63},
			{41, 35, 32, 63}, {41, 36, 32, 63}, {41, 37, 32, 63}, {41, 38, 32, 63}, {41, 39, 32, 63},
			{41, 40, 32, 63}, {41, 41, 32, 63}, {41, 42, 32, 63}, {41, 43, 32, 63}, {41, 44, 32, 63},
			{41, 45, 32, 63}, {41, 46, 32, 63}, {41, 47, 32, 63}, {41, 48, 32, 63}, {41, 49, 32, 63},
			{41, 50, 32, 63}, {41, 51, 32, 63}, {41, 52, 32, 63}, {41, 53, 32, 63}, {41, 54, 32, 63},
			{41, 55, 32, 63}, {41, 56, 32, 63}, {41, 57, 32, 63}, {41, 58, 32, 63}, {41, 59, 32, 63},
			{41, 60, 32, 63}, {41, 61, 32, 63}, {41, 62, 32, 63}, {41, 63, 32, 63}, {42, 32, 32, 63},
			{42, 33, 32, 63}, {42, 34, 32, 63}, {42, 35, 32, 63}, {42, 36, 32, 63}, {42, 37, 32, 63},
			{42, 38, 32, 63}, {42, 39, 32, 63}, {42, 40, 32, 63}, {42, 41, 32, 63}, {42, 42, 32, 63},
			{42, 43, 32, 63}, {42, 44, 32, 63}, {42, 45, 32, 63}, {42, 46, 32, 63}, {42, 47, 32, 63},
			{42, 48, 32, 63}, {42, 49, 32, 63}, {42, 50, 32, 63}, {42, 51, 32, 63}, {42, 52, 32, 63},
			{42, 53, 32, 63}, {42, 54, 32, 63}, {42, 55, 32, 63}, {42, 56, 32, 63}, {42, 57, 32, 63},
			{42, 58, 32, 63}, {42, 59, 32, 63}, {42, 60, 32, 63}, {42, 61, 32, 63}, {42, 62, 32, 63},
			{42, 63, 32, 63}, {43, 32, 32, 63}, {43, 33, 32, 63}, {43, 34, 32, 63}, {43, 35, 32, 63},
			{43, 36, 32, 63}, {43, 37, 32, 63}, {43, 38, 32, 63}, {43, 39, 32, 63}, {43, 40, 32, 63},
			{43, 41, 32, 63}, {43, 42, 32, 63}, {43, 43, 32, 63}, {43, 44, 32, 63}, {43, 45, 32, 63},
			{43, 46, 32, 63}, {43, 47, 32, 63}, {43, 48, 32, 63}, {43, 49, 32, 63}, {43, 50, 32, 63},
			{43, 51, 32, 63}, {43, 52, 32, 63}, {43, 53, 32, 63}, {43, 54, 32, 63}, {43, 55, 32, 63},
			{43, 56, 32, 63}, {43, 57, 32, 63}, {43, 58, 32, 63}, {43, 59, 32, 63}, {43, 60, 32, 63},
			{43, 61, 32, 63}, {43, 62, 32, 63}, {43, 63, 32, 63}, {44, 32, 32, 63}, {44, 33, 32, 63},
			{44, 34, 32, 63}, {44, 35, 32, 63}, {44, 36, 32, 63}, {44, 37, 32, 63}, {44, 38, 32, 63},
			{44, 39, 32, 63}, {44, 40, 32, 63}, {44, 41, 32, 63}, {44, 42, 32, 63}, {44, 43, 32, 63},
			{44, 44, 32, 63}, {44, 45, 32, 63}, {44, 46, 32, 63}, {44, 47, 32, 63}, {44, 48, 32, 63},
			{44, 49, 32, 63}, {44, 50, 32, 63}, {44, 51, 32, 63}, {44, 52, 32, 63}, {44, 53, 32, 63},
			{44, 54, 32, 63}, {44, 55, 32, 63}, {44, 56, 32, 63}, {44, 57, 32, 63}, {44, 58, 32, 63},
			{44, 59, 32, 63}, {44, 60, 32, 63}, {44, 61, 32, 63}, {44, 62, 32, 63}, {44, 63, 32, 63},
			{45, 32, 32, 63}, {45, 33, 32, 63}, {45, 34, 32, 63}, {45, 35, 32, 63}, {45, 36, 32, 63},
			{45, 37, 32, 63}, {45, 38, 32, 63}, {45, 39, 32, 63}, {45, 40, 32, 63}, {45, 41, 32, 63},
			{45, 42, 32, 63}, {45, 43, 32, 63}, {45, 44, 32, 63}, {45, 45, 32, 63}, {45, 46, 32, 63},
			{45, 47, 32, 63}, {45, 48, 32, 63}, {45, 49, 32, 63}, {45, 50, 32, 63}, {45, 51, 32, 63},
			{45, 52, 32, 63}, {45, 53, 32, 63}, {45, 54, 32, 63}, {45, 55, 32, 63}, {45, 56, 32, 63},
			{45, 57, 32, 63}, {45, 58, 32, 63}, {45, 59, 32, 63}, {45, 60, 32, 63}, {45, 61, 32, 63},
			{45, 62, 32, 63}, {45, 63, 32, 63}, {46, 32, 32, 63}, {46, 33, 32, 63}, {46, 34, 32, 63},
			{46, 35, 32, 63}, {46, 36, 32, 63}, {46, 37, 32, 63}, {46, 38, 32, 63}, {46, 39, 32, 63},
			{46, 40, 32, 63}, {46, 41, 32, 63}, {46, 42, 32, 63}, {46, 43, 32, 63}, {46, 44, 32, 63},
			{46, 45, 32, 63}, {46, 46, 32, 63}, {46, 47, 32, 63}, {46, 48, 32, 63}, {46, 49, 32, 63},
			{46, 50, 32, 63}, {46, 51, 32, 63}, {46, 52, 32, 63}, {46, 53, 32, 63}, {46, 54, 32, 63},
			{46, 55, 32, 63}, {46, 56, 32, 63}, {46, 57, 32, 63}, {46, 58, 32, 63}, {46, 59, 32, 63},
			{46, 60, 32, 63}, {46, 61, 32, 63}, {46, 62, 32, 63}, {46, 63, 32, 63}, {47, 32, 32, 63},
			{47, 33, 32, 63}, {47, 34, 32, 63}, {47, 35, 32, 63}, {47, 36, 32, 63}, {47, 37, 32, 63},
			{47, 38, 32, 63}, {47, 39, 32, 63}, {47, 40, 32, 63}, {47, 41, 32, 63}, {47, 42, 32, 63},
			{47, 43, 32, 63}, {47, 44, 32, 63}, {47, 45, 32, 63}, {47, 46, 32, 63}, {47, 47, 32, 63},
			{47, 48, 32, 63}, {47, 49, 32, 63}, {47, 50, 32, 63}, {47, 51, 32, 63}, {47, 52, 32, 63},
			{47, 53, 32, 63}, {47, 54, 32, 63}, {47, 55, 32, 63}, {47, 56, 32, 63}, {47, 57, 32, 63},
			{47, 58, 32, 63}, {47, 59, 32, 63}, {47, 60, 32, 63}, {47, 61, 32, 63}, {47, 62, 32, 63},
			{47, 63, 32, 63}, {48, 32, 32, 63}, {48, 33, 32, 63}, {48, 34, 32, 63}, {48, 35, 32, 63},
			{48, 36, 32, 63}, {48, 37, 32, 63}, {48, 38, 32, 63}, {48, 39, 32, 63}, {48, 40, 32, 63},
			{48, 41, 32, 63}, {48, 42, 32, 63}, {48, 43, 32, 63}, {48, 44, 32, 63}, {48, 45, 32, 63},
			{48, 46, 32, 63}, {48, 47, 32, 63}, {48, 48, 32, 63}, {48, 49, 32, 63}, {48, 50, 32, 63},
			{48, 51, 32, 63}, {48, 52, 32, 63}, {48, 53, 32, 63}, {48, 54, 32, 63}, {48, 55, 32, 63},
			{48, 56, 32, 63}, {48, 57, 32, 63}, {48, 58, 32, 63}, {48, 59, 32, 63}, {48, 60, 32, 63},
			{48, 61, 32, 63}, {48, 62, 32, 63}, {48, 63, 32, 63}, {49, 32, 32, 63}, {49, 33, 32, 63},
			{49, 34, 32, 63}, {49, 35, 32, 63}, {49, 36, 32, 63}, {49, 37, 32, 63}, {49, 38, 32, 63},
			{49, 39, 32, 63}, {49, 40, 32, 63}, {49, 41, 32, 63}, {49, 42, 32, 63}, {49, 43, 32, 63},
			{49, 44, 32, 63}, {49, 45, 32, 63}, {49, 46, 32, 63}, {49, 47, 32, 63}, {49, 48, 32, 63},
			{49, 49, 32, 63}, {49, 50, 32, 63}, {49, 51, 32, 63}, {49, 52, 32, 63}, {49, 53, 32, 63},
			{49, 54, 32, 63}, {49, 55, 32, 63}, {49, 56, 32, 63}, {49, 57, 32, 63}, {49, 58, 32, 63},
			{49, 59, 32, 63}, {49, 60, 32, 63}, {49, 61, 32, 63}, {49, 62, 32, 63}, {49, 63, 32, 63},
			{50, 32, 32, 63}, {50, 33, 32, 63}, {50, 34, 32, 63}, {50, 35, 32, 63}, {50, 36, 32, 63},
			{50, 37, 32, 63}, {50, 38, 32, 63}, {50, 39, 32, 63}, {50, 40, 32, 63}, {50, 41, 32, 63},
			{50, 42, 32, 63}, {50, 43, 32, 63}, {50, 44, 32, 63}, {50, 45, 32, 63}, {50, 46, 32, 63},
			{50, 47, 32, 63}, {50, 48, 32, 63}, {50, 49, 32, 63}, {50, 50, 32, 63}, {50, 51, 32, 63},
			{50, 52, 32, 63}, {50, 53, 32, 63}, {50, 54, 32, 63}, {50, 55, 32, 63}, {50, 56, 32, 63},
			{50, 57, 32, 63}, {50, 58, 32, 63}, {50, 59, 32, 63}, {50, 60, 32, 63}, {50, 61, 32, 63},
			{50, 62, 32, 63}, {50, 63, 32, 63}, {51, 32, 32, 63}, {51, 33, 32, 63}, {51, 34, 32, 63},
			{51, 35, 32, 63}, {51, 36, 32, 63}, {51, 37, 32, 63}, {51, 38, 32, 63}, {51, 39, 32, 63},
			{51, 40, 32, 63}, {51, 41, 32, 63}, {51, 42, 32, 63}, {51, 43, 32, 63}, {51, 44, 32, 63},
			{51, 45, 32, 63}, {51, 46, 32, 63}, {51, 47, 32, 63}, {51, 48, 32, 63}, {51, 49, 32, 63},
			{51, 50, 32, 63}, {51, 51, 32, 63}, {51, 52, 32, 63}, {51, 53, 32, 63}, {51, 54, 32, 63},
			{51, 55, 32, 63}, {51, 56, 32, 63}, {51, 57, 32, 63}, {51, 58, 32, 63}, {51, 59, 32, 63},
			{51, 60, 32, 63}, {51, 61, 32, 63}, {51, 62, 32, 63}, {51, 63, 32, 63}, {52, 32, 32, 63},
			{52, 33, 32, 63}, {52, 34, 32, 63}, {52, 35, 32, 63}, {52, 36, 32, 63}, {52, 37, 32, 63},
			{52, 38, 32, 63}, {52, 39, 32, 63}, {52, 40, 32, 63}, {52, 41, 32, 63}, {52, 42, 32, 63},
			{52, 43, 32, 63}, {52, 44, 32, 63}, {52, 45, 32, 63}, {52, 46, 32, 63}, {52, 47, 32, 63},
			{52, 48, 32, 63}, {52, 49, 32, 63}, {52, 50, 32, 63}, {52, 51, 32, 63}, {52, 52, 32, 63},
			{52, 53, 32, 63}, {52, 54, 32, 63}, {52, 55, 32, 63}, {52, 56, 32, 63}, {52, 57, 32, 63},
			{52, 58, 32, 63}, {52, 59, 32, 63}, {52, 60, 32, 63}, {52, 61, 32, 63}, {52, 62, 32, 63},
			{52, 63, 32, 63}, {53, 32, 32, 63}, {53, 33, 32, 63}, {53, 34, 32, 63}, {53, 35, 32, 63},
			{53, 36, 32, 63}, {53, 37, 32, 63}, {53, 38, 32, 63}, {53, 39, 32, 63}, {53, 40, 32, 63},
			{53, 41, 32, 63}, {53, 42, 32, 63}, {53, 43, 32, 63}, {53, 44, 32, 63}, {53, 45, 32, 63},
			{53, 46, 32, 63}, {53, 47, 32, 63}, {53, 48, 32, 63}, {53, 49, 32, 63}, {53, 50, 32, 63},
			{53, 51, 32, 63}, {53, 52, 32, 63}, {53, 53, 32, 63}, {53, 54, 32, 63}, {53, 55, 32, 63},
			{53, 56, 32, 63}, {53, 57, 32, 63}, {53, 58, 32, 63}, {53, 59, 32, 63}, {53, 60, 32, 63},
			{53, 61, 32, 63}, {53, 62, 32, 63}, {53, 63, 32, 63}, {54, 32, 32, 63}, {54, 33, 32, 63},
			{54, 34, 32, 63}, {54, 35, 32, 63}, {54, 36, 32, 63}, {54, 37, 32, 63}, {54, 38, 32, 63},
			{54, 39, 32, 63}, {54, 40, 32, 63}, {54, 41, 32, 63}, {54, 42, 32, 63}, {54, 43, 32, 63},
			{54, 44, 32, 63}, {54, 45, 32, 63}, {54, 46, 32, 63}, {54, 47, 32, 63}, {54, 48, 32, 63},
			{54, 49, 32, 63}, {54, 50, 32, 63}, {54, 51, 32, 63}, {54, 52, 32, 63}, {54, 53, 32, 63},
			{54, 54, 32, 63}, {54, 55, 32, 63}, {54, 56, 32, 63}, {54, 57, 32, 63}, {54, 58, 32, 63},
			{54, 59, 32, 63}, {54, 60, 32, 63}, {54, 61, 32, 63}, {54, 62, 32, 63}, {54, 63, 32, 63},
			{55, 32, 32, 63}, {55, 33, 32, 63}, {55, 34, 32, 63}, {55, 35, 32, 63}, {55, 36, 32, 63},
			{55, 37, 32, 63}, {55, 38, 32, 63}, {55, 39, 32, 63}, {55, 40, 32, 63}, {55, 41, 32, 63},
			{55, 42, 32, 63}, {55, 43, 32, 63}, {55, 44, 32, 63}, {55, 45, 32, 63}, {55, 46, 32, 63},
			{55, 47, 32, 63}, {55, 48, 32, 63}, {55, 49, 32, 63}, {55, 50, 32, 63}, {55, 51, 32, 63},
			{55, 52, 32, 63}, {55, 53, 32, 63}, {55, 54, 32, 63}, {55, 55, 32, 63}, {55, 56, 32, 63},
			{55, 57, 32, 63}, {55, 58, 32, 63}, {55, 59, 32, 63}, {55, 60, 32, 63}, {55, 61, 32, 63},
			{55, 62, 32, 63}, {55, 63, 32, 63}, {56, 32, 32, 63}, {56, 33, 32, 63}, {56, 34, 32, 63},
			{56, 35, 32, 63}, {56, 36, 32, 63}, {56, 37, 32, 63}, {56, 38, 32, 63}, {56, 39, 32, 63},
			{56, 40, 32, 63}, {56, 41, 32, 63}, {56, 42, 32, 63}, {56, 43, 32, 63}, {56, 44, 32, 63},
			{56, 45, 32, 63}, {56, 46, 32, 63}, {56, 47, 32, 63}, {56, 48, 32, 63}, {56, 49, 32, 63},
			{56, 50, 32, 63}, {56, 51, 32, 63}, {56, 52, 32, 63}, {56, 53, 32, 63}, {56, 54, 32, 63},
			{56, 55, 32, 63}, {56, 56, 32, 63}, {56, 57, 32, 63}, {56, 58, 32, 63}, {56, 59, 32, 63},
			{56, 60, 32, 63}, {56, 61, 32, 63}, {56, 62, 32, 63}, {56, 63, 32, 63}, {57, 32, 32, 63},
			{57, 33, 32, 63}, {57, 34, 32, 63}, {57, 35, 32, 63}, {57, 36, 32, 63}, {57, 37, 32, 63},
			{57, 38, 32, 63}, {57, 39, 32, 63}, {57, 40, 32, 63}, {57, 41, 32, 63}, {57, 42, 32, 63},
			{57, 43, 32, 63}, {57, 44, 32, 63}, {57, 45, 32, 63}, {57, 46, 32, 63}, {57, 47, 32, 63},
			{57, 48, 32, 63}, {57, 49, 32, 63}, {57, 50, 32, 63}, {57, 51, 32, 63}, {57, 52, 32, 63},
			{57, 53, 32, 63}, {57, 54, 32, 63}, {57, 55, 32, 63}, {57, 56, 32, 63}, {57, 57, 32, 63},
			{57, 58, 32, 63}, {57, 59, 32, 63}, {57, 60, 32, 63}, {57, 61, 32, 63}, {57, 62, 32, 63},
			{57, 63, 32, 63}, {58, 32, 32, 63}, {58, 33, 32, 63}, {58, 34, 32, 63}, {58, 35, 32, 63},
			{58, 36, 32, 63}, {58, 37, 32, 63}, {58, 38, 32, 63}, {58, 39, 32, 63}, {58, 40, 32, 63},
			{58, 41, 32, 63}, {58, 42, 32, 63}, {58, 43, 32, 63}, {58, 44, 32, 63}, {58, 45, 32, 63},
			{58, 46, 32, 63}, {58, 47, 32, 63}, {58, 48, 32, 63}, {58, 49, 32, 63}, {58, 50, 32, 63},
			{58, 51, 32, 63}, {58, 52, 32, 63}, {58, 53, 32, 63}, {58, 54, 32, 63}, {58, 55, 32, 63},
			{58, 56, 32, 63}, {58, 57, 32, 63}, {58, 58, 32, 63}, {58, 59, 32, 63}, {58, 60, 32, 63},
			{58, 61, 32, 63}, {58, 62, 32, 63}, {58, 63, 32, 63}, {59, 32, 32, 63}, {59, 33, 32, 63},
			{59, 34, 32, 63}, {59, 35, 32, 63}, {59, 36, 32, 63}, {59, 37, 32, 63}, {59, 38, 32, 63},
			{59, 39, 32, 63}, {59, 40, 32, 63}, {59, 41, 32, 63}, {59, 42, 32, 63}, {59, 43, 32, 63},
			{59, 44, 32, 63}, {59, 45, 32, 63}, {59, 46, 32, 63}, {59, 47, 32, 63}, {59, 48, 32, 63},
			{59, 49, 32, 63}, {59, 50, 32, 63}, {59, 51, 32, 63}, {59, 52, 32, 63}, {59, 53, 32, 63},
			{59, 54, 32, 63}, {59, 55, 32, 63}, {59, 56, 32, 63}, {59, 57, 32, 63}, {59, 58, 32, 63},
			{59, 59, 32, 63}, {59, 60, 32, 63}, {59, 61, 32, 63}, {59, 62, 32, 63}, {59, 63, 32, 63},
			{40, 32, 64, 79}, {40, 33, 64, 79}, {40, 34, 64, 79}, {40, 35, 64, 79}, {40, 36, 64, 79},
			{40, 37, 64, 79}, {40, 38, 64, 79}, {40, 39, 64, 79}, {40, 40, 64, 79}, {40, 41, 64, 79},
			{40, 42, 64, 79}, {40, 43, 64, 79}, {40, 44, 64, 79}, {40, 45, 64, 79}, {40, 46, 64, 79},
			{40, 47, 64, 79}, {40, 48, 64, 79}, {40, 49, 64, 79}, {40, 50, 64, 79}, {40, 51, 64, 79},
			{40, 52, 64, 79}, {40, 53, 64, 79}, {40, 54, 64, 79}, {40, 55, 64, 79}, {40, 56, 64, 79},
			{40, 57, 64, 79}, {40, 58, 64, 79}, {40, 59, 64, 79}, {40, 60, 64, 79}, {40, 61, 64, 79},
			{40, 62, 64, 79}, {40, 63, 64, 79}, {41, 32, 64, 79}, {41, 33, 64, 79}, {41, 34, 64, 79},
			{41, 35, 64, 79}, {41, 36, 64, 79}, {41, 37, 64, 79}, {41, 38, 64, 79}, {41, 39, 64, 79},
			{41, 40, 64, 79}, {41, 41, 64, 79}, {41, 42, 64, 79}, {41, 43, 64, 79}, {41, 44, 64, 79},
			{41, 45, 64, 79}, {41, 46, 64, 79}, {41, 47, 64, 79}, {41, 48, 64, 79}, {41, 49, 64, 79},
			{41, 50, 64, 79}, {41, 51, 64, 79}, {41, 52, 64, 79}, {41, 53, 64, 79}, {41, 54, 64, 79},
			{41, 55, 64, 79}, {41, 56, 64, 79}, {41, 57, 64, 79}, {41, 58, 64, 79}, {41, 59, 64, 79},
			{41, 60, 64, 79}, {41, 61, 64, 79}, {41, 62, 64, 79}, {41, 63, 64, 79}, {42, 32, 64, 79},
			{42, 33, 64, 79}, {42, 34, 64, 79}, {42, 35, 64, 79}, {42, 36, 64, 79}, {42, 37, 64, 79},
			{42, 38, 64, 79}, {42, 39, 64, 79}, {42, 40, 64, 79}, {42, 41, 64, 79}, {42, 42, 64, 79},
			{42, 43, 64, 79}, {42, 44, 64, 79}, {42, 45, 64, 79}, {42, 46, 64, 79}, {42, 47, 64, 79},
			{42, 48, 64, 79}, {42, 49, 64, 79}, {42, 50, 64, 79}, {42, 51, 64, 79}, {42, 52, 64, 79},
			{42, 53, 64, 79}, {42, 54, 64, 79}, {42, 55, 64, 79}, {42, 56, 64, 79}, {42, 57, 64, 79},
			{42, 58, 64, 79}, {42, 59, 64, 79}, {42, 60, 64, 79}, {42, 61, 64, 79}, {42, 62, 64, 79},
			{42, 63, 64, 79}, {43, 32, 64, 79}, {43, 33, 64, 79}, {43, 34, 64, 79}, {43, 35, 64, 79},
			{43, 36, 64, 79}, {43, 37, 64, 79}, {43, 38, 64, 79}, {43, 39, 64, 79}, {43, 40, 64, 79},
			{43, 41, 64, 79}, {43, 42, 64, 79}, {43, 43, 64, 79}, {43, 44, 64, 79}, {43, 45, 64, 79},
			{43, 46, 64, 79}, {43, 47, 64, 79}, {43, 48, 64, 79}, {43, 49, 64, 79}, {43, 50, 64, 79},
			{43, 51, 64, 79}, {43, 52, 64, 79}, {43, 53, 64, 79}, {43, 54, 64, 79}, {43, 55, 64, 79},
			{43, 56, 64, 79}, {43, 57, 64, 79}, {43, 58, 64, 79}, {43, 59, 64, 79}, {43, 60, 64, 79},
			{43, 61, 64, 79}, {43, 62, 64, 79}, {43, 63, 64, 79}, {44, 32, 64, 79}, {44, 33, 64, 79},
			{44, 34, 64, 79}, {44, 35, 64, 79}, {44, 36, 64, 79}, {44, 37, 64, 79}, {44, 38, 64, 79},
			{44, 39, 64, 79}, {44, 40, 64, 79}, {44, 41, 64, 79}, {44, 42, 64, 79}, {44, 43, 64, 79},
			{44, 44, 64, 79}, {44, 45, 64, 79}, {44, 46, 64, 79}, {44, 47, 64, 79}, {44, 48, 64, 79},
			{44, 49, 64, 79}, {44, 50, 64, 79}, {44, 51, 64, 79}, {44, 52, 64, 79}, {44, 53, 64, 79},
			{44, 54, 64, 79}, {44, 55, 64, 79}, {44, 56, 64, 79}, {44, 57, 64, 79}, {44, 58, 64, 79},
			{44, 59, 64, 79}, {44, 60, 64, 79}, {44, 61, 64, 79}, {44, 62, 64, 79}, {44, 63, 64, 79},
			{45, 32, 64, 79}, {45, 33, 64, 79}, {45, 34, 64, 79}, {45, 35, 64, 79}, {45, 36, 64, 79},
			{45, 37, 64, 79}, {45, 38, 64, 79}, {45, 39, 64, 79}, {45, 40, 64, 79}, {45, 41, 64, 79},
			{45, 42, 64, 79}, {45, 43, 64, 79}, {45, 44, 64, 79}, {45, 45, 64, 79}, {45, 46, 64, 79},
			{45, 47, 64, 79}, {45, 48, 64, 79}, {45, 49, 64, 79}, {45, 50, 64, 79}, {45, 51, 64, 79},
			{45, 52, 64, 79}, {45, 53, 64, 79}, {45, 54, 64, 79}, {45, 55, 64, 79}, {45, 56, 64, 79},
			{45, 57, 64, 79}, {45, 58, 64, 79}, {45, 59, 64, 79}, {45, 60, 64, 79}, {45, 61, 64, 79},
			{45, 62, 64, 79}, {45, 63, 64, 79}, {46, 32, 64, 79}, {46, 33, 64, 79}, {46, 34, 64, 79},
			{46, 35, 64, 79}, {46, 36, 64, 79}, {46, 37, 64, 79}, {46, 38, 64, 79}, {46, 39, 64, 79},
			{46, 40, 64, 79}, {46, 41, 64, 79}, {46, 42, 64, 79}, {46, 43, 64, 79}, {46, 44, 64, 79},
			{46, 45, 64, 79}, {46, 46, 64, 79}, {46, 47, 64, 79}, {46, 48, 64, 79}, {46, 49, 64, 79},
			{46, 50, 64, 79}, {46, 51, 64, 79}, {46, 52, 64, 79}, {46, 53, 64, 79}, {46, 54, 64, 79},
			{46, 55, 64, 79}, {46, 56, 64, 79}, {46, 57, 64, 79}, {46, 58, 64, 79}, {46, 59, 64, 79},
			{46, 60, 64, 79}, {46, 61, 64, 79}, {46, 62, 64, 79}, {46, 63, 64, 79}, {47, 32, 64, 79},
			{47, 33, 64, 79}, {47, 34, 64, 79}, {47, 35, 64, 79}, {47, 36, 64, 79}, {47, 37, 64, 79},
			{47, 38, 64, 79}, {47, 39, 64, 79}, {47, 40, 64, 79}, {47, 41, 64, 79}, {47, 42, 64, 79},
			{47, 43, 64, 79}, {47, 44, 64, 79}, {47, 45, 64, 79}, {47, 46, 64, 79}, {47, 47, 64, 79},
			{47, 48, 64, 79}, {47, 49, 64, 79}, {47, 50, 64, 79}, {47, 51, 64, 79}, {47, 52, 64, 79},
			{47, 53, 64, 79}, {47, 54, 64, 79}, {47, 55, 64, 79}, {47, 56, 64, 79}, {47, 57, 64, 79},
			{47, 58, 64, 79}, {47, 59, 64, 79}, {47, 60, 64, 79}, {47, 61, 64, 79}, {47, 62, 64, 79},
			{47, 63, 64, 79}, {48, 32, 64, 79}, {48, 33, 64, 79}, {48, 34, 64, 79}, {48, 35, 64, 79},
			{48, 36, 64, 79}, {48, 37, 64, 79}, {48, 38, 64, 79}, {48, 39, 64, 79}, {48, 40, 64, 79},
			{48, 41, 64, 79}, {48, 42, 64, 79}, {48, 43, 64, 79}, {48, 44, 64, 79}, {48, 45, 64, 79},
			{48, 46, 64, 79}, {48, 47, 64, 79}, {48, 48, 64, 79}, {48, 49, 64, 79}, {48, 50, 64, 79},
			{48, 51, 64, 79}, {48, 52, 64, 79}, {48, 53, 64, 79}, {48, 54, 64, 79}, {48, 55, 64, 79},
			{48, 56, 64, 79}, {48, 57, 64, 79}, {48, 58, 64, 79}, {48, 59, 64, 79}, {48, 60, 64, 79},
			{48, 61, 64, 79}, {48, 62, 64, 79}, {48, 63, 64, 79}, {49, 32, 64, 79}, {49, 33, 64, 79},
			{49, 34, 64, 79}, {49, 35, 64, 79}, {49, 36, 64, 79}, {49, 37, 64, 79}, {49, 38, 64, 79},
			{49, 39, 64, 79}, {49, 40, 64, 79}, {49, 41, 64, 79}, {49, 42, 64, 79}, {49, 43, 64, 79},
			{49, 44, 64, 79}, {49, 45, 64, 79}, {49, 46, 64, 79}, {49, 47, 64, 79}, {49, 48, 64, 79},
			{49, 49, 64, 79}, {49, 50, 64, 79}, {49, 51, 64, 79}, {49, 52, 64, 79}, {49, 53, 64, 79},
			{49, 54, 64, 79}, {49, 55, 64, 79}, {49, 56, 64, 79}, {49, 57, 64, 79}, {49, 58, 64, 79},
			{49, 59, 64, 79}, {49, 60, 64, 79}, {49, 61, 64, 79}, {49, 62, 64, 79}, {49, 63, 64, 79},
			{50, 32, 64, 79}, {50, 33, 64, 79}, {50, 34, 64, 79}, {50, 35, 64, 79}, {50, 36, 64, 79},
			{50, 37, 64, 79}, {50, 38, 64, 79}, {50, 39, 64, 79}, {50, 40, 64, 79}, {50, 41, 64, 79},
			{50, 42, 64, 79}, {50, 43, 64, 79}, {50, 44, 64, 79}, {50, 45, 64, 79}, {50, 46, 64, 79},
			{50, 47, 64, 79}, {50, 48, 64, 79}, {50, 49, 64, 79}, {50, 50, 64, 79}, {50, 51, 64, 79},
			{50, 52, 64, 79}, {50, 53, 64, 79}, {50, 54, 64, 79}, {50, 55, 64, 79}, {50, 56, 64, 79},
			{50, 57, 64, 79}, {50, 58, 64, 79}, {50, 59, 64, 79}, {50, 60, 64, 79}, {50, 61, 64, 79},
			{50, 62, 64, 79}, {50, 63, 64, 79}, {51, 32, 64, 79}, {51, 33, 64, 79}, {51, 34, 64, 79},
			{51, 35, 64, 79}, {51, 36, 64, 79}, {51, 37, 64, 79}, {51, 38, 64, 79}, {51, 39, 64, 79},
			{51, 40, 64, 79}, {51, 41, 64, 79}, {51, 42, 64, 79}, {51, 43, 64, 79}, {51, 44, 64, 79},
			{51, 45, 64, 79}, {51, 46, 64, 79}, {51, 47, 64, 79}, {51, 48, 64, 79}, {51, 49, 64, 79},
			{51, 50, 64, 79}, {51, 51, 64, 79}, {51, 52, 64, 79}, {51, 53, 64, 79}, {51, 54, 64, 79},
			{51, 55, 64, 79}, {51, 56, 64, 79}, {51, 57, 64, 79}, {51, 58, 64, 79}, {51, 59, 64, 79},
			{51, 60, 64, 79}, {51, 61, 64, 79}, {51, 62, 64, 79}, {51, 63, 64, 79}, {52, 32, 64, 79},
			{52, 33, 64, 79}, {52, 34, 64, 79}, {52, 35, 64, 79}, {52, 36, 64, 79}, {52, 37, 64, 79},
			{52, 38, 64, 79}, {52, 39, 64, 79}, {52, 40, 64, 79}, {52, 41, 64, 79}, {52, 42, 64, 79},
			{52, 43, 64, 79}, {52, 44, 64, 79}, {52, 45, 64, 79}, {52, 46, 64, 79}, {52, 47, 64, 79},
			{52, 48, 64, 79}, {52, 49, 64, 79}, {52, 50, 64, 79}, {52, 51, 64, 79}, {52, 52, 64, 79},
			{52, 53, 64, 79}, {52, 54, 64, 79}, {52, 55, 64, 79}, {52, 56, 64, 79}, {52, 57, 64, 79},
			{52, 58, 64, 79}, {52, 59, 64, 79}, {52, 60, 64, 79}, {52, 61, 64, 79}, {52, 62, 64, 79},
			{52, 63, 64, 79}, {53, 32, 64, 79}, {53, 33, 64, 79}, {53, 34, 64, 79}, {53, 35, 64, 79},
			{53, 36, 64, 79}, {53, 37, 64, 79}, {53, 38, 64, 79}, {53, 39, 64, 79}, {53, 40, 64, 79},
			{53, 41, 64, 79}, {53, 42, 64, 79}, {53, 43, 64, 79}, {53, 44, 64, 79}, {53, 45, 64, 79},
			{53, 46, 64, 79}, {53, 47, 64, 79}, {53, 48, 64, 79}, {53, 49, 64, 79}, {53, 50, 64, 79},
			{53, 51, 64, 79}, {53, 52, 64, 79}, {53, 53, 64, 79}, {53, 54, 64, 79}, {53, 55, 64, 79},
			{53, 56, 64, 79}, {53, 57, 64, 79}, {53, 58, 64, 79}, {53, 59, 64, 79}, {53, 60, 64, 79},
			{53, 61, 64, 79}, {53, 62, 64, 79}, {53, 63, 64, 79}, {54, 32, 64, 79}, {54, 33, 64, 79},
			{54, 34, 64, 79}, {54, 35, 64, 79}, {54, 36, 64, 79}, {54, 37, 64, 79}, {54, 38, 64, 79},
			{54, 39, 64, 79}, {54, 40, 64, 79}, {54, 41, 64, 79}, {54, 42, 64, 79}, {54, 43, 64, 79},
			{54, 44, 64, 79}, {54, 45, 64, 79}, {54, 46, 64, 79}, {54, 47, 64, 79}, {54, 48, 64, 79},
			{54, 49, 64, 79}, {54, 50, 64, 79}, {54, 51, 64, 79}, {54, 52, 64, 79}, {54, 53, 64, 79},
			{54, 54, 64, 79}, {54, 55, 64, 79}, {54, 56, 64, 79}, {54, 57, 64, 79}, {54, 58, 64, 79},
			{54, 59, 64, 79}, {54, 60, 64, 79}, {54, 61, 64, 79}, {54, 62, 64, 79}, {54, 63, 64, 79},
			{55, 32, 64, 79}, {55, 33, 64, 79}, {55, 34, 64, 79}, {55, 35, 64, 79}, {55, 36, 64, 79},
			{55, 37, 64, 79}, {55, 38, 64, 79}, {55, 39, 64, 79}, {55, 40, 64, 79}, {55, 41, 64, 79},
			{55, 42, 64, 79}, {55, 43, 64, 79}, {55, 44, 64, 79}, {55, 45, 64, 79}, {55, 46, 64, 79},
			{55, 47, 64, 79}, {55, 48, 64, 79}, {55, 49, 64, 79}, {55, 50, 64, 79}, {55, 51, 64, 79},
			{55, 52, 64, 79}, {55, 53, 64, 79}, {55, 54, 64, 79}, {55, 55, 64, 79}, {55, 56, 64, 79},
			{55, 57, 64, 79}, {55, 58, 64, 79}, {55, 59, 64, 79}, {55, 60, 64, 79}, {55, 61, 64, 79},
			{55, 62, 64, 79}, {55, 63, 64, 79}, {56, 32, 64, 79}, {56, 33, 64, 79}, {56, 34, 64, 79},
			{56, 35, 64, 79}, {56, 36, 64, 79}, {56, 37, 64, 79}, {56, 38, 64, 79}, {56, 39, 64, 79},
			{56, 40, 64, 79}, {56, 41, 64, 79}, {56, 42, 64, 79}, {56, 43, 64, 79}, {56, 44, 64, 79},
			{56, 45, 64, 79}, {56, 46, 64, 79}, {56, 47, 64, 79}, {56, 48, 64, 79}, {56, 49, 64, 79},
			{56, 50, 64, 79}, {56, 51, 64, 79}, {56, 52, 64, 79}, {56, 53, 64, 79}, {56, 54, 64, 79},
			{56, 55, 64, 79}, {56, 56, 64, 79}, {56, 57, 64, 79}, {56, 58, 64, 79}, {56, 59, 64, 79},
			{56, 60, 64, 79}, {56, 61, 64, 79}, {56, 62, 64, 79}, {56, 63, 64, 79}, {57, 32, 64, 79},
			{57, 33, 64, 79}, {57, 34, 64, 79}, {57, 35, 64, 79}, {57, 36, 64, 79}, {57, 37, 64, 79},
			{57, 38, 64, 79}, {57, 39, 64, 79}, {57, 40, 64, 79}, {57, 41, 64, 79}, {57, 42, 64, 79},
			{57, 43, 64, 79}, {57, 44, 64, 79}, {57, 45, 64, 79}, {57, 46, 64, 79}, {57, 47, 64, 79},
			{57, 48, 64, 79}, {57, 49, 64, 79}, {57, 50, 64, 79}, {57, 51, 64, 79}, {57, 52, 64, 79},
			{57, 53, 64, 79}, {57, 54, 64, 79}, {57, 55, 64, 79}, {57, 56, 64, 79}, {57, 57, 64, 79},
			{57, 58, 64, 79}, {57, 59, 64, 79}, {57, 60, 64, 79}, {57, 61, 64, 79}, {57, 62, 64, 79},
			{57, 63, 64, 79}, {58, 32, 64, 79}, {58, 33, 64, 79}, {58, 34, 64, 79}, {58, 35, 64, 79},
			{58, 36, 64, 79}, {58, 37, 64, 79}, {58, 38, 64, 79}, {58, 39, 64, 79}, {58, 40, 64, 79},
			{58, 41, 64, 79}, {58, 42, 64, 79}, {58, 43, 64, 79}, {58, 44, 64, 79}, {58, 45, 64, 79},
			{58, 46, 64, 79}, {58, 47, 64, 79}, {58, 48, 64, 79}, {58, 49, 64, 79}, {58, 50, 64, 79},
			{58, 51, 64, 79}, {58, 52, 64, 79}, {58, 53, 64, 79}, {58, 54, 64, 79}, {58, 55, 64, 79},
			{58, 56, 64, 79}, {58, 57, 64, 79}, {58, 58, 64, 79}, {58, 59, 64, 79}, {58, 60, 64, 79},
			{58, 61, 64, 79}, {58, 62, 64, 79}, {58, 63, 64, 79}, {59, 32, 64, 79}, {59, 33, 64, 79},
			{59, 34, 64, 79}, {59, 35, 64, 79}, {59, 36, 64, 79}, {59, 37, 64, 79}, {59, 38, 64, 79},
			{59, 39, 64, 79}, {59, 40, 64, 79}, {59, 41, 64, 79}, {59, 42, 64, 79}, {59, 43, 64, 79},
			{59, 44, 64, 79}, {59, 45, 64, 79}, {59, 46, 64, 79}, {59, 47, 64, 79}, {59, 48, 64, 79},
			{59, 49, 64, 79}, {59, 50, 64, 79}, {59, 51, 64, 79}, {59, 52, 64, 79}, {59, 53, 64, 79},
			{59, 54, 64, 79}, {59, 55, 64, 79}, {59, 56, 64, 79}, {59, 57, 64, 79}, {59, 58, 64, 79},
			{59, 59, 64, 79}, {59, 60, 64, 79}, {59, 61, 64, 79}, {59, 62, 64, 79}, {59, 63, 64, 79},
			{40, 64, 30, 31}, {40, 65, 30, 31}, {40, 66, 30, 31}, {40, 67, 30, 31}, {40, 68, 30, 31},
			{40, 69, 30, 31}, {41, 64, 30, 31}, {41, 65, 30, 31}, {41, 66, 30, 31}, {41, 67, 30, 31},
			{41, 68, 30, 31}, {41, 69, 30, 31}, {42, 64, 30, 31}, {42, 65, 30, 31}, {42, 66, 30, 31},
			{42, 67, 30, 31}, {42, 68, 30, 31}, {42, 69, 30, 31}, {43, 64, 30, 31}, {43, 65, 30, 31},
			{43, 66, 30, 31}, {43, 67, 30, 31}, {43, 68, 30, 31}, {43, 69, 30, 31}, {44, 64, 30, 31},
			{44, 65, 30, 31}, {44, 66, 30, 31}, {44, 67, 30, 31}, {44, 68, 30, 31}, {44, 69, 30, 31},
			{45, 64, 30, 31}, {45, 65, 30, 31}, {45, 66, 30, 31}, {45, 67, 30, 31}, {45, 68, 30, 31},
			{45, 69, 30, 31}, {46, 64, 30, 31}, {46, 65, 30, 31}, {46, 66, 30, 31}, {46, 67, 30, 31},
			{46, 68, 30, 31}, {46, 69, 30, 31}, {47, 64, 30, 31}, {47, 65, 30, 31}, {47, 66, 30, 31},
			{47, 67, 30, 31}, {47, 68, 30, 31}, {47, 69, 30, 31}, {48, 64, 30, 31}, {48, 65, 30, 31},
			{48, 66, 30, 31}, {48, 67, 30, 31}, {48, 68, 30, 31}, {48, 69, 30, 31}, {49, 64, 30, 31},
			{49, 65, 30, 31}, {49, 66, 30, 31}, {49, 67, 30, 31}, {49, 68, 30, 31}, {49, 69, 30, 31},
			{50, 64, 30, 31}, {50, 65, 30, 31}, {50, 66, 30, 31}, {50, 67, 30, 31}, {50, 68, 30, 31},
			{50, 69, 30, 31}, {51, 64, 30, 31}, {51, 65, 30, 31}, {51, 66, 30, 31}, {51, 67, 30, 31},
			{51, 68, 30, 31}, {51, 69, 30, 31}, {52, 64, 30, 31}, {52, 65, 30, 31}, {52, 66, 30, 31},
			{52, 67, 30, 31}, {52, 68, 30, 31}, {52, 69, 30, 31}, {53, 64, 30, 31}, {53, 65, 30, 31},
			{53, 66, 30, 31}, {53, 67, 30, 31}, {53, 68, 30, 31}, {53, 69, 30, 31}, {54, 64, 30, 31},
			{54, 65, 30, 31}, {54, 66, 30, 31}, {54, 67, 30, 31}, {54, 68, 30, 31}, {54, 69, 30, 31},
			{55, 64, 30, 31}, {55, 65, 30, 31}, {55, 66, 30, 31}, {55, 67, 30, 31}, {55, 68, 30, 31},
			{55, 69, 30, 31}, {56, 64, 30, 31}, {56, 65, 30, 31}, {56, 66, 30, 31}, {56, 67, 30, 31},
			{56, 68, 30, 31}, {56, 69, 30, 31}, {57, 64, 30, 31}, {57, 65, 30, 31}, {57, 66, 30, 31},
			{57, 67, 30, 31}, {57, 68, 30, 31}, {57, 69, 30, 31}, {58, 64, 30, 31}, {58, 65, 30, 31},
			{58, 66, 30, 31}, {58, 67, 30, 31}, {58, 68, 30, 31}, {58, 69, 30, 31}, {59, 64, 30, 31},
			{59, 65, 30, 31}, {59, 66, 30, 31}, {59, 67, 30, 31}, {59, 68, 30, 31}, {59, 69, 30, 31},
			{40, 64, 32, 63}, {40, 65, 32, 63}, {40, 66, 32, 63}, {40, 67, 32, 63}, {40, 68, 32, 63},
			{40, 69, 32, 63}, {41, 64, 32, 63}, {41, 65, 32, 63}, {41, 66, 32, 63}, {41, 67, 32, 63},
			{41, 68, 32, 63}, {41, 69, 32, 63}, {42, 64, 32, 63}, {42, 65, 32, 63}, {42, 66, 32, 63},
			{42, 67, 32, 63}, {42, 68, 32, 63}, {42, 69, 32, 63}, {43, 64, 32, 63}, {43, 65, 32, 63},
			{43, 66, 32, 63}, {43, 67, 32, 63}, {43, 68, 32, 63}, {43, 69, 32, 63}, {44, 64, 32, 63},
			{44, 65, 32, 63}, {44, 66, 32, 63}, {44, 67, 32, 63}, {44, 68, 32, 63}, {44, 69, 32, 63},
			{45, 64, 32, 63}, {45, 65, 32, 63}, {45, 66, 32, 63}, {45, 67, 32, 63}, {45, 68, 32, 63},
			{45, 69, 32, 63}, {46, 64, 32, 63}, {46, 65, 32, 63}, {46, 66, 32, 63}, {46, 67, 32, 63},
			{46, 68, 32, 63}, {46, 69, 32, 63}, {47, 64, 32, 63}, {47, 65, 32, 63}, {47, 66, 32, 63},
			{47, 67, 32, 63}, {47, 68, 32, 63}, {47, 69, 32, 63}, {48, 64, 32, 63}, {48, 65, 32, 63},
			{48, 66, 32, 63}, {48, 67, 32, 63}, {48, 68, 32, 63}, {48, 69, 32, 63}, {49, 64, 32, 63},
			{49, 65, 32, 63}, {49, 66, 32, 63}, {49, 67, 32, 63}, {49, 68, 32, 63}, {49, 69, 32, 63},
			{50, 64, 32, 63}, {50, 65, 32, 63}, {50, 66, 32, 63}, {50, 67, 32, 63}, {50, 68, 32, 63},
			{50, 69, 32, 63}, {51, 64, 32, 63}, {51, 65, 32, 63}, {51, 66, 32, 63}, {51, 67, 32, 63},
			{51, 68, 32, 63}, {51, 69, 32, 63}, {52, 64, 32, 63}, {52, 65, 32, 63}, {52, 66, 32, 63},
			{52, 67, 32, 63}, {52, 68, 32, 63}, {52, 69, 32, 63}, {53, 64, 32, 63}, {53, 65, 32, 63},
			{53, 66, 32, 63}, {53, 67, 32, 63}, {53, 68, 32, 63}, {53, 69, 32, 63}, {54, 64, 32, 63},
			{54, 65, 32, 63}, {54, 66, 32, 63}, {54, 67, 32, 63}, {54, 68, 32, 63}, {54, 69, 32, 63},
			{55, 64, 32, 63}, {55, 65, 32, 63}, {55, 66, 32, 63}, {55, 67, 32, 63}, {55, 68, 32, 63},
			{55, 69, 32, 63}, {56, 64, 32, 63}, {56, 65, 32, 63}, {56, 66, 32, 63}, {56, 67, 32, 63},
			{56, 68, 32, 63}, {56, 69, 32, 63}, {57, 64, 32, 63}, {57, 65, 32, 63}, {57, 66, 32, 63},
			{57, 67, 32, 63}, {57, 68, 32, 63}, {57, 69, 32, 63}, {58, 64, 32, 63}, {58, 65, 32, 63},
			{58, 66, 32, 63}, {58, 67, 32, 63}, {58, 68, 32, 63}, {58, 69, 32, 63}, {59, 64, 32, 63},
			{59, 65, 32, 63}, {59, 66, 32, 63}, {59, 67, 32, 63}, {59, 68, 32, 63}, {59, 69, 32, 63},
			{40, 64, 64, 79}, {40, 65, 64, 79}, {40, 66, 64, 79}, {40, 67, 64, 79}, {40, 68, 64, 79},
			{40, 69, 64, 79}, {41, 64, 64, 79}, {41, 65, 64, 79}, {41, 66, 64, 79}, {41, 67, 64, 79},
			{41, 68, 64, 79}, {41, 69, 64, 79}, {42, 64, 64, 79}, {42, 65, 64, 79}, {42, 66, 64, 79},
			{42, 67, 64, 79}, {42, 68, 64, 79}, {42, 69, 64, 79}, {43, 64, 64, 79}, {43, 65, 64, 79},
			{43, 66, 64, 79}, {43, 67, 64, 79}, {43, 68, 64, 79}, {43, 69, 64, 79}, {44, 64, 64, 79},
			{44, 65, 64, 79}, {44, 66, 64, 79}, {44, 67, 64, 79}, {44, 68, 64, 79}, {44, 69, 64, 79},
			{45, 64, 64, 79}, {45, 65, 64, 79}, {45, 66, 64, 79}, {45, 67, 64, 79}, {45, 68, 64, 79},
			{45, 69, 64, 79}, {46, 64, 64, 79}, {46, 65, 64, 79}, {46, 66, 64, 79}, {46, 67, 64, 79},
			{46, 68, 64, 79}, {46, 69, 64, 79}, {47, 64, 64, 79}, {47, 65, 64, 79}, {47, 66, 64, 79},
			{47, 67, 64, 79}, {47, 68, 64, 79}, {47, 69, 64, 79}, {48, 64, 64, 79}, {48, 65, 64, 79},
			{48, 66, 64, 79}, {48, 67, 64, 79}, {48, 68, 64, 79}, {48, 69, 64, 79}, {49, 64, 64, 79},
			{49, 65, 64, 79}, {49, 66, 64, 79}, {49, 67, 64, 79}, {49, 68, 64, 79}, {49, 69, 64, 79},
			{50, 64, 64, 79}, {50, 65, 64, 79}, {50, 66, 64, 79}, {50, 67, 64, 79}, {50, 68, 64, 79},
			{50, 69, 64, 79}, {51, 64, 64, 79}, {51, 65, 64, 79}, {51, 66, 64, 79}, {51, 67, 64, 79},
			{51, 68, 64, 79}, {51, 69, 64, 79}, {52, 64, 64, 79}, {52, 65, 64, 79}, {52, 66, 64, 79},
			{52, 67, 64, 79}, {52, 68, 64, 79}, {52, 69, 64, 79}, {53, 64, 64, 79}, {53, 65, 64, 79},
			{53, 66, 64, 79}, {53, 67, 64, 79}, {53, 68, 64, 79}, {53, 69, 64, 79}, {54, 64, 64, 79},
			{54, 65, 64, 79}, {54, 66, 64, 79}, {54, 67, 64, 79}, {54, 68, 64, 79}, {54, 69, 64, 79},
			{55, 64, 64, 79}, {55, 65, 64, 79}, {55, 66, 64, 79}, {55, 67, 64, 79}, {55, 68, 64, 79},
			{55, 69, 64, 79}, {56, 64, 64, 79}, {56, 65, 64, 79}, {56, 66, 64, 79}, {56, 67, 64, 79},
			{56, 68, 64, 79}, {56, 69, 64, 79}, {57, 64, 64, 79}, {57, 65, 64, 79}, {57, 66, 64, 79},
			{57, 67, 64, 79}, {57, 68, 64, 79}, {57, 69, 64, 79}, {58, 64, 64, 79}, {58, 65, 64, 79},
			{58, 66, 64, 79}, {58, 67, 64, 79}, {58, 68, 64, 79}, {58, 69, 64, 79}, {59, 64, 64, 79},
			{59, 65, 64, 79}, {59, 66, 64, 79}, {59, 67, 64, 79}, {59, 68, 64, 79}, {59, 69, 64, 79},
		},
	}
	body2cropped = testBody{
		label:  2,
		offset: dvid.Point3d{30, 20, 40},
		size:   dvid.Point3d{50, 50, 20},
		blockSpans: []dvid.Span{
			{1, 0, 0, 2},
			// {1, 1, 0, 2},
			// {1, 2, 0, 2},
		},
		voxelSpans: []dvid.Span{
			{40, 20, 30, 31}, {40, 21, 30, 31}, {40, 22, 30, 31}, {40, 23, 30, 31}, {40, 24, 30, 31},
			{40, 25, 30, 31}, {40, 26, 30, 31}, {40, 27, 30, 31}, {40, 28, 30, 31}, {40, 29, 30, 31},
			{40, 30, 30, 31}, {40, 31, 30, 31}, {41, 20, 30, 31}, {41, 21, 30, 31}, {41, 22, 30, 31},
			{41, 23, 30, 31}, {41, 24, 30, 31}, {41, 25, 30, 31}, {41, 26, 30, 31}, {41, 27, 30, 31},
			{41, 28, 30, 31}, {41, 29, 30, 31}, {41, 30, 30, 31}, {41, 31, 30, 31}, {42, 20, 30, 31},
			{42, 21, 30, 31}, {42, 22, 30, 31}, {42, 23, 30, 31}, {42, 24, 30, 31}, {42, 25, 30, 31},
			{42, 26, 30, 31}, {42, 27, 30, 31}, {42, 28, 30, 31}, {42, 29, 30, 31}, {42, 30, 30, 31},
			{42, 31, 30, 31}, {43, 20, 30, 31}, {43, 21, 30, 31}, {43, 22, 30, 31}, {43, 23, 30, 31},
			{43, 24, 30, 31}, {43, 25, 30, 31}, {43, 26, 30, 31}, {43, 27, 30, 31}, {43, 28, 30, 31},
			{43, 29, 30, 31}, {43, 30, 30, 31}, {43, 31, 30, 31}, {44, 20, 30, 31}, {44, 21, 30, 31},
			{44, 22, 30, 31}, {44, 23, 30, 31}, {44, 24, 30, 31}, {44, 25, 30, 31}, {44, 26, 30, 31},
			{44, 27, 30, 31}, {44, 28, 30, 31}, {44, 29, 30, 31}, {44, 30, 30, 31}, {44, 31, 30, 31},
			{45, 20, 30, 31}, {45, 21, 30, 31}, {45, 22, 30, 31}, {45, 23, 30, 31}, {45, 24, 30, 31},
			{45, 25, 30, 31}, {45, 26, 30, 31}, {45, 27, 30, 31}, {45, 28, 30, 31}, {45, 29, 30, 31},
			{45, 30, 30, 31}, {45, 31, 30, 31}, {46, 20, 30, 31}, {46, 21, 30, 31}, {46, 22, 30, 31},
			{46, 23, 30, 31}, {46, 24, 30, 31}, {46, 25, 30, 31}, {46, 26, 30, 31}, {46, 27, 30, 31},
			{46, 28, 30, 31}, {46, 29, 30, 31}, {46, 30, 30, 31}, {46, 31, 30, 31}, {47, 20, 30, 31},
			{47, 21, 30, 31}, {47, 22, 30, 31}, {47, 23, 30, 31}, {47, 24, 30, 31}, {47, 25, 30, 31},
			{47, 26, 30, 31}, {47, 27, 30, 31}, {47, 28, 30, 31}, {47, 29, 30, 31}, {47, 30, 30, 31},
			{47, 31, 30, 31}, {48, 20, 30, 31}, {48, 21, 30, 31}, {48, 22, 30, 31}, {48, 23, 30, 31},
			{48, 24, 30, 31}, {48, 25, 30, 31}, {48, 26, 30, 31}, {48, 27, 30, 31}, {48, 28, 30, 31},
			{48, 29, 30, 31}, {48, 30, 30, 31}, {48, 31, 30, 31}, {49, 20, 30, 31}, {49, 21, 30, 31},
			{49, 22, 30, 31}, {49, 23, 30, 31}, {49, 24, 30, 31}, {49, 25, 30, 31}, {49, 26, 30, 31},
			{49, 27, 30, 31}, {49, 28, 30, 31}, {49, 29, 30, 31}, {49, 30, 30, 31}, {49, 31, 30, 31},
			{50, 20, 30, 31}, {50, 21, 30, 31}, {50, 22, 30, 31}, {50, 23, 30, 31}, {50, 24, 30, 31},
			{50, 25, 30, 31}, {50, 26, 30, 31}, {50, 27, 30, 31}, {50, 28, 30, 31}, {50, 29, 30, 31},
			{50, 30, 30, 31}, {50, 31, 30, 31}, {51, 20, 30, 31}, {51, 21, 30, 31}, {51, 22, 30, 31},
			{51, 23, 30, 31}, {51, 24, 30, 31}, {51, 25, 30, 31}, {51, 26, 30, 31}, {51, 27, 30, 31},
			{51, 28, 30, 31}, {51, 29, 30, 31}, {51, 30, 30, 31}, {51, 31, 30, 31}, {52, 20, 30, 31},
			{52, 21, 30, 31}, {52, 22, 30, 31}, {52, 23, 30, 31}, {52, 24, 30, 31}, {52, 25, 30, 31},
			{52, 26, 30, 31}, {52, 27, 30, 31}, {52, 28, 30, 31}, {52, 29, 30, 31}, {52, 30, 30, 31},
			{52, 31, 30, 31}, {53, 20, 30, 31}, {53, 21, 30, 31}, {53, 22, 30, 31}, {53, 23, 30, 31},
			{53, 24, 30, 31}, {53, 25, 30, 31}, {53, 26, 30, 31}, {53, 27, 30, 31}, {53, 28, 30, 31},
			{53, 29, 30, 31}, {53, 30, 30, 31}, {53, 31, 30, 31}, {54, 20, 30, 31}, {54, 21, 30, 31},
			{54, 22, 30, 31}, {54, 23, 30, 31}, {54, 24, 30, 31}, {54, 25, 30, 31}, {54, 26, 30, 31},
			{54, 27, 30, 31}, {54, 28, 30, 31}, {54, 29, 30, 31}, {54, 30, 30, 31}, {54, 31, 30, 31},
			{55, 20, 30, 31}, {55, 21, 30, 31}, {55, 22, 30, 31}, {55, 23, 30, 31}, {55, 24, 30, 31},
			{55, 25, 30, 31}, {55, 26, 30, 31}, {55, 27, 30, 31}, {55, 28, 30, 31}, {55, 29, 30, 31},
			{55, 30, 30, 31}, {55, 31, 30, 31}, {56, 20, 30, 31}, {56, 21, 30, 31}, {56, 22, 30, 31},
			{56, 23, 30, 31}, {56, 24, 30, 31}, {56, 25, 30, 31}, {56, 26, 30, 31}, {56, 27, 30, 31},
			{56, 28, 30, 31}, {56, 29, 30, 31}, {56, 30, 30, 31}, {56, 31, 30, 31}, {57, 20, 30, 31},
			{57, 21, 30, 31}, {57, 22, 30, 31}, {57, 23, 30, 31}, {57, 24, 30, 31}, {57, 25, 30, 31},
			{57, 26, 30, 31}, {57, 27, 30, 31}, {57, 28, 30, 31}, {57, 29, 30, 31}, {57, 30, 30, 31},
			{57, 31, 30, 31}, {58, 20, 30, 31}, {58, 21, 30, 31}, {58, 22, 30, 31}, {58, 23, 30, 31},
			{58, 24, 30, 31}, {58, 25, 30, 31}, {58, 26, 30, 31}, {58, 27, 30, 31}, {58, 28, 30, 31},
			{58, 29, 30, 31}, {58, 30, 30, 31}, {58, 31, 30, 31}, {59, 20, 30, 31}, {59, 21, 30, 31},
			{59, 22, 30, 31}, {59, 23, 30, 31}, {59, 24, 30, 31}, {59, 25, 30, 31}, {59, 26, 30, 31},
			{59, 27, 30, 31}, {59, 28, 30, 31}, {59, 29, 30, 31}, {59, 30, 30, 31}, {59, 31, 30, 31},
			{40, 20, 32, 63}, {40, 21, 32, 63}, {40, 22, 32, 63}, {40, 23, 32, 63}, {40, 24, 32, 63},
			{40, 25, 32, 63}, {40, 26, 32, 63}, {40, 27, 32, 63}, {40, 28, 32, 63}, {40, 29, 32, 63},
			{40, 30, 32, 63}, {40, 31, 32, 63}, {41, 20, 32, 63}, {41, 21, 32, 63}, {41, 22, 32, 63},
			{41, 23, 32, 63}, {41, 24, 32, 63}, {41, 25, 32, 63}, {41, 26, 32, 63}, {41, 27, 32, 63},
			{41, 28, 32, 63}, {41, 29, 32, 63}, {41, 30, 32, 63}, {41, 31, 32, 63}, {42, 20, 32, 63},
			{42, 21, 32, 63}, {42, 22, 32, 63}, {42, 23, 32, 63}, {42, 24, 32, 63}, {42, 25, 32, 63},
			{42, 26, 32, 63}, {42, 27, 32, 63}, {42, 28, 32, 63}, {42, 29, 32, 63}, {42, 30, 32, 63},
			{42, 31, 32, 63}, {43, 20, 32, 63}, {43, 21, 32, 63}, {43, 22, 32, 63}, {43, 23, 32, 63},
			{43, 24, 32, 63}, {43, 25, 32, 63}, {43, 26, 32, 63}, {43, 27, 32, 63}, {43, 28, 32, 63},
			{43, 29, 32, 63}, {43, 30, 32, 63}, {43, 31, 32, 63}, {44, 20, 32, 63}, {44, 21, 32, 63},
			{44, 22, 32, 63}, {44, 23, 32, 63}, {44, 24, 32, 63}, {44, 25, 32, 63}, {44, 26, 32, 63},
			{44, 27, 32, 63}, {44, 28, 32, 63}, {44, 29, 32, 63}, {44, 30, 32, 63}, {44, 31, 32, 63},
			{45, 20, 32, 63}, {45, 21, 32, 63}, {45, 22, 32, 63}, {45, 23, 32, 63}, {45, 24, 32, 63},
			{45, 25, 32, 63}, {45, 26, 32, 63}, {45, 27, 32, 63}, {45, 28, 32, 63}, {45, 29, 32, 63},
			{45, 30, 32, 63}, {45, 31, 32, 63}, {46, 20, 32, 63}, {46, 21, 32, 63}, {46, 22, 32, 63},
			{46, 23, 32, 63}, {46, 24, 32, 63}, {46, 25, 32, 63}, {46, 26, 32, 63}, {46, 27, 32, 63},
			{46, 28, 32, 63}, {46, 29, 32, 63}, {46, 30, 32, 63}, {46, 31, 32, 63}, {47, 20, 32, 63},
			{47, 21, 32, 63}, {47, 22, 32, 63}, {47, 23, 32, 63}, {47, 24, 32, 63}, {47, 25, 32, 63},
			{47, 26, 32, 63}, {47, 27, 32, 63}, {47, 28, 32, 63}, {47, 29, 32, 63}, {47, 30, 32, 63},
			{47, 31, 32, 63}, {48, 20, 32, 63}, {48, 21, 32, 63}, {48, 22, 32, 63}, {48, 23, 32, 63},
			{48, 24, 32, 63}, {48, 25, 32, 63}, {48, 26, 32, 63}, {48, 27, 32, 63}, {48, 28, 32, 63},
			{48, 29, 32, 63}, {48, 30, 32, 63}, {48, 31, 32, 63}, {49, 20, 32, 63}, {49, 21, 32, 63},
			{49, 22, 32, 63}, {49, 23, 32, 63}, {49, 24, 32, 63}, {49, 25, 32, 63}, {49, 26, 32, 63},
			{49, 27, 32, 63}, {49, 28, 32, 63}, {49, 29, 32, 63}, {49, 30, 32, 63}, {49, 31, 32, 63},
			{50, 20, 32, 63}, {50, 21, 32, 63}, {50, 22, 32, 63}, {50, 23, 32, 63}, {50, 24, 32, 63},
			{50, 25, 32, 63}, {50, 26, 32, 63}, {50, 27, 32, 63}, {50, 28, 32, 63}, {50, 29, 32, 63},
			{50, 30, 32, 63}, {50, 31, 32, 63}, {51, 20, 32, 63}, {51, 21, 32, 63}, {51, 22, 32, 63},
			{51, 23, 32, 63}, {51, 24, 32, 63}, {51, 25, 32, 63}, {51, 26, 32, 63}, {51, 27, 32, 63},
			{51, 28, 32, 63}, {51, 29, 32, 63}, {51, 30, 32, 63}, {51, 31, 32, 63}, {52, 20, 32, 63},
			{52, 21, 32, 63}, {52, 22, 32, 63}, {52, 23, 32, 63}, {52, 24, 32, 63}, {52, 25, 32, 63},
			{52, 26, 32, 63}, {52, 27, 32, 63}, {52, 28, 32, 63}, {52, 29, 32, 63}, {52, 30, 32, 63},
			{52, 31, 32, 63}, {53, 20, 32, 63}, {53, 21, 32, 63}, {53, 22, 32, 63}, {53, 23, 32, 63},
			{53, 24, 32, 63}, {53, 25, 32, 63}, {53, 26, 32, 63}, {53, 27, 32, 63}, {53, 28, 32, 63},
			{53, 29, 32, 63}, {53, 30, 32, 63}, {53, 31, 32, 63}, {54, 20, 32, 63}, {54, 21, 32, 63},
			{54, 22, 32, 63}, {54, 23, 32, 63}, {54, 24, 32, 63}, {54, 25, 32, 63}, {54, 26, 32, 63},
			{54, 27, 32, 63}, {54, 28, 32, 63}, {54, 29, 32, 63}, {54, 30, 32, 63}, {54, 31, 32, 63},
			{55, 20, 32, 63}, {55, 21, 32, 63}, {55, 22, 32, 63}, {55, 23, 32, 63}, {55, 24, 32, 63},
			{55, 25, 32, 63}, {55, 26, 32, 63}, {55, 27, 32, 63}, {55, 28, 32, 63}, {55, 29, 32, 63},
			{55, 30, 32, 63}, {55, 31, 32, 63}, {56, 20, 32, 63}, {56, 21, 32, 63}, {56, 22, 32, 63},
			{56, 23, 32, 63}, {56, 24, 32, 63}, {56, 25, 32, 63}, {56, 26, 32, 63}, {56, 27, 32, 63},
			{56, 28, 32, 63}, {56, 29, 32, 63}, {56, 30, 32, 63}, {56, 31, 32, 63}, {57, 20, 32, 63},
			{57, 21, 32, 63}, {57, 22, 32, 63}, {57, 23, 32, 63}, {57, 24, 32, 63}, {57, 25, 32, 63},
			{57, 26, 32, 63}, {57, 27, 32, 63}, {57, 28, 32, 63}, {57, 29, 32, 63}, {57, 30, 32, 63},
			{57, 31, 32, 63}, {58, 20, 32, 63}, {58, 21, 32, 63}, {58, 22, 32, 63}, {58, 23, 32, 63},
			{58, 24, 32, 63}, {58, 25, 32, 63}, {58, 26, 32, 63}, {58, 27, 32, 63}, {58, 28, 32, 63},
			{58, 29, 32, 63}, {58, 30, 32, 63}, {58, 31, 32, 63}, {59, 20, 32, 63}, {59, 21, 32, 63},
			{59, 22, 32, 63}, {59, 23, 32, 63}, {59, 24, 32, 63}, {59, 25, 32, 63}, {59, 26, 32, 63},
			{59, 27, 32, 63}, {59, 28, 32, 63}, {59, 29, 32, 63}, {59, 30, 32, 63}, {59, 31, 32, 63},
			{40, 20, 64, 79}, {40, 21, 64, 79}, {40, 22, 64, 79}, {40, 23, 64, 79}, {40, 24, 64, 79},
			{40, 25, 64, 79}, {40, 26, 64, 79}, {40, 27, 64, 79}, {40, 28, 64, 79}, {40, 29, 64, 79},
			{40, 30, 64, 79}, {40, 31, 64, 79}, {41, 20, 64, 79}, {41, 21, 64, 79}, {41, 22, 64, 79},
			{41, 23, 64, 79}, {41, 24, 64, 79}, {41, 25, 64, 79}, {41, 26, 64, 79}, {41, 27, 64, 79},
			{41, 28, 64, 79}, {41, 29, 64, 79}, {41, 30, 64, 79}, {41, 31, 64, 79}, {42, 20, 64, 79},
			{42, 21, 64, 79}, {42, 22, 64, 79}, {42, 23, 64, 79}, {42, 24, 64, 79}, {42, 25, 64, 79},
			{42, 26, 64, 79}, {42, 27, 64, 79}, {42, 28, 64, 79}, {42, 29, 64, 79}, {42, 30, 64, 79},
			{42, 31, 64, 79}, {43, 20, 64, 79}, {43, 21, 64, 79}, {43, 22, 64, 79}, {43, 23, 64, 79},
			{43, 24, 64, 79}, {43, 25, 64, 79}, {43, 26, 64, 79}, {43, 27, 64, 79}, {43, 28, 64, 79},
			{43, 29, 64, 79}, {43, 30, 64, 79}, {43, 31, 64, 79}, {44, 20, 64, 79}, {44, 21, 64, 79},
			{44, 22, 64, 79}, {44, 23, 64, 79}, {44, 24, 64, 79}, {44, 25, 64, 79}, {44, 26, 64, 79},
			{44, 27, 64, 79}, {44, 28, 64, 79}, {44, 29, 64, 79}, {44, 30, 64, 79}, {44, 31, 64, 79},
			{45, 20, 64, 79}, {45, 21, 64, 79}, {45, 22, 64, 79}, {45, 23, 64, 79}, {45, 24, 64, 79},
			{45, 25, 64, 79}, {45, 26, 64, 79}, {45, 27, 64, 79}, {45, 28, 64, 79}, {45, 29, 64, 79},
			{45, 30, 64, 79}, {45, 31, 64, 79}, {46, 20, 64, 79}, {46, 21, 64, 79}, {46, 22, 64, 79},
			{46, 23, 64, 79}, {46, 24, 64, 79}, {46, 25, 64, 79}, {46, 26, 64, 79}, {46, 27, 64, 79},
			{46, 28, 64, 79}, {46, 29, 64, 79}, {46, 30, 64, 79}, {46, 31, 64, 79}, {47, 20, 64, 79},
			{47, 21, 64, 79}, {47, 22, 64, 79}, {47, 23, 64, 79}, {47, 24, 64, 79}, {47, 25, 64, 79},
			{47, 26, 64, 79}, {47, 27, 64, 79}, {47, 28, 64, 79}, {47, 29, 64, 79}, {47, 30, 64, 79},
			{47, 31, 64, 79}, {48, 20, 64, 79}, {48, 21, 64, 79}, {48, 22, 64, 79}, {48, 23, 64, 79},
			{48, 24, 64, 79}, {48, 25, 64, 79}, {48, 26, 64, 79}, {48, 27, 64, 79}, {48, 28, 64, 79},
			{48, 29, 64, 79}, {48, 30, 64, 79}, {48, 31, 64, 79}, {49, 20, 64, 79}, {49, 21, 64, 79},
			{49, 22, 64, 79}, {49, 23, 64, 79}, {49, 24, 64, 79}, {49, 25, 64, 79}, {49, 26, 64, 79},
			{49, 27, 64, 79}, {49, 28, 64, 79}, {49, 29, 64, 79}, {49, 30, 64, 79}, {49, 31, 64, 79},
			{50, 20, 64, 79}, {50, 21, 64, 79}, {50, 22, 64, 79}, {50, 23, 64, 79}, {50, 24, 64, 79},
			{50, 25, 64, 79}, {50, 26, 64, 79}, {50, 27, 64, 79}, {50, 28, 64, 79}, {50, 29, 64, 79},
			{50, 30, 64, 79}, {50, 31, 64, 79}, {51, 20, 64, 79}, {51, 21, 64, 79}, {51, 22, 64, 79},
			{51, 23, 64, 79}, {51, 24, 64, 79}, {51, 25, 64, 79}, {51, 26, 64, 79}, {51, 27, 64, 79},
			{51, 28, 64, 79}, {51, 29, 64, 79}, {51, 30, 64, 79}, {51, 31, 64, 79}, {52, 20, 64, 79},
			{52, 21, 64, 79}, {52, 22, 64, 79}, {52, 23, 64, 79}, {52, 24, 64, 79}, {52, 25, 64, 79},
			{52, 26, 64, 79}, {52, 27, 64, 79}, {52, 28, 64, 79}, {52, 29, 64, 79}, {52, 30, 64, 79},
			{52, 31, 64, 79}, {53, 20, 64, 79}, {53, 21, 64, 79}, {53, 22, 64, 79}, {53, 23, 64, 79},
			{53, 24, 64, 79}, {53, 25, 64, 79}, {53, 26, 64, 79}, {53, 27, 64, 79}, {53, 28, 64, 79},
			{53, 29, 64, 79}, {53, 30, 64, 79}, {53, 31, 64, 79}, {54, 20, 64, 79}, {54, 21, 64, 79},
			{54, 22, 64, 79}, {54, 23, 64, 79}, {54, 24, 64, 79}, {54, 25, 64, 79}, {54, 26, 64, 79},
			{54, 27, 64, 79}, {54, 28, 64, 79}, {54, 29, 64, 79}, {54, 30, 64, 79}, {54, 31, 64, 79},
			{55, 20, 64, 79}, {55, 21, 64, 79}, {55, 22, 64, 79}, {55, 23, 64, 79}, {55, 24, 64, 79},
			{55, 25, 64, 79}, {55, 26, 64, 79}, {55, 27, 64, 79}, {55, 28, 64, 79}, {55, 29, 64, 79},
			{55, 30, 64, 79}, {55, 31, 64, 79}, {56, 20, 64, 79}, {56, 21, 64, 79}, {56, 22, 64, 79},
			{56, 23, 64, 79}, {56, 24, 64, 79}, {56, 25, 64, 79}, {56, 26, 64, 79}, {56, 27, 64, 79},
			{56, 28, 64, 79}, {56, 29, 64, 79}, {56, 30, 64, 79}, {56, 31, 64, 79}, {57, 20, 64, 79},
			{57, 21, 64, 79}, {57, 22, 64, 79}, {57, 23, 64, 79}, {57, 24, 64, 79}, {57, 25, 64, 79},
			{57, 26, 64, 79}, {57, 27, 64, 79}, {57, 28, 64, 79}, {57, 29, 64, 79}, {57, 30, 64, 79},
			{57, 31, 64, 79}, {58, 20, 64, 79}, {58, 21, 64, 79}, {58, 22, 64, 79}, {58, 23, 64, 79},
			{58, 24, 64, 79}, {58, 25, 64, 79}, {58, 26, 64, 79}, {58, 27, 64, 79}, {58, 28, 64, 79},
			{58, 29, 64, 79}, {58, 30, 64, 79}, {58, 31, 64, 79}, {59, 20, 64, 79}, {59, 21, 64, 79},
			{59, 22, 64, 79}, {59, 23, 64, 79}, {59, 24, 64, 79}, {59, 25, 64, 79}, {59, 26, 64, 79},
			{59, 27, 64, 79}, {59, 28, 64, 79}, {59, 29, 64, 79}, {59, 30, 64, 79}, {59, 31, 64, 79},
		},
	}
	body3 = testBody{
		label:  3,
		offset: dvid.Point3d{40, 40, 10},
		size:   dvid.Point3d{20, 20, 30},
		blockSpans: []dvid.Span{
			{0, 1, 1, 1},
			{1, 1, 1, 1},
		},
		voxelSpans: []dvid.Span{
			{10, 40, 40, 59}, {10, 41, 40, 59}, {10, 42, 40, 59}, {10, 43, 40, 59}, {10, 44, 40, 59},
			{10, 45, 40, 59}, {10, 46, 40, 59}, {10, 47, 40, 59}, {10, 48, 40, 59}, {10, 49, 40, 59},
			{10, 50, 40, 59}, {10, 51, 40, 59}, {10, 52, 40, 59}, {10, 53, 40, 59}, {10, 54, 40, 59},
			{10, 55, 40, 59}, {10, 56, 40, 59}, {10, 57, 40, 59}, {10, 58, 40, 59}, {10, 59, 40, 59},
			{11, 40, 40, 59}, {11, 41, 40, 59}, {11, 42, 40, 59}, {11, 43, 40, 59}, {11, 44, 40, 59},
			{11, 45, 40, 59}, {11, 46, 40, 59}, {11, 47, 40, 59}, {11, 48, 40, 59}, {11, 49, 40, 59},
			{11, 50, 40, 59}, {11, 51, 40, 59}, {11, 52, 40, 59}, {11, 53, 40, 59}, {11, 54, 40, 59},
			{11, 55, 40, 59}, {11, 56, 40, 59}, {11, 57, 40, 59}, {11, 58, 40, 59}, {11, 59, 40, 59},
			{12, 40, 40, 59}, {12, 41, 40, 59}, {12, 42, 40, 59}, {12, 43, 40, 59}, {12, 44, 40, 59},
			{12, 45, 40, 59}, {12, 46, 40, 59}, {12, 47, 40, 59}, {12, 48, 40, 59}, {12, 49, 40, 59},
			{12, 50, 40, 59}, {12, 51, 40, 59}, {12, 52, 40, 59}, {12, 53, 40, 59}, {12, 54, 40, 59},
			{12, 55, 40, 59}, {12, 56, 40, 59}, {12, 57, 40, 59}, {12, 58, 40, 59}, {12, 59, 40, 59},
			{13, 40, 40, 59}, {13, 41, 40, 59}, {13, 42, 40, 59}, {13, 43, 40, 59}, {13, 44, 40, 59},
			{13, 45, 40, 59}, {13, 46, 40, 59}, {13, 47, 40, 59}, {13, 48, 40, 59}, {13, 49, 40, 59},
			{13, 50, 40, 59}, {13, 51, 40, 59}, {13, 52, 40, 59}, {13, 53, 40, 59}, {13, 54, 40, 59},
			{13, 55, 40, 59}, {13, 56, 40, 59}, {13, 57, 40, 59}, {13, 58, 40, 59}, {13, 59, 40, 59},
			{14, 40, 40, 59}, {14, 41, 40, 59}, {14, 42, 40, 59}, {14, 43, 40, 59}, {14, 44, 40, 59},
			{14, 45, 40, 59}, {14, 46, 40, 59}, {14, 47, 40, 59}, {14, 48, 40, 59}, {14, 49, 40, 59},
			{14, 50, 40, 59}, {14, 51, 40, 59}, {14, 52, 40, 59}, {14, 53, 40, 59}, {14, 54, 40, 59},
			{14, 55, 40, 59}, {14, 56, 40, 59}, {14, 57, 40, 59}, {14, 58, 40, 59}, {14, 59, 40, 59},
			{15, 40, 40, 59}, {15, 41, 40, 59}, {15, 42, 40, 59}, {15, 43, 40, 59}, {15, 44, 40, 59},
			{15, 45, 40, 59}, {15, 46, 40, 59}, {15, 47, 40, 59}, {15, 48, 40, 59}, {15, 49, 40, 59},
			{15, 50, 40, 59}, {15, 51, 40, 59}, {15, 52, 40, 59}, {15, 53, 40, 59}, {15, 54, 40, 59},
			{15, 55, 40, 59}, {15, 56, 40, 59}, {15, 57, 40, 59}, {15, 58, 40, 59}, {15, 59, 40, 59},
			{16, 40, 40, 59}, {16, 41, 40, 59}, {16, 42, 40, 59}, {16, 43, 40, 59}, {16, 44, 40, 59},
			{16, 45, 40, 59}, {16, 46, 40, 59}, {16, 47, 40, 59}, {16, 48, 40, 59}, {16, 49, 40, 59},
			{16, 50, 40, 59}, {16, 51, 40, 59}, {16, 52, 40, 59}, {16, 53, 40, 59}, {16, 54, 40, 59},
			{16, 55, 40, 59}, {16, 56, 40, 59}, {16, 57, 40, 59}, {16, 58, 40, 59}, {16, 59, 40, 59},
			{17, 40, 40, 59}, {17, 41, 40, 59}, {17, 42, 40, 59}, {17, 43, 40, 59}, {17, 44, 40, 59},
			{17, 45, 40, 59}, {17, 46, 40, 59}, {17, 47, 40, 59}, {17, 48, 40, 59}, {17, 49, 40, 59},
			{17, 50, 40, 59}, {17, 51, 40, 59}, {17, 52, 40, 59}, {17, 53, 40, 59}, {17, 54, 40, 59},
			{17, 55, 40, 59}, {17, 56, 40, 59}, {17, 57, 40, 59}, {17, 58, 40, 59}, {17, 59, 40, 59},
			{18, 40, 40, 59}, {18, 41, 40, 59}, {18, 42, 40, 59}, {18, 43, 40, 59}, {18, 44, 40, 59},
			{18, 45, 40, 59}, {18, 46, 40, 59}, {18, 47, 40, 59}, {18, 48, 40, 59}, {18, 49, 40, 59},
			{18, 50, 40, 59}, {18, 51, 40, 59}, {18, 52, 40, 59}, {18, 53, 40, 59}, {18, 54, 40, 59},
			{18, 55, 40, 59}, {18, 56, 40, 59}, {18, 57, 40, 59}, {18, 58, 40, 59}, {18, 59, 40, 59},
			{19, 40, 40, 59}, {19, 41, 40, 59}, {19, 42, 40, 59}, {19, 43, 40, 59}, {19, 44, 40, 59},
			{19, 45, 40, 59}, {19, 46, 40, 59}, {19, 47, 40, 59}, {19, 48, 40, 59}, {19, 49, 40, 59},
			{19, 50, 40, 59}, {19, 51, 40, 59}, {19, 52, 40, 59}, {19, 53, 40, 59}, {19, 54, 40, 59},
			{19, 55, 40, 59}, {19, 56, 40, 59}, {19, 57, 40, 59}, {19, 58, 40, 59}, {19, 59, 40, 59},
			{20, 40, 40, 59}, {20, 41, 40, 59}, {20, 42, 40, 59}, {20, 43, 40, 59}, {20, 44, 40, 59},
			{20, 45, 40, 59}, {20, 46, 40, 59}, {20, 47, 40, 59}, {20, 48, 40, 59}, {20, 49, 40, 59},
			{20, 50, 40, 59}, {20, 51, 40, 59}, {20, 52, 40, 59}, {20, 53, 40, 59}, {20, 54, 40, 59},
			{20, 55, 40, 59}, {20, 56, 40, 59}, {20, 57, 40, 59}, {20, 58, 40, 59}, {20, 59, 40, 59},
			{21, 40, 40, 59}, {21, 41, 40, 59}, {21, 42, 40, 59}, {21, 43, 40, 59}, {21, 44, 40, 59},
			{21, 45, 40, 59}, {21, 46, 40, 59}, {21, 47, 40, 59}, {21, 48, 40, 59}, {21, 49, 40, 59},
			{21, 50, 40, 59}, {21, 51, 40, 59}, {21, 52, 40, 59}, {21, 53, 40, 59}, {21, 54, 40, 59},
			{21, 55, 40, 59}, {21, 56, 40, 59}, {21, 57, 40, 59}, {21, 58, 40, 59}, {21, 59, 40, 59},
			{22, 40, 40, 59}, {22, 41, 40, 59}, {22, 42, 40, 59}, {22, 43, 40, 59}, {22, 44, 40, 59},
			{22, 45, 40, 59}, {22, 46, 40, 59}, {22, 47, 40, 59}, {22, 48, 40, 59}, {22, 49, 40, 59},
			{22, 50, 40, 59}, {22, 51, 40, 59}, {22, 52, 40, 59}, {22, 53, 40, 59}, {22, 54, 40, 59},
			{22, 55, 40, 59}, {22, 56, 40, 59}, {22, 57, 40, 59}, {22, 58, 40, 59}, {22, 59, 40, 59},
			{23, 40, 40, 59}, {23, 41, 40, 59}, {23, 42, 40, 59}, {23, 43, 40, 59}, {23, 44, 40, 59},
			{23, 45, 40, 59}, {23, 46, 40, 59}, {23, 47, 40, 59}, {23, 48, 40, 59}, {23, 49, 40, 59},
			{23, 50, 40, 59}, {23, 51, 40, 59}, {23, 52, 40, 59}, {23, 53, 40, 59}, {23, 54, 40, 59},
			{23, 55, 40, 59}, {23, 56, 40, 59}, {23, 57, 40, 59}, {23, 58, 40, 59}, {23, 59, 40, 59},
			{24, 40, 40, 59}, {24, 41, 40, 59}, {24, 42, 40, 59}, {24, 43, 40, 59}, {24, 44, 40, 59},
			{24, 45, 40, 59}, {24, 46, 40, 59}, {24, 47, 40, 59}, {24, 48, 40, 59}, {24, 49, 40, 59},
			{24, 50, 40, 59}, {24, 51, 40, 59}, {24, 52, 40, 59}, {24, 53, 40, 59}, {24, 54, 40, 59},
			{24, 55, 40, 59}, {24, 56, 40, 59}, {24, 57, 40, 59}, {24, 58, 40, 59}, {24, 59, 40, 59},
			{25, 40, 40, 59}, {25, 41, 40, 59}, {25, 42, 40, 59}, {25, 43, 40, 59}, {25, 44, 40, 59},
			{25, 45, 40, 59}, {25, 46, 40, 59}, {25, 47, 40, 59}, {25, 48, 40, 59}, {25, 49, 40, 59},
			{25, 50, 40, 59}, {25, 51, 40, 59}, {25, 52, 40, 59}, {25, 53, 40, 59}, {25, 54, 40, 59},
			{25, 55, 40, 59}, {25, 56, 40, 59}, {25, 57, 40, 59}, {25, 58, 40, 59}, {25, 59, 40, 59},
			{26, 40, 40, 59}, {26, 41, 40, 59}, {26, 42, 40, 59}, {26, 43, 40, 59}, {26, 44, 40, 59},
			{26, 45, 40, 59}, {26, 46, 40, 59}, {26, 47, 40, 59}, {26, 48, 40, 59}, {26, 49, 40, 59},
			{26, 50, 40, 59}, {26, 51, 40, 59}, {26, 52, 40, 59}, {26, 53, 40, 59}, {26, 54, 40, 59},
			{26, 55, 40, 59}, {26, 56, 40, 59}, {26, 57, 40, 59}, {26, 58, 40, 59}, {26, 59, 40, 59},
			{27, 40, 40, 59}, {27, 41, 40, 59}, {27, 42, 40, 59}, {27, 43, 40, 59}, {27, 44, 40, 59},
			{27, 45, 40, 59}, {27, 46, 40, 59}, {27, 47, 40, 59}, {27, 48, 40, 59}, {27, 49, 40, 59},
			{27, 50, 40, 59}, {27, 51, 40, 59}, {27, 52, 40, 59}, {27, 53, 40, 59}, {27, 54, 40, 59},
			{27, 55, 40, 59}, {27, 56, 40, 59}, {27, 57, 40, 59}, {27, 58, 40, 59}, {27, 59, 40, 59},
			{28, 40, 40, 59}, {28, 41, 40, 59}, {28, 42, 40, 59}, {28, 43, 40, 59}, {28, 44, 40, 59},
			{28, 45, 40, 59}, {28, 46, 40, 59}, {28, 47, 40, 59}, {28, 48, 40, 59}, {28, 49, 40, 59},
			{28, 50, 40, 59}, {28, 51, 40, 59}, {28, 52, 40, 59}, {28, 53, 40, 59}, {28, 54, 40, 59},
			{28, 55, 40, 59}, {28, 56, 40, 59}, {28, 57, 40, 59}, {28, 58, 40, 59}, {28, 59, 40, 59},
			{29, 40, 40, 59}, {29, 41, 40, 59}, {29, 42, 40, 59}, {29, 43, 40, 59}, {29, 44, 40, 59},
			{29, 45, 40, 59}, {29, 46, 40, 59}, {29, 47, 40, 59}, {29, 48, 40, 59}, {29, 49, 40, 59},
			{29, 50, 40, 59}, {29, 51, 40, 59}, {29, 52, 40, 59}, {29, 53, 40, 59}, {29, 54, 40, 59},
			{29, 55, 40, 59}, {29, 56, 40, 59}, {29, 57, 40, 59}, {29, 58, 40, 59}, {29, 59, 40, 59},
			{30, 40, 40, 59}, {30, 41, 40, 59}, {30, 42, 40, 59}, {30, 43, 40, 59}, {30, 44, 40, 59},
			{30, 45, 40, 59}, {30, 46, 40, 59}, {30, 47, 40, 59}, {30, 48, 40, 59}, {30, 49, 40, 59},
			{30, 50, 40, 59}, {30, 51, 40, 59}, {30, 52, 40, 59}, {30, 53, 40, 59}, {30, 54, 40, 59},
			{30, 55, 40, 59}, {30, 56, 40, 59}, {30, 57, 40, 59}, {30, 58, 40, 59}, {30, 59, 40, 59},
			{31, 40, 40, 59}, {31, 41, 40, 59}, {31, 42, 40, 59}, {31, 43, 40, 59}, {31, 44, 40, 59},
			{31, 45, 40, 59}, {31, 46, 40, 59}, {31, 47, 40, 59}, {31, 48, 40, 59}, {31, 49, 40, 59},
			{31, 50, 40, 59}, {31, 51, 40, 59}, {31, 52, 40, 59}, {31, 53, 40, 59}, {31, 54, 40, 59},
			{31, 55, 40, 59}, {31, 56, 40, 59}, {31, 57, 40, 59}, {31, 58, 40, 59}, {31, 59, 40, 59},
			{32, 40, 40, 59}, {32, 41, 40, 59}, {32, 42, 40, 59}, {32, 43, 40, 59}, {32, 44, 40, 59},
			{32, 45, 40, 59}, {32, 46, 40, 59}, {32, 47, 40, 59}, {32, 48, 40, 59}, {32, 49, 40, 59},
			{32, 50, 40, 59}, {32, 51, 40, 59}, {32, 52, 40, 59}, {32, 53, 40, 59}, {32, 54, 40, 59},
			{32, 55, 40, 59}, {32, 56, 40, 59}, {32, 57, 40, 59}, {32, 58, 40, 59}, {32, 59, 40, 59},
			{33, 40, 40, 59}, {33, 41, 40, 59}, {33, 42, 40, 59}, {33, 43, 40, 59}, {33, 44, 40, 59},
			{33, 45, 40, 59}, {33, 46, 40, 59}, {33, 47, 40, 59}, {33, 48, 40, 59}, {33, 49, 40, 59},
			{33, 50, 40, 59}, {33, 51, 40, 59}, {33, 52, 40, 59}, {33, 53, 40, 59}, {33, 54, 40, 59},
			{33, 55, 40, 59}, {33, 56, 40, 59}, {33, 57, 40, 59}, {33, 58, 40, 59}, {33, 59, 40, 59},
			{34, 40, 40, 59}, {34, 41, 40, 59}, {34, 42, 40, 59}, {34, 43, 40, 59}, {34, 44, 40, 59},
			{34, 45, 40, 59}, {34, 46, 40, 59}, {34, 47, 40, 59}, {34, 48, 40, 59}, {34, 49, 40, 59},
			{34, 50, 40, 59}, {34, 51, 40, 59}, {34, 52, 40, 59}, {34, 53, 40, 59}, {34, 54, 40, 59},
			{34, 55, 40, 59}, {34, 56, 40, 59}, {34, 57, 40, 59}, {34, 58, 40, 59}, {34, 59, 40, 59},
			{35, 40, 40, 59}, {35, 41, 40, 59}, {35, 42, 40, 59}, {35, 43, 40, 59}, {35, 44, 40, 59},
			{35, 45, 40, 59}, {35, 46, 40, 59}, {35, 47, 40, 59}, {35, 48, 40, 59}, {35, 49, 40, 59},
			{35, 50, 40, 59}, {35, 51, 40, 59}, {35, 52, 40, 59}, {35, 53, 40, 59}, {35, 54, 40, 59},
			{35, 55, 40, 59}, {35, 56, 40, 59}, {35, 57, 40, 59}, {35, 58, 40, 59}, {35, 59, 40, 59},
			{36, 40, 40, 59}, {36, 41, 40, 59}, {36, 42, 40, 59}, {36, 43, 40, 59}, {36, 44, 40, 59},
			{36, 45, 40, 59}, {36, 46, 40, 59}, {36, 47, 40, 59}, {36, 48, 40, 59}, {36, 49, 40, 59},
			{36, 50, 40, 59}, {36, 51, 40, 59}, {36, 52, 40, 59}, {36, 53, 40, 59}, {36, 54, 40, 59},
			{36, 55, 40, 59}, {36, 56, 40, 59}, {36, 57, 40, 59}, {36, 58, 40, 59}, {36, 59, 40, 59},
			{37, 40, 40, 59}, {37, 41, 40, 59}, {37, 42, 40, 59}, {37, 43, 40, 59}, {37, 44, 40, 59},
			{37, 45, 40, 59}, {37, 46, 40, 59}, {37, 47, 40, 59}, {37, 48, 40, 59}, {37, 49, 40, 59},
			{37, 50, 40, 59}, {37, 51, 40, 59}, {37, 52, 40, 59}, {37, 53, 40, 59}, {37, 54, 40, 59},
			{37, 55, 40, 59}, {37, 56, 40, 59}, {37, 57, 40, 59}, {37, 58, 40, 59}, {37, 59, 40, 59},
			{38, 40, 40, 59}, {38, 41, 40, 59}, {38, 42, 40, 59}, {38, 43, 40, 59}, {38, 44, 40, 59},
			{38, 45, 40, 59}, {38, 46, 40, 59}, {38, 47, 40, 59}, {38, 48, 40, 59}, {38, 49, 40, 59},
			{38, 50, 40, 59}, {38, 51, 40, 59}, {38, 52, 40, 59}, {38, 53, 40, 59}, {38, 54, 40, 59},
			{38, 55, 40, 59}, {38, 56, 40, 59}, {38, 57, 40, 59}, {38, 58, 40, 59}, {38, 59, 40, 59},
			{39, 40, 40, 59}, {39, 41, 40, 59}, {39, 42, 40, 59}, {39, 43, 40, 59}, {39, 44, 40, 59},
			{39, 45, 40, 59}, {39, 46, 40, 59}, {39, 47, 40, 59}, {39, 48, 40, 59}, {39, 49, 40, 59},
			{39, 50, 40, 59}, {39, 51, 40, 59}, {39, 52, 40, 59}, {39, 53, 40, 59}, {39, 54, 40, 59},
			{39, 55, 40, 59}, {39, 56, 40, 59}, {39, 57, 40, 59}, {39, 58, 40, 59}, {39, 59, 40, 59},
		},
	}
	body4 = testBody{
		label:  4,
		offset: dvid.Point3d{75, 40, 60},
		size:   dvid.Point3d{20, 20, 30},
		blockSpans: []dvid.Span{
			{1, 1, 2, 2},
			{2, 1, 2, 2},
		},
		voxelSpans: []dvid.Span{
			{60, 40, 75, 94}, {60, 41, 75, 94}, {60, 42, 75, 94}, {60, 43, 75, 94}, {60, 44, 75, 94},
			{60, 45, 75, 94}, {60, 46, 75, 94}, {60, 47, 75, 94}, {60, 48, 75, 94}, {60, 49, 75, 94},
			{60, 50, 75, 94}, {60, 51, 75, 94}, {60, 52, 75, 94}, {60, 53, 75, 94}, {60, 54, 75, 94},
			{60, 55, 75, 94}, {60, 56, 75, 94}, {60, 57, 75, 94}, {60, 58, 75, 94}, {60, 59, 75, 94},
			{61, 40, 75, 94}, {61, 41, 75, 94}, {61, 42, 75, 94}, {61, 43, 75, 94}, {61, 44, 75, 94},
			{61, 45, 75, 94}, {61, 46, 75, 94}, {61, 47, 75, 94}, {61, 48, 75, 94}, {61, 49, 75, 94},
			{61, 50, 75, 94}, {61, 51, 75, 94}, {61, 52, 75, 94}, {61, 53, 75, 94}, {61, 54, 75, 94},
			{61, 55, 75, 94}, {61, 56, 75, 94}, {61, 57, 75, 94}, {61, 58, 75, 94}, {61, 59, 75, 94},
			{62, 40, 75, 94}, {62, 41, 75, 94}, {62, 42, 75, 94}, {62, 43, 75, 94}, {62, 44, 75, 94},
			{62, 45, 75, 94}, {62, 46, 75, 94}, {62, 47, 75, 94}, {62, 48, 75, 94}, {62, 49, 75, 94},
			{62, 50, 75, 94}, {62, 51, 75, 94}, {62, 52, 75, 94}, {62, 53, 75, 94}, {62, 54, 75, 94},
			{62, 55, 75, 94}, {62, 56, 75, 94}, {62, 57, 75, 94}, {62, 58, 75, 94}, {62, 59, 75, 94},
			{63, 40, 75, 94}, {63, 41, 75, 94}, {63, 42, 75, 94}, {63, 43, 75, 94}, {63, 44, 75, 94},
			{63, 45, 75, 94}, {63, 46, 75, 94}, {63, 47, 75, 94}, {63, 48, 75, 94}, {63, 49, 75, 94},
			{63, 50, 75, 94}, {63, 51, 75, 94}, {63, 52, 75, 94}, {63, 53, 75, 94}, {63, 54, 75, 94},
			{63, 55, 75, 94}, {63, 56, 75, 94}, {63, 57, 75, 94}, {63, 58, 75, 94}, {63, 59, 75, 94},
			{64, 40, 75, 94}, {64, 41, 75, 94}, {64, 42, 75, 94}, {64, 43, 75, 94}, {64, 44, 75, 94},
			{64, 45, 75, 94}, {64, 46, 75, 94}, {64, 47, 75, 94}, {64, 48, 75, 94}, {64, 49, 75, 94},
			{64, 50, 75, 94}, {64, 51, 75, 94}, {64, 52, 75, 94}, {64, 53, 75, 94}, {64, 54, 75, 94},
			{64, 55, 75, 94}, {64, 56, 75, 94}, {64, 57, 75, 94}, {64, 58, 75, 94}, {64, 59, 75, 94},
			{65, 40, 75, 94}, {65, 41, 75, 94}, {65, 42, 75, 94}, {65, 43, 75, 94}, {65, 44, 75, 94},
			{65, 45, 75, 94}, {65, 46, 75, 94}, {65, 47, 75, 94}, {65, 48, 75, 94}, {65, 49, 75, 94},
			{65, 50, 75, 94}, {65, 51, 75, 94}, {65, 52, 75, 94}, {65, 53, 75, 94}, {65, 54, 75, 94},
			{65, 55, 75, 94}, {65, 56, 75, 94}, {65, 57, 75, 94}, {65, 58, 75, 94}, {65, 59, 75, 94},
			{66, 40, 75, 94}, {66, 41, 75, 94}, {66, 42, 75, 94}, {66, 43, 75, 94}, {66, 44, 75, 94},
			{66, 45, 75, 94}, {66, 46, 75, 94}, {66, 47, 75, 94}, {66, 48, 75, 94}, {66, 49, 75, 94},
			{66, 50, 75, 94}, {66, 51, 75, 94}, {66, 52, 75, 94}, {66, 53, 75, 94}, {66, 54, 75, 94},
			{66, 55, 75, 94}, {66, 56, 75, 94}, {66, 57, 75, 94}, {66, 58, 75, 94}, {66, 59, 75, 94},
			{67, 40, 75, 94}, {67, 41, 75, 94}, {67, 42, 75, 94}, {67, 43, 75, 94}, {67, 44, 75, 94},
			{67, 45, 75, 94}, {67, 46, 75, 94}, {67, 47, 75, 94}, {67, 48, 75, 94}, {67, 49, 75, 94},
			{67, 50, 75, 94}, {67, 51, 75, 94}, {67, 52, 75, 94}, {67, 53, 75, 94}, {67, 54, 75, 94},
			{67, 55, 75, 94}, {67, 56, 75, 94}, {67, 57, 75, 94}, {67, 58, 75, 94}, {67, 59, 75, 94},
			{68, 40, 75, 94}, {68, 41, 75, 94}, {68, 42, 75, 94}, {68, 43, 75, 94}, {68, 44, 75, 94},
			{68, 45, 75, 94}, {68, 46, 75, 94}, {68, 47, 75, 94}, {68, 48, 75, 94}, {68, 49, 75, 94},
			{68, 50, 75, 94}, {68, 51, 75, 94}, {68, 52, 75, 94}, {68, 53, 75, 94}, {68, 54, 75, 94},
			{68, 55, 75, 94}, {68, 56, 75, 94}, {68, 57, 75, 94}, {68, 58, 75, 94}, {68, 59, 75, 94},
			{69, 40, 75, 94}, {69, 41, 75, 94}, {69, 42, 75, 94}, {69, 43, 75, 94}, {69, 44, 75, 94},
			{69, 45, 75, 94}, {69, 46, 75, 94}, {69, 47, 75, 94}, {69, 48, 75, 94}, {69, 49, 75, 94},
			{69, 50, 75, 94}, {69, 51, 75, 94}, {69, 52, 75, 94}, {69, 53, 75, 94}, {69, 54, 75, 94},
			{69, 55, 75, 94}, {69, 56, 75, 94}, {69, 57, 75, 94}, {69, 58, 75, 94}, {69, 59, 75, 94},
			{70, 40, 75, 94}, {70, 41, 75, 94}, {70, 42, 75, 94}, {70, 43, 75, 94}, {70, 44, 75, 94},
			{70, 45, 75, 94}, {70, 46, 75, 94}, {70, 47, 75, 94}, {70, 48, 75, 94}, {70, 49, 75, 94},
			{70, 50, 75, 94}, {70, 51, 75, 94}, {70, 52, 75, 94}, {70, 53, 75, 94}, {70, 54, 75, 94},
			{70, 55, 75, 94}, {70, 56, 75, 94}, {70, 57, 75, 94}, {70, 58, 75, 94}, {70, 59, 75, 94},
			{71, 40, 75, 94}, {71, 41, 75, 94}, {71, 42, 75, 94}, {71, 43, 75, 94}, {71, 44, 75, 94},
			{71, 45, 75, 94}, {71, 46, 75, 94}, {71, 47, 75, 94}, {71, 48, 75, 94}, {71, 49, 75, 94},
			{71, 50, 75, 94}, {71, 51, 75, 94}, {71, 52, 75, 94}, {71, 53, 75, 94}, {71, 54, 75, 94},
			{71, 55, 75, 94}, {71, 56, 75, 94}, {71, 57, 75, 94}, {71, 58, 75, 94}, {71, 59, 75, 94},
			{72, 40, 75, 94}, {72, 41, 75, 94}, {72, 42, 75, 94}, {72, 43, 75, 94}, {72, 44, 75, 94},
			{72, 45, 75, 94}, {72, 46, 75, 94}, {72, 47, 75, 94}, {72, 48, 75, 94}, {72, 49, 75, 94},
			{72, 50, 75, 94}, {72, 51, 75, 94}, {72, 52, 75, 94}, {72, 53, 75, 94}, {72, 54, 75, 94},
			{72, 55, 75, 94}, {72, 56, 75, 94}, {72, 57, 75, 94}, {72, 58, 75, 94}, {72, 59, 75, 94},
			{73, 40, 75, 94}, {73, 41, 75, 94}, {73, 42, 75, 94}, {73, 43, 75, 94}, {73, 44, 75, 94},
			{73, 45, 75, 94}, {73, 46, 75, 94}, {73, 47, 75, 94}, {73, 48, 75, 94}, {73, 49, 75, 94},
			{73, 50, 75, 94}, {73, 51, 75, 94}, {73, 52, 75, 94}, {73, 53, 75, 94}, {73, 54, 75, 94},
			{73, 55, 75, 94}, {73, 56, 75, 94}, {73, 57, 75, 94}, {73, 58, 75, 94}, {73, 59, 75, 94},
			{74, 40, 75, 94}, {74, 41, 75, 94}, {74, 42, 75, 94}, {74, 43, 75, 94}, {74, 44, 75, 94},
			{74, 45, 75, 94}, {74, 46, 75, 94}, {74, 47, 75, 94}, {74, 48, 75, 94}, {74, 49, 75, 94},
			{74, 50, 75, 94}, {74, 51, 75, 94}, {74, 52, 75, 94}, {74, 53, 75, 94}, {74, 54, 75, 94},
			{74, 55, 75, 94}, {74, 56, 75, 94}, {74, 57, 75, 94}, {74, 58, 75, 94}, {74, 59, 75, 94},
			{75, 40, 75, 94}, {75, 41, 75, 94}, {75, 42, 75, 94}, {75, 43, 75, 94}, {75, 44, 75, 94},
			{75, 45, 75, 94}, {75, 46, 75, 94}, {75, 47, 75, 94}, {75, 48, 75, 94}, {75, 49, 75, 94},
			{75, 50, 75, 94}, {75, 51, 75, 94}, {75, 52, 75, 94}, {75, 53, 75, 94}, {75, 54, 75, 94},
			{75, 55, 75, 94}, {75, 56, 75, 94}, {75, 57, 75, 94}, {75, 58, 75, 94}, {75, 59, 75, 94},
			{76, 40, 75, 94}, {76, 41, 75, 94}, {76, 42, 75, 94}, {76, 43, 75, 94}, {76, 44, 75, 94},
			{76, 45, 75, 94}, {76, 46, 75, 94}, {76, 47, 75, 94}, {76, 48, 75, 94}, {76, 49, 75, 94},
			{76, 50, 75, 94}, {76, 51, 75, 94}, {76, 52, 75, 94}, {76, 53, 75, 94}, {76, 54, 75, 94},
			{76, 55, 75, 94}, {76, 56, 75, 94}, {76, 57, 75, 94}, {76, 58, 75, 94}, {76, 59, 75, 94},
			{77, 40, 75, 94}, {77, 41, 75, 94}, {77, 42, 75, 94}, {77, 43, 75, 94}, {77, 44, 75, 94},
			{77, 45, 75, 94}, {77, 46, 75, 94}, {77, 47, 75, 94}, {77, 48, 75, 94}, {77, 49, 75, 94},
			{77, 50, 75, 94}, {77, 51, 75, 94}, {77, 52, 75, 94}, {77, 53, 75, 94}, {77, 54, 75, 94},
			{77, 55, 75, 94}, {77, 56, 75, 94}, {77, 57, 75, 94}, {77, 58, 75, 94}, {77, 59, 75, 94},
			{78, 40, 75, 94}, {78, 41, 75, 94}, {78, 42, 75, 94}, {78, 43, 75, 94}, {78, 44, 75, 94},
			{78, 45, 75, 94}, {78, 46, 75, 94}, {78, 47, 75, 94}, {78, 48, 75, 94}, {78, 49, 75, 94},
			{78, 50, 75, 94}, {78, 51, 75, 94}, {78, 52, 75, 94}, {78, 53, 75, 94}, {78, 54, 75, 94},
			{78, 55, 75, 94}, {78, 56, 75, 94}, {78, 57, 75, 94}, {78, 58, 75, 94}, {78, 59, 75, 94},
			{79, 40, 75, 94}, {79, 41, 75, 94}, {79, 42, 75, 94}, {79, 43, 75, 94}, {79, 44, 75, 94},
			{79, 45, 75, 94}, {79, 46, 75, 94}, {79, 47, 75, 94}, {79, 48, 75, 94}, {79, 49, 75, 94},
			{79, 50, 75, 94}, {79, 51, 75, 94}, {79, 52, 75, 94}, {79, 53, 75, 94}, {79, 54, 75, 94},
			{79, 55, 75, 94}, {79, 56, 75, 94}, {79, 57, 75, 94}, {79, 58, 75, 94}, {79, 59, 75, 94},
			{80, 40, 75, 94}, {80, 41, 75, 94}, {80, 42, 75, 94}, {80, 43, 75, 94}, {80, 44, 75, 94},
			{80, 45, 75, 94}, {80, 46, 75, 94}, {80, 47, 75, 94}, {80, 48, 75, 94}, {80, 49, 75, 94},
			{80, 50, 75, 94}, {80, 51, 75, 94}, {80, 52, 75, 94}, {80, 53, 75, 94}, {80, 54, 75, 94},
			{80, 55, 75, 94}, {80, 56, 75, 94}, {80, 57, 75, 94}, {80, 58, 75, 94}, {80, 59, 75, 94},
			{81, 40, 75, 94}, {81, 41, 75, 94}, {81, 42, 75, 94}, {81, 43, 75, 94}, {81, 44, 75, 94},
			{81, 45, 75, 94}, {81, 46, 75, 94}, {81, 47, 75, 94}, {81, 48, 75, 94}, {81, 49, 75, 94},
			{81, 50, 75, 94}, {81, 51, 75, 94}, {81, 52, 75, 94}, {81, 53, 75, 94}, {81, 54, 75, 94},
			{81, 55, 75, 94}, {81, 56, 75, 94}, {81, 57, 75, 94}, {81, 58, 75, 94}, {81, 59, 75, 94},
			{82, 40, 75, 94}, {82, 41, 75, 94}, {82, 42, 75, 94}, {82, 43, 75, 94}, {82, 44, 75, 94},
			{82, 45, 75, 94}, {82, 46, 75, 94}, {82, 47, 75, 94}, {82, 48, 75, 94}, {82, 49, 75, 94},
			{82, 50, 75, 94}, {82, 51, 75, 94}, {82, 52, 75, 94}, {82, 53, 75, 94}, {82, 54, 75, 94},
			{82, 55, 75, 94}, {82, 56, 75, 94}, {82, 57, 75, 94}, {82, 58, 75, 94}, {82, 59, 75, 94},
			{83, 40, 75, 94}, {83, 41, 75, 94}, {83, 42, 75, 94}, {83, 43, 75, 94}, {83, 44, 75, 94},
			{83, 45, 75, 94}, {83, 46, 75, 94}, {83, 47, 75, 94}, {83, 48, 75, 94}, {83, 49, 75, 94},
			{83, 50, 75, 94}, {83, 51, 75, 94}, {83, 52, 75, 94}, {83, 53, 75, 94}, {83, 54, 75, 94},
			{83, 55, 75, 94}, {83, 56, 75, 94}, {83, 57, 75, 94}, {83, 58, 75, 94}, {83, 59, 75, 94},
			{84, 40, 75, 94}, {84, 41, 75, 94}, {84, 42, 75, 94}, {84, 43, 75, 94}, {84, 44, 75, 94},
			{84, 45, 75, 94}, {84, 46, 75, 94}, {84, 47, 75, 94}, {84, 48, 75, 94}, {84, 49, 75, 94},
			{84, 50, 75, 94}, {84, 51, 75, 94}, {84, 52, 75, 94}, {84, 53, 75, 94}, {84, 54, 75, 94},
			{84, 55, 75, 94}, {84, 56, 75, 94}, {84, 57, 75, 94}, {84, 58, 75, 94}, {84, 59, 75, 94},
			{85, 40, 75, 94}, {85, 41, 75, 94}, {85, 42, 75, 94}, {85, 43, 75, 94}, {85, 44, 75, 94},
			{85, 45, 75, 94}, {85, 46, 75, 94}, {85, 47, 75, 94}, {85, 48, 75, 94}, {85, 49, 75, 94},
			{85, 50, 75, 94}, {85, 51, 75, 94}, {85, 52, 75, 94}, {85, 53, 75, 94}, {85, 54, 75, 94},
			{85, 55, 75, 94}, {85, 56, 75, 94}, {85, 57, 75, 94}, {85, 58, 75, 94}, {85, 59, 75, 94},
			{86, 40, 75, 94}, {86, 41, 75, 94}, {86, 42, 75, 94}, {86, 43, 75, 94}, {86, 44, 75, 94},
			{86, 45, 75, 94}, {86, 46, 75, 94}, {86, 47, 75, 94}, {86, 48, 75, 94}, {86, 49, 75, 94},
			{86, 50, 75, 94}, {86, 51, 75, 94}, {86, 52, 75, 94}, {86, 53, 75, 94}, {86, 54, 75, 94},
			{86, 55, 75, 94}, {86, 56, 75, 94}, {86, 57, 75, 94}, {86, 58, 75, 94}, {86, 59, 75, 94},
			{87, 40, 75, 94}, {87, 41, 75, 94}, {87, 42, 75, 94}, {87, 43, 75, 94}, {87, 44, 75, 94},
			{87, 45, 75, 94}, {87, 46, 75, 94}, {87, 47, 75, 94}, {87, 48, 75, 94}, {87, 49, 75, 94},
			{87, 50, 75, 94}, {87, 51, 75, 94}, {87, 52, 75, 94}, {87, 53, 75, 94}, {87, 54, 75, 94},
			{87, 55, 75, 94}, {87, 56, 75, 94}, {87, 57, 75, 94}, {87, 58, 75, 94}, {87, 59, 75, 94},
			{88, 40, 75, 94}, {88, 41, 75, 94}, {88, 42, 75, 94}, {88, 43, 75, 94}, {88, 44, 75, 94},
			{88, 45, 75, 94}, {88, 46, 75, 94}, {88, 47, 75, 94}, {88, 48, 75, 94}, {88, 49, 75, 94},
			{88, 50, 75, 94}, {88, 51, 75, 94}, {88, 52, 75, 94}, {88, 53, 75, 94}, {88, 54, 75, 94},
			{88, 55, 75, 94}, {88, 56, 75, 94}, {88, 57, 75, 94}, {88, 58, 75, 94}, {88, 59, 75, 94},
			{89, 40, 75, 94}, {89, 41, 75, 94}, {89, 42, 75, 94}, {89, 43, 75, 94}, {89, 44, 75, 94},
			{89, 45, 75, 94}, {89, 46, 75, 94}, {89, 47, 75, 94}, {89, 48, 75, 94}, {89, 49, 75, 94},
			{89, 50, 75, 94}, {89, 51, 75, 94}, {89, 52, 75, 94}, {89, 53, 75, 94}, {89, 54, 75, 94},
			{89, 55, 75, 94}, {89, 56, 75, 94}, {89, 57, 75, 94}, {89, 58, 75, 94}, {89, 59, 75, 94},
		},
	}
	bodyleft = testBody{
		label:  4,
		offset: dvid.Point3d{75, 40, 60},
		size:   dvid.Point3d{20, 20, 21},
		blockSpans: []dvid.Span{
			{1, 1, 2, 2},
			{2, 1, 2, 2},
		},
		voxelSpans: []dvid.Span{
			{60, 40, 75, 94}, {60, 41, 75, 94}, {60, 42, 75, 94}, {60, 43, 75, 94}, {60, 44, 75, 94},
			{60, 45, 75, 94}, {60, 46, 75, 94}, {60, 47, 75, 94}, {60, 48, 75, 94}, {60, 49, 75, 94},
			{60, 50, 75, 94}, {60, 51, 75, 94}, {60, 52, 75, 94}, {60, 53, 75, 94}, {60, 54, 75, 94},
			{60, 55, 75, 94}, {60, 56, 75, 94}, {60, 57, 75, 94}, {60, 58, 75, 94}, {60, 59, 75, 94},
			{61, 40, 75, 94}, {61, 41, 75, 94}, {61, 42, 75, 94}, {61, 43, 75, 94}, {61, 44, 75, 94},
			{61, 45, 75, 94}, {61, 46, 75, 94}, {61, 47, 75, 94}, {61, 48, 75, 94}, {61, 49, 75, 94},
			{61, 50, 75, 94}, {61, 51, 75, 94}, {61, 52, 75, 94}, {61, 53, 75, 94}, {61, 54, 75, 94},
			{61, 55, 75, 94}, {61, 56, 75, 94}, {61, 57, 75, 94}, {61, 58, 75, 94}, {61, 59, 75, 94},
			{62, 40, 75, 94}, {62, 41, 75, 94}, {62, 42, 75, 94}, {62, 43, 75, 94}, {62, 44, 75, 94},
			{62, 45, 75, 94}, {62, 46, 75, 94}, {62, 47, 75, 94}, {62, 48, 75, 94}, {62, 49, 75, 94},
			{62, 50, 75, 94}, {62, 51, 75, 94}, {62, 52, 75, 94}, {62, 53, 75, 94}, {62, 54, 75, 94},
			{62, 55, 75, 94}, {62, 56, 75, 94}, {62, 57, 75, 94}, {62, 58, 75, 94}, {62, 59, 75, 94},
			{63, 40, 75, 94}, {63, 41, 75, 94}, {63, 42, 75, 94}, {63, 43, 75, 94}, {63, 44, 75, 94},
			{63, 45, 75, 94}, {63, 46, 75, 94}, {63, 47, 75, 94}, {63, 48, 75, 94}, {63, 49, 75, 94},
			{63, 50, 75, 94}, {63, 51, 75, 94}, {63, 52, 75, 94}, {63, 53, 75, 94}, {63, 54, 75, 94},
			{63, 55, 75, 94}, {63, 56, 75, 94}, {63, 57, 75, 94}, {63, 58, 75, 94}, {63, 59, 75, 94},
			{64, 40, 75, 94}, {64, 41, 75, 94}, {64, 42, 75, 94}, {64, 43, 75, 94}, {64, 44, 75, 94},
			{64, 45, 75, 94}, {64, 46, 75, 94}, {64, 47, 75, 94}, {64, 48, 75, 94}, {64, 49, 75, 94},
			{64, 50, 75, 94}, {64, 51, 75, 94}, {64, 52, 75, 94}, {64, 53, 75, 94}, {64, 54, 75, 94},
			{64, 55, 75, 94}, {64, 56, 75, 94}, {64, 57, 75, 94}, {64, 58, 75, 94}, {64, 59, 75, 94},
			{65, 40, 75, 94}, {65, 41, 75, 94}, {65, 42, 75, 94}, {65, 43, 75, 94}, {65, 44, 75, 94},
			{65, 45, 75, 94}, {65, 46, 75, 94}, {65, 47, 75, 94}, {65, 48, 75, 94}, {65, 49, 75, 94},
			{65, 50, 75, 94}, {65, 51, 75, 94}, {65, 52, 75, 94}, {65, 53, 75, 94}, {65, 54, 75, 94},
			{65, 55, 75, 94}, {65, 56, 75, 94}, {65, 57, 75, 94}, {65, 58, 75, 94}, {65, 59, 75, 94},
			{66, 40, 75, 94}, {66, 41, 75, 94}, {66, 42, 75, 94}, {66, 43, 75, 94}, {66, 44, 75, 94},
			{66, 45, 75, 94}, {66, 46, 75, 94}, {66, 47, 75, 94}, {66, 48, 75, 94}, {66, 49, 75, 94},
			{66, 50, 75, 94}, {66, 51, 75, 94}, {66, 52, 75, 94}, {66, 53, 75, 94}, {66, 54, 75, 94},
			{66, 55, 75, 94}, {66, 56, 75, 94}, {66, 57, 75, 94}, {66, 58, 75, 94}, {66, 59, 75, 94},
			{67, 40, 75, 94}, {67, 41, 75, 94}, {67, 42, 75, 94}, {67, 43, 75, 94}, {67, 44, 75, 94},
			{67, 45, 75, 94}, {67, 46, 75, 94}, {67, 47, 75, 94}, {67, 48, 75, 94}, {67, 49, 75, 94},
			{67, 50, 75, 94}, {67, 51, 75, 94}, {67, 52, 75, 94}, {67, 53, 75, 94}, {67, 54, 75, 94},
			{67, 55, 75, 94}, {67, 56, 75, 94}, {67, 57, 75, 94}, {67, 58, 75, 94}, {67, 59, 75, 94},
			{68, 40, 75, 94}, {68, 41, 75, 94}, {68, 42, 75, 94}, {68, 43, 75, 94}, {68, 44, 75, 94},
			{68, 45, 75, 94}, {68, 46, 75, 94}, {68, 47, 75, 94}, {68, 48, 75, 94}, {68, 49, 75, 94},
			{68, 50, 75, 94}, {68, 51, 75, 94}, {68, 52, 75, 94}, {68, 53, 75, 94}, {68, 54, 75, 94},
			{68, 55, 75, 94}, {68, 56, 75, 94}, {68, 57, 75, 94}, {68, 58, 75, 94}, {68, 59, 75, 94},
			{69, 40, 75, 94}, {69, 41, 75, 94}, {69, 42, 75, 94}, {69, 43, 75, 94}, {69, 44, 75, 94},
			{69, 45, 75, 94}, {69, 46, 75, 94}, {69, 47, 75, 94}, {69, 48, 75, 94}, {69, 49, 75, 94},
			{69, 50, 75, 94}, {69, 51, 75, 94}, {69, 52, 75, 94}, {69, 53, 75, 94}, {69, 54, 75, 94},
			{69, 55, 75, 94}, {69, 56, 75, 94}, {69, 57, 75, 94}, {69, 58, 75, 94}, {69, 59, 75, 94},
			{70, 40, 75, 94}, {70, 41, 75, 94}, {70, 42, 75, 94}, {70, 43, 75, 94}, {70, 44, 75, 94},
			{70, 45, 75, 94}, {70, 46, 75, 94}, {70, 47, 75, 94}, {70, 48, 75, 94}, {70, 49, 75, 94},
			{70, 50, 75, 94}, {70, 51, 75, 94}, {70, 52, 75, 94}, {70, 53, 75, 94}, {70, 54, 75, 94},
			{70, 55, 75, 94}, {70, 56, 75, 94}, {70, 57, 75, 94}, {70, 58, 75, 94}, {70, 59, 75, 94},
			{71, 40, 75, 94}, {71, 41, 75, 94}, {71, 42, 75, 94}, {71, 43, 75, 94}, {71, 44, 75, 94},
			{71, 45, 75, 94}, {71, 46, 75, 94}, {71, 47, 75, 94}, {71, 48, 75, 94}, {71, 49, 75, 94},
			{71, 50, 75, 94}, {71, 51, 75, 94}, {71, 52, 75, 94}, {71, 53, 75, 94}, {71, 54, 75, 94},
			{71, 55, 75, 94}, {71, 56, 75, 94}, {71, 57, 75, 94}, {71, 58, 75, 94}, {71, 59, 75, 94},
			{72, 40, 75, 94}, {72, 41, 75, 94}, {72, 42, 75, 94}, {72, 43, 75, 94}, {72, 44, 75, 94},
			{72, 45, 75, 94}, {72, 46, 75, 94}, {72, 47, 75, 94}, {72, 48, 75, 94}, {72, 49, 75, 94},
			{72, 50, 75, 94}, {72, 51, 75, 94}, {72, 52, 75, 94}, {72, 53, 75, 94}, {72, 54, 75, 94},
			{72, 55, 75, 94}, {72, 56, 75, 94}, {72, 57, 75, 94}, {72, 58, 75, 94}, {72, 59, 75, 94},
			{73, 40, 75, 94}, {73, 41, 75, 94}, {73, 42, 75, 94}, {73, 43, 75, 94}, {73, 44, 75, 94},
			{73, 45, 75, 94}, {73, 46, 75, 94}, {73, 47, 75, 94}, {73, 48, 75, 94}, {73, 49, 75, 94},
			{73, 50, 75, 94}, {73, 51, 75, 94}, {73, 52, 75, 94}, {73, 53, 75, 94}, {73, 54, 75, 94},
			{73, 55, 75, 94}, {73, 56, 75, 94}, {73, 57, 75, 94}, {73, 58, 75, 94}, {73, 59, 75, 94},
			{74, 40, 75, 94}, {74, 41, 75, 94}, {74, 42, 75, 94}, {74, 43, 75, 94}, {74, 44, 75, 94},
			{74, 45, 75, 94}, {74, 46, 75, 94}, {74, 47, 75, 94}, {74, 48, 75, 94}, {74, 49, 75, 94},
			{74, 50, 75, 94}, {74, 51, 75, 94}, {74, 52, 75, 94}, {74, 53, 75, 94}, {74, 54, 75, 94},
			{74, 55, 75, 94}, {74, 56, 75, 94}, {74, 57, 75, 94}, {74, 58, 75, 94}, {74, 59, 75, 94},
			{75, 40, 75, 94}, {75, 41, 75, 94}, {75, 42, 75, 94}, {75, 43, 75, 94}, {75, 44, 75, 94},
			{75, 45, 75, 94}, {75, 46, 75, 94}, {75, 47, 75, 94}, {75, 48, 75, 94}, {75, 49, 75, 94},
			{75, 50, 75, 94}, {75, 51, 75, 94}, {75, 52, 75, 94}, {75, 53, 75, 94}, {75, 54, 75, 94},
			{75, 55, 75, 94}, {75, 56, 75, 94}, {75, 57, 75, 94}, {75, 58, 75, 94}, {75, 59, 75, 94},
			{76, 40, 75, 94}, {76, 41, 75, 94}, {76, 42, 75, 94}, {76, 43, 75, 94}, {76, 44, 75, 94},
			{76, 45, 75, 94}, {76, 46, 75, 94}, {76, 47, 75, 94}, {76, 48, 75, 94}, {76, 49, 75, 94},
			{76, 50, 75, 94}, {76, 51, 75, 94}, {76, 52, 75, 94}, {76, 53, 75, 94}, {76, 54, 75, 94},
			{76, 55, 75, 94}, {76, 56, 75, 94}, {76, 57, 75, 94}, {76, 58, 75, 94}, {76, 59, 75, 94},
			{77, 40, 75, 94}, {77, 41, 75, 94}, {77, 42, 75, 94}, {77, 43, 75, 94}, {77, 44, 75, 94},
			{77, 45, 75, 94}, {77, 46, 75, 94}, {77, 47, 75, 94}, {77, 48, 75, 94}, {77, 49, 75, 94},
			{77, 50, 75, 94}, {77, 51, 75, 94}, {77, 52, 75, 94}, {77, 53, 75, 94}, {77, 54, 75, 94},
			{77, 55, 75, 94}, {77, 56, 75, 94}, {77, 57, 75, 94}, {77, 58, 75, 94}, {77, 59, 75, 94},
			{78, 40, 75, 94}, {78, 41, 75, 94}, {78, 42, 75, 94}, {78, 43, 75, 94}, {78, 44, 75, 94},
			{78, 45, 75, 94}, {78, 46, 75, 94}, {78, 47, 75, 94}, {78, 48, 75, 94}, {78, 49, 75, 94},
			{78, 50, 75, 94}, {78, 51, 75, 94}, {78, 52, 75, 94}, {78, 53, 75, 94}, {78, 54, 75, 94},
			{78, 55, 75, 94}, {78, 56, 75, 94}, {78, 57, 75, 94}, {78, 58, 75, 94}, {78, 59, 75, 94},
			{79, 40, 75, 94}, {79, 41, 75, 94}, {79, 42, 75, 94}, {79, 43, 75, 94}, {79, 44, 75, 94},
			{79, 45, 75, 94}, {79, 46, 75, 94}, {79, 47, 75, 94}, {79, 48, 75, 94}, {79, 49, 75, 94},
			{79, 50, 75, 94}, {79, 51, 75, 94}, {79, 52, 75, 94}, {79, 53, 75, 94}, {79, 54, 75, 94},
			{79, 55, 75, 94}, {79, 56, 75, 94}, {79, 57, 75, 94}, {79, 58, 75, 94}, {79, 59, 75, 94},
			{80, 40, 75, 80}, {80, 40, 87, 89}, {80, 40, 93, 94},
		},
	}
	bodysplit = testBody{
		label:  5,
		offset: dvid.Point3d{75, 40, 80},
		size:   dvid.Point3d{20, 20, 10},
		blockSpans: []dvid.Span{
			{2, 1, 2, 2},
		},
		voxelSpans: []dvid.Span{
			{80, 40, 81, 86}, {80, 40, 90, 92}, // These first 2 test splits interleaved in one span.
			{80, 41, 75, 94}, {80, 42, 75, 94}, {80, 43, 75, 94}, {80, 44, 75, 94},
			{80, 45, 75, 94}, {80, 46, 75, 94}, {80, 47, 75, 94}, {80, 48, 75, 94}, {80, 49, 75, 94},
			{80, 50, 75, 94}, {80, 51, 75, 94}, {80, 52, 75, 94}, {80, 53, 75, 94}, {80, 54, 75, 94},
			{80, 55, 75, 94}, {80, 56, 75, 94}, {80, 57, 75, 94}, {80, 58, 75, 94}, {80, 59, 75, 94},
			{81, 40, 75, 94}, {81, 41, 75, 94}, {81, 42, 75, 94}, {81, 43, 75, 94}, {81, 44, 75, 94},
			{81, 45, 75, 94}, {81, 46, 75, 94}, {81, 47, 75, 94}, {81, 48, 75, 94}, {81, 49, 75, 94},
			{81, 50, 75, 94}, {81, 51, 75, 94}, {81, 52, 75, 94}, {81, 53, 75, 94}, {81, 54, 75, 94},
			{81, 55, 75, 94}, {81, 56, 75, 94}, {81, 57, 75, 94}, {81, 58, 75, 94}, {81, 59, 75, 94},
			{82, 40, 75, 94}, {82, 41, 75, 94}, {82, 42, 75, 94}, {82, 43, 75, 94}, {82, 44, 75, 94},
			{82, 45, 75, 94}, {82, 46, 75, 94}, {82, 47, 75, 94}, {82, 48, 75, 94}, {82, 49, 75, 94},
			{82, 50, 75, 94}, {82, 51, 75, 94}, {82, 52, 75, 94}, {82, 53, 75, 94}, {82, 54, 75, 94},
			{82, 55, 75, 94}, {82, 56, 75, 94}, {82, 57, 75, 94}, {82, 58, 75, 94}, {82, 59, 75, 94},
			{83, 40, 75, 94}, {83, 41, 75, 94}, {83, 42, 75, 94}, {83, 43, 75, 94}, {83, 44, 75, 94},
			{83, 45, 75, 94}, {83, 46, 75, 94}, {83, 47, 75, 94}, {83, 48, 75, 94}, {83, 49, 75, 94},
			{83, 50, 75, 94}, {83, 51, 75, 94}, {83, 52, 75, 94}, {83, 53, 75, 94}, {83, 54, 75, 94},
			{83, 55, 75, 94}, {83, 56, 75, 94}, {83, 57, 75, 94}, {83, 58, 75, 94}, {83, 59, 75, 94},
			{84, 40, 75, 94}, {84, 41, 75, 94}, {84, 42, 75, 94}, {84, 43, 75, 94}, {84, 44, 75, 94},
			{84, 45, 75, 94}, {84, 46, 75, 94}, {84, 47, 75, 94}, {84, 48, 75, 94}, {84, 49, 75, 94},
			{84, 50, 75, 94}, {84, 51, 75, 94}, {84, 52, 75, 94}, {84, 53, 75, 94}, {84, 54, 75, 94},
			{84, 55, 75, 94}, {84, 56, 75, 94}, {84, 57, 75, 94}, {84, 58, 75, 94}, {84, 59, 75, 94},
			{85, 40, 75, 94}, {85, 41, 75, 94}, {85, 42, 75, 94}, {85, 43, 75, 94}, {85, 44, 75, 94},
			{85, 45, 75, 94}, {85, 46, 75, 94}, {85, 47, 75, 94}, {85, 48, 75, 94}, {85, 49, 75, 94},
			{85, 50, 75, 94}, {85, 51, 75, 94}, {85, 52, 75, 94}, {85, 53, 75, 94}, {85, 54, 75, 94},
			{85, 55, 75, 94}, {85, 56, 75, 94}, {85, 57, 75, 94}, {85, 58, 75, 94}, {85, 59, 75, 94},
			{86, 40, 75, 94}, {86, 41, 75, 94}, {86, 42, 75, 94}, {86, 43, 75, 94}, {86, 44, 75, 94},
			{86, 45, 75, 94}, {86, 46, 75, 94}, {86, 47, 75, 94}, {86, 48, 75, 94}, {86, 49, 75, 94},
			{86, 50, 75, 94}, {86, 51, 75, 94}, {86, 52, 75, 94}, {86, 53, 75, 94}, {86, 54, 75, 94},
			{86, 55, 75, 94}, {86, 56, 75, 94}, {86, 57, 75, 94}, {86, 58, 75, 94}, {86, 59, 75, 94},
			{87, 40, 75, 94}, {87, 41, 75, 94}, {87, 42, 75, 94}, {87, 43, 75, 94}, {87, 44, 75, 94},
			{87, 45, 75, 94}, {87, 46, 75, 94}, {87, 47, 75, 94}, {87, 48, 75, 94}, {87, 49, 75, 94},
			{87, 50, 75, 94}, {87, 51, 75, 94}, {87, 52, 75, 94}, {87, 53, 75, 94}, {87, 54, 75, 94},
			{87, 55, 75, 94}, {87, 56, 75, 94}, {87, 57, 75, 94}, {87, 58, 75, 94}, {87, 59, 75, 94},
			{88, 40, 75, 94}, {88, 41, 75, 94}, {88, 42, 75, 94}, {88, 43, 75, 94}, {88, 44, 75, 94},
			{88, 45, 75, 94}, {88, 46, 75, 94}, {88, 47, 75, 94}, {88, 48, 75, 94}, {88, 49, 75, 94},
			{88, 50, 75, 94}, {88, 51, 75, 94}, {88, 52, 75, 94}, {88, 53, 75, 94}, {88, 54, 75, 94},
			{88, 55, 75, 94}, {88, 56, 75, 94}, {88, 57, 75, 94}, {88, 58, 75, 94}, {88, 59, 75, 94},
			{89, 40, 75, 94}, {89, 41, 75, 94}, {89, 42, 75, 94}, {89, 43, 75, 94}, {89, 44, 75, 94},
			{89, 45, 75, 94}, {89, 46, 75, 94}, {89, 47, 75, 94}, {89, 48, 75, 94}, {89, 49, 75, 94},
			{89, 50, 75, 94}, {89, 51, 75, 94}, {89, 52, 75, 94}, {89, 53, 75, 94}, {89, 54, 75, 94},
			{89, 55, 75, 94}, {89, 56, 75, 94}, {89, 57, 75, 94}, {89, 58, 75, 94}, {89, 59, 75, 94},
		},
	}
	body6 = testBody{
		label:  6,
		offset: dvid.Point3d{8, 10, 7},
		size:   dvid.Point3d{52, 50, 10},
		blockSpans: []dvid.Span{
			{0, 0, 0, 1},
			{0, 1, 0, 1},
		},
		voxelSpans: []dvid.Span{
			{8, 11, 9, 31}, {8, 12, 9, 59}, {8, 13, 9, 59}, {8, 14, 9, 59},
			{8, 15, 19, 59}, {8, 16, 19, 59}, {8, 17, 19, 59}, {8, 18, 19, 59},
			{8, 19, 29, 59}, {8, 20, 29, 59}, {8, 21, 29, 59}, {8, 22, 29, 59},
			{8, 23, 39, 59}, {8, 24, 39, 59}, {8, 25, 39, 59}, {8, 26, 39, 59},
			{8, 23, 39, 59}, {8, 24, 39, 59}, {8, 25, 39, 59}, {8, 26, 39, 59},
			{8, 27, 39, 59}, {8, 28, 39, 59}, {8, 29, 39, 59}, {8, 30, 39, 59},
			{8, 31, 39, 59}, {8, 32, 39, 59}, {8, 33, 39, 59}, {8, 34, 39, 59},
			{8, 35, 39, 59}, {8, 36, 39, 59}, {8, 37, 45, 59}, {8, 38, 39, 59},
			{8, 39, 39, 59}, {8, 40, 39, 59}, {8, 41, 42, 59}, {8, 42, 39, 56},
			{8, 43, 39, 59}, {8, 44, 39, 59}, {8, 45, 39, 59}, {8, 46, 39, 50},

			{8, 11, 9, 59}, {8, 12, 9, 59}, {8, 13, 9, 59}, {8, 14, 9, 59},
			{8, 15, 19, 59}, {8, 16, 19, 59}, {8, 17, 19, 59}, {8, 18, 19, 59},
			{8, 19, 29, 59}, {8, 20, 29, 59}, {8, 21, 29, 59}, {8, 22, 29, 59},
			{8, 23, 39, 59}, {8, 24, 39, 59}, {8, 25, 39, 59}, {8, 26, 39, 59},
			{8, 23, 39, 59}, {8, 24, 39, 59}, {8, 25, 39, 59}, {8, 26, 39, 59},
			{8, 27, 39, 59}, {8, 28, 39, 59}, {8, 29, 39, 59}, {8, 30, 39, 59},
			{8, 31, 39, 59}, {8, 32, 39, 59}, {8, 33, 39, 59}, {8, 34, 39, 59},
			{8, 35, 39, 59}, {8, 36, 39, 59}, {8, 37, 45, 59}, {8, 38, 39, 59},
			{8, 39, 39, 59}, {8, 40, 39, 59}, {8, 41, 42, 59}, {8, 42, 39, 56},
			{8, 43, 39, 59}, {8, 44, 39, 59}, {8, 45, 39, 59}, {8, 46, 39, 50},

			{9, 11, 9, 59}, {9, 12, 9, 59}, {9, 13, 9, 59}, {9, 14, 9, 59},
			{9, 15, 19, 59}, {9, 16, 19, 59}, {9, 17, 19, 59}, {9, 18, 19, 59},
			{9, 19, 29, 59}, {9, 20, 29, 59}, {9, 21, 29, 59}, {9, 22, 29, 59},
			{9, 23, 39, 59}, {9, 24, 39, 59}, {9, 25, 39, 59}, {9, 26, 39, 59},
			{9, 23, 39, 59}, {9, 24, 39, 59}, {9, 25, 39, 59}, {9, 26, 39, 59},
			{9, 27, 39, 59}, {9, 28, 39, 59}, {9, 29, 39, 59}, {9, 30, 39, 59},
			{9, 31, 39, 59}, {9, 32, 39, 59}, {9, 33, 39, 59}, {9, 34, 39, 59},
			{9, 35, 39, 59}, {9, 36, 39, 59}, {9, 37, 45, 59}, {9, 38, 39, 59},
			{9, 39, 39, 59}, {9, 40, 39, 59}, {9, 41, 42, 59}, {9, 42, 39, 56},
			{9, 43, 39, 59}, {9, 44, 39, 59}, {9, 45, 39, 59}, {9, 46, 39, 50},

			{10, 11, 9, 59}, {10, 12, 9, 59}, {10, 13, 9, 59}, {10, 14, 9, 59},
			{10, 15, 19, 59}, {10, 16, 19, 59}, {10, 17, 19, 59}, {10, 18, 19, 59},
			{10, 19, 29, 59}, {10, 20, 29, 59}, {10, 21, 29, 59}, {10, 22, 29, 59},
			{10, 23, 39, 59}, {10, 24, 39, 59}, {10, 25, 39, 59}, {10, 26, 39, 59},
			{10, 23, 39, 59}, {10, 24, 39, 59}, {10, 25, 39, 59}, {10, 26, 39, 59},
			{10, 27, 39, 59}, {10, 28, 39, 59}, {10, 29, 39, 59}, {10, 30, 39, 59},
			{10, 31, 39, 59}, {10, 32, 39, 59}, {10, 33, 39, 59}, {10, 34, 39, 59},
			{10, 35, 39, 59}, {10, 36, 39, 59}, {10, 37, 45, 59}, {10, 38, 39, 59},
			{10, 39, 39, 59}, {10, 40, 39, 59}, {10, 41, 42, 59}, {10, 42, 39, 56},
			{10, 43, 39, 59}, {10, 44, 39, 59}, {10, 45, 39, 59}, {10, 46, 39, 50},

			{11, 11, 9, 59}, {11, 12, 9, 59}, {11, 13, 9, 59}, {11, 14, 9, 59},
			{11, 15, 19, 59}, {11, 16, 19, 59}, {11, 17, 19, 59}, {11, 18, 19, 59},
			{11, 19, 29, 59}, {11, 20, 29, 59}, {11, 21, 29, 59}, {11, 22, 29, 59},
			{11, 23, 39, 59}, {11, 24, 39, 59}, {11, 25, 39, 59}, {11, 26, 39, 59},
			{11, 23, 39, 59}, {11, 24, 39, 59}, {11, 25, 39, 59}, {11, 26, 39, 59},
			{11, 27, 39, 59}, {11, 28, 39, 59}, {11, 29, 39, 59}, {11, 30, 39, 59},
			{11, 31, 39, 59}, {11, 32, 39, 59}, {11, 33, 39, 59}, {11, 34, 39, 59},
			{11, 35, 39, 59}, {11, 36, 39, 59}, {11, 37, 45, 59}, {11, 38, 39, 59},
			{11, 39, 39, 59}, {11, 40, 39, 59}, {11, 41, 42, 59}, {11, 42, 39, 56},
			{11, 43, 39, 59}, {11, 44, 39, 59}, {11, 45, 39, 59}, {11, 46, 39, 50},

			{12, 11, 9, 59}, {12, 12, 9, 59}, {12, 13, 9, 59}, {12, 14, 9, 59},
			{12, 15, 19, 59}, {12, 16, 19, 59}, {12, 17, 19, 59}, {12, 18, 19, 59},
			{12, 19, 29, 59}, {12, 20, 29, 59}, {12, 21, 29, 59}, {12, 22, 29, 59},
			{12, 23, 39, 59}, {12, 24, 39, 59}, {12, 25, 39, 59}, {12, 26, 39, 59},
			{12, 23, 39, 59}, {12, 24, 39, 59}, {12, 25, 39, 59}, {12, 26, 39, 59},
			{12, 27, 39, 59}, {12, 28, 39, 59}, {12, 29, 39, 59}, {12, 30, 39, 59},
			{12, 31, 39, 59}, {12, 32, 39, 59}, {12, 33, 39, 59}, {12, 34, 39, 59},
			{12, 35, 39, 59}, {12, 36, 39, 59}, {12, 37, 45, 59}, {12, 38, 39, 59},
			{12, 39, 39, 59}, {12, 40, 39, 59}, {12, 41, 42, 59}, {12, 42, 39, 56},
			{12, 43, 39, 59}, {12, 44, 39, 59}, {12, 45, 39, 59}, {12, 46, 39, 50},
		},
	}
	body7 = testBody{
		label:  7,
		offset: dvid.Point3d{68, 10, 7},
		size:   dvid.Point3d{52, 50, 10},
		blockSpans: []dvid.Span{
			{2, 0, 0, 1},
			{2, 1, 0, 1},
		},
		voxelSpans: []dvid.Span{
			{78, 11, 9, 59}, {78, 12, 9, 59}, {78, 13, 9, 59}, {78, 14, 9, 59},
			{78, 15, 19, 59}, {78, 16, 19, 59}, {78, 17, 19, 59}, {78, 18, 19, 59},
			{78, 19, 29, 59}, {78, 20, 29, 59}, {78, 21, 29, 59}, {78, 22, 29, 59},
			{78, 23, 39, 59}, {78, 24, 39, 59}, {78, 25, 39, 59}, {78, 26, 39, 59},
			{78, 23, 39, 59}, {78, 24, 39, 59}, {78, 25, 39, 59}, {78, 26, 39, 59},
			{78, 27, 39, 59}, {78, 28, 39, 59}, {78, 29, 39, 59}, {78, 30, 39, 59},
			{78, 31, 39, 59}, {78, 32, 39, 59}, {78, 33, 39, 59}, {78, 34, 39, 59},
			{78, 35, 39, 59}, {78, 36, 39, 59}, {78, 37, 45, 59}, {78, 38, 39, 59},
			{78, 39, 39, 59}, {78, 40, 39, 59}, {78, 41, 42, 59}, {78, 42, 39, 56},
			{78, 43, 39, 59}, {78, 44, 39, 59}, {78, 45, 39, 59}, {78, 46, 39, 50},

			{79, 11, 9, 59}, {79, 12, 9, 59}, {79, 13, 9, 59}, {79, 14, 9, 59},
			{79, 15, 19, 59}, {79, 16, 19, 59}, {79, 17, 19, 59}, {79, 18, 19, 59},
			{79, 19, 29, 59}, {79, 20, 29, 59}, {79, 21, 29, 59}, {79, 22, 29, 59},
			{79, 23, 39, 59}, {79, 24, 39, 59}, {79, 25, 39, 59}, {79, 26, 39, 59},
			{79, 23, 39, 59}, {79, 24, 39, 59}, {79, 25, 39, 59}, {79, 26, 39, 59},
			{79, 27, 39, 59}, {79, 28, 39, 59}, {79, 29, 39, 59}, {79, 30, 39, 59},
			{79, 31, 39, 59}, {79, 32, 39, 59}, {79, 33, 39, 59}, {79, 34, 39, 59},
			{79, 35, 39, 59}, {79, 36, 39, 59}, {79, 37, 45, 59}, {79, 38, 39, 59},
			{79, 39, 39, 59}, {79, 40, 39, 59}, {79, 41, 42, 59}, {79, 42, 39, 56},
			{79, 43, 39, 59}, {79, 44, 39, 59}, {79, 45, 39, 59}, {79, 46, 39, 50},

			{80, 11, 9, 59}, {80, 12, 9, 59}, {80, 13, 9, 59}, {80, 14, 9, 59},
			{80, 15, 19, 59}, {80, 16, 19, 59}, {80, 17, 19, 59}, {80, 18, 19, 59},
			{80, 19, 29, 59}, {80, 20, 29, 59}, {80, 21, 29, 59}, {80, 22, 29, 59},
			{80, 23, 39, 59}, {80, 24, 39, 59}, {80, 25, 39, 59}, {80, 26, 39, 59},
			{80, 23, 39, 59}, {80, 24, 39, 59}, {80, 25, 39, 59}, {80, 26, 39, 59},
			{80, 27, 39, 59}, {80, 28, 39, 59}, {80, 29, 39, 59}, {80, 30, 39, 59},
			{80, 31, 39, 59}, {80, 32, 39, 59}, {80, 33, 39, 59}, {80, 34, 39, 59},
			{80, 35, 39, 59}, {80, 36, 39, 59}, {80, 37, 45, 59}, {80, 38, 39, 59},
			{80, 39, 39, 59}, {80, 40, 39, 59}, {80, 41, 42, 59}, {80, 42, 39, 56},
			{80, 43, 39, 59}, {80, 44, 39, 59}, {80, 45, 39, 59}, {80, 46, 39, 50},

			{81, 11, 9, 59}, {81, 12, 9, 59}, {81, 13, 9, 59}, {81, 14, 9, 59},
			{81, 15, 19, 59}, {81, 16, 19, 59}, {81, 17, 19, 59}, {81, 18, 19, 59},
			{81, 19, 29, 59}, {81, 20, 29, 59}, {81, 21, 29, 59}, {81, 22, 29, 59},
			{81, 23, 39, 59}, {81, 24, 39, 59}, {81, 25, 39, 59}, {81, 26, 39, 59},
			{81, 23, 39, 59}, {81, 24, 39, 59}, {81, 25, 39, 59}, {81, 26, 39, 59},
			{81, 27, 39, 59}, {81, 28, 39, 59}, {81, 29, 39, 59}, {81, 30, 39, 59},
			{81, 31, 39, 59}, {81, 32, 39, 59}, {81, 33, 39, 59}, {81, 34, 39, 59},
			{81, 35, 39, 59}, {81, 36, 39, 59}, {81, 37, 45, 59}, {81, 38, 39, 59},
			{81, 39, 39, 59}, {81, 40, 39, 59}, {81, 41, 42, 59}, {81, 42, 39, 56},
			{81, 43, 39, 59}, {81, 44, 39, 59}, {81, 45, 39, 59}, {81, 46, 39, 50},

			{82, 11, 9, 59}, {82, 12, 9, 59}, {82, 13, 9, 59}, {82, 14, 9, 59},
			{82, 15, 19, 59}, {82, 16, 19, 59}, {82, 17, 19, 59}, {82, 18, 19, 59},
			{82, 19, 29, 59}, {82, 20, 29, 59}, {82, 21, 29, 59}, {82, 22, 29, 59},
			{82, 23, 39, 59}, {82, 24, 39, 59}, {82, 25, 39, 59}, {82, 26, 39, 59},
			{82, 23, 39, 59}, {82, 24, 39, 59}, {82, 25, 39, 59}, {82, 26, 39, 59},
			{82, 27, 39, 59}, {82, 28, 39, 59}, {82, 29, 39, 59}, {82, 30, 39, 59},
			{82, 31, 39, 59}, {82, 32, 39, 59}, {82, 33, 39, 59}, {82, 34, 39, 59},
			{82, 35, 39, 59}, {82, 36, 39, 59}, {82, 37, 45, 59}, {82, 38, 39, 59},
			{82, 39, 39, 59}, {82, 40, 39, 59}, {82, 41, 42, 59}, {82, 42, 39, 56},
			{82, 43, 39, 59}, {82, 44, 39, 59}, {82, 45, 39, 59}, {82, 46, 39, 50},
		},
	}
	bodies = []testBody{
		body1, body2, body3, body4, bodysplit, body6, body7,
	}
)
