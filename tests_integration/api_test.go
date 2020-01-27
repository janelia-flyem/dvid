package tests_integration

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	// Declare the data types the DVID server should support
	"github.com/janelia-flyem/dvid/datatype/annotation"
	_ "github.com/janelia-flyem/dvid/datatype/keyvalue"
	_ "github.com/janelia-flyem/dvid/datatype/labelarray"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/datatype/labelvol"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
)

type labelVol struct {
	size      dvid.Point3d
	blockSize dvid.Point3d
	offset    dvid.Point3d
}

// Each voxel in volume has sequential labels in X, Y, then Z order.
// volSize = size of volume in blocks
// blockSize = size of a block in voxels
func (vol labelVol) postLabelVolume(t *testing.T, labelsName string, uuid dvid.UUID) {
	server.CreateTestInstance(t, uuid, "labelblk", labelsName, dvid.Config{})

	offset := vol.offset

	nx := vol.size[0] * vol.blockSize[0]
	ny := vol.size[1] * vol.blockSize[1]
	nz := vol.size[2] * vol.blockSize[2]

	buf := make([]byte, nx*ny*nz*8)
	var label uint64
	var x, y, z, v int32
	for z = 0; z < nz; z++ {
		for y = 0; y < ny; y++ {
			for x = 0; x < nx; x++ {
				label++
				binary.LittleEndian.PutUint64(buf[v:v+8], label)
				v += 8
			}
		}
	}
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, labelsName, nx, ny, nz, offset[0], offset[1], offset[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(buf))
}

// the label in the test volume should just be the voxel index + 1 when iterating in ZYX order.
// The passed (x,y,z) should be world coordinates, not relative to the volume offset.
func (vol labelVol) label(x, y, z int32) uint64 {
	if x < vol.offset[0] || x >= vol.offset[0]+vol.size[0]*vol.blockSize[0] {
		return 0
	}
	if y < vol.offset[1] || y >= vol.offset[1]+vol.size[1]*vol.blockSize[1] {
		return 0
	}
	if z < vol.offset[2] || z >= vol.offset[2]+vol.size[2]*vol.blockSize[2] {
		return 0
	}
	x -= vol.offset[0]
	y -= vol.offset[1]
	z -= vol.offset[2]
	nx := vol.size[0] * vol.blockSize[0]
	nxy := nx * vol.size[1] * vol.blockSize[1]
	return uint64(z*nxy) + uint64(y*nx) + uint64(x+1)
}

type sliceTester struct {
	orient string
	width  int32
	height int32
	offset dvid.Point3d // offset of slice
}

func (s sliceTester) apiStr(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/%s/%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, name, s.orient, s.width, s.height, s.offset[0], s.offset[1], s.offset[2])
}

// make sure the given labels match what would be expected from the test volume.
func (s sliceTester) testLabel(t *testing.T, vol labelVol, img *dvid.Image) {
	data := img.Data()
	var x, y, z int32
	i := 0
	switch s.orient {
	case "xy":
		for y = 0; y < s.height; y++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := y + s.offset[1]
				vz := s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "xz":
		for z = 0; z < s.height; z++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "yz":
		for z = 0; z < s.height; z++ {
			for y = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := s.offset[0]
				vy := y * s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	default:
		t.Fatalf("Unknown slice orientation %q\n", s.orient)
	}
}

// Note: Sync tests between labelblk, labelvol, and annotations are handled in those packages.

func TestCommitAndBranch(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	apiStr := fmt.Sprintf("%srepos", server.WebAPIPath)
	r := server.TestHTTP(t, "POST", apiStr, nil)
	var jsonResp map[string]interface{}

	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repo creation response: %s\n", string(r))
	}
	v, ok := jsonResp["root"]
	if !ok {
		t.Fatalf("No 'root' metadata returned: %s\n", string(r))
	}
	uuidStr, ok := v.(string)
	if !ok {
		t.Fatalf("Couldn't cast returned 'root' data (%v) into string.\n", v)
	}
	uuid := dvid.UUID(uuidStr)

	// Shouldn't be able to create branch on open node.
	versionReq := fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", versionReq, nil)

	branchReq := fmt.Sprintf("%snode/%s/branch", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", branchReq, bytes.NewReader([]byte(`{"branch": "mybranch"}`)))

	apiStr = fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid)

	// Add a keyvalue and ROI instance.
	server.CreateTestInstance(t, uuid, "keyvalue", "mykv", dvid.Config{})
	server.CreateTestInstance(t, uuid, "roi", "myroi", dvid.Config{})

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Make sure committed nodes can only be read.
	// We shouldn't be able to write to keyvalue.
	keyReq := fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))

	// unless we have an admin token
	server.SetAdminToken("my-secret-token")
	keyReq += "?admintoken=my-secret-token"
	server.TestHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))

	// Should be able to still POST to ROI since this particular type of POST is non-mutating.
	apiStr = fmt.Sprintf("%snode/%s/myroi/ptquery", server.WebAPIPath, uuid)
	queryJSON := "[[10, 10, 10], [20, 20, 20], [30, 30, 30], [40, 40, 40], [50, 50, 50]]"
	server.TestHTTP(t, "POST", apiStr, bytes.NewReader([]byte(queryJSON))) // we have no ROI so just testing HTTP.

	// Should be able to create version and branch now that we've committed parent.
	respData := server.TestHTTP(t, "POST", versionReq, nil)
	resp := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}

	respData = server.TestHTTP(t, "POST", branchReq, bytes.NewReader([]byte(`{"branch": "mybranch"}`)))
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}

	// creating a branch with the same name should fail
	server.TestBadHTTP(t, "POST", branchReq, bytes.NewReader([]byte(`{"branch": "mybranch"}`)))

	// We should be able to write to that keyvalue now in the child.
	keyReq = fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, resp.Child)
	server.TestHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))

	// We should also be able to write to the repo-wide log.
	logReq := fmt.Sprintf("%srepo/%s/log", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", logReq, bytes.NewBufferString(`{"log": ["a log mesage"]}`))
}

func TestReloadMetadata(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Add data instances
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "keyvalue", "foo", config)
	server.CreateTestInstance(t, uuid, "labelblk", "labeldata", config)
	server.CreateTestInstance(t, uuid, "labelmap", "segmentation", config)
	server.CreateTestInstance(t, uuid, "roi", "someroi", config)

	datastore.CloseReopenTest()

	// Make sure repo UUID still there
	jsonStr, err := datastore.MarshalJSON()
	if err != nil {
		t.Fatalf("can't get repos JSON: %v\n", err)
	}
	var jsonResp map[string](map[string]interface{})

	if err := json.Unmarshal(jsonStr, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repos info response: %s\n", jsonStr)
	}
	if len(jsonResp) != 1 {
		t.Errorf("reloaded repos had more than one repo: %v\n", jsonResp)
	}
	for k := range jsonResp {
		if dvid.UUID(k) != uuid {
			t.Fatalf("Expected uuid %s, got %s.  Full JSON:\n%v\n", uuid, k, jsonResp)
		}
	}

	// Make sure the data instances are still there.
	_, err = datastore.GetDataByUUIDName(uuid, "foo")
	if err != nil {
		t.Errorf("Couldn't get keyvalue data instance after reload\n")
	}
	_, err = datastore.GetDataByUUIDName(uuid, "labeldata")
	if err != nil {
		t.Errorf("Couldn't get labelblk data instance after reload\n")
	}
	_, err = datastore.GetDataByUUIDName(uuid, "segmentation")
	if err != nil {
		t.Errorf("Couldn't get labelmap data instance after reload\n")
	}
	_, err = datastore.GetDataByUUIDName(uuid, "someroi")
	if err != nil {
		t.Errorf("Couldn't get roi data instance after reload\n")
	}
}

func TestNewInstance(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()

	payload := bytes.NewBufferString(`{"typename": "keyvalue", "dataname": "testkv", "compression": "none"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	payload = bytes.NewBufferString(`{"typename": "keyvalue", "dataname": "testkv2", "compression": "none"}`)
	apiStr = fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	payload = bytes.NewBufferString(`{"typename": "annotation", "dataname": "testannot", "compression": "none"}`)
	apiStr = fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	payload = bytes.NewBufferString(`{"typename": "labelarray", "dataname": "testlabelarray"}`)
	apiStr = fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)
}

func TestInstanceTags(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()

	payload := bytes.NewBufferString(`{"typename": "keyvalue", "dataname": "testkv", "compression": "none", "tags": "type=meshes,stuff=something-something"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	getURL := fmt.Sprintf("%snode/%s/testkv/info", server.WebAPIPath, uuid)
	got := server.TestHTTP(t, "GET", getURL, nil)
	var jsonResp struct {
		Base struct {
			Tags map[string]string
		}
	}
	if err := json.Unmarshal(got, &jsonResp); err != nil {
		t.Fatalf("couldn't unmarshal response: %s\n", string(got))
	}
	if len(jsonResp.Base.Tags) == 0 || jsonResp.Base.Tags["type"] != "meshes" || jsonResp.Base.Tags["stuff"] != "something-something" {
		t.Errorf("Got bad response: %s\n", string(got))
		t.Errorf("Parsed jsonResp: %v\n", jsonResp.Base)
	}

	payload = bytes.NewBufferString(`{"foo": "something about foo", "bar": "something about bar"}`)
	tagURL := fmt.Sprintf("%snode/%s/testkv/tags", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", tagURL, payload)

	got = server.TestHTTP(t, "GET", tagURL, nil)
	var jsonResp2 struct {
		Tags map[string]string
	}
	if err := json.Unmarshal(got, &jsonResp2); err != nil {
		t.Fatalf("couldn't unmarshal response: %s\n", string(got))
	}
	if len(jsonResp2.Tags) != 4 || jsonResp2.Tags["foo"] != "something about foo" || jsonResp2.Tags["bar"] != "something about bar" {
		t.Fatalf("Got bad response: %s\n", string(got))
	}

	tagURL += "?replace=true"
	payload = bytes.NewBufferString(`{}`)
	server.TestHTTP(t, "POST", tagURL, payload)

	jsonResp2.Tags = nil
	got = server.TestHTTP(t, "GET", tagURL, nil)
	if err := json.Unmarshal(got, &jsonResp2); err != nil {
		t.Fatalf("couldn't unmarshal response: %s\n", string(got))
	}
	if len(jsonResp2.Tags) != 0 {
		t.Fatalf("Got bad response: %v\n", jsonResp2)
	}
}

func TestSyncs(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()

	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)
	server.CreateTestInstance(t, uuid, "annotation", "synapses", config)

	server.CreateTestSync(t, uuid, "synapses", "labels,bodies")

	labels, err := labelblk.GetByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatalf("Can't obtain data instance via GetByUUIDName: %v\n", err)
	}
	bodies, err := labelvol.GetByUUIDName(uuid, "bodies")
	if err != nil {
		t.Fatalf("Can't obtain data instance via GetByUUIDName: %v\n", err)
	}
	synapses, err := annotation.GetByUUIDName(uuid, "synapses")
	if err != nil {
		t.Fatalf("Couldn't get synapses data instance: %v\n", err)
	}

	syncs := synapses.SyncedData()
	if len(syncs) != 2 {
		t.Errorf("Expected 2 syncs, got %d syncs instead.\n", len(syncs))
	}
	_, found := syncs[labels.DataUUID()]
	if !found {
		t.Errorf("Expected labels UUID (%s) got: %v\n", labels.DataUUID(), syncs)
	}
	_, found = syncs[bodies.DataUUID()]
	if !found {
		t.Errorf("Expected bodies UUID (%s) got: %v\n", bodies.DataUUID(), syncs)
	}

	server.CreateTestInstance(t, uuid, "labelvol", "bodies2", config)
	bodies2, err := labelvol.GetByUUIDName(uuid, "bodies2")
	if err != nil {
		t.Fatalf("Can't obtain data instance via GetByUUIDName: %v\n", err)
	}
	server.CreateTestSync(t, uuid, "synapses", "bodies2")

	syncs = synapses.SyncedData()
	if len(syncs) != 3 {
		t.Errorf("Expected 3 syncs, got %d syncs instead.\n", len(syncs))
	}
	_, found = syncs[labels.DataUUID()]
	if !found {
		t.Errorf("Expected labels UUID (%s) got: %v\n", labels.DataUUID(), syncs)
	}
	_, found = syncs[bodies.DataUUID()]
	if !found {
		t.Errorf("Expected bodies UUID (%s) got: %v\n", bodies.DataUUID(), syncs)
	}
	_, found = syncs[bodies2.DataUUID()]
	if !found {
		t.Errorf("Expected bodies2 UUID (%s) got: %v\n", bodies2.DataUUID(), syncs)
	}

	server.CreateTestInstance(t, uuid, "labelvol", "bodies3", config)
	server.CreateTestReplaceSync(t, uuid, "synapses", "bodies3")

	syncs = synapses.SyncedData()
	if len(syncs) != 1 {
		t.Errorf("Expected 1 sync, got %d syncs instead.\n", len(syncs))
	}
	bodies3, err := labelvol.GetByUUIDName(uuid, "bodies3")
	if err != nil {
		t.Fatalf("Can't obtain data instance via GetByUUIDName: %v\n", err)
	}
	_, found = syncs[bodies3.DataUUID()]
	if !found {
		t.Errorf("Expected bodies3 UUID (%s) got: %v\n", bodies3.DataUUID(), syncs)
	}

	server.CreateTestReplaceSync(t, uuid, "synapses", "")
	syncs = synapses.SyncedData()
	if len(syncs) != 0 {
		t.Errorf("Expected 0 sync, got instead %v\n", syncs)
	}
}

func TestBlobStore(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelarray", "labels", config)

	// Load up a variety of random data.
	postURL := fmt.Sprintf("%snode/%s/labels/blobstore", server.WebAPIPath, uuid)
	n := 20
	testData := make([][]byte, n)
	ref := make([]string, n)
	for i := 0; i < n; i++ {
		testData[i] = make([]byte, 10+i*10)
		rand.Read(testData[i])

		payload := bytes.NewBuffer(testData[i])
		respData := server.TestHTTP(t, "POST", postURL, payload)
		resp := struct {
			Reference string `json:"reference"`
		}{}
		if err := json.Unmarshal(respData, &resp); err != nil {
			t.Fatalf("got bad response for POST data %d: %s\n", i, respData)
		}
		if len(resp.Reference) == 0 {
			t.Fatalf("couldn't decipher reference for POST data %d: %s\n", i, respData)
		}
		ref[i] = resp.Reference
	}

	// Retrieve and check data.
	for i := 0; i < n; i++ {
		getURL := fmt.Sprintf("%snode/%s/labels/blobstore/%s", server.WebAPIPath, uuid, ref[i])
		got := server.TestHTTP(t, "GET", getURL, nil)
		if !reflect.DeepEqual(testData[i], got) {
			t.Errorf("Expected %d bytes from data %d, ref %s: got %d bytes that didn't match\n", len(testData[i]), i, ref[i], len(got))
		}
	}
}
