package labelsz

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/annotation"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

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
func (v *testVolume) add(label uint64, ox, oy, oz int32, sx, sy, sz int32) {
	nx := v.size.Value(0)
	ny := v.size.Value(1)
	nxy := nx * ny
	for z := oz; z < oz+sz; z++ {
		for y := oy; y < oy+sy; y++ {
			p := (z*nxy + y*nx + ox) * 8
			for x := ox; x < ox+sx; x++ {
				binary.LittleEndian.PutUint64(v.data[p:p+8], label)
				p += 8
			}
		}
	}
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func createLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	volume := newTestVolume(128, 128, 128)
	volume.add(100, 0, 0, 0, 64, 128, 128)
	volume.add(200, 64, 0, 0, 64, 128, 64)
	volume.add(300, 64, 0, 64, 64, 128, 64)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

// test ROI has offset (32, 32, 32) and size (64, 64, 64)
var testSpans = []dvid.Span{
	dvid.Span{1, 1, 1, 2}, dvid.Span{1, 2, 1, 2},
	dvid.Span{2, 1, 1, 2}, dvid.Span{2, 2, 1, 2},
}

func getROIReader() io.Reader {
	jsonBytes, err := json.Marshal(testSpans)
	if err != nil {
		log.Fatalf("Can't encode spans into JSON: %v\n", err)
	}
	return bytes.NewReader(jsonBytes)
}

type mergeJSON string

func (mjson mergeJSON) send(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(string(mjson)))
}

func getBytesRLE(t *testing.T, rles dvid.RLEs) *bytes.Buffer {
	n := len(rles)
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(n)) // Placeholder for # spans
	rleBytes, err := rles.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to serialize RLEs: %v\n", err)
	}
	buf.Write(rleBytes)
	return buf
}

func checkSequencing(t *testing.T, uuid dvid.UUID) {
	// Check if we have correct sequencing for no ROI labelsz.
	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of noroi labelsz: %v\n", err)
	}

	url := fmt.Sprintf("%snode/%s/noroi/top/3/PreSyn", server.WebAPIPath, uuid)
	data := server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16384},{"Label":200,"Size":8192},{"Label":300,"Size":8192}]` {
		t.Errorf("Got back incorrect PreSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/count/100/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":100,"PreSyn":16384}` {
		t.Errorf("Got back incorrect PreSyn noroi count for label 100:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/count/200/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":200,"PreSyn":8192}` {
		t.Errorf("Got back incorrect PreSyn noroi count for label 200:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":14415},{"Label":300,"Size":7936},{"Label":200,"Size":7440}]` {
		t.Errorf("Got back incorrect PostSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":30799},{"Label":300,"Size":16128},{"Label":200,"Size":15632}]` {
		t.Errorf("Got back incorrect AllSync noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/15633/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":30799},{"Label":300,"Size":16128}]` {
		t.Errorf("Got back incorrect AllSyn noroi threshold:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/1000/AllSyn?offset=1&n=2", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":300,"Size":16128},{"Label":200,"Size":15632}]` {
		t.Errorf("Got back incorrect AllSyn noroi threshold:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/1000/AllSyn?offset=8&n=2", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[]` {
		t.Errorf("Got back incorrect AllSyn noroi threshold:\n%v\n", string(data))
	}

	// Check if we have correct sequencing for ROI labelsz.
	// ROI constitutes the inner eight 32^3 blocks.
	// There are 16 PostSyn in each ROI dimension.
	// There are also 16 PreSyn in each ROI dimension.
	if err := datastore.BlockOnUpdating(uuid, "withroi"); err != nil {
		t.Fatalf("Error blocking on sync of withroi labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/0/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[]` {
		t.Errorf("Incorrectly handled top n=0 case, expected [] got: %v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/3/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":2048},{"Label":200,"Size":1024},{"Label":300,"Size":1024}]` {
		t.Errorf("Got back incorrect PreSyn withroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":2048},{"Label":200,"Size":1024},{"Label":300,"Size":1024}]` {
		t.Errorf("Got back incorrect PostSyn withroi ranking:\n%v\n", string(data))
	}

	// Check fewer and larger N requests.
	url = fmt.Sprintf("%snode/%s/noroi/top/2/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16384},{"Label":200,"Size":8192}]` {
		t.Errorf("Got back incorrect N=2 ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/4/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16384},{"Label":200,"Size":8192},{"Label":300,"Size":8192}]` {
		t.Errorf("Got back incorrect N=4 ranking:\n%v\n", string(data))
	}

	// Test annotation move of a PostSyn from label 100->300 and also label 200->300
	url = fmt.Sprintf("%snode/%s/mysynapses/move/32_32_32/75_21_69", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, nil)

	url = fmt.Sprintf("%snode/%s/mysynapses/move/68_20_20/77_21_69", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, nil)

	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of noroi labelsz: %v\n", err)
	}
	url = fmt.Sprintf("%snode/%s/noroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":14414},{"Label":300,"Size":7938},{"Label":200,"Size":7439}]` {
		t.Errorf("Got back incorrect PostSyn noroi ranking after move from label 100->300:\n%v\n", string(data))
	}

	// First move took synapse out of ROI so there should be one less for label 100.
	if err := datastore.BlockOnUpdating(uuid, "withroi"); err != nil {
		t.Fatalf("Error blocking on sync of labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/5/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":2047},{"Label":200,"Size":1024},{"Label":300,"Size":1024}]` {
		t.Errorf("Got back incorrect post-move PostSyn withroi ranking:\n%v\n", string(data))
	}

	// Test annotation deletion of moved PostSyn from label 300
	url = fmt.Sprintf("%snode/%s/mysynapses/element/75_21_69", server.WebAPIPath, uuid)
	server.TestHTTP(t, "DELETE", url, nil)

	url = fmt.Sprintf("%snode/%s/mysynapses/element/77_21_69", server.WebAPIPath, uuid)
	server.TestHTTP(t, "DELETE", url, nil)

	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of noroi labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":14414},{"Label":300,"Size":7936},{"Label":200,"Size":7439}]` {
		t.Errorf("Got back incorrect PostSyn noroi ranking after deletions from label 300:\n%v\n", string(data))
	}

	// Check sync on merge.
	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of bodies: %v\n", err)
	}
	testMerge := mergeJSON(`[200, 300]`)
	testMerge.send(t, uuid, "bodies")

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}
	time.Sleep(1 * time.Second)
	if err := datastore.BlockOnUpdating(uuid, "mysynapses"); err != nil {
		t.Fatalf("Error blocking on sync of synapses: %v\n", err)
	}
	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of labelsz: %v\n", err)
	}
	if err := datastore.BlockOnUpdating(uuid, "withroi"); err != nil {
		t.Fatalf("Error blocking on sync of labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/5/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":2048},{"Label":100,"Size":2047}]` {
		t.Errorf("Got back incorrect post-merge PostSyn withroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/count/100/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":100,"PostSyn":2047}` {
		t.Errorf("Got back incorrect post-merge PostSyn withroi count of label 100:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16384},{"Label":200,"Size":16384}]` {
		t.Errorf("Got back incorrect post-merge PreSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":15375},{"Label":100,"Size":14414}]` {
		t.Errorf("Got back incorrect post-merge PostSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":31759},{"Label":100,"Size":30798}]` {
		t.Errorf("Got back incorrect post-merge AllSyn noroi ranking:\n%v\n", string(data))
	}

	// Check threshold endpoint

	url = fmt.Sprintf("%snode/%s/withroi/threshold/2048/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":2048}]` {
		t.Errorf("Got back incorrect post-merge PostSyn withroi threshold:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/16384/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16384},{"Label":200,"Size":16384}]` {
		t.Errorf("Got back incorrect post-merge PreSyn noroi threshold:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/15000/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":15375}]` {
		t.Errorf("Got back incorrect post-merge PostSyn noroi threshold:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/threshold/0/PostSyn?offset=1&n=1", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":14414}]` {
		t.Errorf("Got back incorrect post-merge PostSyn noroi threshold with offset/n:\n%v\n", string(data))
	}

	// Create the sparsevol encoding for split area with label 100 -> 150.
	// Split has offset (0, 0, 0) with size (19, 19, 19).
	// PreSyn in split = 5 x 5 x 5 = 125
	// PostSyn in split = 4 x 4 x 4 = 64
	var rles dvid.RLEs
	for z := int32(0); z < 19; z++ {
		for y := int32(0); y < 19; y++ {
			start := dvid.Point3d{0, y, z}
			rles = append(rles, dvid.NewRLE(start, 19))
		}
	}
	buf := getBytesRLE(t, rles)

	// Submit the split sparsevol
	url = fmt.Sprintf("%snode/%s/%s/split/%d?splitlabel=150", server.WebAPIPath, uuid, "bodies", 100)
	data = server.TestHTTP(t, "POST", url, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(data, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}

	// Check sync on split.
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}
	time.Sleep(1 * time.Second)
	if err := datastore.BlockOnUpdating(uuid, "mysynapses"); err != nil {
		t.Fatalf("Error blocking on sync of synapses: %v\n", err)
	}
	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":16384},{"Label":100,"Size":16259},{"Label":150,"Size":125}]` {
		t.Errorf("Got back incorrect post-split PreSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/3/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":15375},{"Label":100,"Size":14350},{"Label":150,"Size":64}]` {
		t.Errorf("Got back incorrect post-split PostSyn noroi ranking:\n%v\n", string(data))
	}

	// Create the encoding for coarse split area in block coordinates from label 200.
	// Split has offset (64, 96, 96) with size (64, 32, 32).
	// PreSyn in split = 16 x 8 x 8 = 1024
	// PostSyn in split = 16 x 8 x 8 = 1024
	rles = dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{2, 3, 3}, 2),
	}
	buf = getBytesRLE(t, rles)

	// Submit the coarse split of 200 -> 250
	url = fmt.Sprintf("%snode/%s/%s/split-coarse/200?splitlabel=250", server.WebAPIPath, uuid, "bodies")
	data = server.TestHTTP(t, "POST", url, buf)
	jsonVal = make(map[string]uint64)
	if err := json.Unmarshal(data, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}

	// Check sync on split.
	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}
	time.Sleep(1 * time.Second)
	if err := datastore.BlockOnUpdating(uuid, "mysynapses"); err != nil {
		t.Fatalf("Error blocking on sync of synapses: %v\n", err)
	}
	if err := datastore.BlockOnUpdating(uuid, "noroi"); err != nil {
		t.Fatalf("Error blocking on sync of labelsz: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/5/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":16259},{"Label":200,"Size":15360},{"Label":250,"Size":1024},{"Label":150,"Size":125}]` {
		t.Errorf("Got back incorrect post-coarsesplit PreSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/5/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":14351},{"Label":100,"Size":14350},{"Label":250,"Size":1024},{"Label":150,"Size":64}]` {
		t.Errorf("Got back incorrect post-coarsesplit PostSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/top/5/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":30609},{"Label":200,"Size":29711},{"Label":250,"Size":2048},{"Label":150,"Size":189}]` {
		t.Errorf("Got back incorrect post-coarsesplit AllSyn noroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/noroi/count/200/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":200,"AllSyn":29711}` {
		t.Errorf("Got back incorrect post-coarsesplit AllSyn noroi count of label 200:\n%v\n", string(data))
	}

	// Check the ROI-restricted labelsz instance which should only be affected by merge.
	url = fmt.Sprintf("%snode/%s/withroi/top/5/PreSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":100,"Size":2048},{"Label":200,"Size":2048}]` {
		t.Errorf("Got back incorrect post-coarsesplit PreSyn withroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/5/PostSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":2048},{"Label":100,"Size":2047}]` {
		t.Errorf("Got back incorrect post-coarsesplit PostSyn withroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/top/5/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `[{"Label":200,"Size":4096},{"Label":100,"Size":4095}]` {
		t.Errorf("Got back incorrect post-coarsesplit AllSyn withroi ranking:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/count/200/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":200,"AllSyn":4096}` {
		t.Errorf("Got back incorrect post-coarsesplit AllSyn withroi count of label 200:\n%v\n", string(data))
	}

	url = fmt.Sprintf("%snode/%s/withroi/count/100/AllSyn", server.WebAPIPath, uuid)
	data = server.TestHTTP(t, "GET", url, nil)
	if string(data) != `{"Label":100,"AllSyn":4095}` {
		t.Errorf("Got back incorrect post-coarsesplit AllSyn withroi count of label 100:\n%v\n", string(data))
	}
}

func TestLabels(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := datastore.NewTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	// Establish syncs
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Populate the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Add annotations syncing with "labels" instance.
	server.CreateTestInstance(t, uuid, "annotation", "mysynapses", config)
	server.CreateTestSync(t, uuid, "mysynapses", "labels,bodies")

	// Create a ROI that will be used for our labelsz.
	server.CreateTestInstance(t, uuid, "roi", "myroi", config)
	roiRequest := fmt.Sprintf("%snode/%s/myroi/roi", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", roiRequest, getROIReader())

	// Create labelsz instances synced to the above annotations.
	server.CreateTestInstance(t, uuid, "labelsz", "noroi", config)
	server.CreateTestSync(t, uuid, "noroi", "mysynapses")
	config.Set("ROI", fmt.Sprintf("myroi,%s", uuid))
	server.CreateTestInstance(t, uuid, "labelsz", "withroi", config)
	server.CreateTestSync(t, uuid, "withroi", "mysynapses")

	// PUT first batch of synapses.
	var synapses annotation.Elements
	var x, y, z int32
	// This should put 31x31x31 (29,791) PostSyn in volume with fewer in label 200 than 300.
	// There will be 15 along each dimension from 0 -> 63, then 16 from 64 -> 127.
	// Label 100 will have 15 x 31 x 31 = 14415
	// Label 200 will have 16 x 31 x 15 = 7440
	// Label 300 will have 16 x 31 x 16 = 7936
	for z = 4; z < 128; z += 4 {
		for y = 4; y < 128; y += 4 {
			for x = 4; x < 128; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PostSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
			}
		}
	}
	// This should put 32x32x32 (32,768) PreSyn in volume split 1/2, 1/4, 1/4
	for z = 2; z < 128; z += 4 {
		for y = 2; y < 128; y += 4 {
			for x = 2; x < 128; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PreSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
			}
		}
	}
	testJSON, err := json.Marshal(synapses)
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("%snode/%s/mysynapses/elements", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, strings.NewReader(string(testJSON)))

	checkSequencing(t, uuid)
}

func TestLabelsResync(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := datastore.NewTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	// Establish syncs
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Populate the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Add annotations syncing with "labels" instance.
	server.CreateTestInstance(t, uuid, "annotation", "mysynapses", config)
	server.CreateTestSync(t, uuid, "mysynapses", "labels,bodies")

	// Create a ROI that will be used for our labelsz.
	server.CreateTestInstance(t, uuid, "roi", "myroi", config)
	roiRequest := fmt.Sprintf("%snode/%s/myroi/roi", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", roiRequest, getROIReader())

	// PUT first batch of synapses.
	var synapses annotation.Elements
	var x, y, z int32
	// This should put 31x31x31 (29,791) PostSyn in volume with fewer in label 200 than 300.
	// There will be 15 along each dimension from 0 -> 63, then 16 from 64 -> 127.
	// Label 100 will have 15 x 31 x 31 = 14415
	// Label 200 will have 16 x 31 x 15 = 7440
	// Label 300 will have 16 x 31 x 16 = 7936
	for z = 4; z < 128; z += 4 {
		for y = 4; y < 128; y += 4 {
			for x = 4; x < 128; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PostSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
			}
		}
	}
	// This should put 32x32x32 (32,768) PreSyn in volume split 1/2, 1/4, 1/4
	for z = 2; z < 128; z += 4 {
		for y = 2; y < 128; y += 4 {
			for x = 2; x < 128; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PreSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
			}
		}
	}
	testJSON, err := json.Marshal(synapses)
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("%snode/%s/mysynapses/elements", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, strings.NewReader(string(testJSON)))

	// Create labelsz instances synced to the above annotations AFTER population so need resync.
	server.CreateTestInstance(t, uuid, "labelsz", "noroi", config)
	server.CreateTestSync(t, uuid, "noroi", "mysynapses")
	config.Set("ROI", fmt.Sprintf("myroi,%s", uuid))
	server.CreateTestInstance(t, uuid, "labelsz", "withroi", config)
	server.CreateTestSync(t, uuid, "withroi", "mysynapses")

	// Do the reload.
	url = fmt.Sprintf("%snode/%s/noroi/reload", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, nil)
	url = fmt.Sprintf("%snode/%s/withroi/reload", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, nil)

	checkSequencing(t, uuid)
}

func TestLabelmap(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := datastore.NewTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	sz := int32(128) // size of one cube side in voxels
	numVoxels := sz * sz * sz
	numLabels := 100
	data := make([]byte, numVoxels*8)
	offset := 0
	for v := 0; v < int(numVoxels); v++ {
		label := uint64(rand.Int()%numLabels) + 1
		binary.LittleEndian.PutUint64(data[offset:offset+8], label)
		offset += 8
	}
	_ = createLabelTestVolume(t, uuid, "labels")
	apiStr := fmt.Sprintf("%snode/%s/labels/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, sz, sz, sz)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(data))

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Add annotations syncing with "labels" instance.
	server.CreateTestInstance(t, uuid, "annotation", "mysynapses", config)
	server.CreateTestSync(t, uuid, "mysynapses", "labels")

	// Create a ROI that will be used for our labelsz.
	server.CreateTestInstance(t, uuid, "roi", "myroi", config)
	roiRequest := fmt.Sprintf("%snode/%s/myroi/roi", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", roiRequest, getROIReader())

	// Create labelsz instances synced to the above annotations.
	server.CreateTestInstance(t, uuid, "labelsz", "noroi", config)
	server.CreateTestSync(t, uuid, "noroi", "mysynapses")
	config.Set("ROI", fmt.Sprintf("myroi,%s", uuid))
	server.CreateTestInstance(t, uuid, "labelsz", "withroi", config)
	server.CreateTestSync(t, uuid, "withroi", "mysynapses")

	labels := []uint64{10, 20, 30, 40, 50}
	postsynCounts := make(map[uint64]int, numLabels)
	presynCounts := make(map[uint64]int, numLabels)
	var synapses annotation.Elements
	var x, y, z int32
	// This should put 31x31x31 (29791) PostSyn
	for z = 4; z < sz; z += 4 {
		for y = 4; y < sz; y += 4 {
			for x = 4; x < sz; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PostSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
				offset := (z*sz*sz + y*sz + x) * 8
				label := binary.LittleEndian.Uint64(data[offset : offset+8])
				postsynCounts[label] = postsynCounts[label] + 1
			}
		}
	}
	// This should put 32x32x32 (32768) PreSyn in volume
	for z = 2; z < sz; z += 4 {
		for y = 2; y < sz; y += 4 {
			for x = 2; x < sz; x += 4 {
				e := annotation.Element{
					annotation.ElementNR{
						Pos:  dvid.Point3d{x, y, z},
						Kind: annotation.PreSyn,
					},
					[]annotation.Relationship{},
				}
				synapses = append(synapses, e)
				offset := (z*sz*sz + y*sz + x) * 8
				label := binary.LittleEndian.Uint64(data[offset : offset+8])
				presynCounts[label] = presynCounts[label] + 1
			}
		}
	}
	testJSON, err := json.Marshal(synapses)
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("%snode/%s/mysynapses/elements", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, strings.NewReader(string(testJSON)))

	postStr := "["
	for i, label := range labels {
		postStr += strconv.Itoa(int(label))
		if i != len(labels)-1 {
			postStr += ","
		}
	}
	postStr += "]"
	url = fmt.Sprintf("%snode/%s/noroi/counts/PreSyn", server.WebAPIPath, uuid)
	retData := server.TestHTTP(t, "GET", url, bytes.NewBuffer([]byte(postStr)))
	var retVal []struct {
		Label  uint64
		PreSyn int
	}
	if err := json.Unmarshal(retData, &retVal); err != nil {
		t.Errorf("Unable to decode return value: %v\n", retData)
	}
	for _, val := range retVal {
		expected, found := presynCounts[val.Label]
		if !found {
			t.Fatalf("Bad label %d returned: %s\n", val.Label, string(retData))
		}
		if expected != val.PreSyn {
			t.Fatalf("Expected label %d presyn to have %d, got %d\n", val.Label, expected, val.PreSyn)
		}
	}
	url = fmt.Sprintf("%snode/%s/noroi/counts/PostSyn", server.WebAPIPath, uuid)
	retData = server.TestHTTP(t, "GET", url, bytes.NewBuffer([]byte(postStr)))
	var retVal2 []struct {
		Label   uint64
		PostSyn int
	}
	if err := json.Unmarshal(retData, &retVal2); err != nil {
		t.Errorf("Unable to decode return value: %v\n", retData)
	}
	for _, val := range retVal2 {
		expected, found := postsynCounts[val.Label]
		if !found {
			t.Fatalf("Bad label %d returned: %s\n", val.Label, string(retData))
		}
		if expected != val.PostSyn {
			t.Fatalf("Expected label %d postsyn to have %d, got %d\n", val.Label, expected, val.PostSyn)
		}
	}
	url = fmt.Sprintf("%snode/%s/noroi/counts/AllSyn", server.WebAPIPath, uuid)
	retData = server.TestHTTP(t, "GET", url, bytes.NewBuffer([]byte(postStr)))
	var retVal3 []struct {
		Label  uint64
		AllSyn int
	}
	if err := json.Unmarshal(retData, &retVal3); err != nil {
		t.Errorf("Unable to decode return value: %v\n", retData)
	}
	for _, val := range retVal3 {
		expectedPre, found := presynCounts[val.Label]
		if !found {
			t.Fatalf("Bad label %d returned: %s\n", val.Label, string(retData))
		}
		expectedPost, found := postsynCounts[val.Label]
		if !found {
			t.Fatalf("Bad label %d returned: %s\n", val.Label, string(retData))
		}
		if expectedPre+expectedPost != val.AllSyn {
			t.Fatalf("Expected label %d allsyn to have %d+%d, got %d\n", val.Label, expectedPre, expectedPost, val.AllSyn)
		}
	}

	// merge 10+20 into 30 and recheck
	url = fmt.Sprintf("%snode/%s/labels/merge", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, bytes.NewBufferString("[30,10,20]"))

	if err := datastore.BlockOnUpdating(uuid, "mysynapses"); err != nil {
		t.Fatalf("Error blocking on sync of annotations: %v\n", err)
	}

	url = fmt.Sprintf("%snode/%s/noroi/count/30/PreSyn", server.WebAPIPath, uuid)
	retData = server.TestHTTP(t, "GET", url, nil)
	presynMerged := presynCounts[10] + presynCounts[20] + presynCounts[30]
	if string(retData) != fmt.Sprintf(`{"Label":30,"PreSyn":%d}`, presynMerged) {
		t.Errorf("Got back incorrect post-merge PreSyn noroi count of label 30: %s\nlabel 10+20+30 = %d+%d+%d = %d\n",
			string(retData), presynCounts[10], presynCounts[20], presynCounts[30], presynMerged)
	}

	url = fmt.Sprintf("%snode/%s/noroi/count/10/PreSyn", server.WebAPIPath, uuid)
	retData = server.TestHTTP(t, "GET", url, nil)
	if string(retData) != `{"Label":10,"PreSyn":0}` {
		t.Errorf("Got back incorrect post-merge PreSyn noroi count of label 10: %s\n", string(retData))
	}
	url = fmt.Sprintf("%snode/%s/noroi/count/20/PreSyn", server.WebAPIPath, uuid)
	retData = server.TestHTTP(t, "GET", url, nil)
	if string(retData) != `{"Label":20,"PreSyn":0}` {
		t.Errorf("Got back incorrect post-merge PreSyn noroi count of label 20: %s\n", string(retData))
	}
}
