package labelmeta

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

var (
	lmetatype datastore.TypeService
	testMu  sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if lmetatype == nil {
		var err error
		lmetatype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get synapse type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func TestSynapseRepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, lmetatype, "labelmetas", config)
	if err != nil {
		t.Errorf("Unable to create keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast data service into synapse.Data\n")
	}
	oldData := *data

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, data); err != nil {
		t.Fatalf("Unable to save repo during synapse persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUID(uuid, "labelmetas")
	if err != nil {
		t.Fatalf("Can't get synapse instance from reloaded test db: %v\n", err)
	}
	data2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not synapse.Data\n")
	}
	if !oldData.Equals(data2) {
		t.Errorf("Expected %v, got %v\n", oldData, *data2)
	}
}

var testData = Labelmetas{
        {
		Pos:    dvid.Point3d{33,30,31},
                Label:  14372907,
                Status: NotExamined,
                Tags:   []Tag{"Review","PAM"},
		Prop:   map[string]string{
                        "Name":"PAM-sc1",
                        "Notes": "fubar"
                        }
        },
        {
		Pos:    dvid.Point3d{15,27,35},
                Label:  15138572,
                Status: Finalized,
                Tags:   []Tag{"Review","PAM"},
		Prop:   map[string]string{
                        "Name":"PAM-12",
                        "Notes": "Wassup"
                        }
        },
	{
		Pos:    dvid.Point3d{102,472,99},
                Label:  4567891,
                Status: HardToTrace,
                Tags:   []Tag{"Review","PPL1"},
		Prop:   map[string]string{
                        "Name":"PPL1-zul",
                        "Note": "Goes out of ROI"
                        }
        },
        {
		Pos:    dvid.Point3d{515,327,135},
                Label:  5758397,
                Status: Orphan,
                Tags:   []Tag{"Review","PPL1"},
		Prop:   map[string]string{
                        "Name":"PPL1-17",
                        "Note": "No PSDs"
                        }
        }
}

var expectedLabel1 = Labelmetas{
    {
                Pos:    dvid.Point3d{33,30,31},
                Label:  14372907,
                Status: NotExamined,
                Tags:   []Tag{"Review","PAM"},
                Prop:   map[string]string{
		            "Name":"PAM-sc1",
                            "Notes": "fubar"
			}
     }
}
# Here
var expectedLabel2 = Labelmetas{
        {
	        Pos:    dvid.Point3d{15,27,35},
                Label:  15138572,
			Status: Finalized,
                Tags:   []Tag{"Review","PAM"},
                Prop:   map[string]string{
                        "Name":"PAM-12",
                        "Notes": "Wassup"
                        }
        }
}

var expectedLabel2a = Labelmetas{

}

var expectedLabel2b = Labelmetas{

}

var expectedLabel2c = Labelmetas{

}

var expectedLabel7 = Labelmetas{

}

var expectedLabel3 = Labelmetas{

}

var expectedLabel3a = Labelmetas{

}

var expectedLabel4 = Labelmetas{

}

var expected3 = Labelmetas{

}

var afterDelete = Labelmetas{
        {
                Pos:    dvid.Point3d{33,30,31},
                Label:  14372907,
                Status: NotExamined,
                Tags:   []Tag{"Review","PAM"},
                Prop:   map[string]string{
                        "Name":"PAM-sc1",
			"Notes": "fubar"
                        }
        },
        {
                Pos:    dvid.Point3d{15,27,35},
                Label:  15138572,
			Status: Finalized,
                Tags:   []Tag{"Review","PAM"},
                Prop:   map[string]string{
                        "Name":"PAM-12",
                        "Notes": "Wassup"
                        }
        },
        {
                Pos:    dvid.Point3d{102,472,99},
                Label:  4567891,
                Status: HardToTrace,
                Tags:   []Tag{"Review","PPL1"},
                Prop:   map[string]string{
                        "Name":"PPL1-zul",
                        "Note": "Goes out of ROI"
                        }
        }
}

func getTag(tag Tag, lmetas Labelmetas) Labelmetas {
	var result Labelmetas
	for _, lmeta := range lmetas {
		for _, ltag := range lmeta.Tags {
			if ltag == tag {
				result = append(result, lmeta)
				break
			}
		}
	}
	return result
}

func testResponse(t *testing.T, expected Labelmetas, template string, args ...interface{}) {
	url := fmt.Sprintf(template, args...)
	returnValue := server.TestHTTP(t, "GET", url, nil)
	got := Labelmetas{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected.Normalize(), got.Normalize()) {
		t.Errorf("Expected:\n%v\nGot:\n%v\n", expected.Normalize(), got.Normalize())
	}
}

func TestRequests(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, lmetatype, "mylabelmetas", config)
	if err != nil {
		t.Fatalf("Error creating new data instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not synapse.Data\n")
	}

	// PUT first batch of labelmetas
	testJSON, err := json.Marshal(testData)
	if err != nil {
		t.Fatal(err)
	}
	url1 := fmt.Sprintf("%snode/%s/%s/labelmeta", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", url1, strings.NewReader(string(testJSON)))

	// GET labelmetas back within superset bounding box and make sure all data is there.
	testResponse(t, testData, "%snode/%s/%s/labelsall", server.WebAPIPath, uuid, data.DataName())

	// Test GET
	testResponse(t, expectedLabel1, "%snode/%s/%s/label/14372907", server.WebAPIPath, uuid, data.DataName())

	// Test Tag 1
	tag := Tag("PAM")
	pam_bodies := getTag(tag, testData)
	testResponse(t, pam_bodies, "%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag)

	// Test Tag 2
	tag2 := Tag("PPL1")
	PPL1 := getTag(tag2, testData)
	testResponse(t, PPL1, "%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag2)

	// Test move
	//url5 := fmt.Sprintf("%snode/%s/%s/move/127_63_99/127_64_100", server.WebAPIPath, uuid, data.DataName())
	//server.TestHTTP(t, "POST", url5, nil)
	//testResponse(t, afterMove, "%snode/%s/%s/elements/1000_1000_1000/0_0_0", server.WebAPIPath, uuid, data.DataName())
	// --- check tag
	//synapse2 = getTag(tag, afterMove)
	//testResponse(t, synapse2, "%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag)

	// Test delete
	url6 := fmt.Sprintf("%snode/%s/%s/label/5758397", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "DELETE", url6, nil)
	testResponse(t, afterDelete, "%snode/%s/%s/labelsall", server.WebAPIPath, uuid, data.DataName())

	// --- check tag
	pplremaining = getTag(tag2, afterDelete)
	testResponse(t, pplremaining, "%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag2)
}

/*
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
*/

func TestLabels(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	server.CreateTestInstance(t, uuid, "labelvol", "bodies", config)

	// Establish syncs
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Populate the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	if err := labelblk.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Add labelmetas syncing with "labels" instance.
	server.CreateTestInstance(t, uuid, "labelmeta", "mybodyannotations", config)
	server.CreateTestSync(t, uuid, "mybodyannotations", "labels,bodies")

	// PUT first batch of labelmetas
	testJSON, err := json.Marshal(testData)
	if err != nil {
		t.Fatal(err)
	}
	url1 := fmt.Sprintf("%snode/%s/mylabelmetas/elements", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url1, strings.NewReader(string(testJSON)))

	// Test if labels were properly denormalized.  For the POST we have synchronized label denormalization.
	// If this were to become asynchronous, we'd want to block on updating like the labelblk<->labelvol sync.

	testResponse(t, expectedLabel1, "%snode/%s/mylabelmetas/label/1", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel2, "%snode/%s/mylabelmetas/label/2", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel3, "%snode/%s/mylabelmetas/label/3", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel4, "%snode/%s/mylabelmetas/label/4", server.WebAPIPath, uuid)

	// Make change to labelblk and make sure our label labelmetas have been adjusted (case A)
	_ = modifyLabelTestVolume(t, uuid, "labels")

	if err := BlockOnUpdating(uuid, "mylabelmetas"); err != nil {
		t.Fatalf("Error blocking on sync of labels->labelmetas: %v\n", err)
	}

	testResponse(t, expectedLabel1, "%snode/%s/mylabelmetas/label/1", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel2a, "%snode/%s/mylabelmetas/label/2", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel3a, "%snode/%s/mylabelmetas/label/3", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel4, "%snode/%s/mylabelmetas/label/4", server.WebAPIPath, uuid)

	// Make change to labelvol and make sure our label labelmetas have been adjusted (case B).
	// Merge 3 into 2.
	testMerge := mergeJSON(`[2, 3]`)
	testMerge.send(t, uuid, "bodies")

	if err := labelblk.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	testResponse(t, expectedLabel1, "%snode/%s/mylabelmetas/label/1", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel2b, "%snode/%s/mylabelmetas/label/2", server.WebAPIPath, uuid)
	testResponse(t, Labelmetas{}, "%snode/%s/mylabelmetas/label/3", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel4, "%snode/%s/mylabelmetas/label/4", server.WebAPIPath, uuid)

	// Now split label 2b off and check if labelmetas also split

	// Create the sparsevol encoding for split area
	numspans := len(bodysplit.voxelSpans)
	rles := make(dvid.RLEs, numspans, numspans)
	for i, span := range bodysplit.voxelSpans {
		start := dvid.Point3d{span[2], span[1], span[0]}
		length := span[3] - span[2] + 1
		rles[i] = dvid.NewRLE(start, length)
	}
	buf := getBytesRLE(t, rles)

	// Submit the split sparsevol
	reqStr := fmt.Sprintf("%snode/%s/%s/split/%d?splitlabel=7", server.WebAPIPath, uuid, "bodies", 2)
	r := server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal := make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}

	// Verify that the labelmetas are correct.
	if err := BlockOnUpdating(uuid, "mylabelmetas"); err != nil {
		t.Fatalf("Error blocking on sync of split->labelmetas: %v\n", err)
	}
	testResponse(t, expectedLabel2c, "%snode/%s/mylabelmetas/label/2", server.WebAPIPath, uuid)
	testResponse(t, expectedLabel7, "%snode/%s/mylabelmetas/label/7", server.WebAPIPath, uuid)

	// Try a coarse split.

	// Create the encoding for split area in block coordinates.
	rles = dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{3, 1, 3}, 1),
	}
	buf = getBytesRLE(t, rles)

	// Submit the coarse split
	reqStr = fmt.Sprintf("%snode/%s/%s/split-coarse/2?splitlabel=8", server.WebAPIPath, uuid, "bodies")
	r = server.TestHTTP(t, "POST", reqStr, buf)
	jsonVal = make(map[string]uint64)
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get new label from split.  Instead got: %v\n", jsonVal)
	}

	// Verify that the labelmetas are correct.
	if err := BlockOnUpdating(uuid, "mylabelmetas"); err != nil {
		t.Fatalf("Error blocking on sync of split->labelmetas: %v\n", err)
	}
	testResponse(t, expectedLabel2c, "%snode/%s/mylabelmetas/label/8", server.WebAPIPath, uuid)
	testResponse(t, Labelmetas{}, "%snode/%s/mylabelmetas/label/2", server.WebAPIPath, uuid)
}

// A single label block within the volume
type testBody struct {
	label        uint64
	offset, size dvid.Point3d
	blockSpans   dvid.Spans
	voxelSpans   dvid.Spans
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
func (v *testVolume) add(body testBody, label uint64) {
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

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func createLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	volume := newTestVolume(128, 128, 128)
	volume.add(body1, 1)
	volume.add(body2, 2)
	volume.add(body3, 3)
	volume.add(body4, 4)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

func modifyLabelTestVolume(t *testing.T, uuid dvid.UUID, name string) *testVolume {
	volume := newTestVolume(128, 128, 128)
	volume.add(body1, 1)
	volume.add(body2a, 2)
	volume.add(body3a, 3)
	volume.add(body4, 4)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

type mergeJSON string

func (mjson mergeJSON) send(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer([]byte(mjson)))
}

var (
	bodies = []testBody{
		{
			label:  1,
			offset: dvid.Point3d{10, 10, 30},
			size:   dvid.Point3d{20, 20, 10},
			blockSpans: []dvid.Span{
				{1, 0, 0, 0},
			},
			voxelSpans: []dvid.Span{
				{35, 27, 11, 28}, {36, 28, 13, 25},
			},
		}, {
			label:  2,
			offset: dvid.Point3d{10, 25, 35},
			size:   dvid.Point3d{30, 10, 10},
			blockSpans: []dvid.Span{
				{1, 0, 0, 0},
			},
			voxelSpans: []dvid.Span{
				{40, 30, 12, 20},
			},
		}, {
			label:  3,
			offset: dvid.Point3d{10, 20, 36},
			size:   dvid.Point3d{120, 45, 65},
			blockSpans: []dvid.Span{
				{1, 0, 0, 0},
				{3, 2, 4, 4},
			},
			voxelSpans: []dvid.Span{
				{37, 25, 13, 15}, {99, 63, 126, 127},
			},
		}, {
			label:  4,
			offset: dvid.Point3d{75, 40, 75},
			size:   dvid.Point3d{20, 10, 10},
			blockSpans: []dvid.Span{
				{2, 1, 2, 2},
			},
			voxelSpans: []dvid.Span{
				{80, 47, 87, 89},
			},
		}, { // Modification to original label 2 body where we switch a span that was in label 3
			label:  2,
			offset: dvid.Point3d{10, 24, 35},
			size:   dvid.Point3d{30, 10, 10},
			blockSpans: []dvid.Span{
				{1, 0, 0, 0},
			},
			voxelSpans: []dvid.Span{
				{37, 25, 13, 15},
			},
		}, { // Modification to original label 3 body where we switch in a span that was in label 2
			label:  3,
			offset: dvid.Point3d{10, 20, 36},
			size:   dvid.Point3d{120, 45, 65},
			blockSpans: []dvid.Span{
				{1, 0, 0, 0},
				{3, 2, 4, 4},
			},
			voxelSpans: []dvid.Span{
				{40, 30, 12, 20}, {99, 63, 126, 127},
			},
		}, { // Body to split
			label:  7,
			offset: dvid.Point3d{10, 24, 36},
			size:   dvid.Point3d{12, 7, 5},
			blockSpans: []dvid.Span{
				{0, 0, 0, 0},
				{0, 0, 0, 0},
			},
			voxelSpans: []dvid.Span{
				{37, 25, 10, 15},
				{40, 30, 19, 21},
			},
		},
	}
	body1     = bodies[0]
	body2     = bodies[1]
	body3     = bodies[2]
	body4     = bodies[3]
	body2a    = bodies[4]
	body3a    = bodies[5]
	bodysplit = bodies[6]
)
