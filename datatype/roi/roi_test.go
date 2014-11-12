package roi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	roitype datastore.TypeService
	testMu  sync.Mutex
)

var testSpans = []tuple{
	tuple{100, 101, 200, 210}, tuple{100, 102, 200, 210}, tuple{100, 103, 201, 212},
	tuple{101, 101, 201, 213}, tuple{101, 102, 202, 215}, tuple{101, 103, 202, 216},
	tuple{102, 101, 200, 210}, tuple{102, 103, 201, 216}, tuple{102, 104, 203, 217},
	tuple{103, 101, 200, 210}, tuple{103, 103, 200, 210}, tuple{103, 105, 201, 212},
}

func getSpansJSON(spans []tuple) io.Reader {
	jsonBytes, err := json.Marshal(spans)
	if err != nil {
		log.Fatalf("Can't encode spans into JSON: %s\n", err.Error())
	}
	return bytes.NewReader(jsonBytes)
}

func putSpansJSON(data []byte) ([]tuple, error) {
	var spans []tuple
	if err := json.Unmarshal(data, &spans); err != nil {
		return nil, err
	}
	return spans, nil
}

var testPoints = []dvid.Point3d{
	dvid.Point3d{6400, 3232, 3167}, // false
	dvid.Point3d{6400, 3232, 3200}, // true
	dvid.Point3d{6719, 3232, 3200}, // true
	dvid.Point3d{6720, 3232, 3200}, // false
	dvid.Point3d{6720, 4100, 3263}, // false
}

var expectedInclusions = []bool{
	false,
	true,
	true,
	false,
	false,
}

func getPointsJSON(pts []dvid.Point3d) io.Reader {
	jsonBytes, err := json.Marshal(pts)
	if err != nil {
		log.Fatalf("Can't encode points into JSON: %s\n", err.Error())
	}
	return bytes.NewReader(jsonBytes)
}

func putInclusionJSON(data []byte) ([]bool, error) {
	var inclusions []bool
	if err := json.Unmarshal(data, &inclusions); err != nil {
		return nil, err
	}
	return inclusions, nil
}

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if roitype == nil {
		var err error
		roitype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get ROI type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

func TestROIRequests(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create the ROI dataservice.
	repo, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(roitype, "roi", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not roi.Data\n")
	}

	// PUT an ROI
	roiRequest := fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", roiRequest, getSpansJSON(testSpans))

	// Get back the ROI
	returnedData := server.TestHTTP(t, "GET", roiRequest, nil)
	spans, err := putSpansJSON(returnedData)
	if err != nil {
		t.Errorf("Error on getting back JSON from roi GET: %s\n", err.Error())
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(spans, testSpans) {
		t.Errorf("Bad PUT/GET ROI roundtrip\nOriginal:\n%s\nReturned:\n%s\n", testSpans, spans)
	}

	// Test the ptquery
	ptqueryRequest := fmt.Sprintf("%snode/%s/%s/ptquery", server.WebAPIPath, uuid, data.DataName())
	returnedData = server.TestHTTP(t, "POST", ptqueryRequest, getPointsJSON(testPoints))
	inclusions, err := putInclusionJSON(returnedData)
	if err != nil {
		t.Fatalf("Error on getting back JSON from ptquery: %s\n", err.Error())
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(inclusions, expectedInclusions) {
		t.Errorf("Bad ptquery results\nOriginal:\n%s\nReturned:\n%s\n", expectedInclusions, inclusions)
	}

	// Test ROI mask out of range -- should be all 0.
	maskRequest := fmt.Sprintf("%snode/%s/%s/mask/0_1_2/100_100_100/10_40_70", server.WebAPIPath, uuid, data.DataName())
	returnedData = server.TestHTTP(t, "GET", maskRequest, nil)
	if len(returnedData) != 100*100*100 {
		t.Errorf("Expected mask volume of %d bytes, got %d bytes instead\n", 100*100*100, len(returnedData))
	}
	for i, value := range returnedData {
		if value != 0 {
			t.Errorf("Expected all-zero mask, got %d at index %d\n", value, i)
			break
		}
	}

	// Test ROI mask within range.
	maskRequest = fmt.Sprintf("%snode/%s/%s/mask/0_1_2/100_100_100/6350_3232_3200", server.WebAPIPath, uuid, data.DataName())
	returnedData = server.TestHTTP(t, "GET", maskRequest, nil)
	if len(returnedData) != 100*100*100 {
		t.Errorf("Expected mask volume of %d bytes, got %d bytes instead\n", 100*100*100, len(returnedData))
	}
	// Check first block plane
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			value := returnedData[y*100+x]
			if x < 50 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x >= 50 && y < 64 && value != 1 {
				t.Errorf("Expected mask to be 1 at (%d, %d) within ROI, got %d instead\n", x, y, value)
				break
			}
			// tuple{100, 103, 201, 212}
			if x <= 81 && y >= 64 && y < 96 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x > 81 && y >= 64 && y < 96 && value != 1 {
				t.Errorf("Expected mask to be 1 at (%d, %d) within ROI, got %d instead\n", x, y, value)
				break
			}
		}
	}
	// Check second block plane
	offset := 32 * 100 * 100 // moves to next block in Z
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			value := returnedData[offset+y*100+x]
			if x < 50 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x <= 81 && y < 32 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x > 81 && y < 32 && value != 1 {
				t.Errorf("Expected mask to be 1 at (%d, %d) within ROI, got %d instead\n", x, y, value)
				break
			}
			if y >= 32 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
		}
	}
	// Check last block plane
	offset = 96 * 100 * 100 // moves to last ROI layer in Z
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			value := returnedData[offset+y*100+x]
			if x < 50 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x >= 50 && y < 32 && value != 1 {
				t.Errorf("Expected mask to be 1 at (%d, %d) within ROI, got %d instead\n", x, y, value)
				break
			}
			if y >= 32 && y < 64 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
			if x >= 50 && y >= 64 && y < 96 && value != 1 {
				t.Errorf("Expected mask to be 1 at (%d, %d) within ROI, got %d instead\n", x, y, value)
				break
			}
			if y >= 96 && value != 0 {
				t.Errorf("Expected mask to be zero at (%d, %d) before ROI, got %d instead\n", x, y, value)
				break
			}
		}
	}
}

func TestROICreateAndSerialize(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice1, err := repo.NewData(roitype, "myroi", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	roi1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not roi.Data\n")
	}
	if roi1.DataName() != "myroi" {
		t.Errorf("New roi data instance name set incorrectly: %q != %q\n",
			roi1.DataName(), "myroi")
	}

	config.Set("BlockSize", "15,16,17")
	dataservice2, err := repo.NewData(roitype, "myroi2", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	roi2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not roi.Data\n")
	}

	if roi1.InstanceID() == roi2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			roi1.InstanceID(), roi2.InstanceID())
	}

	// Test persistence of storage.
	roi2.MinZ = 13
	roi2.MaxZ = 3098
	gobBytes, err := roi2.GobEncode()
	if err != nil {
		t.Fatalf("Could not Gob encode roi: %s\n", err.Error())
	}

	var received Data
	if err = received.GobDecode(gobBytes); err != nil {
		t.Fatalf("Could not decode Gob-encoded roi: %s\n", err.Error())
	}

	if !reflect.DeepEqual(*(roi2.Data), *(received.Data)) {
		t.Errorf("ROI base Data has bad roundtrip:\nOriginal:\n%v\nReceived:\n%v\n",
			*(roi2.Data), *(received.Data))
	}

	if !reflect.DeepEqual(roi2.Properties, received.Properties) {
		t.Errorf("ROI extended properties has bad roundtrip:\nOriginal:\n%v\nReceived:\n%v\n",
			roi2.Properties, received.Properties)
	}
}

func TestROIPartition(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create the ROI dataservice.
	repo, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(roitype, "roi", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not roi.Data\n")
	}

	// PUT an ROI
	roiRequest := fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, data.DataName())
	req, err := http.NewRequest("POST", roiRequest, getSpansJSON(testSpans))
	if err != nil {
		t.Errorf("Unsuccessful POST request (%s): %s\n", roiRequest, err.Error())
	}
	serverCtx := datastore.NewServerContext(context.Background(), repo, versionID)
	w := httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi POST, status %s, for roi %q\n", w.Code, data.DataName())
	}

	// Request the standard subvolume partitioning
	partitionReq := fmt.Sprintf("%snode/%s/%s/partition?batchsize=5&optimized=true", server.WebAPIPath, uuid,
		data.DataName())
	req, err = http.NewRequest("GET", partitionReq, nil)
	if err != nil {
		t.Errorf("Unsuccessful GET request (%s): %s\n", partitionReq, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi GET, status %s, for roi %q\n", w.Code, data.DataName())
	}
	var subvolJSON, expectedJSON interface{}
	response := w.Body.Bytes()
	if err := json.Unmarshal(response, &subvolJSON); err != nil {
		t.Errorf("Can't unmarshal JSON: %s\n", w.Body.Bytes())
	}
	json.Unmarshal([]byte(expectedPartition), &expectedJSON)
	if !reflect.DeepEqual(subvolJSON, expectedJSON) {
		t.Errorf("Error doing optimized subvolume partitioning.  Got bad result:\n%s\n",
			string(response))
	}
}

func TestROISimplePartition(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create the ROI dataservice.
	repo, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(roitype, "roi", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not roi.Data\n")
	}

	// PUT an ROI
	roiRequest := fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, data.DataName())
	req, err := http.NewRequest("POST", roiRequest, getSpansJSON(testSpans))
	if err != nil {
		t.Errorf("Unsuccessful POST request (%s): %s\n", roiRequest, err.Error())
	}
	serverCtx := datastore.NewServerContext(context.Background(), repo, versionID)
	w := httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi POST, status %s, for roi %q\n", w.Code, data.DataName())
	}

	// Request the standard subvolume partitioning
	partitionReq := fmt.Sprintf("%snode/%s/%s/partition?batchsize=5", server.WebAPIPath, uuid,
		data.DataName())
	req, err = http.NewRequest("GET", partitionReq, nil)
	if err != nil {
		t.Errorf("Unsuccessful GET request (%s): %s\n", partitionReq, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi GET, status %s, for roi %q\n", w.Code, data.DataName())
	}
	var subvolJSON, expectedJSON interface{}
	response := w.Body.Bytes()
	if err := json.Unmarshal(response, &subvolJSON); err != nil {
		t.Errorf("Can't unmarshal JSON: %s\n", w.Body.Bytes())
	}
	json.Unmarshal([]byte(expectedSimplePartition), &expectedJSON)
	if !reflect.DeepEqual(subvolJSON, expectedJSON) {
		t.Errorf("Error doing simple subvolume partitioning.  Got bad result:\n%s\n",
			string(response))
	}
}

const expectedPartition = `
		{
		    "NumTotalBlocks": 450,
		    "NumActiveBlocks": 152,
		    "NumSubvolumes": 3,
		    "ROI": {
		        "MinChunk": [
		            0,
		            0,
		            100
		        ],
		        "MaxChunk": [
		            217,
		            105,
		            103
		        ]
		    },
		    "Subvolumes": [
		        {
		            "MinPoint": [
		                6400,
		                3232,
		                3200
		            ],
		            "MaxPoint": [
		                6591,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                200,
		                101,
		                100
		            ],
		            "MaxChunk": [
		                205,
		                105,
		                104
		            ],
		            "TotalBlocks": 150,
		            "ActiveBlocks": 61
		        },
		        {
		            "MinPoint": [
		                6592,
		                3232,
		                3200
		            ],
		            "MaxPoint": [
		                6783,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                206,
		                101,
		                100
		            ],
		            "MaxChunk": [
		                211,
		                105,
		                104
		            ],
		            "TotalBlocks": 150,
		            "ActiveBlocks": 67
		        },
		        {
		            "MinPoint": [
		                6784,
		                3232,
		                3200
		            ],
		            "MaxPoint": [
		                6975,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                212,
		                101,
		                100
		            ],
		            "MaxChunk": [
		                217,
		                105,
		                104
		            ],
		            "TotalBlocks": 150,
		            "ActiveBlocks": 24
		        }
		    ]
		}
`

const expectedSimplePartition = `
		{
		    "NumTotalBlocks": 875,
		    "NumActiveBlocks": 152,
		    "NumSubvolumes": 4,
		    "ROI": {
		        "MinChunk": [
		            0,
		            0,
		            100
		        ],
		        "MaxChunk": [
		            217,
		            105,
		            103
		        ]
		    },
		    "Subvolumes": [
		        {
		            "MinPoint": [
		                6368,
		                3232,
		                3136
		            ],
		            "MaxPoint": [
		                6527,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                199,
		                101,
		                98
		            ],
		            "MaxChunk": [
		                203,
		                105,
		                104
		            ],
		            "TotalBlocks": 175,
		            "ActiveBlocks": 37
		        },
		        {
		            "MinPoint": [
		                6528,
		                3232,
		                3136
		            ],
		            "MaxPoint": [
		                6687,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                204,
		                101,
		                98
		            ],
		            "MaxChunk": [
		                208,
		                105,
		                104
		            ],
		            "TotalBlocks": 175,
		            "ActiveBlocks": 60
		        },
		        {
		            "MinPoint": [
		                6688,
		                3232,
		                3136
		            ],
		            "MaxPoint": [
		                6847,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                209,
		                101,
		                98
		            ],
		            "MaxChunk": [
		                213,
		                105,
		                104
		            ],
		            "TotalBlocks": 175,
		            "ActiveBlocks": 43
		        },
		        {
		            "MinPoint": [
		                6848,
		                3232,
		                3136
		            ],
		            "MaxPoint": [
		                7007,
		                3391,
		                3359
		            ],
		            "MinChunk": [
		                214,
		                101,
		                98
		            ],
		            "MaxChunk": [
		                218,
		                105,
		                104
		            ],
		            "TotalBlocks": 175,
		            "ActiveBlocks": 12
		        }
		    ]
		}
`

func TestROIRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice1, err := repo.NewData(roitype, "myroi", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	roi1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not roi.Data\n")
	}
	if roi1.DataName() != "myroi" {
		t.Errorf("New roi data instance name set incorrectly: %q != %q\n",
			roi1.DataName(), "myroi")
	}

	config.Set("BlockSize", "15,16,17")
	dataservice2, err := repo.NewData(roitype, "myroi2", config)
	if err != nil {
		t.Errorf("Error creating new roi instance: %s\n", err.Error())
	}
	roi2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not roi.Data\n")
	}
	roi2.MinZ = 13
	roi2.MaxZ = 3098
	oldData := *roi2

	// Check instance IDs
	if roi1.InstanceID() == roi2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			roi1.InstanceID(), roi2.InstanceID())
	}

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during ROI persistence test: %s\n", err.Error())
	}
	oldRepoUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldRepoUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldRepoUUID, err.Error())
	}
	dataservice3, err := repo2.GetDataByName("myroi2")
	if err != nil {
		t.Fatalf("Can't get first ROI instance from reloaded test db: %s\n", err.Error())
	}
	roi2new, ok := dataservice3.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 3 is not roi.Data\n")
	}
	if !reflect.DeepEqual(oldData, *roi2new) {
		t.Errorf("Expected %v, got %v\n", oldData, *roi2new)
	}
}
