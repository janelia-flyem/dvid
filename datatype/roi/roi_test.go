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

func putSpansJSON(buf *bytes.Buffer) ([]tuple, error) {
	var spans []tuple
	if err := json.Unmarshal(buf.Bytes(), &spans); err != nil {
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

func putInclusionJSON(buf *bytes.Buffer) ([]bool, error) {
	var inclusions []bool
	if err := json.Unmarshal(buf.Bytes(), &inclusions); err != nil {
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
	req, err := http.NewRequest("POST", roiRequest, getSpansJSON(testSpans))
	if err != nil {
		t.Errorf("Unsuccessful POST request (%s): %s\n", roiRequest, err.Error())
	}
	serverCtx := datastore.NewContext(context.Background(), repo, versionID)
	w := httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi POST, status %s, for roi %q\n", w.Code, data.DataName())
	}

	// Get back the ROI
	req, err = http.NewRequest("GET", roiRequest, nil)
	if err != nil {
		t.Errorf("Unsuccessful GET request (%s): %s\n", roiRequest, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response roi GET, status %s, for roi %q\n", w.Code, data.DataName())
	}
	spans, err := putSpansJSON(w.Body)
	if err != nil {
		t.Errorf("Error on getting back JSON from roi GET: %s\n", err.Error())
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(spans, testSpans) {
		t.Errorf("Bad PUT/GET ROI roundtrip\nOriginal:\n%s\nReturned:\n%s\n", testSpans, spans)
	}

	// Test the ptquery
	ptqueryRequest := fmt.Sprintf("%snode/%s/%s/ptquery", server.WebAPIPath, uuid, data.DataName())
	req, err = http.NewRequest("POST", ptqueryRequest, getPointsJSON(testPoints))
	if err != nil {
		t.Fatalf("Unsuccessful POST ptquery request (%s): %s\n", ptqueryRequest, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Bad server response to ptquery, status %s, for roi %q\n", w.Code, data.DataName())
	}
	inclusions, err := putInclusionJSON(w.Body)
	if err != nil {
		t.Fatalf("Error on getting back JSON from ptquery: %s\n", err.Error())
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(inclusions, expectedInclusions) {
		t.Errorf("Bad ptquery results\nOriginal:\n%s\nReturned:\n%s\n", expectedInclusions, inclusions)
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
