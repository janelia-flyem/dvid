package keyvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	kvtype datastore.TypeService
	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if kvtype == nil {
		var err error
		kvtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get keyvalue type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// Make sure new keyvalue data have different IDs.
func TestNewKeyvalueDifferent(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice1, err := repo.NewData(kvtype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kv1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not keyvalue.Data\n")
	}
	if kv1.DataName() != "instance1" {
		t.Errorf("New keyvalue data instance name set incorrectly: %q != %q\n",
			kv1.DataName(), "instance1")
	}

	dataservice2, err := repo.NewData(kvtype, "instance2", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kv2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not keyvalue.Data\n")
	}

	if kv1.InstanceID() == kv2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			kv1.InstanceID(), kv2.InstanceID())
	}
}

func TestKeyvalueRoundTrip(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(kvtype, "roundtripper", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not keyvalue.Data\n")
	}

	ctx := datastore.NewVersionedContext(dataservice, versionID)

	keyStr := "testkey"
	value := []byte("I like Japan and this is some unicode: \u65e5\u672c\u8a9e")

	if err = kvdata.PutData(ctx, keyStr, value); err != nil {
		t.Errorf("Could not put keyvalue data: %s\n", err.Error())
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr)
	if err != nil {
		t.Fatalf("Could not get keyvalue data: %s\n", err.Error())
	}
	if !found {
		t.Fatalf("Could not find put keyvalue\n")
	}
	if bytes.Compare(value, retrieved) != 0 {
		t.Errorf("keyvalue retrieved %q != put %q\n", string(retrieved), string(value))
	}
}

func testRequest(t *testing.T, repo datastore.Repo, versionID dvid.VersionID, name dvid.DataString, versioned bool) {
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	config.SetVersioned(versioned)
	dataservice, err := repo.NewData(kvtype, name, config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	serverCtx := datastore.NewContext(context.Background(), repo, versionID)

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	req, err := http.NewRequest("POST", key1req, strings.NewReader(value1))
	if err != nil {
		t.Errorf("Unsuccessful POST request (%s): %s\n", key1req, err.Error())
	}
	w := httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad response keyvalue %q POST, status %s, for key %q\n", w.Code,
			data.DataName(), key1)
	}

	// Get back k/v
	req, err = http.NewRequest("GET", key1req, nil)
	if err != nil {
		t.Errorf("Unsuccessful GET request (%s): %s\n", key1req, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad response keyvalue %q GET, status %s\n", key1, w.Code)
	}
	retrieved := string(w.Body.Bytes())
	if retrieved != value1 {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, retrieved)
	}

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := "more good stuff"
	key2req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	req, err = http.NewRequest("POST", key2req, strings.NewReader(value2))
	if err != nil {
		t.Errorf("Unsuccessful POST request (%s): %s\n", key2req, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad response keyvalue %q POST, status %s, for key %q\n", w.Code,
			data.DataName(), key2)
	}

	// Check return of all keys between given keys.
	rangereq := fmt.Sprintf("%snode/%s/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(),
		"my", "zebra")
	req, err = http.NewRequest("GET", rangereq, strings.NewReader(value2))
	if err != nil {
		t.Errorf("Unsuccessful GET request (%s): %s\n", rangereq, err.Error())
	}
	w = httptest.NewRecorder()
	data.ServeHTTP(serverCtx, w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad response keyvalue %q POST, status %s, for key %q\n", w.Code,
			data.DataName(), key2)
	}
	var retrievedKeys []string
	returnValue := w.Body.Bytes()
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %s\n", err.Error())
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != "mykey" && retrievedKeys[0] != "my2ndKey" {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s (code %d)\n",
			key1, key2, string(returnValue), w.Code)
	}
}

func TestKeyvalueRequests(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()

	testRequest(t, repo, versionID, "versioned", true)
	testRequest(t, repo, versionID, "unversioned", false)
}
