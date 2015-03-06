package keyvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"

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

func TestKeyvalueRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.Set("MaxKeySize", "31")
	dataservice, err := repo.NewData(kvtype, "mykv", config)
	if err != nil {
		t.Errorf("Unable to create keyvalue instance: %s\n", err.Error())
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast keyvalue data service into keyvalue.Data\n")
	}
	oldData := *kvdata

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during keyvalue persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mykv")
	if err != nil {
		t.Fatalf("Can't get keyvalue instance from reloaded test db: %s\n", err.Error())
	}
	kvdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not keyvalue.Data\n")
	}
	if !reflect.DeepEqual(oldData, *kvdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *kvdata2)
	}
}

func testRequest(t *testing.T, repo datastore.Repo, versionID dvid.VersionID, name dvid.InstanceName, versioned bool) {
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	dataservice, err := repo.NewData(kvtype, name, config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Get back k/v
	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if string(returnValue) != value1 {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := "more good stuff"
	key2req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Add 3rd k/v
	key3 := "heresanotherkey"
	value3 := "my 3rd value"
	key3req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3req, strings.NewReader(value3))

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(),
		"my", "zebra")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %s\n", err.Error())
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != "mykey" && retrievedKeys[0] != "my2ndKey" {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key2, key1, string(returnValue))
	}

	// Check return of all keys
	allkeyreq := fmt.Sprintf("%snode/%s/%s/keys", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "GET", allkeyreq, nil)

	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %s\n", err.Error())
	}
	if len(retrievedKeys) != 3 || retrievedKeys[0] != "heresanotherkey" && retrievedKeys[1] != "my2ndKey" && retrievedKeys[2] != "mykey" {
		t.Errorf("Bad all key request return.  Expected: [%q,%q,%q].  Got: %s\n",
			key3, key2, key1, string(returnValue))
	}
}

func TestKeyvalueRequests(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()

	testRequest(t, repo, versionID, "versioned", true)
	testRequest(t, repo, versionID, "unversioned", false)
}

func TestKeyvalueVersioning(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()

	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	dataservice, err := repo.NewData(kvtype, "versiontest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := "more good stuff"
	key2req := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Create a new version in repo
	if err = repo.Lock(uuid); err != nil {
		t.Errorf("Unable to lock root node %s: %s\n", uuid, err.Error())
	}
	uuid2, err := repo.NewVersion(uuid)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %s\n", uuid, err.Error())
	}
	_, err = datastore.VersionFromUUID(uuid2)
	if err != nil {
		t.Fatalf("Unable to get version ID from new uuid %s: %s\n", uuid2, err.Error())
	}

	// Change the 2nd k/v
	value2new := "this is completely different"
	key2newreq := fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid2, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2newreq, strings.NewReader(value2new))

	// Get the first version value
	returnValue := server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != value2 {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", key2, value2, string(returnValue))
	}

	// Get the second version value
	returnValue = server.TestHTTP(t, "GET", key2newreq, nil)
	if string(returnValue) != value2new {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", key2, value2new, string(returnValue))
	}

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/%s/%s", server.WebAPIPath, uuid, data.DataName(),
		"my", "zebra")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %s\n", err.Error())
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != "mykey" && retrievedKeys[0] != "my2ndKey" {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key1, key2, string(returnValue))
	}
}
