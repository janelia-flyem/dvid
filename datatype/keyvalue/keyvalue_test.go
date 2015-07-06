package keyvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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
func initTestRepo() (dvid.UUID, dvid.VersionID) {
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

	uuid, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice1, err := datastore.NewData(uuid, kvtype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %v\n", err)
	}
	kv1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not keyvalue.Data\n")
	}
	if kv1.DataName() != "instance1" {
		t.Errorf("New keyvalue data instance name set incorrectly: %q != %q\n",
			kv1.DataName(), "instance1")
	}

	dataservice2, err := datastore.NewData(uuid, kvtype, "instance2", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %v\n", err)
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

	uuid, versionID := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "roundtripper", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not keyvalue.Data\n")
	}

	ctx := datastore.NewVersionedCtx(dataservice, versionID)

	keyStr := "testkey"
	value := []byte("I like Japan and this is some unicode: \u65e5\u672c\u8a9e")

	if err = kvdata.PutData(ctx, keyStr, value); err != nil {
		t.Errorf("Could not put keyvalue data: %v\n", err)
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr)
	if err != nil {
		t.Fatalf("Could not get keyvalue data: %v\n", err)
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

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "mykv", config)
	if err != nil {
		t.Errorf("Unable to create keyvalue instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast keyvalue data service into keyvalue.Data\n")
	}
	oldData := *kvdata

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, kvdata); err != nil {
		t.Fatalf("Unable to save repo during keyvalue persistence test: %v\n", err)
	}
	tests.CloseReopenStore()

	dataservice2, err := datastore.GetDataByUUID(uuid, "mykv")
	if err != nil {
		t.Fatalf("Can't get keyvalue instance from reloaded test db: %v\n", err)
	}
	kvdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not keyvalue.Data\n")
	}
	if !oldData.Equals(kvdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *kvdata2)
	}
}

func testRequest(t *testing.T, uuid dvid.UUID, versionID dvid.VersionID, name dvid.InstanceName) {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, name, config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Get back k/v
	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if string(returnValue) != value1 {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Expect error if no key used.
	badrequest := fmt.Sprintf("%snode/%s/%s/key/", server.WebAPIPath, uuid, data.DataName())
	server.TestBadHTTP(t, "GET", badrequest, nil)

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := "more good stuff"
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Add 3rd k/v
	key3 := "heresanotherkey"
	value3 := "my 3rd value"
	key3req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3req, strings.NewReader(value3))

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, data.DataName(),
		"my", "zebra")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != "mykey" && retrievedKeys[0] != "my2ndKey" {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key2, key1, string(returnValue))
	}

	// Check return of all keys
	allkeyreq := fmt.Sprintf("%snode/%s/%s/keys", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "GET", allkeyreq, nil)

	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 3 || retrievedKeys[0] != "heresanotherkey" && retrievedKeys[1] != "my2ndKey" && retrievedKeys[2] != "mykey" {
		t.Errorf("Bad all key request return.  Expected: [%q,%q,%q].  Got: %s\n",
			key3, key2, key1, string(returnValue))
	}
}

func TestKeyvalueRequests(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid, versionID := initTestRepo()

	testRequest(t, uuid, versionID, "mykeyvalue")
}

func TestKeyvalueVersioning(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "versiontest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := "more good stuff"
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Create a new version in repo
	if err = datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Errorf("Unable to lock root node %s: %v\n", uuid, err)
	}
	uuid2, err := datastore.NewVersion(uuid, "some child", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid, err)
	}
	_, err = datastore.VersionFromUUID(uuid2)
	if err != nil {
		t.Fatalf("Unable to get version ID from new uuid %s: %v\n", uuid2, err)
	}

	// Change the 2nd k/v
	uuid2val := "this is completely different"
	uuid2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid2, data.DataName(), key2)
	server.TestHTTP(t, "POST", uuid2req, strings.NewReader(uuid2val))

	// Get the first version value
	returnValue := server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != value2 {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", key2, value2, string(returnValue))
	}

	// Get the second version value
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, data.DataName(),
		"my", "zebra")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != "mykey" && retrievedKeys[0] != "my2ndKey" {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key1, key2, string(returnValue))
	}

	// Commit the repo
	if err = datastore.Commit(uuid2, "my 2nd commit msg", []string{"changed 2nd k/v"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}
	uuid3, err := datastore.NewVersion(uuid2, "some child", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid2, err)
	}

	// Delete the 2nd k/v
	uuid3req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid3, data.DataName(), key2)
	server.TestHTTP(t, "DELETE", uuid3req, nil)

	server.TestBadHTTP(t, "GET", uuid3req, nil)

	// Make sure the 2nd k/v is correct for each of previous versions.
	returnValue = server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != value2 {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", key2, value2, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}

	// Make a child
	if err = datastore.Commit(uuid3, "my 3rd commit msg", []string{"deleted 2nd k/v"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}
	uuid4, err := datastore.NewVersion(uuid3, "some child", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid3, err)
	}

	// Change the 2nd k/v
	uuid4val := "we are reintroducing this k/v"
	uuid4req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid4, data.DataName(), key2)
	server.TestHTTP(t, "POST", uuid4req, strings.NewReader(uuid4val))

	if err = datastore.Commit(uuid4, "commit node 4", []string{"we modified stuff"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid4, err)
	}

	// Make sure the 2nd k/v is correct for each of previous versions.
	returnValue = server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != value2 {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", key2, value2, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}
	server.TestBadHTTP(t, "GET", uuid3req, nil)
	returnValue = server.TestHTTP(t, "GET", uuid4req, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on fourth version, key %q: expected %s, got %s\n", key2, uuid4val, string(returnValue))
	}

	// Let's try a merge!

	// Make a child off the 2nd version from root.
	uuid5, err := datastore.NewVersion(uuid2, "some child", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid2, err)
	}

	// Store new stuff in 2nd k/v
	uuid5val := "this is forked value"
	uuid5req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid5, data.DataName(), key2)
	server.TestHTTP(t, "POST", uuid5req, strings.NewReader(uuid5val))

	returnValue = server.TestHTTP(t, "GET", uuid5req, nil)
	if string(returnValue) != uuid5val {
		t.Errorf("Error on merged child, key %q: expected %q, got %q\n", key2, uuid5val, string(returnValue))
	}

	// Commit node
	if err = datastore.Commit(uuid5, "forked node", []string{"we modified stuff"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid5, err)
	}

	// Should be able to merge using conflict-free (disjoint at key level) merge even though
	// its conflicted.  Will get lazy error on request.
	badChild, err := datastore.Merge([]dvid.UUID{uuid4, uuid5}, "some child", datastore.MergeConflictFree)
	if err != nil {
		t.Errorf("Error doing merge: %v\n", err)
	}
	childreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, badChild, data.DataName(), key2)
	server.TestBadHTTP(t, "GET", childreq, nil)

	// Branch, and then delete 2nd k/v and commit.
	uuid6, err := datastore.NewVersion(uuid5, "some child", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid5, err)
	}

	uuid6req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid6, data.DataName(), key2)
	server.TestHTTP(t, "DELETE", uuid6req, nil)
	server.TestBadHTTP(t, "GET", uuid6req, nil)

	if err = datastore.Commit(uuid6, "deleted forked node 2nd k/v", []string{"we modified stuff"}); err != nil {
		t.Errorf("Unable to commit node %s: %s\n", uuid6, err)
	}

	// Should now be able to correctly merge the two branches.
	goodChild, err := datastore.Merge([]dvid.UUID{uuid4, uuid6}, "merging stuff", datastore.MergeConflictFree)
	if err != nil {
		t.Errorf("Error doing merge: %v\n", err)
	}

	// We should be able to see just the original uuid4 value of the 2nd k/v
	childreq = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, goodChild, data.DataName(), key2)
	returnValue = server.TestHTTP(t, "GET", childreq, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on merged child, key %q: expected %q, got %q\n", key2, uuid4val, string(returnValue))
	}
}

// Test added after error in getting two paths to the same ancestor k/v after merge.
func TestDiamondGetOnMerge(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "mergetest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	if err = datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Errorf("Unable to lock root node %s: %v\n", uuid, err)
	}
	uuid2, err := datastore.NewVersion(uuid, "first child", nil)
	if err != nil {
		t.Fatalf("Unable to create 1st child off root %s: %v\n", uuid, err)
	}
	if err = datastore.Commit(uuid2, "first child", nil); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}
	uuid3, err := datastore.NewVersion(uuid, "second child", nil)
	if err != nil {
		t.Fatalf("Unable to create 2nd child off root %s: %v\n", uuid, err)
	}
	if err = datastore.Commit(uuid3, "second child", nil); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid3, err)
	}

	child, err := datastore.Merge([]dvid.UUID{uuid2, uuid3}, "merging stuff", datastore.MergeConflictFree)
	if err != nil {
		t.Errorf("Error doing merge: %v\n", err)
	}

	// We should be able to see just the original uuid value of the k/v
	childreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, child, data.DataName(), key1)
	returnValue := server.TestHTTP(t, "GET", childreq, nil)
	if string(returnValue) != value1 {
		t.Errorf("Error on merged child, key %q: expected %q, got %q\n", key1, value1, string(returnValue))
	}
}
