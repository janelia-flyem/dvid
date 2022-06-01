package neuronjson

import (
	"archive/tar"
	bytes "bytes"
	"encoding/json"
	"fmt"
	io "io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"google.golang.org/api/iterator"
)

var (
	jsontype datastore.TypeService
	testMu   sync.Mutex
)

var sampleData = map[uint64]string{
	1000: `"bodyid": 1000, "position": [100, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]`,
	1001: `"bodyid": 1001, "position": [110, 111, 112], "avg_location": "113, 114, 115", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]`,
	1002: `"bodyid": 1002, "position": [100, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]`,
	1003: `"bodyid": 1003, "position": [102, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]`,
	2000: `"bodyid": 2000, "position": [200, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]`,
	2001: `"bodyid": 2001, "position": [200, 111, 112], "avg_location": "213, 214, 215", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]`,
	2002: `"bodyid": 2002, "position": [200, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]`,
	2003: `"bodyid": 2003, "position": [202, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]`,
	3000: `"bodyid": 3000, "position": [300, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]`,
	3001: `"bodyid": 3001, "position": [300, 111, 113], "avg_location": "313, 314, 315", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]`,
	3002: `"bodyid": 3002, "position": [302, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]`,
	3003: `"bodyid": 3003, "position": [303, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]`,
}

// ---- Test stub for DocGetter and DocIterator instead of firestore Client

type testDocGetter struct {
	index int
}

func (dg *testDocGetter) Data() (value map[string]interface{}) {
	if dg.index < 0 || dg.index >= len(sampleData) {
		return
	}
	data := sampleData[uint64(dg.index)]
	if err := json.Unmarshal([]byte(data), &value); err != nil {
		dvid.Errorf("unable to unmarshal JSON data: %s (error = %q)\n", data, err)
		return nil
	}
	return
}

type testIterator struct {
	index int
}

func (ti *testIterator) Next() (DocGetter, error) {
	if ti.index >= len(sampleData) {
		return nil, iterator.Done
	}
	doc := &testDocGetter{index: ti.index}
	ti.index++
	return doc, nil
}

func (ti *testIterator) Close() {}

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if jsontype == nil {
		var err error
		jsontype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get neuronjson type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func equalJSON(vx, vy interface{}) bool {
	if reflect.TypeOf(vx) != reflect.TypeOf(vy) {
		return false
	}
	switch x := vx.(type) {
	case map[string]interface{}:
		y := vy.(map[string]interface{})
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			val2 := y[k]
			if (v == nil) != (val2 == nil) {
				return false
			}
			if !equalJSON(v, val2) {
				return false
			}
		}
		return true

	case []interface{}:
		y := vy.([]interface{})
		if len(x) != len(y) {
			return false
		}
		var matches int
		flagged := make([]bool, len(y))
		for _, v := range x {
			for i, v2 := range y {
				if equalJSON(v, v2) && !flagged[i] {
					matches++
					flagged[i] = true

					break
				}
			}
		}
		return matches == len(x)

	default:
		return vx == vy
	}
}

// equalObjectJSON compares two []byte of JSON objects, ignoring ordering.
func equalObjectJSON(x, y []byte) bool {
	var vx, vy map[string]interface{}
	if err := json.Unmarshal(x, &vx); err != nil {
		return false
	}
	if err := json.Unmarshal(y, &vy); err != nil {
		return false
	}
	return equalJSON(vx, vy)
}

// Make sure new neuronjson data have different IDs.
func TestNewNeuronjsonDifferent(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice1, err := datastore.NewData(uuid, jsontype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new neuronjson instance: %v\n", err)
	}
	kv1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not neuronjson.Data\n")
	}
	if kv1.DataName() != "instance1" {
		t.Errorf("New neuronjson data instance name set incorrectly: %q != %q\n",
			kv1.DataName(), "instance1")
	}

	dataservice2, err := datastore.NewData(uuid, jsontype, "instance2", config)
	if err != nil {
		t.Errorf("Error creating new neuronjson instance: %v\n", err)
	}
	kv2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not neuronjson.Data\n")
	}

	if kv1.InstanceID() == kv2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			kv1.InstanceID(), kv2.InstanceID())
	}
}

func TestNeuronjsonRoundTrip(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, versionID := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, jsontype, "roundtripper", config)
	if err != nil {
		t.Errorf("Error creating new neuronjson instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not neuronjson.Data\n")
	}

	ctx := datastore.NewVersionedCtx(dataservice, versionID)

	keyStr := "1234"
	value := []byte(`{"a string": "foo", "a number": 1234, "a list": [1, 2, 3]}`)

	if err = kvdata.PutData(ctx, keyStr, value); err != nil {
		t.Errorf("Could not put neuronjson data: %v\n", err)
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr)
	if err != nil {
		t.Fatalf("Could not get neuronjson data: %v\n", err)
	}
	if !found {
		t.Fatalf("Could not find put neuronjson\n")
	}
	if !equalObjectJSON(value, retrieved) {
		t.Errorf("neuronjson retrieved %q != put %q\n", string(retrieved), string(value))
	}
}

func TestNeuronjsonRepoPersistence(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, jsontype, "annotations", config)
	if err != nil {
		t.Errorf("Unable to create neuronjson instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast neuronjson data service into neuronjson.Data\n")
	}
	oldData := *kvdata

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, kvdata); err != nil {
		t.Fatalf("Unable to save repo during neuronjson persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "annotations")
	if err != nil {
		t.Fatalf("Can't get neuronjson instance from reloaded test db: %v\n", err)
	}
	kvdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not neuronjson.Data\n")
	}
	if !oldData.Equals(kvdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *kvdata2)
	}
}

func testRequest(t *testing.T, uuid dvid.UUID, versionID dvid.VersionID, name dvid.InstanceName) {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, jsontype, name, config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not neuronjson.Data\n")
	}

	key1 := "1000"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	resp := server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("HEAD on %s did not return 404 (File not found).  Status = %d\n", key1req, resp.Code)
	}

	// PUT a value
	value1 := `{"a string": "foo", "a number": 1234, "a list": [1, 2, 3]}`
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	resp = server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("HEAD on %s did not return 200 (OK).  Status = %d\n", key1req, resp.Code)
	}

	// Get back k/v
	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if !equalObjectJSON(returnValue, []byte(value1)) {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Expect error if no key used.
	badrequest := fmt.Sprintf("%snode/%s/%s/key/", server.WebAPIPath, uuid, data.DataName())
	server.TestBadHTTP(t, "GET", badrequest, nil)

	// Add 2nd k/v
	key2 := "2000"
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key2)

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("HEAD on %s did not return 404 (File Not Found).  Status = %d\n", key2req, resp.Code)
	}

	value2 := `{"a string": "moo", "a number": 2345, "a list": ["mom", "pop"]}`
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("HEAD on %s did not return 200 (OK).  Status = %d\n", key2req, resp.Code)
	}

	// Add 3rd k/v
	key3 := "3000"
	value3 := `{"a string": "goo", "a number": 3456, "a list": [23]}`
	key3req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3req, strings.NewReader(value3))

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, data.DataName(), "0", "2100")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != key2 && retrievedKeys[0] != key1 {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key2, key1, string(returnValue))
	}

	// Check return of all keys
	allkeyreq := fmt.Sprintf("%snode/%s/%s/keys", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "GET", allkeyreq, nil)

	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 3 || retrievedKeys[0] != key1 && retrievedKeys[1] != key2 && retrievedKeys[2] != key3 {
		t.Errorf("Bad all key request return.  Expected: [%q,%q,%q].  Got: %s\n",
			key3, key2, key1, string(returnValue))
	}
}

func TestKeyvalueRequests(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, versionID := initTestRepo()

	testRequest(t, uuid, versionID, "annotations")
}

var testData = []struct {
	key string
	val string
}{
	{"1000", `{"bodyid": 1000, "a number": 3456, "position": [150,250,380]}`},
	{"2000", `{"bodyid": 2000, "bar":"another string", "baz":[1, 2, 3]}`},
	{"3000", `{"bodyid": 3000, "a number": 3456, "a list": [23]}`},
	{"4000", `{"position": [151, 251, 301], "bodyid": 4000, "soma_side": "LHS"}`},
}

func TestKeyvalueRange(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	config.Set("versioned", "false")
	_, err := datastore.NewData(uuid, jsontype, "unversiontest", config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}

	// PUT a value
	key1req := fmt.Sprintf("%snode/%s/unversiontest/key/%s", server.WebAPIPath, uuid, testData[0].key)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(testData[0].val))

	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if !equalObjectJSON(returnValue, []byte(testData[0].val)) {
		t.Errorf("Error on key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Add 2nd k/v
	key2req := fmt.Sprintf("%snode/%s/unversiontest/key/%s", server.WebAPIPath, uuid, testData[1].key)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(testData[1].val))

	// Test
	rangeReq := fmt.Sprintf("%snode/%s/unversiontest/keyrangevalues/0/2001", server.WebAPIPath, uuid)
	expectedJSON := fmt.Sprintf(`{"%s":%s,"%s":%s}`, testData[0].key, testData[0].val, testData[1].key, testData[1].val)

	returnValue = server.TestHTTP(t, "GET", rangeReq+"?json=true", nil)
	if !equalObjectJSON(returnValue, []byte(expectedJSON)) {
		t.Errorf("Error on keyrangevalues: got %s, expected %s\n", string(returnValue), expectedJSON)
	}
}

func TestAnnotationVersioning(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, jsontype, "versiontest", config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not neuronjson.Data\n")
	}

	// Add the first 4 annotations
	var keyreq = make([]string, len(testData))
	for i := 0; i < len(testData); i++ {
		keyreq[i] = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), testData[i].key)
		server.TestHTTP(t, "POST", keyreq[i], strings.NewReader(testData[i].val))
	}

	// Use the batch POST for all 4 annotations with key += 10000
	var kvs KeyValues
	kvs.Kvs = make([]*KeyValue, 4)
	for i := 0; i < 4; i++ {
		kvs.Kvs[i] = &KeyValue{
			Key:   fmt.Sprintf("1%s", testData[i].key),
			Value: []byte(testData[i].val),
		}
	}
	serialization, err := pb.Marshal(&kvs)
	if err != nil {
		t.Fatalf("couldn't serialize keyvalues: %v\n", err)
	}
	kvsPostReq := fmt.Sprintf("%snode/%s/%s/keyvalues", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", kvsPostReq, bytes.NewReader(serialization))

	// Create a new version in repo
	if err = datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Errorf("Unable to lock root node %s: %v\n", uuid, err)
	}
	uuid2, err := datastore.NewVersion(uuid, "some child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid, err)
	}
	_, err = datastore.VersionFromUUID(uuid2)
	if err != nil {
		t.Fatalf("Unable to get version ID from new uuid %s: %v\n", uuid2, err)
	}

	// Change key 2000
	uuid2val := `{"bodyid": 2000, "data": "new stuff"}`
	uuid2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid2, data.DataName(), testData[1].key)
	server.TestHTTP(t, "POST", uuid2req, strings.NewReader(uuid2val))

	// Get the first version value
	returnValue := server.TestHTTP(t, "GET", keyreq[0], nil)
	if !equalObjectJSON(returnValue, []byte(testData[0].val)) {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Get the second version value
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, []byte(uuid2val)) {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, uuid2val, string(returnValue))
	}

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, data.DataName(), "10", "2010")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Errorf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != testData[1].key && retrievedKeys[0] != testData[0].key {
		t.Errorf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			testData[0].key, testData[1].key, string(returnValue))
	}

	// Check values from batch POST using individual key gets
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("1%s", testData[i].key)
		keyreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), k)
		returnValue := server.TestHTTP(t, "GET", keyreq, nil)
		if !equalObjectJSON(returnValue, []byte(testData[i].val)) {
			t.Errorf("Expected batch POST key %q to have value %s, got %d instead\n", k, testData[i].val, returnValue)
		}
	}

	// Check some values from batch POST using GET /keyvalues?jsontar=true
	getreq1 := fmt.Sprintf("%snode/%s/%s/keyvalues?jsontar=true", server.WebAPIPath, uuid, data.DataName())
	tardata := server.TestHTTP(t, "GET", getreq1, bytes.NewBufferString(`["1000","2000","4000"]`))
	tarbuf := bytes.NewBuffer(tardata)
	tr := tar.NewReader(tarbuf)
	expectedKeys := []string{"1000", "2000", "4000"}
	expectedVals := []string{testData[0].val, testData[1].val, testData[3].val}
	keyNum := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error parsing tar: %v\n", err)
		}
		if hdr.Name != expectedKeys[keyNum] {
			t.Fatalf("expected for key %d %q, got %q", keyNum, expectedKeys[keyNum], hdr.Name)
		}
		var val bytes.Buffer
		if _, err := io.Copy(&val, tr); err != nil {
			t.Fatalf("error reading tar data: %v\n", err)
		}
		returnValue := val.Bytes()
		if !equalObjectJSON(returnValue, []byte(expectedVals[keyNum])) {
			t.Errorf("Expected batch POST key %q to have value %s, got %d instead\n", expectedKeys[keyNum], expectedVals[keyNum], returnValue)
		}
		dvid.Infof("Key: %s, Value: %s\n", hdr.Name, returnValue)
		keyNum++
	}
	if keyNum != 3 {
		t.Fatalf("Got %d keys when there should have been 3\n", keyNum)
	}

	// Check some values from batch POST using GET /keyvalues (protobuf3)
	getreq2 := fmt.Sprintf("%snode/%s/%s/keyvalues", server.WebAPIPath, uuid, data.DataName())
	pbufKeys := Keys{
		Keys: expectedKeys,
	}
	keysSerialization, err := pb.Marshal(&pbufKeys)
	if err != nil {
		t.Fatalf("couldn't serialized protobuf keys: %v\n", err)
	}
	keyvaluesSerialization := server.TestHTTP(t, "GET", getreq2, bytes.NewBuffer(keysSerialization))
	var pbKVs KeyValues
	if err := pb.Unmarshal(keyvaluesSerialization, &pbKVs); err != nil {
		t.Fatalf("couldn't unmarshal keyvalues protobuf: %v\n", err)
	}
	if len(pbKVs.Kvs) != 3 {
		t.Fatalf("expected 3 kv pairs returned, got %d\n", len(pbKVs.Kvs))
	}
	for keyNum, kv := range pbKVs.Kvs {
		if kv.Key != expectedKeys[keyNum] {
			t.Fatalf("expected for key %d %q, got %q", keyNum, expectedKeys[keyNum], kv.Key)
		}
		if !equalObjectJSON(kv.Value, []byte(expectedVals[keyNum])) {
			t.Errorf("Expected batch POST key %q to have value %s, got %d instead\n", expectedKeys[keyNum], testData[keyNum].val, returnValue)
		}
	}

	// Commit the repo
	if err = datastore.Commit(uuid2, "my 2nd commit msg", []string{"changed 2nd k/v"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}

	// Make grandchild of root
	uuid3, err := datastore.NewVersion(uuid2, "some child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid2, err)
	}

	// Delete the 2nd k/v
	uuid3req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid3, data.DataName(), testData[1].key)
	server.TestHTTP(t, "DELETE", uuid3req, nil)

	server.TestBadHTTP(t, "GET", uuid3req, nil)

	// Make sure the 2nd k/v is correct for each of previous versions.
	returnValue = server.TestHTTP(t, "GET", keyreq[1], nil)
	if !equalObjectJSON(returnValue, []byte(testData[1].val)) {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", testData[1].key, testData[1].val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, []byte(uuid2val)) {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, uuid2val, string(returnValue))
	}
}
