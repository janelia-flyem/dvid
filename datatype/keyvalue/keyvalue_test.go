package keyvalue

import (
	"archive/tar"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
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
	return datastore.NewTestRepo()
}

// Make sure new keyvalue data have different IDs.
func TestNewKeyvalueDifferent(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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

	keyStr := "testkey.-{}03`~| %@\x01"
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "mykv")
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
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
	}

	key1 := "mykey"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	resp := server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("HEAD on %s did not return 404 (File not found).  Status = %d\n", key1req, resp.Code)
	}

	// PUT a value
	value1 := "some stuff"
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	resp = server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("HEAD on %s did not return 200 (OK).  Status = %d\n", key1req, resp.Code)
	}

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
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key2)

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("HEAD on %s did not return 404 (File Not Found).  Status = %d\n", key2req, resp.Code)
	}

	value2 := "more good stuff"
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("HEAD on %s did not return 200 (OK).  Status = %d\n", key2req, resp.Code)
	}

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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, versionID := initTestRepo()

	testRequest(t, uuid, versionID, "mykeyvalue")
}

func testHTTPRange(t *testing.T, urlStr, rangeHeader string) *httptest.ResponseRecorder {
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		t.Fatalf("couldn't create range request %q: %v", urlStr, err)
	}
	req.Header.Set("Range", rangeHeader)
	resp := httptest.NewRecorder()
	server.ServeSingleHTTP(resp, req)
	return resp
}

func TestKeyvalueRangeReads(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "rangekv", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
	}

	postKey := func(key, value string) string {
		req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key)
		server.TestHTTP(t, "POST", req, strings.NewReader(value))
		return req
	}

	plainKey := "plain"
	plainValue := "abcdefghijklmnopqrstuvwxyz"
	plainReq := postKey(plainKey, plainValue)
	jsonKey := "json"
	jsonReq := postKey(jsonKey, "1234567890")
	jsonFullKey := "json-full"
	postKey(jsonFullKey, `{"ok":true}`)
	fullKey := "full"
	fullValue := "full value"
	postKey(fullKey, fullValue)

	returnValue := server.TestHTTP(t, "GET", plainReq+"?byte_start=2&byte_end=5", nil)
	if string(returnValue) != "cdef" {
		t.Fatalf("expected /key range read to return %q, got %q", "cdef", string(returnValue))
	}
	if returnValue = server.TestHTTP(t, "GET", plainReq+"?byte_start=0&byte_end=0", nil); string(returnValue) != "a" {
		t.Fatalf("expected /key single-byte range read to return %q, got %q", "a", string(returnValue))
	}
	server.TestBadHTTP(t, "GET", plainReq+"?byte_start=2", nil)
	server.TestBadHTTP(t, "GET", plainReq+"?byte_start=5&byte_end=2", nil)
	server.TestBadHTTP(t, "GET", plainReq+"?byte_start=0&byte_end=100", nil)
	server.TestBadHTTP(t, "GET", plainReq+"?byte_start=-1&byte_end=2", nil)

	resp := server.TestHTTPResponse(t, "HEAD", plainReq, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected HEAD status %d, got %d", http.StatusOK, resp.Code)
	}
	if got := resp.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected HEAD Accept-Ranges %q, got %q", "bytes", got)
	}

	resp = testHTTPRange(t, plainReq, "bytes=2-5")
	if resp.Code != http.StatusPartialContent {
		t.Fatalf("expected HTTP Range status %d, got %d", http.StatusPartialContent, resp.Code)
	}
	if resp.Body.String() != "cdef" {
		t.Fatalf("expected HTTP Range body %q, got %q", "cdef", resp.Body.String())
	}
	if got := resp.Header().Get("Content-Range"); got != "bytes 2-5/26" {
		t.Fatalf("expected Content-Range %q, got %q", "bytes 2-5/26", got)
	}
	if got := resp.Header().Get("Content-Length"); got != "4" {
		t.Fatalf("expected Content-Length %q, got %q", "4", got)
	}
	if got := resp.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected Accept-Ranges %q, got %q", "bytes", got)
	}

	resp = testHTTPRange(t, plainReq, "bytes=24-")
	if resp.Code != http.StatusPartialContent || resp.Body.String() != "yz" {
		t.Fatalf("expected open-ended HTTP Range response 206 with %q, got %d with %q", "yz", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Range"); got != "bytes 24-25/26" {
		t.Fatalf("expected open-ended Content-Range %q, got %q", "bytes 24-25/26", got)
	}

	resp = testHTTPRange(t, plainReq, "bytes=-3")
	if resp.Code != http.StatusPartialContent || resp.Body.String() != "xyz" {
		t.Fatalf("expected suffix HTTP Range response 206 with %q, got %d with %q", "xyz", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Range"); got != "bytes 23-25/26" {
		t.Fatalf("expected suffix Content-Range %q, got %q", "bytes 23-25/26", got)
	}

	resp = testHTTPRange(t, plainReq, "bytes=24-100")
	if resp.Code != http.StatusPartialContent || resp.Body.String() != "yz" {
		t.Fatalf("expected clamped HTTP Range response 206 with %q, got %d with %q", "yz", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Range"); got != "bytes 24-25/26" {
		t.Fatalf("expected clamped Content-Range %q, got %q", "bytes 24-25/26", got)
	}

	resp = testHTTPRange(t, plainReq, "bytes=26-30")
	if resp.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected unsatisfiable HTTP Range status %d, got %d", http.StatusRequestedRangeNotSatisfiable, resp.Code)
	}
	if got := resp.Header().Get("Content-Range"); got != "bytes */26" {
		t.Fatalf("expected unsatisfiable Content-Range %q, got %q", "bytes */26", got)
	}

	for _, rangeHeader := range []string{"bytes=0-1,4-5", "bytes=5-2", "bytes=abc", "items=0-5"} {
		resp = testHTTPRange(t, plainReq, rangeHeader)
		if resp.Code != http.StatusOK {
			t.Fatalf("expected unsupported or malformed Range %q to return status %d, got %d", rangeHeader, http.StatusOK, resp.Code)
		}
		if resp.Body.String() != plainValue {
			t.Fatalf("expected unsupported or malformed Range %q to return full body %q, got %q", rangeHeader, plainValue, resp.Body.String())
		}
	}

	resp = testHTTPRange(t, plainReq+"?byte_start=0&byte_end=1", "bytes=2-5")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected combined query/header range status %d, got %d", http.StatusBadRequest, resp.Code)
	}

	getJSON := fmt.Sprintf("%snode/%s/%s/keyvalues?json=true", server.WebAPIPath, uuid, data.DataName())
	jsonRequests := `[{"key":"` + jsonKey + `","byte_start":2,"byte_end":5},{"key":"` + jsonFullKey + `"},{"key":"missing","byte_start":0,"byte_end":2}]`
	returnValue = server.TestHTTP(t, "GET", getJSON, strings.NewReader(jsonRequests))
	expectedJSON := fmt.Sprintf(`{%q:3456,%q:{"ok":true},"missing":{}}`, jsonKey, jsonFullKey)
	if string(returnValue) != expectedJSON {
		t.Fatalf("expected ranged JSON /keyvalues response %s, got %s", expectedJSON, string(returnValue))
	}

	server.TestBadHTTP(t, "GET", getJSON, strings.NewReader(`[{"key":"json","byte_start":2}]`))
	server.TestBadHTTP(t, "GET", getJSON, strings.NewReader(`[{"key":"json","byte_start":0,"byte_end":100}]`))
	server.TestBadHTTP(t, "GET", getJSON, strings.NewReader(`[{"key":"plain","byte_start":2,"byte_end":5}]`))

	getTar := fmt.Sprintf("%snode/%s/%s/keyvalues?jsontar=true", server.WebAPIPath, uuid, data.DataName())
	tarRequests := `[{"key":"` + plainKey + `","byte_start":4,"byte_end":7},{"key":"missing","byte_start":0,"byte_end":2}]`
	tardata := server.TestHTTP(t, "GET", getTar, strings.NewReader(tarRequests))
	tr := tar.NewReader(bytes.NewReader(tardata))
	expectedTar := []struct {
		key   string
		value string
	}{
		{plainKey, "efgh"},
		{"missing", ""},
	}
	for i, expected := range expectedTar {
		hdr, err := tr.Next()
		if err != nil {
			t.Fatalf("error reading tar entry %d: %v", i, err)
		}
		if hdr.Name != expected.key {
			t.Fatalf("expected tar entry %d key %q, got %q", i, expected.key, hdr.Name)
		}
		var val bytes.Buffer
		if _, err := io.Copy(&val, tr); err != nil {
			t.Fatalf("error reading tar value for %q: %v", hdr.Name, err)
		}
		if val.String() != expected.value {
			t.Fatalf("expected tar value for %q to be %q, got %q", hdr.Name, expected.value, val.String())
		}
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected end of tar data, got %v", err)
	}
	server.TestBadHTTP(t, "GET", getTar, strings.NewReader(`[{"key":"plain","byte_start":0,"byte_end":100}]`))

	getProtobuf := fmt.Sprintf("%snode/%s/%s/keyvalues", server.WebAPIPath, uuid, data.DataName())
	keys := proto.Keys{
		Keys: []string{fullKey},
		KeyRanges: []*proto.KeyRange{
			{Key: plainKey, ByteStart: 1, ByteEnd: 3},
			{Key: "missing", ByteStart: 0, ByteEnd: 1},
		},
	}
	keysSerialization, err := pb.Marshal(&keys)
	if err != nil {
		t.Fatalf("couldn't serialize ranged protobuf keys: %v", err)
	}
	keyvaluesSerialization := server.TestHTTP(t, "GET", getProtobuf, bytes.NewReader(keysSerialization))
	var pbKVs proto.KeyValues
	if err := pb.Unmarshal(keyvaluesSerialization, &pbKVs); err != nil {
		t.Fatalf("couldn't unmarshal ranged keyvalues protobuf: %v", err)
	}
	if len(pbKVs.Kvs) != 3 {
		t.Fatalf("expected 3 ranged protobuf kv pairs returned, got %d", len(pbKVs.Kvs))
	}
	expectedProto := []struct {
		key   string
		value string
	}{
		{fullKey, fullValue},
		{plainKey, "bcd"},
		{"missing", ""},
	}
	for i, expected := range expectedProto {
		if pbKVs.Kvs[i].Key != expected.key {
			t.Fatalf("expected protobuf kv %d key %q, got %q", i, expected.key, pbKVs.Kvs[i].Key)
		}
		if string(pbKVs.Kvs[i].Value) != expected.value {
			t.Fatalf("expected protobuf kv %d value %q, got %q", i, expected.value, string(pbKVs.Kvs[i].Value))
		}
	}

	badKeys := proto.Keys{
		KeyRanges: []*proto.KeyRange{{Key: plainKey, ByteStart: 10, ByteEnd: 5}},
	}
	badKeysSerialization, err := pb.Marshal(&badKeys)
	if err != nil {
		t.Fatalf("couldn't serialize bad ranged protobuf keys: %v", err)
	}
	server.TestBadHTTP(t, "GET", getProtobuf, bytes.NewReader(badKeysSerialization))

	returnValue = server.TestHTTP(t, "GET", jsonReq+"?byte_start=2&byte_end=5", nil)
	if string(returnValue) != "3456" {
		t.Fatalf("expected /key range read on JSON value to return %q, got %q", "3456", string(returnValue))
	}
}

type resolveResp struct {
	Child dvid.UUID `json:"child"`
}

func TestKeyvalueRange(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	config.Set("versioned", "false")
	_, err := datastore.NewData(uuid, kvtype, "unversiontest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}

	// PUT a value
	key1 := "key1"
	value1 := `[1, 2, 3]`
	key1req := fmt.Sprintf("%snode/%s/unversiontest/key/%s", server.WebAPIPath, uuid, key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if string(returnValue) != value1 {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Add 2nd k/v
	key2 := "key2"
	value2 := `{"foo":"a string", "bar":"another string", "baz":[1, 2, 3]}`
	key2req := fmt.Sprintf("%snode/%s/unversiontest/key/%s", server.WebAPIPath, uuid, key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Test
	rangeReq := fmt.Sprintf("%snode/%s/unversiontest/keyrangevalues/a/zz", server.WebAPIPath, uuid)
	expectedJSON := `{"key1":[1, 2, 3],"key2":` + value2 + "}"

	returnValue = server.TestHTTP(t, "GET", rangeReq+"?json=true", nil)
	if string(returnValue) != expectedJSON {
		t.Errorf("Error on keyrangevalues: got %s, expected %s\n", string(returnValue), expectedJSON)
	}
}

func TestKeyvalueUnversioned(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	config.Set("versioned", "false")
	dataservice, err := datastore.NewData(uuid, kvtype, "unversiontest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
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
	uuid2, err := datastore.NewVersion(uuid, "some child", "", nil)
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

	// Now the first version value should equal the new value
	returnValue := server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Error on unversioned key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}

	// Get the second version value
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Error on unversioned key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
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

	// Make grandchild of root
	uuid3, err := datastore.NewVersion(uuid2, "some child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid2, err)
	}

	// Delete the 2nd k/v
	uuid3req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid3, data.DataName(), key2)
	server.TestHTTP(t, "DELETE", uuid3req, nil)

	server.TestBadHTTP(t, "GET", uuid3req, nil)

	// Make sure the 2nd k/v is now missing for previous versions.
	server.TestBadHTTP(t, "GET", key2req, nil)
	server.TestBadHTTP(t, "GET", uuid2req, nil)

	// Make a child
	if err = datastore.Commit(uuid3, "my 3rd commit msg", []string{"deleted 2nd k/v"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}
	uuid4, err := datastore.NewVersion(uuid3, "some child", "", nil)
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
	if string(returnValue) != uuid4val {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", key2, uuid4val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", key2, uuid4val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid3req, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on third version, key %q: expected %s, got %s\n", key2, uuid4val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid4req, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on fourth version, key %q: expected %s, got %s\n", key2, uuid4val, string(returnValue))
	}
}

func TestKeyvalueVersioning(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "versiontest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := `"some stuff"`
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Add 2nd k/v
	key2 := "my2ndkey"
	value2 := `"more good stuff"`
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Use the batch POST
	payloadSize := 1000
	var kvs proto.KeyValues
	kvs.Kvs = make([]*proto.KeyValue, 5)
	var payload [5][]byte
	for i := 0; i < 5; i++ {
		payload[i] = make([]byte, i*payloadSize+10)
		n, err := rand.Read(payload[i])
		if n != i*payloadSize+10 {
			t.Fatalf("couldn't create payload %d\n", i)
		}
		if err != nil {
			t.Fatalf("couldn't create payload %d: %v\n", i, err)
		}
		kvs.Kvs[i] = &proto.KeyValue{
			Key:   fmt.Sprintf("batchkey-%d", i),
			Value: payload[i],
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

	// Check some values using GET /keyvalues (json)
	getreq0 := fmt.Sprintf("%snode/%s/%s/keyvalues?json=true", server.WebAPIPath, uuid, data.DataName())
	expectedKeys := []string{key1, key2, "missing-key"}

	jsonKeys, err := json.Marshal(expectedKeys)
	if err != nil {
		t.Fatalf("couldn't parse expectedKeys\n")
	}

	returnValue = server.TestHTTP(t, "GET", getreq0, bytes.NewBuffer(jsonKeys))
	expectedJSON := fmt.Sprintf(`{%q:%s,%q:%s,"missing-key":{}}`, key1, value1, key2, value2)
	if string(returnValue) != expectedJSON {
		t.Errorf("Error on keyvalues JSON return: got %s, expected %s\n", string(returnValue), expectedJSON)
	}

	// Check values from batch POST using individual key gets
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("batchkey-%d", i)
		keyreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), k)
		returnValue := server.TestHTTP(t, "GET", keyreq, nil)
		if len(returnValue) != i*payloadSize+10 {
			t.Errorf("Expected batch POST key %q to have value with %d bytes, got %d instead\n", k, i+10, len(returnValue))
		}
		if !bytes.Equal(payload[i], returnValue) {
			t.Fatalf("bad response for key %q\n", k)
		}
	}

	// Check some values from batch POST using GET /keyvalues?jsontar=true
	getreq1 := fmt.Sprintf("%snode/%s/%s/keyvalues?jsontar=true", server.WebAPIPath, uuid, data.DataName())
	tardata := server.TestHTTP(t, "GET", getreq1, bytes.NewBufferString(`["batchkey-0","batchkey-1","batchkey-4","batchkey-1000"]`))
	tarbuf := bytes.NewBuffer(tardata)
	tr := tar.NewReader(tarbuf)
	expectedKeys = []string{"batchkey-0", "batchkey-1", "batchkey-4", "batchkey-1000"}
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
		var i int
		if _, err = fmt.Sscanf(hdr.Name, "batchkey-%d", &i); err != nil {
			t.Fatalf("error parsing tar file hdr %q: %v\n", hdr.Name, err)
		}
		var val bytes.Buffer
		if _, err := io.Copy(&val, tr); err != nil {
			t.Fatalf("error reading tar data: %v\n", err)
		}
		returnValue := val.Bytes()
		if i != 1000 {
			if len(returnValue) != i*payloadSize+10 {
				t.Errorf("Expected batch POST key %q to have value with %d bytes, got %d instead\n", hdr.Name, i*payloadSize+10, len(returnValue))
			}
			if !bytes.Equal(payload[i], returnValue) {
				t.Fatalf("bad response for key %q\n", hdr.Name)
			}
		} else {
			if len(returnValue) != 0 {
				t.Fatalf("expected 0 byte response for key %q, got %d bytes\n", hdr.Name, len(returnValue))
			}
		}
		keyNum++
	}
	if keyNum != 4 {
		t.Fatalf("Got %d keys when there should have been 4\n", keyNum)
	}
	tardata = server.TestHTTP(t, "GET", getreq1, bytes.NewBufferString(`["batchkey-3"]`))
	tarbuf = bytes.NewBuffer(tardata)
	tr = tar.NewReader(tarbuf)
	expectedKeys = []string{"batchkey-3"}
	keyNum = 0
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
		var i int
		if _, err = fmt.Sscanf(hdr.Name, "batchkey-%d", &i); err != nil {
			t.Fatalf("error parsing tar file hdr %q: %v\n", hdr.Name, err)
		}
		var val bytes.Buffer
		if _, err := io.Copy(&val, tr); err != nil {
			t.Fatalf("error reading tar data: %v\n", err)
		}
		returnValue := val.Bytes()
		if len(returnValue) != i*payloadSize+10 {
			t.Errorf("Expected batch POST key %q to have value with %d bytes, got %d instead\n", hdr.Name, i*payloadSize+10, len(returnValue))
		}
		if !bytes.Equal(payload[i], returnValue) {
			t.Fatalf("bad response for key %q\n", hdr.Name)
		}
		keyNum++
	}
	if keyNum != 1 {
		t.Fatalf("Got %d keys when there should have been 1\n", keyNum)
	}

	// Check some values from batch POST using GET /keyvalues (protobuf3)
	getreq2 := fmt.Sprintf("%snode/%s/%s/keyvalues", server.WebAPIPath, uuid, data.DataName())
	expectedKeys = []string{"batchkey-1", "batchkey-2", "batchkey-3", "batchkey-1000"}
	pbufKeys := proto.Keys{
		Keys: expectedKeys,
	}
	keysSerialization, err := pb.Marshal(&pbufKeys)
	if err != nil {
		t.Fatalf("couldn't serialized protobuf keys: %v\n", err)
	}
	keyvaluesSerialization := server.TestHTTP(t, "GET", getreq2, bytes.NewBuffer(keysSerialization))
	var pbKVs proto.KeyValues
	if err := pb.Unmarshal(keyvaluesSerialization, &pbKVs); err != nil {
		t.Fatalf("couldn't unmarshal keyvalues protobuf: %v\n", err)
	}
	if len(pbKVs.Kvs) != 4 {
		t.Fatalf("expected 4 kv pairs returned, got %d\n", len(pbKVs.Kvs))
	}
	for keyNum, kv := range pbKVs.Kvs {
		if kv.Key != expectedKeys[keyNum] {
			t.Fatalf("expected for key %d %q, got %q", keyNum, expectedKeys[keyNum], kv.Key)
		}
		var i int
		if _, err = fmt.Sscanf(kv.Key, "batchkey-%d", &i); err != nil {
			t.Fatalf("error parsing key %d %q: %v\n", keyNum, kv.Key, err)
		}
		if i != 1000 {
			if len(kv.Value) != i*payloadSize+10 {
				t.Errorf("Expected batch POST key %q to have value with %d bytes, got %d instead\n", kv.Key, i*payloadSize+10, len(kv.Value))
			}
			if !bytes.Equal(payload[i], kv.Value) {
				t.Fatalf("bad response for key %q\n", kv.Key)
			}
		} else {
			if len(kv.Value) != 0 {
				t.Fatalf("expected 0 byte value for key %q but got %d bytes instead\n", kv.Key, len(kv.Value))
			}
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
	uuid4, err := datastore.NewVersion(uuid3, "some child", "", nil)
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
	uuid5, err := datastore.NewVersion(uuid2, "some child", "some child", nil)
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

	// Manually fix conflict: Branch, and then delete 2nd k/v and commit.
	uuid6, err := datastore.NewVersion(uuid5, "some child", "", nil)
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

	// Apply the automatic conflict resolution using ordering.
	resolveNote := fmt.Sprintf(`{"data":["versiontest"],"parents":[%q,%q],"note":"automatic resolved merge"}`, uuid5, uuid4)
	resolveReq := fmt.Sprintf("%srepo/%s/resolve", server.WebAPIPath, uuid4)
	returnValue = server.TestHTTP(t, "POST", resolveReq, bytes.NewBufferString(resolveNote))
	resolveResp := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(returnValue, &resolveResp); err != nil {
		t.Fatalf("Can't parse return of resolve request: %s\n", string(returnValue))
	}

	// We should now see the uuid5 version of the 2nd k/v in the returned merged node.
	childreq = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, resolveResp.Child, data.DataName(), key2)
	returnValue = server.TestHTTP(t, "GET", childreq, nil)
	if string(returnValue) != uuid5val {
		t.Errorf("Error on auto merged child, key %q: expected %q, got %q\n", key2, uuid5val, string(returnValue))
	}

	// Introduce a child off root but don't add 2nd k/v to it.
	uuid7, err := datastore.NewVersion(uuid, "2nd child off root", "2nd child off root", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid, err)
	}
	if err = datastore.Commit(uuid7, "useless node", []string{"we modified nothing!"}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid7, err)
	}

	// Now merge the previously merged node with the newly created "blank" child off root.
	if err = datastore.Commit(goodChild, "this was a good merge", []string{}); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", goodChild, err)
	}
	merge2, err := datastore.Merge([]dvid.UUID{goodChild, uuid7}, "merging a useless path", datastore.MergeConflictFree)
	if err != nil {
		t.Errorf("Error doing merge: %v\n", err)
	}
	merge3, err := datastore.Merge([]dvid.UUID{uuid7, goodChild}, "merging a useless path in reverse order", datastore.MergeConflictFree)
	if err != nil {
		t.Errorf("Error doing merge: %v\n", err)
	}

	// We should still be conflict free since 2nd key in left parent path will take precedent over shared 2nd key
	// in root.  This tests our invalidation of ancestors.
	toughreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, merge2, data.DataName(), key2)
	returnValue = server.TestHTTP(t, "GET", toughreq, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on merged child, key %q: expected %q, got %q\n", key2, uuid4val, string(returnValue))
	}
	toughreq = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, merge3, data.DataName(), key2)
	returnValue = server.TestHTTP(t, "GET", toughreq, nil)
	if string(returnValue) != uuid4val {
		t.Errorf("Error on merged child, key %q: expected %q, got %q\n", key2, uuid4val, string(returnValue))
	}

	// Create a new keyvalue data instance at an interior non-root node.
	uuid8, err := datastore.NewVersion(uuid7, "open leaf child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off %s: %v\n", uuid7, err)
	}
	dataservice, err = datastore.NewData(uuid8, kvtype, "leafdata", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	leafdata, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
	}

	// PUT a value
	key1req = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid8, leafdata.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Add 2nd k/v
	key2req = fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid8, leafdata.DataName(), key2)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Change the 2nd k/v
	server.TestHTTP(t, "POST", key2req, strings.NewReader(uuid2val))

	// Check the values
	returnValue = server.TestHTTP(t, "GET", key1req, nil)
	if string(returnValue) != value1 {
		t.Errorf("Key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}

	// Change the name of the interior data.
	if err = datastore.RenameData(uuid8, "leafdata", "versiontest", "foobar"); err == nil {
		t.Fatalf("Should have been prevented from renaming data 'leafdata' to existing data 'versiontest'!\n")
	}
	if err = datastore.RenameData(uuid8, "leafdata", "renamedData", "foobar"); err != nil {
		t.Fatalf("Error renaming leafdata: %v\n", err)
	}

	// Check the values
	key1req = fmt.Sprintf("%snode/%s/renamedData/key/%s", server.WebAPIPath, uuid8, key1)
	key2req = fmt.Sprintf("%snode/%s/renamedData/key/%s", server.WebAPIPath, uuid8, key2)
	returnValue = server.TestHTTP(t, "GET", key1req, nil)
	if string(returnValue) != value1 {
		t.Errorf("Key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", key2req, nil)
	if string(returnValue) != uuid2val {
		t.Errorf("Key %q: expected %s, got %s\n", key2, uuid2val, string(returnValue))
	}
}

// Test added after error in getting two paths to the same ancestor k/v after merge.
func TestDiamondGetOnMerge(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, kvtype, "mergetest", config)
	if err != nil {
		t.Fatalf("Error creating new keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not keyvalue.Data\n")
	}

	// PUT a value
	key1 := "mykey"
	value1 := "some stuff"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), key1)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	if err = datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Errorf("Unable to lock root node %s: %v\n", uuid, err)
	}
	uuid2, err := datastore.NewVersion(uuid, "first child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create 1st child off root %s: %v\n", uuid, err)
	}
	if err = datastore.Commit(uuid2, "first child", nil); err != nil {
		t.Errorf("Unable to commit node %s: %v\n", uuid2, err)
	}
	uuid3, err := datastore.NewVersion(uuid, "second child", "newbranch", nil)
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

/*
TODO -- Complete when mutation log access added, so we can check mutation is logged and test blobstore
		fetch with reference.

func TestBlobstoreMutationLog(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "keyvalue", "mykv", config)

	key := "mykey"
	value := "some stuff"
	url := fmt.Sprintf("%snode/%s/mykv/key/%s", server.WebAPIPath, uuid, key)
	server.TestHTTP(t, "POST", url, strings.NewReader(value))
}

*/
