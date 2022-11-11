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
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
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

var testJsonSchema = `
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": true,
    "default": {},
    "required": ["bodyid"],
    "properties": {
        "bodyid": {
            "description": "the body id",
            "type": "integer"
        },
        "group": {
            "description": "a group id",
            "type": "integer"
        },
        "status": {
            "description": "neuron status",
            "type": "string"
        },
        "position": {
            "description": "a coordinate somewhere on the body",
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 3,
            "maxItems": 3
        },
        "soma_position": {
            "description": "a coordinate in the neuron soma",
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 3,
            "maxItems": 3
        },
        "tosoma_position": {
            "description": "a coordinate on the neuron's cell body fiber, as near to the soma as possible",
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 3,
            "maxItems": 3
        },
        "root_position": {
            "description": "some 'root' position for the neuron when the soma and 'tosoma' aren't segmented.",
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 3,
            "maxItems": 3
        }
    }
}`

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

func checkBasicAndAll(t *testing.T, basicJSON string, allJSON []byte, user string) {
	var basic NeuronJSON
	if err := json.Unmarshal([]byte(basicJSON), &basic); err != nil {
		t.Fatalf("Couldn't unmarshal basic JSON: %s\n", basicJSON)
	}
	var allList ListNeuronJSON
	if err := json.Unmarshal(allJSON, &allList); err != nil {
		t.Fatalf("Couldn't unmarshal all JSON: %s\n", string(allJSON))
	}
	all := allList[0]
	for field, value := range basic {
		if _, found := all[field+"_user"]; !found {
			t.Fatalf("Couldn't find %q field\n", field+"_user")
		}
		if _, found := all[field+"_time"]; !found {
			t.Fatalf("Couldn't find %q field\n", field+"_time")
		}
		if all[field+"_user"] != user {
			t.Fatalf("%q field got %q, not expected %q\n", field+"_user", all[field+"_user"], user)
		}
		if _, found := all[field]; !found {
			t.Fatalf("Couldn't find %q field\n", field)
		}
		typeAll := reflect.TypeOf(all[field])
		typeBasic := reflect.TypeOf(value)
		if typeAll != typeBasic {
			t.Fatalf("%q field has different types %q vs %q: %v != %v\n", field, typeBasic, typeAll, value, all[field])
		}
		if !reflect.DeepEqual(all[field], value) {
			t.Fatalf("%q field got %q (type %s), not expected %q (type %s)\n", field, all[field], typeAll, value, typeBasic)
		}
	}
}

// returns []byte of updated JSON
func updatedJSONBytes(t *testing.T, origJSON, newJSON string) []byte {
	var vx, vy NeuronJSON
	if err := json.Unmarshal([]byte(origJSON), &vx); err != nil {
		t.Fatalf("can't unmarshal origJSON: %v\n", err)
	}
	if err := json.Unmarshal([]byte(newJSON), &vy); err != nil {
		t.Fatalf("can't unmarshal newJSON: %v\n", err)
	}
	for k, v := range vy {
		if _, found := vx[k]; !found {
			vx[k] = v
		}
	}
	updatedJSON, err := json.Marshal(vx)
	if err != nil {
		t.Fatalf("Couldn't serialize updated JSON (%v): %v\n", vx, err)
	}
	return updatedJSON
}

// equalObjectJSON compares two []byte of JSON objects, ignoring ordering.
func equalObjectJSON(x, y []byte, showFields Fields) bool {
	var vx, vy NeuronJSON
	if err := json.Unmarshal(x, &vx); err != nil {
		return false
	}
	if err := json.Unmarshal(y, &vy); err != nil {
		return false
	}
	return reflect.DeepEqual(removeReservedFields(vx, showFields), removeReservedFields(vy, showFields))
}

// equalListJSON compares two []byte of JSON lists.
func equalListJSON(x, y []byte, showFields Fields) bool {
	var vx, vy []NeuronJSON
	if err := json.Unmarshal(x, &vx); err != nil {
		return false
	}
	if err := json.Unmarshal(y, &vy); err != nil {
		return false
	}
	if len(vx) != len(vy) {
		return false
	}
	if len(vx) == 0 {
		return true // both have 0 objects.
	}
	vx[0] = removeReservedFields(vx[0], showFields)
	vy[0] = removeReservedFields(vy[0], showFields)
	if len(vx) == 1 && reflect.DeepEqual(vx[0], vy[0]) {
		return true
	}
	if len(vx) == 2 {
		vx[1] = removeReservedFields(vx[1], showFields)
		vy[1] = removeReservedFields(vy[1], showFields)
		if reflect.DeepEqual(vx[0], vy[0]) && reflect.DeepEqual(vx[1], vy[1]) {
			return true
		}
		return reflect.DeepEqual(vx[0], vy[1]) && reflect.DeepEqual(vx[1], vy[0])
	}
	return false
}

func TestFields(t *testing.T) {
	foo := NeuronJSON{
		"foo":      "foo value",
		"foo_user": "foo_user value",
		"foo_time": "foo_time value",
		"moo":      "moo value",
		"moo_user": "moo_user value",
		"moo_time": "moo_time value",
	}
	testData := make(NeuronJSON, len(foo))
	for k, v := range foo {
		testData[k] = v
	}
	out := removeReservedFields(testData, ShowAll)
	if !reflect.DeepEqual(out, foo) {
		t.Fatalf("Expected %v, got %v", foo, testData)
	}

	expected := NeuronJSON{
		"foo":      "foo value",
		"foo_user": "foo_user value",
		"moo":      "moo value",
		"moo_user": "moo_user value",
	}
	out = removeReservedFields(testData, ShowUsers)
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("Expected %v\ngot %v\n", expected, testData)
	}

	for k, v := range foo {
		testData[k] = v
	}
	expected = NeuronJSON{
		"foo":      "foo value",
		"foo_time": "foo_time value",
		"moo":      "moo value",
		"moo_time": "moo_time value",
	}
	out = removeReservedFields(testData, ShowTime)
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("Expected %v\ngot %v\n", expected, testData)
	}

	for k, v := range foo {
		testData[k] = v
	}
	expected = NeuronJSON{
		"foo": "foo value",
		"moo": "moo value",
	}
	out = removeReservedFields(testData, ShowBasic)
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("Expected %v\ngot %v\n", expected, testData)
	}
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

	if err = kvdata.PutData(ctx, keyStr, value, nil, true); err != nil {
		t.Errorf("Could not put neuronjson data: %v\n", err)
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr, ShowBasic)
	if err != nil {
		t.Fatalf("Could not get neuronjson data: %v\n", err)
	}
	if !found {
		t.Fatalf("Could not find put neuronjson\n")
	}
	if !equalObjectJSON(value, retrieved, ShowUsers) {
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

func TestMetadataSupport(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	server.CreateTestInstance(t, uuid, "neuronjson", "neurons", dvid.Config{})

	// Test handling of different metadata types.
	reqJSONSchema := fmt.Sprintf("%snode/%s/neurons/json_schema?u=frank", server.WebAPIPath, uuid)
	resp := server.TestHTTPResponse(t, "POST", reqJSONSchema, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", reqJSONSchema, resp.Code, resp.Body.String())
	}
	returnValue := server.TestHTTP(t, "GET", reqJSONSchema, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Errorf("Error in getting json schema: got %s\n", string(returnValue))
	}

	reqSchema1 := fmt.Sprintf("%snode/%s/neurons/schema?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema1, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", reqSchema1, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema1, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Errorf("Error in getting json schema: got %s\n", string(returnValue))
	}
	// -- check legacy /key/schema works for backwards-compatibility
	reqSchema2 := fmt.Sprintf("%snode/%s/neurons/key/schema?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema2, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema2, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Errorf("Error in getting json schema: got %s\n", string(returnValue))
	}
	resp = server.TestHTTPResponse(t, "DELETE", reqSchema2, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("DELETE on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	resp = server.TestHTTPResponse(t, "GET", reqSchema2, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("GET on %s returned %d, not 404: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}

	reqSchema1 = fmt.Sprintf("%snode/%s/neurons/schema_batch?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema1, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", reqSchema1, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema1, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Errorf("Error in getting json schema: got %s\n", string(returnValue))
	}
	// -- check legacy /key/schema works for backwards-compatibility
	reqSchema2 = fmt.Sprintf("%snode/%s/neurons/key/schema_batch?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema2, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema2, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Errorf("Error in getting json schema: got %s\n", string(returnValue))
	}
	resp = server.TestHTTPResponse(t, "DELETE", reqSchema2, nil)
	if resp.Code != http.StatusOK {
		t.Errorf("DELETE on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	resp = server.TestHTTPResponse(t, "GET", reqSchema2, nil)
	if resp.Code != http.StatusNotFound {
		t.Errorf("GET on %s returned %d, not 404: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
}

func TestValidation(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	name := dvid.InstanceName("neurons")
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, jsontype, name, config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not neuronjson.Data\n")
	}

	// Before json schema is installed, bad values should be permitted
	key1 := "1000"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, uuid, data.DataName(), key1)
	badValue := `
	{
		"group": 130911,
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp := server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}
	badValue = `
	{
		"bodyid": 13087493,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23]
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}

	// Store JSON Schema
	schemaReq := fmt.Sprintf("%snode/%s/%s/json_schema?u=frank", server.WebAPIPath, uuid, data.DataName())
	resp = server.TestHTTPResponse(t, "POST", schemaReq, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", schemaReq, resp.Code, resp.Body.String())
	}

	// PUT a value that should conform to schema set.
	okValue := `
	{
		"bodyid": 13087493,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23,32],
		"soma_position": [30, 30, 30],
		"tosoma_position": [14, 1000, 1000],
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(okValue))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}
	okValue = `
	{
		"bodyid": 13087493,
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(okValue))
	if resp.Code != http.StatusOK {
		t.Errorf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}

	// Test values that should not pass validation
	badValue = `
	{
		"group": 130911,
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Errorf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
	}
	badValue = `
	{
		"bodyid": 13087493,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23]
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Errorf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
	}
	badValue = `
	{
		"group": 130911,
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}` // missing bodyid
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Errorf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
	}
	badValue = `
	{
		"bodyid": 13087493,
		"group": "130911",
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}` // group is string, not int
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Errorf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
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
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, uuid, data.DataName(), key1)
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
	if !equalObjectJSON(returnValue, []byte(value1), ShowBasic) {
		t.Errorf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Expect error if no key used.
	badrequest := fmt.Sprintf("%snode/%s/%s/key/", server.WebAPIPath, uuid, data.DataName())
	server.TestBadHTTP(t, "GET", badrequest, nil)

	// Add 2nd k/v
	key2 := "2000"
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s?u=martha", server.WebAPIPath, uuid, data.DataName(), key2)

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
	key3req := fmt.Sprintf("%snode/%s/%s/key/%s?u=shawna", server.WebAPIPath, uuid, data.DataName(), key3)
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

	// Check JSON keyvalues request using both list of ids and strings, latter is for keyvalue datatype compatibility.
	jsonIds := `[2000, 3000]`
	kvreq := fmt.Sprintf("%snode/%s/%s/keyvalues?json=true", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "GET", kvreq, strings.NewReader(jsonIds))

	expectedValue := []byte(`{"2000":` + value2 + `,"3000":` + value3 + "}")
	if !equalObjectJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad keyvalues request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	jsonIds = `["2000", "3000"]`
	returnValue = server.TestHTTP(t, "GET", kvreq, strings.NewReader(jsonIds))

	if !equalObjectJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad keyvalues request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check query
	query := `{"a string": ["moo", "goo"]}`
	queryreq := fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "," + value3 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": ["moo", "goo"], "a number": 2345}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query?show=all", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))
	checkBasicAndAll(t, value2, returnValue, "martha")

	query = `{"a string": ["moo", "goo"], "a number": [2345, 3456]}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "," + value3 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"unused field": "foo"}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "moo", "unused field": "foo"}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check regex query
	query = `{"a string": "re/^(f|m)oo"}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value1 + "," + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "re/^(f|m)oo", "a list": "mom"}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "re/^(f|m)oo", "a list": ["re/.*x", "re/om"]}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Errorf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check if keys are re-POSTed using default update or replace=true.
	value3mod := `{"a string": "goo modified", "a 2nd list": [26]}`
	key3modreq := fmt.Sprintf("%snode/%s/%s/key/%s?u=bill&show=all", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3modreq, strings.NewReader(value3mod))

	returnValue = server.TestHTTP(t, "GET", key3modreq, nil)

	expectedValue = []byte(`{"a string": "goo modified", "a number": 3456, "a list": [23], "a 2nd list": [26]}`)
	var expectedJSON NeuronJSON
	if err := json.Unmarshal(expectedValue, &expectedJSON); err != nil {
		t.Fatalf("Couldn't unmarshal expected basic JSON: %s\n", expectedJSON)
	}
	var responseJSON NeuronJSON
	if err := json.Unmarshal(returnValue, &responseJSON); err != nil {
		t.Fatalf("Couldn't unmarshal response JSON: %s\n", string(returnValue))
	}
	if value, found := responseJSON["a string"]; !found || value != "goo modified" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a string_user"]; !found || value != "bill" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a number"]; !found || !reflect.DeepEqual(value, uint64(3456)) {
		t.Fatalf("Bad response for number (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a number_user"]; !found || value != "shawna" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a list"]; !found || !reflect.DeepEqual(value, []int64{23}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a list_user"]; !found || value != "shawna" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a 2nd list"]; !found || !reflect.DeepEqual(value, []int64{26}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a 2nd list_user"]; !found || value != "bill" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}

	// Check if we can keep old _user and _time while changing value (admin mods)
	value3modA := fmt.Sprintf(`{"a string": "goo modified", "a string_user": "bill", "a string_time": "%s", "a 2nd list": [26]}`, responseJSON["a string_time"])
	key3modAreq := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank&show=user", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3modAreq, strings.NewReader(value3modA))

	returnValue = server.TestHTTP(t, "GET", key3modAreq, nil)

	expectedValue = []byte(fmt.Sprintf(`{"a string": "goo modified", "a string_user": "bill", "a string_time": "%s", "a number": 3456, "a list": [23], "a 2nd list": [26]}`, responseJSON["a string_time"]))
	if err := json.Unmarshal(expectedValue, &expectedJSON); err != nil {
		t.Fatalf("Couldn't unmarshal expected basic JSON: %s\n", expectedJSON)
	}
	if err := json.Unmarshal(returnValue, &responseJSON); err != nil {
		t.Fatalf("Couldn't unmarshal response JSON: %s\n", string(returnValue))
	}
	if value, found := responseJSON["a string"]; !found || value != "goo modified" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a string_user"]; !found || value != "bill" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a number"]; !found || !reflect.DeepEqual(value, uint64(3456)) {
		t.Fatalf("Bad response for number (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a number_user"]; !found || value != "shawna" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a list"]; !found || !reflect.DeepEqual(value, []int64{23}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a list_user"]; !found || value != "shawna" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a 2nd list"]; !found || !reflect.DeepEqual(value, []int64{26}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a 2nd list_user"]; !found || value != "frank" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}

	// Check if keys are re-POSTed using replace=true.
	value3mod = `{"a string": "goo replaced", "only list": [1, 2]}`
	key3modreq = fmt.Sprintf("%snode/%s/%s/key/%s?u=sandra&show=user&replace=true", server.WebAPIPath, uuid, data.DataName(), key3)
	server.TestHTTP(t, "POST", key3modreq, strings.NewReader(value3mod))

	returnValue = server.TestHTTP(t, "GET", key3modreq, nil)

	expectedValue = []byte(value3mod)
	if err := json.Unmarshal(expectedValue, &expectedJSON); err != nil {
		t.Fatalf("Couldn't unmarshal expected basic JSON: %s\n", expectedJSON)
	}
	if err := json.Unmarshal(returnValue, &responseJSON); err != nil {
		t.Fatalf("Couldn't unmarshal response JSON: %s\n", string(returnValue))
	}
	if len(responseJSON) != 4 {
		t.Fatalf("Expected only 4 fields in response after replace, got: %v\n", responseJSON)
	}
	if value, found := responseJSON["a string"]; !found || value != "goo replaced" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["only list"]; !found || !reflect.DeepEqual(value, []int64{1, 2}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a string_user"]; !found || value != "sandra" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["only list_user"]; !found || value != "sandra" {
		t.Fatalf("Bad response: %v\n", responseJSON)
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
	if !equalObjectJSON(returnValue, []byte(testData[0].val), ShowBasic) {
		t.Errorf("Error on key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Add 2nd k/v
	key2req := fmt.Sprintf("%snode/%s/unversiontest/key/%s", server.WebAPIPath, uuid, testData[1].key)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(testData[1].val))

	// Test
	rangeReq := fmt.Sprintf("%snode/%s/unversiontest/keyrangevalues/0/2001", server.WebAPIPath, uuid)
	expectedJSON := fmt.Sprintf(`{"%s":%s,"%s":%s}`, testData[0].key, testData[0].val, testData[1].key, testData[1].val)

	returnValue = server.TestHTTP(t, "GET", rangeReq+"?json=true", nil)
	if !equalObjectJSON(returnValue, []byte(expectedJSON), ShowBasic) {
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
	var kvs proto.KeyValues
	kvs.Kvs = make([]*proto.KeyValue, 4)
	for i := 0; i < 4; i++ {
		kvs.Kvs[i] = &proto.KeyValue{
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
	uuid2req := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, uuid2, data.DataName(), testData[1].key)
	server.TestHTTP(t, "POST", uuid2req, strings.NewReader(uuid2val))

	// Get the first version value
	returnValue := server.TestHTTP(t, "GET", keyreq[0], nil)
	if !equalObjectJSON(returnValue, []byte(testData[0].val), ShowBasic) {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Get the second version value
	expected2val := updatedJSONBytes(t, testData[1].val, uuid2val)
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, expected2val, ShowBasic) {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, uuid2val, string(returnValue))
	}
	// returnValue = server.TestHTTP(t, "GET", uuid2req+"&show=user", nil)
	// if !equalObjectJSON(returnValue, []byte("foo" /*uuid2val*/)) {
	// 	t.Errorf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, uuid2val, string(returnValue))
	// }

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
		if !equalObjectJSON(returnValue, []byte(testData[i].val), ShowBasic) {
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
		if !equalObjectJSON(returnValue, []byte(expectedVals[keyNum]), ShowBasic) {
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
	if len(pbKVs.Kvs) != 3 {
		t.Fatalf("expected 3 kv pairs returned, got %d\n", len(pbKVs.Kvs))
	}
	for keyNum, kv := range pbKVs.Kvs {
		if kv.Key != expectedKeys[keyNum] {
			t.Fatalf("expected for key %d %q, got %q", keyNum, expectedKeys[keyNum], kv.Key)
		}
		if !equalObjectJSON(kv.Value, []byte(expectedVals[keyNum]), ShowBasic) {
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
	if !equalObjectJSON(returnValue, []byte(testData[1].val), ShowBasic) {
		t.Errorf("Error on first version, key %q: expected %s, got %s\n", testData[1].key, testData[1].val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, expected2val, ShowBasic) {
		t.Errorf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, expected2val, string(returnValue))
	}
}
