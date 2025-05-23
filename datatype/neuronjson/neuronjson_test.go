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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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
        },
		"manc_bodyid": {
			"description": "the matching body id in MANC",
			"type": [
				"integer",
				"null"
			]
		},
		"manc_type": {
			"description": "the type of matching body in MANC",
			"type": [
				"string",
				"null"
			]
		},
		"manc_group": {
			"description": "the matching group id in MANC",
			"type": [
				"integer",
				"null"
			]
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
	if len(allList) != 1 {
		t.Fatalf("Can't check allJSON without 1 element, received:\n%s\n", string(allJSON))
	}
	all := allList[0]
	for field, value := range basic {
		if field == "bodyid" {
			continue
		}
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

// equalJSONString compares two JSON strings, ignoring ordering but removing time fields.
func equalJSONString(x, y string) bool {
	var vx, vy map[string]ListNeuronJSON
	if err := json.Unmarshal([]byte(x), &vx); err != nil {
		return false
	}
	o1 := make(map[string]ListNeuronJSON, len(vx))
	for uuid, jsonList := range vx {
		o1[uuid] = jsonList.makeTimeless()
	}
	if err := json.Unmarshal([]byte(y), &vy); err != nil {
		return false
	}
	o2 := make(map[string]ListNeuronJSON, len(vy))
	for uuid, jsonList := range vy {
		o2[uuid] = jsonList.makeTimeless()
	}
	return reflect.DeepEqual(o1, o2)
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
	for i := range vx {
		vx[i] = removeReservedFields(vx[i], showFields)
	}
	for i := range vy {
		vy[i] = removeReservedFields(vy[i], showFields)
	}
	dvid.Infof("equalListJSON: vx = %v\n", vx)
	dvid.Infof("equalListJSON: vy = %v\n", vy)
	return reflect.DeepEqual(vx, vy)
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
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	kv1, ok := dataservice1.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance 1 is not neuronjson.Data\n")
	}
	if kv1.DataName() != "instance1" {
		t.Fatalf("New neuronjson data instance name set incorrectly: %q != %q\n",
			kv1.DataName(), "instance1")
	}

	dataservice2, err := datastore.NewData(uuid, jsontype, "instance2", config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	kv2, ok := dataservice2.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance 2 is not neuronjson.Data\n")
	}

	if kv1.InstanceID() == kv2.InstanceID() {
		t.Fatalf("Instance IDs should be different: %d == %d\n",
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
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not neuronjson.Data\n")
	}

	ctx := datastore.NewVersionedCtx(dataservice, versionID)
	ctx.User = "tester"

	keyStr := "1234"
	value := []byte(`{"bodyid": 1234, "a string": "foo", "a number": 1234, "a list": [1, 2, 3]}`)

	if err = kvdata.PutData(ctx, keyStr, value, nil, true); err != nil {
		t.Fatalf("Could not put neuronjson data: %v\n", err)
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr, nil, ShowBasic)
	if err != nil {
		t.Fatalf("Could not get neuronjson data: %v\n", err)
	}
	if !found {
		t.Fatalf("Could not find put neuronjson\n")
	}
	if !equalObjectJSON(value, retrieved, ShowUsers) {
		t.Fatalf("neuronjson retrieved %q != put %q\n", string(retrieved), string(value))
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
		t.Fatalf("Unable to create neuronjson instance: %v\n", err)
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast neuronjson data service into neuronjson.Data\n")
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
		t.Fatalf("Returned new data instance 2 is not neuronjson.Data\n")
	}
	if !oldData.Equals(kvdata2) {
		t.Fatalf("Expected %v, got %v\n", oldData, *kvdata2)
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
		t.Fatalf("POST on %s returned %d, not 200: %s\n", reqJSONSchema, resp.Code, resp.Body.String())
	}
	returnValue := server.TestHTTP(t, "GET", reqJSONSchema, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Fatalf("Error in getting json schema: got %s\n", string(returnValue))
	}

	reqSchema1 := fmt.Sprintf("%snode/%s/neurons/schema?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema1, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", reqSchema1, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema1, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Fatalf("Error in getting json schema: got %s\n", string(returnValue))
	}
	// -- check legacy /key/schema works for backwards-compatibility
	reqSchema2 := fmt.Sprintf("%snode/%s/neurons/key/schema?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema2, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema2, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Fatalf("Error in getting json schema: got %s\n", string(returnValue))
	}
	resp = server.TestHTTPResponse(t, "DELETE", reqSchema2, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("DELETE on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	resp = server.TestHTTPResponse(t, "GET", reqSchema2, nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("GET on %s returned %d, not 404: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}

	reqSchema1 = fmt.Sprintf("%snode/%s/neurons/schema_batch?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema1, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", reqSchema1, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema1, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Fatalf("Error in getting json schema: got %s\n", string(returnValue))
	}
	// -- check legacy /key/schema works for backwards-compatibility
	reqSchema2 = fmt.Sprintf("%snode/%s/neurons/key/schema_batch?u=frank", server.WebAPIPath, uuid)
	resp = server.TestHTTPResponse(t, "POST", reqSchema2, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	returnValue = server.TestHTTP(t, "GET", reqSchema2, nil)
	if !equalObjectJSON(returnValue, []byte(testJsonSchema), ShowAll) {
		t.Fatalf("Error in getting json schema: got %s\n", string(returnValue))
	}
	resp = server.TestHTTPResponse(t, "DELETE", reqSchema2, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("DELETE on %s returned %d, not 200: %s\n", reqSchema2, resp.Code, resp.Body.String())
	}
	resp = server.TestHTTPResponse(t, "GET", reqSchema2, nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("GET on %s returned %d, not 404: %s\n", reqSchema2, resp.Code, resp.Body.String())
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

	// bodyid field must be present and must match key
	key1 := "1000"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, uuid, data.DataName(), key1)
	badValue := `
	{
		"bodyid": 13087493,
		"group": 130911,
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp := server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Fatalf("POST on %s returned %d, not error for bad bodyid\n", key1req, resp.Code)
	}

	// Before json schema is installed, bad values should be permitted
	badValue = `
	{
		"bodyid": 1000,
		"group": 130911,
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}
	badValue = `
	{
		"bodyid": 1000,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23]
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}

	// Store JSON Schema
	schemaReq := fmt.Sprintf("%snode/%s/%s/json_schema?u=frank", server.WebAPIPath, uuid, data.DataName())
	resp = server.TestHTTPResponse(t, "POST", schemaReq, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", schemaReq, resp.Code, resp.Body.String())
	}
	resp = server.TestHTTPResponse(t, "GET", schemaReq, strings.NewReader(testJsonSchema))
	if resp.Code != http.StatusOK {
		t.Fatalf("GET on %s returned %d, not 200: %s\n", schemaReq, resp.Code, resp.Body.String())
	}
	dvid.Infof("Schema: %s\n", resp.Body.String())

	// PUT a value that should conform to schema set.
	okValue := `
	{
		"bodyid": 1000,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23,32],
		"soma_position": [30, 30, 30],
		"tosoma_position": [14, 1000, 1000],
		"root_position": [15, 15, 15],
		"something_else": "foo",
		"manc_bodyid": 12345,
		"manc_group": 12345
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(okValue))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
	}
	okValue = `
	{
		"bodyid": 1000,
		"root_position": [15, 15, 15],
		"something_else": "foo"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(okValue))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s returned %d, not 200: %s\n", key1req, resp.Code, resp.Body.String())
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
		t.Fatalf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
	}
	badValue = `
	{
		"bodyid": 1000,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23]
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code == http.StatusOK {
		t.Fatalf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
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
		t.Fatalf("POST on %s returned 200 when it shouldn't have been validated\n", key1req)
	}
	// test auto-convert from string to int using multiple fields
	badValue = `
	{
		"bodyid": 1000,
		"group": "130911",
		"status": "Anchor",
		"root_position": [15, 15, 15],
		"something_else": "foo",
		"manc_bodyid": "12345",
		"manc_group": "12345"
	}`
	resp = server.TestHTTPResponse(t, "POST", key1req, strings.NewReader(badValue))
	if resp.Code != http.StatusOK {
		t.Fatalf("POST on %s should have been ok after auto-conversion\n", key1req)
	}
	dvid.Infof("resp: %s\n", resp.Body.String())

	// Make sure the autoconversion actually worked.
	expectedValue := `
	{
		"bodyid": 1000,
		"group": 130911,
		"status": "Anchor",
		"position": [23,23,32],
		"soma_position": [30, 30, 30],
		"tosoma_position": [14, 1000, 1000],
		"root_position": [15, 15, 15],
		"something_else": "foo",
		"manc_bodyid": 12345,
		"manc_group": 12345
	}`
	req := fmt.Sprintf("%snode/%s/%s/key/1000?u=frank", server.WebAPIPath, uuid, data.DataName())
	returnValue := server.TestHTTP(t, "GET", req, nil)
	if !equalObjectJSON(returnValue, []byte(expectedValue), ShowBasic) {
		t.Fatalf("Error on autoconversion: expected %s\ngot %s\n", expectedValue, string(returnValue))
	}
}

func TestKeyvalueUser(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	name := dvid.InstanceName("annotations")

	config := dvid.NewConfig()
	_, err := datastore.NewData(uuid, jsontype, name, config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}

	// PUT a kv
	value := `{"bodyid": 1000, "key1": "foo"}`
	req := fmt.Sprintf("%snode/%s/%s/key/1000?u=frank", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", req, strings.NewReader(value))

	// Change key1 value but force a user.
	value = `{"bodyid": 1000, "key1": "moo", "key1_user": "frank"}`
	req = fmt.Sprintf("%snode/%s/%s/key/1000?u=admin", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", req, strings.NewReader(value))

	// Verify the key1_user is now "someone" and not "frank"
	req = fmt.Sprintf("%snode/%s/%s/key/1000?show=user", server.WebAPIPath, uuid, name)
	returnValue := server.TestHTTP(t, "GET", req, nil)
	if !equalObjectJSON(returnValue, []byte(value), ShowUsers) {
		t.Fatalf("Error: expected %s, got %s\n", value, string(returnValue))
	}

	// Allow deletion of fields via null for non-replace POST
	value = `{"bodyid": 1000, "key1": null, "key1_user": null}`
	req = fmt.Sprintf("%snode/%s/%s/key/1000?u=deleter", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", req, strings.NewReader(value))

	// Verify the both key1 and key1_user fields are deleted
	req = fmt.Sprintf("%snode/%s/%s/key/1000?show=user", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", req, nil)
	if !equalObjectJSON(returnValue, []byte(`{"bodyid": 1000}`), ShowUsers) {
		t.Fatalf("Error: expected %s, got %s\n", `{"bodyid": 1000}`, string(returnValue))
	}

	// PUT back the kv
	value = `{"bodyid": 1000, "key1": "foo"}`
	req = fmt.Sprintf("%snode/%s/%s/key/1000?u=frank", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", req, strings.NewReader(value))

	// Just delete the main field not the _user as well.
	value = `{"bodyid": 1000, "key1": null}`
	req = fmt.Sprintf("%snode/%s/%s/key/1000?u=deleter", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", req, strings.NewReader(value))

	// Verify we deleted key1 but also recorded the deleter identity
	req = fmt.Sprintf("%snode/%s/%s/key/1000?show=user", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", req, nil)
	expected := `{"bodyid": 1000, "key1_user": "deleter"}`
	if !equalObjectJSON(returnValue, []byte(expected), ShowUsers) {
		t.Fatalf("Error: expected %s, got %s\n", expected, string(returnValue))
	}
}

func checkEndpoints(t *testing.T, uuid dvid.UUID) {
	var err error

	name := dvid.InstanceName("annotations")
	key1 := "1000"
	key2 := "2000"
	key3 := "3000"
	value1 := `{"bodyid": 1000, "a string": "foo", "a number": 1234, "a list": [1, 2, 3]}`
	value2 := `{"bodyid": 2000, "a string": "moo", "a number": 2345, "a list": ["mom", "pop"]}`
	value3 := `{"bodyid": 3000, "a string": "goo", "a number": 3456, "a list": [23]}`

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, name, "0", "2100")
	returnValue := server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Fatalf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != key2 && retrievedKeys[0] != key1 {
		t.Fatalf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			key2, key1, string(returnValue))
	}

	// Check return of all keys
	allkeyreq := fmt.Sprintf("%snode/%s/%s/keys", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", allkeyreq, nil)

	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Fatalf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 3 || retrievedKeys[0] != key1 && retrievedKeys[1] != key2 && retrievedKeys[2] != key3 {
		t.Fatalf("Bad all key request return.  Expected: [%q,%q,%q].  Got: %s\n",
			key3, key2, key1, string(returnValue))
	}

	// Check JSON keyvalues request using both list of ids and strings, latter is for keyvalue datatype compatibility.
	jsonIds := `[2000, 3000]`
	kvreq := fmt.Sprintf("%snode/%s/%s/keyvalues?json=true", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", kvreq, strings.NewReader(jsonIds))

	expectedValue := []byte(`{"2000":` + value2 + `,"3000":` + value3 + "}")
	if !equalObjectJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad keyvalues request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	jsonIds = `["2000", "3000"]`
	returnValue = server.TestHTTP(t, "GET", kvreq, strings.NewReader(jsonIds))
	if !equalObjectJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad keyvalues request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Make sure non-existent keys return appropriately
	jsonIds = `["23910", "23", "2000", "14", "3000", "10000"]`
	returnValue = server.TestHTTP(t, "GET", kvreq, strings.NewReader(jsonIds))
	if !equalObjectJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad keyvalues request return using unknown key.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check query
	query := `{"a string": ["moo", "goo"]}`
	queryreq := fmt.Sprintf("%snode/%s/%s/query?u=tester", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "," + value3 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad GET query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad POST query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": ["moo", "goo"], "a number": 2345}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query?show=all&u=tester", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))
	checkBasicAndAll(t, value2, returnValue, "martha")

	query = `{"a string": ["moo", "goo"], "a number": [2345, 3456]}`
	queryreq = fmt.Sprintf("%snode/%s/%s/query?u=tester", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "," + value3 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"unused field": "foo"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "moo", "unused field": "foo"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check regex query
	query = `{"a string": "re/^(f|m)oo"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value1 + "," + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "re/^(f|m)oo", "a list": "mom"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a string": "re/^(f|m)oo", "a list": ["re/.*x", "re/om"]}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + value2 + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad regex query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Get all neuronjson with only bodyid selected as field.
	expectedVal := ListNeuronJSON{
		NeuronJSON{
			"bodyid": uint64(1000),
		},
		NeuronJSON{
			"bodyid": uint64(2000),
		},
		NeuronJSON{
			"bodyid": uint64(3000),
		},
	}
	allreq := fmt.Sprintf("%snode/%s/%s/all?fields=bodyid", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", allreq, nil)
	var neurons ListNeuronJSON
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all?fields=bodyid request: %v\n", err)
	}
	sort.Sort(&neurons)
	if !reflect.DeepEqual(neurons, expectedVal) {
		t.Fatalf("Response to /all?fields=bodyid is incorrect. Expected: %v, Got: %v\n", expectedVal, neurons)
	}
}

func TestKeyvalueRequests(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	name := dvid.InstanceName("annotations")

	config := dvid.NewConfig()
	_, err := datastore.NewData(uuid, jsontype, name, config)
	if err != nil {
		t.Fatalf("Error creating new neuronjson instance: %v\n", err)
	}

	key1 := "1000"
	key1req := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, uuid, name, key1)
	resp := server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("HEAD on %s did not return 404 (File not found).  Status = %d\n", key1req, resp.Code)
	}

	// PUT a value
	value1 := `{"bodyid": 1000, "a string": "foo", "a number": 1234, "a list": [1, 2, 3]}`
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))

	// Expect error if key 0 is used
	badrequest := fmt.Sprintf("%snode/%s/%s/key/0?u=tester", server.WebAPIPath, uuid, name)
	server.TestBadHTTP(t, "POST", badrequest, strings.NewReader(`{"bodyid": 0, "data": "foo"}`))

	// Check HEAD response
	resp = server.TestHTTPResponse(t, "HEAD", key1req, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("HEAD on %s did not return 200 (OK).  Status = %d\n", key1req, resp.Code)
	}

	// Get back k/v
	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if !equalObjectJSON(returnValue, []byte(value1), ShowBasic) {
		t.Fatalf("Error on key %q: expected %s, got %s\n", key1, value1, string(returnValue))
	}

	// Expect error if no key used.
	badrequest = fmt.Sprintf("%snode/%s/%s/key/", server.WebAPIPath, uuid, name)
	server.TestBadHTTP(t, "GET", badrequest, nil)

	// Add 2nd k/v
	key2 := "2000"
	key2req := fmt.Sprintf("%snode/%s/%s/key/%s?u=martha", server.WebAPIPath, uuid, name, key2)

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("HEAD on %s did not return 404 (File Not Found).  Status = %d\n", key2req, resp.Code)
	}

	value2 := `{"bodyid": 2000, "a string": "moo", "a number": 2345, "a list": ["mom", "pop"]}`
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	resp = server.TestHTTPResponse(t, "HEAD", key2req, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("HEAD on %s did not return 200 (OK).  Status = %d\n", key2req, resp.Code)
	}

	// Add 3rd k/v
	key3 := "3000"
	value3 := `{"bodyid": 3000, "a string": "goo", "a number": 3456, "a list": [23]}`
	key3req := fmt.Sprintf("%snode/%s/%s/key/%s?u=shawna", server.WebAPIPath, uuid, name, key3)
	server.TestHTTP(t, "POST", key3req, strings.NewReader(value3))

	// Run tests on current HEAD (memDB)
	checkEndpoints(t, uuid)

	// Commit and create new version so old HEAD now requires getting from persistent store.
	commitReq := fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid)
	payload := bytes.NewBufferString(`{"note": "first version"}`)
	server.TestHTTP(t, "POST", commitReq, payload)

	versionReq := fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, uuid)
	respData := server.TestHTTP(t, "POST", versionReq, nil)
	respNewVersion := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &respNewVersion); err != nil {
		t.Fatalf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	newUUID := respNewVersion.Child

	// Make some changes so new version is different.
	key1req = fmt.Sprintf("%snode/%s/%s/key/%s?u=frank", server.WebAPIPath, newUUID, name, key1)
	value1 = `{"bodyid": 1000, "a string": "new string", "a number": 1111, "a list": [5], "new_field": "new value"}`
	server.TestHTTP(t, "POST", key1req, strings.NewReader(value1))
	key2req = fmt.Sprintf("%snode/%s/%s/key/%s?u=martha", server.WebAPIPath, newUUID, name, key2)
	value2 = `{"bodyid": 2000, "a string": "another string", "a number": 3490, "a list": ["sister", "father"], "new_field": "new value"}`
	server.TestHTTP(t, "POST", key2req, strings.NewReader(value2))

	// Run tests on old version
	checkEndpoints(t, uuid)

	// Run more tests on new version
	uuid = newUUID

	// Check if keys are re-POSTed using default update or replace=true.
	value3mod := `{"bodyid": 3000, "a string": "goo modified", "a 2nd list": [26]}`
	key3modreq := fmt.Sprintf("%snode/%s/%s/key/%s?u=bill&show=all", server.WebAPIPath, uuid, name, key3)
	server.TestHTTP(t, "POST", key3modreq, strings.NewReader(value3mod))

	returnValue = server.TestHTTP(t, "GET", key3modreq, nil)

	expectedValue := []byte(`{"bodyid": 3000, "a string": "goo modified", "a number": 3456, "a list": [23], "a 2nd list": [26]}`)
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

	// Check if we replace or keep old _user and _time while changing and reusing value
	value3modA := fmt.Sprintf(`{"bodyid": 3000, "a string": "goo modified", "a string_user": "bill", "a string_time": "%s", "a number_user": "donald", "a 2nd list": [26]}`, responseJSON["a string_time"])
	key3modAreq := fmt.Sprintf("%snode/%s/%s/key/%s?u=frank&show=all", server.WebAPIPath, uuid, name, key3)
	server.TestHTTP(t, "POST", key3modAreq, strings.NewReader(value3modA))

	returnValue = server.TestHTTP(t, "GET", key3modAreq, nil)
	dvid.Infof("After 1st mod got back: %s\n", string(returnValue))

	expectedValue = []byte(fmt.Sprintf(`{"bodyid": 3000, "a string": "goo modified", "a string_user": "bill", "a string_time": "%s", "a number": 3456, "a list": [23], "a 2nd list": [26]}`, responseJSON["a string_time"]))
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
	if value, found := responseJSON["a number_user"]; !found || value != "donald" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	old_time := responseJSON["a number_time"]
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

	// Check if keys are re-POSTed using replace=true. Don't modify "a number" field.
	time.Sleep(2 * time.Second) // need to change timestamp for test
	value3mod = `{"bodyid": 3000, "a string": "goo replaced", "only list": [1, 2], "a number": 3456}`
	key3modreq = fmt.Sprintf("%snode/%s/%s/key/%s?u=sandra&show=all&replace=true", server.WebAPIPath, uuid, name, key3)
	server.TestHTTP(t, "POST", key3modreq, strings.NewReader(value3mod))

	returnValue = server.TestHTTP(t, "GET", key3modreq, nil)
	dvid.Infof("After replace=true, got back: %s\n", string(returnValue))

	expectedValue = []byte(value3mod)
	if err := json.Unmarshal(expectedValue, &expectedJSON); err != nil {
		t.Fatalf("Couldn't unmarshal expected basic JSON: %s\n", expectedJSON)
	}
	if err := json.Unmarshal(returnValue, &responseJSON); err != nil {
		t.Fatalf("Couldn't unmarshal response JSON: %s\n", string(returnValue))
	}
	if value, found := responseJSON["a number"]; !found || !reflect.DeepEqual(value, uint64(3456)) {
		t.Fatalf("Bad response for number (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a number_user"]; !found || value != "donald" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["a number_time"]; !found || value != old_time {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}

	if value, found := responseJSON["a string"]; !found || value != "goo replaced" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	new_time := responseJSON["a string_time"]

	if value, found := responseJSON["only list"]; !found || !reflect.DeepEqual(value, []int64{1, 2}) {
		t.Fatalf("Bad response (type %s): %v\n", reflect.TypeOf(value), responseJSON)
	}
	if value, found := responseJSON["a string_user"]; !found || value != "sandra" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if value, found := responseJSON["only list_user"]; !found || value != "sandra" {
		t.Fatalf("Bad response: %v\n", responseJSON)
	}
	if len(responseJSON) != 10 {
		t.Fatalf("Expected 10 fields in response after replace, got: %v\n", responseJSON)
	}

	// Check if fieldtimes endpoint has the newer times.
	fieldtimeReq := fmt.Sprintf("%snode/%s/%s/fieldtimes", server.WebAPIPath, uuid, name)
	returnValue = server.TestHTTP(t, "GET", fieldtimeReq, nil)
	if err := json.Unmarshal(returnValue, &responseJSON); err != nil {
		t.Fatalf("Couldn't unmarshal response JSON: %s\n", string(returnValue))
	}
	if value, found := responseJSON["a string"]; !found || value != new_time {
		t.Fatalf("Bad timestamp response: %v\n", responseJSON)
	}
}

var testData = []struct {
	key string
	val string
}{
	{"1000", `{"bodyid": 1000, "a number": 3456, "position": [150,250,380], "baz": ""}`},
	{"2000", `{"bodyid": 2000, "bar":"another string", "baz":[1, 2, 3], "somefield": "im here"}`},
	{"3000", `{"bodyid": 3000, "a number": 3456, "a list": [23], "somefield": ""}`},
	{"4000", `{"position": [151, 251, 301], "bodyid": 4000, "soma_side": "LHS", "baz": "some string"}`},
}

var testData2 = []struct {
	key string
	val string
}{
	{"10000", `{"bodyid": 10000, "a number": 3456, "position": [150,250,380], "baz": ""}`},
	{"20000", `{"bodyid": 20000, "bar":"another string", "baz":[1, 2, 3], "somefield": "im here"}`},
	{"30000", `{"bodyid": 30000, "a number": 3456, "a list": [23], "somefield": ""}`},
	{"40000", `{"position": [151, 251, 301], "bodyid": 40000, "soma_side": "LHS", "baz": "some string"}`},
}

func TestFieldNames(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	for _, td := range testData {
		keyreq := fmt.Sprintf("%snode/%s/neurons/key/%s?u=tester", server.WebAPIPath, uuid, td.key)
		server.TestHTTP(t, "POST", keyreq, strings.NewReader(td.val))
	}

	// Test fields request
	fieldsReq := fmt.Sprintf("%snode/%s/neurons/fields", server.WebAPIPath, uuid)
	returnValue := server.TestHTTP(t, "GET", fieldsReq, nil)

	expectedFields := []string{"bodyid", "a number", "position", "baz", "bar", "somefield", "a list", "soma_side"}
	var fields []string
	if err := json.Unmarshal(returnValue, &fields); err != nil {
		t.Fatalf("Unable to parse return from /fields request: %s\nError: %v\n", string(returnValue), err)
	}
	expectedNumFields := (len(expectedFields)-1)*3 + 1 // fields have _user and _time except bodyid
	if len(fields) != expectedNumFields {
		t.Fatalf("Expected %d fields, got %d\n", len(expectedFields), len(fields))
	}
	for _, field := range expectedFields {
		found := false
		for _, f := range fields {
			if f == field {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected field %q not found in fields request\n", field)
		}
	}

	// If we repost without the only instance of a field, it should no longer be in the fields request
	postReq := fmt.Sprintf("%snode/%s/neurons/key/4000?u=changer&replace=true", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", postReq,
		strings.NewReader(`{"bodyid": 4000, "position": [151, 251, 301], "baz": "some string"}`),
	)

	returnValue = server.TestHTTP(t, "GET", fieldsReq, nil)
	if err := json.Unmarshal(returnValue, &fields); err != nil {
		t.Fatalf("Unable to parse return from /fields request: %s\nError: %v\n", string(returnValue), err)
	}
	for _, field := range fields {
		if field == "soma_side" {
			t.Fatalf("Found deleted field soma_side in fields request\n")
		}
	}

	// If we delete an annotation, it should no longer contribute to the fields request.
	// In this case, we delete the only "bar" field.
	delReq := fmt.Sprintf("%snode/%s/neurons/key/2000?u=changer", server.WebAPIPath, uuid)
	server.TestHTTP(t, "DELETE", delReq, nil)

	returnValue = server.TestHTTP(t, "GET", fieldsReq, nil)
	if err := json.Unmarshal(returnValue, &fields); err != nil {
		t.Fatalf("Unable to parse return from /fields request: %s\nError: %v\n", string(returnValue), err)
	}
	for _, field := range fields {
		if field == "bar" {
			t.Fatalf("Found deleted field bar in fields request: %v\n", fields)
		}
	}
}

func TestStressConcurrentRW(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	for n := 0; n < 100; n++ {
		done := make(chan struct{})
		go func(done <-chan struct{}) {
			i := 0
			for {
				i++
				keyreq := fmt.Sprintf("%snode/%s/neurons/key/%d?u=tester", server.WebAPIPath, uuid, i)
				keyval := fmt.Sprintf(`{"bodyid": %d, "somedata": "value-%d"}`, i, i)
				server.TestHTTP(t, "POST", keyreq, strings.NewReader(keyval))
				select {
				case <-done:
					return
				default:
				}
			}
		}(done)

		go func(done <-chan struct{}) {
			i := 0
			for {
				i++
				keyreq := fmt.Sprintf("%snode/%s/neurons/key/%d", server.WebAPIPath, uuid, i)
				keyval := fmt.Sprintf(`{"bodyid": %d, "somedata": "value-%d"}`, i, i)
				server.TestHTTPResponse(t, "GET", keyreq, strings.NewReader(keyval))
				select {
				case <-done:
					return
				default:
				}
			}
		}(done)

		for m := 0; m < 10; m++ {
			rangeReq := fmt.Sprintf("%snode/%s/neurons/keyrangevalues/0/1000", server.WebAPIPath, uuid)
			server.TestHTTP(t, "GET", rangeReq+"?json=true", nil)
			allreq := fmt.Sprintf("%snode/%s/neurons/all", server.WebAPIPath, uuid)
			server.TestHTTP(t, "GET", allreq, nil)
		}
		close(done)
	}
}

func TestBodyidUserTime(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Post a neuron's JSON
	keyreq := fmt.Sprintf("%snode/%s/neurons/key/%s?u=foo&replace=true", server.WebAPIPath, uuid, testData[0].key)
	server.TestHTTP(t, "POST", keyreq, strings.NewReader(testData[0].val))

	// Post again same neuron's JSON
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/%s?u=moo&replace=true", server.WebAPIPath, uuid, testData[0].key)
	server.TestHTTP(t, "POST", keyreq, strings.NewReader(testData[0].val))

	// Get all neuronjson
	allreq := fmt.Sprintf("%snode/%s/neurons/all?show=all", server.WebAPIPath, uuid)
	returnValue := server.TestHTTP(t, "GET", allreq, nil)
	var neurons ListNeuronJSON
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all request: %s\nError: %v\n", string(returnValue), err)
	}
	if len(neurons) != 1 {
		t.Fatalf("Expected only 1 neuron, got %d\n", len(neurons))
	}
	if _, found := neurons[0]["bodyid_user"]; found {
		t.Fatalf("Expected no bodyid_user field, got %v\n", neurons[0]["bodyid_user"])
	}
	if _, found := neurons[0]["bodyid_time"]; found {
		t.Fatalf("Expected no bodyid_time field, got %v\n", neurons[0]["bodyid_time"])
	}

	// Post with bodyid_user or bodyid_time should not be allowed
	neuron := `{"bodyid": 1000, "bodyid_user": "zoo", "a number": 3456, "position": [150,250,380], "baz": ""}`
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/%s?u=moo&replace=true", server.WebAPIPath, uuid, "1000")
	server.TestBadHTTP(t, "POST", keyreq, strings.NewReader(neuron))

	neuron = `{"bodyid": 1000, "bodyid_time": "2023-11-11T22:44:50-05:00", "a number": 3456, "position": [150,250,380], "baz": ""}`
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/%s?u=moo&replace=true", server.WebAPIPath, uuid, "1000")
	server.TestBadHTTP(t, "POST", keyreq, strings.NewReader(neuron))
}

// TestUserTime tests that user and time are properly recorded.
func TestUserTime(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Test JSON to be posted.
	neuron1 := `{"bodyid": 1, "number": 1234, "string": "foo"}`
	neuron2 := `{"bodyid": 2, "number": 3456, "hash": "0x1234567890abcdef"}`

	// POST the annotations
	keyreq := fmt.Sprintf("%snode/%s/neurons/key/1?u=user1", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", keyreq, strings.NewReader(neuron1))
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/2?u=user2", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", keyreq, strings.NewReader(neuron2))

	// Get all neuronjson
	allreq := fmt.Sprintf("%snode/%s/neurons/all?show=all", server.WebAPIPath, uuid)
	returnValue := server.TestHTTP(t, "GET", allreq, nil)
	var neurons ListNeuronJSON
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all request: %s\nError: %v\n", string(returnValue), err)
	}
	sort.Sort(&neurons)

	// Test that user and time were recorded correctly
	if len(neurons) != 2 {
		t.Fatalf("Expected 2 neuron, got %d\n", len(neurons))
	}
	if value, found := neurons[0]["number_user"]; !found || value != "user1" {
		t.Fatalf("Expected 'user1', got %v\n", value)
	}
	ival, found := neurons[0]["number_time"]
	if !found {
		t.Fatalf("Expected number_time field but got none\n")
	}
	neuron1_number_time, ok := ival.(string)
	if !ok {
		t.Fatalf("Expected string for number_time, got %v\n", reflect.TypeOf(ival))
	}
	if value, found := neurons[1]["number_user"]; !found || value != "user2" {
		t.Fatalf("Expected 'user2', got %v\n", value)
	}
	if _, found := neurons[1]["number_time"]; !found {
		t.Fatalf("Expected number_time field but got none\n")
	}

	// Sleep for a second so we know the timestamps will be different.
	time.Sleep(1 * time.Second)

	// Test update of neuron1
	neuron1a := `{"bodyid": 1, "number_user": "user1a", "string": "moo"}`
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/1?u=user3", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", keyreq, strings.NewReader(neuron1a))
	keyreq = fmt.Sprintf("%snode/%s/neurons/key/1?show=all", server.WebAPIPath, uuid)
	returnValue = server.TestHTTP(t, "GET", keyreq, nil)
	var neuron NeuronJSON
	if err := json.Unmarshal(returnValue, &neuron); err != nil {
		t.Fatalf("Unable to parse return: %s\nError: %v\n", string(returnValue), err)
	}
	if value, found := neuron["number_user"]; !found || value != "user1a" {
		t.Fatalf("Expected 'user1a', got %v\n", value)
	}
	if value, found := neuron["string_time"]; !found || value == neuron1_number_time {
		t.Error("Expected new string_time, got same time as before updating string field\n")
	}
	if value, found := neuron["string_user"]; !found || value != "user3" {
		t.Fatalf("Expected new string_user 'user3', got same user (%s) as before updating string field\n", value)
	}
}

func TestAll(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	allNeurons := make(ListNeuronJSON, len(testData))
	var keyreq = make([]string, len(testData))
	for i := 0; i < len(testData); i++ {
		keyreq[i] = fmt.Sprintf("%snode/%s/neurons/key/%s?u=tester", server.WebAPIPath, uuid, testData[i].key)
		server.TestHTTP(t, "POST", keyreq[i], strings.NewReader(testData[i].val))
		if err := json.Unmarshal([]byte(testData[i].val), &(allNeurons[i])); err != nil {
			t.Fatalf("Unable to parse test annotation %d: %v\n", i, err)
		}
	}

	// Get all neuronjson
	allreq := fmt.Sprintf("%snode/%s/neurons/all", server.WebAPIPath, uuid)
	returnValue := server.TestHTTP(t, "GET", allreq, nil)
	var neurons ListNeuronJSON
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all request: %v\n", err)
	}
	sort.Sort(&neurons)
	if !reflect.DeepEqual(neurons, allNeurons) {
		t.Fatalf("Response to /all is incorrect. Expected: %v, Got: %v from JSON: %s\n", allNeurons, neurons, string(returnValue))
	}

	// Get only ["baz", "position"] fields from /all neuronjson
	expectedVal := ListNeuronJSON{
		NeuronJSON{
			"bodyid":   uint64(1000),
			"baz":      "",
			"position": []int64{150, 250, 380},
		},
		NeuronJSON{
			"bodyid": uint64(2000),
			"baz":    []int64{1, 2, 3},
		},
		NeuronJSON{
			"bodyid":   uint64(4000),
			"baz":      "some string",
			"position": []int64{151, 251, 301},
		},
	}
	allreq = fmt.Sprintf("%snode/%s/neurons/all?fields=baz,position", server.WebAPIPath, uuid)
	returnValue = server.TestHTTP(t, "GET", allreq, nil)
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all request: %v\n", err)
	}
	sort.Sort(&neurons)
	for i := 0; i < 3; i++ {
		dvid.Infof("Expected %d: bodyid %s, baz %s, position %s", i, reflect.TypeOf(expectedVal[i]["bodyid"]), reflect.TypeOf(expectedVal[i]["position"]), reflect.TypeOf(expectedVal[i]["baz"]))
		dvid.Infof("Received %d: bodyid %s, baz %s, position %s", i, reflect.TypeOf(neurons[i]["bodyid"]), reflect.TypeOf(neurons[i]["position"]), reflect.TypeOf(neurons[i]["baz"]))
	}
	if !reflect.DeepEqual(neurons, expectedVal) {
		t.Fatalf("Response to /all is incorrect. Expected: %v, Got: %v\n", expectedVal, neurons)
	}
}

func TestAllWithUser(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	allNeurons := make(ListNeuronJSON, len(testData))
	var keyreq = make([]string, len(testData))
	for i := 0; i < len(testData); i++ {
		keyreq[i] = fmt.Sprintf("%snode/%s/neurons/key/%s?u=tester", server.WebAPIPath, uuid, testData[i].key)
		server.TestHTTP(t, "POST", keyreq[i], strings.NewReader(testData[i].val))
		if err := json.Unmarshal([]byte(testData[i].val), &(allNeurons[i])); err != nil {
			t.Fatalf("Unable to parse test annotation %d: %v\n", i, err)
		}
	}

	// Get all neuronjson with only baz_user selected as field.
	expectedVal := ListNeuronJSON{
		NeuronJSON{
			"bodyid":   uint64(1000),
			"baz_user": "tester",
		},
		NeuronJSON{
			"bodyid":   uint64(2000),
			"baz_user": "tester",
		},
		NeuronJSON{
			"bodyid":   uint64(4000),
			"baz_user": "tester",
		},
	}
	allreq := fmt.Sprintf("%snode/%s/neurons/all?show=user&fields=baz_user", server.WebAPIPath, uuid)
	returnValue := server.TestHTTP(t, "GET", allreq, nil)
	var neurons ListNeuronJSON
	if err := json.Unmarshal(returnValue, &neurons); err != nil {
		t.Fatalf("Unable to parse return from /all request: %v\n", err)
	}
	sort.Sort(&neurons)
	if !reflect.DeepEqual(neurons, expectedVal) {
		t.Fatalf("Response to /all is incorrect. Expected: %v, Got: %v\n", expectedVal, neurons)
	}
}

func TestDeleteWithNull(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	allNeurons := make(ListNeuronJSON, len(testData))
	var keyreq = make([]string, len(testData))
	for i := 0; i < len(testData); i++ {
		keyreq[i] = fmt.Sprintf("%snode/%s/neurons/key/%s?u=tester", server.WebAPIPath, uuid, testData[i].key)
		server.TestHTTP(t, "POST", keyreq[i], strings.NewReader(testData[i].val))
		if err := json.Unmarshal([]byte(testData[i].val), &(allNeurons[i])); err != nil {
			t.Fatalf("Unable to parse test annotation %d: %v\n", i, err)
		}
	}

	// Delete the "a number" field from key 3000.
	deljson := `{"bodyid": 3000, "a number": null, "a list": [23], "somefield": ""}`
	req := fmt.Sprintf("%snode/%s/neurons/key/3000", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", req+"?u=mrdeleter", strings.NewReader(deljson))

	// Read the key and make sure "a number" is gone.
	var expected, received NeuronJSON
	expectedJSON := `{"bodyid": 3000, "a list": [23], "somefield": ""}`
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("Unable to parse return: %v\n", err)
	}
	returnValue := server.TestHTTP(t, "GET", req, nil)
	if err := json.Unmarshal(returnValue, &received); err != nil {
		t.Fatalf("Unable to parse return: %v\n", err)
	}
	if !reflect.DeepEqual(received, expected) {
		t.Fatalf("Delete using null failed. Expected: %v, Got: %s\n", expected, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", req+"?show=all", nil)
	if err := json.Unmarshal(returnValue, &received); err != nil {
		t.Fatalf("Unable to parse return: %v\n", err)
	}
	if user, found := received["a number_user"]; !found {
		t.Fatalf("Delete using null failed. Expected 'a number_user' field, got: %v\n", received)
	} else {
		if user != "mrdeleter" {
			t.Fatalf("Expected 'a number_user' to be 'mrdeleter', got %s\n", user)
		}
	}
	if _, found := received["a number_time"]; !found {
		t.Fatalf("Delete using null failed. Expected 'a number_time' field, got: %v\n", received)
	}
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
	key1req := fmt.Sprintf("%snode/%s/unversiontest/key/%s?u=tester", server.WebAPIPath, uuid, testData[0].key)
	server.TestHTTP(t, "POST", key1req, strings.NewReader(testData[0].val))

	returnValue := server.TestHTTP(t, "GET", key1req, nil)
	if !equalObjectJSON(returnValue, []byte(testData[0].val), ShowBasic) {
		t.Fatalf("Error on key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Add 2nd k/v
	key2req := fmt.Sprintf("%snode/%s/unversiontest/key/%s?u=tester", server.WebAPIPath, uuid, testData[1].key)
	server.TestHTTP(t, "POST", key2req, strings.NewReader(testData[1].val))

	// Test
	rangeReq := fmt.Sprintf("%snode/%s/unversiontest/keyrangevalues/0/2001", server.WebAPIPath, uuid)
	expectedJSON := fmt.Sprintf(`{"%s":%s,"%s":%s}`, testData[0].key, testData[0].val, testData[1].key, testData[1].val)

	returnValue = server.TestHTTP(t, "GET", rangeReq+"?json=true", nil)
	if !equalObjectJSON(returnValue, []byte(expectedJSON), ShowBasic) {
		t.Fatalf("Error on keyrangevalues: got %s, expected %s\n", string(returnValue), expectedJSON)
	}
}

func TestFieldExistenceAndVersioning(t *testing.T) {
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
		keyreq[i] = fmt.Sprintf("%snode/%s/%s/key/%s?u=tester", server.WebAPIPath, uuid, data.DataName(), testData[i].key)
		server.TestHTTP(t, "POST", keyreq[i], strings.NewReader(testData[i].val))
	}

	// Check field existence query
	query := `{"a number": "exists/0"}`
	queryreq := fmt.Sprintf("%snode/%s/%s/query?u=tester", server.WebAPIPath, uuid, data.DataName())
	returnValue := server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue := []byte("[" + testData[1].val + "," + testData[3].val + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad existence query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"a number": "exists/1", "position": "exists/0"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + testData[2].val + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad existence query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"somefield": "exists/0"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + testData[0].val + "," + testData[3].val + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad existence query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Check if field is missing or empty string.
	query = `[{"baz": "exists/0"}, {"baz": ""}]`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + testData[0].val + "," + testData[2].val + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad ORed baz existence query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Use the batch POST for all 4 annotations with key += 10000
	var kvs proto.KeyValues
	kvs.Kvs = make([]*proto.KeyValue, 4)
	for i := 0; i < 4; i++ {
		kvs.Kvs[i] = &proto.KeyValue{
			Key:   testData2[i].key,
			Value: []byte(testData2[i].val),
		}
	}
	serialization, err := pb.Marshal(&kvs)
	if err != nil {
		t.Fatalf("couldn't serialize keyvalues: %v\n", err)
	}
	kvsPostReq := fmt.Sprintf("%snode/%s/%s/keyvalues?u=tester", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", kvsPostReq, bytes.NewReader(serialization))

	// Commit current version
	if err = datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Fatalf("Unable to lock root node %s: %v\n", uuid, err)
	}

	// Verify we can still do queries on committed version.
	query = `{"a number": "exists/1", "position": "exists/0"}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte("[" + testData[2].val + "," + testData2[2].val + "]")
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad existence query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Make new version for additional testing.
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
	returnValue = server.TestHTTP(t, "GET", keyreq[0], nil)
	if !equalObjectJSON(returnValue, []byte(testData[0].val), ShowBasic) {
		t.Fatalf("Error on first version, key %q: expected %s, got %s\n", testData[0].key, testData[0].val, string(returnValue))
	}

	// Get the second version value
	expected2val := updatedJSONBytes(t, testData[1].val, uuid2val)
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, expected2val, ShowBasic) {
		t.Fatalf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, uuid2val, string(returnValue))
	}

	// Check return of first two keys in range.
	rangereq := fmt.Sprintf("%snode/%s/%s/keyrange/%s/%s", server.WebAPIPath, uuid, data.DataName(), "10", "2010")
	returnValue = server.TestHTTP(t, "GET", rangereq, nil)

	var retrievedKeys []string
	if err = json.Unmarshal(returnValue, &retrievedKeys); err != nil {
		t.Fatalf("Bad key range request unmarshal: %v\n", err)
	}
	if len(retrievedKeys) != 2 || retrievedKeys[1] != testData[1].key && retrievedKeys[0] != testData[0].key {
		t.Fatalf("Bad key range request return.  Expected: [%q,%q].  Got: %s\n",
			testData[0].key, testData[1].key, string(returnValue))
	}

	// Check values from batch POST using individual key gets
	for i := 0; i < 4; i++ {
		k := testData2[i].key
		keyreq := fmt.Sprintf("%snode/%s/%s/key/%s", server.WebAPIPath, uuid, data.DataName(), k)
		returnValue := server.TestHTTP(t, "GET", keyreq, nil)
		if !equalObjectJSON(returnValue, []byte(testData2[i].val), ShowBasic) {
			t.Fatalf("Expected batch POST key %q to have value %s, got %d instead\n", k, testData[i].val, returnValue)
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
			t.Fatalf("Expected batch POST key %q to have value %s, got %d instead\n", expectedKeys[keyNum], expectedVals[keyNum], returnValue)
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
			t.Fatalf("Expected batch POST key %q to have value %s, got %d instead\n", expectedKeys[keyNum], testData[keyNum].val, returnValue)
		}
	}

	// Commit the repo
	if err = datastore.Commit(uuid2, "my 2nd commit msg", []string{"changed 2nd k/v"}); err != nil {
		t.Fatalf("Unable to commit node %s: %v\n", uuid2, err)
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
		t.Fatalf("Error on first version, key %q: expected %s, got %s\n", testData[1].key, testData[1].val, string(returnValue))
	}
	returnValue = server.TestHTTP(t, "GET", uuid2req, nil)
	if !equalObjectJSON(returnValue, expected2val, ShowBasic) {
		t.Fatalf("Error on second version, key %q: expected %s, got %s\n", testData[1].key, expected2val, string(returnValue))
	}
}
