package neuronjson

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/server"
)

var sampleData = map[uint64]string{
	1000: `{"bodyid": 1000, "position": [100, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]}`,
	1001: `{"bodyid": 1001, "position": [110, 111, 112], "avg_location": "113, 114, 115", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]}`,
	1002: `{"bodyid": 1002, "position": [100, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]}`,
	1003: `{"bodyid": 1003, "position": [102, 101, 102], "avg_location": "103, 104, 105", "_user": "nobody@gmail.com", "_timestamp": 1619751219.934025, "class": "Interneuron (TBD)", "tags": ["group1"]}`,
	2000: `{"bodyid": 2000, "position": [200, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]}`,
	2001: `{"bodyid": 2001, "position": [200, 111, 112], "avg_location": "213, 214, 215", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]}`,
	2002: `{"bodyid": 2002, "position": [200, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]}`,
	2003: `{"bodyid": 2003, "position": [202, 201, 202], "avg_location": "203, 204, 205", "_user": "another@gmail.com", "_timestamp": 1619751219.934025, "class": "9A", "tags": ["group2"]}`,
	3000: `{"bodyid": 3000, "position": [300, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]}`,
	3001: `{"bodyid": 3001, "position": [300, 111, 113], "avg_location": "313, 314, 315", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]}`,
	3002: `{"bodyid": 3002, "position": [302, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]}`,
	3003: `{"bodyid": 3003, "position": [303, 301, 303], "avg_location": "303, 304, 305", "_user": "third@gmail.com", "_timestamp": 1619751219.934025, "class": "9B", "tags": ["group3"]}`,
}

func TestQueryBodyIDs(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	payload := bytes.NewBufferString(`{"typename": "neuronjson", "dataname": "neurons"}`)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	for bodyid, jsonStr := range sampleData {
		keyreq := fmt.Sprintf("%snode/%s/neurons/key/%d?u=tester", server.WebAPIPath, uuid, bodyid)
		server.TestHTTP(t, "POST", keyreq, strings.NewReader(jsonStr))
	}

	// Test query of body IDs
	query := `{"bodyid": [1000, 2000, 3000]}`
	queryreq := fmt.Sprintf("%snode/%s/%s/query", server.WebAPIPath, uuid, "neurons")
	returnValue := server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue := []byte(fmt.Sprintf("[%s,%s,%s]", sampleData[1000], sampleData[2000], sampleData[3000]))
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"bodyid": 2000}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte(fmt.Sprintf("[%s]", sampleData[2000]))
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `[{"bodyid": [1000, 2000]}, {"bodyid": [3000]}]`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte(fmt.Sprintf("[%s,%s,%s]", sampleData[1000], sampleData[2000], sampleData[3000]))
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	query = `{"bodyid": [3003], "position": [303, 301, 303]}`
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	expectedValue = []byte(fmt.Sprintf("[%s]", sampleData[3003]))
	if !equalListJSON(returnValue, expectedValue, ShowBasic) {
		t.Fatalf("Bad query request return.  Expected:%v.  Got: %v\n", string(expectedValue), string(returnValue))
	}

	// Test if we use bodyid=true query string
	query = `{"class": "9A"}`
	queryreq = fmt.Sprintf("%snode/%s/neurons/query?onlyid=true", server.WebAPIPath, uuid)
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	if string(returnValue) != "[2000,2001,2002,2003]" {
		t.Fatalf("Bad query request return.\nExpected: [2000,2001,2002,2003]\nGot: %v\n", string(returnValue))
	}

	// Commit current version and make new child to test querying on backing store instead of memory.
	if err := datastore.Commit(uuid, "my commit msg", []string{"stuff one", "stuff two"}); err != nil {
		t.Fatalf("Unable to lock root node %s: %v\n", uuid, err)
	}
	_, err := datastore.NewVersion(uuid, "some child", "", nil)
	if err != nil {
		t.Fatalf("Unable to create new version off node %s: %v\n", uuid, err)
	}
	queryreq = fmt.Sprintf("%snode/%s/neurons/query?onlyid=true", server.WebAPIPath, uuid)
	returnValue = server.TestHTTP(t, "POST", queryreq, strings.NewReader(query))

	if string(returnValue) != "[2000,2001,2002,2003]" {
		t.Fatalf("Bad query request return.\nExpected: [2000,2001,2002,2003]\nGot: %v\n", string(returnValue))
	}
}
