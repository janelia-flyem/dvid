package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func testLog(t *testing.T, got, expect string) {
	re := regexp.MustCompile(`\S+\s+(.+)$`)
	matches := re.FindStringSubmatch(got)
	if len(matches) != 2 {
		t.Errorf("Unable to match log line.  Got %q, expected %q: %v\n", got, expect, matches)
		return
	}
	if matches[1] != expect {
		t.Errorf("Bad log line. Got %q, expected %q\n", matches[1], expect)
	}
}

func TestNote(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Post a note
	note := "everything is awesome"
	jsonStr := fmt.Sprintf(`{"note": %q}`, note)
	payload := bytes.NewBufferString(jsonStr)
	apiStr := fmt.Sprintf("%snode/%s/note", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Verify it was saved.
	r := TestHTTP(t, "GET", apiStr, nil)
	jsonResp := make(map[string]string)
	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal log response: %s\n", string(r))
	}
	if len(jsonResp) != 1 {
		t.Errorf("Bad note return: %s\n", string(r))
	}
	data, ok := jsonResp["note"]
	if !ok {
		t.Fatalf("No 'note' data returned: %s\n", string(r))
	}
	if data != note {
		t.Errorf("expected note to be %q, got: %s\n", note, data)
	}
}

func TestLog(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Post a log
	payload := bytes.NewBufferString(`{"log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr := fmt.Sprintf("%snode/%s/log", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Verify it was saved.
	r := TestHTTP(t, "GET", apiStr, nil)
	jsonResp := make(map[string][]string)
	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal log response: %s\n", string(r))
	}
	if len(jsonResp) != 1 {
		t.Errorf("Bad log return: %s\n", string(r))
	}
	data, ok := jsonResp["log"]
	if !ok {
		t.Fatalf("No 'log' data returned: %s\n", string(r))
	}
	if len(data) != 3 {
		t.Fatalf("Got wrong # of lines in log: %v\n", data)
	}
	testLog(t, data[0], "line1")
	testLog(t, data[1], "line2")
	testLog(t, data[2], "some more stuff in a line")

	// Add some more to log
	payload = bytes.NewBufferString(`{"log": ["line4", "line5"]}`)
	apiStr = fmt.Sprintf("%snode/%s/log", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Verify it was appended.
	r = TestHTTP(t, "GET", apiStr, nil)
	jsonResp = make(map[string][]string)
	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal log response: %s\n", string(r))
	}
	if len(jsonResp) != 1 {
		t.Errorf("Bad log return: %s\n", string(r))
	}
	data, ok = jsonResp["log"]
	if !ok {
		t.Fatalf("No 'log' data returned: %s\n", string(r))
	}
	if len(data) != 5 {
		t.Errorf("Got wrong # of lines in log: %v\n", data)
	}
	testLog(t, data[3], "line4")
	testLog(t, data[4], "line5")
}

func TestCommitBranchMergeDelete(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Shouldn't be able to create branch on open node.
	versionReq := fmt.Sprintf("%snode/%s/newversion", WebAPIPath, uuid)
	TestBadHTTP(t, "POST", versionReq, nil)

	// Check commit status
	checkReq := fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	retVal := TestHTTP(t, "GET", checkReq, nil)
	if string(retVal) != `{"Locked":false}` {
		t.Errorf("Expected unlocked commit status, got: %s\n", string(retVal))
	}

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr := fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Check commit status
	checkReq = fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	retVal = TestHTTP(t, "GET", checkReq, nil)
	if string(retVal) != `{"Locked":true}` {
		t.Errorf("Expected locked commit status, got: %s\n", string(retVal))
	}

	// Make sure committed nodes can only be read.
	// We shouldn't be able to write to log.
	payload = bytes.NewBufferString(`{"log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr = fmt.Sprintf("%snode/%s/log", WebAPIPath, uuid)
	TestBadHTTP(t, "POST", apiStr, payload)

	// Should be able to create branch now that we've committed parent.
	respData := TestHTTP(t, "POST", versionReq, nil)
	resp := struct {
		Child string `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	parent1 := dvid.UUID(resp.Child)

	// Create a sibling.
	branchReq := fmt.Sprintf("%snode/%s/branch", WebAPIPath, uuid)
	bpayload := bytes.NewBufferString(`{"branch": "mybranch"}`)
	respData = TestHTTP(t, "POST", branchReq, bpayload)
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	parent2 := dvid.UUID(resp.Child)

	// Commit both parents
	payload = bytes.NewBufferString(`{"note": "This is first parent"}`)
	apiStr = fmt.Sprintf("%snode/%s/commit", WebAPIPath, parent1)
	TestHTTP(t, "POST", apiStr, payload)

	payload = bytes.NewBufferString(`{"note": "This is second parent"}`)
	apiStr = fmt.Sprintf("%snode/%s/commit", WebAPIPath, parent2)
	TestHTTP(t, "POST", apiStr, payload)

	// Merge the two disjoint branches.
	mergeJSON := fmt.Sprintf(`{"mergeType": "conflict-free", "note": "This is my merged node", "parents": [%q, %q]}`, parent1[:7], parent2)
	payload = bytes.NewBufferString(mergeJSON)
	apiStr = fmt.Sprintf("%srepo/%s/merge", WebAPIPath, parent1)
	TestHTTP(t, "POST", apiStr, payload)
}
