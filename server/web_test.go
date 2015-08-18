package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

func createRepo(t *testing.T) dvid.UUID {
	apiStr := fmt.Sprintf("%srepos", WebAPIPath)
	r := TestHTTP(t, "POST", apiStr, nil)
	var jsonResp map[string]interface{}

	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repo creation response: %s\n", string(r))
	}
	v, ok := jsonResp["root"]
	if !ok {
		t.Fatalf("No 'root' metadata returned: %s\n", string(r))
	}
	uuid, ok := v.(string)
	if !ok {
		t.Fatalf("Couldn't cast returned 'root' data (%v) into string.\n", v)
	}
	return dvid.UUID(uuid)
}

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

func TestLog(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := createRepo(t)

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

func TestDeleteInstance(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := createRepo(t)

	// Shouldn't be able to delete instance without "imsure"
	delReq := fmt.Sprintf("%srepo/%s/%s", WebAPIPath, uuid, "absent-name")
	TestBadHTTP(t, "DELETE", delReq, nil)

	// Shouldn't be able to delete an instance that doesn't exist.
	delReq = fmt.Sprintf("%srepo/%s/%s?imsure=true", WebAPIPath, uuid, "absent-name")
	TestBadHTTP(t, "DELETE", delReq, nil)
}

func TestCommitBranchMerge(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := createRepo(t)

	// Shouldn't be able to create branch on open node.
	branchReq := fmt.Sprintf("%snode/%s/branch", WebAPIPath, uuid)
	TestBadHTTP(t, "POST", branchReq, nil)

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr := fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Make sure committed nodes can only be read.
	// We shouldn't be able to write to log.
	payload = bytes.NewBufferString(`{"log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr = fmt.Sprintf("%snode/%s/log", WebAPIPath, uuid)
	TestBadHTTP(t, "POST", apiStr, payload)

	// Should be able to create branch now that we've committed parent.
	respData := TestHTTP(t, "POST", branchReq, nil)
	resp := struct {
		Child string `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	parent1 := dvid.UUID(resp.Child)

	// Create a sibling.
	respData = TestHTTP(t, "POST", branchReq, nil)
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
