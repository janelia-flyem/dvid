package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"testing"
	"time"

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

func TestReload(t *testing.T) {
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

	// Reload all metadata
	reloadStr := fmt.Sprintf("%sserver/reload-metadata", WebAPIPath)
	TestHTTP(t, "POST", reloadStr, nil)

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

func TestHTTPCreate(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()

	jsonStr := `{"alias": "myrepo", "description": "test repo", "root": "28841c8277e044a7b187dda03e18da13"}`
	payload := bytes.NewBufferString(jsonStr)
	apiStr := fmt.Sprintf("%srepos", WebAPIPath)
	TestHTTP(t, "POST", apiStr, payload)

	// Verify it was saved.
	apiStr += "/info"
	r := TestHTTP(t, "GET", apiStr, nil)
	jsonResp := make(map[string]map[string]interface{})
	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repos/info response: %s\n", string(r))
	}
	if len(jsonResp) != 1 {
		t.Errorf("Bad repos/infoote return: %s\n", string(r))
	}
	repoData, ok := jsonResp["28841c8277e044a7b187dda03e18da13"]
	if !ok {
		t.Fatalf("Expected root repo info, got: %s\n", string(r))
	}
	rootI, ok := repoData["Root"]
	if !ok {
		t.Fatalf("Expected repo info, got: %v\n", repoData)
	}
	uuidStr, ok := rootI.(string)
	if !ok {
		t.Fatalf("can't interpret root info (%v) as string\n", rootI)
	}
	if uuidStr != "28841c8277e044a7b187dda03e18da13" {
		t.Fatalf("Got bad root: %s\n", uuidStr)
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

	// Check branch versions
	apiStr = fmt.Sprintf("%srepo/%s/branch-versions/master", WebAPIPath, parent1)
	r := TestHTTP(t, "GET", apiStr, nil)
	versionsResp := make([]string, 2)
	if err := json.Unmarshal(r, &versionsResp); err != nil {
		t.Fatalf("Unable to unmarshal branch versions: %s\n", string(r))
	}
	if len(versionsResp) != 2 {
		t.Errorf("Bad branch versions return: %s\n", string(r))
	}
	if versionsResp[0] != string(parent1) {
		t.Errorf("Expected master leaf to be %s, got %s\n", parent1, versionsResp[0])
	}
	if versionsResp[1] != string(uuid) {
		t.Errorf("Expected master root to be %s, got %s\n", uuid, versionsResp[1])
	}

	apiStr = fmt.Sprintf("%srepo/%s/branch-versions/mybranch", WebAPIPath, parent2)
	r = TestHTTP(t, "GET", apiStr, nil)
	if err := json.Unmarshal(r, &versionsResp); err != nil {
		t.Fatalf("Unable to unmarshal branch versions: %s\n", string(r))
	}
	if len(versionsResp) != 2 {
		t.Errorf("Bad branch versions return: %s\n", string(r))
	}
	if versionsResp[0] != string(parent2) {
		t.Errorf("Expected mybranch leaf to be %s, got %s\n", parent2, versionsResp[0])
	}
	if versionsResp[1] != string(uuid) {
		t.Errorf("Expected mybranch root to be %s, got %s\n", uuid, versionsResp[1])
	}

	apiStr = fmt.Sprintf("%srepo/%s/branch-versions/foo", WebAPIPath, parent2)
	r = TestHTTP(t, "GET", apiStr, nil)
	if string(r) != "[]" {
		t.Errorf("Bad branch versions return: %s\n", string(r))
	}

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

func TestAssignableUUID(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr := fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Test assigned UUID on new version
	versionReq := fmt.Sprintf("%snode/%s/newversion", WebAPIPath, uuid)
	payload = bytes.NewBufferString(`{"uuid": "f3870173ad1d4a6a872b9fd860e246b3"}`)
	respData := TestHTTP(t, "POST", versionReq, payload)
	resp := struct {
		Child string `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	if resp.Child != "f3870173ad1d4a6a872b9fd860e246b3" {
		t.Errorf("Expected new version UUID to be f3870173ad1d4a6a872b9fd860e246b3, got %s\n", resp.Child)
	}
	if _, err := dvid.StringToUUID(resp.Child); err != nil {
		t.Errorf("bad child uuid: %v\n", err)
	}

	// Shouldn't be able to request a second node with same UUID.
	TestBadHTTP(t, "POST", versionReq, payload)

	// Shouldn't be able to request a node with bad UUID.
	payload = bytes.NewBufferString(`{"uuid": "f3870173ad1d4a6a872b9f0e246b3"}`)
	TestBadHTTP(t, "POST", versionReq, payload)

	payload = bytes.NewBufferString(`{"uuid": "fXX70173ad1d4a6a872b9fd860e246b3"}`)
	TestBadHTTP(t, "POST", versionReq, payload)

	// Create a sibling.
	branchReq := fmt.Sprintf("%snode/%s/branch", WebAPIPath, uuid)
	payload = bytes.NewBufferString(`{"branch": "mybranch", "uuid": "7487347145aa42d0bda8ae6f27d4605c"}`)
	respData = TestHTTP(t, "POST", branchReq, payload)
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	if resp.Child != "7487347145aa42d0bda8ae6f27d4605c" {
		t.Errorf("Expected new version UUID to be 7487347145aa42d0bda8ae6f27d4605c, got %s\n", resp.Child)
	}
	if _, err := dvid.StringToUUID(resp.Child); err != nil {
		t.Errorf("bad child uuid: %v\n", err)
	}
}

func doMetadataReads(t *testing.T, uuid dvid.UUID, wg *sync.WaitGroup, done chan bool) {
	for {
		getURL := fmt.Sprintf("%srepo/%s/info", WebAPIPath, uuid)
		got := TestHTTP(t, "GET", getURL, nil)
		var jsonResp struct {
			DAG struct {
				Nodes map[string]struct {
					Branch, Note string
					Log          []string
					UUID         string
					VersionID    int
					Locked       bool
					Parents      []int
					Children     []int
					Created      string
					Updated      string
				}
			}
		}
		if err := json.Unmarshal(got, &jsonResp); err != nil {
			t.Fatalf("couldn't unmarshal response: %s\n", string(got))
		}

		for _, meta := range jsonResp.DAG.Nodes {
			curUUID := dvid.UUID(meta.UUID)
			TestHTTP(t, "GET", fmt.Sprintf("%srepo/%s/info", WebAPIPath, curUUID), nil)
			TestHTTP(t, "GET", fmt.Sprintf("%snode/%s/note", WebAPIPath, curUUID), nil)
			TestHTTP(t, "GET", fmt.Sprintf("%snode/%s/log", WebAPIPath, curUUID), nil)
		}

		select {
		case <-done:
			wg.Done()
			return
		default:
		}
	}
}

func commitAndBranch(t *testing.T, uuid dvid.UUID) (child1, child2 dvid.UUID) {
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr := fmt.Sprintf("%snode/%s/commit", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, payload)

	// Create child
	versionReq := fmt.Sprintf("%snode/%s/newversion", WebAPIPath, uuid)
	respData := TestHTTP(t, "POST", versionReq, nil)
	resp := struct {
		Child string `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Fatalf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	child1 = dvid.UUID(resp.Child)

	// Create a random branch
	branchData := fmt.Sprintf(`{"branch": "branch-%d"}`, rand.Int())
	branchReq := fmt.Sprintf("%snode/%s/branch", WebAPIPath, uuid)
	payload = bytes.NewBufferString(branchData)
	respData = TestHTTP(t, "POST", branchReq, payload)
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	child2 = dvid.UUID(resp.Child)
	return
}

func TestStressConcurrentMetadataOps(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer CloseTest()
	uuid, _ := datastore.NewTestRepo()

	wg := new(sync.WaitGroup)
	done := make(chan bool)
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go doMetadataReads(t, uuid, wg, done)
	}

	for i := 0; i < 100; i++ {
		child1, _ := commitAndBranch(t, uuid)
		uuid = child1
		time.Sleep(10 * time.Millisecond)
	}
	close(done)
	wg.Wait()
}
