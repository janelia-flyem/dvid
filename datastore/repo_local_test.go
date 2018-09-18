// +build !clustered,!gcloud

package datastore

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestRepoGobEncoding(t *testing.T) {
	uuid := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	versionID := dvid.VersionID(23)
	repoID := dvid.RepoID(13)

	repo := newRepo(uuid, versionID, repoID, "foobar")
	repo.alias = "just some alias"
	repo.log = []string{
		"Did this",
		"Then that",
		"And the other thing",
	}
	repo.properties = map[string]interface{}{
		"foo": 42,
		"bar": "some string",
		"baz": []int{3, 9, 7},
	}

	encoding, err := repo.GobEncode()
	if err != nil {
		t.Fatalf("Could not encode repo: %v\n", err)
	}
	received := repoT{}
	if err = received.GobDecode(encoding); err != nil {
		t.Fatalf("Could not decode repo: %v\n", err)
	}

	// Did we serialize OK
	repo.dag = nil
	received.dag = nil
	if len(received.properties) != 3 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	foo, ok := received.properties["foo"]
	if !ok || foo != 42 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	bar, ok := received.properties["bar"]
	if !ok || bar != "some string" {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	baz, ok := received.properties["baz"]
	if !ok || !reflect.DeepEqual(baz, []int{3, 9, 7}) {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	repo.properties = nil
	received.properties = nil
	fmt.Printf("repo: created %s, updated %s\n", repo.created, repo.updated)
	fmt.Printf("recv: created %s, updated %s\n", received.created, received.updated)
	if !reflect.DeepEqual(*repo, received) {
		t.Fatalf("Repo Gob messed up:\nOriginal: %v\nReceived: %v\n", *repo, received)
	}
}

func makeTestVersions(t *testing.T) {
	root, err := NewRepo("test repo", "test repo description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(root, "root node", nil); err != nil {
		t.Fatal(err)
	}

	child1, err := NewVersion(root, "note describing child 1", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(child1, "child 1", nil); err != nil {
		t.Fatal(err)
	}

	// Test ability to set UUID of child
	assignedUUID := dvid.UUID("0c8bc973dba74729880dd1bdfd8d0c5e")
	child2, err := NewVersion(root, "note describing child 2", "child2", &assignedUUID)
	if err != nil {
		t.Fatal(err)
	}

	log2 := []string{"This is line 1 of log", "This is line 2 of log", "Last line for multiline log"}
	if err := Commit(child2, "child 2 assigned", log2); err != nil {
		t.Fatal(err)
	}

	// Make uncommitted child 3
	child3, err := NewVersion(root, "note describing child 3", "child3", nil)
	if err != nil {
		t.Fatal(err)
	}
	nodelog := []string{`My first node-level log line.!(;#)}`, "Second line is here!!!"}
	if err := AddToNodeLog(child3, nodelog); err != nil {
		t.Fatal(err)
	}
}

func TestRepoPersistence(t *testing.T) {
	OpenTest()

	makeTestVersions(t)

	// Save this metadata
	jsonBytes, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	jsonStr := string(jsonBytes)
	mutidJSON := fmt.Sprintf(`"MutationID":%d`, InitialMutationID)
	mutidPos := strings.Index(jsonStr, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include MutationID set to %d, but didn't find it\n", InitialMutationID)
	}
	jsonStr = jsonStr[:mutidPos] + jsonStr[mutidPos+len(mutidJSON):]

	mutidJSON = fmt.Sprintf(`"SavedMutationID":%d`, InitialMutationID+StrideMutationID)
	mutidPos = strings.Index(jsonStr, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include SavedMutationID set to %d, but didn't find it\n", InitialMutationID+StrideMutationID)
	}
	jsonStr = jsonStr[:mutidPos] + jsonStr[mutidPos+len(mutidJSON):]

	// Shutdown and restart.
	CloseReopenTest()
	defer CloseTest()

	// Check if metadata is same
	jsonBytes2, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	jsonStr2 := string(jsonBytes2)
	mutidJSON = fmt.Sprintf(`"MutationID":%d`, InitialMutationID+StrideMutationID)
	mutidPos = strings.Index(jsonStr2, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include MutationID set to %d, but didn't find it\n", InitialMutationID)
	}
	jsonStr2 = jsonStr2[:mutidPos] + jsonStr2[mutidPos+len(mutidJSON):]

	mutidJSON = fmt.Sprintf(`"SavedMutationID":%d`, InitialMutationID+2*StrideMutationID)
	mutidPos = strings.Index(jsonStr2, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include SavedMutationID set to %d, but didn't find it\n", InitialMutationID+2*StrideMutationID)
	}
	jsonStr2 = jsonStr2[:mutidPos] + jsonStr2[mutidPos+len(mutidJSON):]

	if jsonStr != jsonStr2 {
		t.Errorf("\nRepo metadata JSON changes on close/reopen:\n\nOld:\n%s\n\nNew:\n%s\n", jsonStr, jsonStr2)
	}
}

// Make sure each new repo has a different local ID.
func TestNewRepoDifferent(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root1, err := NewRepo("test repo 1", "test repo 1 description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	root2, err := NewRepo("test repo 2", "test repo 2 description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// Delve down into private methods to make sure internal IDs are different.
	repo1, err := manager.repoFromUUID(root1)
	if err != nil {
		t.Fatal(err)
	}
	repo2, err := manager.repoFromUUID(root2)
	if err != nil {
		t.Fatal(err)
	}
	if root1 == root2 {
		t.Errorf("New repos share uuid: %s\n", root1)
	}
	if repo1.id == repo2.id {
		t.Errorf("New repos share repo id: %d\n", repo1.id)
	}
	if repo1.version == repo2.version {
		t.Errorf("New repos share version id: %d\n", repo1.version)
	}
	if repo1.alias == repo2.alias {
		t.Errorf("New repos share alias: %s\n", repo1.alias)
	}
}

func TestUUIDAssignment(t *testing.T) {
	OpenTest()
	defer CloseTest()

	uuidStr1 := "de305d5475b4431badb2eb6b9e546014"
	myuuid := dvid.UUID(uuidStr1)
	root, err := NewRepo("test repo", "test repo description", &myuuid, "")
	if err != nil {
		t.Fatal(err)
	}
	if root != myuuid {
		t.Errorf("Assigned root UUID %q != created root UUID %q\n", myuuid, root)
	}

	// Check if branches can also have assigned UUIDs
	if err := Commit(root, "root node", nil); err != nil {
		t.Fatal(err)
	}
	uuidStr2 := "8fa05d5475b4431badb2eb6b9e0123014"
	myuuid2 := dvid.UUID(uuidStr2)
	child, err := NewVersion(myuuid, "note describing uuid2", "", &myuuid2)
	if err != nil {
		t.Fatal(err)
	}
	if child != myuuid2 {
		t.Errorf("Assigned child UUID %q != created child UUID %q\n", myuuid2, child)
	}

	// Make sure we can lookup assigned UUIDs
	uuid, _, err := MatchingUUID(uuidStr1[:10])
	if err != nil {
		t.Errorf("Error matching UUID fragment %s: %v\n", uuidStr1[:10], err)
	}
	if uuid != myuuid {
		t.Errorf("Error getting back correct UUID %s from %s\n", myuuid, uuid)
	}
}
