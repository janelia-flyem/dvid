// +build !clustered,!gcloud

package datastore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/dvid/storage/local"
	"github.com/janelia-flyem/go/uuid"
)

func TestRepoGobEncoding(t *testing.T) {
	uuid := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	versionID := dvid.VersionID(23)
	repoID := dvid.RepoID(13)

	repo := newRepo(uuid, versionID, repoID)
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
	if !reflect.DeepEqual(*repo, received) {
		t.Fatalf("Repo Gob messed up:\nOriginal: %v\nReceived: %v\n", *repo, received)
	}
}

const (
	WebAddress   = "localhost:8657"
	RPCAddress   = "localhost:8658"
	WebClientDir = ""
)

var (
	engine storage.Engine
	count  int
	dbpath string
	mu     sync.Mutex
)

func useStore() {
	mu.Lock()
	defer mu.Unlock()
	if count == 0 {
		dbpath = filepath.Join(os.TempDir(), fmt.Sprintf("dvid-test-%s", uuid.NewV4()))
		var err error
		engine, err = local.CreateBlankStore(dbpath)
		if err != nil {
			log.Fatalf("Can't create a blank test datastore: %v\n", err)
		}
		if err = storage.Initialize(engine, "testdb"); err != nil {
			log.Fatalf("Can't initialize test datastore: %v\n", err)
		}
		if err = InitMetadata(engine); err != nil {
			log.Fatalf("Can't write blank datastore metadata: %v\n", err)
		}
		if err = Initialize(); err != nil {
			log.Fatalf("Can't initialize datastore management: %v\n", err)
		}
	}
	count++
}

// closeReopenStore forces close of the underlying storage engine and then reopening
// the datastore.  Useful for testing metadata persistence.
func closeReopenStore() {
	mu.Lock()
	defer mu.Unlock()
	dvid.BlockOnActiveCgo()
	if engine == nil {
		log.Fatalf("Attempted to close and reopen non-existant engine!")
	}
	engine.Close()

	var err error
	create := false
	engine, err = local.NewKeyValueStore(dbpath, create, dvid.Config{})
	if err != nil {
		log.Fatalf("Error reopening test db at %s: %v\n", dbpath, err)
	}
	if err = storage.Initialize(engine, "testdb"); err != nil {
		log.Fatalf("CloseReopenStore: bad storage.Initialize(): %v\n", err)
	}
	if err = Initialize(); err != nil {
		log.Fatalf("CloseReopenStore: can't initialize datastore management: %v\n", err)
	}
}

func closeStore() {
	mu.Lock()
	defer mu.Unlock()
	count--
	if count == 0 {
		dvid.BlockOnActiveCgo()
		if engine == nil {
			log.Fatalf("Attempted to close non-existant engine!")
		}
		// Close engine and delete store.
		engine.Close()
		engine = nil
		if err := os.RemoveAll(dbpath); err != nil {
			log.Fatalf("Unable to cleanup test store: %s\n", dbpath)
		}
	}
}

func TestCommandLine(t *testing.T) {
	useStore()
	defer closeStore()

}

func makeTestVersions(t *testing.T) {
	root, err := NewRepo("test repo", "test repo description", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(root, "root node", nil); err != nil {
		t.Fatal(err)
	}

	child1, err := NewVersion(root, "note describing child 1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(child1, "child 1", nil); err != nil {
		t.Fatal(err)
	}

	// Test ability to set UUID of child
	assignedUUID := dvid.UUID("0c8bc973dba74729880dd1bdfd8d0c5e")
	child2, err := NewVersion(root, "note describing child 2", &assignedUUID)
	if err != nil {
		t.Fatal(err)
	}

	log2 := []string{"This is line 1 of log", "This is line 2 of log", "Last line for multiline log"}
	if err := Commit(child2, "child 2 assigned", log2); err != nil {
		t.Fatal(err)
	}

	// Make uncommitted child 3
	child3, err := NewVersion(root, "note describing child 3", nil)
	if err != nil {
		t.Fatal(err)
	}
	nodelog := []string{`My first node-level log line.!(;#)}`, "Second line is here!!!"}
	if err := AddToNodeLog(child3, nodelog); err != nil {
		t.Fatal(err)
	}
}

func TestRepoPersistence(t *testing.T) {
	useStore()

	makeTestVersions(t)

	// Save this metadata
	jsonBytes, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	// Shutdown and restart.
	closeReopenStore()
	defer closeStore()

	// Check if metadata is same
	jsonBytes2, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(jsonBytes, jsonBytes2) {
		t.Errorf("\nRepo metadata JSON changes on close/reopen:\n\nOld:\n%s\n\nNew:\n%s\n", string(jsonBytes), string(jsonBytes2))
	}
}

// Make sure each new repo has a different local ID.
func TestNewRepoDifferent(t *testing.T) {
	useStore()
	defer closeStore()

	root1, err := NewRepo("test repo 1", "test repo 1 description", nil)
	if err != nil {
		t.Fatal(err)
	}

	root2, err := NewRepo("test repo 2", "test repo 2 description", nil)
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
		t.Errorf("New repos share uuid: %d\n", root1)
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
	useStore()
	defer closeStore()

	uuidStr1 := "de305d5475b4431badb2eb6b9e546014"
	myuuid := dvid.UUID(uuidStr1)
	root, err := NewRepo("test repo", "test repo description", &myuuid)
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
	child, err := NewVersion(myuuid, "note describing uuid2", &myuuid2)
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
