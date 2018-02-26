/* Test datastore for testing datastore and other packages. */

package datastore

import (
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	WebAddress   = "localhost:8657"
	RPCAddress   = "localhost:8658"
	WebClientDir = ""
)

type testStoreT struct {
	sync.Mutex
	backend *storage.Backend
}

var (
	testStore = testStoreT{}
)

// NewTestRepo returns a new datastore.Repo suitable for testing.
func NewTestRepo() (dvid.UUID, dvid.VersionID) {
	uuid, err := NewRepo("testRepo", "A test repository", nil, "foobar")
	if err != nil {
		log.Fatalf("Unable to create new testing repo: %v\n", err)
	}

	versionID, err := VersionFromUUID(uuid)
	if err != nil {
		log.Fatalf("Unable to get version ID from repo root UUID %s\n", uuid)
	}
	return uuid, versionID
}

func openStore(create bool) {
	dvid.Infof("Opening test datastore.  Create = %v\n", create)
	if create {
		var err error
		testStore.backend, err = storage.GetTestableBackend()
		if err != nil {
			log.Fatal(err)
		}
	}
	initMetadata, err := storage.Initialize(dvid.Config{}, testStore.backend)
	if err != nil {
		log.Fatalf("Can't initialize test datastore: %v\n", err)
	}
	if err := Initialize(initMetadata, &InstanceConfig{}); err != nil {
		log.Fatalf("Can't initialize datastore management: %v\n", err)
	}
	dvid.Infof("Storage initialized.  initMetadata = %v\n", initMetadata)
}

func OpenTest() {
	testStore.Lock()
	dvid.Infof("Opening test datastore...\n")
	openStore(true)
}

// CloseReopenTest forces close and then reopening of the datastore, useful for testing
// persistence.  We only allow close/reopen when all tests not avaiting close/reopen are finished.
func CloseReopenTest() {
	dvid.Infof("Closing test datastore for reopen test...\n")
	storage.Shutdown()
	dvid.Infof("Reopening test datastore...\n")
	openStore(false)
}

func CloseTest() {
	dvid.Infof("Closing and deleting test datastore...\n")
	Shutdown()
	testableEng := storage.GetTestableEngine()
	if testableEng == nil {
		log.Fatalf("Could not find a storage engine that was testable")
	}
	config, _ := testStore.backend.StoreConfig("default")
	testableEng.Delete(config)
	testStore.Unlock()
}
