/* Test datastore for testing datastore and other packages. */

package datastore

import (
	"fmt"
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
	backend map[string]*dvid.StoreConfig
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

// getTestStoreConfig returns a configuration, amenable to testing, based on compiled-in engines.
func getTestStoreConfig() (map[string]*dvid.StoreConfig, error) {
	testableEng := storage.GetTestableEngine()
	if testableEng == nil {
		return nil, fmt.Errorf("Could not find a storage engine that was testable")
	}
	return testableEng.GetTestConfig()
}

func openStore(create bool) {
	dvid.Infof("Opening test datastore.  Create = %v\n", create)
	if create {
		var err error
		testStore.backend, err = getTestStoreConfig()
		if err != nil {
			log.Fatalf("Unable to get testable storage configuration: %v\n", err)
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
	dvid.Infof("Reopening test datastore...\n")
	storage.Close()
	openStore(false)
}

func CloseTest() {
	dvid.Infof("Closing and deleting test datastore...\n")
	testableEng := storage.GetTestableEngine()
	if testableEng == nil {
		log.Fatalf("Could not find a storage engine that was testable")
	}
	testableEng.Delete(*testStore.backend["default"])

	testStore.Unlock()
}
