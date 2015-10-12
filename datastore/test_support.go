/* Test datastore for testing datastore and other packages. */

package datastore

import (
	"fmt"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/uuid"
)

const (
	WebAddress   = "localhost:8657"
	RPCAddress   = "localhost:8658"
	WebClientDir = ""
)

type testStoreT struct {
	sync.Mutex
	config *dvid.StoreConfig
}

var (
	testStore = testStoreT{}
)

// NewTestRepo returns a new datastore.Repo suitable for testing.
func NewTestRepo() (dvid.UUID, dvid.VersionID) {
	uuid, err := NewRepo("testRepo", "A test repository", nil)
	if err != nil {
		log.Fatalf("Unable to create new testing repo: %v\n", err)
	}

	versionID, err := VersionFromUUID(uuid)
	if err != nil {
		log.Fatalf("Unable to get version ID from repo root UUID %s\n", uuid)
	}
	return uuid, versionID
}

// GetTestStoreConfig returns a configuration, amenable to testing, based on compiled-in engines.
func GetTestStoreConfig() (*dvid.StoreConfig, error) {
	testableEng := storage.GetTestableEngine()
	if testableEng == nil {
		return nil, fmt.Errorf("Could not find a storage engine that was testable")
	}
	dbname := fmt.Sprintf("dvid-test-%x", uuid.NewV4().Bytes())
	engConfig := dvid.EngineConfig{Engine: testableEng.GetName(), Path: dbname, Testing: true}
	return &dvid.StoreConfig{Mutable: engConfig}, nil
}

func openStore(create bool) {
	dvid.Infof("Opening test datastore.  Create = %v\n", create)
	if create {
		var err error
		testStore.config, err = GetTestStoreConfig()
		if err != nil {
			log.Fatalf("Unable to get testable storage configuration: %v\n", err)
		}
	}
	initMetadata, err := storage.Initialize(dvid.Config{}, testStore.config)
	if err != nil {
		log.Fatalf("Can't initialize test datastore: %v\n", err)
	}
	if err := Initialize(initMetadata); err != nil {
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
	testableEng.Delete(testStore.config.Mutable)

	testStore.Unlock()
}
