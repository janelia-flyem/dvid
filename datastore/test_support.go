/* Test datastore for testing datastore and other packages. */

package datastore

import (
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type testStoreT struct {
	sync.Mutex
	engines map[storage.Alias]storage.TestableEngine
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

func openStores(create bool, datamap ...DataStorageMap) {
	if len(datamap) > 1 {
		log.Fatalf("can't have more than one data mapping in opening stores")
	}
	dvid.Infof("Opening test datastore.  Create = %v\n", create)
	if create {
		var err error
		if len(datamap) == 1 {
			testStore.engines, testStore.backend, err = storage.GetTestableBackend(datamap[0].KVStores, datamap[0].LogStores)
		} else {
			testStore.engines, testStore.backend, err = storage.GetTestableBackend(nil, nil)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	initMetadata, err := storage.Initialize(dvid.Config{}, testStore.backend)
	if err != nil {
		log.Fatalf("Can't initialize test datastore: %v\n", err)
	}
	if err := Initialize(initMetadata, InstanceConfig{}); err != nil {
		log.Fatalf("Can't initialize datastore management: %v\n", err)
	}
	dvid.Infof("Storage initialized.  initMetadata = %v\n", initMetadata)
}

// DataStorageMap describes mappings from various instance and data type
// specifications to KV and Log stores.
type DataStorageMap struct {
	KVStores  storage.DataMap
	LogStores storage.DataMap
}

func OpenTest(datamap ...DataStorageMap) {
	testStore.Lock()
	openStores(true, datamap...)
}

// CloseReopenTest forces close and then reopening of the datastore, useful for testing
// persistence.  We only allow close/reopen when all tests not avaiting close/reopen are finished.
func CloseReopenTest(datamap ...DataStorageMap) {
	dvid.Infof("Closing test datastore for reopen test...\n")
	storage.Shutdown()
	dvid.Infof("Reopening test datastore...\n")
	openStores(false, datamap...)
}

func CloseTest() {
	Shutdown()
	for alias, engine := range testStore.engines {
		storeConfig := testStore.backend.Stores[alias]
		engine.Delete(storeConfig)
		dvid.Infof("Deleted test engine %q with backend %q\n", engine, storeConfig)
	}
	testStore.Unlock()
}
