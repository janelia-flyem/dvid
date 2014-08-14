// +build !clustered,!gcloud

package tests

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/janelia-flyem/go/go-uuid/uuid"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/dvid/storage/local"
)

const (
	TestWebAddress   = "localhost:8657"
	TestRPCAddress   = "localhost:8658"
	TestWebClientDir = ""
)

var (
	engine storage.Engine
	count  int
	dbpath string
	mu     sync.Mutex
)

func UseStore() {
	mu.Lock()
	defer mu.Unlock()
	if count == 0 {
		dbpath = filepath.Join(os.TempDir(), fmt.Sprintf("dvid-test-%d", uuid.NewUUID()))
		var err error
		engine, err = local.CreateBlankStore(dbpath)
		if err != nil {
			log.Fatalf("Can't create a test datastore: %s\n", err.Error())
		}
		if err = storage.Initialize(engine, "testdb"); err != nil {
			log.Fatalf("Can't initialize test datastore: %s\n", err.Error())
		}
		if err = datastore.InitMetadata(engine); err != nil {
			log.Fatalf("Can't write blank datastore metadata: %s\n", err.Error())
		}
		if err = server.Initialize(); err != nil {
			log.Fatalf("Can't initialize server: %s\n", err.Error())
		}
	}
	count++
}

func CloseStore() {
	mu.Lock()
	defer mu.Unlock()
	count--
	if count == 0 {
		go func() {
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
		}()
	}
}
