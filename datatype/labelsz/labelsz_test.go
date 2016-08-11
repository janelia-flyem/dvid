package labelsz

import (
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var (
	syntype datastore.TypeService
	testMu  sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if syntype == nil {
		var err error
		syntype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get synapse type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}
