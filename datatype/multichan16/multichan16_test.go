package multichan16

import (
	"log"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	dtype  datastore.TypeService
	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if dtype == nil {
		var err error
		dtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get multichan16 type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

func TestBasic(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	config := dvid.NewConfig()
	config.SetVersioned(true)
	_, err := repo.NewData(dtype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new multichan16 instance: %s\n", err.Error())
	}
}
