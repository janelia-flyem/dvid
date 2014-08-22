package multichan16

import (
	"log"
	"reflect"
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

func TestMultichan16RepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(dtype, "mymultichan16", config)
	if err != nil {
		t.Errorf("Unable to create multichan16 instance: %s\n", err.Error())
	}
	mcdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast multichan16 data service into multichan16.Data\n")
	}
	oldData := *mcdata

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during multichan16 persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mymultichan16")
	if err != nil {
		t.Fatalf("Can't get multichan16 instance from reloaded test db: %s\n", err.Error())
	}
	mcdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not multichan16.Data\n")
	}
	if !reflect.DeepEqual(oldData, *mcdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *mcdata2)
	}
}
