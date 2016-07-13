package multichan16

import (
	"log"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

var (
	dtype  datastore.TypeService
	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if dtype == nil {
		var err error
		dtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get multichan16 type: %v\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func TestBasic(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	_, err := datastore.NewData(uuid, dtype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new multichan16 instance: %v\n", err)
	}
}

func TestMultichan16RepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, dtype, "mymultichan16", config)
	if err != nil {
		t.Errorf("Unable to create multichan16 instance: %v\n", err)
	}
	mcdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast multichan16 data service into multichan16.Data\n")
	}
	oldData := *mcdata

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, mcdata); err != nil {
		t.Fatalf("Unable to save repo during multichan16 persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "mymultichan16")
	if err != nil {
		t.Fatalf("Can't get multichan16 instance from reloaded test db: %v\n", err)
	}
	mcdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not multichan16.Data\n")
	}
	if !oldData.Equals(mcdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *mcdata2)
	}
}
