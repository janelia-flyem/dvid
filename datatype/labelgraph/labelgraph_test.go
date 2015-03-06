package labelgraph

import (
	"log"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	dtype datastore.TypeService
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	if dtype == nil {
		var err error
		dtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get labelgraph type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// Make sure new labelgraph data have different IDs.
func TestNewLabelgraphDifferent(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice1, err := repo.NewData(dtype, "lg1", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 1: %s\n", err.Error())
	}
	data1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not labelgraph.Data\n")
	}
	dataservice2, err := repo.NewData(dtype, "lg2", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 2: %s\n", err.Error())
	}
	data2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not labelgraph.Data\n")
	}
	if data1.InstanceID() == data2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			data1.InstanceID(), data2.InstanceID())
	}
}

func TestLabelgraphRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := repo.NewData(dtype, "lg", config)
	if err != nil {
		t.Errorf("Unable to create labelgraph instance: %s\n", err.Error())
	}
	lgdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labelgraph data service into labelgraph.Data\n")
	}
	oldData := *lgdata

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during labelgraph persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("lg")
	if err != nil {
		t.Fatalf("Can't get labelgraph instance from reloaded test db: %s\n", err.Error())
	}
	lgdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not labelgraph.Data\n")
	}
	if !reflect.DeepEqual(oldData, *lgdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *lgdata2)
	}
}
