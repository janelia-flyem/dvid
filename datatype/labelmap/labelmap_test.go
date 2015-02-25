package labelmap

import (
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"

	"github.com/janelia-flyem/dvid/datatype/labelblk"
)

var (
	labelsT, labelmapT datastore.TypeService
	labelsName         dvid.InstanceName
	testMu             sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()

	repo, versionID := tests.NewRepo()
	if labelsT == nil {
		var err error
		labelsT, err = datastore.TypeServiceByName(labelblk.TypeName)
		if err != nil {
			log.Fatalf("Can't get labelblk type: %s\n", err.Error())
		}
		config := dvid.NewConfig()
		config.SetVersioned(true)
		labelsName = "mylabels"
		_, err = repo.NewData(labelsT, labelsName, config)
		if err != nil {
			log.Fatalf("Error creating labelblk instance for labelmap test: %s\n", err.Error())
		}
		labelmapT, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get labelmap type: %s\n", err.Error())
		}

	}
	return repo, versionID
}

func TestLabelmapRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Creation of labelmap should require specification of underlying labelblk.
	config := dvid.NewConfig()
	config.SetVersioned(true)
	mylabelmap, err := repo.NewData(labelmapT, "mylabelmap", config)
	if err == nil {
		t.Fatalf("Creation of labelmap should have errored if no labels specified!\n")
	}

	// Try again with proper config.
	config.Set("Labels", string(labelsName))
	mylabelmap, err = repo.NewData(labelmapT, "mylabelmap", config)
	if err != nil {
		t.Fatalf("Unable to create labelmap instance: %s\n", err.Error())
	}

	// Make sure the binary serializations for LabelsRef are OK.
	data, ok := mylabelmap.(*Data)
	if !ok {
		t.Fatalf("Labelmap instance does not return *labelmap.Data\n")
	}
	data.Ready = true
	oldData := *data

	b, err := data.Labels.MarshalBinary()
	if err != nil {
		t.Fatalf("Error serializing label reference: %s\n", err.Error())
	}
	if len(b) == 0 {
		t.Fatalf("Empty serialization of label reference\n")
	}

	var ref LabelsRef
	if err = ref.UnmarshalBinary(b); err != nil {
		t.Fatalf("Unable to deserialize label reference: %s\n", err.Error())
	}
	if ref.name != dvid.InstanceName("mylabels") {
		t.Errorf("Bad (de)serialization of label reference: %s != %s\n",
			ref.name, "mylabels")
	}

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during labelmap persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mylabelmap")
	if err != nil {
		t.Fatalf("Can't get labelmap instance from reloaded test db: %s\n", err.Error())
	}
	data2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not labelmap.Data\n")
	}
	if !reflect.DeepEqual(oldData.Data, data2.Data) {
		t.Errorf("labelmap base Data has bad roundtrip:\nOriginal:\n%v\nReceived:\n%v\n",
			oldData.Data, data2.Data)
	}
	if !reflect.DeepEqual(oldData.Properties, data2.Properties) {
		t.Errorf("labelmap extended Data has bad roundtrip:\nOriginal:\n%v\nReceived:\n%v\n",
			oldData.Properties, data2.Properties)
	}
}
