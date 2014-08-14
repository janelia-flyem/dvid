package labelmap

import (
	"log"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"

	"github.com/janelia-flyem/dvid/datatype/labels64"
)

var (
	labelsT, labelmapT datastore.TypeService
	testMu             sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if labelsT == nil {
		var err error
		labelsT, err = datastore.TypeServiceByName(labels64.TypeName)
		if err != nil {
			log.Fatalf("Can't get labels64 type: %s\n", err.Error())
		}
		labelmapT, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get labelmap type: %s\n", err.Error())
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
	_, err := repo.NewData(labelsT, "mylabels", config)
	if err != nil {
		t.Fatalf("Error creating new labels64 instance: %s\n", err.Error())
	}

	// Creation of labelmap should require specification of underlying labels64.
	mylabelmap, err := repo.NewData(labelmapT, "mylabelmap", config)
	if err == nil {
		t.Fatalf("Creation of labelmap should have errored if no labels specified!\n")
	}

	// Try again with proper config.
	config.Set("Labels", "mylabels")
	mylabelmap, err = repo.NewData(labelmapT, "mylabelmap", config)
	if err != nil {
		t.Fatalf("Unable to create labelmap instance: %s\n", err.Error())
	}

	// Make sure the binary serializations for LabelsRef are OK.
	data, ok := mylabelmap.(*Data)
	if !ok {
		t.Fatalf("Labelmap instance does not return *labelmap.Data\n")
	}

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
	if ref.name != dvid.DataString("mylabels") {
		t.Errorf("Bad (de)serialization of label reference: %s != %s\n",
			ref.name, "mylabels")
	}
}
