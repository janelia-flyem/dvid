package keyvalue

import (
	"bytes"
	"log"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	kvtype datastore.TypeService
	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if kvtype == nil {
		var err error
		kvtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get keyvalue type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// Make sure new keyvalue data have different IDs.
func TestNewKeyvalueDifferent(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice1, err := repo.NewData(kvtype, "instance1", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kv1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not keyvalue.Data\n")
	}
	if kv1.DataName() != "instance1" {
		t.Errorf("New keyvalue data instance name set incorrectly: %q != %q\n",
			kv1.DataName(), "instance1")
	}

	dataservice2, err := repo.NewData(kvtype, "instance2", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kv2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not keyvalue.Data\n")
	}

	if kv1.InstanceID() == kv2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			kv1.InstanceID(), kv2.InstanceID())
	}
}

func TestKeyValueRoundTrip(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(kvtype, "roundtripper", config)
	if err != nil {
		t.Errorf("Error creating new keyvalue instance: %s\n", err.Error())
	}
	kvdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not keyvalue.Data\n")
	}

	ctx := datastore.NewVersionedContext(dataservice, versionID)

	keyStr := "testkey"
	value := []byte("I like Japan and this is some unicode: \u65e5\u672c\u8a9e")

	if err = kvdata.PutData(ctx, keyStr, value); err != nil {
		t.Errorf("Could not put keyvalue data: %s\n", err.Error())
	}

	retrieved, found, err := kvdata.GetData(ctx, keyStr)
	if err != nil {
		t.Fatalf("Could not get keyvalue data: %s\n", err.Error())
	}
	if !found {
		t.Fatalf("Could not find put keyvalue\n")
	}
	if bytes.Compare(value, retrieved) != 0 {
		t.Errorf("keyvalue retrieved %q != put %q\n", string(retrieved), string(value))
	}
}
