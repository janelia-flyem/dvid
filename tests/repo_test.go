package tests

import (
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/imageblk"
	_ "github.com/janelia-flyem/dvid/datatype/imagetile"
)

func TestDataAndChildCreation(t *testing.T) {
	UseStore()
	defer CloseStore()

	repo, _ := NewRepo()

	grayscale8, err := datastore.TypeServiceByName("uint8blk")
	if err != nil {
		t.Fatalf("Could not get grayscale8 type: %s\n", err.Error())
	}

	config := dvid.NewConfig()
	_, err = repo.NewData(grayscale8, "grayscale", config)
	if err != nil {
		t.Errorf("Could not create uint8 data instance: %s\n", err.Error())
	}

	// Don't allow versioning on an unlocked node.
	_, err = repo.NewVersion(repo.RootUUID())
	if err == nil {
		t.Errorf("Should not allow versioning of an unlocked node!")
	}

	// Lock and retry
	if err = repo.Lock(repo.RootUUID()); err != nil {
		t.Errorf("Error locking node %s: %s\n", repo.RootUUID(), err.Error())
	}
	_, err = repo.NewVersion(repo.RootUUID())
	if err != nil {
		t.Errorf("Error trying to create child off root %s: %s\n", repo.RootUUID(), err.Error())
	}
}
