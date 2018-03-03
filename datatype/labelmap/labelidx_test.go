package labelmap

import (
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// Stress-test indexing during ingestion handling.
func TestIndexing(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})
	d, err := GetByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatal(err)
	}
	v, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		t.Fatal(err)
	}
	if d == nil || v == 0 {
		t.Fatalf("bad version returned\n")
	}
}
