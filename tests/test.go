/*
	The tests package provides integration testing for DVID.
*/
package tests

import (
	"log"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func init() {
	dvid.SetLogMode(dvid.WarningMode)
}

// NewRepo returns a new datastore.Repo suitable for testing.
func NewRepo() (datastore.Repo, dvid.VersionID) {
	repo, err := datastore.NewRepo("testRepo", "A test repository")
	if err != nil {
		log.Fatalf("Unable to create new testing repo: %s\n", err.Error())
	}

	versionID, err := datastore.VersionFromUUID(repo.RootUUID())
	if err != nil {
		log.Fatalf("Unable to get version ID from repo root UUID %s\n", repo.RootUUID())
	}
	return repo, versionID
}
