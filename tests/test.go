/*
	The tests package provides integration testing for DVID.
*/
package tests

import (
	"log"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

func init() {
	dvid.SetLogMode(dvid.WarningMode)
}

// NewRepo returns a new Repo suitable for testing.
func NewRepo() (datastore.Repo, dvid.VersionID) {
	if server.Repos == nil {
		log.Fatalf("NewRepo(): Cannot return test Repo when test store hasn't been initialized.\n")
	}

	repo, err := server.Repos.NewRepo()
	if err != nil {
		log.Fatalf("Unable to create new testing repo: %s\n", err.Error())
	}

	versionID, err := server.Repos.VersionFromUUID(repo.RootUUID())
	if err != nil {
		log.Fatalf("Unable to get version ID from repo root UUID %s\n", repo.RootUUID())
	}
	return repo, versionID
}
