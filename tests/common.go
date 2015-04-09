/*
	The tests package provides integration testing for DVID.
*/
package tests

import (
	"log"
	"math/rand"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func init() {
	dvid.SetLogMode(dvid.WarningMode)
}

// NewRepo returns a new datastore.Repo suitable for testing.
func NewRepo() (datastore.Repo, dvid.VersionID) {
	repo, err := datastore.NewRepo("testRepo", "A test repository", nil)
	if err != nil {
		log.Fatalf("Unable to create new testing repo: %s\n", err.Error())
	}

	versionID, err := datastore.VersionFromUUID(repo.RootUUID())
	if err != nil {
		log.Fatalf("Unable to get version ID from repo root UUID %s\n", repo.RootUUID())
	}
	return repo, versionID
}

// RandomBytes returns a slices of random bytes.
func RandomBytes(numBytes int32) []byte {
	buf := make([]byte, numBytes)
	src := rand.NewSource(time.Now().UnixNano())
	var offset int32
	for {
		val := int64(src.Int63())
		for i := 0; i < 8; i++ {
			if offset >= numBytes {
				return buf
			}
			buf[offset] = byte(val)
			offset++
			val >>= 8
		}
	}
}
