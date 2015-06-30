// +build !clustered,!gcloud

package local

import (
	"fmt"
	"os"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Initialize the storage systems given a configuration, path to datastore.  Unlike cluster
// and google cloud storage systems, which get initialized on DVID start using init(), the
// local storage system waits until it receives a path and configuration data from a
// "serve" command.
func Initialize(path string, config dvid.Config) error {
	create := false
	kvEngine, err := NewKeyValueStore(path, create, config)
	if err != nil {
		return err
	}
	return storage.Initialize(kvEngine, kvEngine.String())
}

// CreateBlankStore creates a new local key-value database at the given path,
// deleting any data that was present in the given path.
func CreateBlankStore(path string) (storage.Engine, error) {
	// See if an old test database exists.  If so, delete.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.RemoveAll(path); err != nil {
			return nil, fmt.Errorf("Can't delete old datastore %q: %v", path, err)
		}
	}

	// Make a directory at the path.
	if err := os.MkdirAll(path, 0744); err != nil {
		return nil, fmt.Errorf("Can't make directory at %s: %v", path, err)
	}

	// Make the local key value store
	create := true
	store, err := NewKeyValueStore(path, create, dvid.Config{})
	if err != nil {
		return nil, fmt.Errorf("Can't create key-value store: %v", err)
	}
	return store, nil
}
