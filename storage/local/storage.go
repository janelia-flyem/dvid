// +build !clustered,!gcloud

package local

import (
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
	return storage.Initialize(kvEngine, Version)
}
