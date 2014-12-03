// +build !clustered,!gcloud

package storage

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
)

var manager managerT

// managerT should be implemented for each type of storage implementation (local, clustered, gcloud)
// and it should fulfill a storage.Manager interface.
type managerT struct {
	// True if Setupmanager and SetupTiers have been called.
	setup bool

	// Tiers
	metadata  MetaDataStorer
	smalldata SmallDataStorer
	bigdata   BigDataStorer

	// Cached type-asserted interfaces
	graphEngine Engine
	graphDB     GraphDB
	graphSetter GraphSetter
	graphGetter GraphGetter

	enginesAvail []string
}

func MetaDataStore() (MetaDataStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting MetaDataStore")
	}
	return manager.metadata, nil
}

func SmallDataStore() (SmallDataStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting SmallDataStore")
	}
	return manager.smalldata, nil
}

func BigDataStore() (BigDataStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting BigaDataStore")
	}
	return manager.bigdata, nil
}

func GraphStore() (GraphDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Graph DB not initialized before requesting it")
	}
	return manager.graphDB, nil
}

// EnginesAvailable returns a description of the available storage engines.
func EnginesAvailable() string {
	return strings.Join(manager.enginesAvail, "; ")
}

// Shutdown handles any storage-specific shutdown procedures.
func Shutdown() {
	// Place to be put any storage engine shutdown code.
}

// Initialize the storage systems given a configuration, path to datastore.  Unlike cluster
// and google cloud storage systems, which get initialized on DVID start using init(), the
// local storage system waits until it receives a path and configuration data from a
// "serve" command.
func Initialize(kvEngine Engine, description string) error {
	kvDB, ok := kvEngine.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("Database %q is not a valid ordered key-value database", kvEngine.String())
	}

	var err error
	manager.graphEngine, err = NewGraphStore(kvDB)
	if err != nil {
		return err
	}
	manager.graphDB, ok = manager.graphEngine.(GraphDB)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph database", kvEngine.String())
	}
	manager.graphSetter, ok = manager.graphEngine.(GraphSetter)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph setter", kvEngine.String())
	}
	manager.graphGetter, ok = manager.graphEngine.(GraphGetter)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph getter", kvEngine.String())
	}

	// Setup the three tiers of storage.  In the case of a single local server with
	// embedded storage engines, it's simpler because we don't worry about cross-process
	// synchronization.
	manager.metadata = kvDB
	manager.smalldata = kvDB
	manager.bigdata = kvDB

	manager.enginesAvail = append(manager.enginesAvail, description)

	manager.setup = true
	return nil
}

// DeleteDataInstance removes all data context key-value pairs from all tiers of storage.
func DeleteDataInstance(instanceID dvid.InstanceID) error {
	if !manager.setup {
		return fmt.Errorf("Can't delete data instance %d before storage manager is initialized", instanceID)
	}

	// Determine all database tiers that are distinct.
	dbs := []OrderedKeyValueDB{manager.smalldata}
	if manager.smalldata != manager.bigdata {
		dbs = append(dbs, manager.bigdata)
	}

	// For each storage tier, remove all key-values with the given instance id.
	for _, db := range dbs {
		minKey, maxKey := DataContextKeyRange(instanceID)
		if err := db.DeleteRange(nil, minKey, maxKey); err != nil {
			return err
		}
	}
	return nil
}
