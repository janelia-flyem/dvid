// +build !clustered,!gcloud

package storage

import (
	"fmt"
	"strings"
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
	kvEngine    Engine
	kvDB        OrderedKeyValueDB
	kvSetter    OrderedKeyValueSetter
	kvGetter    OrderedKeyValueGetter
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
	var ok bool

	manager.kvEngine = kvEngine

	manager.kvDB, ok = manager.kvEngine.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("Database %q is not a valid ordered key-value database", kvEngine.GetName())
	}
	manager.kvSetter, ok = manager.kvEngine.(OrderedKeyValueSetter)
	if !ok {
		return fmt.Errorf("Database %q is not a valid ordered key-value setter", kvEngine.GetName())
	}
	manager.kvGetter, ok = manager.kvEngine.(OrderedKeyValueGetter)
	if !ok {
		return fmt.Errorf("Database %q is not a valid ordered key-value getter", kvEngine.GetName())
	}

	var err error
	manager.graphEngine, err = NewGraphStore(manager.kvDB)
	if err != nil {
		return err
	}
	manager.graphDB, ok = manager.graphEngine.(GraphDB)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph database", kvEngine.GetName())
	}
	manager.graphSetter, ok = manager.graphEngine.(GraphSetter)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph setter", kvEngine.GetName())
	}
	manager.graphGetter, ok = manager.graphEngine.(GraphGetter)
	if !ok {
		return fmt.Errorf("Database %q cannot support a graph getter", kvEngine.GetName())
	}

	// Setup the three tiers of storage.  In the case of a single local server with
	// embedded storage engines, it's simpler because we don't worry about cross-process
	// synchronization.
	manager.metadata = manager.kvDB
	manager.smalldata = manager.kvDB
	manager.bigdata = manager.kvDB

	manager.enginesAvail = append(manager.enginesAvail, description)

	manager.setup = true
	return nil
}
