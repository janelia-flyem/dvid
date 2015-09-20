// +build !clustered,!gcloud

package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

var manager managerT

// managerT should be implemented for each type of storage implementation (local, clustered, gcloud)
// and it should fulfill a storage.Manager interface.
type managerT struct {
	setup bool

	// Tiers
	metadata  MetaDataStorer
	mutable   MutableStorer
	immutable ImmutableStorer

	uniqueDBs []Closer // Keep track of unique stores for closing.

	// Cached type-asserted interfaces
	graphEngine Engine
	graphDB     GraphDB
	graphSetter GraphSetter
	graphGetter GraphGetter
}

func MetaDataStore() (MetaDataStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting MetaDataStore")
	}
	return manager.metadata, nil
}

func MutableStore() (MutableStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting MutableStore")
	}
	return manager.mutable, nil
}

func ImmutableStore() (ImmutableStorer, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Key-value store not initialized before requesting ImmutableStorer")
	}
	return manager.immutable, nil
}

func GraphStore() (GraphDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Graph DB not initialized before requesting it")
	}
	return manager.graphDB, nil
}

// Close handles any storage-specific shutdown procedures.
func Close() {
	if manager.setup {
		dvid.Infof("Closing datastore...\n")
		for _, store := range manager.uniqueDBs {
			store.Close()
		}
	}
}

// Initialize the storage systems.  Returns a bool + error where the bool is
// true if the metadata store is newly created and needs initialization.
func Initialize(cmdline dvid.Config, sc *dvid.StoreConfig) (created bool, err error) {
	// Initialize the proper engines.
	engine := GetEngine(sc.Mutable.Engine)
	if engine == nil {
		return false, fmt.Errorf("Can't get mutable store %q", sc.Mutable.Engine)
	}
	mutableEng, ok := engine.(MutableEngine)
	if !ok {
		return false, fmt.Errorf("Mutable engine %q does not support NewMutableStore()", sc.Mutable.Engine)
	}
	mutableDB, newMutable, err := mutableEng.NewMutableStore(sc.Mutable)
	if err != nil {
		return false, err
	}
	manager.uniqueDBs = []Closer{mutableDB}

	var metadataDB MetaDataStorer
	var newMetadata bool
	engine = GetEngine(sc.MetaData.Engine)
	if engine == nil {
		// Reuse mutable.
		metadataDB, ok = mutableDB.(MetaDataStorer)
		if !ok {
			return false, fmt.Errorf("No MetaData store specified and Mutable store %q cannot fulfill interface", sc.Mutable.Engine)
		}
		dvid.Infof("No dedicated MetaData store.  Using mutable store %q\n", sc.Mutable.Engine)
		newMetadata = newMutable
	} else {
		metadataEng, ok := engine.(MetaDataEngine)
		if !ok {
			return false, fmt.Errorf("MetaData engine %q does not support NewMetaDataStore()", sc.MetaData.Engine)
		}
		metadataDB, newMetadata, err = metadataEng.NewMetaDataStore(sc.MetaData)
		if err != nil {
			return false, err
		}
		manager.uniqueDBs = append(manager.uniqueDBs, metadataDB)
	}

	var immutableDB ImmutableStorer
	engine = GetEngine(sc.Immutable.Engine)
	if engine == nil {
		// Reuse mutable.
		immutableDB, ok = mutableDB.(ImmutableStorer)
		if !ok {
			return false, fmt.Errorf("No MetaData store specified and Mutable store %q cannot fulfill interface", sc.Mutable.Engine)
		}
		dvid.Infof("No dedicated Immutable store.  Using mutable store %q\n", sc.Mutable.Engine)
	} else {
		immutableEng, ok := engine.(ImmutableEngine)
		if !ok {
			return false, fmt.Errorf("Immutable engine %q does not support NewImmutableStore()", sc.Immutable.Engine)
		}
		immutableDB, _, err = immutableEng.NewImmutableStore(sc.Immutable)
		if err != nil {
			return false, err
		}
		manager.uniqueDBs = append(manager.uniqueDBs, immutableDB)
	}

	// Initialize the stores using the engines.
	manager.metadata = metadataDB
	manager.mutable = mutableDB
	manager.immutable = immutableDB

	manager.setup = true

	// Setup the graph store using mutable store.
	kvDB, ok := mutableDB.(OrderedKeyValueDB)
	if !ok {
		return false, fmt.Errorf("Mutable DB %q is not a valid ordered key-value database", mutableDB)
	}

	manager.graphDB, err = NewGraphStore(kvDB)
	if err != nil {
		return false, err
	}
	manager.graphSetter, ok = manager.graphDB.(GraphSetter)
	if !ok {
		return false, fmt.Errorf("Database %q cannot support a graph setter", kvDB)
	}
	manager.graphGetter, ok = manager.graphDB.(GraphGetter)
	if !ok {
		return false, fmt.Errorf("Database %q cannot support a graph getter", kvDB)
	}
	return newMetadata, nil
}

// DeleteDataInstance removes a data instance across all versions and tiers of storage.
func DeleteDataInstance(data dvid.Data) error {
	if !manager.setup {
		return fmt.Errorf("Can't delete data instance %q before storage manager is initialized", data.DataName())
	}

	// Determine all database tiers that are distinct.
	dbs := []OrderedKeyValueDB{manager.mutable}
	if manager.mutable != manager.immutable {
		dbs = append(dbs, manager.immutable)
	}

	// For each storage tier, remove all key-values with the given instance id.
	dvid.Infof("Starting delete of instance %d: name %q, type %s\n", data.InstanceID(), data.DataName(), data.TypeName())
	ctx := NewDataContext(data, 0)
	for _, db := range dbs {
		if err := db.DeleteAll(ctx, true); err != nil {
			return err
		}
	}
	return nil
}
