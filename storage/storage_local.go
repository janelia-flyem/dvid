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

	// cache the default stores at both global and datatype level
	defaultStore  dvid.Store
	metadataStore dvid.Store
	datatypeStore map[dvid.TypeString]dvid.Store

	uniqueDBs []dvid.Store // Keep track of unique stores for closing.

	// Cached type-asserted interfaces
	graphEngine Engine
	graphDB     GraphDB
	graphSetter GraphSetter
	graphGetter GraphGetter
}

func DefaultStore() (dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting default store")
	}
	if manager.defaultStore == nil {
		return nil, fmt.Errorf("No default store has been initialized")
	}
	return manager.defaultStore, nil
}

func MetaDataKVStore() (OrderedKeyValueDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting MetaDataStore")
	}
	kvstore, ok := manager.metadataStore.(OrderedKeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Metadata store %q is not an ordered key-value store!", manager.metadataStore)
	}
	return kvstore, nil
}

func DefaultKVStore() (KeyValueDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting DefaultStore")
	}
	kvstore, ok := manager.defaultStore.(KeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Default store %q is not a key-value store!", manager.defaultStore)
	}
	return kvstore, nil
}

func DefaultOrderedKVStore() (OrderedKeyValueDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting DefaultStore")
	}
	kvstore, ok := manager.defaultStore.(OrderedKeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Default store %q is not an ordered key-value store!", manager.defaultStore)
	}
	return kvstore, nil
}

func GraphStore() (GraphDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting GraphStore")
	}
	return manager.graphDB, nil
}

// GetAssignedStore returns the store assigned to a particular datatype.
func GetAssignedStore(typename dvid.TypeString) (dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting store for %s", typename)
	}
	store, found := manager.datatypeStore[typename]
	if !found {
		return manager.defaultStore, nil
	}
	return store, nil
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

// check if this configuration has already been setup; if not, setup and record availability
func addUniqueDB(c dvid.StoreConfig) (dvid.Store, bool, error) {
	// Check if this store configuration already has been setup
	for _, db := range manager.uniqueDBs {
		if db.Equal(c) {
			return db, false, nil
		}
	}

	// Open new store given this configuration
	db, created, err := NewStore(c)
	if err != nil {
		return nil, false, err
	}
	manager.uniqueDBs = append(manager.uniqueDBs, db)
	return db, created, err
}

// Initialize the storage systems.  Returns a bool + error where the bool is
// true if the metadata store is newly created and needs initialization.
// The map of store configurations should be keyed by either a datatype name,
// "default", or "metadata".
func Initialize(cmdline dvid.Config, sc map[string]dvid.StoreConfig) (created bool, err error) {
	// Get required default store.
	defaultConfig, found := sc["default"]
	if !found {
		// Check if legacy "mutable" is available
		defaultConfig, found = sc["mutable"]
		if !found {
			return false, fmt.Errorf("storage.default needs to be set in configuration TOML file")
		}
		dvid.Warningf("storage.mutable in TOML config has been deprecated; please use storage.default\n")
	}
	var defaultStore dvid.Store
	defaultStore, created, err = NewStore(defaultConfig)
	if err != nil {
		return
	}
	manager.defaultStore = defaultStore
	manager.uniqueDBs = []dvid.Store{defaultStore}

	// Was a metadata store configured?
	var metadataStore dvid.Store
	metadataConfig, found := sc["metadata"]
	if found && !defaultStore.Equal(metadataConfig) {
		metadataStore, created, err = addUniqueDB(metadataConfig)
		if err != nil {
			return
		}
	} else {
		metadataStore = defaultStore
	}
	manager.metadataStore = metadataStore

	// Load any datatype-specific stores, checking to see if it's already handled.
	manager.datatypeStore = make(map[dvid.TypeString]dvid.Store)
	for name, c := range sc {
		if name == "default" || name == "metadata" {
			continue
		}
		var store dvid.Store
		store, _, err = addUniqueDB(c)
		if err != nil {
			err = fmt.Errorf("storage.%s %v", name, err)
			return
		}
		manager.datatypeStore[dvid.TypeString(name)] = store
	}
	manager.setup = true

	// Setup the graph store
	var store dvid.Store
	store, err = GetAssignedStore("labelgraph")
	if err != nil {
		return
	}
	var ok bool
	kvdb, ok := store.(OrderedKeyValueDB)
	if !ok {
		return false, fmt.Errorf("assigned labelgraph store %q isn't ordered kv db", store)
	}
	manager.graphDB, err = NewGraphStore(kvdb)
	if err != nil {
		return false, err
	}
	manager.graphSetter, ok = manager.graphDB.(GraphSetter)
	if !ok {
		return false, fmt.Errorf("Database %q cannot support a graph setter", kvdb)
	}
	manager.graphGetter, ok = manager.graphDB.(GraphGetter)
	if !ok {
		return false, fmt.Errorf("Database %q cannot support a graph getter", kvdb)
	}
	return
}

// DeleteDataInstance removes a data instance.
func DeleteDataInstance(data dvid.Data) error {
	if !manager.setup {
		return fmt.Errorf("Can't delete data instance %q before storage manager is initialized", data.DataName())
	}

	// Get the store for the data instance.
	store, err := data.BackendStore()
	if err != nil {
		return err
	}
	db, ok := store.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("store assigned to data %q is not an ordered kv db with ability to delete all", data.DataName())
	}

	dvid.Infof("Starting delete of instance %d: name %q, type %s\n", data.InstanceID(), data.DataName(), data.TypeName())
	ctx := NewDataContext(data, 0)
	if err := db.DeleteAll(ctx, true); err != nil {
		return err
	}
	return nil
}
