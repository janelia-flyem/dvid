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
	setup bool

	// cache the default stores at both global and datatype level
	defaultStore  dvid.Store
	metadataStore dvid.Store

	instanceStore map[string]dvid.Store
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

// GetAssignedStore returns the store assigned based on instance or type.
func GetAssignedStore(dataname dvid.InstanceName, uuid dvid.UUID, typename dvid.TypeString) (dvid.Store, error) {
	store, found, err := GetAssignedStoreByInstance(dataname, uuid)
	if err != nil {
		return nil, err
	}
	if found {
		return store, nil
	}
	store, err = GetAssignedStoreByType(typename)
	if err != nil {
		return nil, fmt.Errorf("Cannot get assigned store for data %q, type %q", dataname, typename)
	}
	return store, nil
}

// GetAssignedStoreByInstance returns the store assigned to a particular data instance.
func GetAssignedStoreByInstance(name dvid.InstanceName, root dvid.UUID) (store dvid.Store, found bool, err error) {
	if !manager.setup {
		err = fmt.Errorf("Storage manager not initialized before requesting store for %s/%s", name, root)
		return
	}
	// For now, return the root UUID of the DAG containing the given UUID.
	// TODO: Adjust when we make UUID-based data instances.

	dataid := string(name) + string(root)
	store, found = manager.instanceStore[dataid]
	return
}

// GetAssignedStoreByType returns the store assigned to a particular datatype.
func GetAssignedStoreByType(typename dvid.TypeString) (dvid.Store, error) {
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
	dvid.Infof("default store -> %s\n", defaultStore)

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
	dvid.Infof("metadata store -> %s\n", metadataStore)

	// Load any data instance or datatype-specific stores, checking
	// to see if it's already handled.
	manager.instanceStore = make(map[string]dvid.Store)
	manager.datatypeStore = make(map[dvid.TypeString]dvid.Store)
	for name, c := range sc {
		if name == "default" || name == "metadata" {
			continue
		}
		var store dvid.Store
		store, _, err = addUniqueDB(c)
		if err != nil {
			err = fmt.Errorf("storage.%s: %v", name, err)
			return
		}

		// Cache the store to datatype or data instance.
		name = strings.Trim(name, "\"")
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 1:
			manager.datatypeStore[dvid.TypeString(name)] = store
		case 2:
			manager.instanceStore[parts[0]+parts[1]] = store
		default:
			err = fmt.Errorf("bad store name specification %q", name)
			return
		}
	}
	manager.setup = true

	// Setup the graph store
	var store dvid.Store
	store, err = GetAssignedStoreByType("labelgraph")
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
