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

	stores        map[Alias]dvid.Store
	instanceStore map[dvid.DataSpecifier]dvid.Store
	datatypeStore map[dvid.TypeString]dvid.Store

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

// GetStoreByAlias returns a store by the alias given to it in the configuration TOML file, e.g., "raid6".
func GetStoreByAlias(alias Alias) (dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting GetStoreByAlias")
	}
	store, found := manager.stores[alias]
	if !found {
		return nil, fmt.Errorf("could not find store with alias %q in TOML config file", alias)
	}
	return store, nil
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

	dataid := dvid.GetDataSpecifier(name, root)
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
		for alias, store := range manager.stores {
			dvid.Infof("Closing store %q: %s...\n", alias, store)
			store.Close()
		}
		manager.setup = false
	}
	manager.stores = nil
	manager.instanceStore = nil
	manager.datatypeStore = nil
	manager.defaultStore = nil
	manager.metadataStore = nil
}

// Initialize the storage systems.  Returns a bool + error where the bool is
// true if the metadata store is newly created and needs initialization.
// The map of store configurations should be keyed by either a datatype name,
// "default", or "metadata".
func Initialize(cmdline dvid.Config, backend *Backend) (createdMetadata bool, err error) {
	// Open all the backend stores
	manager.stores = make(map[Alias]dvid.Store, len(backend.Stores))
	var gotDefault, gotMetadata, createdDefault, lastCreated bool
	var lastStore dvid.Store
	for alias, dbconfig := range backend.Stores {
		var store dvid.Store
		for dbalias, db := range manager.stores {
			if db.Equal(dbconfig) {
				return false, fmt.Errorf("Store %q configuration is duplicate of store %q", alias, dbalias)
			}
		}
		store, created, err := NewStore(dbconfig)
		if err != nil {
			return false, err
		}
		switch alias {
		case backend.Metadata:
			gotMetadata = true
			createdMetadata = created
			manager.metadataStore = store
		case backend.Default:
			gotDefault = true
			createdDefault = created
			manager.defaultStore = store
		}
		manager.stores[alias] = store
		lastStore = store
		lastCreated = created
	}

	// Return if we don't have default or metadata stores.  Should really be caught
	// at configuration loading, but here as well as double check.
	if !gotDefault {
		if len(backend.Stores) == 1 {
			manager.defaultStore = lastStore
			createdDefault = lastCreated
		} else {
			return false, fmt.Errorf("either backend.default or a single store must be set in configuration TOML file")
		}
	}
	if !gotMetadata {
		manager.metadataStore = manager.defaultStore
		createdMetadata = createdDefault
	}
	dvid.Infof("Default store: %s\n", manager.defaultStore)
	dvid.Infof("Metadata store: %s\n", manager.metadataStore)

	// Make all data instance or datatype-specific store assignments.
	manager.instanceStore = make(map[dvid.DataSpecifier]dvid.Store)
	manager.datatypeStore = make(map[dvid.TypeString]dvid.Store)
	for dataspec, alias := range backend.Mapping {
		if dataspec == "default" || dataspec == "metadata" {
			continue
		}
		store, found := manager.stores[alias]
		if !found {
			err = fmt.Errorf("bad backend store alias: %q -> %q", dataspec, alias)
			return
		}
		// Cache the store for mapped datatype or data instance.
		name := strings.Trim(string(dataspec), "\"")
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 1:
			manager.datatypeStore[dvid.TypeString(name)] = store
		case 2:
			dataid := dvid.GetDataSpecifier(dvid.InstanceName(parts[0]), dvid.UUID(parts[1]))
			manager.instanceStore[dataid] = store
		default:
			err = fmt.Errorf("bad backend data specification: %s", dataspec)
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
