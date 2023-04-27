//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package storage

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
)

var manager managerT
var datatypes map[dvid.TypeString]struct{} // set of all compiled datatypes

type managerT struct {
	setup bool

	metadataStore dvid.Store

	stores map[Alias]dvid.Store

	storeMap storeAssignment // database assignments for data instances
	logMap   storeAssignment // log assignments for data instances

	gcache groupcacheT // groupcache support
}

// mappings of data instances to stores using various criteria
type storeAssignment struct {
	defaultStore dvid.Store
	instance     map[dvid.DataSpecifier]dvid.Store
	datatype     map[dvid.TypeString]dvid.Store
}

func (s *storeAssignment) init() {
	s.instance = make(map[dvid.DataSpecifier]dvid.Store)
	s.datatype = make(map[dvid.TypeString]dvid.Store)
}

func (s *storeAssignment) cache(store dvid.Store, dataspec dvid.DataSpecifier) error {
	spec := strings.Trim(string(dataspec), "\"")
	instanceParts := strings.Split(spec, ":")
	tagParts := strings.Split(spec, "=")
	switch {
	case len(instanceParts) == 2:
		dataid := dvid.GetDataSpecifier(dvid.InstanceName(instanceParts[0]), dvid.UUID(instanceParts[1]))
		s.instance[dataid] = store
	case len(tagParts) == 2:
		dataid := dvid.GetDataSpecifierByTag(tagParts[0], tagParts[1])
		s.instance[dataid] = store
	case len(instanceParts) == 1 && len(tagParts) == 1: // either datatype or data UUID
		for t := range datatypes {
			if t == dvid.TypeString(spec) {
				s.datatype[dvid.TypeString(spec)] = store
				return nil
			}
		}
		s.instance[dvid.DataSpecifier(spec)] = store // Assume must be DataUUID
	default:
		return fmt.Errorf("bad backend data specification: %s", dataspec)
	}
	return nil
}

func getAssignedStore(assignMap storeAssignment, ds DataSpec) (store dvid.Store, spec dvid.DataSpecifier) {
	var found bool

	// Assignment priority:
	// 1. DataUUID
	spec = dvid.DataSpecifier(ds.DataUUID())
	store, found = assignMap.instance[spec]
	if found {
		dvid.Infof("Data %q assigned to store %q by DataUUID (%s).\n",
			ds.DataName(), store, ds.DataUUID())
		return
	}

	// 2. InstanceName, VersionUUID
	spec = dvid.GetDataSpecifier(ds.DataName(), ds.RootUUID())
	store, found = assignMap.instance[spec]
	if found {
		dvid.Infof("Data %q assigned to store %q by InstanceName (%s), RootUUID (%s).\n",
			ds.DataName(), store, ds.DataName(), ds.RootUUID())
		return
	}

	// 3. Tags
	for tag, value := range ds.Tags() {
		spec = dvid.GetDataSpecifierByTag(tag, value)
		store, found = assignMap.instance[spec]
		if found {
			dvid.Infof("Data %q assigned to store %q by tag %q (%s).\n",
				ds.DataName(), store, tag, value)
			return
		}
	}

	// 4. TypeName
	spec = dvid.DataSpecifier(ds.TypeName())
	store, found = assignMap.datatype[ds.TypeName()]
	if found {
		dvid.Infof("Data %q assigned to store %q by TypeName (%s).\n",
			ds.DataName(), store, ds.TypeName())
		return
	}

	// 5. Default store
	return assignMap.defaultStore, ""
}

func getManagerStore(ds DataSpec, logMap bool) (store dvid.Store, err error) {
	if !manager.setup {
		return nil, fmt.Errorf("can't get assigned store before storage manager is setup")
	}

	var spec dvid.DataSpecifier
	if logMap {
		store, spec = getAssignedStore(manager.logMap, ds)
	} else {
		store, spec = getAssignedStore(manager.storeMap, ds)

		// See if this is using caching and if so, establish a wrapper around it.
		if _, supported := manager.gcache.supported[spec]; supported {
			store, err = wrapGroupcache(store, manager.gcache.cache)
			if err != nil {
				dvid.Errorf("Unable to wrap groupcache around store %s for data instance %q (%s): %v\n",
					store, ds.DataName(), ds.DataUUID(), err)
			} else {
				dvid.Infof("Returning groupcache-wrapped store %s for data instance %q (%s)\n",
					store, ds.DataName(), ds.DataUUID())
			}
		}
	}
	return
}

// Initialize the storage systems.  Returns a bool + error where the bool is
// true if the metadata store is newly created and needs initialization.
// The map of store configurations should be keyed by either a datatype name,
// "default", or "metadata".
func Initialize(cmdline dvid.Config, backend *Backend, compiledTypes map[dvid.TypeString]struct{}) (createdMetadata bool, err error) {
	datatypes = compiledTypes

	// Allocate maps for stores.
	manager.stores = make(map[Alias]dvid.Store, len(backend.Stores))
	manager.storeMap.init()
	manager.logMap.init()

	// Open all the backend stores
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
			dvid.TimeErrorf("dbconfig: %v\n", dbconfig)
			return false, fmt.Errorf("bad store %q: %v", alias, err)
		}
		if alias == backend.Metadata {
			gotMetadata = true
			createdMetadata = created
			manager.metadataStore = store
		}
		if alias == backend.DefaultKVDB {
			gotDefault = true
			createdDefault = created
			manager.storeMap.defaultStore = store
		}
		if alias == backend.DefaultLog {
			manager.logMap.defaultStore = store
		}
		manager.stores[alias] = store
		lastStore = store
		lastCreated = created
	}

	// Return if we don't have default or metadata stores.  Should really be caught
	// at configuration loading, but here as well as double check.
	if !gotDefault {
		if len(backend.Stores) == 1 {
			manager.storeMap.defaultStore = lastStore
			createdDefault = lastCreated
		} else {
			return false, fmt.Errorf("either backend.default or a single store must be set in configuration TOML file")
		}
	}
	if !gotMetadata {
		manager.metadataStore = manager.storeMap.defaultStore
		createdMetadata = createdDefault
	}
	dvid.TimeInfof("Default kv store: %s\n", manager.storeMap.defaultStore)
	dvid.TimeInfof("Default log store: %s\n", manager.logMap.defaultStore)
	dvid.TimeInfof("Metadata store: %s\n", manager.metadataStore)

	// Setup the groupcache if specified.
	err = setupGroupcache(backend.Groupcache)
	if err != nil {
		return
	}

	// Make all data instance, tag-specific, or datatype-specific store assignments.
	for dataspec, alias := range backend.KVAssign {
		if dataspec == "default" || dataspec == "metadata" {
			continue
		}
		store, found := manager.stores[alias]
		if !found {
			err = fmt.Errorf("bad backend store alias: %q -> %q", dataspec, alias)
			return
		}
		if err = manager.storeMap.cache(store, dataspec); err != nil {
			return
		}
	}
	for dataspec, alias := range backend.LogAssign {
		if dataspec == "default" {
			continue
		}
		store, found := manager.stores[alias]
		if !found {
			err = fmt.Errorf("bad backend store alias: %q -> %q", dataspec, alias)
			return
		}
		if err = manager.logMap.cache(store, dataspec); err != nil {
			return
		}
	}
	manager.setup = true
	return
}

func AllStores() (map[Alias]dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting stores")
	}
	return manager.stores, nil
}

func DefaultKVStore() (dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting default kv store")
	}
	if manager.storeMap.defaultStore == nil {
		return nil, fmt.Errorf("No default kv store has been initialized")
	}
	return manager.storeMap.defaultStore, nil
}

func DefaultLogStore() (dvid.Store, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting default log store")
	}
	if manager.logMap.defaultStore == nil {
		return nil, fmt.Errorf("No default log store has been initialized")
	}
	return manager.logMap.defaultStore, nil
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

func DefaultKVDB() (KeyValueDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting DefaultKVDB")
	}
	kvstore, ok := manager.storeMap.defaultStore.(KeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Default store %q is not a key-value store!", manager.storeMap.defaultStore)
	}
	return kvstore, nil
}

func DefaultOrderedKVDB() (OrderedKeyValueDB, error) {
	if !manager.setup {
		return nil, fmt.Errorf("Storage manager not initialized before requesting DefaultKVDB")
	}
	kvstore, ok := manager.storeMap.defaultStore.(OrderedKeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Default store %q is not an ordered key-value store!", manager.storeMap.defaultStore)
	}
	return kvstore, nil
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

// DataSpec is an interface for properties of data instances useful for store assignment.
type DataSpec interface {
	DataName() dvid.InstanceName
	DataUUID() dvid.UUID
	RootUUID() dvid.UUID
	Tags() map[string]string
	TypeName() dvid.TypeString
}

// GetAssignedStore returns the store based on DataUUID, (Name, RootUUID), tag, or type,
// in that order.  In some cases, this store may include a caching wrapper if the data
// instance has been configured to use groupcache.
func GetAssignedStore(ds DataSpec) (dvid.Store, error) {
	return getManagerStore(ds, false)
}

// GetAssignedLog returns the append-only log based on DataUUID, (Name, RootUUID), tag,
// or type, in that order.
func GetAssignedLog(ds DataSpec) (dvid.Store, error) {
	return getManagerStore(ds, true)
}

// Shutdown handles any storage-specific shutdown procedures.
func Shutdown() {
	if manager.setup {
		for alias, store := range manager.stores {
			dvid.Infof("Closing store %q: %s...\n", alias, store)
			store.Close()
		}
		manager.setup = false
	}
	KafkaShutdown()
	manager = managerT{}
}

// DeleteDataInstance removes a data instance.
func DeleteDataInstance(data dvid.Data) error {
	if !manager.setup {
		return fmt.Errorf("Can't delete data instance %q before storage manager is initialized", data.DataName())
	}

	// Get the store for the data instance.
	store, err := data.KVStore()
	if err != nil {
		return err
	}
	db, ok := store.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("store assigned to data %q is not an ordered kv db with ability to delete all", data.DataName())
	}

	dvid.Infof("Starting delete of instance %d: name %q, type %s\n", data.InstanceID(), data.DataName(), data.TypeName())
	ctx := NewDataContext(data, 0)
	if err := db.DeleteAll(ctx); err != nil {
		return err
	}
	return nil
}
