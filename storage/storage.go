package storage

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/janelia-flyem/go/semver"
)

// Alias is a nickname for a storage configuration, e.g., "raid6" for
// basholeveldb on RAID6 drives. It is used in the DVID TOML configuration file
// like [storage.alias]
type Alias string

// DataMap describes how data instances and types are mapped to
// available storage systems.
type DataMap map[dvid.DataSpecifier]Alias

// Backend provide data instance to store mappings gleaned from DVID configuration.
// Currently, a data instance can be mapped to one KV and one Log store.
type Backend struct {
	Metadata    Alias // The store that should be used for metadata storage.
	DefaultLog  Alias // The log that should be used by default.
	DefaultKVDB Alias // The key-value datastore that should be used by default.
	Stores      map[Alias]dvid.StoreConfig
	KVStore     DataMap
	LogStore    DataMap
	Groupcache  GroupcacheConfig
}

// StoreConfig returns a data specifier's assigned store configuration.
// The DataSpecifier can be "default" or "metadata" as well as datatype names
// and data instance specifications.
func (b Backend) StoreConfig(d dvid.DataSpecifier) (config dvid.StoreConfig, found bool) {
	if d == "default" {
		config, found = b.Stores[b.DefaultKVDB]
		return
	}
	if d == "metadata" {
		config, found = b.Stores[b.Metadata]
		return
	}
	alias, ok := b.KVStore[d]
	if !ok {
		return
	}
	config, found = b.Stores[alias]
	return
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
	GraphDB    bool
}

// Engine is a storage engine that can create a storage instance, dvid.Store, which could be
// a database directory in the case of an embedded database Engine implementation.
// Engine implementations can fulfill a variety of interfaces, checkable by runtime cast checks,
// e.g., myGetter, ok := myEngine.(OrderedKeyValueGetter)
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
// Each Engine implementation should call storage.Register() to register its availability.
type Engine interface {
	fmt.Stringer

	// GetName returns a simple driver identifier like "basholeveldb", "kvautobus" or "bigtable".
	GetName() string

	// IsDistributed returns whether the engine is a distributed DB (engine should manage request throttling)
	IsDistributed() bool

	// GetSemVer returns the semantic versioning info.
	GetSemVer() semver.Version

	// NewStore returns a new storage engine given the passed configuration.
	// Should return true for initMetadata if the store needs initialization of metadata.
	NewStore(dvid.StoreConfig) (db dvid.Store, initMetadata bool, err error)
}

// RepairableEngine is a storage engine that can be repaired.
type RepairableEngine interface {
	Engine
	Repair(path string) error
}

// TestableEngine is a storage engine that allows creation of temporary stores for testing.
type TestableEngine interface {
	Engine
	AddTestConfig(*Backend) (Alias, error)
	Delete(dvid.StoreConfig) error
}

var (
	// initialized by RegisterEngine() calls during init() within each storage engine
	availEngines map[string]Engine
)

// EnginesAvailable returns a description of the available storage engines.
func EnginesAvailable() string {
	var engines []string
	for e := range availEngines {
		engines = append(engines, e)
	}
	return strings.Join(engines, "; ")
}

// RegisterEngine registers an Engine for DVID use.
func RegisterEngine(e Engine) {
	dvid.Debugf("Engine %q registered with DVID server.\n", e)
	if availEngines == nil {
		availEngines = map[string]Engine{e.GetName(): e}
	} else {
		availEngines[e.GetName()] = e
	}
}

// GetEngine returns an Engine of the given name.
func GetEngine(name string) Engine {
	if availEngines == nil {
		return nil
	}
	e, found := availEngines[name]
	if !found {
		return nil
	}
	return e
}

// GetTestableBackend returns testable engines and a storage backend that combines all
// testable engine configurations.
func GetTestableBackend(kvMap, logMap DataMap) (map[Alias]TestableEngine, *Backend, error) {
	var found bool
	engines := make(map[Alias]TestableEngine)
	backend := new(Backend)
	for _, e := range availEngines {
		tEng, ok := e.(TestableEngine)
		if ok {
			alias, err := tEng.AddTestConfig(backend)
			if err != nil {
				dvid.Errorf("checking engine %q: %v\n", e, err)
			} else {
				engines[alias] = tEng
				found = true
			}
		}
	}
	backend.KVStore = kvMap
	backend.LogStore = logMap
	if !found {
		return nil, nil, fmt.Errorf("could not find any testable storage configuration")
	}
	return engines, backend, nil
}

// NewStore checks if a given engine is available and if so, returns
// a store opened with the configuration.
func NewStore(c dvid.StoreConfig) (db dvid.Store, created bool, err error) {
	if availEngines == nil {
		return nil, false, fmt.Errorf("no available storage engines")
	}
	e, found := availEngines[c.Engine]
	if !found {
		return nil, false, fmt.Errorf("engine %q not available", c.Engine)
	}
	return e.NewStore(c)
}

// Repair repairs a named engine's store at given path.
func Repair(name, path string) error {
	e := GetEngine(name)
	if e == nil {
		return fmt.Errorf("Could not find engine with name %q", name)
	}
	repairer, ok := e.(RepairableEngine)
	if !ok {
		return fmt.Errorf("Engine %q has no capability to be repaired.", name)
	}
	return repairer.Repair(path)
}

// SizeViewer stores are able to return the size in bytes stored for a given range of Key.
type SizeViewer interface {
	GetApproximateSizes(ranges []KeyRange) ([]uint64, error)
}

// GetDataSizes returns a list of storage sizes in bytes for each data instance in the store.
// A list of InstanceID can be optionally supplied so only those instances are queried.
// This requires some scanning of the database so could take longer than normal requests,
// particularly if a list of instances is not given.
// Note that the underlying store must support both the OrderedKeyValueGetter and SizeViewer interface,
// else this function returns nil.
func GetDataSizes(store dvid.Store, instances []dvid.InstanceID) (map[dvid.InstanceID]uint64, error) {
	db, ok := store.(OrderedKeyValueGetter)
	if !ok {
		dvid.Infof("Cannot get data sizes for store %s, which is not an OrderedKeyValueGetter store", db)
		return nil, nil
	}
	sv, ok := db.(SizeViewer)
	if !ok {
		dvid.Infof("Cannot get data sizes for store %s, which is not an SizeViewer store", db)
		return nil, nil
	}
	// Handle prespecified instance IDs.
	if len(instances) != 0 {
		return getInstanceSizes(sv, instances)
	}

	// Scan store and get all instances.
	var ids []dvid.InstanceID
	var curID dvid.InstanceID
	for {
		var done bool
		var err error
		curID, done, err = getNextInstance(db, curID)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		ids = append(ids, curID)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return getInstanceSizes(sv, ids)
}
