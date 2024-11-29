package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/blang/semver"

	"github.com/janelia-flyem/dvid/dvid"
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
	KVAssign    DataMap
	LogAssign   DataMap
	Groupcache  GroupcacheConfig
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
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

	// GetName returns a simple driver identifier like "badger", "kvautobus" or "bigtable".
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

// GetTestableBackend returns a testable engine and backend.
func GetTestableBackend(kvMap, logMap DataMap) (map[Alias]TestableEngine, *Backend, error) {
	// any engine used for testing should be added below.
	kvTestPreferences := []string{"badger", "basholeveldb", "filelog", "filestore"}
	var found bool
	engines := make(map[Alias]TestableEngine)
	backend := new(Backend)
	for _, name := range kvTestPreferences {
		e, ok := availEngines[name]
		if !ok {
			continue
		}
		tEng, ok := e.(TestableEngine)
		if ok {
			alias, err := tEng.AddTestConfig(backend)
			if err != nil {
				dvid.Errorf("checking engine %q: %v\n", e, err)
			} else {
				dvid.Infof("Found testable engine %q with alias %q\n", e, alias)
				engines[alias] = tEng
				found = true
			}
		}
	}
	backend.KVAssign = kvMap
	backend.LogAssign = logMap
	dvid.Infof("For new testable backend (%v) assigned KV: %v, Log: %v\n", backend, kvMap, logMap)
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
		return fmt.Errorf("could not find engine with name %q", name)
	}
	repairer, ok := e.(RepairableEngine)
	if !ok {
		return fmt.Errorf("engine %q has no capability to be repaired", name)
	}
	return repairer.Repair(path)
}

// VersionHistogram is a map of # versions to # keys that have that many versions.
type VersionHistogram map[int]int

func (vh VersionHistogram) Clone() VersionHistogram {
	clone := make(VersionHistogram, len(vh))
	for k, v := range vh {
		clone[k] = v
	}
	return clone
}

// KeyUsage is a map of TKeyClass to VersionHistogram.
type KeyUsage map[uint8]VersionHistogram

func (ku KeyUsage) Clone() KeyUsage {
	clone := make(KeyUsage, len(ku))
	for k, v := range ku {
		clone[k] = v.Clone()
	}
	return clone
}

// Add adds a key with the # versions and tombstones.
func (ku KeyUsage) Add(class uint8, versions int, tombstones int) {
	vh, found := ku[class]
	if !found {
		vh = make(VersionHistogram)
		ku[class] = vh
	}
	vh[versions]++
	vh[0] += tombstones
}

// KeyUsageViewer stores can return how many keys are stored and a histogram of the
// number of versions per key for each data instance given by the key ranges.
type KeyUsageViewer interface {
	GetKeyUsage(ranges []KeyRange) (histPerInstance []KeyUsage, err error)
}

// GetStoreKeyUsage returns a histogram map[# versions][# keys] for each
// data instance in the store.
func GetStoreKeyUsage(store dvid.Store) (map[dvid.InstanceID]KeyUsage, error) {
	db, ok := store.(OrderedKeyValueGetter)
	if !ok {
		dvid.Infof("Cannot get key usage for store %s, which is not an OrderedKeyValueGetter store\n", db)
		return nil, nil
	}
	viewer, ok := store.(KeyUsageViewer)
	if !ok {
		dvid.Infof("Cannot get key usage for store %s, which is not a KeyUsageViewer store\n", db)
		return nil, nil
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

	return getKeyUsage(viewer, ids)
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
		dvid.Infof("Cannot get data sizes for store %s, which is not an OrderedKeyValueGetter store\n", db)
		return nil, nil
	}
	sv, ok := db.(SizeViewer)
	if !ok {
		dvid.Infof("Cannot get data sizes for store %s, which is not an SizeViewer store\n", db)
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

// BlockOp is a type-specific operation with an optional WaitGroup to
// sync mapping before reduce.
type BlockOp struct {
	Op interface{}
	Wg *sync.WaitGroup
}

// Block is the unit passed down channels to chunk handlers.  Chunks can be passed
// from lower-level database access functions to type-specific chunk processing.
type Block struct {
	*BlockOp
	Coord dvid.ChunkPoint3d
	Value []byte
}

// BlockFunc is a function that accepts a Block.
type BlockFunc func(*Block) error

// GridProps describes the properties of a GridStore.
// This matches neuroglancer precomputed volume specs.
type GridProps struct {
	VolumeSize dvid.Point3d
	ChunkSize  dvid.Point3d
	Encoding   string     // "raw", "jpeg", or "compressed_segmentation"
	Resolution [3]float64 // resolution in nm for a voxel along dimensions
}

// GridStoreGetter describes nD block getter functions
type GridStoreGetter interface {
	GridProperties(scaleLevel int) (GridProps, error)
	GridGet(scaleLevel int, blockCoord dvid.ChunkPoint3d) ([]byte, error)
	GridGetVolume(scaleLevel int, minBlock, maxBlock dvid.ChunkPoint3d, ordered bool, f BlockFunc) error
}
