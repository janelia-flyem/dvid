/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces in addition to the core Engine interface, which all
	storage engines should satisfy.

	Keys are specified as a combination of Context and a datatype-specific byte slice,
	typically called an "type-specific key" (TKey) in DVID docs and code.  The Context
	provides DVID-wide namespacing and as such, must use one of the Context implementations
	within the storage package.  (This is enforced by making Context a Go opaque interface.)
	The type-specific key formatting is entirely up to the datatype designer, although
	use of dvid.Index is suggested.

	Initially we are concentrating on key-value backends but expect to support
	graph and perhaps relational databases, either using specialized databases
	or software layers on top of an ordered key-value store.

	Although we assume lexicographically ordering for range queries, there is some
	variation in how variable size keys are treated.  We assume all storage engines,
	after appropriate DVID drivers, use the following definition of ordering:

		A string s precedes a string t in lexicographic order if:

		* s is a prefix of t, or
		* if c and d are respectively the first character of s and t in which s and t differ,
		  then c precedes d in character order.
		* if s and t are equivalent for all of s, but t is longer

		Note: For the characters that are alphabetical letters, the character order coincides
		with the alphabetical order. Digits precede letters, and uppercase letters precede
		lowercase ones.

		Examples:

		composer precedes computer
		house precedes household
		Household precedes house
		H2O precedes HOTEL
		mydex precedes mydexterity

		Note that the above is different than shortlex order, which would group strings
		based on length first.

	The above lexicographical ordering is used by default for levedb variants.
*/
package storage

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/janelia-flyem/go/semver"
)

// Key is the slice of bytes used to store a value in a storage engine.  It internally
// represents a number of DVID features like a data instance ID, version, and a
// type-specific key component.
type Key []byte

// KeyRange is a range of keys that is closed at the beginning and open at the end.
type KeyRange struct {
	Start   Key // Range includes this Key.
	OpenEnd Key // Range extend to but does not include this Key.
}

// TKey is the type-specific component of a key.  Each data instance will insert
// key components into a class of TKey.  Typically, the first byte is unique for
// a given class of keys within a datatype.  For a given class of type-specific keys,
// they must either be (1) identical length or (2) if varying length, not share a
// common prefix.  The second case can be trivially fulfilled by ending a
// type-specific key with a unique (not occuring within these type-specific keys)
// termination byte like 0x00.
type TKey []byte

const (
	tkeyMinByte      = 0x00
	tkeyStandardByte = 0x01
	tkeyMaxByte      = 0xFF

	TKeyMinClass = 0x00
	TKeyMaxClass = 0xFF
)

// TKeyClass partitions the TKey space into a maximum of 256 classes.
type TKeyClass byte

func NewTKey(class TKeyClass, tkey []byte) TKey {
	b := make([]byte, 2+len(tkey))
	b[0] = byte(class)
	b[1] = tkeyStandardByte
	if tkey != nil {
		copy(b[2:], tkey)
	}
	return TKey(b)
}

// MinTKey returns the lexicographically smallest TKey for this class.
func MinTKey(class TKeyClass) TKey {
	return TKey([]byte{byte(class), tkeyMinByte})
}

// MaxTKey returns the lexicographically largest TKey for this class.
func MaxTKey(class TKeyClass) TKey {
	return TKey([]byte{byte(class), tkeyMaxByte})
}

// Class returns the TKeyClass of a TKey.
func (tk TKey) Class() (TKeyClass, error) {
	if len(tk) == 0 {
		return 0, fmt.Errorf("can't get class of length 0 TKey")
	}
	return TKeyClass(tk[0]), nil
}

// ClassBytes returns the bytes for a class of TKey, suitable for decoding by
// each data instance.
func (tk TKey) ClassBytes(class TKeyClass) ([]byte, error) {
	if tk[0] != byte(class) {
		return nil, fmt.Errorf("bad type-specific key: expected class %v got %v", class, tk[0])
	}
	return tk[2:], nil
}

// KeyValue stores a full storage key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// TKeyValue stores a type-specific key-value pair.
type TKeyValue struct {
	K TKey
	V []byte
}

// Deserialize returns a type key-value pair where the value has been deserialized.
func (kv TKeyValue) Deserialize(uncompress bool) (TKeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return TKeyValue{kv.K, value}, err
}

// TKeyValues is a slice of type-specific key-value pairs that can be sorted.
type TKeyValues []TKeyValue

func (kv TKeyValues) Len() int      { return len(kv) }
func (kv TKeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv TKeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K, kv[j].K) <= 0
}

// DataStoreType describes the semantics of a particular data store.
type DataStoreType uint8

const (
	UnknownData DataStoreType = iota
	MetaData
	Mutable
	Immutable
)

// Alias is a nickname for a storage configuration, e.g., "raid6" for basholeveldb on RAID6 drives.
// It is used in the DVID TOML configuration file like [storage.alias]
type Alias string

// Backend provide data instance to store mappings gleaned from DVID configuration.
type Backend struct {
	Default    Alias // The store that should be used by default.
	Metadata   Alias // The store that should be used for metadata storage.
	Stores     map[Alias]dvid.StoreConfig
	Mapping    map[dvid.DataSpecifier]Alias
	Groupcache GroupcacheConfig
}

// StoreConfig returns a data specifier's assigned store configuration.
// The DataSpecifier can be "default" or "metadata" as well as datatype names
// and data instance specifications.
func (b Backend) StoreConfig(d dvid.DataSpecifier) (config dvid.StoreConfig, found bool) {
	if d == "default" {
		config, found = b.Stores[b.Default]
		return
	}
	if d == "metadata" {
		config, found = b.Stores[b.Metadata]
		return
	}
	alias, ok := b.Mapping[d]
	if !ok {
		return
	}
	config, found = b.Stores[alias]
	return
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

// TestableEngine is a storage engine that allows creation and deletion of
// some data using a name.
type TestableEngine interface {
	Engine
	GetTestConfig() (*Backend, error)
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

// GetTestableEngine returns a Testable engine, i.e. has ability to create and delete database.
func GetTestableEngine() TestableEngine {
	for _, e := range availEngines {
		testableEng, ok := e.(TestableEngine)
		if ok {
			return testableEng
		}
	}
	return nil
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

// Op enumerates the types of single key-value operations that can be performed for storage engines.
type Op uint8

const (
	GetOp Op = iota
	PutOp
	DeleteOp
	CommitOp
)

// ChunkOp is a type-specific operation with an optional WaitGroup to
// sync mapping before reduce.
type ChunkOp struct {
	Op interface{}
	Wg *sync.WaitGroup
}

// Chunk is the unit passed down channels to chunk handlers.  Chunks can be passed
// from lower-level database access functions to type-specific chunk processing.
type Chunk struct {
	*ChunkOp
	*TKeyValue
}

// ChunkFunc is a function that accepts a Chunk.
type ChunkFunc func(*Chunk) error

// PatchFunc is a function that accepts a value and patches that value in place
type PatchFunc func([]byte) error

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
	GraphDB    bool
}

// KeyChan is a channel of full (not type-specific) keys.
type KeyChan chan Key

// ---- Storage interfaces ------

// Accessor provides a variety of convenience functions for getting
// different types of stores.  For each accessor function, a nil
// store means it is not available.
type Accessor interface {
	GetKeyValueDB() (KeyValueDB, error)
	GetOrderedKeyValueDB() (OrderedKeyValueDB, error)
	GetKeyValueBatcher() (KeyValueBatcher, error)
	GetGraphDB() (GraphDB, error)
}

// KeyValueIngestable implementations allow ingestion of data without necessarily allowing
// immediate reads of the ingested data.
type KeyValueIngestable interface {
	// KeyValueIngest accepts mutations without any guarantee that the ingested key value
	// will be immediately readable.  This allows immutable stores to accept data that will
	// be later processed into its final readable, immutable form.
	KeyValueIngest(Context, TKey, []byte) error
}

type KeyValueGetter interface {
	// Get returns a value given a key.
	Get(ctx Context, k TKey) ([]byte, error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(ctx Context, kStart, kEnd TKey) ([]*TKeyValue, error)

	// KeysInRange returns a range of type-specific key components spanning (kStart, kEnd).
	KeysInRange(ctx Context, kStart, kEnd TKey) ([]TKey, error)

	// SendKeysInRange sends a range of keys down a key channel.
	SendKeysInRange(ctx Context, kStart, kEnd TKey, ch KeyChan) error

	// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key-value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype/imageblk.ProcessChunk() for an example.  If the ChunkFunc returns
	// an error, it is expected that the ProcessRange should immediately terminate and
	// propagate the error.
	ProcessRange(ctx Context, kStart, kEnd TKey, op *ChunkOp, f ChunkFunc) error

	// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
	// retrieval like DVID-to-DVID communication and should not be used by data type
	// implementations if possible because each version's key-value pairs are sent
	// without filtering by the current version and its ancestor graph.  A nil is sent
	// down the channel when the range is complete.  The query can be cancelled by sending
	// a value down the cancel channel.
	RawRangeQuery(kStart, kEnd Key, keysOnly bool, out chan *KeyValue, cancel <-chan struct{}) error
}

type KeyValueSetter interface {
	// Put writes a value with given key in a possibly versioned context.
	Put(Context, TKey, []byte) error

	// Delete deletes a key-value pair so that subsequent Get on the key returns nil.
	// For versioned data in mutable stores, Delete() will create a tombstone for the version
	// unlike RawDelete or DeleteAll.
	Delete(Context, TKey) error

	// RawPut is a low-level function that puts a key-value pair using full keys.
	// This can be used in conjunction with RawRangeQuery.  It does not automatically
	// delete any associated tombstone, unlike the Delete() function, so tombstone
	// deletion must be handled via RawDelete().
	RawPut(Key, []byte) error

	// RawDelete is a low-level function.  It deletes a key-value pair using full keys
	// without any context.  This can be used in conjunction with RawRangeQuery.
	RawDelete(Key) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(Context, []TKeyValue) error

	// DeleteRange removes all key-value pairs with keys in the given range.
	// If versioned data in mutable stores, this will create tombstones in the version
	// unlike RawDelete or DeleteAll.
	DeleteRange(ctx Context, kStart, kEnd TKey) error

	// DeleteAll removes all key-value pairs for the context.  If allVersions is true,
	// then all versions of the data instance are deleted.
	DeleteAll(ctx Context, allVersions bool) error
}

// KeyValueDB provides an interface to the simplest storage API: a key-value store.
type KeyValueDB interface {
	dvid.Store
	KeyValueGetter
	KeyValueSetter
}

// OrderedKeyValueDB addes range queries and range puts to a base KeyValueDB.
type OrderedKeyValueDB interface {
	dvid.Store
	OrderedKeyValueGetter
	OrderedKeyValueSetter
}

// KeyValueBatcher allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type KeyValueBatcher interface {
	NewBatch(ctx Context) Batch
}

// KeyValueRequester allows operations to be queued so that
// they can be handled as a batch job.  (See RequestBuffer for
// more information.)
type KeyValueRequester interface {
	NewBuffer(ctx Context) RequestBuffer
}

// PatchDB implements inteface for patchign a DB value atomically
type PatchDB interface {
	// Patch patches the value at the given key with function f
	// The patching function should work on uninitialized data.
	Patch(Context, TKey, PatchFunc) error
}

// RequestBufferSubset implements a subset of the ordered key/value interface.
// It declares interface common to both ordered key value and RequestBuffer
type BufferableOps interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(Context, []TKeyValue) error

	// DeleteRange removes all key-value pairs with keys in the given range.
	// If versioned data in mutable stores, this will create tombstones in the version
	// unlike RawDelete or DeleteAll.
	DeleteRange(ctx Context, kStart, kEnd TKey) error

	// ProcessRange will process all gets when flush is called
	ProcessRange(ctx Context, kStart, kEnd TKey, op *ChunkOp, f ChunkFunc) error
}

// RequestBuffer allows one to queue up several requests and then process
// them all as a batch operation (if the driver supports batch
// operations).  There is no guarantee on the order or atomicity
// of these operations.
type RequestBuffer interface {
	BufferableOps

	// ProcessList will process all gets when flush is called
	ProcessList(ctx Context, tkeys []TKey, op *ChunkOp, f ChunkFunc) error

	// PutCallback writes a value with given key in a possibly versioned context and signals the callback
	PutCallback(Context, TKey, []byte, chan error) error

	// Flush processes all the queued jobs
	Flush() error
}

// Batch groups operations into a transaction.
// Clear() and Close() were removed due to how other key-value stores implement batches.
// It's easier to implement cross-database handling of a simple write/delete batch
// that commits then closes rather than something that clears.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(TKey)

	// Put adds to the batch a put using the given key-value.
	Put(k TKey, v []byte)

	// Commits a batch of operations and closes the write batch.
	Commit() error
}

// GraphSetter defines operations that modify a graph
type GraphSetter interface {
	// CreateGraph creates a graph with the given context.
	CreateGraph(ctx Context) error

	// AddVertex inserts an id of a given weight into the graph
	AddVertex(ctx Context, id dvid.VertexID, weight float64) error

	// AddEdge adds an edge between vertex id1 and id2 with the provided weight
	AddEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error

	// SetVertexWeight modifies the weight of vertex id
	SetVertexWeight(ctx Context, id dvid.VertexID, weight float64) error

	// SetEdgeWeight modifies the weight of the edge defined by id1 and id2
	SetEdgeWeight(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error

	// SetVertexProperty adds arbitrary data to a vertex using a string key
	SetVertexProperty(ctx Context, id dvid.VertexID, key string, value []byte) error
	// SetEdgeProperty adds arbitrary data to an edge using a string key
	SetEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string, value []byte) error
	// RemoveVertex removes the vertex and its properties and edges
	RemoveVertex(ctx Context, id dvid.VertexID) error
	// RemoveEdge removes the edge defined by id1 and id2 and its properties
	RemoveEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID) error
	// RemoveGraph removes the entire graph including all vertices, edges, and properties
	RemoveGraph(ctx Context) error
	// RemoveVertexProperty removes the property data for vertex id at the key
	RemoveVertexProperty(ctx Context, id dvid.VertexID, key string) error
	// RemoveEdgeProperty removes the property data for edge at the key
	RemoveEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string) error
}

// GraphGetter defines operations that retrieve information from a graph
type GraphGetter interface {
	// GetVertices retrieves a list of all vertices in the graph
	GetVertices(ctx Context) ([]dvid.GraphVertex, error)
	// GetEdges retrieves a list of all edges in the graph
	GetEdges(ctx Context) ([]dvid.GraphEdge, error)
	// GetVertex retrieves a vertex given a vertex id
	GetVertex(ctx Context, id dvid.VertexID) (dvid.GraphVertex, error)
	// GetVertex retrieves an edges between two vertex IDs
	GetEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID) (dvid.GraphEdge, error)
	// GetVertexProperty retrieves a property as a byte array given a vertex id
	GetVertexProperty(ctx Context, id dvid.VertexID, key string) ([]byte, error)
	// GetEdgeProperty retrieves a property as a byte array given an edge defined by id1 and id2
	GetEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string) ([]byte, error)
}

// GraphDB defines the entire interface that a graph database should support
type GraphDB interface {
	GraphSetter
	GraphGetter
	Close()
}

// SizeViewer stores are able to return the size in bytes stored for a given range of Key.
type SizeViewer interface {
	GetApproximateSizes(ranges []KeyRange) ([]uint64, error)
}

// GetDataSizes returns a list of storage sizes in bytes for each data instance in the store.
// A list of InstanceID can be optionally supplied so only those instances are queried.
// This requires some scanning of the database so could take longer than normal requests,
// particularly if a list of instances is not given.
// Note that the underlying store must support both the OrderedKeyValueGetter and SizeViewer interface.
func GetDataSizes(store dvid.Store, instances []dvid.InstanceID) (map[dvid.InstanceID]uint64, error) {
	db, ok := store.(OrderedKeyValueGetter)
	if !ok {
		return nil, fmt.Errorf("cannot get data sizes for store %s, which is not an OrderedKeyValueGetter store", db)
	}
	sv, ok := db.(SizeViewer)
	if !ok {
		return nil, fmt.Errorf("cannot get data sizes for store %s, which is not a SizeViewer store", db)
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

func getNextInstance(db OrderedKeyValueGetter, curID dvid.InstanceID) (nextID dvid.InstanceID, finished bool, err error) {
	begKey := constructDataKey(curID+1, 0, 0, minTKey)
	endKey := constructDataKey(dvid.MaxInstanceID, dvid.MaxVersionID, dvid.MaxClientID, maxTKey)

	ch := make(chan *KeyValue)
	cancel := make(chan struct{})

	// Process each key received by range query.
	ctx := DataContext{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			kv := <-ch
			if kv == nil || kv.K == nil {
				finished = true
				return
			}
			nextID, err = ctx.InstanceFromKey(kv.K)
			if err != nil {
				cancel <- struct{}{}
				return
			}
			if nextID != curID {
				cancel <- struct{}{}
				return
			}
		}
	}()

	keysOnly := true
	if queryErr := db.RawRangeQuery(begKey, endKey, keysOnly, ch, cancel); queryErr != nil {
		err = queryErr
	}
	wg.Wait()
	return
}

func getInstanceSizes(sv SizeViewer, instances []dvid.InstanceID) (map[dvid.InstanceID]uint64, error) {
	ranges := make([]KeyRange, len(instances))
	for i, curID := range instances {
		beg := constructDataKey(curID, 0, 0, minTKey)
		end := constructDataKey(curID+1, 0, 0, minTKey)
		ranges[i] = KeyRange{Start: beg, OpenEnd: end}
	}
	s, err := sv.GetApproximateSizes(ranges)
	if err != nil {
		return nil, err
	}
	if len(s) != len(instances) {
		return nil, fmt.Errorf("only got back %d instance sizes, not the requested %d instances", len(s), len(instances))
	}
	sizes := make(map[dvid.InstanceID]uint64, len(instances))
	for i, curID := range instances {
		sizes[curID] = s[i]
	}
	return sizes, nil
}
