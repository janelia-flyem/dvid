/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces in addition to the core Engine interface, which all
	storage engines should satisfy.

	Initially we are concentrating on key-value backends but expect to support
	graph and perhaps relational databases.

	Each local storage engine must implement the following package function:

	func NewKeyValueStore(path string, create bool, options *Options) (Engine, error)

	If DVID is compiled without gcloud or clustered build flags, a local storage engine
	is selected through build tags, e.g., "hyperleveldb", "basholeveldb", or "bolt".
*/
package storage

import (
	"bytes"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

// Key provides an abstraction for everything a versioned storage engine needs to know
// about how to construct key bytes and interpret the returned, versioned key/value pairs.
// The exact Key implementations are typically at the datastore or datatype level where
// each data instance can determine the best way to structure keys, depending on maximum
// versions per unique index, access speed requirements, etc.  Keeping the Key implementations
// at the datastore level also allows consistent partioning of possibly different versions
// of the key space.  To ensure consistency in how the key space is handled, best practice
// is to define an opaque Key type at the datastore level and force use of its interface
// in setting and retrieving key components.  (See datastore.Key)
type Key interface {
	// Bytes returns the key representation as a slice of bytes
	Bytes() []byte

	// BytesToKey returns a Key given its representation as a slice of bytes
	BytesToKey([]byte) (Key, error)

	String() string

	// Versioned is true if multiple versions of a key/value could be available.
	Versioned() bool

	// Returns lower bound key for versions of given byte slice key representation.
	MinVersionKey([]byte) (Key, error)

	// Returns upper bound key for versions of given byte slice key representation.
	MaxVersionKey([]byte) (Key, error)

	// VersionedKeyValue returns the key/value pair corresponding to this key's version
	// given a list of key/value pairs across many versions.
	VersionedKeyValue([]KeyValue) (KeyValue, error)
}

// KeyValue stores a key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// Deserialize returns a key-value pair where the value has been deserialized.
func (kv KeyValue) Deserialize(uncompress bool) (KeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return KeyValue{kv.K, value}, err
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K.Bytes(), kv[j].K.Bytes()) <= 0
}

// ---- The three tiers of storage for a DVID datastore, characterized by the
// ---- maximum size of values and the bandwidth/latency.  These interfaces are
// ---- initialized based on build flags for whether DVID is running locally,
// ---- in a cluster, or in a cloud with services.

var (
	// Metadata is the interface for storing DVID datastore metadata like the
	// repositories and associated DAGs.  It is characterized by the following:
	// (1) not big data, (2) ideally in memory, (3) strongly consistent across all
	// DVID processes, e.g., all front-end DVID apps.  Of the three tiers of storage
	// (MetadataStore, SmallFastStore, BigDataStore), Metadata should have
	// the smallest capacity and the lowest latency.
	MetaData metaData

	// SmallData is the interface for storing key-only or small key/value pairs that
	// require much more capacity and potentially higher latency than Metadata.
	SmallData smallData

	// BigData is the interface for storing DVID key/value pairs that are relatively
	// large compared to key/value pairs used in SmallData.  This interface should be used
	// for blocks of voxels and large denormalized data like the multi-scale surface of a
	// given label.  This store should have considerably more capacity and potentially
	// higher latency than SmallData.
	BigData bigData
)

type metaData interface {
	KeyValueDB
}

type smallData interface {
	OrderedKeyValueDB
}

type bigData interface {
	OrderedKeyValueDB
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
	KeyValue
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
	GraphDB    bool
}

// Engine implementations can fulfill a variety of interfaces and can be checked by
// runtime cast checks, e.g., myGetter, ok := myEngine.(OrderedKeyValueGetter)
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type Engine interface {
	GetName() string
	GetConfig() dvid.Config
	Close()
}

// A DataAncestor encapsulates what the storage layer needs to know about the version DAG
// to support versioning for a particular data instance.  In particular, it lets the
// storage layer compute the keys (across all ancestors) needed to fulfill a Get request.
type DataAncestor struct {
	uuid  dvid.UUID
	bloom interface{} // TODO -- add bitly's dabloom
}

// A Context encapsulates information needed to store and retrieve data from
// the storage engines.  One of the storage interfaces below can be obtained
// by doing a type assertion on a Context.  For example:
//
//     kvGetter := Context.(KeyValueGetter)
//
// Contexts are obtained via storage backend-specific NewStorageContext()
// function calls.  These calls can be found in each storage engine implementation
// that is only compiled if the appropriate build tags are used.
type Context interface {
	// VersionedKey returns the first key in the ancestor path that holds a value.
	// This allows efficient deltas by only storing changed key/values.
	VersionedKey(Key) ([]byte, error)

	// Depth returns the one more than number of ancestors of the current version,
	// where the first ancestor is the current version.
	Depth() int

	// Ancestor returns an ancestor of the current version where depth 0
	// is the current version, depth 1 is its parent, and so forth.
	Ancestor(depth int) *DataAncestor
}

// ---- Storage interfaces ------

type KeyValueGetter interface {
	// Get returns a value given a key.
	Get(k Key) (v []byte, err error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(kStart, kEnd Key) (values []*KeyValue, err error)

	// KeysInRange returns a range of keys spanning (kStart, kEnd).
	KeysInRange(kStart, kEnd Key) (keys []Key, err error)

	// ProcessRange sends a range of key/value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key/value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype.voxels.ProcessChunk() for an example.
	ProcessRange(kStart, kEnd Key, op *ChunkOp, f func(*Chunk)) (err error)
}

type KeyValueSetter interface {
	// Put writes a value with given key.
	Put(k Key, v []byte) error

	// Delete removes an entry given key.
	Delete(k Key) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(values []KeyValue) error
}

// KeyValueDB provides an interface to the simplest storage API: a key/value store.
type KeyValueDB interface {
	KeyValueGetter
	KeyValueSetter
}

// OrderedKeyValueDB addes range queries and range puts to a base KeyValueDB.
type OrderedKeyValueDB interface {
	OrderedKeyValueGetter
	OrderedKeyValueSetter
}

// Batchers allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type Batcher interface {
	NewBatch() Batch
}

// Batch groups operations into a transaction.
// Clear() and Close() were removed due to how other key-value stores implement batches.
// It's easier to implement cross-database handling of a simple write/delete batch
// that commits then closes rather than something that clears.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(k Key)

	// Put adds to the batch a put using the given key/value.
	Put(k Key, v []byte)

	// Commits a batch of operations and closes the write batch.
	Commit() error
}

// BulkIniters can employ even more aggressive optimization in loading large
// data since they can assume an uninitialized blank database.
type BulkIniter interface {
}

// BulkWriter employ some sort of optimization to efficiently write large
// amount of data.  For some key-value databases, this requires keys to
// be presorted.
type BulkWriter interface {
}

// GraphSetter defines operations that modify a graph
type GraphSetter interface {
	// CreateGraph creates a graph using a name defined with the Key interface
	CreateGraph(graph Key) error

	// AddVertex inserts an id of a given weight into the graph
	AddVertex(graph Key, id VertexID, weight float64) error

	// AddEdge adds an edge between vertex id1 and id2 with the provided weight
	AddEdge(graph Key, id1 VertexID, id2 VertexID, weight float64) error

	// SetVertexWeight modifies the weight of vertex id
	SetVertexWeight(graph Key, id VertexID, weight float64) error

	// SetEdgeWeight modifies the weight of the edge defined by id1 and id2
	SetEdgeWeight(graph Key, id1 VertexID, id2 VertexID, weight float64) error

	// SetVertexProperty adds arbitrary data to a vertex using a string key
	SetVertexProperty(graph Key, id VertexID, key string, value []byte) error

	// SetEdgeProperty adds arbitrary data to an edge using a string key
	SetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string, value []byte) error

	// RemoveVertex removes the vertex and its properties and edges
	RemoveVertex(graph Key, id VertexID) error

	// RemoveEdge removes the edge defined by id1 and id2 and its properties
	RemoveEdge(graph Key, id1 VertexID, id2 VertexID) error

	// RemoveGraph removes the entire graph including all vertices, edges, and properties
	RemoveGraph(graph Key) error

	// RemoveVertexProperty removes the property data for vertex id at the key
	RemoveVertexProperty(graph Key, id VertexID, key string) error

	// RemoveEdgeProperty removes the property data for edge at the key
	RemoveEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) error
}

// GraphGetter defines operations that retrieve information from a graph
type GraphGetter interface {
	// GetVertices retrieves a list of all vertices in the graph
	GetVertices(graph Key) ([]GraphVertex, error)

	// GetEdges retrieves a list of all edges in the graph
	GetEdges(graph Key) ([]GraphEdge, error)

	// GetVertex retrieves a vertex given a vertex id
	GetVertex(graph Key, id VertexID) (GraphVertex, error)

	// GetVertex retrieves an edges between two vertex IDs
	GetEdge(graph Key, id1 VertexID, id2 VertexID) (GraphEdge, error)

	// GetVertexProperty retrieves a property as a byte array given a vertex id
	GetVertexProperty(graph Key, id VertexID, key string) ([]byte, error)

	// GetEdgeProperty retrieves a property as a byte array given an edge defined by id1 and id2
	GetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) ([]byte, error)
}

// GraphDB defines the entire interface that a graph database should support
type GraphDB interface {
	GraphSetter
	GraphGetter
}

func Shutdown() {
	ShutdownFUSE()
}
