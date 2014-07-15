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
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

// Engine implementations can fulfill a variety of interfaces and can be checked by
// runtime cast checks, e.g., myGetter, ok := myEngine.(OrderedKeyValueGetter)
// Data types can throw a warning at init time if the backend doesn't support required
// interfaces, or they can choose to implement multiple ways of handling data.
type Engine interface {
	GetName() string
	GetConfig() dvid.Config
	Close()
}

// ---- The three tiers of key/value storage for a DVID datastore, characterized by
// ---- the maximum size of values and the bandwidth/latency, that provide a layer
// ---- of semantics on top of underlying storage engines.  These interfaces are
// ---- implemented based on build flags for whether DVID is running locally,
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
	// require much more capacity and allow higher latency than MetaData.
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
	dvid.KeyValue
}

// Requirements lists required backend interfaces for a type.
type Requirements struct {
	BulkIniter bool
	BulkWriter bool
	Batcher    bool
	GraphDB    bool
}

// ---- Storage interfaces ------

type KeyValueGetter interface {
	// Get returns a value given a key.
	Get(k dvid.Key) (v []byte, err error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(kStart, kEnd dvid.Key) (values []dvid.KeyValue, err error)

	// KeysInRange returns a range of keys spanning (kStart, kEnd).
	KeysInRange(kStart, kEnd dvid.Key) (keys []dvid.Key, err error)

	// ProcessRange sends a range of key/value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key/value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype.voxels.ProcessChunk() for an example.
	ProcessRange(kStart, kEnd dvid.Key, op *ChunkOp, f func(*Chunk)) (err error)
}

type KeyValueSetter interface {
	// Put writes a value with given key.
	Put(k dvid.Key, v []byte) error

	// Delete removes an entry given key.
	Delete(k dvid.Key) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(values []dvid.KeyValue) error
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

// KeyValueBatcher allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type KeyValueBatcher interface {
	NewBatch() Batch
}

// Batch groups operations into a transaction.
// Clear() and Close() were removed due to how other key-value stores implement batches.
// It's easier to implement cross-database handling of a simple write/delete batch
// that commits then closes rather than something that clears.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(k dvid.Key)

	// Put adds to the batch a put using the given key/value.
	Put(k dvid.Key, v []byte)

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
	// CreateGraph creates a graph using a name defined with the dvid.Key interface
	CreateGraph(graph dvid.Key) error

	// AddVertex inserts an id of a given weight into the graph
	AddVertex(graph dvid.Key, id dvid.VertexID, weight float64) error

	// AddEdge adds an edge between vertex id1 and id2 with the provided weight
	AddEdge(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error

	// SetVertexWeight modifies the weight of vertex id
	SetVertexWeight(graph dvid.Key, id dvid.VertexID, weight float64) error

	// SetEdgeWeight modifies the weight of the edge defined by id1 and id2
	SetEdgeWeight(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error

	// SetVertexProperty adds arbitrary data to a vertex using a string key
	SetVertexProperty(graph dvid.Key, id dvid.VertexID, key string, value []byte) error

	// SetEdgeProperty adds arbitrary data to an edge using a string key
	SetEdgeProperty(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID, key string, value []byte) error

	// RemoveVertex removes the vertex and its properties and edges
	RemoveVertex(graph dvid.Key, id dvid.VertexID) error

	// RemoveEdge removes the edge defined by id1 and id2 and its properties
	RemoveEdge(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID) error

	// RemoveGraph removes the entire graph including all vertices, edges, and properties
	RemoveGraph(graph dvid.Key) error

	// RemoveVertexProperty removes the property data for vertex id at the key
	RemoveVertexProperty(graph dvid.Key, id dvid.VertexID, key string) error

	// RemoveEdgeProperty removes the property data for edge at the key
	RemoveEdgeProperty(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID, key string) error
}

// GraphGetter defines operations that retrieve information from a graph
type GraphGetter interface {
	// GetVertices retrieves a list of all vertices in the graph
	GetVertices(graph dvid.Key) ([]GraphVertex, error)

	// GetEdges retrieves a list of all edges in the graph
	GetEdges(graph dvid.Key) ([]dvid.GraphEdge, error)

	// GetVertex retrieves a vertex given a vertex id
	GetVertex(graph dvid.Key, id dvid.VertexID) (dvid.GraphVertex, error)

	// GetVertex retrieves an edges between two vertex IDs
	GetEdge(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID) (dvid.GraphEdge, error)

	// GetVertexProperty retrieves a property as a byte array given a vertex id
	GetVertexProperty(graph dvid.Key, id dvid.VertexID, key string) ([]byte, error)

	// GetEdgeProperty retrieves a property as a byte array given an edge defined by id1 and id2
	GetEdgeProperty(graph dvid.Key, id1 dvid.VertexID, id2 dvid.VertexID, key string) ([]byte, error)
}

// GraphDB defines the entire interface that a graph database should support
type GraphDB interface {
	GraphSetter
	GraphGetter
}
