/*
	Package storage provides a unified interface to a number of storage engines.
	Since each storage engine has different capabilities, this package defines a
	number of interfaces in addition to the core Engine interface, which all
	storage engines should satisfy.

	Keys are specified as a combination of Context and a datatype-specific byte slice.
	The Context provides DVID-wide namespacing and as such, must use one of the
	Context implementations within the storage package.  (It is implemented as a
	Go opaque interface.)  The datatype-specific byte slice formatting is entirely
	up to the datatype designer, although use of dvid.Index is suggested.

	The storage engines should accept a nil Context, which allows direct saving of a
	raw key without use of a ConstructKey() transformation.  In general, though,
	keys passed

	Initially we are concentrating on key-value backends but expect to support
	graph and perhaps relational databases, either using specialized databases
	or software layers on top of an ordered key-value store.

	Each local key-value engine must implement the following package function:

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

// KeyValue stores a key-value pair.
type KeyValue struct {
	K []byte
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
	return bytes.Compare(kv[i].K, kv[j].K) <= 0
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

// --- The three tiers of storage might gain new interfaces when we add cluster
// --- support to DVID.

// MetaDataStorer is the interface for storing DVID datastore metadata like the
// repositories and associated DAGs.  It is characterized by the following:
// (1) not big data, (2) ideally in memory, (3) strongly consistent across all
// DVID processes, e.g., all front-end DVID apps.  Of the three tiers of storage
// (Metadata, SmallData, BigData), MetaData should have
// the smallest capacity and the lowest latency.
type MetaDataStorer interface {
	OrderedKeyValueDB
}

// SmallDataStorer is the interface for storing key-only or small key-value pairs that
// require much more aggregate capacity and allow higher latency than MetaData.  This is
// typically used for indexing where the values aren't too large.
type SmallDataStorer interface {
	OrderedKeyValueDB
}

// BigDataStorer is the interface for storing DVID key-value pairs that are relatively
// large compared to key-value pairs used in SmallData.  This interface should be used
// for blocks of voxels and large denormalized data like the multi-scale surface of a
// given label.  This store should have considerably more capacity and potentially
// higher latency than SmallData.
type BigDataStorer interface {
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
	*KeyValue
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
	Get(ctx Context, k []byte) (v []byte, err error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(ctx Context, kStart, kEnd []byte) (values []*KeyValue, err error)

	// KeysInRange returns a range of full keys spanning (kStart, kEnd).  Note that
	// the returned keys are the full keys (including context from ctx.ConstructKey()).
	// To access the original indices passed in with a non-nil Context, use
	// something like storage.DataContextIndex(key).
	KeysInRange(ctx Context, kStart, kEnd []byte) (keys [][]byte, err error)

	// ProcessRange sends a range of key-value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key-value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype.voxels.ProcessChunk() for an example.
	ProcessRange(ctx Context, kStart, kEnd []byte, op *ChunkOp, f func(*Chunk)) (err error)
}

type KeyValueSetter interface {
	// Put writes a value with given key.
	Put(ctx Context, k, v []byte) error

	// Delete removes an entry given key.
	Delete(ctx Context, k []byte) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(ctx Context, values []KeyValue) error
}

// KeyValueDB provides an interface to the simplest storage API: a key-value store.
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
	NewBatch(ctx Context) Batch
}

// Batch groups operations into a transaction.
// Clear() and Close() were removed due to how other key-value stores implement batches.
// It's easier to implement cross-database handling of a simple write/delete batch
// that commits then closes rather than something that clears.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(k []byte)

	// Put adds to the batch a put using the given key-value.
	Put(k, v []byte)

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
}
