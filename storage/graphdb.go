package storage

import (
	"github.com/janelia-flyem/dvid/dvid"
)

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
