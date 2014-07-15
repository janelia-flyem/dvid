/*
   This file defines fundamental graph structures that should
   be brokered by the graph interface.
*/

package dvid

// ElementProperties is a set of strings corresponding to properties
// stored at an graph edge or vertex (it might make sense to store some
// properties with the vertex or edge)
type ElementProperties map[string]struct{}

// VertexID is a 64 bit label ID for vertices in the graph
type VertexID uint64

// smaller ID should be first
type VertexPairID struct {
	Vertex1 VertexID
	Vertex2 VertexID
}

// GraphElement defines fundametal data common to both vertices and edges
type GraphElement struct {
	Properties ElementProperties
	Weight     float64
}

// GraphVertex defines a vertex in a graph; a vertex should have a unique id
type GraphVertex struct {
	*GraphElement
	Id       VertexID
	Vertices []VertexID
}

// GraphEdge defines an edge in a graph; if a directed edge is desired it
// must be specified as an edge property
type GraphEdge struct {
	*GraphElement
	Vertexpair VertexPairID
}
