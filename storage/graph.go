package storage

type ElementProperties map[string]struct{} // ?! -- maybe allow simple properties to be stored

type VertexID uint64

// smaller ID should be first
type VertexPairID struct {
	Vertex1 VertexID
	Vertex2 VertexID
}

// some of the fields might just be embedded in the name
// only allow one edge between node pairs
// only support unidirectional edges directly (direction must be a property)

type GraphElement struct {
	Properties ElementProperties
	Weight     float64
}

type GraphVertex struct {
	*GraphElement
	Id       VertexID
	Vertices []VertexID
}

type GraphEdge struct {
	*GraphElement
	Vertexpair VertexPairID
}
