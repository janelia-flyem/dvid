package storage

type ElementProperties map[string]struct{} // ?! -- maybe allow simple properties to be stored

type VertexID uint64

// smaller ID should be first
type VertexPairID struct {
	vertex1 VertexID
	vertex2 VertexID
}

// some of the fields might just be embedded in the name
// only allow one edge between node pairs
// only support unidirectional edges directly (direction must be a property)

type GraphElement struct {
	properties ElementProperties
	weight     float64
}

type GraphVertex struct {
	*GraphElement
	id       VertexID
	vertices []VertexID
}

type GraphEdge struct {
	*GraphElement
	vertexpair VertexPairID
}
