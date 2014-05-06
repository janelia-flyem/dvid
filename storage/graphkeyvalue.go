// +build graphkeyvalue

/*
   Package interacts with a key-value datastore to satisfy the GraphDB interface.
   For now, this is the only functionality that implements the GraphDB interface.  In the future,
   we might develop plugins for graph databases allowing deployers to choose.  This module
   is implemented as a separate engine but really just reuses the chosen key value engine.
   This distinction might be relevant in the future if multiple engines are supported.

   Most actions that involve multiple writes are done as a batched transaction to maintain
   atomicity.  However, transactions should probably be supported so that read-write actions
   are performed as one atomic step.
*/

package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/janelia-flyem/dvid/dvid"
)

// graphType defines the type of graph key inserted in a key/value datastore
type graphType byte

const (
	keyVertex graphType = iota
	keyEdge
	keyVertexProperty
	keyEdgeProperty
	keyMax
)

const vertexIDSize = 8

// graphKey implements the Key interface taking the DVID key as the base and
// encoding specific information relating to the graph
type graphKey struct {
	keytype  graphType
	basekey  []byte
	vertex1  VertexID
	vertex2  VertexID
	property string
}

func (gk *graphKey) KeyType() KeyType {
	return KeyData
}

func (gk *graphKey) BytesToKey(b []byte) (Key, error) {
	// key should be at least the size of the base key plus the graph type
	// and size of the vertex id
	prekeySize := len(gk.basekey)

	if len(b) < (prekeySize + 9) {
		return nil, fmt.Errorf("Malformed graphKey bytes (too few): %x", b)
	}

	// parse out key
	basekey := b[0:prekeySize]
	start := prekeySize
	keyType := graphType(b[start])
	start += 1
	vertex1 := VertexID(binary.BigEndian.Uint64(b[start:]))
	start += vertexIDSize
	vertex2 := VertexID(0)
	if keyType == keyEdge || keyType == keyEdgeProperty {
		vertex2 = VertexID(binary.BigEndian.Uint64(b[start:]))
		start += vertexIDSize
	}
	property := ""
	if keyType == keyVertexProperty || keyType == keyEdgeProperty {
		property = string(b[start:])
	}

	return &graphKey{keyType, basekey, vertex1, vertex2, property}, nil
}

// Bytes writes byte representation of a key that encodes vertex, edge, and property information
func (gk *graphKey) Bytes() []byte {
	b := gk.basekey
	// keytype determines how to encode the data
	b = append(b, byte(gk.keytype))

	vertex1 := gk.vertex1
	vertex2 := gk.vertex2
	// always ensure that the smaller vertex goes first for an edge
	if gk.keytype == keyEdge || gk.keytype == keyEdgeProperty {
		if gk.vertex1 > gk.vertex2 {
			vertex2 = gk.vertex1
			vertex1 = gk.vertex2
		}
	}

	buf := make([]byte, vertexIDSize, vertexIDSize)
	binary.BigEndian.PutUint64(buf, uint64(vertex1))
	b = append(b, buf...)

	if gk.keytype == keyEdge || gk.keytype == keyEdgeProperty {
		buf = make([]byte, vertexIDSize, vertexIDSize)
		binary.BigEndian.PutUint64(buf, uint64(vertex2))
		b = append(b, buf...)
	}
	if gk.property != "" {
		b = append(b, []byte(gk.property)...)
	}
	return b
}

// BytestoString returns the string of the byte representation of the key
func (gk *graphKey) BytesString() string {
	return string(gk.Bytes())
}

// String returns a formatted string of the bytes
func (gk *graphKey) String() string {
	return fmt.Sprintf("%x", gk.Bytes())
}

// GraphKeyValueDB defines a type that embeds a KeyValueDB using that engine to
// store all graph objects
type GraphKeyValueDB struct {
	OrderedKeyValueDB
	dbbatch Batcher
}

// NewGraphStore returns a graph backend that uses the provided keyvalue datastore (the keyvalue
// parameter will likely be unnecessary for future graph implementations)
func NewGraphStore(path string, create bool, config dvid.Config, kvdb OrderedKeyValueDB) (Engine, error) {
	dbbatch, ok := kvdb.(Batcher)
	var err error
	if !ok {
		err = fmt.Errorf("DVID key-value needed by graph does not support batch write")
	}
	graphdb := &GraphKeyValueDB{kvdb, dbbatch}
	return graphdb, err
}

// GetName returns the name of theengien
func (db *GraphKeyValueDB) GetName() string {
	return "graph db using key value datastore"
}

// GetConfig returns the configuration for the graph (no configuration currently supported)
func (db *GraphKeyValueDB) GetConfig() dvid.Config {
	return dvid.Config{}
}

// Close does nothing, explicitly close the key value DB instead
func (db *GraphKeyValueDB) Close() {
}

// -- Add Serialization Capabilities for Vertex and Edge

// SerializableVertex defines a vertex that is easily serializable
type SerializableVertex struct {
	Properties ElementProperties
	Weight     float64
	Id         VertexID
	Vertices   []VertexID
}

// SerializableEdge defines an edge that is easily serializable
type SerializableEdge struct {
	Properties ElementProperties
	Weight     float64
	Vertex1    VertexID
	Vertex2    VertexID
}

// serializeVertex serializes a GraphVertex (compression turned off for now)
func (db *GraphKeyValueDB) serializeVertex(vert GraphVertex) []byte {
	vertexser := SerializableVertex{vert.Properties, vert.Weight, vert.Id, vert.Vertices}
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	data, _ := dvid.Serialize(vertexser, compression, dvid.NoChecksum)
	return data
}

// serializeEdge serializes a GraphEdge (compression turned off for now)
func (db *GraphKeyValueDB) serializeEdge(edge GraphEdge) []byte {
	edgeser := SerializableEdge{edge.Properties, edge.Weight, edge.Vertexpair.Vertex1, edge.Vertexpair.Vertex2}
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	data, _ := dvid.Serialize(edgeser, compression, dvid.NoChecksum)
	return data
}

// deserializeVertex deserializes a GraphVertex (compression turned off for now)
func (db *GraphKeyValueDB) deserializeVertex(vertexdata []byte) (GraphVertex, error) {
	vertexser := new(SerializableVertex)
	var vertex GraphVertex
	err := dvid.Deserialize(vertexdata, vertexser)
	if err != nil {
		return vertex, err
	}
	vertex = GraphVertex{&GraphElement{vertexser.Properties, vertexser.Weight}, vertexser.Id, vertexser.Vertices}
	return vertex, nil
}

// deserializeEdge deserializes a GraphEdge (compression turned off for now)
func (db *GraphKeyValueDB) deserializeEdge(edgedata []byte) (GraphEdge, error) {
	edgeser := new(SerializableEdge)
	err := dvid.Deserialize(edgedata, edgeser)
	var edge GraphEdge
	if err != nil {
		return edge, err
	}
	edge = GraphEdge{&GraphElement{edgeser.Properties, edgeser.Weight}, VertexPairID{edgeser.Vertex1, edgeser.Vertex2}}
	return edge, nil
}

// CreateGraph does nothing as the graph keyspace uniquely defines a graph when
// using a single key/value datastore
func (db *GraphKeyValueDB) CreateGraph(graph Key) error {
	return nil
}

// AddVertex requires one key/value put
func (db *GraphKeyValueDB) AddVertex(graph Key, id VertexID, weight float64) error {
	properties := make(ElementProperties)
	var vertices []VertexID
	vertex := GraphVertex{&GraphElement{properties, weight}, id, vertices}
	data := db.serializeVertex(vertex)
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	err := db.Put(vertexKey, data)
	return err
}

// AddEdge reads both vertices, modifies the vertex edge lists, and then creates an edge
// (2 read ops, 3 write ops)
func (db *GraphKeyValueDB) AddEdge(graph Key, id1 VertexID, id2 VertexID, weight float64) error {
	// find vertex data
	vertex1, err := db.GetVertex(graph, id1)
	if err != nil {
		return err
	}
	vertex2, err := db.GetVertex(graph, id2)
	if err != nil {
		return err
	}

	vertexKey1 := &graphKey{keyVertex, graph.Bytes(), id1, 0, ""}
	vertexKey2 := &graphKey{keyVertex, graph.Bytes(), id2, 0, ""}

	// make sure all writing is done in a batch to maintain atomicity
	batcher := db.dbbatch.NewBatch()

	found := false
	for _, vertex := range vertex1.Vertices {
		if vertex == id2 {
			found = true
			break
		}
	}
	if !found {
		vertex1.Vertices = append(vertex1.Vertices, id2)
	}
	found = false
	for _, vertex := range vertex2.Vertices {
		if vertex == id1 {
			found = true
			break
		}
	}
	if !found {
		vertex2.Vertices = append(vertex2.Vertices, id1)
	}

	// edge lists in vertices are modified
	data1 := db.serializeVertex(vertex1)
	data2 := db.serializeVertex(vertex2)
	batcher.Put(vertexKey1, data1)
	batcher.Put(vertexKey2, data2)

	// GraphEdge should have smaller id as id1
	properties := make(ElementProperties)
	if id1 > id2 {
		temp := id2
		id2 = id1
		id1 = temp
	}
	edge := GraphEdge{&GraphElement{properties, weight}, VertexPairID{id1, id2}}
	data := db.serializeEdge(edge)

	EdgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}

	batcher.Put(EdgeKey, data)
	err = batcher.Commit()
	return err
}

// SetVertexWeight requires 1 read, 1 write
func (db *GraphKeyValueDB) SetVertexWeight(graph Key, id VertexID, weight float64) error {
	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}

	vertex.Weight = weight
	data := db.serializeVertex(vertex)
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	err = db.Put(vertexKey, data)
	return err
}

// SetEdgeWeight requires 1 read, 1 write
func (db *GraphKeyValueDB) SetEdgeWeight(graph Key, id1 VertexID, id2 VertexID, weight float64) error {
	edge, err := db.GetEdge(graph, id1, id2)
	if err != nil {
		return nil
	}

	edge.Weight = weight
	data := db.serializeEdge(edge)
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	err = db.Put(edgeKey, data)
	return err
}

// SetVertexProperty modifies the vertex and adds a property vertex key (1 read, 2 writes)
func (db *GraphKeyValueDB) SetVertexProperty(graph Key, id VertexID, key string, value []byte) error {
	// load data
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}

	// batch write to maintain atomicity
	batcher := db.dbbatch.NewBatch()

	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}
	batcher.Put(propKey, value)

	vertex.Properties[key] = struct{}{}
	data := db.serializeVertex(vertex)
	batcher.Put(vertexKey, data)
	err = batcher.Commit()
	return err
}

// SetEdgeProperty modifies the edge and adds a property vertex key (1 read, 2 writes)
func (db *GraphKeyValueDB) SetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string, value []byte) error {
	// load data
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}

	batcher := db.dbbatch.NewBatch()

	edge, err := db.GetEdge(graph, id1, id2)
	if err != nil {
		return nil
	}

	batcher.Put(propKey, value)

	edge.Properties[key] = struct{}{}
	data := db.serializeEdge(edge)
	batcher.Put(edgeKey, data)
	err = batcher.Commit()
	return err
}

// RemoveGraph retrieves all keys with the graph prefix and deletes them
func (db *GraphKeyValueDB) RemoveGraph(graph Key) error {
	batcher := db.dbbatch.NewBatch()

	keylb := &graphKey{keyVertex, graph.Bytes(), 0, 0, ""}
	keyub := &graphKey{keyMax, graph.Bytes(), 0, 0, ""}
	keys, err := db.KeysInRange(keylb, keyub)
	if err != nil {
		return err
	}

	// the whole graph is deleted as a batch
	for _, key := range keys {
		batcher.Delete(key)
	}
	err = batcher.Commit()
	return err
}

// RemoveVertex removes the vertex and all of its edges and properties
// (1 read, 1 + num edges + num properties writes)
func (db *GraphKeyValueDB) RemoveVertex(graph Key, id VertexID) error {
	batcher := db.dbbatch.NewBatch()

	keylb := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, ""}
	keyub := &graphKey{keyVertexProperty, graph.Bytes(), id, 1, ""}
	keys, err := db.KeysInRange(keylb, keyub)
	if err != nil {
		return err
	}

	// batch deletion for vertex and edge
	for _, key := range keys {
		batcher.Delete(key)
		if err != nil {
			return err
		}
	}

	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return err
	}

	// edges are removed in separate transactions from vertex and properties!
	for _, vid := range vertex.Vertices {
		db.RemoveEdge(graph, id, vid)
	}

	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	batcher.Delete(vertexKey)
	err = batcher.Commit()
	return err
}

// RemoveEdge removes the edge and all of its properties and modifies affected vertices
// (2 read, 3 + num properties writes)
func (db *GraphKeyValueDB) RemoveEdge(graph Key, id1 VertexID, id2 VertexID) error {
	// find vertex data
	vertex1, err := db.GetVertex(graph, id1)
	if err != nil {
		return err
	}
	vertex2, err := db.GetVertex(graph, id2)
	if err != nil {
		return err
	}

	vertexKey1 := &graphKey{keyVertex, graph.Bytes(), id1, 0, ""}
	vertexKey2 := &graphKey{keyVertex, graph.Bytes(), id2, 0, ""}

	// all writes are batched
	batcher := db.dbbatch.NewBatch()

	// remove vertex from vertex list
	delkey := 0
	for i, vertex := range vertex1.Vertices {
		if vertex == id2 {
			delkey = i
			break
		}
	}
	vertex1.Vertices = append(vertex1.Vertices[0:delkey], vertex1.Vertices[delkey+1:]...)
	delkey = 0
	for i, vertex := range vertex2.Vertices {
		if vertex == id1 {
			delkey = i
			break
		}
	}
	vertex2.Vertices = append(vertex2.Vertices[0:delkey], vertex2.Vertices[delkey+1:]...)

	// update vertices
	data1 := db.serializeVertex(vertex1)
	data2 := db.serializeVertex(vertex2)
	batcher.Put(vertexKey1, data1)
	batcher.Put(vertexKey2, data2)

	keylb := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, ""}
	keyub := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2 + 1, ""}
	keys, err := db.KeysInRange(keylb, keyub)
	if err != nil {
		return err
	}

	for _, key := range keys {
		batcher.Delete(key)
		if err != nil {
			return err
		}
	}

	vertexKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	batcher.Delete(vertexKey)
	err = batcher.Commit()
	return err
}

// RemoveVertexProperty retrieves vertex and batch removes property and modifies vertex
// (1 read, 2 writes)
func (db *GraphKeyValueDB) RemoveVertexProperty(graph Key, id VertexID, key string) error {
	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}

	delete(vertex.Properties, key)
	data := db.serializeVertex(vertex)
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}

	batcher := db.dbbatch.NewBatch()

	batcher.Put(vertexKey, data)
	if err != nil {
		return nil
	}

	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}
	batcher.Delete(propKey)
	err = batcher.Commit()
	return err
}

// RemoveEdgeProperty retrieves edge and batch removes property and modifies edge
// (1 read, 2 writes)
func (db *GraphKeyValueDB) RemoveEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) error {
	edge, err := db.GetEdge(graph, id1, id2)
	if err != nil {
		return nil
	}

	delete(edge.Properties, key)
	data := db.serializeEdge(edge)
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}

	batcher := db.dbbatch.NewBatch()

	batcher.Put(edgeKey, data)
	if err != nil {
		return nil
	}

	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}
	batcher.Delete(propKey)
	err = batcher.Commit()
	return err
}

// GetVertices uses a range query to get all vertices (#reads = #vertices)
func (db *GraphKeyValueDB) GetVertices(graph Key) ([]GraphVertex, error) {
	minid := VertexID(0)
	maxid := ^minid

	keylb := &graphKey{keyVertex, graph.Bytes(), minid, 0, ""}
	keyub := &graphKey{keyVertex, graph.Bytes(), maxid, 0, ""}
	keyvalues, err := db.GetRange(keylb, keyub)
	var vertexlist []GraphVertex
	if err != nil {
		return vertexlist, err
	}

	for _, keyvalue := range keyvalues {
		vertex, err := db.deserializeVertex(keyvalue.V)
		if err != nil {
			return vertexlist, err
		}
		vertexlist = append(vertexlist, vertex)
	}

	return vertexlist, nil
}

// GetVertex performs 1 read
func (db *GraphKeyValueDB) GetVertex(graph Key, id VertexID) (GraphVertex, error) {
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	var vertex GraphVertex
	data, err := db.Get(vertexKey)
	if err != nil {
		return vertex, err
	}
	vertex, err = db.deserializeVertex(data)
	return vertex, err
}

// GetEdge performs 1 read
func (db *GraphKeyValueDB) GetEdge(graph Key, id1 VertexID, id2 VertexID) (GraphEdge, error) {
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	var edge GraphEdge
	data, err := db.Get(edgeKey)
	if err != nil {
		return edge, err
	}
	edge, err = db.deserializeEdge(data)
	return edge, err
}

// GetEdges uses a range query to get all edges (#reads = #edges)
func (db *GraphKeyValueDB) GetEdges(graph Key) ([]GraphEdge, error) {
	minid := VertexID(0)
	maxid := ^minid

	keylb := &graphKey{keyEdge, graph.Bytes(), minid, minid, ""}
	keyub := &graphKey{keyEdge, graph.Bytes(), maxid, maxid, ""}

	keyvalues, err := db.GetRange(keylb, keyub)
	var edgelist []GraphEdge
	if err != nil {
		return edgelist, err
	}

	for _, keyvalue := range keyvalues {
		edge, err := db.deserializeEdge(keyvalue.V)
		if err != nil {
			return edgelist, err
		}
		edgelist = append(edgelist, edge)
	}

	return edgelist, nil
}

// GetVertexProperty performs 1 read (property name with vertex id encoded in key)
func (db *GraphKeyValueDB) GetVertexProperty(graph Key, id VertexID, key string) ([]byte, error) {
	// load data
	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}
	data, err := db.Get(propKey)
	return data, err
}

// GetEdgeProperty performs 1 read (property name with vertex ids encoded in key)
func (db *GraphKeyValueDB) GetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) ([]byte, error) {
	// load data
	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}
	data, err := db.Get(propKey)
	return data, err
}
