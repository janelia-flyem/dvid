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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/janelia-flyem/dvid/dvid"
)

// graphType enumerates the graph key types
type graphType byte

const (
	keyVertex graphType = iota
	keyEdge
	keyVertexProperty
	keyEdgeProperty
	keyMax
)

const vertexIDSize = 8

// graphIndex implements the dvid.Index interface and is the graph-specific
// component of a DataKey for the graph db.
type graphIndex struct {
	keytype  graphType
	vertex1  dvid.VertexID
	vertex2  dvid.VertexID
	property string
}

// ----- dvid.Index implementation -----

// Duplicate returns a duplicate Index
func (i *graphIndex) Duplicate() dvid.Index {
	dup := *i
	return &dup
}

// Bytes returns a byte representation of the Index.  Integer components of
// the Index should probably be serialized in big endian for improved
// lexicographic ordering.
func (i *graphIndex) Bytes() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(i.keytype))

	vertex1 := i.vertex1
	vertex2 := i.vertex2
	// always ensure that the smaller vertex goes first for an edge
	if i.keytype == keyEdge || i.keytype == keyEdgeProperty {
		if i.vertex1 > i.vertex2 {
			vertex2 = i.vertex1
			vertex1 = i.vertex2
		}
	}
	binary.Write(buf, binary.BigEndian, uint64(vertex1))

	if i.keytype == keyEdge || i.keytype == keyEdgeProperty {
		binary.Write(buf, binary.BigEndian, uint64(vertex2))
	}
	if i.property != "" {
		buf.WriteString(i.property)
	}
	return buf.Bytes()
}

// BytesToIndex returns an Index from a byte representation
// TODO -- Add error checking on slice format.
func (i *graphIndex) IndexFromBytes(b []byte) error {
	keyType := graphType(b[0])
	vertex1 := dvid.VertexID(binary.BigEndian.Uint64(b[1 : 1+vertexIDSize]))
	vertex2 := dvid.VertexID(0)
	start := 1 + vertexIDSize
	if keyType == keyEdge || keyType == keyEdgeProperty {
		vertex2 = dvid.VertexID(binary.BigEndian.Uint64(b[start : start+vertexIDSize]))
		start += vertexIDSize
	}
	property := ""
	if keyType == keyVertexProperty || keyType == keyEdgeProperty {
		property = string(b[start:])
	}

	*i = graphIndex{keyType, vertex1, vertex2, property}
	return nil
}

// Hash provides a consistent mapping from an Index to an integer (0,n]
func (i *graphIndex) Hash(n int) int {
	return int(i.vertex1+i.vertex2) % n
}

// Scheme returns a string describing the indexing scheme.
func (i *graphIndex) Scheme() string {
	return "Graph Indexing"
}

// String returns a hexadecimal string representation
func (i *graphIndex) String() string {
	return fmt.Sprintf("<GraphType %d: vertex1 %d, vertex2 %d, prop %s>",
		i.keytype, i.vertex1, i.vertex2, i.property)
}

// GraphKeyValueDB defines a type that embeds a KeyValueDB using that engine to
// store all graph objects
type GraphKeyValueDB struct {
	OrderedKeyValueDB
	dbbatch KeyValueBatcher
}

// NewGraphStore returns a graph backend that uses the provided keyvalue datastore.
// If a first-class graph store is used in the future, this function must be changed.
func NewGraphStore(kvdb OrderedKeyValueDB) (GraphDB, error) {
	dbbatch, ok := kvdb.(KeyValueBatcher)
	var err error
	if !ok {
		err = fmt.Errorf("DVID key-value needed by graph does not support batch write")
	}
	graphdb := &GraphKeyValueDB{kvdb, dbbatch}
	return graphdb, err
}

// GetName returns the name of theengien
func (db *GraphKeyValueDB) String() string {
	return "graph db using key value datastore"
}

// Close does nothing, explicitly close the key value DB instead
func (db *GraphKeyValueDB) Close() {
}

// -- Add Serialization Capabilities for Vertex and Edge

// serializeVertex serializes a dvid.GraphVertex (compression turned off for now)
func (db *GraphKeyValueDB) serializeVertex(vert dvid.GraphVertex) []byte {
	// encode: vertex id, vertex weight, num vertices, vertex array,
	// num properties, property array
	total_size := 24 + 8*len(vert.Vertices) + 8

	// find size for property strings (account for null character)
	for propname, _ := range vert.Properties {
		total_size += len(propname) + 1
	}

	// create byte buffer
	buf := make([]byte, total_size, total_size)

	// encode vertex id
	start := 0
	binary.LittleEndian.PutUint64(buf[start:], uint64(vert.Id))
	start += 8

	// encode vertex weight
	floatbits := math.Float64bits(vert.Weight)
	binary.LittleEndian.PutUint64(buf[start:], floatbits)
	start += 8

	// encode number of vertices
	binary.LittleEndian.PutUint64(buf[start:], uint64(len(vert.Vertices)))
	start += 8

	// encode vertex partners
	for _, vertpartner := range vert.Vertices {
		binary.LittleEndian.PutUint64(buf[start:], uint64(vertpartner))
		start += 8
	}

	// encode number of properties
	binary.LittleEndian.PutUint64(buf[start:], uint64(len(vert.Properties)))
	start += 8

	// encode property strings
	for propname, _ := range vert.Properties {
		for _, propchar := range propname {
			buf[start] = byte(propchar)
			start += 1
		}
		buf[start] = byte(0)
		start += 1
	}

	// encode DVID related info (currenlty compression is disabled)
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	finalbuf, _ := dvid.SerializeData(buf, compression, dvid.NoChecksum)

	return finalbuf
}

// serializeEdge serializes a dvid.GraphEdge (compression turned off for now)
func (db *GraphKeyValueDB) serializeEdge(edge dvid.GraphEdge) []byte {
	// encode: vertex1 id, vertex2 id, weight,
	// num properties, property array
	total_size := 32

	// find size for property strings (account for null character)
	for propname, _ := range edge.Properties {
		total_size += len(propname) + 1
	}

	// create byte buffer
	buf := make([]byte, total_size, total_size)

	// encode vertex 1
	start := 0
	binary.LittleEndian.PutUint64(buf[start:], uint64(edge.Vertexpair.Vertex1))
	start += 8

	// encode vertex 2
	binary.LittleEndian.PutUint64(buf[start:], uint64(edge.Vertexpair.Vertex2))
	start += 8

	// encode weight
	floatbits := math.Float64bits(edge.Weight)
	binary.LittleEndian.PutUint64(buf[start:], floatbits)
	start += 8

	// encode number of properties
	binary.LittleEndian.PutUint64(buf[start:], uint64(len(edge.Properties)))
	start += 8

	// encode property strings
	for propname, _ := range edge.Properties {
		for _, propchar := range propname {
			buf[start] = byte(propchar)
			start += 1
		}
		buf[start] = byte(0)
		start += 1
	}

	// encode DVID related info (currenlty compression is disabled)
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	finalbuf, _ := dvid.SerializeData(buf, compression, dvid.NoChecksum)

	return finalbuf
}

// deserializeVertex deserializes a dvid.GraphVertex (compression turned off for now)
func (db *GraphKeyValueDB) deserializeVertex(vertexdata []byte) (dvid.GraphVertex, error) {
	// create vertex to be returned
	vert := dvid.GraphVertex{GraphElement: &dvid.GraphElement{}}

	// if vertexdata is empty return an error
	if vertexdata == nil || len(vertexdata) == 0 {
		return vert, fmt.Errorf("Vertex data empty")
	}

	// boilerplate deserialization from DVID
	data, _, err := dvid.DeserializeData(vertexdata, true)
	if err != nil {
		return vert, err
	}

	// load data from vertex (vertex id, vertex weight, num vertices,
	// vertex array, num properties, property array

	// load vertex id
	start := 0
	vert.Id = dvid.VertexID(binary.LittleEndian.Uint64(data[start:]))
	start += 8

	// load vertex weight
	floatbits := binary.LittleEndian.Uint64(data[start:])
	vert.Weight = math.Float64frombits(floatbits)
	start += 8

	// number of vertices
	count := binary.LittleEndian.Uint64(data[start:])
	start += 8

	vert.Vertices = make([]dvid.VertexID, count, count)
	// load vertices
	for i := uint64(0); i < count; i++ {
		vert.Vertices[i] = dvid.VertexID(binary.LittleEndian.Uint64(data[start:]))
		start += 8
	}

	// number of properties
	vert.Properties = make(dvid.ElementProperties)
	count = binary.LittleEndian.Uint64(data[start:])
	start += 8

	// create property strings
	for i := uint64(0); i < count; i++ {
		propertyname := string("")
		// null separated strings
		for data[start] != byte(0) {
			propertyname += string(data[start])
			start += 1
		}
		vert.Properties[propertyname] = struct{}{}

		// increment beyond null
		start += 1
	}

	return vert, nil
}

// deserializeEdge deserializes a dvid.GraphEdge (compression turned off for now)
func (db *GraphKeyValueDB) deserializeEdge(edgedata []byte) (dvid.GraphEdge, error) {
	// create edge to be returned
	edge := dvid.GraphEdge{GraphElement: &dvid.GraphElement{}}

	// if edgedata is empty return an error
	if edgedata == nil || len(edgedata) == 0 {
		return edge, fmt.Errorf("Edge data empty")
	}

	// boilerplate deserialization from DVID
	data, _, err := dvid.DeserializeData(edgedata, true)
	if err != nil {
		return edge, err
	}

	// load data from edge (vertex1 id, vertex2 id, edge weight,
	// num properties, property array

	// load vertex1 id
	start := 0
	edge.Vertexpair.Vertex1 = dvid.VertexID(binary.LittleEndian.Uint64(data[start:]))
	start += 8

	// load vertex2 id
	edge.Vertexpair.Vertex2 = dvid.VertexID(binary.LittleEndian.Uint64(data[start:]))
	start += 8

	// load edge weight
	floatbits := binary.LittleEndian.Uint64(data[start:])
	edge.Weight = math.Float64frombits(floatbits)
	start += 8

	// number of properties
	count := binary.LittleEndian.Uint64(data[start:])
	start += 8

	// create property strings
	edge.Properties = make(dvid.ElementProperties)
	for i := uint64(0); i < count; i++ {
		propertyname := string("")
		// null separated strings
		for data[start] != 0 {
			propertyname += string(data[start])
			start += 1
		}
		edge.Properties[propertyname] = struct{}{}

		// increment beyond null
		start += 1
	}

	return edge, nil
}

// CreateGraph does nothing as the graph keyspace uniquely defines a graph when
// using a single key-value datastore
func (db *GraphKeyValueDB) CreateGraph(ctx Context) error {
	return nil
}

// AddVertex requires one key-value put
func (db *GraphKeyValueDB) AddVertex(ctx Context, id dvid.VertexID, weight float64) error {
	properties := make(dvid.ElementProperties)
	var vertices []dvid.VertexID
	vertex := dvid.GraphVertex{&dvid.GraphElement{properties, weight}, id, vertices}
	data := db.serializeVertex(vertex)
	index := &graphIndex{keyVertex, id, 0, ""}
	err := db.Put(ctx, index.Bytes(), data)
	return err
}

// AddEdge reads both vertices, modifies the vertex edge lists, and then creates an edge
// (2 read ops, 3 write ops)
func (db *GraphKeyValueDB) AddEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error {
	// find vertex data
	vertex1, err := db.GetVertex(ctx, id1)
	if err != nil {
		return err
	}
	vertex2, err := db.GetVertex(ctx, id2)
	if err != nil {
		return err
	}

	vertexIndex1 := &graphIndex{keyVertex, id1, 0, ""}
	vertexIndex2 := &graphIndex{keyVertex, id2, 0, ""}

	// make sure all writing is done in a batch to maintain atomicity
	batcher := db.dbbatch.NewBatch(ctx)

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
	batcher.Put(vertexIndex1.Bytes(), data1)
	batcher.Put(vertexIndex2.Bytes(), data2)

	// GraphEdge should have smaller id as id1
	properties := make(dvid.ElementProperties)
	if id1 > id2 {
		temp := id2
		id2 = id1
		id1 = temp
	}
	edge := dvid.GraphEdge{&dvid.GraphElement{properties, weight}, dvid.VertexPairID{id1, id2}}
	data := db.serializeEdge(edge)

	edgeIndex := &graphIndex{keyEdge, id1, id2, ""}

	batcher.Put(edgeIndex.Bytes(), data)
	err = batcher.Commit()
	return err
}

// SetVertexWeight requires 1 read, 1 write
func (db *GraphKeyValueDB) SetVertexWeight(ctx Context, id dvid.VertexID, weight float64) error {
	vertex, err := db.GetVertex(ctx, id)
	if err != nil {
		return nil
	}

	vertex.Weight = weight
	data := db.serializeVertex(vertex)
	vertexIndex := &graphIndex{keyVertex, id, 0, ""}
	err = db.Put(ctx, vertexIndex.Bytes(), data)
	return err
}

// SetEdgeWeight requires 1 read, 1 write
func (db *GraphKeyValueDB) SetEdgeWeight(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, weight float64) error {
	edge, err := db.GetEdge(ctx, id1, id2)
	if err != nil {
		return nil
	}

	edge.Weight = weight
	data := db.serializeEdge(edge)
	edgeIndex := &graphIndex{keyEdge, id1, id2, ""}
	err = db.Put(ctx, edgeIndex.Bytes(), data)
	return err
}

// SetVertexProperty modifies the vertex and adds a property vertex key (1 read, 2 writes)
func (db *GraphKeyValueDB) SetVertexProperty(ctx Context, id dvid.VertexID, key string, value []byte) error {
	// load data
	vertexIndex := &graphIndex{keyVertex, id, 0, ""}
	propIndex := &graphIndex{keyVertexProperty, id, 0, key}

	// batch write to maintain atomicity
	batcher := db.dbbatch.NewBatch(ctx)

	vertex, err := db.GetVertex(ctx, id)
	if err != nil {
		return nil
	}
	batcher.Put(propIndex.Bytes(), value)

	vertex.Properties[key] = struct{}{}
	data := db.serializeVertex(vertex)
	batcher.Put(vertexIndex.Bytes(), data)
	err = batcher.Commit()
	return err
}

// SetEdgeProperty modifies the edge and adds a property vertex key (1 read, 2 writes)
func (db *GraphKeyValueDB) SetEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string, value []byte) error {
	// load data
	edgeIndex := &graphIndex{keyEdge, id1, id2, ""}
	propIndex := &graphIndex{keyEdgeProperty, id1, id2, key}

	batcher := db.dbbatch.NewBatch(ctx)

	edge, err := db.GetEdge(ctx, id1, id2)
	if err != nil {
		return nil
	}

	batcher.Put(propIndex.Bytes(), value)

	edge.Properties[key] = struct{}{}
	data := db.serializeEdge(edge)
	batcher.Put(edgeIndex.Bytes(), data)
	err = batcher.Commit()
	return err
}

// RemoveGraph retrieves all keys with the graph prefix and deletes them
func (db *GraphKeyValueDB) RemoveGraph(ctx Context) error {
	batcher := db.dbbatch.NewBatch(ctx)

	keylb := &graphIndex{keyVertex, 0, 0, ""}
	keyub := &graphIndex{keyMax, 0, 0, ""}
	keys, err := db.KeysInRange(ctx, keylb.Bytes(), keyub.Bytes())
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
func (db *GraphKeyValueDB) RemoveVertex(ctx Context, id dvid.VertexID) error {
	batcher := db.dbbatch.NewBatch(ctx)

	keylb := &graphIndex{keyVertexProperty, id, 0, ""}
	keyub := &graphIndex{keyVertexProperty, id, 1, ""}
	keys, err := db.KeysInRange(ctx, keylb.Bytes(), keyub.Bytes())
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

	vertex, err := db.GetVertex(ctx, id)
	if err != nil {
		return err
	}

	// edges are removed in separate transactions from vertex and properties!
	for _, vid := range vertex.Vertices {
		db.RemoveEdge(ctx, id, vid)
	}

	vertexIndex := &graphIndex{keyVertex, id, 0, ""}
	batcher.Delete(vertexIndex.Bytes())
	err = batcher.Commit()
	return err
}

// RemoveEdge removes the edge and all of its properties and modifies affected vertices
// (2 read, 3 + num properties writes)
func (db *GraphKeyValueDB) RemoveEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID) error {
	// find vertex data
	vertex1, err := db.GetVertex(ctx, id1)
	if err != nil {
		return err
	}
	vertex2, err := db.GetVertex(ctx, id2)
	if err != nil {
		return err
	}

	vertexKey1 := &graphIndex{keyVertex, id1, 0, ""}
	vertexKey2 := &graphIndex{keyVertex, id2, 0, ""}

	// all writes are batched
	batcher := db.dbbatch.NewBatch(ctx)

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
	batcher.Put(vertexKey1.Bytes(), data1)
	batcher.Put(vertexKey2.Bytes(), data2)

	keylb := &graphIndex{keyEdgeProperty, id1, id2, ""}
	keyub := &graphIndex{keyEdgeProperty, id1, id2 + 1, ""}
	keys, err := db.KeysInRange(ctx, keylb.Bytes(), keyub.Bytes())
	if err != nil {
		return err
	}

	for _, key := range keys {
		batcher.Delete(key)
		if err != nil {
			return err
		}
	}

	vertexIndex := &graphIndex{keyEdge, id1, id2, ""}
	batcher.Delete(vertexIndex.Bytes())
	err = batcher.Commit()
	return err
}

// RemoveVertexProperty retrieves vertex and batch removes property and modifies vertex
// (1 read, 2 writes)
func (db *GraphKeyValueDB) RemoveVertexProperty(ctx Context, id dvid.VertexID, key string) error {
	vertex, err := db.GetVertex(ctx, id)
	if err != nil {
		return nil
	}

	delete(vertex.Properties, key)
	data := db.serializeVertex(vertex)
	vertexIndex := &graphIndex{keyVertex, id, 0, ""}

	batcher := db.dbbatch.NewBatch(ctx)

	batcher.Put(vertexIndex.Bytes(), data)
	if err != nil {
		return nil
	}

	propIndex := &graphIndex{keyVertexProperty, id, 0, key}
	batcher.Delete(propIndex.Bytes())
	err = batcher.Commit()
	return err
}

// RemoveEdgeProperty retrieves edge and batch removes property and modifies edge
// (1 read, 2 writes)
func (db *GraphKeyValueDB) RemoveEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string) error {
	edge, err := db.GetEdge(ctx, id1, id2)
	if err != nil {
		return nil
	}

	delete(edge.Properties, key)
	data := db.serializeEdge(edge)
	edgeIndex := &graphIndex{keyEdge, id1, id2, ""}

	batcher := db.dbbatch.NewBatch(ctx)

	batcher.Put(edgeIndex.Bytes(), data)
	if err != nil {
		return nil
	}

	propIndex := &graphIndex{keyEdgeProperty, id1, id2, key}
	batcher.Delete(propIndex.Bytes())
	err = batcher.Commit()
	return err
}

// GetVertices uses a range query to get all vertices (#reads = #vertices)
func (db *GraphKeyValueDB) GetVertices(ctx Context) ([]dvid.GraphVertex, error) {
	minid := dvid.VertexID(0)
	maxid := ^minid

	keylb := &graphIndex{keyVertex, minid, 0, ""}
	keyub := &graphIndex{keyVertex, maxid, 0, ""}
	keyvalues, err := db.GetRange(ctx, keylb.Bytes(), keyub.Bytes())
	var vertexlist []dvid.GraphVertex
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
func (db *GraphKeyValueDB) GetVertex(ctx Context, id dvid.VertexID) (dvid.GraphVertex, error) {
	vertexIndex := &graphIndex{keyVertex, id, 0, ""}
	var vertex dvid.GraphVertex
	data, err := db.Get(ctx, vertexIndex.Bytes())
	if err != nil {
		return vertex, err
	}
	vertex, err = db.deserializeVertex(data)
	return vertex, err
}

// GetEdge performs 1 read
func (db *GraphKeyValueDB) GetEdge(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID) (dvid.GraphEdge, error) {
	edgeIndex := &graphIndex{keyEdge, id1, id2, ""}
	var edge dvid.GraphEdge
	data, err := db.Get(ctx, edgeIndex.Bytes())
	if err != nil {
		return edge, err
	}
	edge, err = db.deserializeEdge(data)
	return edge, err
}

// GetEdges uses a range query to get all edges (#reads = #edges)
func (db *GraphKeyValueDB) GetEdges(ctx Context) ([]dvid.GraphEdge, error) {
	minid := dvid.VertexID(0)
	maxid := ^minid

	keylb := &graphIndex{keyEdge, minid, minid, ""}
	keyub := &graphIndex{keyEdge, maxid, maxid, ""}

	keyvalues, err := db.GetRange(ctx, keylb.Bytes(), keyub.Bytes())
	var edgelist []dvid.GraphEdge
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
func (db *GraphKeyValueDB) GetVertexProperty(ctx Context, id dvid.VertexID, key string) ([]byte, error) {
	// load data
	propIndex := &graphIndex{keyVertexProperty, id, 0, key}
	data, err := db.Get(ctx, propIndex.Bytes())
	return data, err
}

// GetEdgeProperty performs 1 read (property name with vertex ids encoded in key)
func (db *GraphKeyValueDB) GetEdgeProperty(ctx Context, id1 dvid.VertexID, id2 dvid.VertexID, key string) ([]byte, error) {
	// load data
	propIndex := &graphIndex{keyEdgeProperty, id1, id2, key}
	data, err := db.Get(ctx, propIndex.Bytes())
	return data, err
}
