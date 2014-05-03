// +build graphkeyvalue

/*
   Package interacts with a key-value datastore to satisfy the GraphDB interface.
   For now, this is the only functionality that implements the GraphDB interface.  In the future,
   we might develop plugins for graph databases allowing deployers to choose.  This module
   is implemented as a separate engine but really just reuses the chosen key value engine.
   This distinction might be relevant in the future if multiple engines are supported.
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

// keySize is the expected DVID prefix size after which graph specific key
// information is encoded
const keySize = 9

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
	if len(b) < (keySize + 9) {
		return nil, fmt.Errorf("Malformed graphKey bytes (too few): %x", b)
	}
	if b[0] != byte(KeyData) {
		return nil, fmt.Errorf("Cannot convert %s Key Type into graphKey", KeyType(b[0]))
	}
	/*
	        start := 1
	   	dataset, length := dvid.LocalID32FromBytes(b[start:])
	   	start += length
	   	data, length := dvid.LocalIDFromBytes(b[start:])
	   	start += length
	   	version, _ := dvid.LocalIDFromBytes(b[start:])
	   	start += length

	   	basekey := &DataKey{dvid.DatasetLocalID(dataset), dvid.DataLocalID(data), dvid.VersionLocalID(version)}
	*/
	basekey := b[0:keySize]
	start := keySize
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

	return &graphKey{graphType(b[0]), basekey, vertex1, vertex2, property}, nil
}

// always write smaller node out first
func (gk *graphKey) Bytes() []byte {
	b := gk.basekey
	b = append(b, byte(gk.keytype))

	vertex1 := gk.vertex1
	vertex2 := gk.vertex2
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

func (gk *graphKey) BytesString() string {
	return string(gk.Bytes())
}

func (gk *graphKey) String() string {
	return fmt.Sprintf("%x", gk.Bytes())
}

type GraphKeyValueDB struct {
	KeyValueDB
}

// NewGraphStore returns a graph backend that uses the provided keyvalue datastore (the keyvalue
// parameter will likely be unnecessary for future graph implementations)
func NewGraphStore(path string, create bool, config dvid.Config, kvdb KeyValueDB) (Engine, error) {
	_, ok := kvdb.(Batcher)
	var err error
	if !ok {
		err = fmt.Errorf("DVID key-value needed by graph does not support batch write")
	}
	graphdb := &GraphKeyValueDB{kvdb}
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

type serializableVertex struct {
	properties ElementProperties
	weight     float64
	id         VertexID
	vertices   []VertexID
}

type serializableEdge struct {
	properties ElementProperties
	weight     float64
	vertex1    VertexID
	vertex2    VertexID
}

func (db *GraphKeyValueDB) serializeVertex(vert GraphVertex) []byte {
	vertexser := serializableVertex{vert.Properties, vert.Weight, vert.Id, vert.Vertices}
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	data, _ := dvid.Serialize(vertexser, compression, dvid.NoChecksum)
	return data
}

func (db *GraphKeyValueDB) serializeEdge(edge GraphEdge) []byte {
	edgeser := serializableEdge{edge.Properties, edge.Weight, edge.Vertexpair.Vertex1, edge.Vertexpair.Vertex2}
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	data, _ := dvid.Serialize(edgeser, compression, dvid.NoChecksum)
	return data
}

func (db *GraphKeyValueDB) deserializeVertex(vertexdata []byte) (GraphVertex, error) {
	vertexser := new(serializableVertex)
	var vertex GraphVertex
	err := dvid.Deserialize(vertexdata, vertexser)
	if err != nil {
		return vertex, err
	}
	vertex = GraphVertex{&GraphElement{vertexser.properties, vertexser.weight}, vertexser.id, vertexser.vertices}
	return vertex, nil
}

func (db *GraphKeyValueDB) deserializeEdge(edgedata []byte) (GraphEdge, error) {
	edgeser := new(serializableEdge)
	err := dvid.Deserialize(edgedata, edgeser)
	var edge GraphEdge
	if err != nil {
		return edge, err
	}
	edge = GraphEdge{&GraphElement{edgeser.properties, edgeser.weight}, VertexPairID{edgeser.vertex1, edgeser.vertex2}}
	return edge, nil
}

func (db *GraphKeyValueDB) CreateGraph(graph Key) error {
	return nil
}

func (db *GraphKeyValueDB) AddVertex(graph Key, id VertexID, weight float64) error {
	var properties ElementProperties
	var vertices []VertexID
	vertex := GraphVertex{&GraphElement{properties, weight}, id, vertices}
	data := db.serializeVertex(vertex)
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	err := db.Put(vertexKey, data)
	return err
}

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

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

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

	data1 := db.serializeVertex(vertex1)
	data2 := db.serializeVertex(vertex2)
	batcher.Put(vertexKey1, data1)
	batcher.Put(vertexKey2, data2)

	var properties ElementProperties
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

func (db *GraphKeyValueDB) SetVertexProperty(graph Key, id VertexID, key string, value []byte) error {
	// load data
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

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

func (db *GraphKeyValueDB) SetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string, value []byte) error {
	// load data
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

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

func (db *GraphKeyValueDB) RemoveGraph(graph Key) error {
	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

	keylb := &graphKey{keyVertex, graph.Bytes(), 0, 0, ""}
	keyub := &graphKey{keyMax, graph.Bytes(), 0, 0, ""}
	keys, err := db.KeysInRange(keylb, keyub)
	if err != nil {
		return err
	}

	for _, key := range keys {
		batcher.Delete(key)
	}
	err = batcher.Commit()
	return err
}

func (db *GraphKeyValueDB) RemoveVertex(graph Key, id VertexID) error {
	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

	keylb := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, ""}
	keyub := &graphKey{keyVertexProperty, graph.Bytes(), id, 1, ""}
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

	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return err
	}

	for _, vid := range vertex.Vertices {
		db.RemoveEdge(graph, id, vid)
	}

	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	batcher.Delete(vertexKey)
	err = batcher.Commit()
	return err
}

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

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

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

func (db *GraphKeyValueDB) RemoveVertexProperty(graph Key, id VertexID, key string) error {
	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}

	delete(vertex.Properties, key)
	data := db.serializeVertex(vertex)
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

	batcher.Put(vertexKey, data)
	if err != nil {
		return nil
	}

	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}
	batcher.Delete(propKey)
	err = batcher.Commit()
	return err
}

func (db *GraphKeyValueDB) RemoveEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) error {
	edge, err := db.GetEdge(graph, id1, id2)
	if err != nil {
		return nil
	}

	delete(edge.Properties, key)
	data := db.serializeEdge(edge)
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}

	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

	batcher.Put(edgeKey, data)
	if err != nil {
		return nil
	}

	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}
	batcher.Delete(propKey)
	err = batcher.Commit()
	return err
}

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

func (db *GraphKeyValueDB) GetEdges(graph Key) ([]GraphEdge, error) {
	minid := VertexID(0)
	maxid := ^minid

	keylb := &graphKey{keyVertex, graph.Bytes(), minid, minid, ""}
	keyub := &graphKey{keyVertex, graph.Bytes(), maxid, maxid, ""}

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

func (db *GraphKeyValueDB) GetVertexProperty(graph Key, id VertexID, key string) ([]byte, error) {
	// load data
	propKey := &graphKey{keyVertexProperty, graph.Bytes(), id, 0, key}
	data, err := db.Get(propKey)
	return data, err
}

func (db *GraphKeyValueDB) GetEdgeProperty(graph Key, id1 VertexID, id2 VertexID, key string) ([]byte, error) {
	// load data
	propKey := &graphKey{keyEdgeProperty, graph.Bytes(), id1, id2, key}
	data, err := db.Get(propKey)
	return data, err
}
