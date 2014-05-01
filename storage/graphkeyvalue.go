// +build graphkeyvalue

/*
   Package interacts with a key-value datastore to satisfy the GraphDB interface.
   For now, this is the only functionality that implements the GraphDB interface.  In the future,
   we might develop plugins for graph databases allowing deployment to choose.  This module
   is implemented as a separate engine but really just reuses the chosen key value engine.
   This distinction might be relevant in the future if multiple engines are supported.
*/

package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/janelia-flyem/dvid/dvid"
)

type graphType byte

const (
	keyVertex graphType = iota
	keyEdge
	keyVertexProperty
	keyEdgeProperty
)

// if keySize is no longer 9, there will be problems
const keySize = 9

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
	if len(b) < 18 {
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
	vertex1 := VertexID(binary.BigEndian.Uint64(b[start:]))
	start += 8
	vertex2 := VertexID(0)
	if graphType(b[0]) == keyEdge || graphType(b[0]) == keyEdgeProperty {
		vertex2 = VertexID(binary.BigEndian.Uint64(b[start:]))
		start += 8
	}
	property := ""
	if graphType(b[0]) == keyVertexProperty || graphType(b[0]) == keyEdgeProperty {
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

	buf := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(buf, uint64(vertex1))
	b = append(b, buf...)

	if gk.keytype == keyEdge || gk.keytype == keyEdgeProperty {
		buf = make([]byte, 8, 8)
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
	vertexser := serializableVertex{vert.properties, vert.weight, vert.id, vert.vertices}
	compression, _ := dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
	data, _ := dvid.Serialize(vertexser, compression, dvid.NoChecksum)
	return data
}

func (db *GraphKeyValueDB) serializeEdge(edge GraphEdge) []byte {
	edgeser := serializableEdge{edge.properties, edge.weight, edge.vertexpair.vertex1, edge.vertexpair.vertex2}
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
	var properties ElementProperties
	if id1 > id2 {
		temp := id2
		id2 = id1
		id1 = temp
	}
	edge := GraphEdge{&GraphElement{properties, weight}, VertexPairID{id1, id2}}
	data := db.serializeEdge(edge)

	EdgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	err := db.Put(EdgeKey, data)
	return err
}

func (db *GraphKeyValueDB) SetVertexWeight(graph Key, id VertexID, weight float64) error {
	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}

	vertex.weight = weight
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

	edge.weight = weight
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
	if err != nil {
		return nil
	}

	vertex.properties[key] = struct{}{}
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
	if err != nil {
		return nil
	}

	edge.properties[key] = struct{}{}
	data := db.serializeEdge(edge)
	batcher.Put(edgeKey, data)
	err = batcher.Commit()
	return err
}

func (db *GraphKeyValueDB) RemoveGraph(graph Key) error {
	var kvDB KeyValueDB
	dbbatch, _ := kvDB.(Batcher)
	batcher := dbbatch.NewBatch()

	keys, err := db.KeysInRange(graph, graph)
	if err != nil {
		return err
	}

	for _, key := range keys {
		batcher.Delete(key)
		if err != nil {
			return err
		}
	}
	err = batcher.Commit()
	return nil
}

func (db *GraphKeyValueDB) RemoveVertex(graph Key, id VertexID) error {
	vertexKey := &graphKey{keyVertex, graph.Bytes(), id, 0, ""}
	err := db.Delete(vertexKey)
	return err
}

func (db *GraphKeyValueDB) RemoveEdge(graph Key, id1 VertexID, id2 VertexID) error {
	edgeKey := &graphKey{keyEdge, graph.Bytes(), id1, id2, ""}
	err := db.Delete(edgeKey)
	return err
}

func (db *GraphKeyValueDB) RemoveVertexProperty(graph Key, id VertexID, key string) error {
	vertex, err := db.GetVertex(graph, id)
	if err != nil {
		return nil
	}

	delete(vertex.properties, key)
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

	delete(edge.properties, key)
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
