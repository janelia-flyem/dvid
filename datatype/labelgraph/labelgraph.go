/*
	Package keyvalue implements DVID support for data using the underlying graph storage engine.
*/
package labelgraph

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/gojsonschema"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/labelgraph"
)

const graphSchema = `
{ "$schema": "http://json-schema.org/schema#",
  "title": "Representation for a graph.  List of vertices with weights and their edges.  The weight terms are not mandatory",
  "type": "object",
  "definitions": {
    "vertex": {
      "description": "Describes a vertex in a graph",
      "type": "object",
      "properties": {
        "id": { "type": "integer", "description": "64 bit ID for vertex" },
        "weight": { "type": "number", "description": "Weight/size of vertex" }
      },
      "required": ["id"]
    },
    "edge": {
      "description": "Describes an edge in a graph",
      "type": "object",
      "properties": {
        "id1": { "type": "integer", "description": "64 bit ID for vertex1" },
        "id2": { "type": "integer", "description": "64 bit ID for vertex2" },
        "weight": { "type": "number", "description": "Weight/size of edge" }
      },
      "required": ["id1", "id2"]
    }
  },
  "properties": {
    "vertices": { 
      "description": "array of vertices",
      "type": "array",
      "items": {"$ref": "#/definitions/vertex"},
      "uniqueItems": true
    },
    "edges": { 
      "description": "array of edges",
      "type": "array",
      "items": {"$ref": "#/definitions/edge"},
      "uniqueItems": true
    }
  }
}
`

const HelpMessage = `
API for 'labelgraph' datatype (github.com/janelia-flyem/dvid/datatype/labelgraph)
History and therefore UNDO is not yet supported.  I will need to access the KeyValue
setter and maintain a transaction log (how to avoid collisions with graph keyspacee
if using the same key value store -- avoid conflict by doing a dynamic check?!).
=============================================================================

HTTP API (Level 2 REST):

Note that browsers support HTTP PUT and DELETE via javascript but only GET/POST are
included in HTML specs.  For ease of use in constructing clients, HTTP POST is used
to create or modify resources in an idempotent fashion.

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET <api URL>/node/3f8c/stuff/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.

GET  <api URL>/node/<UUID>/<data name>/subgraph
POST  <api URL>/node/<UUID>/<data name>/subgraph
DELETE  <api URL>/node/<UUID>/<data name>/subgraph

    Performs graph-wide applications.  GET (retrieve subgraph), POST (add subgraph -- does
    not change existing graph connections), DELETE (delete whole graph or subgraph indicated
    by a list of nodes or list of edges).  POSTs or DELETEs using this URI will erase
    all merge history.

    Example: 

    GET <api URL>/node/3f8c/stuff

    Returns the graph associated with the data "stuff" in version
    node 3f8c.  An optional JSON can be specified listing the vertices to be included.

    The "Content-type" of the HTTP response are
    "application/json" as a node list and edge list.  Vertex elements contain a "id"
    and "weight" (float).  Edge elements contain "id1", "id2", and "weight" (float).
    When deleting or asking for a subgraph, specifying the vertex list will delete or retrieve
    the edges from these vertices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.

    
POST  <api URL>/node/<UUID>/<data name>/merge/[nohistory]

    Merge a list of vertices as specified by a vertex array called "vertices".
    The last vertex is the vertex ID that will be used.  If nohistory is specified,
    the history of this transaction is not saved.  Edge and Vertex weights will be summed.
    If different weights are desired, all edge and vertex weights should be specified as done
    when posting a subgraph.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.



POST  <api URL>/node/<UUID>/<data name>/undomerge

    Undoes last merge.  An error message is returned if no UNDO occurs.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.

GET  <api URL>/node/<UUID>/<data name>/property/<vertex1>/<key>
POST (really PUT)  <api URL>/node/<UUID>/<data name>/property/<vertex>/<key>
DELETE  <api URL>/node/<UUID>/<data name>/property/<vertex>/<key>
    
    Retrive or set a vertex property.

    The "Content-type" of the HTTP response and the request are
    "application/octet-stream" for arbitrary binary data.
    
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    vertex        ID of vertex
    key           Name of the property

GET  <api URL>/node/<UUID>/<data name>/property/<vertex1>/<vertex2>/<key>
POST (really PUT)  <api URL>/node/<UUID>/<data name>/property/<vertex1>/<vertex2>/<key>
DELETE <api URL>/node/<UUID>/<data name>/property/<vertex1>/<vertex2>/<key>
    
    Retrive or set an edge property.

    The "Content-type" of the HTTP response and the request are
    "application/octet-stream" for arbitrary binary data.
    
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    vertex1       ID of vertex1
    vertex2       ID of vertex2
    key           Name of the property
`

func init() {
	kvtype := NewDatatype()
	kvtype.DatatypeID = &datastore.DatatypeID{
		Name:    "labelgraph",
		Url:     RepoUrl,
		Version: Version,
	}
	datastore.RegisterDatatype(kvtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
}

// Datatype embeds the datastore's Datatype to create a unique type for keyvalue functions.
type Datatype struct {
	datastore.Datatype
}

type labelVertex struct {
	id     storage.VertexID
	weight float64
}

type labelEdge struct {
	id1    storage.VertexID
	id2    storage.VertexID
	weight float64
}

type LabelGraph struct {
	vertices []labelVertex
	edges    []labelEdge
}

// NewDatatype returns a pointer to a new keyvalue Datatype with default values set.
func NewDatatype() (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
		GraphDB:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to new keyvalue data with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(id, dtype, c)
	if err != nil {
		return nil, err
	}
	return &Data{Data: basedata}, nil
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

func (d *Data) getGraphDB(uuid dvid.UUID) (storage.Key, storage.GraphDB, error) {
	// Compute the key
	var key storage.Key
	var db storage.GraphDB

	versionID, err := server.VersionLocalID(uuid)
	if err != nil {
		return key, db, err
	}
	key = d.DataKey(versionID, dvid.IndexString(""))
	db, err = server.GraphDB()
	if err != nil {
		return key, db, err
	}
	return key, db, nil
}

func (d *Data) handleSubgraph(uuid dvid.UUID, w http.ResponseWriter, labelgraph *LabelGraph, method string) error {
	key, db, err := d.getGraphDB(uuid)
	if err != nil {
		return err
	}
	labelgraph2 := new(LabelGraph)

	if method == "get" {
		var vertices []storage.GraphVertex
		var edges []storage.GraphEdge
		if len(labelgraph.vertices) > 0 {
			for _, vertex := range labelgraph.vertices {
				storedvert, _ := db.GetVertex(key, vertex.id)
				vertices = append(vertices, storedvert)
				for _, vert2 := range storedvert.Vertices {
					if storedvert.Id < vert2 {
						edge, _ := db.GetEdge(key, storedvert.Id, vert2)
						edges = append(edges, edge)
					}
				}
			}
		} else {
			vertices, _ = db.GetVertices(key)
			edges, _ = db.GetEdges(key)
		}
		for _, vertex := range vertices {
			labelgraph2.vertices = append(labelgraph2.vertices, labelVertex{vertex.Id, vertex.Weight})
		}
		for _, edge := range edges {
			labelgraph2.edges = append(labelgraph2.edges, labelEdge{edge.Vertexpair.Vertex1, edge.Vertexpair.Vertex2, edge.Weight})
		}
		m, err := json.Marshal(labelgraph2)
		if err != nil {
			return fmt.Errorf("Could not serialize graph")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(m))
	} else if method == "post" {
		for _, vertex := range labelgraph.vertices {
			db.AddVertex(key, vertex.id, vertex.weight)
		}
		for _, edge := range labelgraph.edges {
			db.AddEdge(key, edge.id1, edge.id2, edge.weight)
		}
	} else if method == "delete" {
		for _, vertex := range labelgraph.vertices {
			db.RemoveVertex(key, vertex.id)
		}
		for _, edge := range labelgraph.edges {
			db.RemoveEdge(key, edge.id1, edge.id2)
		}
	} else {
		err = fmt.Errorf("Does not support PUT")
	}

	return err
}

func (d *Data) handleMerge(uuid dvid.UUID, w http.ResponseWriter, labelgraph *LabelGraph, method string) error {
	key, db, err := d.getGraphDB(uuid)
	if err != nil {
		return err
	}

	numverts := len(labelgraph.vertices)
	if numverts < 2 {
		return fmt.Errorf("Must specify at least two vertices for merging")
	}

	var overlapweights map[storage.VertexID]float64
	vertweight := float64(0)
	var keepvertex storage.GraphVertex
	var allverts map[storage.VertexID]struct{}
	var keepverts map[storage.VertexID]struct{}

	// accumulate weights, find common edges
	for i, vertex := range labelgraph.vertices {
		vert, _ := db.GetVertex(key, vertex.id)
		allverts[vert.Id] = struct{}{}
		vertweight += vert.Weight

		if i == (numverts - 1) {
			keepvertex = vert
		} else {
			for _, vert2 := range vert.Vertices {
				edge, _ := db.GetEdge(key, vert.Id, vert2)
				overlapweights[vert2] += edge.Weight
			}
		}
	}

	// examine keep vertex (save edges so only the weights are updated)
	for _, vert2 := range keepvertex.Vertices {
		edge, _ := db.GetEdge(key, keepvertex.Id, vert2)
		overlapweights[vert2] += edge.Weight
		keepverts[vert2] = struct{}{}
	}

	// use specified weights even if marked as 0
	for _, edge := range labelgraph.edges {
		id := edge.id1
		baseid := edge.id2
		if keepvertex.Id == edge.id1 {
			id = edge.id2
			baseid = edge.id1
		}
		if baseid == keepvertex.Id {
			if _, ok := overlapweights[id]; ok {
				overlapweights[id] = edge.weight
			}
		}
	}

	// if user specifies an edge weight other than 0 (?! -- somehow allow in future)
	// use that weight
	if labelgraph.vertices[numverts-1].weight != 0 {
		vertweight = labelgraph.vertices[numverts-1].weight
	}

	for id2, newweight := range overlapweights {
		if _, ok := allverts[id2]; !ok {
			// only examine edges where the node is not internal
			if _, ok := keepverts[id2]; !ok {
				// if not in keepverts create new edge
				db.AddEdge(key, keepvertex.Id, id2, newweight)
			} else {
				// else just update weight
				db.SetEdgeWeight(key, keepvertex.Id, id2, newweight)
			}
		}
	}
	// update vertex weight
	db.SetVertexWeight(key, labelgraph.vertices[numverts-1].id, vertweight)

	// remove old vertices which will remove the old edges
	for i, vertex := range labelgraph.vertices {
		if i == (numverts - 1) {
			break
		}
		db.RemoveVertex(key, vertex.id)
	}

	return nil
}

func (d *Data) handleProperty(uuid dvid.UUID, w http.ResponseWriter, r *http.Request, path []string, method string) error {
	key, db, err := d.getGraphDB(uuid)
	if err != nil {
		return err
	}

	edgemode := false
	propertyname := path[1]
	if len(path) == 3 {
		edgemode = false
		propertyname = path[2]
	} else if len(path) != 2 {
		return fmt.Errorf("Incorrect number of parameters specified for handling properties")
	}
	temp, err := strconv.Atoi(path[0])
	if err != nil {
		return fmt.Errorf("Vertex number not provided")
	}
	id1 := storage.VertexID(temp)

	id2 := storage.VertexID(0)
	if edgemode {
		temp, err := strconv.Atoi(path[1])
		if err != nil {
			return fmt.Errorf("Vertex number not provided")
		}
		id2 = storage.VertexID(temp)
	}

	if method == "delete" {
		if edgemode {
			db.RemoveEdgeProperty(key, id1, id2, propertyname)
		} else {
			db.RemoveVertexProperty(key, id1, propertyname)
		}
	} else if method == "get" {
		var data []byte
		if edgemode {
			data, err = db.GetEdgeProperty(key, id1, id2, propertyname)
		} else {
			data, err = db.GetVertexProperty(key, id1, propertyname)
		}
		if err != nil {
			return err
		}
		uncompress := true
		value, _, e := dvid.DeserializeData(data, uncompress)
		if e != nil {
			err = fmt.Errorf("Unable to deserialize data for property '%s': %s\n", propertyname, e.Error())
			return err
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = w.Write(value)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
	} else if method == "post" {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		serialization, err := dvid.SerializeData(data, d.Compression, d.Checksum)
		if err != nil {
			return fmt.Errorf("Unable to serialize data: %s\n", err.Error())
		}
		if edgemode {
			err = db.SetEdgeProperty(key, id1, id2, propertyname, serialization)
		} else {
			err = db.SetVertexProperty(key, id1, propertyname, serialization)
		}
	}

	return err
}

func (d *Data) ExtractGraph(r *http.Request) (*LabelGraph, error) {
	labelgraph := new(LabelGraph)
	if r.Body == nil {
		return labelgraph, nil
	}

	// read json
	decoder := json.NewDecoder(r.Body)
	var json_data map[string]interface{}
	err := decoder.Decode(&json_data)

	if err != nil {
		return labelgraph, err
	}

	// check schema
	var schema_data interface{}
	json.Unmarshal([]byte(graphSchema), &schema_data)

	schema, err := gojsonschema.NewJsonSchemaDocument(schema_data)
	validationResult := schema.Validate(json_data)
	if !validationResult.Valid() {
		err = fmt.Errorf("JSON did not pass validation")
		return labelgraph, err
	}
	err = decoder.Decode(labelgraph)

	return labelgraph, err
}

// --- DataService interface ---

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return nil
}

// DoHTTP handles all incoming HTTP requests for this data.
func (d *Data) DoHTTP(uuid dvid.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")

	method := strings.ToLower(r.Method)
	var comment string

	if len(parts) < 4 {
		err := fmt.Errorf("No resource specified in URI")
		server.BadRequest(w, r, err.Error())
		return err
	}
	if method == "put" {
		err := fmt.Errorf("PUT requests not supported")
		server.BadRequest(w, r, err.Error())
		return err
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil
	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil
	case "subgraph":
		labelgraph, err := d.ExtractGraph(r)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		err = d.handleSubgraph(uuid, w, labelgraph, method)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
	case "merge":
		labelgraph, err := d.ExtractGraph(r)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		err = d.handleMerge(uuid, w, labelgraph, method)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
	case "property":
		err := d.handleProperty(uuid, w, r, parts[4:], method)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
	case "undomerge":
		err := fmt.Errorf("undomerge not yet implemented")
		server.BadRequest(w, r, err.Error())
		return err
	default:
		err := fmt.Errorf("%s not found", parts[3])
		server.BadRequest(w, r, err.Error())
		return err
	}

	dvid.ElapsedTime(dvid.Debug, startTime, comment, "success")
	return nil
}
