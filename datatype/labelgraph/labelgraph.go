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
        "Id": { "type": "integer", "description": "64 bit ID for vertex" },
        "Weight": { "type": "number", "description": "Weight/size of vertex" }
      },
      "required": ["Id"]
    },
    "edge": {
      "description": "Describes an edge in a graph",
      "type": "object",
      "properties": {
        "Id1": { "type": "integer", "description": "64 bit ID for vertex1" },
        "Id2": { "type": "integer", "description": "64 bit ID for vertex2" },
        "Weight": { "type": "number", "description": "Weight/size of edge" }
      },
      "required": ["Id1", "Id2"]
    }
  },
  "properties": {
    "Vertices": { 
      "description": "array of vertices",
      "type": "array",
      "items": {"$ref": "#/definitions/vertex"},
      "uniqueItems": true
    },
    "Edges": { 
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
	Id     storage.VertexID
	Weight float64
}

type labelEdge struct {
	Id1    storage.VertexID
	Id2    storage.VertexID
	Weight float64
}

type LabelGraph struct {
	Vertices []labelVertex
	Edges    []labelEdge
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
	return key, db, err
}

func (d *Data) handleSubgraph(uuid dvid.UUID, w http.ResponseWriter, labelgraph *LabelGraph, method string) error {
	key, db, err := d.getGraphDB(uuid)
	if err != nil {
		return err
	}
	labelgraph2 := new(LabelGraph)

	// ?! do not grab edges that connect to outside vertices
	if method == "get" {
		var vertices []storage.GraphVertex
		var edges []storage.GraphEdge
		if len(labelgraph.Vertices) > 0 {
			for _, vertex := range labelgraph.Vertices {
				storedvert, err := db.GetVertex(key, vertex.Id)
				if err != nil {
					return fmt.Errorf("Failed to retrieve vertix %d: %s\n", vertex.Id, err.Error())
				}
				vertices = append(vertices, storedvert)
				for _, vert2 := range storedvert.Vertices {
					if storedvert.Id < vert2 {
						edge, err := db.GetEdge(key, storedvert.Id, vert2)
						if err != nil {
							return fmt.Errorf("Failed to retrieve edge %d-%d: %s\n", storedvert.Id, vert2, err.Error())
						}
						edges = append(edges, edge)
					}
				}
			}
		} else {
			vertices, err = db.GetVertices(key)
			if err != nil {
				return fmt.Errorf("Failed to retrieve vertices: %s\n", err.Error())
			}
			edges, err = db.GetEdges(key)
			if err != nil {
				return fmt.Errorf("Failed to retrieve edges: %s\n", err.Error())
			}
		}
		for _, vertex := range vertices {
			labelgraph2.Vertices = append(labelgraph2.Vertices, labelVertex{vertex.Id, vertex.Weight})
		}
		for _, edge := range edges {
			labelgraph2.Edges = append(labelgraph2.Edges, labelEdge{edge.Vertexpair.Vertex1, edge.Vertexpair.Vertex2, edge.Weight})
		}
		m, err := json.Marshal(labelgraph2)
		if err != nil {
			return fmt.Errorf("Could not serialize graph")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(m))
	} else if method == "post" {
		for _, vertex := range labelgraph.Vertices {
			err := db.AddVertex(key, vertex.Id, vertex.Weight)
			if err != nil {
				return fmt.Errorf("Failed to add vertex: %s\n", err.Error())
			}
		}
		for _, edge := range labelgraph.Edges {
			err := db.AddEdge(key, edge.Id1, edge.Id2, edge.Weight)
			if err != nil {
				return fmt.Errorf("Failed to add edge: %s\n", err.Error())
			}
		}
	} else if method == "delete" {
		if len(labelgraph.Vertices) > 0 || len(labelgraph.Edges) > 0 {
			for _, vertex := range labelgraph.Vertices {
				db.RemoveVertex(key, vertex.Id)
			}
			for _, edge := range labelgraph.Edges {
				db.RemoveEdge(key, edge.Id1, edge.Id2)
			}
		} else {
			err = db.RemoveGraph(key)
			if err != nil {
				return fmt.Errorf("Failed to remove graph: %s\n", err.Error())
			}
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

	numverts := len(labelgraph.Vertices)
	if numverts < 2 {
		return fmt.Errorf("Must specify at least two vertices for merging")
	}

	overlapweights := make(map[storage.VertexID]float64)
	vertweight := float64(0)
	var keepvertex storage.GraphVertex
	allverts := make(map[storage.VertexID]struct{})
	keepverts := make(map[storage.VertexID]struct{})

	// accumulate weights, find common edges
	for i, vertex := range labelgraph.Vertices {
		vert, err := db.GetVertex(key, vertex.Id)
		if err != nil {
			return fmt.Errorf("Failed to retrieve vertex %d: %s\n", vertex.Id, err.Error())
		}
		allverts[vert.Id] = struct{}{}
		vertweight += vert.Weight

		if i == (numverts - 1) {
			keepvertex = vert
		} else {
			for _, vert2 := range vert.Vertices {
				edge, err := db.GetEdge(key, vert.Id, vert2)
				if err != nil {
					return fmt.Errorf("Failed to retrieve edge %d-%d: %s\n", vertex.Id, vert2, err.Error())
				}
				overlapweights[vert2] += edge.Weight
			}
		}
	}

	// examine keep vertex (save edges so only the weights are updated)
	for _, vert2 := range keepvertex.Vertices {
		edge, err := db.GetEdge(key, keepvertex.Id, vert2)
		if err != nil {
			return fmt.Errorf("Failed to retrieve edge %d-%d: %s\n", keepvertex.Id, vert2, err.Error())
		}
		overlapweights[vert2] += edge.Weight
		keepverts[vert2] = struct{}{}
	}

	// use specified weights even if marked as 0
	for _, edge := range labelgraph.Edges {
		id := edge.Id1
		baseid := edge.Id2
		if keepvertex.Id == edge.Id1 {
			id = edge.Id2
			baseid = edge.Id1
		}
		if baseid == keepvertex.Id {
			if _, ok := overlapweights[id]; ok {
				overlapweights[id] = edge.Weight
			}
		}
	}

	// if user specifies an edge weight other than 0 (?! -- somehow allow in future)
	// use that weight
	if labelgraph.Vertices[numverts-1].Weight != 0 {
		vertweight = labelgraph.Vertices[numverts-1].Weight
	}

	for id2, newweight := range overlapweights {
		if _, ok := allverts[id2]; !ok {
			// only examine edges where the node is not internal
			if _, ok := keepverts[id2]; !ok {
				// if not in keepverts create new edge
				err := db.AddEdge(key, keepvertex.Id, id2, newweight)
				if err != nil {
					return fmt.Errorf("Failed to create edge %d-%d: %s\n", keepvertex.Id, id2, err.Error())
				}
			} else {
				// else just update weight
				err := db.SetEdgeWeight(key, keepvertex.Id, id2, newweight)
				if err != nil {
					return fmt.Errorf("Failed to update weight on edge %d-%d: %s\n", keepvertex.Id, id2, err.Error())
				}
			}
		}
	}
	// update vertex weight
	err = db.SetVertexWeight(key, labelgraph.Vertices[numverts-1].Id, vertweight)
	if err != nil {
		return fmt.Errorf("Failed to update weight on vertex %d: %s\n", keepvertex.Id, err.Error())
	}

	// remove old vertices which will remove the old edges
	for i, vertex := range labelgraph.Vertices {
		if i == (numverts - 1) {
			break
		}
		err := db.RemoveVertex(key, vertex.Id)
		if err != nil {
			return fmt.Errorf("Failed to remove vertex %d: %s\n", vertex.Id, err.Error())
		}
	}

	return nil
}

func (d *Data) handleProperty(uuid dvid.UUID, w http.ResponseWriter, r *http.Request, path []string, method string) error {
	key, db, err := d.getGraphDB(uuid)
	if err != nil {
		return err
	}

	edgemode := false
	var propertyname string
	if len(path) == 3 {
		edgemode = true
		propertyname = path[2]
	} else if len(path) != 2 {
		return fmt.Errorf("Incorrect number of parameters specified for handling properties")
	} else {
		propertyname = path[1]
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
			if err != nil {
				return fmt.Errorf("Failed to remove edge property %d-%d %s: %s\n", id1, id2, propertyname, err.Error())
			}
		} else {
			db.RemoveVertexProperty(key, id1, propertyname)
			if err != nil {
				return fmt.Errorf("Failed to remove vertex property %d %s: %s\n", id1, propertyname, err.Error())
			}
		}
	} else if method == "get" {
		var data []byte
		if edgemode {
			data, err = db.GetEdgeProperty(key, id1, id2, propertyname)
		} else {
			data, err = db.GetVertexProperty(key, id1, propertyname)
		}
		if err != nil {
			return fmt.Errorf("Failed to get property %s: %s\n", propertyname, err.Error())
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
			return err
		}
	} else if method == "post" {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
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
		if err != nil {
			return fmt.Errorf("Failed to add property %s: %s\n", propertyname, err.Error())
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
	//decoder := json.NewDecoder(r.Body)
	//err := decoder.Decode(&json_data)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return labelgraph, err
	}
	if len(data) == 0 {
		return labelgraph, nil
	}

	err = json.Unmarshal(data, labelgraph)
	var json_data map[string]interface{}
	err = json.Unmarshal(data, &json_data)

	if err != nil {
		return labelgraph, err
	}

	// check schema
	var schema_data interface{}
	json.Unmarshal([]byte(graphSchema), &schema_data)

	schema, err := gojsonschema.NewJsonSchemaDocument(schema_data)
	if err != nil {
		err = fmt.Errorf("JSON schema did not build")
		return labelgraph, err
	}

	validationResult := schema.Validate(json_data)
	if !validationResult.Valid() {
		err = fmt.Errorf("JSON did not pass validation")
		return labelgraph, err
	}

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
		return err
	}
	if method == "put" {
		err := fmt.Errorf("PUT requests not supported")
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
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil
	case "subgraph":
		labelgraph, err := d.ExtractGraph(r)
		if err != nil {
			return err
		}
		err = d.handleSubgraph(uuid, w, labelgraph, method)
		if err != nil {
			return err
		}
		return nil
	case "merge":
		labelgraph, err := d.ExtractGraph(r)
		if err != nil {
			return err
		}
		err = d.handleMerge(uuid, w, labelgraph, method)
		if err != nil {
			return err
		}
		return nil
	case "property":
		err := d.handleProperty(uuid, w, r, parts[4:], method)
		if err != nil {
			return err
		}
		return nil
	case "undomerge":
		err := fmt.Errorf("undomerge not yet implemented")
		return err
	default:
		err := fmt.Errorf("%s not found", parts[3])
		return err
	}

	dvid.ElapsedTime(dvid.Debug, startTime, comment, "success")
	return nil
}
