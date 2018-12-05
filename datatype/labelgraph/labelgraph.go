/*
	Package labelgraph implements DVID support for data using the underlying graph storage engine.
*/
package labelgraph

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/gojsonschema"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelgraph"
	TypeName = "labelgraph"
)

const graphSchema = `
{ "$schema": "http://json-schema.org/schema#",
  "title": "Representation for a graph.  List of vertices with weights and their edges.  The weight terms are not mandatory",
  "type": "object",
  "definitions": {
    "transaction": {
      "description": "Transaction for locking vertex",
      "type": "object",
      "properties": {
        "Id": { "type": "number", "description": "64 bit ID for vertex" },
        "Trans": {"type": "number", "description": "64 bit transaction number" }
      },
      "required": ["Id", "Trans"]
    },
    "vertex": {
      "description": "Describes a vertex in a graph",
      "type": "object",
      "properties": {
        "Id": { "type": "number", "description": "64 bit ID for vertex >0" },
        "Weight": { "type": "number", "description": "Weight/size of vertex" }
      },
      "required": ["Id"]
    },
    "edge": {
      "description": "Describes an edge in a graph",
      "type": "object",
      "properties": {
        "Id1": { "type": "number", "description": "64 bit ID for vertex1 >0" },
        "Id2": { "type": "number", "description": "64 bit ID for vertex2 >0" },
        "Weight": { "type": "number", "description": "Weight/size of edge" }
      },
      "required": ["Id1", "Id2"]
    }
  },
  "properties": {
    "Transactions": {
        "description": "array of transactions",
        "type": ["array", "null"],
        "items": {"$ref": "#/definitions/transaction"},
        "uniqueItems": true
    },
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
  },
  "required": ["Vertices", "Edges"]
}
`

const helpMessage = `
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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET  <api URL>/node/<UUID>/<data name>/subgraph
POST  <api URL>/node/<UUID>/<data name>/subgraph
DELETE  <api URL>/node/<UUID>/<data name>/subgraph

    Performs graph-wide applications.  Calling this will set a data-wide lock.  If another subgraph
    call is performed during this operation, an error will be returned.  Users should try making a
    request after a few hundred milliseconds.  For now, there will be only one DVID client handling
    requests and it can probably handle a few hundred wait requests per second without
    many performance issues.  GET (retrieve subgraph),
    POST (add subgraph -- does not change existing graph connections), DELETE (delete whole
    graph or subgraph indicated by a list of nodes or list of edges).  POSTs or DELETEs using
    this URI will erase all merge history.

    Example: 

    GET <api URL>/node/3f8c/stuff/subgraph

    Returns the graph associated with the data "stuff" in version
    node 3f8c.  An optional JSON can be specified listing the vertices to be included.

    The "Content-type" of the HTTP response are
    "application/json" as a node list and edge list.  Vertex elements contain a "id"
    and "weight" (float).  Edge elements contain "id1", "id2", and "weight" (float).
    When deleting or asking for a subgraph, specifying the vertex list will delete or retrieve
    the edges from these vertices.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.

    Query-string Options:

    unsafe        Disable check of incoming JSON file (since schema verification is slow currently).
                  Default false.
    
POST  <api URL>/node/<UUID>/<data name>/merge/[nohistory]

    Merge a list of vertices as specified by a vertex array called "vertices".
    The last vertex is the vertex ID that will be used.  If nohistory is specified,
    the history of this transaction is not saved.  Edge and Vertex weights will be summed.
    If different weights are desired, all edge and vertex weights should be specified as done
    when posting a subgraph.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.


POST  <api URL>/node/<UUID>/<data name>/undomerge

    Undoes last merge.  An error message is returned if no UNDO occurs.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.


GET  <api URL>/node/<UUID>/<data name>/neighbors/<vertex>

    Retrieves the vertices/edges that are connected to the given vertex.

    The "Content-type" of the HTTP response are
    "application/json" as a node list and edge list.  Vertex elements contain a "id"
    and "weight" (float).  Edge elements contain "id1", "id2", and "weight" (float).

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    vertex        ID of vertex


POST  <api URL>/node/<UUID>/<data name>/weight

    Updates the weight associated with the provided vertices and edges.  Requests
    should be made in JSON following the graph schema.  The weights provided represent
    the increment that should be applied to the weight for a vertex or edge.  No more than
    1000 vertices should be associated with these edges and vertices being updated.  DVID
    guarantees the atomicity of this transaction by locking the vertices.  If the vertex,
    doesn't exist it will be created.  But an edge cannot be created unless one of its constituent
    vertices has been created (or is specified in this update call)
    
    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.


GET  <api URL>/node/<UUID>/<data name>/propertytransaction/<edges|vertices>/<property>
POST (really PUT)  <api URL>/node/<UUID>/<data name>/propertytransaction/<edges|vertices>/<property>
    
    Retrieve or set the property given by <property> for a set of vertices or edges.  Both GET and POST
    requests must send binary data encoding the vertices that will need to be locked for
    this transaction.  POST transactions should then list all the vertices and edges with the data
    to be posted.  Only 1000 vertices can be locked for a given transaction.  GET requests to
    non-existent properties will return a transaction id for the relevant vertices and empty data
    for the vertex/edge.
    
    The "Content-type" of the HTTP response and the request are
    "application/octet-stream" for arbitrary binary data. 

    REQUEST BINARY FORMAT (all numbers are 8 byte unsigned numbers)

    <NUM VERTICES TO LOCK>
    array: <VERTEX NUMBER><TRANSACTION ID|set to 0 if GET>
    <NUM PROPERTIES TO GET/PUT>
    array: <VERTEX ID1><VERTEX ID2|if edge><DATA SIZE|if posting><DATA CHUNK|if posting>

    RESPONSE BINARY FORMAT (all numbers are 8 byte unsigned numbers)

    <NUM VERTICES SUCCESSFULLY LOCKED>
    array: <VERTEX NUMBER><TRANSACTION ID>
    <NUM VERTICES UNSUCCESSFULLY LOCKED>
    array: <VERTEX NUMBER>
    <NUM PROPERTIES TO GET>
    array: <VERTEX ID1><VERTEX ID2|if edge><DATA SIZE><DATA CHUNK>
    
    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    property           Name of the property


GET  <api URL>/node/<UUID>/<data name>/property/<vertex1>/<key>
POST (really PUT)  <api URL>/node/<UUID>/<data name>/property/<vertex>/<key>
DELETE  <api URL>/node/<UUID>/<data name>/property/<vertex>/<key>
    
    Retrive or set a vertex property.

    The "Content-type" of the HTTP response and the request are
    "application/octet-stream" for arbitrary binary data.
    
    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    vertex1       ID of vertex1
    vertex2       ID of vertex2
    key           Name of the property

    
TODO:

* Bulk loading/retrieving (compression of JSON files)
* Allow concurrent bulk reads
* Handle tranactions across multiple DVID clients
* Consider transaction/lock handling at a lower-level (Neo4j solutions?); atomicity of commands?

* Implement transaction history as keyvalue array and support undo command (allow users to flush transaction history and perform mergers without transactions)
* Implement better atomicity at storage level to prevent weirdness (all writes should be in a batch -- most currently are)
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// labelVertex stores a subset of information contained in GraphVertex for interfacing with client
type labelVertex struct {
	Id     dvid.VertexID
	Weight float64
}

// labelEdge stores a subset of information contained in GraphVertex for interfacing with client
type labelEdge struct {
	Id1    dvid.VertexID
	Id2    dvid.VertexID
	Weight float64
}

// LabelGraph encodes data exchanged with a client
type LabelGraph struct {
	Transactions []transactionItem // transaction ids associated with vertices
	Vertices     []labelVertex
	Edges        []labelEdge
}

// --- structures for handling concurrency by associating transaction ids for given vertices ---

// transacitonItem defines an transaction id associated with a vertex (used for concurrent access)
type transactionItem struct {
	Id    dvid.VertexID
	Trans uint64
}

// transactionLog holds reference to the last transaction performed on a given vertex
// (only for relevant API)
type transactionLog struct {
	transaction_queue    map[dvid.VertexID]uint64 // map of vertex to transaction id
	requestor_channels   map[uint64]chan struct{} // keep track of current requesters
	current_id           uint64                   // current transaction id -- always increases
	mutex                sync.Mutex
	current_requestor_id uint64 // current id assigned to new transaction requests
}

// transactionGroup defines a set of vertex transactions
type transactionGroup struct {
	log                  *transactionLog
	current_requestor_id uint64                   // unique id of requestor
	trans_channel        <-chan struct{}          // ability to wait for changes to transactions
	locked_ids           map[dvid.VertexID]uint64 // vertex ids that this group is locking
	lockedold_ids        []dvid.VertexID          // vertex ids that this group could not lock
	outdated_ids         []dvid.VertexID          // vertex ids which were not given the correct tranaction ids (non-readonly modes)
}

// NewTransactionGroup returns a pointer to a new transaction group
func NewTransactionGroup(log *transactionLog, current_requestor_id uint64) *transactionGroup {
	return &transactionGroup{
		log:                  log,
		current_requestor_id: current_requestor_id,
		trans_channel:        log.requestor_channels[current_requestor_id],
		locked_ids:           make(map[dvid.VertexID]uint64),
	}
}

// waitForChange waits on channels for another tranaction group to close (look for changes)
func (t *transactionGroup) waitForChange() {
	_ = <-t.trans_channel
}

// closeTransaction updates all ids for locked transactions
func (t *transactionGroup) closeTransaction() {
	t.log.removeTransactionGroup(t.locked_ids, t.current_requestor_id)
}

// addTransaction adds a vertex to the locked list (only called internally)
func (t *transactionGroup) addTransaction(vertex dvid.VertexID, trans uint64) {
	t.locked_ids[vertex] = trans
}

// exportTransaction writes out a list of vertices and their new transction id (0 if the vertex
// was invalid -- busy or requestor provided the wrong transaction id for non-readonly)
func (t *transactionGroup) exportTransactions() []transactionItem {
	var vertices []transactionItem

	for vertex, id := range t.locked_ids {
		vertices = append(vertices, transactionItem{vertex, id})
	}
	for _, vertex := range t.lockedold_ids {
		vertices = append(vertices, transactionItem{vertex, 0})
	}
	for _, vertex := range t.outdated_ids {
		vertices = append(vertices, transactionItem{vertex, 0})
	}

	return vertices
}

// exportTransaciton writes out vertices in binary (number of locked vertices, list of vertex and id,
// number of ids without locked vertex, list unlocked vertices)
func (t *transactionGroup) exportTransactionsBinary() []byte {
	start := 0
	total_size := 16 + len(t.locked_ids)*8*2 + len(t.lockedold_ids)*8 + len(t.outdated_ids)*8
	buf := make([]byte, total_size, total_size)
	binary.LittleEndian.PutUint64(buf[start:], uint64(len(t.locked_ids)))
	start += 8

	for vertex, id := range t.locked_ids {
		binary.LittleEndian.PutUint64(buf[start:], uint64(vertex))
		start += 8
		binary.LittleEndian.PutUint64(buf[start:], id)
		start += 8
	}

	binary.LittleEndian.PutUint64(buf[start:], uint64(len(t.lockedold_ids)+len(t.outdated_ids)))
	start += 8
	for _, vertex := range t.lockedold_ids {
		binary.LittleEndian.PutUint64(buf[start:], uint64(vertex))
		start += 8
	}
	for _, vertex := range t.outdated_ids {
		binary.LittleEndian.PutUint64(buf[start:], uint64(vertex))
		start += 8
	}

	return buf
}

// NewTransactionLog creats a pointer to transaction log, initializing relevant maps
func NewTransactionLog() *transactionLog {
	return &transactionLog{
		transaction_queue:  make(map[dvid.VertexID]uint64),
		requestor_channels: make(map[uint64]chan struct{}),
		current_id:         1,
	}
}

// removeTransactionGroup sets the new transaction ids for locked vertices, removed the requestor
// channel, and updates all other requestor channels
func (t *transactionLog) removeTransactionGroup(locked_ids map[dvid.VertexID]uint64, trans_id uint64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for vertex, id := range locked_ids {
		t.transaction_queue[vertex] = id
	}
	delete(t.requestor_channels, trans_id)

	for _, channel := range t.requestor_channels {
		select {
		case channel <- struct{}{}:
			// signal this channel
		default:
			// otherwise ignore
		}
	}
}

// createTransactionGroup takes a list of vertices (with up-to-date transaction ids if not
// read only) and locks those vertices.  Vertices that have an out-of-date transaction are not
// locked (non-readonly).  Vertices that are currently locked by another requstor are also not locked.
func (t *transactionLog) createTransactionGroup(vertices []transactionItem, readonly bool) (*transactionGroup, error) {
	if len(vertices) > 1000 {
		return nil, fmt.Errorf("Do not support transaction with over 1000 ids")
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.requestor_channels[t.current_requestor_id] = make(chan struct{}, 1)
	transaction_group := NewTransactionGroup(t, t.current_requestor_id)
	t.current_requestor_id += 1

	for _, trans_record := range vertices {
		current_transaction, ok := t.transaction_queue[trans_record.Id]

		// readonly -- just grab current ID or create a new one if never examiend
		if !ok && readonly {
			current_transaction = t.current_id
			t.current_id += 1
		} else if !ok || (!readonly && (current_transaction != trans_record.Trans)) {
			transaction_group.outdated_ids = append(transaction_group.outdated_ids, trans_record.Id)
			continue
		} else if current_transaction == 0 {
			// 0 denotes a locked transaction
			transaction_group.lockedold_ids = append(transaction_group.lockedold_ids, trans_record.Id)
			continue
		} else if !readonly {
			// make a new transaction number if this is not readonly
			current_transaction = t.current_id
			t.current_id += 1
		}

		t.transaction_queue[trans_record.Id] = 0
		transaction_group.addTransaction(trans_record.Id, current_transaction)
	}
	return transaction_group, nil
}

// createTransactionGroupBinary reads through binary representation of vertex / transaction id
// pairs and calls creatTransactionGroup
func (t *transactionLog) createTransactionGroupBinary(data []byte, readonly bool) (*transactionGroup, int, error) {
	start := 0
	numtrans := binary.LittleEndian.Uint64(data[start:])
	start += 8

	var vertices []transactionItem
	for i := uint64(0); i < numtrans; i++ {
		vertex := binary.LittleEndian.Uint64(data[start:])
		start += 8
		trans := binary.LittleEndian.Uint64(data[start:])
		start += 8

		vertices = append(vertices, transactionItem{dvid.VertexID(vertex), trans})
	}

	transaction_group, err := t.createTransactionGroup(vertices, readonly)
	return transaction_group, start, err
}

// Type embeds the datastore's Type to create a unique type for labelgraph functions.
type Type struct {
	datastore.Type
}

// NewDatatype returns a pointer to a new keyvalue Datatype with default values set.
func NewType() *Type {
	dtype := new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			BulkIniter: false,
			BulkWriter: false,
			Batcher:    true,
			GraphDB:    true,
		},
	}
	return dtype
}

// --- TypeService interface ---

// NewDataService returns a pointer to new keyvalue data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	return &Data{Data: basedata}, nil
}

// Help returns help mesage for datatype
func (dtype *Type) Help() string {
	return fmt.Sprintf(helpMessage)
}

// Data embeds the datastore's Data and extends it with transaction properties
// (default values are okay after deserializing).
type Data struct {
	*datastore.Data
	transaction_log *transactionLog
	busy            bool
	datawide_mutex  sync.Mutex
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return true
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base *datastore.Data
	}{
		d.Data,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Help returns help mesage for datatype
func (d *Data) Help() string {
	return fmt.Sprintf(helpMessage)
}

// initializeLog ensures that the transaction_log has been created
func (d *Data) initializeLog() {
	d.datawide_mutex.Lock()
	defer d.datawide_mutex.Unlock()
	if d.transaction_log == nil {
		d.transaction_log = NewTransactionLog()
	}
}

// setBusy checks if data is busy with a large transaction (currently small transactions do not
// set some busy signal to prevent a large transaction from occurring -- applications should avoid
// collisions with bulk actions)
func (d *Data) setBusy() bool {
	d.datawide_mutex.Lock()
	defer d.datawide_mutex.Unlock()
	if d.busy {
		return false
	}
	d.busy = true
	return true
}

// setNotBusy checks if a the data instance is busy with some other request
func (d *Data) setNotBusy() {
	d.datawide_mutex.Lock()
	defer d.datawide_mutex.Unlock()
	d.busy = false
}

// handleSubgraph loads, retrieves, or deletes a subgraph (more description in REST interface)
func (d *Data) handleSubgraphBulk(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, labelgraph *LabelGraph, method string) error {
	var err error
	if !d.setBusy() {
		return fmt.Errorf("Server busy with bulk transaction")
	}
	defer d.setNotBusy()

	// initial new graph
	labelgraph2 := new(LabelGraph)
	labelgraph2.Transactions = make([]transactionItem, 0)
	labelgraph2.Vertices = make([]labelVertex, 0)
	labelgraph2.Edges = make([]labelEdge, 0)

	// ?! do not grab edges that connect to outside vertices
	if method == "get" {
		var vertices []dvid.GraphVertex
		var edges []dvid.GraphEdge
		if len(labelgraph.Vertices) > 0 {
			used_vertices := make(map[dvid.VertexID]struct{})
			for _, vertex := range labelgraph.Vertices {
				used_vertices[vertex.Id] = struct{}{}
				storedvert, err := db.GetVertex(ctx, vertex.Id)
				if err != nil {
					return fmt.Errorf("Failed to retrieve vertix %d: %v\n", vertex.Id, err)
				}
				vertices = append(vertices, storedvert)
				for _, vert2 := range storedvert.Vertices {
					if _, ok := used_vertices[vert2]; !ok {
						edge, err := db.GetEdge(ctx, storedvert.Id, vert2)
						if err != nil {
							return fmt.Errorf("Failed to retrieve edge %d-%d: %v\n", storedvert.Id, vert2, err)
						}
						edges = append(edges, edge)
					}
				}
			}
		} else {
			// if no set of vertices are supplied, just grab the whole graph
			vertices, err = db.GetVertices(ctx)
			if err != nil {
				return fmt.Errorf("Failed to retrieve vertices: %v\n", err)
			}
			edges, err = db.GetEdges(ctx)
			if err != nil {
				return fmt.Errorf("Failed to retrieve edges: %v\n", err)
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
		// add list of vertices and edges from supplied JSON -- overwrite existing values
		for _, vertex := range labelgraph.Vertices {
			err := db.AddVertex(ctx, vertex.Id, vertex.Weight)
			if err != nil {
				return fmt.Errorf("Failed to add vertex: %v\n", err)
			}
		}
		for _, edge := range labelgraph.Edges {
			err := db.AddEdge(ctx, edge.Id1, edge.Id2, edge.Weight)
			if err != nil {
				return fmt.Errorf("Failed to add edge: %v\n", err)
			}
		}
	} else if method == "delete" {
		// delete the vertices supplied and all of their edges or delete the whole graph
		if len(labelgraph.Vertices) > 0 || len(labelgraph.Edges) > 0 {
			for _, vertex := range labelgraph.Vertices {
				db.RemoveVertex(ctx, vertex.Id)
			}
			for _, edge := range labelgraph.Edges {
				db.RemoveEdge(ctx, edge.Id1, edge.Id2)
			}
		} else {
			err = db.RemoveGraph(ctx)
			if err != nil {
				return fmt.Errorf("Failed to remove graph: %v\n", err)
			}
		}
	} else {
		err = fmt.Errorf("Does not support PUT")
	}

	return err
}

func (d *Data) extractOpenVertices(labelgraph *LabelGraph) []transactionItem {
	var open_vertices []transactionItem
	check_vertices := make(map[dvid.VertexID]struct{})
	for _, vertex := range labelgraph.Vertices {
		if vertex.Id != 0 {
			if _, ok2 := check_vertices[vertex.Id]; !ok2 {
				open_vertices = append(open_vertices, transactionItem{vertex.Id, 0})
				check_vertices[vertex.Id] = struct{}{}
			}
		}
	}
	for _, edge := range labelgraph.Edges {
		if edge.Id1 != 0 {
			if _, ok2 := check_vertices[edge.Id1]; !ok2 {
				open_vertices = append(open_vertices, transactionItem{edge.Id1, 0})
				check_vertices[edge.Id1] = struct{}{}
			}
			if _, ok2 := check_vertices[edge.Id2]; !ok2 {
				open_vertices = append(open_vertices, transactionItem{edge.Id2, 0})
				check_vertices[edge.Id2] = struct{}{}
			}
		}
	}
	return open_vertices
}

// handleWeightUpdate POST vertex/edge weight increment/decrement.  POSTing to an uncreated
// node or edge will create the node or edge (default 0 weight).  Limit of 1000 vertices and 1000 edges.
func (d *Data) handleWeightUpdate(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, labelgraph *LabelGraph) error {

	// collect all vertices that need to be locked and wrap in transaction ("read only")
	open_vertices := d.extractOpenVertices(labelgraph)
	transaction_group, err := d.transaction_log.createTransactionGroup(open_vertices, true)

	if err != nil {
		transaction_group.closeTransaction()
		return fmt.Errorf("Could not create transaction group")
	}

	leftover := true

	// iterate until leftovers are empty
	for leftover {
		if len(transaction_group.lockedold_ids) == 0 {
			leftover = false
		}

		for i, vertex := range labelgraph.Vertices {
			// if it was already examined, continue
			if vertex.Id == 0 {
				continue
			}
			// if it the vertex was not locked, continue
			if _, ok := transaction_group.locked_ids[vertex.Id]; !ok {
				continue
			}

			// retrieve vertex, update or create depending on error status
			storedvert, err := db.GetVertex(ctx, vertex.Id)
			if err != nil {
				err = db.AddVertex(ctx, vertex.Id, vertex.Weight)
				if err != nil {
					transaction_group.closeTransaction()
					return fmt.Errorf("Failed to add vertex: %v\n", err)
				}
			} else {
				// increment/decrement weight
				err = db.SetVertexWeight(ctx, vertex.Id, storedvert.Weight+vertex.Weight)
				if err != nil {
					transaction_group.closeTransaction()
					return fmt.Errorf("Failed to add vertex: %v\n", err)
				}
			}

			// do not revisit
			labelgraph.Vertices[i].Id = 0
		}
		for i, edge := range labelgraph.Edges {
			// if it was already examined, continue
			if edge.Id1 == 0 {
				continue
			}

			// if it the edge was not locked, continue
			if _, ok := transaction_group.locked_ids[edge.Id1]; !ok {
				continue
			}
			if _, ok := transaction_group.locked_ids[edge.Id2]; !ok {
				continue
			}

			// retrieve edge, update or create depending on error status
			storededge, err := db.GetEdge(ctx, edge.Id1, edge.Id2)
			if err != nil {
				err = db.AddEdge(ctx, edge.Id1, edge.Id2, edge.Weight)
				if err != nil {
					transaction_group.closeTransaction()
					return fmt.Errorf("Failed to add edge: %v\n", err)
				}
			} else {
				// increment/decrement weight
				err = db.SetEdgeWeight(ctx, edge.Id1, edge.Id2, storededge.Weight+edge.Weight)
				if err != nil {
					transaction_group.closeTransaction()
					return fmt.Errorf("Failed to update edge: %v\n", err)
				}
			}

			// do not revisit
			labelgraph.Edges[i].Id1 = 0
		}

		if leftover {
			// wait on update to channel if there are leftovers
			transaction_group.waitForChange()

			// close transaction, create new transaction from leftovers in labelgraph
			transaction_group.closeTransaction()
			open_vertices = d.extractOpenVertices(labelgraph)
			transaction_group, _ = d.transaction_log.createTransactionGroup(open_vertices, true)
		}
	}

	transaction_group.closeTransaction()
	return nil
}

// handleMerge merges a list of vertices onto the final vertex in the Vertices list
func (d *Data) handleMerge(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, labelgraph *LabelGraph) error {

	numverts := len(labelgraph.Vertices)
	if numverts < 2 {
		return fmt.Errorf("Must specify at least two vertices for merging")
	}

	overlapweights := make(map[dvid.VertexID]float64)
	vertweight := float64(0)
	var keepvertex dvid.GraphVertex
	allverts := make(map[dvid.VertexID]struct{})
	keepverts := make(map[dvid.VertexID]struct{})

	// accumulate weights, find common edges
	for i, vertex := range labelgraph.Vertices {
		vert, err := db.GetVertex(ctx, vertex.Id)
		if err != nil {
			return fmt.Errorf("Failed to retrieve vertex %d: %v\n", vertex.Id, err)
		}
		allverts[vert.Id] = struct{}{}
		vertweight += vert.Weight

		if i == (numverts - 1) {
			keepvertex = vert
		} else {
			for _, vert2 := range vert.Vertices {
				edge, err := db.GetEdge(ctx, vert.Id, vert2)
				if err != nil {
					return fmt.Errorf("Failed to retrieve edge %d-%d: %v\n", vertex.Id, vert2, err)
				}
				overlapweights[vert2] += edge.Weight
			}
		}
	}

	// examine keep vertex (save edges so only the weights are updated)
	for _, vert2 := range keepvertex.Vertices {
		edge, err := db.GetEdge(ctx, keepvertex.Id, vert2)
		if err != nil {
			return fmt.Errorf("Failed to retrieve edge %d-%d: %v\n", keepvertex.Id, vert2, err)
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
				err := db.AddEdge(ctx, keepvertex.Id, id2, newweight)
				if err != nil {
					return fmt.Errorf("Failed to create edge %d-%d: %v\n", keepvertex.Id, id2, err)
				}
			} else {
				// else just update weight
				err := db.SetEdgeWeight(ctx, keepvertex.Id, id2, newweight)
				if err != nil {
					return fmt.Errorf("Failed to update weight on edge %d-%d: %v\n", keepvertex.Id, id2, err)
				}
			}
		}
	}

	// update vertex weight
	err := db.SetVertexWeight(ctx, labelgraph.Vertices[numverts-1].Id, vertweight)
	if err != nil {
		return fmt.Errorf("Failed to update weight on vertex %d: %v\n", keepvertex.Id, err)
	}

	// remove old vertices which will remove the old edges
	for i, vertex := range labelgraph.Vertices {
		if i == (numverts - 1) {
			break
		}
		err := db.RemoveVertex(ctx, vertex.Id)
		if err != nil {
			return fmt.Errorf("Failed to remove vertex %d: %v\n", vertex.Id, err)
		}
	}
	return nil
}

// handleNeighbors returns the vertices and edges connected to the provided vertex
// (Should I protect this transaction like the update weight function?  It is probably
// unnecessary because any update based on retrieved information will be written to a
// property which has transactional protection)
func (d *Data) handleNeighbors(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, path []string) error {
	labelgraph := new(LabelGraph)

	temp, err := strconv.Atoi(path[0])
	if err != nil {
		return fmt.Errorf("Vertex number not provided")
	}
	id := dvid.VertexID(temp)

	storedvert, err := db.GetVertex(ctx, id)
	if err != nil {
		return fmt.Errorf("Failed to retrieve vertix %d: %v\n", id, err)
	}

	labelgraph.Vertices = append(labelgraph.Vertices, labelVertex{storedvert.Id, storedvert.Weight})

	for _, vert2 := range storedvert.Vertices {
		vertex, err := db.GetVertex(ctx, vert2)
		if err != nil {
			return fmt.Errorf("Failed to retrieve vertex %d: %v\n", vertex.Id, err)
		}
		labelgraph.Vertices = append(labelgraph.Vertices, labelVertex{vertex.Id, vertex.Weight})
		edge, err := db.GetEdge(ctx, id, vert2)
		if err != nil {
			return fmt.Errorf("Failed to retrieve edge %d-%d: %v\n", vertex.Id, vert2, err)
		}
		labelgraph.Edges = append(labelgraph.Edges, labelEdge{edge.Vertexpair.Vertex1, edge.Vertexpair.Vertex2, edge.Weight})
	}
	m, err := json.Marshal(labelgraph)
	if err != nil {
		return fmt.Errorf("Could not serialize graph")
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
	return nil
}

// handelPropertyTransaction allows gets/posts (really puts) of edge or vertex properties.
func (d *Data) handlePropertyTransaction(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, r *http.Request, path []string, method string) error {
	if len(path) < 2 {
		return fmt.Errorf("Must specify edges or vertices in URI and property name")
	}
	if method == "delete" {
		return fmt.Errorf("Transactional delete not supported")
	}

	edgemode := false
	if path[0] == "edges" {
		edgemode = true
	} else if path[0] != "vertices" {
		return fmt.Errorf("Must specify edges or vertices in URI")
	}
	propertyname := path[1]

	readonly := false
	if method == "get" {
		readonly = true
	}
	data, err := ioutil.ReadAll(r.Body)

	// only allow 1000 vertices to be locked
	transactions, start, err := d.transaction_log.createTransactionGroupBinary(data, readonly)
	defer transactions.closeTransaction()
	if err != nil {
		return fmt.Errorf("Failed to create property transaction: %v", err)
	}

	returned_data := transactions.exportTransactionsBinary()

	if method == "post" {
		// deserialize transaction (vertex or edge) -- use URI?
		num_properties := binary.LittleEndian.Uint64(data[start:])
		start += 8

		for i := uint64(0); i < num_properties; i++ {
			temp := binary.LittleEndian.Uint64(data[start:])
			id := dvid.VertexID(temp)
			var id2 dvid.VertexID
			start += 8
			if edgemode {
				temp = binary.LittleEndian.Uint64(data[start:])
				id2 = dvid.VertexID(temp)
				start += 8
			}
			data_size := binary.LittleEndian.Uint64(data[start:])
			start += 8
			data_begin := start

			start += int(data_size)
			data_end := start

			if data_begin == data_end {
				continue
			}

			// check if post is possible
			if _, ok := transactions.locked_ids[id]; !ok {
				continue
			}
			if edgemode {
				if _, ok := transactions.locked_ids[id2]; !ok {
					continue
				}
			}

			// execute post
			serialization, err := dvid.SerializeData(data[data_begin:data_end], d.Compression(), d.Checksum())
			if err != nil {
				return fmt.Errorf("Unable to serialize data: %v\n", err)
			}
			if edgemode {
				err = db.SetEdgeProperty(ctx, id, id2, propertyname, serialization)
			} else {
				err = db.SetVertexProperty(ctx, id, propertyname, serialization)
			}
			if err != nil {
				return fmt.Errorf("Failed to add property %s: %v\n", propertyname, err)
			}
		}
	} else {
		num_properties := binary.LittleEndian.Uint64(data[start:])
		start += 8
		num_properties_loc := len(returned_data)
		longbuf := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(longbuf, 0)
		returned_data = append(returned_data, longbuf...)
		num_executed_transactions := uint64(0)

		// read the vertex or edge properties desired
		for i := uint64(0); i < num_properties; i++ {
			temp := binary.LittleEndian.Uint64(data[start:])
			id := dvid.VertexID(temp)
			var id2 dvid.VertexID
			start += 8
			if edgemode {
				temp := binary.LittleEndian.Uint64(data[start:])
				id2 = dvid.VertexID(temp)
				start += 8
			}

			// check if post is possible
			if _, ok := transactions.locked_ids[id]; !ok {
				continue
			}
			if edgemode {
				if _, ok := transactions.locked_ids[id2]; !ok {
					continue
				}
			}

			// execute get command
			var dataout []byte
			if edgemode {
				dataout, err = db.GetEdgeProperty(ctx, id, id2, propertyname)
			} else {
				dataout, err = db.GetVertexProperty(ctx, id, propertyname)
			}

			// serialize return data only if there is return data and no error;
			// otherwise return just return the id and size of 0
			var data_serialized []byte
			if (err == nil) && len(dataout) > 0 {
				uncompress := true
				data_serialized, _, err = dvid.DeserializeData(dataout, uncompress)
				if err != nil {
					return fmt.Errorf("Unable to deserialize data for property '%s': %v\n", propertyname, err)
				}
			}

			// save transaction
			num_executed_transactions += 1
			binary.LittleEndian.PutUint64(longbuf, uint64(id))
			returned_data = append(returned_data, longbuf...)
			if edgemode {
				binary.LittleEndian.PutUint64(longbuf, uint64(id2))
				returned_data = append(returned_data, longbuf...)
			}
			binary.LittleEndian.PutUint64(longbuf, uint64(len(data_serialized)))
			returned_data = append(returned_data, longbuf...)
			returned_data = append(returned_data, data_serialized...)
		}

		// update the number of transactions
		binary.LittleEndian.PutUint64(returned_data[num_properties_loc:], num_executed_transactions)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(returned_data)
	return err
}

// handleProperty retrieves or deletes properties that can be added to a vertex or edge -- data posted
// or retrieved uses default compression
func (d *Data) handleProperty(ctx *datastore.VersionedCtx, db storage.GraphDB, w http.ResponseWriter, r *http.Request, path []string, method string) error {

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
	id1 := dvid.VertexID(temp)

	id2 := dvid.VertexID(0)
	if edgemode {
		temp, err := strconv.Atoi(path[1])
		if err != nil {
			return fmt.Errorf("Vertex number not provided")
		}
		id2 = dvid.VertexID(temp)
	}

	// remove a property from a vertex or edge
	if method == "delete" {
		if edgemode {
			db.RemoveEdgeProperty(ctx, id1, id2, propertyname)
			if err != nil {
				return fmt.Errorf("Failed to remove edge property %d-%d %s: %v\n", id1, id2, propertyname, err)
			}
		} else {
			db.RemoveVertexProperty(ctx, id1, propertyname)
			if err != nil {
				return fmt.Errorf("Failed to remove vertex property %d %s: %v\n", id1, propertyname, err)
			}
		}
	} else if method == "get" {
		var data []byte
		if edgemode {
			data, err = db.GetEdgeProperty(ctx, id1, id2, propertyname)
		} else {
			data, err = db.GetVertexProperty(ctx, id1, propertyname)
		}
		if err != nil {
			return fmt.Errorf("Failed to get property %s: %v\n", propertyname, err)
		}
		uncompress := true
		value, _, e := dvid.DeserializeData(data, uncompress)
		if e != nil {
			err = fmt.Errorf("Unable to deserialize data for property '%s': %v\n", propertyname, e.Error())
			return err
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = w.Write(value)
		if err != nil {
			return err
		}
	} else if method == "post" {
		// read as binary and load into propertyname
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}
		serialization, err := dvid.SerializeData(data, d.Compression(), d.Checksum())
		if err != nil {
			return fmt.Errorf("Unable to serialize data: %v\n", err)
		}
		if edgemode {
			err = db.SetEdgeProperty(ctx, id1, id2, propertyname, serialization)
		} else {
			err = db.SetVertexProperty(ctx, id1, propertyname, serialization)
		}
		if err != nil {
			return fmt.Errorf("Failed to add property %s: %v\n", propertyname, err)
		}
	}

	return err
}

// ExtractGraph takes the client's supplied JSON, verifies that it conforms to the
// schema, and loads it into the LabelGraph data structure
func (d *Data) ExtractGraph(r *http.Request, disableSchema bool) (*LabelGraph, error) {
	labelgraph := new(LabelGraph)
	if r.Body == nil {
		return labelgraph, nil
	}

	// read json
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return labelgraph, err
	}
	if len(data) == 0 {
		return labelgraph, nil
	}

	// load data labelgraph and generic string-inteface{} map
	err = json.Unmarshal(data, labelgraph)

	if err != nil {
		return labelgraph, err
	}

	if !disableSchema {
		// check schema
		var schema_data interface{}
		json.Unmarshal([]byte(graphSchema), &schema_data)

		schema, err := gojsonschema.NewJsonSchemaDocument(schema_data)
		if err != nil {
			err = fmt.Errorf("JSON schema did not build")
			return labelgraph, err
		}

		var json_data map[string]interface{}
		err = json.Unmarshal(data, &json_data)
		if err != nil {
			return labelgraph, err
		}

		validationResult := schema.Validate(json_data)
		if !validationResult.Valid() {
			err = fmt.Errorf("JSON did not pass validation")
			return labelgraph, err
		}
	}

	return labelgraph, err
}

// --- DataService interface ---

// DoRPC acts as a switchboard for RPC commands -- not supported
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
	// --- Don't time labelgraph ops because they are very small and frequent.
	// --- TODO -- Implement monitoring system that aggregates logged ops instead of
	// ----------- printing out each one.
	// timedLog := dvid.NewTimeLog()

	db, err := storage.GraphStore()
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	// make sure transaction log is created
	d.initializeLog()

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts) < 4 {
		server.BadRequest(w, r, "No resource specified in URI")
		return
	}
	method := strings.ToLower(r.Method)
	if method == "put" {
		server.BadRequest(w, r, "PUT requests not supported")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
	case "subgraph":
		// disable json schema validation (will speedup POST command)
		queryStrings := r.URL.Query()
		disableSchemaT := dvid.InstanceName(queryStrings.Get("unsafe"))
		disableSchema := false
		if len(disableSchemaT) != 0 && disableSchemaT == "true" {
			disableSchema = true
		}
		labelgraph, err := d.ExtractGraph(r, disableSchema)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		err = d.handleSubgraphBulk(ctx, db, w, labelgraph, method)
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "neighbors":
		if method != "get" {
			server.BadRequest(w, r, "Only supports GETs")
			return
		}
		err := d.handleNeighbors(ctx, db, w, parts[4:])
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "merge":
		if method != "post" {
			server.BadRequest(w, r, "Only supports POSTs")
			return
		}
		labelgraph, err := d.ExtractGraph(r, false)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		err = d.handleMerge(ctx, db, w, labelgraph)
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "weight":
		if method != "post" {
			server.BadRequest(w, r, "Only supports POSTs")
		}
		labelgraph, err := d.ExtractGraph(r, false)
		if err != nil {
			server.BadRequest(w, r, err)
		}
		err = d.handleWeightUpdate(ctx, db, w, labelgraph)
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "propertytransaction":
		err := d.handlePropertyTransaction(ctx, db, w, r, parts[4:], method)
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "property":
		err := d.handleProperty(ctx, db, w, r, parts[4:], method)
		if err != nil {
			server.BadRequest(w, r, err)
		}
	case "undomerge":
		// not supported until transaction history is supported
		server.BadRequest(w, r, "undomerge not yet implemented")
	default:
		server.BadAPIRequest(w, r, d)
	}
	//timedLog.Infof("Successful labelgraph op %s on %q", parts[3], d.DataName())
	return
}
