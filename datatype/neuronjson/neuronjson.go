/*
	Package neuronjson implements DVID support for neuron JSON annotations
*/
package neuronjson

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	io "io"
	"io/ioutil"
	"math"
	"net/http"
	reflect "reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	pb "google.golang.org/protobuf/proto"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/datatype/keyvalue"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/neuronjson"
	TypeName = "neuronjson"
)

const helpMessage = `
API for 'neuronjson' datatype (github.com/janelia-flyem/dvid/datatype/neuronjson)
=============================================================================

The neuronjson datatype is similar supports most of the keyvalue datatype methods
but extends them to include queries.  

The keys are body identifier uint64 that are represented as strings for 
backward-compatibility with clients that used to use the keyvalue datatype 
for these neuron JSON annotations. The values are assumed to be JSON data, 
and the queries are similar to how Firestore handles queries.

Note: UUIDs referenced below are strings that may either be a unique prefix of a
hexadecimal UUID string (e.g., 3FA22) or a branch leaf specification that adds
a colon (":") followed by the case-dependent branch name.  In the case of a
branch leaf specification, the unique UUID prefix just identifies the repo of
the branch, and the UUID referenced is really the leaf of the branch name.
For example, if we have a DAG with root A -> B -> C where C is the current
HEAD or leaf of the "master" (default) branch, then asking for "B:master" is
the same as asking for "C".  If we add another version so A -> B -> C -> D, then
references to "B:master" now return the data from "D".

Command-line:

$ dvid repo <UUID> new neuronjson <data name> <settings...>

	Adds newly named neuronjson data to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new neuronjson stuff

	Arguments:

	UUID           Hexadecimal string with enough characters to uniquely identify a version node.
	data name      Name of data to create, e.g., "myblobs"
	settings       Configuration settings in "key=value" format separated by spaces.

	Configuration Settings (case-insensitive keys):

	Versioned      Set to "false" or "0" if the neuronjson instance is unversioned (repo-wide).
				   An unversioned neuronjson will only use the UUIDs to look up the repo and
				   not differentiate between versions in the same repo.  Note that unlike
				   versioned data, distribution (push/pull) of unversioned data is not defined 
				   at this time.

$ dvid -stdin node <UUID> <data name> put <key> < data

	Puts stdin data into the neuronjson data instance under the given key.

$ dvid node <UUID> <dataname> import-kv <keyvalue instance name>

	Imports the data from a keyvalue instance within the same repo.

	Example:

	$ dvid repo 3f8c myNeuronJSON import-kv myOldKV

	The above imports data from the keyvalue instance "myOldKV" into the neuronjson
	instance "myNeuronJSON".
	
	------------------

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
	data name     Name of neuronjson data instance.

GET <api URL>/node/<UUID>/<data name>/tags
POST <api URL>/node/<UUID>/<data name>/tags?<options>

	GET retrieves JSON of tags for this instance.
	POST appends or replaces tags provided in POST body.  Expects JSON to be POSTed
	with the following format:

	{ "tag1": "anything you want", "tag2": "something else" }

	To delete tags, pass an empty object with query string "replace=true".

	POST Query-string Options:

	replace   Set to "true" if you want passed tags to replace and not be appended to current tags.
				Default operation is false (append).

GET  <api URL>/node/<UUID>/<data name>/<schema type>
POST <api URL>/node/<UUID>/<data name>/<schema type>
DEL  <api URL>/node/<UUID>/<data name>/<schema type> 
HEAD <api URL>/node/<UUID>/<data name>/<schema type> 

	Performs operations on metadata schema depending on the HTTP verb.  
	If the "json_schema" type is POSTed, it will be used to validate
	future writes of neuron annotations via POST /key, /keyvalues, etc.

	Example: 

	GET <api URL>/node/3f8c/neuron_annotations/json_schema

	Returns any JSON schema for validation stored for version node 3f8c.

	The "Content-type" of the HTTP response (and usually the request) are "application/json".

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of keyvalue data instance.
	schema type	  One of "json_schema" (validation), "schema" (neutu/neu3), "schema_batch" (neutu/neu3)
				
GET  <api URL>/node/<UUID>/<data name>/all[?query-options]

	Returns a list of all JSON annotations

	GET Query-string Options:

	show		If "user", shows *_user fields.
				If "time", shows *_time fields.
				If "all", shows both *_user and *_time fields.
				If unset (default), shows neither *_user or *_time fields.
			
GET  <api URL>/node/<UUID>/<data name>/keys

	Returns all keys for this data instance in JSON format:

	[key1, key2, ...]

GET  <api URL>/node/<UUID>/<data name>/fields

	Returns all field names in annotations for the most recent version:

	["field1", "field2", ...]

GET  <api URL>/node/<UUID>/<data name>/keyrange/<key1>/<key2>

	Returns all keys between 'key1' and 'key2' for this data instance in JSON format:

	[key1, key2, ...]

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key1          Lexicographically lowest alphanumeric key in range.
	key2          Lexicographically highest alphanumeric key in range.

GET  <api URL>/node/<UUID>/<data name>/keyrangevalues/<key1>/<key2>?<options>

	This has the same response as the GET /neuronjsons endpoint but a different way of
	specifying the keys.  In this endpoint, you specify a range of keys.  In the other
	endpoint, you must explicitly send the keys in a GET payload, which may not be
	fully supported.

	Note that this endpoint streams data to the requester, which prevents setting HTTP
	status to error if the streaming has already started.  Instead, malformed output
	will inform the requester of an error.

	Response types:

	1) json (values are expected to be valid JSON or an error is returned)

		{
			"key1": value1,
			"key2": value2,
			...
		}

	2) tar

		A tarfile is returned with each keys specifying the file names and
		values as the file bytes.

	3) protobuf3
	
		neuronjson data needs to be serialized in a format defined by the following 
		protobuf3 definitions:

		message KeyValue {
			string key = 1;
			bytes value = 2;
		}

		message KeyValues {
			repeated KeyValue kvs = 1;
		}

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key1          Lexicographically lowest alphanumeric key in range.
	key2          Lexicographically highest alphanumeric key in range.

	Query-string Options (only one of these allowed):

	json        If set to "true", the response will be JSON as above and the values must
				  be valid JSON or an error will be returned.
	tar			If set to "true", the response will be a tarfile with keys as file names.
	protobuf	Default, or can be set to "true". Response will be protobuf KeyValues response

	Additional query option:

	check		If json=true, setting check=false will tell server to trust that the
				  values will be valid JSON instead of parsing it as a check.

	show		If "user", shows *_user fields.
				If "time", shows *_time fields.
				If "all", shows both *_user and *_time fields.
				If unset (default), shows neither *_user or *_time fields.
			  

GET  <api URL>/node/<UUID>/<data name>/key/<key>[?query-options]

	For a given neuron id key, returns a value depending on the options.  

	Example: 

	GET <api URL>/node/3f8c/stuff/key/myfile.dat

	Returns the data associated with the key "myfile.dat" of instance "stuff" in version
	node 3f8c.

	The "Content-type" of the HTTP response (and usually the request) are
	"application/octet-stream" for arbitrary binary data.

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key           The uint64 of a neuron identifier

	GET Query-string Options:

	show		If "user", shows *_user fields.
				If "time", shows *_time fields.
				If "all", shows both *_user and *_time fields.
				If unset (default), shows neither *_user or *_time fields.


POST <api URL>/node/<UUID>/<data name>/key/<key>

	Updates a key-value pair, modifying the fields with the POSTed JSON fields.
	Note that unlike POST /key in keyvalue datatype instances, this operation updates
	fields by defaults (using old fields not overwritten) rather than replacing
	the entire annotation. The replace behavior can be explicitly set if desired
	to match old keyvalue semantics.  

	For each field, a *_user and *_time field will be added to the annotation unless
	one is already present.  The *_user field will be set to the user making the
	request and the *_time field will be set to the current time. If the current
	field value is the same as the new value, the *_user and *_time fields will
	not be updated.

	Example: 

	POST <api URL>/node/3f8c/stuff/key/15319

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key           The uint64 of a neuron identifier
	
	POSTs will be logged as a Kafka JSON message with the following format:
	{ 
		"Action": "postkv",
		"Key": <key>,
		"Bytes": <number of bytes in data>,
		"UUID": <UUID on which POST was done>
	}

	POST Query-string Options:

	conditional	List of fields separated by commas that should not be overwritten if set.

	replace		If "true" will remove any fields not present


DELETE <api URL>/node/<UUID>/<data name>/key/<key> 
HEAD   <api URL>/node/<UUID>/<data name>/key/<key> 

	Performs operations on a key-value pair depending on the HTTP verb.  

	For HEAD returns:
	200 (OK) if a sparse volume of the given label exists within any optional bounds.
	404 (File not Found) if there is no sparse volume for the given label within any optional bounds.

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key           The uint64 of a neuron identifier
				

GET <api URL>/node/<UUID>/<data name>/keyvalues[?query-options]

	Allows batch query of data. 

	Unless using one of the JSON query options listed below, requested keys and
	returned neuronjson data is serialized in a format defined by the following 
	protobuf3 definitions:

		message KeyValue {
			string key = 1;
			bytes value = 2;
		}

		message Keys {
			repeated string keys = 1;
		}
		
		message KeyValues {
			repeated KeyValue kvs = 1;
		}
	
	The query body must include a Keys serialization and a KeyValues serialization is
	returned.

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.

	Query-string Options:

	show		If "user", shows *_user fields.
				If "time", shows *_time fields.
				If "all", shows both *_user and *_time fields.
				If unset (default), shows neither *_user or *_time fields.

	Query-string Options (only one of these allowed):

	json        If true (default false), query body must be JSON array of keys and returns JSON.
	jsontar		If set to any value for GET, query body must be JSON array of string keys
				  and the returned data will be a tarfile with keys as file names.
	protobuf	If set to "true", the response will be protobuf KeyValues response

	Response types:

	1) json (values are expected to be valid JSON or an error is returned)

		{
			"key1": value1,
			"key2": value2,
			...
		}

	2) tar

		A tarfile is returned with each keys specifying the file names and
		values as the file bytes.

	3) protobuf3
	
		KeyValue data needs to be serialized in a format defined by the following 
		protobuf3 definitions:

		message KeyValue {
			string key = 1;
			bytes value = 2;
		}

		message KeyValues {
			repeated KeyValue kvs = 1;
		}


POST <api URL>/node/<UUID>/<data name>/keyvalues[?query-options]

	Allows batch ingest of data. Each POSTed neuron annotation is handled in same
	was as decribed in POST /key.
	
	The POST body must include a KeyValues serialization as defined by the following
	protobuf3 definitions:

		message KeyValue {
			string key = 1;
			bytes value = 2;
		}

		message Keys {
			repeated string keys = 1;
		}
		
		message KeyValues {
			repeated KeyValue kvs = 1;
		}
	
	
	POSTs will be logged as a series of Kafka JSON messages, each with the format equivalent
	to the single POST /key:
	{ 
		"Action": "postkv",
		"Key": <key>,
		"Bytes": <number of bytes in data>,
		"UUID": <UUID on which POST was done>
	}

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.

	Query-string Options:

	replace		If "true" will remove any fields not present


GET <api URL>/node/<UUID>/<data name>/query[?show=...]
POST <api URL>/node/<UUID>/<data name>/query[?show=...]

	Both GET and POST methods are permitted to launch queries, however the
	POST method is deprecated because it will be blocked for committed versions.
	The JSON query format uses field names as the keys, and desired values.
	Example:
	{ "bodyid": 23, "hemilineage": "0B", ... }
	Each field value must be true, i.e., the conditions or ANDed together.

	If a list of queries (JSON object per query) is POSTed, the results for each query are ORed
	together with duplicate annotations removed.

	A JSON list of objects that matches the query is returned.

	Arguments:

	UUID 		Hexadecimal string with enough characters to uniquely identify a version node.
	data name	Name of neuronjson data instance.

	GET Query-string Options:

	onlyid		If true (false by default), will only return a list of body ids that match.

	show		If "user", shows *_user fields.
				If "time", shows *_time fields.
				If "all", shows both *_user and *_time fields.
				If unset (default), shows neither *_user or *_time fields.
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(map[string]interface{}{})
}

// ---- Interface for Firestore-like persistence that can be stubbed for tests.

type DocGetter interface {
	Data() map[string]interface{}
}

type DocIterator interface {
	Next() (doc DocGetter, err error) // err must be iterator.Done if finished, see pkg "google.golang.org/api/iterator"
	Close()
}

// ---- Firestore implementation of DocGetter and DocIterator.

type firestoreDocGetter struct {
	doc *firestore.DocumentSnapshot
}

func (f *firestoreDocGetter) Data() map[string]interface{} {
	return f.doc.Data()
}

type firestoreIterator struct {
	client *firestore.Client
	it     *firestore.DocumentIterator
}

func (fi *firestoreIterator) Next() (DocGetter, error) {
	firestoreDoc, err := fi.it.Next()
	if err != nil {
		return nil, err
	}
	return &firestoreDocGetter{firestoreDoc}, nil
}

func (fi *firestoreIterator) Close() {
	fi.client.Close()
}

func firestoreOpen(projectID, datasetID string) (DocIterator, error) {
	ctx := context.Background()
	fi := &firestoreIterator{}
	var err error
	if fi.client, err = firestore.NewClient(ctx, projectID); err != nil {
		return nil, fmt.Errorf("could not connect to Firestore for project %q: %v", projectID, err)
	}
	fi.it = fi.client.Collection("clio_annotations_global").Doc("neurons").Collection(datasetID).Where("_head", "==", true).Documents(ctx)
	return fi, nil
}

// Type embeds the datastore's Type to create a unique type for neuronjson functions.
type Type struct {
	datastore.Type
}

// NewType returns a pointer to a new neuronjson Type with default values set.
func NewType() *Type {
	dtype := new(Type)
	dtype.Type = datastore.Type{
		Name:         TypeName,
		URL:          RepoURL,
		Version:      Version,
		Requirements: &storage.Requirements{},
	}
	return dtype
}

// --- TypeService interface ---

// NewDataService returns a pointer to new neuronjson data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	return &Data{Data: basedata}, nil
}

func (dtype *Type) Help() string {
	return fmt.Sprint(helpMessage)
}

// GetByUUIDName returns a pointer to labelblk data given a UUID and data name.
func GetByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance '%s' is not a neuronjson datatype", name)
	}
	return data, nil
}

////////////

type Fields uint8

const (
	ShowBasic Fields = iota
	ShowUsers
	ShowTime
	ShowAll
)

func showFields(r *http.Request) Fields {
	switch r.URL.Query().Get("show") {
	case "user":
		return ShowUsers
	case "time":
		return ShowTime
	case "all":
		return ShowAll
	default:
		return ShowBasic
	}
}

type Metadata uint8

const (
	// JSONSchema is validation JSON schema for annotations
	JSONSchema Metadata = iota

	// NeuSchema is JSON for neutu/neu3 clients
	NeuSchema

	// NeuSchemaBatch is JSON for neutu/neu3 clients
	NeuSchemaBatch
)

func (m Metadata) String() string {
	switch m {
	case JSONSchema:
		return "json_schema"
	case NeuSchema:
		return "schema"
	case NeuSchemaBatch:
		return "schema_batch"
	default:
		return "unknown metadata"
	}
}

// Data embeds the datastore's Data and extends it with neuronjson properties.
type Data struct {
	*datastore.Data

	// The in-memory neuron annotations for HEAD version
	db     map[uint64]NeuronJSON
	ids    []uint64            // sorted list of body ids
	fields map[string]struct{} // list of all fields among the annotations
	dbMu   sync.RWMutex

	// The in-memory metadata for HEAD version
	compiledSchema *jsonschema.Schema // cached on setting of JSONSchema value for rapid validate
	metadata       map[Metadata][]byte
	metadataMu     sync.RWMutex
}

func (d *Data) Equals(d2 *Data) bool {
	return d.Data.Equals(d2.Data)
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended struct{}
	}{
		d.Data,
		struct{}{},
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

// Remove any fields that have underscore prefix.
func removeReservedFields(data NeuronJSON, showFields Fields) NeuronJSON {
	var showUser, showTime bool
	switch showFields {
	case ShowBasic:
		// don't show either user or time -- default values
	case ShowUsers:
		showUser = true
	case ShowTime:
		showTime = true
	case ShowAll:
		return data
	}
	out := data.copy()
	for field := range data {
		if (!showUser && strings.HasSuffix(field, "_user")) || (!showTime && strings.HasSuffix(field, "_time")) {
			delete(out, field)
		}
	}
	return out
}

// Parses keys as body ids, including things like 'a' that might be used in keyrange/0/a.
func parseKeyStr(key string) (uint64, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("key string is empty")
	}
	if key[0] > '9' {
		return math.MaxUint64, nil
	} else if key[0] < '0' {
		return 0, nil
	}
	return strconv.ParseUint(key, 10, 64)
}

// Get bodyid from a JSON-like map
func getBodyID(data map[string]interface{}) (uint64, error) {
	bodyidVal, ok := data["bodyid"]
	if !ok {
		return 0, fmt.Errorf("neuronjson record has no 'bodyid' field")
	}
	bodyid, ok := bodyidVal.(int64)
	if !ok {
		return 0, fmt.Errorf("neuronjson record 'bodyid' is not a uint64 value: %v", bodyidVal)
	}
	return uint64(bodyid), nil
}

///// Persistence of neuronjson data

// getData gets a map value using a key
func (d *Data) getData(ctx storage.Context, keyStr string) (value NeuronJSON, found bool, err error) {
	var db storage.OrderedKeyValueDB
	db, err = datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return
	}
	tk, err := NewTKey(keyStr)
	if err != nil {
		return
	}
	data, err := db.Get(ctx, tk)
	if err != nil {
		return nil, false, fmt.Errorf("error in retrieving key '%s': %v", keyStr, err)
	}
	if data == nil {
		return
	}
	if err = json.Unmarshal(data, &value); err != nil {
		return
	}
	return value, true, nil
}

// putData puts a key / map value at a given uuid
func (d *Data) putData(ctx storage.Context, keyStr string, value NeuronJSON) error {
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tk, err := NewTKey(keyStr)
	if err != nil {
		return err
	}
	return db.Put(ctx, tk, data)
}

// deleteData deletes a key-value pair
func (d *Data) deleteData(ctx storage.Context, keyStr string) error {
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	tk, err := NewTKey(keyStr)
	if err != nil {
		return err
	}
	return db.Delete(ctx, tk)
}

// process a range of keys using supplied function.
func (d *Data) processAllKeys(ctx storage.Context, f func(key string)) error {
	minTKey := storage.MinTKey(keyAnnotation)
	maxTKey := storage.MaxTKey(keyAnnotation)
	return d.processKeysInRange(ctx, minTKey, maxTKey, f)
}

// process a range of keys using supplied function.
func (d *Data) processKeysInRange(ctx storage.Context, minTKey, maxTKey storage.TKey, f func(key string)) error {
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	tkeys, err := db.KeysInRange(ctx, minTKey, maxTKey)
	if err != nil {
		return err
	}
	for _, tkey := range tkeys {
		key, err := DecodeTKey(tkey)
		if err != nil {
			return err
		}
		f(key)
	}
	return nil
}

// process a range of key-value pairs using supplied function.
func (d *Data) processRange(ctx storage.Context, f func(key string, value map[string]interface{})) error {
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	first := storage.MinTKey(keyAnnotation)
	last := storage.MaxTKey(keyAnnotation)
	err = db.ProcessRange(ctx, first, last, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil {
			return nil
		}
		kv := c.TKeyValue
		if kv.V == nil {
			return nil
		}
		key, err := DecodeTKey(kv.K)
		if err != nil {
			return err
		}
		var value map[string]interface{}
		if err := json.Unmarshal(kv.V, &value); err != nil {
			return err
		}
		f(key, value)
		return nil
	})
	return err
}

// Initialize loads mutable properties of the neuronjson data instance,
// which in this case is the in-memory neuron json map for the HEAD version.
func (d *Data) Initialize() {
	tlog := dvid.NewTimeLog()

	leafMain := string(d.RootUUID()) + ":master"
	leafUUID, leafV, err := datastore.MatchingUUID(leafMain)
	if err != nil {
		dvid.Criticalf("Can't find the leaf node of the main/master branch: %v", err)
		return
	}
	dvid.Infof("Loading neuron annotations JSON into memory for neuronjson %q ...\n", d.DataName())
	ctx := datastore.NewVersionedCtx(d, leafV)

	d.initMemoryDB()

	// Load all the data into memory.
	if sch, err := d.getJSONSchema(ctx); err == nil {
		if sch != nil {
			d.compiledSchema = sch
		}
	} else {
		dvid.Criticalf("Can't load JSON schema for neuronjson %q: %v\n", d.DataName(), err)
	}
	if value, err := d.loadMetadata(ctx, NeuSchema); err == nil {
		dvid.Infof("Metadata load of neutu/neu3 JSON schema for %s: %d bytes\n", leafUUID[:6], len(value))
		if value != nil {
			d.metadata[NeuSchema] = value
		}
	} else {
		dvid.Criticalf("Can't load neutu/neu3 schema for neuronjson %q: %v\n", d.DataName(), err)
	}
	if value, err := d.loadMetadata(ctx, NeuSchemaBatch); err == nil {
		dvid.Infof("Metadata load of neutu/neu3 JSON batch schema for %s: %d bytes\n", leafUUID[:6], len(value))
		if value != nil {
			d.metadata[NeuSchemaBatch] = value
		}
	} else {
		dvid.Criticalf("Can't load neutu/neu3 batch schema for neuronjson %q: %v\n", d.DataName(), err)
	}

	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Criticalf("Can't setup ordered keyvalue db for neuronjson %q: %v\n", d.DataName(), err)
		return
	}

	numLoaded := 0
	err = db.ProcessRange(ctx, MinAnnotationTKey, MaxAnnotationTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil {
			return nil
		}
		kv := c.TKeyValue
		if kv.V == nil {
			return nil
		}
		key, err := DecodeTKey(kv.K)
		if err != nil {
			return err
		}

		bodyid, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return fmt.Errorf("received non-integer key %q during neuronjson load from database: %v", key, err)
		}

		var annotation NeuronJSON
		if err := json.Unmarshal(kv.V, &annotation); err != nil {
			return fmt.Errorf("unable to decode annotation for bodyid %d, skipping: %v", bodyid, err)
		}

		d.db[bodyid] = annotation
		d.ids = append(d.ids, bodyid)
		for field := range annotation {
			d.fields[field] = struct{}{}
		}

		numLoaded++
		if numLoaded%1000 == 0 {
			tlog.Infof("Loaded %d annotations into neuronjson instance %q", numLoaded, d.DataName())
		}
		return nil
	})
	if err != nil {
		dvid.Criticalf("Error on loading neuron annotations from database into neuronjson %q: %v\n", d.DataName(), err)
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })
	tlog.Infof("Completed loading of %d annotations into neuronjson instance %q HEAD version %s",
		numLoaded, d.DataName(), string(leafUUID[:6]))
}

// Load documents into backing store and in-memory database.
func (d *Data) loadData(ctx *datastore.VersionedCtx, docStore DocIterator) error {
	tlog := dvid.NewTimeLog()

	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("unable to get keyvalue database: %v", err)
	}

	d.dbMu.Lock() // Note that mutex is NOT unlocked if firestore DB doesn't load because we don't want
	defer d.dbMu.Unlock()

	d.db = make(map[uint64]NeuronJSON)
	d.fields = make(map[string]struct{})
	numdocs := 0
	for {
		doc, err := docStore.Next()
		if err == iterator.Done {
			break
		}
		docGetter, ok := doc.(DocGetter)
		if !ok {
			return fmt.Errorf("loadData(): DocIterator did not return a DocGetter")
		}
		if err != nil {
			return fmt.Errorf("documents iterator error: %v", err)
		}
		annotation := docGetter.Data()
		for field := range annotation {
			d.fields[field] = struct{}{}
		}
		bodyid, err := getBodyID(annotation)
		if err != nil {
			return fmt.Errorf("error getting bodyID from annotation: %v", annotation)
		}
		d.db[bodyid] = annotation
		d.ids = append(d.ids, bodyid) // assumes there are no duplicate bodyid among HEAD annotations

		data, err := json.Marshal(annotation)
		if err != nil {
			return err
		}
		keyStr := strconv.FormatUint(bodyid, 10)
		tk, err := NewTKey(keyStr)
		if err != nil {
			return err
		}
		if err := db.Put(ctx, tk, data); err != nil {
			return err
		}

		numdocs++
		if numdocs%1000 == 0 {
			tlog.Infof("Loaded %d HEAD annotations", numdocs)
		}
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })
	tlog.Infof("Completed loading of %d HEAD annotations", numdocs)
	return nil
}

// --- DataInitializer interface ---

// InitDataHandlers initializes ephemeral data for this instance, which is
// the in-memory keyvalue store where the values are neuron annotation JSON.
func (d *Data) InitDataHandlers() error {
	d.initMemoryDB()
	return nil
}

type kvType interface {
	DataName() dvid.InstanceName
	StreamKV(v dvid.VersionID) (chan storage.KeyValue, error)
}

func (d *Data) initMemoryDB() {
	d.dbMu.Lock() // Note that mutex is NOT unlocked if firestore DB doesn't load because we don't want
	defer d.dbMu.Unlock()

	d.db = make(map[uint64]NeuronJSON)
	d.fields = make(map[string]struct{})
	d.metadata = make(map[Metadata][]byte, 3)
	d.ids = []uint64{}
}

func (d *Data) loadFromKV(v dvid.VersionID, kvData kvType) {
	tlog := dvid.NewTimeLog()

	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		dvid.Criticalf("unable to get keyvalue database: %v", err)
		return
	}

	d.initMemoryDB()

	ch, err := kvData.StreamKV(v)
	if err != nil {
		dvid.Errorf("Error in getting stream of data from keyvalue instance %q: %v\n", kvData.DataName(), err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	numLoaded := 0
	numFromKV := 0
	for kv := range ch {
		key := string(kv.K)
		numFromKV++

		// Handle metadata string keys
		switch key {
		case JSONSchema.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, JSONSchema); err != nil {
				dvid.Errorf("Unable to handle JSON schema metadata transfer, skipping: %v\n", err)
			}
			continue
		case NeuSchema.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, NeuSchema); err != nil {
				dvid.Errorf("Unable to handle neutu/neu3 schema metadata transfer, skipping: %v\n", err)
			}
			continue
		case NeuSchemaBatch.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, NeuSchemaBatch); err != nil {
				dvid.Errorf("Unable to handle neutu/neu3 batch schema metadata transfer, skipping: %v\n", err)
			}
			continue
		}

		// Handle numeric keys for neuron annotations
		bodyid, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			dvid.Errorf("Received non-integer key %q during neuronjson load from keyvalue: ignored\n", key)
			continue
		}

		// a) Persist to storage first
		tk, err := NewTKey(key)
		if err != nil {
			dvid.Errorf("unable to encode neuronjson %q key %q, skipping: %v\n", d.DataName(), key, err)
			continue
		}
		if err := db.Put(ctx, tk, kv.V); err != nil {
			dvid.Errorf("unable to persist neuronjson %q key %s annotation, skipping: %v\n", d.DataName(), key, err)
			continue
		}

		// b) Add to in-memory annotations db
		var annotation NeuronJSON
		if err := json.Unmarshal(kv.V, &annotation); err != nil {
			dvid.Errorf("Unable to decode annotation for bodyid %d, skipping: %v\n", bodyid, err)
			continue
		}

		d.db[bodyid] = annotation
		d.ids = append(d.ids, bodyid)
		for field := range annotation {
			d.fields[field] = struct{}{}
		}

		numLoaded++
		if numLoaded%1000 == 0 {
			tlog.Infof("Loaded %d annotations into neuronjson instance %q", numLoaded, d.DataName())
		}
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })
	errored := numFromKV - numLoaded
	tlog.Infof("Completed loading of %d annotations into neuronjson instance %q (%d skipped)",
		numLoaded, d.DataName(), errored)
}

func (d *Data) importKV(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 5 {
		return fmt.Errorf("keyvalue instance name must be specified after importKV")
	}
	var uuidStr, dataName, cmdStr, kvName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &kvName)

	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	sourceKV, err := keyvalue.GetByUUIDName(uuid, dvid.InstanceName(kvName))
	if err != nil {
		return err
	}
	go d.loadFromKV(versionID, sourceKV)

	reply.Output = []byte(fmt.Sprintf("Started loading from keyvalue instance %q into neuronjson instance %q, uuid %s\n",
		kvName, d.DataName(), uuidStr))
	return nil
}

func (d *Data) loadFirestoreDB(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 6 {
		return fmt.Errorf("ProjectID and DatasetID must be specified after %q", "ingest")
	}
	var uuidStr, dataName, cmdStr, projectID, datasetID string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &projectID, &datasetID)

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	vctx := datastore.NewVersionedCtx(d, versionID)

	var docIterator DocIterator
	if docIterator, err = firestoreOpen(projectID, datasetID); err != nil {
		return err
	}
	if err := d.loadData(vctx, docIterator); err != nil {
		return err
	}
	docIterator.Close()

	reply.Output = []byte(fmt.Sprintf("Ingested Firestore annotations from project %q, dataset %q into neuronjson instance %q, uuid %s\n",
		projectID, datasetID, d.DataName(), uuidStr))
	return nil
}

// Metadata key-value support (all non-neuron annotations)

func (d *Data) loadMetadata(ctx storage.VersionedCtx, meta Metadata) (val []byte, err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	var byteVal []byte
	if byteVal, err = db.Get(ctx, tkey); err != nil {
		return
	}
	return byteVal, nil
}

// gets metadata from either in-memory db if HEAD or from store
func (d *Data) getMetadata(ctx storage.VersionedCtx, meta Metadata) (val []byte, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		defer d.metadataMu.RUnlock()
		if val, found := d.metadata[meta]; found {
			return val, nil
		} else {
			return nil, nil
		}
	}
	return d.loadMetadata(ctx, meta)
}

// get fully compiled JSON schema for use -- TODO
func (d *Data) getJSONSchema(ctx storage.VersionedCtx) (sch *jsonschema.Schema, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		sch = d.compiledSchema
		d.metadataMu.RUnlock()
		if sch != nil {
			return
		}
	}

	var tkey storage.TKey
	if tkey, err = getMetadataKey(JSONSchema); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	var byteVal []byte
	if byteVal, err = db.Get(ctx, tkey); err != nil {
		return
	}
	if len(byteVal) == 0 {
		return nil, fmt.Errorf("no JSON Schema available")
	}
	if ctx.Head() {
		d.metadataMu.RLock()
		d.metadata[JSONSchema] = byteVal
		d.metadataMu.RUnlock()
	}

	sch, err = jsonschema.CompileString("schema.json", string(byteVal))
	if err != nil {
		return
	}
	if sch == nil {
		return nil, fmt.Errorf("no JSON Schema available")
	}
	return
}

func (d *Data) putMetadata(ctx storage.VersionedCtx, val []byte, meta Metadata) (err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	if err = db.Put(ctx, tkey, val); err != nil {
		return
	}

	// If we could persist metadata, add it to in-memory db if head.
	if ctx.Head() {
		d.metadataMu.Lock()
		d.metadata[meta] = val
		if meta == JSONSchema {
			d.compiledSchema, err = jsonschema.CompileString("schema.json", string(val))
			if err != nil {
				d.compiledSchema = nil
				dvid.Errorf("Unable to compile json schema: %v\n", err)
			}
		}
		d.metadataMu.Unlock()
	}
	return nil
}

func (d *Data) metadataExists(ctx storage.VersionedCtx, meta Metadata) (exists bool, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		defer d.metadataMu.RUnlock()
		_, found := d.metadata[meta]
		return found, nil
	}
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	return db.Exists(ctx, tkey)
}

func (d *Data) deleteMetadata(ctx storage.VersionedCtx, meta Metadata) (err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	if err = db.Delete(ctx, tkey); err != nil {
		return
	}
	if ctx.Head() {
		d.metadataMu.Lock()
		defer d.metadataMu.Unlock()
		delete(d.metadata, meta)
	}
	return nil
}

/////////

// add bodyid to in-memory list of bodyids
func (d *Data) addBodyID(bodyid uint64) {
	i := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] >= bodyid })
	if i < len(d.ids) && d.ids[i] == bodyid {
		return
	}
	d.ids = append(d.ids, 0)
	copy(d.ids[i+1:], d.ids[i:])
	d.ids[i] = bodyid
}

// delete bodyid from in-memory list of bodyids
func (d *Data) deleteBodyID(bodyid uint64) {
	i := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] == bodyid })
	if i == len(d.ids) {
		return
	}
	d.ids = append(d.ids[:i], d.ids[i+1:]...)
}

// ---- Query support ----

type QueryJSON map[string]interface{}
type ListQueryJSON []QueryJSON

// UnmarshalJSON parses JSON with numbers preferentially converted to uint64
// or int64 if negative, and strings with "re/" as prefix are compiled as
// a regular expression.
func (qj *QueryJSON) UnmarshalJSON(jsonText []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &raw); err != nil {
		return err
	}
	*qj = make(QueryJSON, len(raw))

	dvid.Infof("query unmarshal on: %s\n", string(jsonText))

	for key, val := range raw {
		s := string(val)
		u, err := strconv.ParseUint(s, 10, 64)
		if err == nil {
			(*qj)[key] = u
			continue
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			(*qj)[key] = i
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			(*qj)[key] = f
			continue
		}
		var int64list []int64
		if err = json.Unmarshal(val, &int64list); err == nil {
			(*qj)[key] = int64list
			continue
		}
		if len(s) > 4 && strings.HasPrefix(s, `"re/`) {
			re, err := regexp.Compile(s[4 : len(s)-1])
			if err == nil {
				(*qj)[key] = re
				continue
			}
		}
		var strlist []string
		if err = json.Unmarshal(val, &strlist); err == nil {
			hasRegex := false
			iflist := make([]interface{}, len(strlist))
			for i, s := range strlist {
				if len(s) > 3 && strings.HasPrefix(s, "re/") {
					hasRegex = true
					if re, err := regexp.Compile(s[3:]); err == nil {
						iflist[i] = re
					}
				}
				if iflist[i] == nil {
					iflist[i] = s
				}
			}
			if hasRegex {
				(*qj)[key] = iflist
			} else {
				(*qj)[key] = strlist
			}
			continue
		}
		var listVal interface{}
		if err = json.Unmarshal(val, &listVal); err == nil {
			(*qj)[key] = listVal
			continue
		}
		return fmt.Errorf("unable to parse JSON value %q: %v", s, err)
	}
	return nil
}

type NeuronJSON map[string]interface{}
type ListNeuronJSON []NeuronJSON

func (nj NeuronJSON) copy() NeuronJSON {
	dup := make(NeuronJSON, len(nj))
	for k, v := range nj {
		dup[k] = v
	}
	return dup
}

// UnmarshalJSON parses JSON with numbers preferentially converted to uint64
// or int64 if negative.
func (nj *NeuronJSON) UnmarshalJSON(jsonText []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &raw); err != nil {
		return err
	}
	*nj = make(NeuronJSON, len(raw))

	for key, val := range raw {
		s := string(val)
		u, err := strconv.ParseUint(s, 10, 64)
		if err == nil {
			(*nj)[key] = u
			continue
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			(*nj)[key] = i
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			(*nj)[key] = f
			continue
		}
		var int64list []int64
		if err = json.Unmarshal(val, &int64list); err == nil {
			(*nj)[key] = int64list
			continue
		}
		var strlist []string
		if err = json.Unmarshal(val, &strlist); err == nil {
			(*nj)[key] = strlist
			continue
		}
		var listVal interface{}
		if err = json.Unmarshal(val, &listVal); err == nil {
			(*nj)[key] = listVal
			continue
		}
		return fmt.Errorf("unable to parse JSON value %q: %v", s, err)
	}
	return nil
}

// move the following to Generics when upgrading and requiring Go 1.18

func checkIntMatch(query int64, field []int64) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if fieldValue == query {
			return true
		}
	}
	return false
}

func checkStrMatch(query string, field []string) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if fieldValue == query {
			return true
		}
	}
	return false
}

func checkRegexMatch(query *regexp.Regexp, field []string) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if query.Match([]byte(fieldValue)) {
			return true
		}
	}
	return false
}

// given a query on this field composed of one or a list of values of unknown type,
// see if any of the field's values match a query value regardless of slightly
// different integer typing.
func checkField(queryValue, fieldValue interface{}) bool {
	// if field value is integer of some kind, convert to []int64 assuming MSB not
	// needed for our data.
	// if field value is string, make it []string.
	// Field can be (single or list of) number, string, other.
	var fieldNumList []int64
	var fieldStrList []string
	switch v := fieldValue.(type) {
	case int64:
		fieldNumList = []int64{v}
	case []int64:
		fieldNumList = v
	case uint64:
		fieldNumList = []int64{int64(v)}
	case []uint64:
		fieldNumList = make([]int64, len(v))
		for i, val := range v {
			fieldNumList[i] = int64(val)
		}
	case string:
		fieldStrList = []string{v}
	case []string:
		fieldStrList = v

	default:
		dvid.Errorf("Unknown field value of type %s: %v\n", reflect.TypeOf(v), v)
		return false
	}
	if len(fieldNumList) == 0 && len(fieldStrList) == 0 {
		return false
	}

	// convert query value to list of types as above for field value.
	switch v := queryValue.(type) {
	case int64:
		if checkIntMatch(v, fieldNumList) {
			return true
		}
	case []int64:
		for _, i := range v {
			if checkIntMatch(i, fieldNumList) {
				return true
			}
		}
	case uint64:
		if checkIntMatch(int64(v), fieldNumList) {
			return true
		}
	case []uint64:
		for _, val := range v {
			if checkIntMatch(int64(val), fieldNumList) {
				return true
			}
		}
	case string:
		if checkStrMatch(v, fieldStrList) {
			return true
		}
	case []string:
		for _, s := range v {
			if checkStrMatch(s, fieldStrList) {
				return true
			}
		}
	case *regexp.Regexp:
		if checkRegexMatch(v, fieldStrList) {
			return true
		}
	case []interface{}:
		elem := v[0]
		switch e := elem.(type) {
		case int:
			for _, val := range v {
				if checkIntMatch(int64(val.(int)), fieldNumList) {
					return true
				}
			}
		case string, *regexp.Regexp:
			for _, val := range v {
				switch query := val.(type) {
				case string:
					if checkStrMatch(query, fieldStrList) {
						return true
					}
				case *regexp.Regexp:
					if checkRegexMatch(query, fieldStrList) {
						return true
					}
				}
			}
		default:
			var t = reflect.TypeOf(e)
			dvid.Errorf("neuronjson query value %v has elements of illegal type %v\n", v, t)
		}
	default:
		var t = reflect.TypeOf(v)
		dvid.Errorf("neuronjson query value %v has illegal type %v\n", v, t)
	}
	return false
}

func fieldMatch(queryValue, fieldValue interface{}) bool {
	if queryValue == nil {
		return false
	}
	if fieldValue == nil {
		return false
	}
	return checkField(queryValue, fieldValue)
}

// returns true if at least one query on the list matches the value.
func queryMatch(queryList ListQueryJSON, value map[string]interface{}) (matches bool, err error) {
	if len(queryList) == 0 {
		matches = false
		return
	}
	for _, query := range queryList {
		and_match := true
		for queryKey, queryValue := range query { // all query keys must be present and match
			recordValue, found := value[queryKey]
			if !found || !fieldMatch(queryValue, recordValue) {
				and_match = false
				break
			}
		}
		if and_match {
			return true, nil
		}
	}
	return false, nil
}

func (d *Data) queryInMemory(w http.ResponseWriter, queryL ListQueryJSON, showFields Fields) (err error) {
	d.dbMu.RLock()
	defer d.dbMu.RUnlock()

	numMatches := 0
	var jsonBytes []byte
	for _, value := range d.db {
		var matches bool
		if matches, err = queryMatch(queryL, value); err != nil {
			return
		} else if matches {
			out := removeReservedFields(value, showFields)
			if jsonBytes, err = json.Marshal(out); err != nil {
				break
			}
			if numMatches > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprint(w, string(jsonBytes))
			numMatches++
		}
	}
	return
}

func (d *Data) queryBackingStore(ctx storage.VersionedCtx, w http.ResponseWriter,
	queryL ListQueryJSON, showFields Fields) (err error) {

	process_func := func(key string, value map[string]interface{}) {
		if matches, err := queryMatch(queryL, value); err != nil {
			dvid.Errorf("error in matching process: %v\n", err) // TODO: alter d.processRange to allow return of err
			return
		} else if !matches {
			return
		}
		out := removeReservedFields(value, showFields)
		jsonBytes, err := json.Marshal(out)
		if err != nil {
			dvid.Errorf("error in JSON encoding: %v\n", err)
			return
		}
		fmt.Fprint(w, string(jsonBytes))
	}
	return d.processRange(ctx, process_func)
}

// Query reads POSTed data and returns JSON.
func (d *Data) Query(ctx *datastore.VersionedCtx, w http.ResponseWriter, uuid dvid.UUID, onlyid bool, showFields Fields, in io.ReadCloser) (err error) {
	var queryBytes []byte
	if queryBytes, err = io.ReadAll(in); err != nil {
		return
	}
	// Try to parse as list of queries and if fails, try as object and make it a one-item list.
	var queryL ListQueryJSON
	if err = json.Unmarshal(queryBytes, &queryL); err != nil {
		var queryObj QueryJSON
		if err = queryObj.UnmarshalJSON(queryBytes); err != nil {
			err = fmt.Errorf("unable to parse JSON query: %s", string(queryBytes))
			return
		}
		queryL = ListQueryJSON{queryObj}
	}

	// Perform the query
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, "[")
	if ctx.Head() {
		if err = d.queryInMemory(w, queryL, showFields); err != nil {
			return
		}
	} else {
		if err = d.queryBackingStore(ctx, w, queryL, showFields); err != nil {
			return
		}
	}
	fmt.Fprint(w, "]")
	return
}

// KeyExists returns true if a key is found.
func (d *Data) KeyExists(ctx storage.VersionedCtx, keyStr string) (found bool, err error) {
	if ctx.Head() {
		d.dbMu.RLock()
		defer d.dbMu.RUnlock()
		var bodyid uint64
		bodyid, err = strconv.ParseUint(keyStr, 10, 64)
		if err != nil {
			return false, err
		}
		d.dbMu.RLock()
		defer d.dbMu.RUnlock()
		_, found = d.db[bodyid]
		return found, nil
	}
	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return false, err
	}
	tk, err := NewTKey(keyStr)
	if err != nil {
		return false, err
	}
	return db.Exists(ctx, tk)
}

// GetKeysInRange returns all keys in the range [keyBeg, keyEnd].  Results on HEAD are ordered
// by integer key, while results on other branches are ordered lexicographically.
func (d *Data) GetKeysInRange(ctx storage.VersionedCtx, keyBeg, keyEnd string) (keys []string, err error) {
	var bodyidBeg, bodyidEnd uint64
	if bodyidBeg, err = parseKeyStr(keyBeg); err != nil {
		return
	}
	if bodyidEnd, err = parseKeyStr(keyEnd); err != nil {
		return
	}
	if ctx.Head() {
		d.dbMu.RLock()
		defer d.dbMu.RUnlock()
		begI := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] >= bodyidBeg })
		endI := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] > bodyidEnd })
		size := endI - begI
		if size <= 0 {
			keys = []string{}
			return
		}
		keys = make([]string, size)
		pos := 0
		for i := begI; i < endI; i++ {
			bodyid := d.ids[i]
			keys[pos] = strconv.FormatUint(bodyid, 10)
			pos++
		}
	} else {
		var begTKey, endTKey storage.TKey
		begTKey, err = NewTKey(keyBeg)
		if err != nil {
			return nil, err
		}
		endTKey, err = NewTKey(keyEnd)
		if err != nil {
			return nil, err
		}
		process_func := func(key string) {
			bodyid, err := parseKeyStr(key)
			if err == nil && bodyid >= bodyidBeg && bodyid <= bodyidEnd {
				keys = append(keys, key)
			}
		}
		err = d.processKeysInRange(ctx, begTKey, endTKey, process_func)
	}
	return
}

func (d *Data) GetAll(ctx storage.VersionedCtx, showFields Fields) ([]map[string]interface{}, error) {
	var all []map[string]interface{}
	if ctx.Head() {
		d.dbMu.RLock()
		all = make([]map[string]interface{}, len(d.db)) // use cur size as starting size
		i := 0
		for _, value := range d.db {
			out := removeReservedFields(value, showFields)
			all[i] = out
			i++
		}
		d.dbMu.RUnlock()
	} else {
		process_func := func(key string, value map[string]interface{}) {
			out := removeReservedFields(value, showFields)
			all = append(all, out)
		}
		if err := d.processRange(ctx, process_func); err != nil {
			return nil, err
		}
	}
	return all, nil
}

func (d *Data) GetKeys(ctx storage.VersionedCtx) (out []string, err error) {
	if ctx.Head() {
		out = make([]string, len(d.ids))
		d.dbMu.RLock()
		for i, bodyid := range d.ids {
			out[i] = strconv.FormatUint(bodyid, 10)
		}
		d.dbMu.RUnlock()
	} else {
		process_func := func(key string) {
			out = append(out, key)
		}
		if err := d.processAllKeys(ctx, process_func); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (d *Data) GetFields() ([]string, error) {
	fields := make([]string, len(d.fields))
	i := 0
	for field := range d.fields {
		fields[i] = field
		i++
	}
	return fields, nil
}

// GetData gets a byte value using a key
func (d *Data) GetData(ctx storage.VersionedCtx, keyStr string, showFields Fields) ([]byte, bool, error) {
	// Allow "schema" and "schema_batch" on /key endpoint for backwards compatibility with DVID keyvalue instances.
	switch keyStr {
	case NeuSchema.String():
		data, err := d.getMetadata(ctx, NeuSchema)
		if err != nil {
			return nil, false, fmt.Errorf("unable to retrieve neutu/neu3 JSON schema: %v", err)
		}
		if data != nil {
			return data, true, nil
		}
		return nil, false, nil
	case NeuSchemaBatch.String():
		data, err := d.getMetadata(ctx, NeuSchemaBatch)
		if err != nil {
			return nil, false, fmt.Errorf("unable to retrieve neutu/neu3 JSON batch schema: %v", err)
		}
		if data != nil {
			return data, true, nil
		}
		return nil, false, nil
	}
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return nil, false, err
	}
	var value map[string]interface{}
	var found bool
	if ctx.Head() {
		d.dbMu.RLock()
		defer d.dbMu.RUnlock()
		value, found = d.db[bodyid]
		if !found {
			return nil, false, nil
		}
	} else {
		value, found, err = d.getData(ctx, keyStr)
		if !found || err != nil {
			return nil, false, err
		}
	}
	out := removeReservedFields(value, showFields)
	data, err := json.Marshal(out)
	return data, true, err
}

// update _user and _time fields for any fields newly set or modified.
func updateJSON(origData, newData NeuronJSON, user string, conditionals []string, replace bool) {
	// determine if any fields are being set for the first time or modified
	newlySet := make(map[string]struct{}, len(newData))
	if origData == nil {
		for field := range newData {
			newlySet[field] = struct{}{}
		}
	} else {
		for field, value := range newData {
			if origValue, found := origData[field]; !found || !reflect.DeepEqual(value, origValue) {
				newlySet[field] = struct{}{}
			}
		}

		// carry forward any fields not being modified if replace option is not set
		if !replace {
			protectedFields := make(map[string]struct{}, len(conditionals))
			for _, field := range conditionals {
				protectedFields[field] = struct{}{}
			}
			for field, origValue := range origData {
				if _, found := newData[field]; !found {
					newData[field] = origValue
					continue
				}
				if _, found := protectedFields[field]; found {
					newData[field] = origValue
					delete(newlySet, field)
				}
			}
		}
	}

	// add _user and _time fields for newly set and not prevented via conditionals
	t := time.Now()
	timeStr := t.Format(time.RFC3339)
	for field := range newlySet {
		if field == "bodyid" || field == "user" {
			continue // these fields shouldn't have _user or _time fields added
		}
		if strings.HasSuffix(field, "_time") || strings.HasSuffix(field, "_user") {
			continue // we will handle this with main field
		}
		if _, foundUser := newlySet[field+"_user"]; !foundUser && user != "" {
			newData[field+"_user"] = user
		}
		if _, foundTime := newlySet[field+"_time"]; !foundTime {
			newData[field+"_time"] = timeStr
		}
	}
}

// PutData puts a valid JSON []byte into a neuron key at a given uuid.
// If replace is true, will use given value instead of updating fields that were given.
// If field values are given but do not change, the _user and _time fields will not be updated.
func (d *Data) PutData(ctx *datastore.VersionedCtx, keyStr string, value []byte, conditionals []string, replace bool) error {
	// Allow "schema" and "schema_batch" on /key endpoint for backwards compatibility with DVID keyvalue instances.
	switch keyStr {
	case "0":
		return fmt.Errorf("body id 0 is reserved and so cannot be stored")
	case NeuSchema.String():
		if err := d.putMetadata(ctx, value, NeuSchema); err != nil {
			return fmt.Errorf("unable to handle POST neutu/neu3 schema metadata: %v", err)
		}
		return nil
	case NeuSchemaBatch.String():
		if err := d.putMetadata(ctx, value, NeuSchemaBatch); err != nil {
			return fmt.Errorf("unable to handle POST neutu/neu3 batch schema metadata: %v", err)
		}
		return nil
	}

	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return err
	}

	// validate if we have a JSON schema
	if sch, err := d.getJSONSchema(ctx); err == nil {
		var v interface{}
		if err = json.Unmarshal(value, &v); err != nil {
			return err
		}
		if err = sch.Validate(v); err != nil {
			return err
		}
	} else {
		dvid.Infof("Skipping validation of POST %q neuron annotation: %v\n", d.DataName(), err)
	}

	var newData NeuronJSON
	if err := json.Unmarshal(value, &newData); err != nil {
		return err
	}

	// get original data so we can handle default update and tell which values change for _user/_time fields.
	origData, found, err := d.getData(ctx, keyStr)
	if err != nil {
		return err
	}
	if !found {
		origData = nil
	}
	updateJSON(origData, newData, ctx.User, conditionals, replace)

	// write result
	if ctx.Head() {
		d.dbMu.Lock()
		defer d.dbMu.Unlock()
		d.db[bodyid] = newData
		d.addBodyID(bodyid)
	}
	return d.putData(ctx, keyStr, newData)
}

// DeleteData deletes a key-value pair
func (d *Data) DeleteData(ctx storage.VersionedCtx, keyStr string) error {
	// Allow "schema" and "schema_batch" on /key endpoint for backwards compatibility with DVID keyvalue instances.
	switch keyStr {
	case NeuSchema.String():
		if err := d.deleteMetadata(ctx, NeuSchema); err != nil {
			return fmt.Errorf("unable to handle DELETE neutu/neu3 schema metadata: %v", err)
		}
		return nil
	case NeuSchemaBatch.String():
		if err := d.deleteMetadata(ctx, NeuSchemaBatch); err != nil {
			return fmt.Errorf("unable to handle DELETE neutu/neu3 batch schema metadata: %v", err)
		}
		return nil
	}

	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return err
	}
	if ctx.Head() {
		d.dbMu.Lock()
		defer d.dbMu.Unlock()
		_, found := d.db[bodyid]
		if found {
			delete(d.db, bodyid)
			d.deleteBodyID(bodyid)
		}
	}
	return d.deleteData(ctx, keyStr)
}

// put handles a PUT command-line request.
func (d *Data) put(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("key name must be specified after 'put'")
	}
	if len(cmd.Input) == 0 {
		return fmt.Errorf("no data was passed into standard input")
	}
	var uuidStr, dataName, cmdStr, keyStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &keyStr)

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Store data
	if !d.Versioned() {
		// Map everything to root version.
		versionID, err = datastore.GetRepoRootVersion(versionID)
		if err != nil {
			return err
		}
	}
	ctx := datastore.NewVersionedCtx(d, versionID)
	if err = d.PutData(ctx, keyStr, cmd.Input, nil, false); err != nil {
		return fmt.Errorf("error on put to key %q for neuronjson %q: %v", keyStr, d.DataName(), err)
	}

	reply.Output = []byte(fmt.Sprintf("Put %d bytes into key %q for neuronjson %q, uuid %s\n",
		len(cmd.Input), keyStr, d.DataName(), uuidStr))
	return nil
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

func (d *Data) sendOldJSONValuesInRange(ctx storage.VersionedCtx, w http.ResponseWriter,
	r *http.Request, keyBeg, keyEnd string, showFields Fields) (numKeys int, err error) {

	tarOut := (r.URL.Query().Get("jsontar") == "true") || (r.URL.Query().Get("tar") == "true")
	jsonOut := r.URL.Query().Get("json") == "true"
	checkVal := r.URL.Query().Get("check") == "true"
	if tarOut && jsonOut {
		err = fmt.Errorf("can only specify tar or json output, not both")
		return
	}
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return 0, err
	}

	var kvs proto.KeyValues
	var tw *tar.Writer

	switch {
	case tarOut:
		w.Header().Set("Content-type", "application/tar")
		tw = tar.NewWriter(w)
	case jsonOut:
		w.Header().Set("Content-type", "application/json")
		if _, err = w.Write([]byte("{")); err != nil {
			return
		}
	default:
	}

	// Compute first and last key for range
	first, err := NewTKey(keyBeg)
	if err != nil {
		return 0, err
	}
	last, err := NewTKey(keyEnd)
	if err != nil {
		return 0, err
	}

	var wroteVal bool
	err = db.ProcessRange(ctx, first, last, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil {
			return nil
		}
		kv := c.TKeyValue
		if kv.V == nil {
			return nil
		}
		key, err := DecodeTKey(kv.K)
		if err != nil {
			return err
		}
		var jsonData NeuronJSON
		if err := json.Unmarshal(kv.V, &jsonData); err != nil {
			return err
		}
		out := removeReservedFields(jsonData, showFields)
		jsonBytes, err := json.Marshal(out)
		if err != nil {
			return err
		}
		switch {
		case tarOut:
			hdr := &tar.Header{
				Name: key,
				Size: int64(len(jsonBytes)),
				Mode: 0755,
			}
			if err = tw.WriteHeader(hdr); err != nil {
				return err
			}
			if _, err = tw.Write(jsonBytes); err != nil {
				return err
			}
		case jsonOut:
			if wroteVal {
				if _, err = w.Write([]byte(",")); err != nil {
					return err
				}
			}
			if len(jsonBytes) == 0 {
				jsonBytes = []byte("{}")
			} else if checkVal && !json.Valid(jsonBytes) {
				return fmt.Errorf("bad JSON for key %q", key)
			}
			out := fmt.Sprintf(`"%s":`, key)
			if _, err = w.Write([]byte(out)); err != nil {
				return err
			}
			if _, err = w.Write(jsonBytes); err != nil {
				return err
			}
			wroteVal = true
		default:
			kv := &proto.KeyValue{
				Key:   key,
				Value: jsonBytes,
			}
			kvs.Kvs = append(kvs.Kvs, kv)
		}

		return nil
	})
	switch {
	case tarOut:
		tw.Close()
	case jsonOut:
		if _, err = w.Write([]byte("}")); err != nil {
			return
		}
	default:
		numKeys = len(kvs.Kvs)
		var serialization []byte
		if serialization, err = pb.Marshal(&kvs); err != nil {
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if _, err = w.Write(serialization); err != nil {
			return
		}
	}
	return
}

func (d *Data) sendJSONValuesInRange(ctx storage.VersionedCtx, w http.ResponseWriter,
	r *http.Request, keyBeg, keyEnd string, showFields Fields) (numKeys int, err error) {

	if !ctx.Head() {
		return 0, fmt.Errorf("cannot use range query on non-head version at this time")
	}
	if len(keyBeg) == 0 || len(keyEnd) == 0 {
		return 0, fmt.Errorf("must specify non-empty beginning and ending key")
	}
	tarOut := (r.URL.Query().Get("jsontar") == "true") || (r.URL.Query().Get("tar") == "true")
	jsonOut := r.URL.Query().Get("json") == "true"
	checkVal := r.URL.Query().Get("check") == "true"
	if tarOut && jsonOut {
		err = fmt.Errorf("can only specify tar or json output, not both")
		return
	}

	var tw *tar.Writer
	switch {
	case tarOut:
		w.Header().Set("Content-type", "application/tar")
		tw = tar.NewWriter(w)
	case jsonOut:
		w.Header().Set("Content-type", "application/json")
		if _, err = w.Write([]byte("{")); err != nil {
			return
		}
	default:
	}

	// Accept arbitrary strings for first and last key for range
	bodyidBeg, err := parseKeyStr(keyBeg)
	if err != nil {
		return 0, err
	}
	bodyidEnd, err := parseKeyStr(keyEnd)
	if err != nil {
		return 0, err
	}
	begI := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] >= bodyidBeg })
	endI := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] > bodyidEnd })

	// Collect JSON values in range
	var kvs proto.KeyValues
	var wroteVal bool
	for i := begI; i < endI; i++ {
		bodyid := d.ids[i]
		jsonData, ok := d.db[bodyid]
		if !ok {
			dvid.Errorf("inconsistent neuronjson DB: bodyid %d at pos %d is not in db cache... skipping", bodyid, i)
			continue
		}
		out := removeReservedFields(jsonData, showFields)
		key := strconv.FormatUint(bodyid, 10)
		var jsonBytes []byte
		jsonBytes, err = json.Marshal(out)
		if err != nil {
			return 0, err
		}
		switch {
		case tarOut:
			hdr := &tar.Header{
				Name: key,
				Size: int64(len(jsonBytes)),
				Mode: 0755,
			}
			if err = tw.WriteHeader(hdr); err != nil {
				return
			}
			if _, err = tw.Write(jsonBytes); err != nil {
				return
			}
		case jsonOut:
			if wroteVal {
				if _, err = w.Write([]byte(",")); err != nil {
					return
				}
			}
			if len(jsonBytes) == 0 {
				jsonBytes = []byte("{}")
			} else if checkVal && !json.Valid(jsonBytes) {
				return 0, fmt.Errorf("bad JSON for key %q", key)
			}
			out := fmt.Sprintf(`"%s":`, key)
			if _, err = w.Write([]byte(out)); err != nil {
				return
			}
			if _, err = w.Write(jsonBytes); err != nil {
				return
			}
			wroteVal = true
		default:
			kv := &proto.KeyValue{
				Key:   key,
				Value: jsonBytes,
			}
			kvs.Kvs = append(kvs.Kvs, kv)
		}
	}

	switch {
	case tarOut:
		tw.Close()
	case jsonOut:
		if _, err = w.Write([]byte("}")); err != nil {
			return
		}
	default:
		numKeys = len(kvs.Kvs)
		var serialization []byte
		if serialization, err = pb.Marshal(&kvs); err != nil {
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if _, err = w.Write(serialization); err != nil {
			return
		}
	}
	return
}

func (d *Data) sendJSONKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys []string, checkVal bool, showFields Fields) (writtenBytes int, err error) {
	w.Header().Set("Content-type", "application/json")
	if writtenBytes, err = w.Write([]byte("{")); err != nil {
		return
	}
	var n int
	var wroteVal bool
	for _, key := range keys {
		if wroteVal {
			if n, err = w.Write([]byte(",")); err != nil {
				return
			}
			writtenBytes += n
		}
		var val []byte
		var found bool
		if val, found, err = d.GetData(ctx, key, showFields); err != nil {
			return
		}
		if !found {
			wroteVal = false
			continue
		}
		if len(val) == 0 {
			val = []byte("{}")
		} else if checkVal && !json.Valid(val) {
			err = fmt.Errorf("bad JSON for key %q", key)
			return
		}
		out := fmt.Sprintf(`"%s":`, key)
		if n, err = w.Write([]byte(out)); err != nil {
			return
		}
		writtenBytes += n
		if n, err = w.Write(val); err != nil {
			return
		}
		writtenBytes += n
		wroteVal = true
	}
	_, err = w.Write([]byte("}"))
	return
}

func (d *Data) sendTarKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys []string, showFields Fields) (writtenBytes int, err error) {
	var n int
	w.Header().Set("Content-type", "application/tar")
	tw := tar.NewWriter(w)
	for _, key := range keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(ctx, key, showFields); err != nil {
			return
		}
		if !found {
			val = nil
		}
		hdr := &tar.Header{
			Name: key,
			Size: int64(len(val)),
			Mode: 0755,
		}
		if err = tw.WriteHeader(hdr); err != nil {
			return
		}
		if n, err = tw.Write(val); err != nil {
			return
		}
		writtenBytes += n
	}
	tw.Close()
	return
}

func (d *Data) sendProtobufKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys proto.Keys, showFields Fields) (writtenBytes int, err error) {
	var kvs proto.KeyValues
	kvs.Kvs = make([]*proto.KeyValue, len(keys.Keys))
	for i, key := range keys.Keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(ctx, key, showFields); err != nil {
			return
		}
		if !found {
			val = nil
		}
		kvs.Kvs[i] = &proto.KeyValue{
			Key:   key,
			Value: val,
		}
	}
	var serialization []byte
	if serialization, err = pb.Marshal(&kvs); err != nil {
		return
	}
	w.Header().Set("Content-type", "application/octet-stream")
	if writtenBytes, err = w.Write(serialization); err != nil {
		return
	}
	if writtenBytes != len(serialization) {
		err = fmt.Errorf("unable to write all %d bytes of serialized keyvalues: only %d bytes written", len(serialization), writtenBytes)
	}
	return
}

func (d *Data) handleKeyValues(ctx storage.VersionedCtx, w http.ResponseWriter, r *http.Request,
	uuid dvid.UUID, showFields Fields) (numKeys, writtenBytes int, err error) {

	tarOut := (r.URL.Query().Get("jsontar") == "true") || (r.URL.Query().Get("tar") == "true")
	jsonOut := r.URL.Query().Get("json") == "true"
	checkVal := r.URL.Query().Get("check") == "true"

	if tarOut && jsonOut {
		err = fmt.Errorf("can only specify tar or json output, not both")
		return
	}
	var data []byte
	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	switch {
	case tarOut:
		var keys []string
		if err = json.Unmarshal(data, &keys); err != nil {
			return
		}
		numKeys = len(keys)
		writtenBytes, err = d.sendTarKV(ctx, w, keys, showFields)
	case jsonOut:
		var keys []string
		var keysInt []uint64
		if err = json.Unmarshal(data, &keysInt); err == nil {
			// convert to string keys for compatibility with keyvalue type & downstream code
			keys = make([]string, len(keysInt))
			for i, n := range keysInt {
				keys[i] = strconv.FormatUint(n, 10)
			}
		} else if err = json.Unmarshal(data, &keys); err != nil {
			return
		}
		numKeys = len(keys)
		writtenBytes, err = d.sendJSONKV(ctx, w, keys, checkVal, showFields)
	default:
		var keys proto.Keys
		if err = pb.Unmarshal(data, &keys); err != nil {
			return
		}
		numKeys = len(keys.Keys)
		writtenBytes, err = d.sendProtobufKV(ctx, w, keys, showFields)
	}
	return
}

func (d *Data) handleIngest(ctx *datastore.VersionedCtx, r *http.Request, uuid dvid.UUID) error {
	cond_fields := r.URL.Query().Get("conditionals")
	conditionals := strings.Split(cond_fields, ",")
	replace := r.URL.Query().Get("replace") == "true"
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	var kvs proto.KeyValues
	if err := pb.Unmarshal(data, &kvs); err != nil {
		return err
	}
	for _, kv := range kvs.Kvs {
		err = d.PutData(ctx, kv.Key, kv.Value, conditionals, replace)
		if err != nil {
			return err
		}

		msginfo := map[string]interface{}{
			"Action":    "ingestneuronjson",
			"Key":       kv.Key,
			"Bytes":     len(kv.Value),
			"UUID":      string(uuid),
			"Timestamp": time.Now().String(),
		}
		jsonmsg, _ := json.Marshal(msginfo)
		if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("Error on sending neuronjson POST op to kafka: %v\n", err)
		}
	}
	return nil
}

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprint(helpMessage)
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "put":
		return d.put(request, reply)
	case "import-kv":
		return d.importKV(request, reply)
	case "import-filestore":
		return d.loadFirestoreDB(request, reply)
	default:
		return fmt.Errorf("unknown command.  Data %q [%s] does not support %q command",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
}

func (d *Data) handleSchema(ctx storage.VersionedCtx, w http.ResponseWriter, r *http.Request, uuid dvid.UUID, action string, meta Metadata) error {
	switch action {
	case "head":
		found, err := d.metadataExists(ctx, meta)
		if err != nil {
			return err
		}
		if found {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

	case "get":
		value, err := d.getMetadata(ctx, meta)
		if err != nil {
			return err
		} else if value == nil {
			w.WriteHeader(http.StatusNotFound)
			return nil
		}
		if _, err := w.Write(value); err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")

	case "delete":
		if err := d.deleteMetadata(ctx, meta); err != nil {
			return err
		}

	case "post":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}
		if err := d.putMetadata(ctx, data, meta); err != nil {
			return err
		}

	default:
		return fmt.Errorf("key endpoint does not support %q HTTP verb", action)
	}
	return nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
	timedLog := dvid.NewTimeLog()

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API specification")
		return
	}

	var comment string
	action := strings.ToLower(r.Method)

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return

	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, jsonStr)
		return

	case "tags":
		if action == "post" {
			replace := r.URL.Query().Get("replace") == "true"
			if err := datastore.SetTagsByJSON(d, uuid, replace, r.Body); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		} else {
			jsonBytes, err := d.MarshalJSONTags()
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, string(jsonBytes))
		}

	case JSONSchema.String():
		if err := d.handleSchema(ctx, w, r, uuid, action, JSONSchema); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case NeuSchema.String():
		if err := d.handleSchema(ctx, w, r, uuid, action, NeuSchema); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case NeuSchemaBatch.String():
		if err := d.handleSchema(ctx, w, r, uuid, action, NeuSchemaBatch); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "all":
		kvList, err := d.GetAll(ctx, showFields(r))
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		jsonBytes, err := json.Marshal(kvList)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(jsonBytes))
		comment = "HTTP GET all"

	case "keys":
		keyList, err := d.GetKeys(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		jsonBytes, err := json.Marshal(keyList)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(jsonBytes))
		comment = "HTTP GET keys"

	case "fields":
		fieldList, err := d.GetFields()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		jsonBytes, err := json.Marshal(fieldList)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(jsonBytes))
		comment = "HTTP GET fields"

	case "query":
		if action != "post" && action != "get" {
			server.BadRequest(w, r, fmt.Errorf("only GET or POST methods allowed for /query endpoint"))
			return
		}
		onlyid := r.URL.Query().Get("onlyid") == "true"
		err := d.Query(ctx, w, uuid, onlyid, showFields(r), r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "keyrange":
		if len(parts) < 6 {
			server.BadRequest(w, r, "expect beginning and end keys to follow 'keyrange' endpoint")
			return
		}

		// Return JSON list of keys
		keyBeg := parts[4]
		keyEnd := parts[5]
		keyList, err := d.GetKeysInRange(ctx, keyBeg, keyEnd)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		jsonBytes, err := json.Marshal(keyList)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(jsonBytes))
		comment = fmt.Sprintf("HTTP GET keyrange [%q, %q]", keyBeg, keyEnd)

	case "keyrangevalues":
		if len(parts) < 6 {
			server.BadRequest(w, r, "expect beginning and end keys to follow 'keyrangevalues' endpoint")
			return
		}

		// Return JSON list of keys
		keyBeg := parts[4]
		keyEnd := parts[5]
		w.Header().Set("Content-Type", "application/json")
		if ctx.Head() {
			numKeys, err := d.sendJSONValuesInRange(ctx, w, r, keyBeg, keyEnd, showFields(r))
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP GET keyrangevalues sent %d values for [%q, %q]", numKeys, keyBeg, keyEnd)
		} else {
			numKeys, err := d.sendOldJSONValuesInRange(ctx, w, r, keyBeg, keyEnd, showFields(r))
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP GET keyrangevalues sent %d values for [%q, %q]", numKeys, keyBeg, keyEnd)
		}

	case "keyvalues":
		switch action {
		case "get":
			numKeys, writtenBytes, err := d.handleKeyValues(ctx, w, r, uuid, showFields(r))
			if err != nil {
				server.BadRequest(w, r, "GET /keyvalues on %d keys, data %q: %v", numKeys, d.DataName(), err)
				return
			}
			comment = fmt.Sprintf("HTTP GET keyvalues on %d keys, %d bytes, data %q", numKeys, writtenBytes, d.DataName())
		case "post":
			if err := d.handleIngest(ctx, r, uuid); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP POST keyvalues on data %q", d.DataName())
		default:
			server.BadRequest(w, r, "key endpoint does not support %q HTTP verb", action)
			return
		}

	case "key":
		if len(parts) < 5 {
			server.BadRequest(w, r, "expect key string to follow 'key' endpoint")
			return
		}
		keyStr := parts[4]

		switch action {
		case "head":
			found, err := d.KeyExists(ctx, keyStr)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if found {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return

		case "get":
			// Return value of single key
			value, found, err := d.GetData(ctx, keyStr, showFields(r))
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if !found {
				http.Error(w, fmt.Sprintf("Key %q not found", keyStr), http.StatusNotFound)
				return
			}
			if value != nil || len(value) > 0 {
				_, err = w.Write(value)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
			}
			comment = fmt.Sprintf("HTTP GET key %q of neuronjson %q: %d bytes (%s)", keyStr, d.DataName(), len(value), url)

		case "delete":
			if err := d.DeleteData(ctx, keyStr); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP DELETE data with key %q of neuronjson %q (%s)", keyStr, d.DataName(), url)

		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}

			go func() {
				msginfo := map[string]interface{}{
					"Action":    "postneuronjson",
					"Key":       keyStr,
					"Bytes":     len(data),
					"UUID":      string(uuid),
					"Timestamp": time.Now().String(),
				}
				jsonmsg, _ := json.Marshal(msginfo)
				if err = d.PublishKafkaMsg(jsonmsg); err != nil {
					dvid.Errorf("Error on sending neuronjson POST op to kafka: %v\n", err)
				}
			}()

			cond_fields := r.URL.Query().Get("conditionals")
			conditionals := strings.Split(cond_fields, ",")
			replace := r.URL.Query().Get("replace") == "true"

			err = d.PutData(ctx, keyStr, data, conditionals, replace)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP POST neuronjson '%s': %d bytes (%s)", d.DataName(), len(data), url)
		default:
			server.BadRequest(w, r, "key endpoint does not support %q HTTP verb", action)
			return
		}

	default:
		server.BadAPIRequest(w, r, d)
		return
	}

	timedLog.Infof(comment)
	return
}
