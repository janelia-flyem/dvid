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
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/janelia-flyem/dvid/datastore"
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
			   	
GET  <api URL>/node/<UUID>/<data name>/all

	Returns a list of all JSON annotations

			
GET  <api URL>/node/<UUID>/<data name>/keys

	Returns all keys for this data instance in JSON format:

	[key1, key2, ...]

GET  <api URL>/node/<UUID>/<data name>/fields

	Returns all field names in annotations for the most recent version:

	["field1", "field2", ...]

GET    <api URL>/node/<UUID>/<data name>/schema
HEAD   <api URL>/node/<UUID>/<data name>/schema
POST   <api URL>/node/<UUID>/<data name>/schema
DELETE <api URL>/node/<UUID>/<data name>/schema

	Handles the schema JSON associated with this neuronjson instance based on the HTTP verb.

GET    <api URL>/node/<UUID>/<data name>/schema_batch
HEAD   <api URL>/node/<UUID>/<data name>/schema_batch
POST   <api URL>/node/<UUID>/<data name>/schema_batch
DELETE <api URL>/node/<UUID>/<data name>/schema_batch
	
	Handles the schema batch JSON associated with this neuronjson instance based on the HTTP verb.

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

	GET Query-string Options (only one of these allowed):

	json        If set to "true", the response will be JSON as above and the values must
				  be valid JSON or an error will be returned.
	tar			If set to "true", the response will be a tarfile with keys as file names.
	protobuf	Default, or can be set to "true". Response will be protobuf KeyValues response

	Additional query option:

	check		If json=true, setting check=false will tell server to trust that the
				  values will be valid JSON instead of parsing it as a check.


GET  <api URL>/node/<UUID>/<data name>/key/<key>
POST <api URL>/node/<UUID>/<data name>/key/<key>
DEL  <api URL>/node/<UUID>/<data name>/key/<key> 
HEAD <api URL>/node/<UUID>/<data name>/key/<key> 

	Performs operations on a key-value pair depending on the HTTP verb.  

	Example: 

	GET <api URL>/node/3f8c/stuff/key/myfile.dat

	Returns the data associated with the key "myfile.dat" of instance "stuff" in version
	node 3f8c.

	The "Content-type" of the HTTP response (and usually the request) are
	"application/octet-stream" for arbitrary binary data.

	For HEAD returns:
	200 (OK) if a sparse volume of the given label exists within any optional bounds.
	404 (File not Found) if there is no sparse volume for the given label within any optional bounds.

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key           An alphanumeric key.
	
	POSTs will be logged as a Kafka JSON message with the following format:
	{ 
		"Action": "postkv",
		"Key": <key>,
		"Bytes": <number of bytes in data>,
		"UUID": <UUID on which POST was done>
	}

GET <api URL>/node/<UUID>/<data name>/keyvalues[?jsontar=true]
POST <api URL>/node/<UUID>/<data name>/keyvalues

	Allows batch query or ingest of data. 

	neuronjson data needs to be serialized in a format defined by the following protobuf3 definitions:

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
	
	For GET, the query body must include a Keys serialization and a KeyValues serialization is
	returned.

	For POST, the query body must include a KeyValues serialization.
	
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

	GET Query-string Options (only one of these allowed):

	json        If true, returns JSON
	jsontar		If set to any value for GET, query body must be JSON array of string keys
				and the returned data will be a tarfile with keys as file names.

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

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of neuronjson data instance.
	key1          Lexicographically lowest alphanumeric key in range.
	key2          Lexicographically highest alphanumeric key in range.

	GET Query-string Options (only one of these allowed):

	json        If set to "true", the response will be JSON as above and the values must
					be valid JSON or an error will be returned.
	tar			If set to "true", the response will be a tarfile with keys as file names.
	protobuf	If set to "true", the response will be protobuf KeyValues response

	check		If json=true, setting check=false will tell server to trust that the
					values will be valid JSON instead of parsing it as a check.
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(map[string]interface{}{})
}

// Type embeds the datastore's Type to create a unique type for neuronjson functions.
type Type struct {
	datastore.Type
}

// NewType returns a pointer to a new neuronjson Type with default values set.
func NewType() *Type {
	dtype := new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
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

///// Persistence of neuronjson data

// getData gets a map value using a key
func (d *Data) getData(ctx storage.Context, keyStr string) (value map[string]interface{}, found bool, err error) {
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
func (d *Data) putData(ctx storage.Context, keyStr string, value map[string]interface{}) error {
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
		uncompress := true
		data, _, err := dvid.DeserializeData(kv.V, uncompress)
		if err != nil {
			return fmt.Errorf("unable to deserialize data for key %q: %v", key, err)
		}
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		var value map[string]interface{}
		if err = dec.Decode(&value); err != nil {
			return err
		}
		f(key, value)
		return nil
	})
	return err
}

////////////

// Data embeds the datastore's Data and extends it with neuronjson properties.
type Data struct {
	*datastore.Data

	// The in-memory dataset.
	db     map[uint64]map[string]interface{}
	ids    []uint64            // sorted list of body ids
	fields map[string]struct{} // list of all fields among the annotations
	dbMu   sync.RWMutex
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
func removeReservedFields(data map[string]interface{}) {
	for field := range data {
		if len(field) > 0 && field[0] == '_' {
			delete(data, field)
		}
	}
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

// --- DataInitializer interface ---

// InitDataHandlers initializes ephemeral data for this instance, which is
// the in-memory keyvalue store where the values are neuron annotation JSON.
func (d *Data) InitDataHandlers() error {
	d.db = make(map[uint64]map[string]interface{})
	return nil
}

func (d *Data) loadFirestoreDB(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 6 {
		return fmt.Errorf("ProjectID and DatasetID must be specified after %q", "ingest")
	}
	var uuidStr, dataName, cmdStr, projectID, datasetID string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &projectID, &datasetID)

	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("unable to get keyvalue database: %v", err)
	}
	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	vctx := datastore.NewVersionedCtx(d, versionID)

	tlog := dvid.NewTimeLog()
	numdocs := 0

	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("could not connect to Firestore for project %q: %v", projectID, err)
	}
	defer client.Close()
	it := client.Collection("clio_annotations_global").Doc("neurons").Collection(datasetID).Where("_head", "==", true).Documents(ctx)

	d.dbMu.Lock() // Note that the mutex is NOT unlocked if firestore DB doesn't load.
	d.db = make(map[uint64]map[string]interface{})
	d.fields = make(map[string]struct{})
	for {
		doc, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("documents iterator error -- can't load Firebase for project %q, dataset %q: %v", projectID, datasetID, err)
		}
		annotation := doc.Data()
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
		if err := db.Put(vctx, tk, data); err != nil {
			return err
		}

		numdocs++
		if numdocs%1000 == 0 {
			tlog.Infof("Loaded %d HEAD annotations", numdocs)
		}
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })

	tlog.Infof("Completed loading of %d HEAD annotations", numdocs)
	d.dbMu.Unlock()

	reply.Output = []byte(fmt.Sprintf("Ingested Firestore annotations from project %q, dataset %q into neuronjson instance %q, uuid %s\n",
		projectID, datasetID, d.DataName(), uuidStr))
	return nil
}

func getSchemaKey(batch bool) (tkey storage.TKey, err error) {
	if batch {
		if tkey, err = NewSchemaBatchTKey(); err != nil {
			return
		}
	} else {
		if tkey, err = NewSchemaTKey(); err != nil {
			return
		}
	}
	return tkey, nil
}

func (d *Data) getSchema(ctx storage.VersionedCtx, batch bool) (val []byte, err error) {
	var tkey storage.TKey
	if tkey, err = getSchemaKey(batch); err != nil {
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

func (d *Data) putSchema(ctx storage.VersionedCtx, val []byte, batch bool) (err error) {
	var tkey storage.TKey
	if tkey, err = getSchemaKey(batch); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	return db.Put(ctx, tkey, val)
}

func (d *Data) schemaExists(ctx storage.VersionedCtx, batch bool) (exists bool, err error) {
	var tkey storage.TKey
	if tkey, err = getSchemaKey(batch); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	return db.Exists(ctx, tkey)
}

func (d *Data) deleteSchema(ctx storage.VersionedCtx, batch bool) (err error) {
	var tkey storage.TKey
	if tkey, err = getSchemaKey(batch); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	return db.Delete(ctx, tkey)
}

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

func (d *Data) GetAll(ctx storage.VersionedCtx) ([]map[string]interface{}, error) {
	var out []map[string]interface{}
	if ctx.Head() {
		d.dbMu.RLock()
		out = make([]map[string]interface{}, len(d.db)) // use cur size as starting size
		i := 0
		for _, value := range d.db {
			removeReservedFields(value)
			out[i] = value
			i++
		}
		d.dbMu.RUnlock()
	} else {
		process_func := func(key string, value map[string]interface{}) {
			out = append(out, value)
		}
		if err := d.processRange(ctx, process_func); err != nil {
			return nil, err
		}
	}
	return out, nil
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
func (d *Data) GetData(ctx storage.VersionedCtx, keyStr string) ([]byte, bool, error) {
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
	removeReservedFields(value)
	data, err := json.Marshal(value)
	return data, true, err
}

// PutData puts a byte value into key at a given uuid
func (d *Data) PutData(ctx storage.VersionedCtx, keyStr string, value []byte) error {
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return err
	}
	var jsonData map[string]interface{}
	if err := json.Unmarshal(value, &jsonData); err != nil {
		return err
	}
	if ctx.Head() {
		d.dbMu.Lock()
		defer d.dbMu.Unlock()
		d.db[bodyid] = jsonData
		d.addBodyID(bodyid)
	}
	return d.putData(ctx, keyStr, jsonData)
}

// DeleteData deletes a key-value pair
func (d *Data) DeleteData(ctx storage.VersionedCtx, keyStr string) error {
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
	if err = d.PutData(ctx, keyStr, cmd.Input); err != nil {
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

func (d *Data) sendJSONValuesInRange(ctx storage.VersionedCtx, w http.ResponseWriter, r *http.Request, keyBeg, keyEnd string) (numKeys int, err error) {
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
	var kvs KeyValues
	var wroteVal bool
	for i := begI; i < endI; i++ {
		bodyid := d.ids[i]
		jsonData, ok := d.db[bodyid]
		if !ok {
			dvid.Errorf("inconsistent neuronjson DB: bodyid %d at pos %d is not in db cache... skipping", bodyid, i)
			continue
		}
		key := strconv.FormatUint(bodyid, 10)
		var jsonBytes []byte
		jsonBytes, err = json.Marshal(jsonData)
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
			kv := &KeyValue{
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
		if serialization, err = kvs.Marshal(); err != nil {
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if _, err = w.Write(serialization); err != nil {
			return
		}
	}
	return
}

func (d *Data) sendJSONKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys []string, checkVal bool) (writtenBytes int, err error) {
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
		if val, found, err = d.GetData(ctx, key); err != nil {
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

func (d *Data) sendTarKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys []string) (writtenBytes int, err error) {
	var n int
	w.Header().Set("Content-type", "application/tar")
	tw := tar.NewWriter(w)
	for _, key := range keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(ctx, key); err != nil {
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

func (d *Data) sendProtobufKV(ctx storage.VersionedCtx, w http.ResponseWriter, keys Keys) (writtenBytes int, err error) {
	var kvs KeyValues
	kvs.Kvs = make([]*KeyValue, len(keys.Keys))
	for i, key := range keys.Keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(ctx, key); err != nil {
			return
		}
		if !found {
			val = nil
		}
		kvs.Kvs[i] = &KeyValue{
			Key:   key,
			Value: val,
		}
	}
	var serialization []byte
	if serialization, err = kvs.Marshal(); err != nil {
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

func (d *Data) handleKeyValues(ctx storage.VersionedCtx, w http.ResponseWriter, r *http.Request, uuid dvid.UUID) (numKeys, writtenBytes int, err error) {
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
		writtenBytes, err = d.sendTarKV(ctx, w, keys)
	case jsonOut:
		var keys []string
		if err = json.Unmarshal(data, &keys); err != nil {
			return
		}
		numKeys = len(keys)
		writtenBytes, err = d.sendJSONKV(ctx, w, keys, checkVal)
	default:
		var keys Keys
		if err = keys.Unmarshal(data); err != nil {
			return
		}
		numKeys = len(keys.Keys)
		writtenBytes, err = d.sendProtobufKV(ctx, w, keys)
	}
	return
}

func (d *Data) handleIngest(ctx storage.VersionedCtx, r *http.Request, uuid dvid.UUID) error {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	var kvs KeyValues
	if err := kvs.Unmarshal(data); err != nil {
		return err
	}
	for _, kv := range kvs.Kvs {
		err = d.PutData(ctx, kv.Key, kv.Value)
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
	case "ingest":
		return d.loadFirestoreDB(request, reply)
	default:
		return fmt.Errorf("unknown command.  Data '%s' [%s] does not support '%s' command",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
}

func (d *Data) handleSchema(ctx storage.VersionedCtx, w http.ResponseWriter, r *http.Request, uuid dvid.UUID, action string, batch bool) error {
	switch action {
	case "head":
		found, err := d.schemaExists(ctx, batch)
		if err != nil {
			return err
		}
		if found {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

	case "get":
		value, err := d.getSchema(ctx, batch)
		if err != nil {
			return err
		} else if _, err := w.Write(value); err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")

	case "delete":
		if err := d.deleteSchema(ctx, batch); err != nil {
			return err
		}

	case "post":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}
		if err := d.putSchema(ctx, data, batch); err != nil {
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

	case "schema":
		if err := d.handleSchema(ctx, w, r, uuid, action, false); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "schema_batch":
		if err := d.handleSchema(ctx, w, r, uuid, action, true); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "all":
		kvList, err := d.GetAll(ctx)
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
		numKeys, err := d.sendJSONValuesInRange(ctx, w, r, keyBeg, keyEnd)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		comment = fmt.Sprintf("HTTP GET keyrangevalues sent %d values for [%q, %q]", numKeys, keyBeg, keyEnd)

	case "keyvalues":
		switch action {
		case "get":
			numKeys, writtenBytes, err := d.handleKeyValues(ctx, w, r, uuid)
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
			value, found, err := d.GetData(ctx, keyStr)
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

			err = d.PutData(ctx, keyStr, data)
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
