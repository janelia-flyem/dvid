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
	projectID, found, err := c.GetString("ProjectID")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("neuronjson instances must have ProjectID set in the configuration")
	}
	datasetID, found, err := c.GetString("DatasetID")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("neuronjson instances must have DatasetID set in the configuration")
	}
	return &Data{Data: basedata, ProjectID: projectID, DatasetID: datasetID}, nil
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

// Data embeds the datastore's Data and extends it with neuronjson properties.
type Data struct {
	*datastore.Data

	// Firestore ProjectID
	ProjectID string

	// Dataset to keep in-memory
	DatasetID string

	// Persistence for schema and schema_batch necessary for neuTu/neu3 clients.
	Schema      string
	SchemaBatch string

	// The in-memory dataset.
	db      map[uint64]map[string]interface{}
	ids     []uint64 // sorted list of body ids
	dbReady bool
	dbMu    sync.RWMutex
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties. Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-neuronjson data %q", src.DataName())
	}

	d.ProjectID = d2.ProjectID
	d.DatasetID = d2.DatasetID
	d.Schema = d2.Schema
	d.SchemaBatch = d2.SchemaBatch
	return nil
}

func (d *Data) Equals(d2 *Data) bool {
	return d.Data.Equals(d2.Data)
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended struct {
			ProjectID, DatasetID, Schema, SchemaBatch string
		}
	}{
		d.Data,
		struct {
			ProjectID, DatasetID, Schema, SchemaBatch string
		}{
			ProjectID:   d.ProjectID,
			DatasetID:   d.DatasetID,
			Schema:      d.Schema,
			SchemaBatch: d.SchemaBatch,
		},
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.ProjectID)); err != nil {
		return fmt.Errorf("decoding neuronjson %q: no ProjectID", d.DataName())
	}
	if err := dec.Decode(&(d.DatasetID)); err != nil {
		return fmt.Errorf("decoding neuronjson %q: no DatasetID", d.DataName())
	}
	if err := dec.Decode(&(d.Schema)); err != nil {
		return fmt.Errorf("decoding neuronjson %q: no Schema", d.DataName())
	}
	if err := dec.Decode(&(d.SchemaBatch)); err != nil {
		return fmt.Errorf("decoding neuronjson %q: no SchemaBatch", d.DataName())
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.ProjectID); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.DatasetID); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Schema); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.SchemaBatch); err != nil {
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

func (d *Data) loadFirestoreDB() {
	tlog := dvid.NewTimeLog()
	numdocs := 0

	ctx := context.Background()
	client, err := firestore.NewClient(ctx, d.ProjectID)
	if err != nil {
		dvid.Errorf("Could not connect to Firestore for project %q: %v\n", d.ProjectID, err)
		return
	}
	defer client.Close()
	it := client.Collection("clio_annotations_global").Doc("neurons").Collection(d.DatasetID).Where("_head", "==", true).Documents(ctx)

	d.dbMu.Lock() // Note that the mutex is NOT unlocked if firestore DB doesn't load.

	for {
		doc, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			dvid.Errorf("documents iterator error -- can't load Firebase for project %q, dataset %q: %v", d.ProjectID, d.DatasetID, err)
			return
		}
		annotation := doc.Data()
		bodyid, err := getBodyID(annotation)
		if err != nil {
			dvid.Errorf("error getting bodyID from annotation: %v\n", annotation)
			return
		}
		d.db[bodyid] = annotation
		d.ids = append(d.ids, bodyid) // assumes there are no duplicate bodyid among HEAD annotations
		numdocs++
		if numdocs%1000 == 0 {
			tlog.Infof("Loaded %d HEAD annotations", numdocs)
		}
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })

	tlog.Infof("Completed loading of %d HEAD annotations", numdocs)
	d.dbMu.Unlock()
	d.dbReady = true
}

// InitDataHandlers initializes ephemeral data for this instance, which is
// the in-memory keyvalue store where the values are neuron annotation JSON.
func (d *Data) InitDataHandlers() error {
	d.db = make(map[uint64]map[string]interface{})
	go d.loadFirestoreDB()
	return nil
}

func (d *Data) addBodyID(bodyid uint64) {
	i := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] >= bodyid })
	if i < len(d.ids) && d.ids[i] == bodyid {
		return
	}
	d.ids = append(d.ids, 0)
	copy(d.ids[i+1:], d.ids[i:])
	d.ids[i] = bodyid
}

func (d *Data) deleteBodyID(bodyid uint64) {
	i := sort.Search(len(d.ids), func(i int) bool { return d.ids[i] == bodyid })
	if i == len(d.ids) {
		return
	}
	d.ids = append(d.ids[:i], d.ids[i+1:]...)
}

func (d *Data) addRecord(data map[string]interface{}) error {
	bodyid, err := getBodyID(data)
	if err != nil {
		return err
	}
	d.dbMu.Lock()
	d.db[bodyid] = data
	d.addBodyID(bodyid)
	d.dbMu.Unlock()
	return nil
}

// KeyExists returns true if a key is found.
func (d *Data) KeyExists(keyStr string) (bool, error) {
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return false, err
	}
	d.dbMu.RLock()
	defer d.dbMu.RUnlock()
	_, found := d.db[bodyid]
	return found, nil
}

func (d *Data) GetKeysInRange(keyBeg, keyEnd string) (keys []string, err error) {
	var bodyidBeg, bodyidEnd uint64
	if bodyidBeg, err = parseKeyStr(keyBeg); err != nil {
		return
	}
	if bodyidEnd, err = parseKeyStr(keyEnd); err != nil {
		return
	}
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
	return
}

func (d *Data) GetAll() ([]map[string]interface{}, error) {
	d.dbMu.RLock()
	out := make([]map[string]interface{}, len(d.db))
	i := 0
	for _, data := range d.db {
		removeReservedFields(data)
		out[i] = data
		i++
	}
	d.dbMu.RUnlock()
	return out, nil
}

func (d *Data) GetKeys() ([]string, error) {
	keyStrings := make([]string, len(d.ids))
	d.dbMu.RLock()
	for i, bodyid := range d.ids {
		keyStrings[i] = strconv.FormatUint(bodyid, 10)
	}
	d.dbMu.RUnlock()
	return keyStrings, nil
}

// GetData gets a value using a key
func (d *Data) GetData(keyStr string) ([]byte, bool, error) {
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return nil, false, err
	}
	d.dbMu.RLock()
	defer d.dbMu.RUnlock()
	value, found := d.db[bodyid]
	if !found {
		return nil, false, nil
	}
	removeReservedFields(value)
	data, err := json.Marshal(value)
	return data, true, err
}

// PutData puts a key-value at a given uuid
func (d *Data) PutData(keyStr string, value []byte) error {
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return err
	}
	d.dbMu.Lock()
	defer d.dbMu.Unlock()
	var jsonData map[string]interface{}
	if err := json.Unmarshal(value, &jsonData); err != nil {
		return err
	}
	d.db[bodyid] = jsonData
	d.addBodyID(bodyid)
	return nil
}

// DeleteData deletes a key-value pair
func (d *Data) DeleteData(keyStr string) error {
	bodyid, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		return err
	}
	d.dbMu.Lock()
	defer d.dbMu.Unlock()
	_, found := d.db[bodyid]
	if found {
		delete(d.db, bodyid)
		d.deleteBodyID(bodyid)
	}
	return nil
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
	if err = d.PutData(keyStr, cmd.Input); err != nil {
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

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprint(helpMessage)
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "put":
		return d.put(request, reply)
	default:
		return fmt.Errorf("unknown command.  Data '%s' [%s] does not support '%s' command",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
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

	// abort requests if database wasn't able to be initialized.
	switch parts[3] {
	case "help", "info", "tags":
		// can operate without database
	default:
		if !d.dbReady {
			server.BadRequest(w, r, "firestore not available, see logs")
			return
		}
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
		switch action {
		case "head":
			if d.Schema != "" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return

		case "get":
			value := []byte(d.Schema)
			if _, err := w.Write(value); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			comment = fmt.Sprintf("HTTP GET schema of neuronjson %q: %d bytes (%s)", d.DataName(), len(value), url)

		case "delete":
			d.Schema = ""
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP DELETE schema of neuronjson %q (%s)", d.DataName(), url)

		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			d.Schema = string(data)
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP POST schema neuronjson '%s': %d bytes (%s)", d.DataName(), len(data), url)
		default:
			server.BadRequest(w, r, "key endpoint does not support %q HTTP verb", action)
			return
		}

	case "schema_batch":
		switch action {
		case "head":
			if d.SchemaBatch != "" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return

		case "get":
			value := []byte(d.SchemaBatch)
			if _, err := w.Write(value); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			comment = fmt.Sprintf("HTTP GET schema_batch of neuronjson %q: %d bytes (%s)", d.DataName(), len(value), url)

		case "delete":
			d.SchemaBatch = ""
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP DELETE schema_batch of neuronjson %q (%s)", d.DataName(), url)

		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			d.SchemaBatch = string(data)
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			comment = fmt.Sprintf("HTTP POST schema_batch neuronjson '%s': %d bytes (%s)", d.DataName(), len(data), url)
		default:
			server.BadRequest(w, r, "key endpoint does not support %q HTTP verb", action)
			return
		}

	case "all":
		kvList, err := d.GetAll()
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
		keyList, err := d.GetKeys()
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

	case "keyrange":
		if len(parts) < 6 {
			server.BadRequest(w, r, "expect beginning and end keys to follow 'keyrange' endpoint")
			return
		}

		// Return JSON list of keys
		keyBeg := parts[4]
		keyEnd := parts[5]
		keyList, err := d.GetKeysInRange(keyBeg, keyEnd)
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
		numKeys, err := d.sendJSONValuesInRange(w, r, keyBeg, keyEnd)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		comment = fmt.Sprintf("HTTP GET keyrangevalues sent %d values for [%q, %q]", numKeys, keyBeg, keyEnd)

	case "keyvalues":
		switch action {
		case "get":
			numKeys, writtenBytes, err := d.handleKeyValues(w, r, uuid)
			if err != nil {
				server.BadRequest(w, r, "GET /keyvalues on %d keys, data %q: %v", numKeys, d.DataName(), err)
				return
			}
			comment = fmt.Sprintf("HTTP GET keyvalues on %d keys, %d bytes, data %q", numKeys, writtenBytes, d.DataName())
		case "post":
			if err := d.handleIngest(r, uuid); err != nil {
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
			found, err := d.KeyExists(keyStr)
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
			value, found, err := d.GetData(keyStr)
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
			if err := d.DeleteData(keyStr); err != nil {
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
				if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
					dvid.Errorf("Error on sending neuronjson POST op to kafka: %v\n", err)
				}
			}()

			err = d.PutData(keyStr, data)
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

func (d *Data) sendJSONValuesInRange(w http.ResponseWriter, r *http.Request, keyBeg, keyEnd string) (numKeys int, err error) {
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

func (d *Data) sendJSONKV(w http.ResponseWriter, keys []string, checkVal bool) (writtenBytes int, err error) {
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
		if val, found, err = d.GetData(key); err != nil {
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

func (d *Data) sendTarKV(w http.ResponseWriter, keys []string) (writtenBytes int, err error) {
	var n int
	w.Header().Set("Content-type", "application/tar")
	tw := tar.NewWriter(w)
	for _, key := range keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(key); err != nil {
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

func (d *Data) sendProtobufKV(w http.ResponseWriter, keys Keys) (writtenBytes int, err error) {
	var kvs KeyValues
	kvs.Kvs = make([]*KeyValue, len(keys.Keys))
	for i, key := range keys.Keys {
		var val []byte
		var found bool
		if val, found, err = d.GetData(key); err != nil {
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

func (d *Data) handleKeyValues(w http.ResponseWriter, r *http.Request, uuid dvid.UUID) (numKeys, writtenBytes int, err error) {
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
		writtenBytes, err = d.sendTarKV(w, keys)
	case jsonOut:
		var keys []string
		if err = json.Unmarshal(data, &keys); err != nil {
			return
		}
		numKeys = len(keys)
		writtenBytes, err = d.sendJSONKV(w, keys, checkVal)
	default:
		var keys Keys
		if err = keys.Unmarshal(data); err != nil {
			return
		}
		numKeys = len(keys.Keys)
		writtenBytes, err = d.sendProtobufKV(w, keys)
	}
	return
}

func (d *Data) handleIngest(r *http.Request, uuid dvid.UUID) error {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	var kvs KeyValues
	if err := kvs.Unmarshal(data); err != nil {
		return err
	}
	for _, kv := range kvs.Kvs {
		err = d.PutData(kv.Key, kv.Value)
		if err != nil {
			return err
		}

		msginfo := map[string]interface{}{
			"Action":    "postkv",
			"Key":       kv.Key,
			"Bytes":     len(kv.Value),
			"UUID":      string(uuid),
			"Timestamp": time.Now().String(),
		}
		jsonmsg, _ := json.Marshal(msginfo)
		if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("Error on sending neuronjson POST op to kafka: %v\n", err)
		}
	}
	return nil
}
