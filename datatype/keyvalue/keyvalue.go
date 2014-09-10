/*
	Package keyvalue implements DVID support for data using generic key-value.
*/
package keyvalue

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/keyvalue"
	TypeName = "keyvalue"

	DefaultMaxKeySize = 20
)

const HelpMessage = `
API for 'keyvalue' datatype (github.com/janelia-flyem/dvid/datatype/keyvalue)
=============================================================================

Command-line:

$ dvid repo <UUID> new keyvalue <data name> <settings...>

	Adds newly named key-value data to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new keyvalue stuff

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "myblobs"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    MaxKeySize     Maximum size of keys in terms of characters.  Default is 20.

$ dvid node <UUID> <data name> mount <directory>

	Creates a FUSE file system at given mount directory.  Each version will have
	a separate directory with the UUID as name.  Reading and writing files in this
	directory will be the same as reading and writing keyvalue data to DVID.
	
$ dvid -stdin node <UUID> <data name> put <key> < data

	Puts stdin data into the keyvalue data instance under the given key.

	
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

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET  <api URL>/node/<UUID>/<data name>/<key>[/<key2>]
POST <api URL>/node/<UUID>/<data name>/<key>
DEL  <api URL>/node/<UUID>/<data name>/<key>  (TO DO)

    Performs operations on a key-value pair depending on the HTTP verb.

    Example: 

    GET <api URL>/node/3f8c/stuff/mykey

    Returns the data associated with the key "mykey" of the data "stuff" in version
    node 3f8c.

    The "Content-type" of the HTTP response (and usually the request) are
    "application/octet-stream" for arbitrary binary data.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add/retrieve.
    key           An alphanumeric key.
    key2          If given, return value is a JSON list of all keys between 'key' and 'key2'
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// Type embeds the datastore's Type to create a unique type for keyvalue functions.
type Type struct {
	datastore.Type
}

// NewType returns a pointer to a new keyvalue Type with default values set.
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

// NewData returns a pointer to new keyvalue data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	maxKeySize, found, err := c.GetInt("MaxKeySize")
	if err != nil {
		return nil, err
	}
	if !found {
		maxKeySize = DefaultMaxKeySize
	}
	return &Data{basedata, Properties{maxKeySize}}, nil
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage)
}

type indexT []byte

// Removes all bytes at first 0.
func (i indexT) String() string {
	if len(i) == 0 {
		return ""
	}
	n := bytes.Index(i, []byte{0})
	if n < 0 {
		return string(i)
	}
	return string(i[:n])
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	MaxKeySize int
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data
	Properties
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
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
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Data) getIndex(key string) (indexT, error) {
	if !d.Versioned() {
		return []byte(key), nil
	}
	if len(key) > d.MaxKeySize {
		return nil, fmt.Errorf("Key %q is too long.  Data instance %q is set to max key size of %d",
			key, d.DataName(), d.MaxKeySize)
	}
	index := make(indexT, d.MaxKeySize, d.MaxKeySize)
	copy(index[0:len(key)], []byte(key))
	return index, nil
}

func (d *Data) GetKeysInRange(ctx storage.Context, keyBeg, keyEnd string) ([]string, error) {
	db, err := storage.BigDataStore()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Getting keys in range %s -> %s\n", keyBeg, keyEnd)
	// Compute first and last key for range
	first, err := d.getIndex(keyBeg)
	if err != nil {
		return nil, err
	}
	last, err := d.getIndex(keyEnd)
	if err != nil {
		return nil, err
	}
	keys, err := db.KeysInRange(ctx, []byte(first), []byte(last))
	if err != nil {
		return nil, err
	}
	var keyString string
	keyList := []string{}
	for _, key := range keys {
		index, err := ctx.IndexFromKey(key)
		if err != nil {
			return nil, err
		}
		if d.Versioned() {
			keyString = indexT(index).String()
		} else {
			keyString = string(index)
		}
		keyList = append(keyList, keyString)
	}
	return keyList, nil
}

// GetData gets a value using a key
func (d *Data) GetData(ctx storage.Context, keyStr string) ([]byte, bool, error) {
	db, err := storage.BigDataStore()
	if err != nil {
		return nil, false, err
	}
	index, err := d.getIndex(keyStr)
	if err != nil {
		return nil, false, err
	}
	data, err := db.Get(ctx, []byte(index))
	if err != nil {
		return nil, false, fmt.Errorf("Error in retrieving key '%s': %s", keyStr, err.Error())
	}
	if data == nil {
		return nil, false, nil
	}
	uncompress := true
	value, _, err := dvid.DeserializeData(data, uncompress)
	if err != nil {
		return nil, false, fmt.Errorf("Unable to deserialize data for key '%s': %s\n", keyStr, err.Error())
	}
	return value, true, nil
}

// PutData puts a key-value at a given uuid
func (d *Data) PutData(ctx storage.Context, keyStr string, value []byte) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	serialization, err := dvid.SerializeData(value, d.Compression(), d.Checksum())
	if err != nil {
		return fmt.Errorf("Unable to serialize data: %s\n", err.Error())
	}
	index, err := d.getIndex(keyStr)
	if err != nil {
		return err
	}
	return db.Put(ctx, []byte(index), serialization)
}

// put handles a PUT command-line request.
func (d *Data) put(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("The key name must be specified after 'put'")
	}
	if len(cmd.Input) == 0 {
		return fmt.Errorf("No data was passed into standard input")
	}
	var uuidStr, dataName, cmdStr, keyStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &keyStr)

	// Get repo.
	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Store data
	ctx := datastore.NewVersionedContext(d, versionID)
	if err = d.PutData(ctx, keyStr, cmd.Input); err != nil {
		return fmt.Errorf("Error on put to key %q for keyvalue %q: %s\n", keyStr, d.DataName(),
			err.Error())
	}

	reply.Output = []byte(fmt.Sprintf("Put %d bytes into key %q for keyvalue %q, uuid %s\n",
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
	return fmt.Sprintf(HelpMessage)
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.  For the keyvalue data type, ROI delimiting
// is not available.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("keyvalue.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "mount":
		return d.mount(request, reply)
	case "put":
		return d.put(request, reply)
	default:
		return fmt.Errorf("Unknown command.  Data '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	return nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	_, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n", d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

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

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return
	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return
	default:
	}

	// Get the key and process request
	var comment string
	keyStr := parts[3]
	switch strings.ToLower(r.Method) {
	case "get":
		// TODO: Use one endpoint for now.  Should provide different endpoints:
		// <name>/value/<key>                  (returns value for a single key)
		// <name>/keys?min=begKey&max=endKey   (returns keys between endpoints)
		if len(parts) > 4 {
			// Return JSON list of keys
			keyBeg := keyStr
			keyEnd := parts[4]
			fmt.Printf("get key range: %s\n", url)
			keyList, err := d.GetKeysInRange(storeCtx, keyBeg, keyEnd)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			jsonBytes, err := json.Marshal(keyList)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
		} else {
			fmt.Printf("get single key: %s\n", url)
			// Return value of single key
			value, found, err := d.GetData(storeCtx, keyStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if !found {
				http.Error(w, fmt.Sprintf("Key '%s' not found", keyStr), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, err = w.Write(value)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			comment = fmt.Sprintf("HTTP GET keyvalue '%s': %d bytes (%s)\n", d.DataName(), len(value), url)
		}
	case "post":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		err = d.PutData(storeCtx, keyStr, data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		comment = fmt.Sprintf("HTTP POST keyvalue '%s': %d bytes (%s)\n", d.DataName(), len(data), url)
	default:
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	timedLog.Infof(comment)
}
