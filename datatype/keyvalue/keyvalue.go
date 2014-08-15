/*
	Package keyvalue implements DVID support for data using generic key-value.
*/
package keyvalue

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoUrl  = "github.com/janelia-flyem/dvid/datatype/keyvalue"
	TypeName = "keyvalue"
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

$ dvid node <UUID> <data name> mount <directory>

	Creates a FUSE file system at given mount directory.  Each version will have
	a separate directory with the UUID as name.  Reading and writing files in this
	directory will be the same as reading and writing keyvalue data to DVID.
	
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
	t := NewDatatype()
	t.DatatypeID = &datastore.DatatypeID{
		Name:    TypeName,
		Url:     RepoUrl,
		Version: Version,
	}
	datastore.Register(t)

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

// NewDatatype returns a pointer to a new keyvalue Datatype with default values set.
func NewDatatype() *Datatype {
	dtype := new(Datatype)
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return dtype
}

// --- TypeService interface ---

// NewData returns a pointer to new keyvalue data with default values.
func (dtype *Datatype) NewDataService(r datastore.Repo, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, r, id, name, c)
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

func (d *Data) GetKeysInRange(ctx storage.Context, keyBeg, keyEnd string) ([]string, error) {
	db, err := storage.BigDataStore()
	if err != nil {
		return nil, err
	}
	// Compute first and last key for range
	first := dvid.IndexString(keyBeg)
	last := dvid.IndexString(keyEnd)
	keys, err := db.KeysInRange(ctx, first.Bytes(), last.Bytes())
	if err != nil {
		return nil, err
	}
	keyList := []string{}
	for _, key := range keys {
		index, err := ctx.IndexFromKey(key)
		if err != nil {
			return nil, err
		}
		keyList = append(keyList, string(index))
	}
	return keyList, nil
}

// GetData gets a value using a key
func (d *Data) GetData(ctx storage.Context, keyStr string) ([]byte, bool, error) {
	db, err := storage.BigDataStore()
	if err != nil {
		return nil, false, err
	}
	index := dvid.IndexString(keyStr)
	data, err := db.Get(ctx, index.Bytes())
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
	index := dvid.IndexString(keyStr)
	return db.Put(ctx, index.Bytes(), serialization)
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

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "mount":
		return d.Mount(request, reply)
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
