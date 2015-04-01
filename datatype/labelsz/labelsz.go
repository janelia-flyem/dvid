/*
	Package labelsz supports size queries for labels.  It is updated after
	changes are made to its linked labelvol instance.
*/
package labelsz

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labelvol"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelsz"
	TypeName = "labelsz"
)

const HelpMessage = `
API for label size queries (github.com/janelia-flyem/dvid/datatype/labelsz)
===========================================================================


Command-line:

$ dvid repo <UUID> new labelsz <data name> source=<linked labelvol name>

	Adds labelsz queries on the linked label sparse volume data.

	Example:

	$ dvid repo 3f8c new labelsz bodysizes source=bodyvols

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "bodysizes"
	
    Configuration Settings (case-insensitive keys)

    Source         Name of preexisting labelvol data (required)
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/bodysizes/info

    Returns JSON with configuration settings that include location in DVID space and
    min/max block indices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelsz data.


GET <api URL>/node/<UUID>/<data name>/size/<label>

    Returns JSON of the # voxels for the given label:

    {
    	"Label": 23,
    	"Voxels": 48399
    }
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    label         The 64-bit unsigned int label

GET <api URL>/node/<UUID>/<data name>/sizerange/<min size>/<optional max size>

    Returns JSON list of labels that have # voxels that fall within the given range
    of sizes.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    min size      Minimum # of voxels.
    max size      Optional maximum # of voxels.  If not specified, all labels with volume above minimum
                   are returned.

`

var (
	dtype *Type
)

func init() {
	dtype = new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.Register(dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// --- Labelsurf Datatype -----

// Type just uses voxels data type by composition.
type Type struct {
	datastore.Type
}

// NewData returns a pointer to labelvol data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	// Make sure we have a valid labelvol source
	s, found, err := c.GetString("Source")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make labelsz data without valid 'Source' specifying an associated labelvol.")
	}
	srcname := dvid.InstanceName(s)
	if _, err = labelvol.GetByUUID(uuid, srcname); err != nil {
		return nil, err
	}
	c.Set("sync", s) // This will set base data sync list

	// Initialize the Data for this data type
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	return &Data{basedata, srcname}, nil
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// -------

// GetByUUID returns a pointer to labelsz data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelsz datatype!", name)
	}
	return data, nil
}

// Data of labelsz holds information necessary to construct keys for surface data.
type Data struct {
	*datastore.Data

	// Source is the name of the linked labelvol from which it gets label data
	Source dvid.InstanceName
}

// GetSize returns the size in voxels of the given label.
func (d *Data) GetSize(v dvid.VersionID, label uint64) (uint64, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return 0, fmt.Errorf("Data type imagesz had error initializing store: %s\n", err.Error())
	}

	// Get the start/end keys for the label.
	firstKey := NewLabelSizeTKey(label, 0)
	lastKey := NewLabelSizeTKey(label, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	ctx := datastore.NewVersionedContext(d, v)
	keys, err := store.KeysInRange(ctx, firstKey, lastKey)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, fmt.Errorf("found no size for label %d", label)
	}
	if len(keys) > 1 {
		return 0, fmt.Errorf("found %d sizes for label %d!", len(keys), label)
	}
	_, size, err := DecodeLabelSizeTKey(keys[0])
	return size, err
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
// If maxSize is 0, all mapped labels are returned >= minSize.
func (d *Data) GetSizeRange(v dvid.VersionID, minSize, maxSize uint64) (string, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return "{}", fmt.Errorf("Data type imagesz had error initializing store: %s\n", err.Error())
	}

	// Get the start/end keys for the size range.
	firstKey := NewSizeLabelTKey(minSize, 0)
	var upperBound uint64
	if maxSize != 0 {
		upperBound = maxSize
	} else {
		upperBound = math.MaxUint64
	}
	lastKey := NewSizeLabelTKey(upperBound, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	ctx := datastore.NewVersionedContext(d, v)
	keys, err := store.KeysInRange(ctx, firstKey, lastKey)
	if err != nil {
		return "{}", err
	}

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		labels[i], _, err = DecodeSizeLabelTKey(key)
		if err != nil {
			return "{}", err
		}
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// --- datastore.DataService interface ---------

func (d *Data) Help() string {
	return HelpMessage
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("labelsz.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	return nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	_, versions, err := datastore.FromContext(ctx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n",
			d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}

	// Check the action
	action := strings.ToLower(r.Method)
	if action != "get" {
		server.BadRequest(w, r, "labelsz data can only accept GET HTTP requests")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "Incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, dtype.Help())

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "size":
		// GET <api URL>/node/<UUID>/<data name>/size/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'size' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		size, err := d.GetSize(versionID, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, "{%q: %d, %q: %d}", "Label", label, "Voxels", size)
		timedLog.Infof("HTTP %s: get label %d size", r.Method, label)

	case "sizerange":
		// GET <api URL>/node/<UUID>/<data name>/sizerange/<min size>/<optional max size>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires at least the minimum size to follow 'sizerange' command")
			return
		}
		minSize, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		var maxSize uint64
		if len(parts) >= 6 {
			maxSize, err = strconv.ParseUint(parts[5], 10, 64)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
		}
		jsonStr, err := d.GetSizeRange(versionID, minSize, maxSize)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		timedLog.Infof("HTTP %s: get labels with volume > %d and < %d (%s)", r.Method, minSize, maxSize, r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call '%s' for labelsz data '%s'.  See API help.",
			parts[3], d.DataName())
	}
}
