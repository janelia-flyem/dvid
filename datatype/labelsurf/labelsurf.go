/*
	Package labelsurf supports surfaces for a label.  It can be synced with
	labelblk and is a different view of 64-bit label data.
*/
package labelsurf

import (
	"encoding/gob"
	"fmt"
	"net/http"
	"strconv"
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
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelsurf"
	TypeName = "labelsurf"
)

const HelpMessage = `
API for label surface data type (github.com/janelia-flyem/dvid/datatype/labelsurf)
==================================================================================

Note: Denormalizations like surfaces are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labelsurf <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new labelsurf surfaces

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "surfaces"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Source         Name of associated labelvol data (required)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")
	
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/grayscale/info

    Returns JSON with configuration settings that include location in DVID space and
    min/max block indices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET <api URL>/node/<UUID>/<data name>/surface/<label>

	Returns array of vertices and normals of surface voxels of given label.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    uint32          # Voxels
	    N x float32     Vertices where N = 3 * (# Voxels)
	    N x float32     Normals where N = 3 * (# Voxels)


GET <api URL>/node/<UUID>/<data name>/surface-by-point/<coord>

	Returns array of vertices and normals of surface voxels for label at given voxel.
	The encoding is described in the "surface" request above.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30
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

type Type struct {
	datastore.Type
}

// NewData returns a pointer to label surface data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	return nil, fmt.Errorf("DVID version does not support labelsurf at this time.")
	/*
		// Make sure we have a valid DataService source
		sourcename, found, err := c.GetString("Source")
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("Cannot make labelsurf data without valid 'Source' specifying an associated labelvol.")
		}
		var data datastore.DataService
		data, err = datastore.GetDataByUUID(uuid, dvid.InstanceName(sourcename))
		if err != nil {
			return nil, err
		}
		dtype := data.GetType()
		if dtype.GetTypeName() != "labelvol" {
			return nil, fmt.Errorf("Source data must be of type 'labelvol', not %s", dtype.GetTypeName())
		}

		// Initialize the Data for this data type
		basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
		if err != nil {
			return nil, err
		}
		return &Data{basedata, dvid.DataName(sourcename)}, nil
	*/
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// -------

// GetByUUID returns a pointer to labelsurf data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelsurf datatype!", name)
	}
	return data, nil
}

// Data of labelsurf holds information necessary to construct keys for surface data.
type Data struct {
	*datastore.Data

	// SparseVol is the name of the linked labelvol from which it gets label data
	SparseVol dvid.InstanceName
}

// GetSurface returns a gzipped byte array with # voxels and float32 arrays for vertices and
// normals.
func GetSurface(ctx storage.Context, label uint64) ([]byte, bool, error) {
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return nil, false, fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}

	// Retrieve the precomputed surface or that it's not available.
	data, err := bigdata.Get(ctx, NewLabelSurfaceIndex(label))
	if err != nil {
		return nil, false, fmt.Errorf("Error in retrieving surface for label %d: %s", label, err.Error())
	}
	if data == nil {
		return []byte{}, false, nil
	}
	uncompress := false
	surfaceBytes, _, err := dvid.DeserializeData(data, uncompress)
	if err != nil {
		return nil, false, fmt.Errorf("Unable to deserialize surface for label %d: %s\n", label, err.Error())
	}
	return surfaceBytes, true, nil
}

// GetLabelAtPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	/*
		uuid, err := datastore.UUIDFromVersion(v)
		if err != nil {
			return 0, err
		}

		// Get the associated labelvol
		labelvolData, err := labelvol.GetByUUID(uuid, d.SparseVol)
		if err != nil {
			return 0, err
		}

		// Then see if we have an associated labelblk to this labelvol.  If not we can't do point query.
		labelblkName := labelvolData.Link

		// Get the associated labelblk data instance to do the point query
		labelblkData, err := labelblk.GetByUUID(uuid, labelblkName)

		// Do the point query
		labelBytes, err := labelblkData.GetLabelBytesAtPoint(v, pt)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(labelBytes), nil
	*/
	return 0, nil
}

// --- datastore.DataService interface ---------

func (d *Data) Help() string {
	return HelpMessage
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Errorf("labelsurf.Send() is not implemented yet, so push/pull will not work for this data type.\n")
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
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// Check the action
	action := strings.ToLower(r.Method)
	if action != "get" {
		server.BadRequest(w, r, "labelsurf data can only accept GET HTTP requests")
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

	case "surface":
		// GET <api URL>/node/<UUID>/<data name>/surface/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'surface' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		fmt.Printf("Getting surface for label %d\n", label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		gzipData, found, err := GetSurface(storeCtx, label)
		if err != nil {
			server.BadRequest(w, r, "Error on getting surface for label %d: %s", label, err.Error())
			return
		}
		if !found {
			http.Error(w, fmt.Sprintf("Surface for label '%d' not found", label), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if err := dvid.WriteGzip(gzipData, w, r); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: surface on label %d (%s)", r.Method, label, r.URL)

	case "surface-by-point":
		// GET <api URL>/node/<UUID>/<data name>/surface-by-point/<coord>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'surface-by-point' command")
			return
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		label, err := d.GetLabelAtPoint(versionID, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		gzipData, found, err := GetSurface(storeCtx, label)
		if err != nil {
			server.BadRequest(w, r, "Error on getting surface for label %d: %s", label, err.Error())
			return
		}
		if !found {
			http.Error(w, fmt.Sprintf("Surface for label '%d' not found", label), http.StatusNotFound)
			return
		}
		fmt.Printf("Found surface for label %d: %d bytes (gzip payload)\n", label, len(gzipData))
		w.Header().Set("Content-type", "application/octet-stream")
		if err := dvid.WriteGzip(gzipData, w, r); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: surface-by-point at %s (%s)", r.Method, coord, r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call '%s' for labelsurf data '%s'.  See API help.",
			parts[3], d.DataName())
	}
}
