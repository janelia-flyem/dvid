/*
	Package labelmap implements DVID support for label->label mapping including
	spatial index tracking.

	NOTE: Zero value labels are reserved and useful for setting something as background.
*/
package labelmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelmap"
	TypeName = "labelmap"
)

const HelpMessage = `
API for 'labelmap' datatype (github.com/janelia-flyem/dvid/datatype/labelmap)
=============================================================================

Command-line:

$ dvid repo <UUID> new labelmap <data name> <settings...>

	Adds newly named labelmap data to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new labelmap sp2body Labels=mylabels

    Arguments:

    UUID             Hexidecimal string with enough characters to uniquely identify a version node.
    data name        Name of data to create, e.g., "sp2body"
    settings         Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Labels           Name of labels64 data for which this is a label mapping. (required)
    Versioned        "true" or "false" (default)

$ dvid node <UUID> <data name> load raveler <superpixel-to-segment filename> <segment-to-body filename>

    Loads a superpixel-to-body mapping using two Raveler-formatted text files.

    Example: 

    $ dvid node 3f8c sp2body load raveler superpixel_to_segment_map.txt segment_to_body_map.txt

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.

$ dvid node <UUID> <data name> apply <labels64 data name> <new labels64 data name>

    Applies a labelmap to current labels64 data and creates a new labels64 data.

    Example:

    $ dvid node 3f8c sp2body apply superpixels bodies

	
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
    data name     Name of mapping data.


GET <api URL>/node/<UUID>/<data name>/sparsevol/<mapped label>

	Returns a sparse volume with voxels of the given forward label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Payload descriptor:
	               Bit 0 (LSB) - 8-bit grayscale
	               Bit 1 - 16-bit grayscale
	               Bit 2 - 16-bit normal
	               ...
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Voxels [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
			  ...
	        int32   Length of run
	        bytes   Optional payload dependent on first byte descriptor


GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>

	Returns a sparse volume with voxels that pass through a given voxel.
	The encoding is described in the "sparsevol" request above.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30


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


GET <api URL>/node/<UUID>/<data name>/sizerange/<min size>/<optional max size>

    Returns JSON list of labels that have # voxels that fall within the given range
    of sizes.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    min size      Minimum # of voxels.
    max size      Optional maximum # of voxels.  If not specified, all labels with volume above minimum
                   are returned.


GET <api URL>/node/<UUID>/<data name>/mapping/<label>

    Returns the label to which the given label has been mapped.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.


GET <api URL>/node/<UUID>/<data name>/intersect/<min block>/<max block>

    Returns JSON list of labels that intersect the volume bounded by the min and max blocks.
    Note that the blocks are specified using block coordinates, so if this data instance
    has 32 x 32 x 32 voxel blocks, and we specify min block "1_2_3" and max block "3_4_5",
    the subvolume in voxels will be from min voxel point (32, 64, 96) to max voxel
    point (96, 128, 160).
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    min block     Minimum block coordinate with underscore as separator, e.g., 10_20_30
    max block     Maximum block coordinate with underscore as separator.

GET  <api URL>/node/<UUID>/<data name>/labels/<dims>/<size>/<offset>[/<format>][?throttle=on]

    Retrieves mapped labels for each voxel in the specified extent.

    Example: 

    GET <api URL>/node/3f8c/sp2body/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream".

    Throttling can be enabled by passing a "throttle=on" query string.  Throttling makes sure
    only one compute-intense operation (all API calls that can be throttled) is handled.
    If the server can't initiate the API call right away, a 503 (Service Unavailable) status
    code is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png"
                  nD: uses default "octet-stream".

TODO

GET  <api URL>/node/<UUID>/<data name>/mappings/<dims>/<size>/<offset>

    Returns the mappings in JSON format for the specified extent.

    Example: 

    GET <api URL>/node/3f8c/sp2body/mappings/0_1/512_256/0_0_100

    Returns JSON of form { pre_label1: post_label1, pre_label2: post_label2, ... } corresponding to
    the mappings for an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.


`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(&LabelsRef{})
}

// Type embeds the datastore's Type to create a unique type for labelmap functions.
type Type struct {
	datastore.Type
}

// NewDatatype returns a pointer to a new labelmap Datatype with default values set.
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

// NewDataService returns a pointer to new labelmap data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}

	// Make sure we have valid labels64 data for mapping
	labelname, found, err := c.GetString("Labels")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make labelmap without valid 'Labels' setting.")
	}

	// Make sure there is a valid labels64 instance with the given Labels name
	labelsRef, err := NewLabelsRef(uuid, dvid.DataString(labelname))
	if err != nil {
		return nil, err
	}
	dvid.Debugf("Creating labelmap instance %q with labels %q\n", name, labelsRef)
	return &Data{*basedata, Properties{Labels: labelsRef}}, nil
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// LabelsRef is a reference to an existing labels64 data
type LabelsRef struct {
	uuid dvid.UUID
	name dvid.DataString
}

func NewLabelsRef(uuid dvid.UUID, name dvid.DataString) (LabelsRef, error) {
	return LabelsRef{uuid, name}, nil
}

type labelsRefExport struct {
	UUID dvid.UUID
	Name dvid.DataString
}

// MarshalJSON implements the json.Marshaler interface.
func (ref LabelsRef) MarshalJSON() ([]byte, error) {
	return json.Marshal(labelsRefExport{ref.uuid, ref.name})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ref *LabelsRef) UnmarshalJSON(b []byte) error {
	var labels labelsRefExport
	if err := json.Unmarshal(b, &labels); err != nil {
		return err
	}
	ref.uuid = labels.UUID
	ref.name = labels.Name
	return nil
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
// Since LabelsRef are considered metadata, we don't optimize but reuse JSON.
func (ref LabelsRef) MarshalBinary() ([]byte, error) {
	return ref.MarshalJSON()
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (ref *LabelsRef) UnmarshalBinary(data []byte) error {
	return ref.UnmarshalJSON(data)
}

// GetData returns a pointer to the referenced labels.
func (ref *LabelsRef) GetData() (*labels64.Data, error) {
	return labels64.GetByUUID(ref.uuid, ref.name)
}

func (ref LabelsRef) String() string {
	return fmt.Sprintf("<%s %s>", ref.uuid, ref.name)
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Labels64 data that we will be mapping.
	Labels LabelsRef

	// Ready is true if inverse map, forward map, and spatial queries are ready.
	Ready bool
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	datastore.Data
	Properties
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		&(d.Data),
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

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("labelmap.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "load":
		if len(request.Command) < 7 {
			return fmt.Errorf("Poorly formatted load command.  See command-line help.")
		}
		switch request.Command[4] {
		case "raveler":
			return d.LoadRavelerMaps(request, reply)
		default:
			return fmt.Errorf("Cannot load unknown input file types '%s'", request.Command[3])
		}
	case "apply":
		if len(request.Command) < 6 {
			return fmt.Errorf("Poorly formatted apply command.  See command-line help.")
		}
		return d.ApplyLabelMap(request, reply)
	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
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

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	var op voxels.OpType
	switch action {
	case "get":
		op = voxels.GetOp
	case "post":
		op = voxels.PutOp
	default:
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return

	case "mapping":
		// GET <api URL>/node/<UUID>/<data name>/mapping/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		labelBytes := make([]byte, 8, 8)
		binary.BigEndian.PutUint64(labelBytes, label)
		mapping, err := d.GetLabelMapping(versionID, labelBytes)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, `{ "Mapping": %d }`, mapping)
		timedLog.Infof("HTTP %s: mapping of label '%d' (%s)", r.Method, label, r.URL)

	case "sparsevol":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := labels64.GetSparseVol(storeCtx, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: sparsevol on label %d (%s)", r.Method, label, r.URL)

	case "sparsevol-by-point":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
			return
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		label, err := d.GetLabelAtPoint(storeCtx, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := labels64.GetSparseVol(storeCtx, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, coord, r.URL)

	case "surface":
		// GET <api URL>/node/<UUID>/<data name>/surface/<label>
		fmt.Printf("Getting surface: %s\n", url)
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'surface' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		gzipData, found, err := labels64.GetSurface(storeCtx, label)
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
		label, err := d.GetLabelAtPoint(storeCtx, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		gzipData, found, err := labels64.GetSurface(storeCtx, label)
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
		timedLog.Infof("HTTP %s: surface-by-point at %s (%s)", r.Method, coord, r.URL)

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
		jsonStr, err := labels64.GetSizeRange(d, versionID, minSize, maxSize)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		timedLog.Infof("HTTP %s: get labels with volume > %d and < %d (%s)", r.Method, minSize, maxSize, r.URL)

	case "labels":
		if len(parts) < 7 {
			server.BadRequest(w, r, "'labels' must be followed by shape/size/offset")
			return
		}
		if op == voxels.PutOp {
			server.BadRequest(w, r, "Cannot POST.  Can only GET mapped labels that intersect the given geometry.")
			return
		}
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		labels, err := d.Labels.GetData()
		if err != nil {
			server.BadRequest(w, r, "Error getting labels %q", labels.DataName())
			return
		}

		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, "Error parsing slice: %s", err.Error())
				return
			}
			e, err := labels.NewExtHandler(slice, nil)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			img, err := d.GetMappedImage(versionID, e)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			var formatStr string
			if len(parts) >= 8 {
				formatStr = parts[7]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img.Get(), formatStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, plane, r.URL)
		case 3:
			queryStrings := r.URL.Query()
			if queryStrings.Get("throttle") == "on" {
				select {
				case <-server.Throttle:
					// Proceed with operation, returning throttle token to server at end.
					defer func() {
						server.Throttle <- 1
					}()
				default:
					throttleMsg := fmt.Sprintf("Server already running maximum of %d throttled operations",
						server.MaxThrottledOps)
					http.Error(w, throttleMsg, http.StatusServiceUnavailable)
					return
				}
			}
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, "Error parsing subvolume: %s", err.Error())
				return
			}
			e, err := labels.NewExtHandler(subvol, nil)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			data, err := d.GetMappedVolume(versionID, e)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-type", "application/octet-stream")
			_, err = w.Write(data)
			if err != nil {
				server.BadRequest(w, r, "Error writing data: %s", err.Error())
				return
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
			return
		}

	case "intersect":
		// GET <api URL>/node/<UUID>/<data name>/intersect/<min block>/<max block>
		if len(parts) < 6 {
			server.BadRequest(w, r, "ERROR: DVID requires min & max block coordinates to follow 'intersect' command")
			return
		}
		minPoint, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if minPoint.NumDims() != 3 {
			server.BadRequest(w, r, "ERROR: 'intersect' requires block coordinates to be in 3d, not %d-d", minPoint.NumDims())
			return
		}
		minCoord, ok := minPoint.(dvid.Point3d)
		if !ok {
			server.BadRequest(w, r, "ERROR: 'intersect' requires block coordinates to be 3d.  Got: %s", minPoint)
			return
		}
		maxPoint, err := dvid.StringToPoint(parts[5], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if maxPoint.NumDims() != 3 {
			server.BadRequest(w, r, "ERROR: 'intersect' requires block coordinates to be in 3d, not %d-d", maxPoint.NumDims())
			return
		}
		maxCoord, ok := maxPoint.(dvid.Point3d)
		if !ok {
			server.BadRequest(w, r, "ERROR: 'intersect' requires block coordinates to be 3d.  Got: %s", maxPoint)
			return
		}
		jsonStr, err := d.GetLabelsInVolume(storeCtx, dvid.ChunkPoint3d(minCoord), dvid.ChunkPoint3d(maxCoord))
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		timedLog.Infof("HTTP %s: labels that intersect volume %s -> %s", r.Method, minCoord, maxCoord)

	default:
		server.BadRequest(w, r, "Unrecognized API call '%s' for labelmap data '%s'.  See API help.", parts[3], d.DataName())
	}
}

func loadSegBodyMap(filename string) (map[uint64]uint64, error) {
	timedLog := dvid.NewTimeLog()
	dvid.Infof("Loading segment->body map: %s\n", filename)

	segmentToBodyMap := make(map[uint64]uint64, 100000)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Could not open segment->body map: %s", filename)
	}
	defer file.Close()
	linenum := 0
	lineReader := bufio.NewReader(file)
	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		storage.FileBytesRead <- len(line)
		var segment, body uint64
		if _, err := fmt.Sscanf(line, "%d %d", &segment, &body); err != nil {
			return nil, fmt.Errorf("Error loading segment->body map, line %d in %s", linenum, filename)
		}
		segmentToBodyMap[segment] = body
		linenum++
	}
	timedLog.Infof("Loaded Raveler segment->body file: %s", filename)
	return segmentToBodyMap, nil
}

// NewRavelerForwardMapIndex returns an index that encodes a "label + mapping", where
// the label is a uint64 with top 4 bytes encoding Z and least-significant 4 bytes encoding
// the superpixel ID.  Also, the zero label is reserved.
func (d *Data) NewRavelerForwardMapIndex(z, spid uint32, body uint64) []byte {
	index := make([]byte, 17)
	index[0] = byte(voxels.KeyForwardMap)
	copy(index[1:9], labels64.RavelerSuperpixelBytes(z, spid))
	binary.BigEndian.PutUint64(index[9:17], body)
	return index
}

// LoadRavelerMaps loads maps from Raveler-formatted superpixel->segment and
// segment->body maps.  Ignores any mappings that are in slices outside
// associated labels64 volume.
func (d *Data) LoadRavelerMaps(request datastore.Request, reply *datastore.Response) error {

	// Parse the request
	var uuidStr, dataName, cmdStr, fileTypeStr, spsegStr, segbodyStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &fileTypeStr, &spsegStr, &segbodyStr)

	timedLog := dvid.NewTimeLog()

	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	labelData, err := d.Labels.GetData()
	if err != nil {
		return err
	}
	if !labelData.Ready {
		return fmt.Errorf("Can't load raveler maps if underlying labels64 %q has not been loaded!", labelData.DataName())
	}
	minLabelZ := uint32(labelData.Extents().MinPoint.Value(2))
	maxLabelZ := uint32(labelData.Extents().MaxPoint.Value(2))

	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	d.Ready = false
	if err := repo.Save(); err != nil {
		return err
	}
	if err = repo.AddToLog(request.Command.String()); err != nil {
		return err
	}

	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
	}
	ctx := datastore.NewVersionedContext(d, versionID)

	// Get the seg->body map
	seg2body, err := loadSegBodyMap(segbodyStr)
	if err != nil {
		return err
	}

	var slice, superpixel32 uint32
	var segment, body uint64

	// Get the sp->seg map, persisting each computed sp->body.
	dvid.Infof("Processing superpixel->segment map (Z %d-%d): %s\n", minLabelZ, maxLabelZ, spsegStr)
	file, err := os.Open(spsegStr)
	if err != nil {
		return fmt.Errorf("Could not open superpixel->segment map: %s", spsegStr)
	}
	defer file.Close()
	lineReader := bufio.NewReader(file)
	linenum := 0

	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		storage.FileBytesRead <- len(line)
		if _, err := fmt.Sscanf(line, "%d %d %d", &slice, &superpixel32, &segment); err != nil {
			return fmt.Errorf("Error loading superpixel->segment map, line %d in %s", linenum, spsegStr)
		}
		if slice < minLabelZ || slice > maxLabelZ {
			continue
		}
		if superpixel32 == 0 {
			continue
		}
		if superpixel32 > 0x0000000000FFFFFF {
			return fmt.Errorf("Error in line %d: superpixel id exceeds 24-bit value!", linenum)
		}
		superpixelBytes := labels64.RavelerSuperpixelBytes(slice, superpixel32)
		var found bool
		body, found = seg2body[segment]
		if !found {
			return fmt.Errorf("Segment (%d) in %s not found in %s", segment, spsegStr, segbodyStr)
		}

		// PUT the forward label pair without compression.
		forwardIndex := voxels.NewForwardMapIndex(superpixelBytes, body)
		if err := smalldata.Put(ctx, forwardIndex, dvid.EmptyValue()); err != nil {
			return fmt.Errorf("ERROR on PUT of forward label mapping (%x -> %d): %s\n",
				superpixelBytes, body, err.Error())
		}

		// PUT the inverse label pair without compression.
		inverseIndex := voxels.NewInverseMapIndex(superpixelBytes, body)
		if err := smalldata.Put(ctx, inverseIndex, dvid.EmptyValue()); err != nil {
			return fmt.Errorf("ERROR on PUT of inverse label mapping (%d -> %x): %s\n",
				body, superpixelBytes, err.Error())
		}

		linenum++
		if linenum%1000000 == 0 {
			dvid.Infof("Added %d forward and inverse mappings\n", linenum)
		}
	}
	dvid.Infof("Added %d forward and inverse mappings\n", linenum)
	timedLog.Infof("Processed Raveler superpixel->body files")

	// Spawn goroutine to do spatial processing on associated label volume.
	go d.ProcessSpatially(uuid)

	return nil
}

// ApplyLabelMap creates a new labels64 by applying a label map to existing labels64 data.
func (d *Data) ApplyLabelMap(request datastore.Request, reply *datastore.Response) error {
	if !d.Ready {
		return fmt.Errorf("Can't apply labelmap that hasn't been loaded.")
	}
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceName, destName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceName, &destName)

	// Get the version and repo
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	if err = repo.AddToLog(request.Command.String()); err != nil {
		return err
	}

	// Use existing destination data or a new labels64 data.
	var dest *labels64.Data
	dest, err = labels64.GetByUUID(uuid, dvid.DataString(destName))
	if err != nil {
		config := dvid.NewConfig()
		typeservice, err := datastore.TypeServiceByName("labels64")
		if err != nil {
			return err
		}
		dataservice, err := repo.NewData(typeservice, dvid.DataString(destName), config)
		if err != nil {
			return err
		}
		var ok bool
		dest, ok = dataservice.(*labels64.Data)
		if !ok {
			return fmt.Errorf("Could not create labels64 data instance")
		}
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}
	labelData, err := d.Labels.GetData()
	if err != nil {
		return err
	}
	labelCtx := datastore.NewVersionedContext(labelData, versionID)

	wg := new(sync.WaitGroup)
	op := &denormOp{labelData, nil, dest, versionID, nil}

	extents := labelData.Extents()
	minIndexZ := extents.MinIndex.Value(2)
	maxIndexZ := extents.MaxIndex.Value(2)
	for z := minIndexZ; z <= maxIndexZ; z++ {
		layerLog := dvid.NewTimeLog()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			return fmt.Errorf("Error getting label mapping for block Z %d: %s\n", z, err.Error())
		}

		// Process the labels chunks for this Z
		if op.mapping != nil {
			minIndexZYX := dvid.IndexZYX(minChunkPt)
			maxIndexZYX := dvid.IndexZYX(maxChunkPt)
			begIndex := voxels.NewVoxelBlockIndex(&minIndexZYX)
			endIndex := voxels.NewVoxelBlockIndex(&maxIndexZYX)
			chunkOp := &storage.ChunkOp{op, wg}
			err = bigdata.ProcessRange(labelCtx, begIndex, endIndex, chunkOp, d.ChunkApplyMap)
			wg.Wait()
		}

		layerLog.Debugf("Processed all %s blocks for layer %d/%d", sourceName, z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	timedLog.Infof("Mapped %s to %s using label map %s", sourceName, destName, d.DataName())

	// Set new mapped data to same extents.
	dest.Properties = labelData.Properties
	if err := datastore.SaveRepo(uuid); err != nil {
		dvid.Infof("Could not save READY state to data '%s', uuid %s: %s", d.DataName(), uuid, err.Error())
	}

	// Kickoff denormalizations based on new labels64.
	go dest.ProcessSpatially(uuid)

	return nil
}

// GetLabelMapping returns the mapping for a label.
func (d *Data) GetLabelMapping(versionID dvid.VersionID, label []byte) (uint64, error) {
	begIndex := voxels.NewForwardMapIndex(label, 0)
	endIndex := voxels.NewForwardMapIndex(label, math.MaxUint64)

	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return 0, fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
	}
	ctx := datastore.NewVersionedContext(d, versionID)
	keys, err := smalldata.KeysInRange(ctx, begIndex, endIndex)
	if err != nil {
		return 0, err
	}
	numKeys := len(keys)
	switch {
	case numKeys == 0:
		return 0, fmt.Errorf("Label %d is not mapped to any other label.", label)
	case numKeys > 1:
		var mapped string
		for i := 0; i < len(keys); i++ {
			mapped += fmt.Sprintf("%d ", keys[i])
		}
		return 0, fmt.Errorf("Label %d is mapped to more than one label: %s", label, mapped)
	}
	indexBytes, err := ctx.IndexFromKey(keys[0])
	if err != nil {
		return 0, err
	}
	mapping := binary.BigEndian.Uint64(indexBytes[9:17])

	return mapping, nil
}

// GetBlockMapping returns the label -> mappedLabel map for a given block.
func (d *Data) GetBlockMapping(versionID dvid.VersionID, blockI dvid.Index) (map[string]uint64, error) {

	maxLabel := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	begIndex := voxels.NewSpatialMapIndex(blockI, nil, 0)
	endIndex := voxels.NewSpatialMapIndex(blockI, maxLabel, math.MaxUint64)

	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
	}
	ctx := datastore.NewVersionedContext(d, versionID)
	keys, err := smalldata.KeysInRange(ctx, begIndex, endIndex)
	if err != nil {
		return nil, err
	}
	numKeys := len(keys)
	mapping := make(map[string]uint64, numKeys)
	offset := 1 + dvid.IndexZYXSize
	for _, key := range keys {
		indexBytes, err := ctx.IndexFromKey(key)
		if err != nil {
			return nil, err
		}
		label := indexBytes[offset : offset+8]
		mappedLabel := binary.BigEndian.Uint64(indexBytes[offset+8 : offset+16])
		mapping[string(label)] = mappedLabel
	}
	return mapping, nil
}

// GetBlockLayerMapping gets the label mapping for a Z layer of blocks and stores the result
// in the passed Operation.
func (d *Data) GetBlockLayerMapping(blockZ int32, op *denormOp) (minChunkPt, maxChunkPt dvid.ChunkPoint3d, err error) {

	// Convert blockZ to actual voxel space Z range.
	minChunkPt = dvid.ChunkPoint3d{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], blockZ}
	maxChunkPt = dvid.ChunkPoint3d{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], blockZ}
	minVoxelPt := minChunkPt.MinPoint(op.source.BlockSize())
	maxVoxelPt := minChunkPt.MaxPoint(op.source.BlockSize())

	// Get first and last keys that span that voxel space Z range.
	minZ := uint32(minVoxelPt.Value(2))
	maxZ := uint32(maxVoxelPt.Value(2))
	begIndex := d.NewRavelerForwardMapIndex(minZ, 1, 0)
	endIndex := d.NewRavelerForwardMapIndex(maxZ, 0xFFFFFFFF, math.MaxUint64)

	// Get all forward mappings from the key-value store.
	op.mapping = nil

	smalldata, err := storage.SmallDataStore()
	if err != nil {
		err = fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
		return
	}
	ctx := datastore.NewVersionedContext(d, op.versionID)
	var keys [][]byte
	keys, err = smalldata.KeysInRange(ctx, begIndex, endIndex)
	if err != nil {
		err = fmt.Errorf("Error trying to find mapping of slice between %d and %d: %s",
			minZ, maxZ, err.Error())
		return
	}

	// Cache this layer of blocks' mappings.
	numKeys := len(keys)
	if numKeys != 0 {
		op.mapping = make(map[string]uint64, numKeys)
		var indexBytes []byte
		for _, key := range keys {
			indexBytes, err = ctx.IndexFromKey(key)
			if err != nil {
				return
			}
			label := string(indexBytes[1:9])
			mappedLabel := binary.BigEndian.Uint64(indexBytes[9:17])
			op.mapping[label] = mappedLabel
		}
	}
	return
}

// ChunkApplyMap maps a chunk of labels using the current mapping.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) ChunkApplyMap(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.chunkApplyMap(chunk)
}

func (d *Data) chunkApplyMap(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*denormOp)

	// Get the spatial index associated with this chunk.
	zyx, err := voxels.DecodeVoxelBlockKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error in %s.ChunkApplyMap(): %s", d.Data.DataName(), err.Error())
		return
	}

	// Initialize the label buffers.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.Data.DataName(), err.Error())
		return
	}
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	mappedData := make([]byte, blockBytes, blockBytes)

	// Map this block of labels64.
	var b uint64
	var ok bool
	for start := 0; start < blockBytes; start += 8 {
		a := blockData[start : start+8]

		// Get the label to which the current label is mapped.
		if bytes.Compare(a, labels64.ZeroBytes()) == 0 {
			b = 0
		} else {
			b, ok = op.mapping[string(a)]
			if !ok {
				minZ := zyx.MinPoint(op.source.BlockSize()).Value(2)
				maxZ := zyx.MaxPoint(op.source.BlockSize()).Value(2)
				slice := binary.BigEndian.Uint32(a[0:4])
				dvid.Infof("No mapping found for %x (slice %d) in block with Z %d to %d.  Set to 0.\n",
					a, slice, minZ, maxZ)
				b = 0
			}
		}
		op.source.ByteOrder.PutUint64(mappedData[start:start+8], b)
	}

	// Save the results
	serialization, err := dvid.SerializeData(mappedData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Infof("Unable to serialize block: %s\n", err.Error())
		return
	}
	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Unable to retrieve big data store: %s\n", err.Error())
		return
	}
	ctx := datastore.NewVersionedContext(op.dest, op.versionID)
	bigdata.Put(ctx, voxels.NewVoxelBlockIndex(zyx), serialization)
}
