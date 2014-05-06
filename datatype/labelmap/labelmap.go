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
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/labelmap"
)

const HelpMessage = `
API for 'labelmap' datatype (github.com/janelia-flyem/dvid/datatype/labelmap)
=============================================================================

Command-line:

$ dvid dataset <UUID> new labelmap <data name> <settings...>

	Adds newly named labelmap data to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c new labelmap sp2body Labels=mylabels

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


GET <api URL>/node/<UUID>/<data name>/sizerange/<min size>/<max size>

    Returns JSON list of labels that have # voxels that fall within the given range
    of sizes.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    min size      Minimum # of voxels.
    max size      Maximum # of voxels.


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

GET  <api URL>/node/<UUID>/<data name>/labels/<dims>/<size>/<offset>[/<format>]

    Retrieves mapped labels for each voxel in the specified extent.

    Example: 

    GET <api URL>/node/3f8c/sp2body/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream".

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
	labelmap := NewDatatype()
	labelmap.DatatypeID = &datastore.DatatypeID{
		Name:    "labelmap",
		Url:     RepoUrl,
		Version: Version,
	}
	datastore.RegisterDatatype(labelmap)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
	gob.Register(&LabelsRef{})
}

// Datatype embeds the datastore's Datatype to create a unique type for labelmap functions.
type Datatype struct {
	datastore.Datatype
}

// NewDatatype returns a pointer to a new labelmap Datatype with default values set.
func NewDatatype() (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to new labelmap data with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, c dvid.Config) (datastore.DataService, error) {
	// Make sure we have valid labels64 data for mapping
	name, found, err := c.GetString("Labels")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make labelmap without valid 'Labels' setting.")
	}

	basedata, err := datastore.NewDataService(id, dtype, c)
	if err != nil {
		return nil, err
	}

	// Make sure there is a valid labels64 instance with the given Labels name
	labelsRef, err := NewLabelsRef(dvid.DataString(name), id.DatasetID())
	if err != nil {
		return nil, err
	}
	fmt.Printf("LabelsRef = %s\n", labelsRef)
	return &Data{Data: basedata, Labels: labelsRef}, nil
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// LabelsRef is a reference to an existing labels64 data
type LabelsRef struct {
	name dvid.DataString
	dset dvid.DatasetLocalID
	ptr  *labels64.Data
}

func NewLabelsRef(name dvid.DataString, dset dvid.DatasetLocalID) (LabelsRef, error) {
	ptr, err := labels64.GetByLocalID(dset, name)
	if err != nil {
		return LabelsRef{}, err
	}
	return LabelsRef{name, dset, ptr}, nil
}

type labelsExport struct {
	Name           dvid.DataString
	DatasetLocalID dvid.DatasetLocalID
}

// MarshalJSON implements the json.Marshaler interface.
func (ref LabelsRef) MarshalJSON() ([]byte, error) {
	v := labelsExport{ref.name, ref.dset}
	return json.Marshal(v)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ref *LabelsRef) UnmarshalJSON(b []byte) error {
	var labels labelsExport
	if err := json.Unmarshal(b, &labels); err != nil {
		return err
	}
	ref.name = labels.Name
	ref.dset = labels.DatasetLocalID
	return nil
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (ref LabelsRef) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(ref.name))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(ref.name)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, ref.dset); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (ref *LabelsRef) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	var length uint16
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return err
	}
	name := make([]byte, length)
	if n, err := buf.Read(name); err != nil || n != int(length) {
		return fmt.Errorf("Error reading label reference name.")
	}
	var dset dvid.DatasetLocalID
	if err := binary.Read(buf, binary.LittleEndian, &dset); err != nil {
		return err
	}
	// See if we can associate a dataset pointer, but if not, simply exit and defer
	// to GetData() time.
	labelsName := dvid.DataString(name)
	ptr, err := labels64.GetByLocalID(dset, labelsName)
	if err != nil {
		ptr = nil
	}
	ref.name = labelsName
	ref.dset = dset
	ref.ptr = ptr
	return nil
}

// GetData returns a pointer to the referenced labels and stores the pointer
// into the reference.
func (ref *LabelsRef) GetData() (*labels64.Data, error) {
	if ref.ptr != nil {
		return ref.ptr, nil
	}
	ptr, err := labels64.GetByLocalID(ref.dset, ref.name)
	if err != nil {
		return nil, err
	}
	ref.ptr = ptr
	return ptr, nil
}

func (ref LabelsRef) String() string {
	return string(ref.name)
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data

	// Labels64 data that we will be mapping.
	Labels LabelsRef

	// Ready is true if inverse map, forward map, and spatial queries are ready.
	Ready bool
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
		return d.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this data.
func (d *Data) DoHTTP(uuid dvid.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

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
		return fmt.Errorf("Can only handle GET or POST HTTP verbs")
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil

	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil

	case "mapping":
		// GET <api URL>/node/<UUID>/<data name>/mapping/<label>
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires label ID to follow 'sparsevol' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		labelBytes := make([]byte, 8, 8)
		binary.BigEndian.PutUint64(labelBytes, label)
		mapping, err := d.GetLabelMapping(versionID, labelBytes)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, `{ "Mapping": %d }`, mapping)
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: mapping of label '%d' (%s)", r.Method, label, r.URL)

	case "sparsevol":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires label ID to follow 'sparsevol' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		data, err := d.GetSparseVol(uuid, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: sparsevol on label %d (%s)",
			r.Method, label, r.URL)

	case "sparsevol-by-point":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := d.GetLabelAtPoint(uuid, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		data, err := d.GetSparseVol(uuid, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: sparsevol-by-point at %s (%s)",
			r.Method, coord, r.URL)

	case "surface":
		// GET <api URL>/node/<UUID>/<data name>/surface/<label>
		fmt.Printf("Getting surface: %s\n", url)
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires label ID to follow 'surface' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		gzipData, found, err := d.GetSurface(uuid, label)
		if err != nil {
			return fmt.Errorf("Error on getting surface for label %d: %s", label, err.Error())
		}
		if !found {
			http.Error(w, fmt.Sprintf("Surface for label '%d' not found", label), http.StatusNotFound)
			return nil
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if err := dvid.WriteGzip(gzipData, w, r); err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: surface on label %d (%s)",
			r.Method, label, r.URL)

	case "surface-by-point":
		// GET <api URL>/node/<UUID>/<data name>/surface-by-point/<coord>
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires coord to follow 'surface-by-point' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := d.GetLabelAtPoint(uuid, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		gzipData, found, err := d.GetSurface(uuid, label)
		if err != nil {
			return fmt.Errorf("Error on getting surface for label %d: %s", label, err.Error())
		}
		if !found {
			http.Error(w, fmt.Sprintf("Surface for label '%d' not found", label), http.StatusNotFound)
			return nil
		}
		w.Header().Set("Content-type", "application/octet-stream")
		if err := dvid.WriteGzip(gzipData, w, r); err != nil {
			return err
		}
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: surface-by-point at %s (%s)",
			r.Method, coord, r.URL)

	case "sizerange":
		// GET <api URL>/node/<UUID>/<data name>/sizerange/<min size>/<max size>
		if len(parts) < 6 {
			err := fmt.Errorf("ERROR: DVID requires min & max sizes to follow 'sizerange' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		minSize, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		maxSize, err := strconv.ParseUint(parts[5], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		jsonStr, err := d.GetSizeRange(uuid, minSize, maxSize)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: get labels with volume > %d and < %d (%s)",
			r.Method, minSize, maxSize, r.URL)

	case "labels":
		if len(parts) < 7 {
			return fmt.Errorf("'labels' must be followed by shape/size/offset")
		}
		if op == voxels.PutOp {
			return fmt.Errorf("Cannot POST.  Can only GET mapped labels that intersect the given geometry.")
		}
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			return err
		}
		labels, err := d.Labels.GetData()
		if err != nil {
			return err
		}

		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				return err
			}
			e, err := labels.NewExtHandler(slice, nil)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			img, err := d.GetMappedImage(uuid, e)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			var formatStr string
			if len(parts) >= 8 {
				formatStr = parts[7]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img.Get(), formatStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s (%s)", r.Method, plane, r.URL)
		case 3:
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				return err
			}
			e, err := labels.NewExtHandler(subvol, nil)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			data, err := d.GetMappedVolume(uuid, e)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			w.Header().Set("Content-type", "application/octet-stream")
			_, err = w.Write(data)
			if err != nil {
				return err
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			return fmt.Errorf("DVID currently supports shapes of only 2 and 3 dimensions")
		}

	case "intersect":
		// GET <api URL>/node/<UUID>/<data name>/intersect/<min block>/<max block>
		if len(parts) < 6 {
			err := fmt.Errorf("ERROR: DVID requires min & max block coordinates to follow 'intersect' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		minPoint, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		if minPoint.NumDims() != 3 {
			err := fmt.Errorf("ERROR: 'intersect' requires block coordinates to be in 3d, not %d-d", minPoint.NumDims())
			server.BadRequest(w, r, err.Error())
			return err
		}
		minCoord, ok := minPoint.(dvid.Point3d)
		if !ok {
			err := fmt.Errorf("ERROR: 'intersect' requires block coordinates to be 3d.  Got: %s", minPoint)
			server.BadRequest(w, r, err.Error())
			return err
		}
		maxPoint, err := dvid.StringToPoint(parts[5], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		if maxPoint.NumDims() != 3 {
			err := fmt.Errorf("ERROR: 'intersect' requires block coordinates to be in 3d, not %d-d", maxPoint.NumDims())
			server.BadRequest(w, r, err.Error())
			return err
		}
		maxCoord, ok := maxPoint.(dvid.Point3d)
		if !ok {
			err := fmt.Errorf("ERROR: 'intersect' requires block coordinates to be 3d.  Got: %s", maxPoint)
			server.BadRequest(w, r, err.Error())
			return err
		}
		jsonStr, err := d.GetLabelsInVolume(uuid, dvid.ChunkPoint3d(minCoord), dvid.ChunkPoint3d(maxCoord))
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: labels that intersect volume %s -> %s",
			r.Method, minCoord, maxCoord)

	default:
		return fmt.Errorf("Unrecognized API call '%s' for labelmap data '%s'.  See API help.", parts[3], d.DataName())
	}

	return nil
}

func loadSegBodyMap(filename string) (map[uint64]uint64, error) {
	startTime := time.Now()
	dvid.Log(dvid.Normal, "Loading segment->body map: %s\n", filename)

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
	dvid.ElapsedTime(dvid.Debug, startTime, "Loaded Raveler segment->body file: %s", filename)
	return segmentToBodyMap, nil
}

// NewForwardMapKey returns a datastore.DataKey that encodes a "label + mapping", where
// the label and mapping are both uint64.
func (d *Data) NewForwardMapKey(vID dvid.VersionLocalID, label []byte, mapping uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], label)
	binary.BigEndian.PutUint64(index[9:17], mapping)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewRavelerForwardMapKey returns a datastore.DataKey that encodes a "label + mapping", where
// the label is a uint64 with top 4 bytes encoding Z and least-significant 4 bytes encoding
// the superpixel ID.  Also, the zero label is reserved.
func (d *Data) NewRavelerForwardMapKey(vID dvid.VersionLocalID, z, spid uint32, body uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], labels64.RavelerSuperpixelBytes(z, spid))
	binary.BigEndian.PutUint64(index[9:17], body)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewSpatialMapKey returns a datastore.DataKey that encodes a "spatial index + label + mapping".
func (d *Data) NewSpatialMapKey(vID dvid.VersionLocalID, blockIndex dvid.Index, label []byte,
	mapping uint64) *datastore.DataKey {

	index := make([]byte, 1+dvid.IndexZYXSize+8+8) // s + a + b
	index[0] = byte(KeySpatialMap)
	i := 1 + dvid.IndexZYXSize
	copy(index[1:i], blockIndex.Bytes())
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mapping)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// LoadRavelerMaps loads maps from Raveler-formatted superpixel->segment and
// segment->body maps.  Ignores any mappings that are in slices outside
// associated labels64 volume.
func (d *Data) LoadRavelerMaps(request datastore.Request, reply *datastore.Response) error {

	// Parse the request
	var uuidStr, dataName, cmdStr, fileTypeStr, spsegStr, segbodyStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &fileTypeStr, &spsegStr, &segbodyStr)

	//startTime := time.Now()

	uuid, err := server.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	/*
		service := server.DatastoreService()
		_, versionID, err := service.LocalIDFromUUID(uuid)
		if err != nil {
			return err
		}
		labels, err := d.Labels.GetData()
		if err != nil {
			return err
		}
		if !labels.Ready {
			return fmt.Errorf("Can't load raveler maps if underlying labels64 %q has not been loaded!", labels.DataName())
		}
		minLabelZ := uint32(labels.Extents().MinPoint.Value(2))
		maxLabelZ := uint32(labels.Extents().MaxPoint.Value(2))

		d.Ready = false
		if err := service.SaveDataset(uuid); err != nil {
			return err
		}

		// Get the seg->body map
		seg2body, err := loadSegBodyMap(segbodyStr)
		if err != nil {
			return err
		}

		// Prepare for datastore access
		db, err := server.OrderedKeyValueSetter()
		if err != nil {
			return err
		}

		var slice, superpixel32 uint32
		var segment, body uint64
		forwardIndex := make([]byte, 17)
		forwardIndex[0] = byte(KeyForwardMap)
		inverseIndex := make([]byte, 17)
		inverseIndex[0] = byte(KeyInverseMap)

		// Get the sp->seg map, persisting each computed sp->body.
		dvid.Log(dvid.Normal, "Processing superpixel->segment map (Z %d-%d): %s\n",
			minLabelZ, maxLabelZ, spsegStr)
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
			copy(forwardIndex[1:9], superpixelBytes)
			binary.BigEndian.PutUint64(forwardIndex[9:17], body)
			key := d.DataKey(versionID, dvid.IndexBytes(forwardIndex))
			err = db.Put(key, emptyValue)
			if err != nil {
				return fmt.Errorf("ERROR on PUT of forward label mapping (%x -> %d): %s\n",
					superpixelBytes, body, err.Error())
			}

			// PUT the inverse label pair without compression.
			binary.BigEndian.PutUint64(inverseIndex[1:9], body)
			copy(inverseIndex[9:17], superpixelBytes)
			key = d.DataKey(versionID, dvid.IndexBytes(inverseIndex))
			err = db.Put(key, emptyValue)
			if err != nil {
				return fmt.Errorf("ERROR on PUT of inverse label mapping (%d -> %x): %s\n",
					body, superpixelBytes, err.Error())
			}

			linenum++
			if linenum%1000000 == 0 {
				dvid.Log(dvid.Normal, "Added %d forward and inverse mappings\n", linenum)
			}
		}
		dvid.Log(dvid.Normal, "Added %d forward and inverse mappings\n", linenum)
		dvid.ElapsedTime(dvid.Normal, startTime, "Processed Raveler superpixel->body files")
	*/
	// Spawn goroutine to do spatial processing on associated label volume.
	go d.ProcessSpatially(uuid)

	return nil
}

// ApplyLabelMap creates a new labels64 by applying a label map to existing labels64 data.
func (d *Data) ApplyLabelMap(request datastore.Request, reply *datastore.Response) error {

	if !d.Ready {
		return fmt.Errorf("Can't apply labelmap that hasn't been loaded.")
	}

	startTime := time.Now()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceName, destName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceName, &destName)

	// Get the version
	uuid, err := server.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		return err
	}
	db, err := server.OrderedKeyValueDB()
	if err != nil {
		return err
	}

	// Use existing destination data or a new labels64 data.
	var dest *labels64.Data
	dest, err = labels64.GetByUUID(uuid, dvid.DataString(destName))
	if err != nil {
		config := dvid.NewConfig()
		err = service.NewData(uuid, "labels64", dvid.DataString(destName), config)
		if err != nil {
			return err
		}
		dest, err = labels64.GetByUUID(uuid, dvid.DataString(destName))
		if err != nil {
			return err
		}
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	labels, err := d.Labels.GetData()
	if err != nil {
		return err
	}

	wg := new(sync.WaitGroup)
	op := &denormOp{labels, nil, dest.DataID().ID, versionID, nil}

	dataID := labels.DataID()
	extents := labels.Extents()
	minIndexZ := extents.MinIndex.(dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(dvid.IndexZYX)[2]
	for z := minIndexZ; z <= maxIndexZ; z++ {
		t := time.Now()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			return fmt.Errorf("Error getting label mapping for block Z %d: %s\n", z, err.Error())
		}

		// Process the labels chunks for this Z
		minIndex := dvid.IndexZYX(minChunkPt)
		maxIndex := dvid.IndexZYX(maxChunkPt)
		if op.mapping != nil {
			startKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, minIndex}
			endKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, maxIndex}
			chunkOp := &storage.ChunkOp{op, wg}
			err = db.ProcessRange(startKey, endKey, chunkOp, d.ChunkApplyMap)
			wg.Wait()
		}

		dvid.ElapsedTime(dvid.Debug, t, "Processed all %s blocks for layer %d/%d",
			sourceName, z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Mapped %s to %s using label map %s",
		sourceName, destName, d.DataName())

	// Set new mapped data to same extents.
	dest.Properties = labels.Properties
	if err := server.DatastoreService().SaveDataset(uuid); err != nil {
		dvid.Log(dvid.Normal, "Could not save READY state to data '%s', uuid %s: %s",
			d.DataName(), uuid, err.Error())
	}

	return nil
}

// GetLabelMapping returns the mapping for a label.
func (d *Data) GetLabelMapping(versionID dvid.VersionLocalID, label []byte) (uint64, error) {
	firstKey := d.NewForwardMapKey(versionID, label, 0)
	lastKey := d.NewForwardMapKey(versionID, label, math.MaxUint64)

	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return 0, err
	}
	keys, err := db.KeysInRange(firstKey, lastKey)
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

	b := keys[0].Bytes()
	indexBytes := b[datastore.DataKeyIndexOffset:]
	mapping := binary.BigEndian.Uint64(indexBytes[9:17])

	return mapping, nil
}

// GetBlockMapping returns the label -> mappedLabel map for a given block.
func (d *Data) GetBlockMapping(vID dvid.VersionLocalID, block dvid.IndexZYX) (map[string]uint64, error) {
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return nil, err
	}

	firstKey := d.NewSpatialMapKey(vID, block, nil, 0)
	maxLabel := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	lastKey := d.NewSpatialMapKey(vID, block, maxLabel, math.MaxUint64)

	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return nil, err
	}
	numKeys := len(keys)
	mapping := make(map[string]uint64, numKeys)
	offset := 1 + dvid.IndexZYXSize
	for _, key := range keys {
		dataKey := key.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
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
	firstKey := d.NewRavelerForwardMapKey(op.versionID, minZ, 1, 0)
	lastKey := d.NewRavelerForwardMapKey(op.versionID, maxZ, 0xFFFFFFFF, math.MaxUint64)

	// Get all forward mappings from the key-value store.
	op.mapping = nil

	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return
	}
	var keys []storage.Key
	keys, err = db.KeysInRange(firstKey, lastKey)
	if err != nil {
		err = fmt.Errorf("Could not find mapping with slice between %d and %d: %s",
			minZ, maxZ, err.Error())
		return
	}

	// Cache this layer of blocks' mappings.
	numKeys := len(keys)
	if numKeys != 0 {
		op.mapping = make(map[string]uint64, numKeys)
		for _, key := range keys {
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
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
	db, err := server.OrderedKeyValueSetter()
	if err != nil {
		dvid.Log(dvid.Normal, "Error in %s.ChunkApplyMap(): %s", d.DataID.DataName(), err.Error())
		return
	}

	// Get the spatial index associated with this chunk.
	dataKey := chunk.K.(*datastore.DataKey)
	zyx := dataKey.Index.(*dvid.IndexZYX)

	// Initialize the label buffers.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to deserialize block in '%s': %s\n",
			d.DataID.DataName(), err.Error())
		return
	}
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Log(dvid.Normal, "Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	mappedData := make([]byte, blockBytes, blockBytes)

	// Map this block of labels.
	var b uint64
	var ok bool
	for start := 0; start < blockBytes; start += 8 {
		a := blockData[start : start+8]

		// Get the label to which the current label is mapped.
		if bytes.Compare(a, zeroLabelBytes) == 0 {
			b = 0
		} else {
			b, ok = op.mapping[string(a)]
			if !ok {
				zBeg := zyx.MinPoint(op.source.BlockSize()).Value(2)
				zEnd := zyx.MaxPoint(op.source.BlockSize()).Value(2)
				slice := binary.BigEndian.Uint32(a[0:4])
				dvid.Log(dvid.Normal, "No mapping found for %x (slice %d) in block with Z %d to %d\n",
					a, slice, zBeg, zEnd)
				dvid.Log(dvid.Normal, "Aborting creation of '%s' chunk using '%s' labelmap\n",
					op.source.DataName(), d.DataName())
				return
			}
		}
		op.source.ByteOrder.PutUint64(mappedData[start:start+8], b)
	}

	// Save the results
	mappedKey := &datastore.DataKey{
		Dataset: dataKey.Dataset,
		Data:    op.destID,
		Version: op.versionID,
		Index:   dataKey.Index,
	}
	serialization, err := dvid.SerializeData(mappedData, d.Compression, d.Checksum)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to serialize block: %s\n", err.Error())
		return
	}
	db.Put(mappedKey, serialization)
}
