/*
	Package labelblk tailors the voxels data type for 64-bit labels and allows loading
	of NRGBA images (e.g., Raveler superpixel PNG images) that implicitly use slice Z as
	part of the label index.
*/
package labelblk

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io/ioutil"
	"net/http"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelblk"
	TypeName = "labelblk"
)

const HelpMessage = `
API for label block data type (github.com/janelia-flyem/dvid/datatype/labelblk)
===============================================================================

Note: Denormalizations like sparse volumes are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labelblk <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new labelblk superpixels VoxelSize=3.2,3.2,40.0

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "superpixels"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Sync           Name of preexisting labelvol or labeltile data
    LabelType      "standard" (default) or "raveler" 
    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")

$ dvid node <UUID> <data name> load <offset> <image glob> <settings...>

    Initializes version node to a set of XY label images described by glob of filenames.
    The DVID server must have access to the named files.  Currently, XY images are required.
    Note that how the loaded data is processed depends on the LabelType of this labelblk data.
    If LabelType is "Raveler", DVID assumes we are loading Raveler 24-bit labels and will 
    set the lower 4 bytes of 64-bit label with loaded pixel values and adds the image Z offset 
    as the higher 4 bytes.  If LabelType is "Standard", we read the loaded data and convert
    to 64-bit labels.

    Example: 

    $ dvid node 3f8c superpixels load 0,0,100 "data/*.png" proc=noindex

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of label images, preferably in quotes, e.g., "foo-xy-*.png"

    Configuration Settings (case-insensitive keys)

    Proc          "noindex": prevents creation of denormalized data to speed up obtaining sparse 
    				 volumes and size query responses using the loaded labels.  This is not necessary 
    				 for data that will evaluated using labelmap data, e.g., Raveler superpixels,
    				 and is automatically set if LabelType is "Raveler".

$ dvid node <UUID> <data name> composite <uint8 data name> <new rgba8 data name>

    Creates a RGBA8 image where the RGB is a hash of the labels and the A is the
    grayscale intensity.

    Example: 

    $ dvid node 3f8c bodies composite grayscale bodyview

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
	
	
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


GET  <api URL>/node/<UUID>/<data name>/metadata

	Retrieves a JSON schema (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=true]
GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?throttle=true]
POST <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=true]

    Retrieves or puts label data as binary blob using schema above.  Binary data is simply
    packed 64-bit data.  See 'voxels' API for discussion of 'raw' versus 'isotropic'

    Example: 

    GET <api URL>/node/3f8c/superpixels/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in binary format.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response will be "application/octet-stream".

    Throttling can be enabled by passing a "throttle=true" query string.  Throttling makes sure
    only one compute-intense operation (all API calls that can be throttled) is handled.
    If the server can't initiate the API call right away, a 503 (Service Unavailable) status
    code is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    roi       	  Name of roi data instance used to mask the requested data.

(Assumes labels were loaded using without "proc=noindex")

`

var (
	dtype        Type
	encodeFormat dvid.DataValues

	zeroLabelBytes = make([]byte, 8, 8)

	DefaultBlockSize int32   = imageblk.DefaultBlockSize
	DefaultRes       float32 = imageblk.DefaultRes
	DefaultUnits             = imageblk.DefaultUnits
)

func init() {
	encodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint64,
			Label: TypeName,
		},
	}
	interpolable := false
	dtype = Type{imageblk.NewType(encodeFormat, interpolable)}
	dtype.Type.Name = TypeName
	dtype.Type.URL = RepoURL
	dtype.Type.Version = Version

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.Register(&dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// ZeroBytes returns a slice of bytes that represents the zero label.
func ZeroBytes() []byte {
	return zeroLabelBytes
}

func EncodeFormat() dvid.DataValues {
	return encodeFormat
}

// --- Labels64 Datatype -----

// Type uses imageblk data type by composition.
type Type struct {
	imageblk.Type
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// -------

// GetByUUID returns a pointer to labelblk data given a UUID and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelblk datatype!", name)
	}
	return data, nil
}

// GetByVersion returns a pointer to labelblk data given a UUID and data name.
func GetByVersion(v dvid.VersionID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByVersion(v, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelblk datatype!", name)
	}
	return data, nil
}

// LabelType specifies how the 64-bit label is organized, allowing some bytes to
// encode particular attributes.  For example, the "Raveler" LabelType includes
// the Z-axis coordinate.
type LabelType uint8

const (
	Standard64bit LabelType = iota

	// RavelerLabel uses the Z offset as the higher-order 4 bytes and the
	// superpixel label as the lower 4 bytes.
	RavelerLabel
)

func (lt LabelType) String() string {
	switch lt {
	case Standard64bit:
		return "standard labels"
	case RavelerLabel:
		return "raveler labels"
	default:
		return "unknown label types"
	}
}

// -------  ExtData interface implementation -------------

// Labels are voxels that have uint64 labels.
type Labels struct {
	*imageblk.Voxels
}

func (l *Labels) String() string {
	return fmt.Sprintf("Labels of size %s @ offset %s", l.Size(), l.StartPoint())
}

func (l *Labels) Interpolable() bool {
	return false
}

// Data of labelblk type is an extended form of imageblk Data
type Data struct {
	*imageblk.Data
	Labeling LabelType
}

// NewData returns a pointer to labelblk data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	imgblkData, err := dtype.Type.NewData(uuid, id, name, c)
	if err != nil {
		return nil, err
	}

	// Check if Raveler label.
	// TODO - Remove Raveler code outside of DVID.
	var labelType LabelType = Standard64bit
	s, found, err := c.GetString("LabelType")
	if found {
		switch strings.ToLower(s) {
		case "raveler":
			labelType = RavelerLabel
		case "standard":
		default:
			return nil, fmt.Errorf("unknown label type specified '%s'", s)
		}
	}

	dvid.Infof("Creating labelblk '%s' with %s", name, labelType)
	data := &Data{
		Data:     imgblkData,
		Labeling: labelType,
	}
	return data, nil
}

type propertiesT struct {
	imageblk.Properties
	Labeling LabelType
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended propertiesT
	}{
		&(d.Data.Data),
		propertiesT{
			d.Data.Properties,
			d.Labeling,
		},
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Labeling)); err != nil {
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
	if err := enc.Encode(d.Labeling); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- imageblk.IntData interface -------------

func (d *Data) BlockSize() dvid.Point {
	return d.Properties.BlockSize
}

func (d *Data) Extents() *dvid.Extents {
	return &(d.Properties.Extents)
}

// NewLabels returns labelblk Labels, a representation of externally usable subvolume
// or slice data, given some geometry and optional image data.
// If img is passed in, the function will initialize Voxels with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
//
// TODO : Unlike the standard imageblk.NewVoxels, the labelblk version can modify the
// labels based on the z-coordinate of the given geometry for Raveler labeling.
// This will be removed when Raveler-specific labels are moved outside DVID.
func (d *Data) NewLabels(geom dvid.Geometry, img interface{}) (*Labels, error) {
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	stride := geom.Size().Value(0) * bytesPerVoxel
	var data []byte

	if img == nil {
		numVoxels := geom.NumVoxels()
		if numVoxels <= 0 {
			return nil, fmt.Errorf("Illegal geometry requested: %s", geom)
		}
		requestSize := int64(bytesPerVoxel) * numVoxels
		if requestSize > server.MaxDataRequest {
			return nil, fmt.Errorf("Requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
				requestSize, server.MaxDataRequest)
		}
		data = make([]byte, requestSize)
	} else {
		switch t := img.(type) {
		case image.Image:
			var inputBytesPerVoxel, actualStride int32
			var err error
			data, inputBytesPerVoxel, actualStride, err = dvid.ImageData(t)
			if err != nil {
				return nil, err
			}
			if actualStride != stride {
				// Need to do some conversion here.
				switch d.Labeling {
				case Standard64bit:
					data, err = d.convertTo64bit(geom, data, int(inputBytesPerVoxel), int(actualStride))
					if err != nil {
						return nil, err
					}
				case RavelerLabel:
					data, err = d.addLabelZ(geom, data, actualStride)
					if err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unexpected label type in labelblk: %s", d.Labeling)
				}
			}
		case []byte:
			data = t
			actualLen := int64(len(data))
			expectedLen := int64(bytesPerVoxel) * geom.NumVoxels()
			if actualLen != expectedLen {
				return nil, fmt.Errorf("PUT data was %d bytes, expected %d bytes for %s",
					actualLen, expectedLen, geom)
			}
		default:
			return nil, fmt.Errorf("unexpected image type given to NewVoxels(): %T", t)
		}
	}

	labels := &Labels{
		imageblk.NewVoxels(geom, d.Properties.Values, data, stride),
	}
	return labels, nil
}

// Convert raw image data into a 2d array of 64-bit labels
func (d *Data) convertTo64bit(geom dvid.Geometry, data []uint8, bytesPerVoxel, stride int) ([]byte, error) {
	nx := int(geom.Size().Value(0))
	ny := int(geom.Size().Value(1))
	numBytes := nx * ny * 8
	data64 := make([]byte, numBytes, numBytes)

	var byteOrder binary.ByteOrder
	if geom.DataShape().ShapeDimensions() == 2 {
		byteOrder = binary.BigEndian // This is the default for PNG
	} else {
		byteOrder = binary.LittleEndian
	}

	switch bytesPerVoxel {
	case 1:
		dstI := 0
		for y := 0; y < ny; y++ {
			srcI := y * stride
			for x := 0; x < nx; x++ {
				binary.LittleEndian.PutUint64(data64[dstI:dstI+8], uint64(data[srcI]))
				srcI++
				dstI += 8
			}
		}
	case 2:
		dstI := 0
		for y := 0; y < ny; y++ {
			srcI := y * stride
			for x := 0; x < nx; x++ {
				value := byteOrder.Uint16(data[srcI : srcI+2])
				binary.LittleEndian.PutUint64(data64[dstI:dstI+8], uint64(value))
				srcI += 2
				dstI += 8
			}
		}
	case 4:
		dstI := 0
		for y := 0; y < ny; y++ {
			srcI := y * stride
			for x := 0; x < nx; x++ {
				value := byteOrder.Uint32(data[srcI : srcI+4])
				binary.LittleEndian.PutUint64(data64[dstI:dstI+8], uint64(value))
				srcI += 4
				dstI += 8
			}
		}
	case 8:
		dstI := 0
		for y := 0; y < ny; y++ {
			srcI := y * stride
			for x := 0; x < nx; x++ {
				value := byteOrder.Uint64(data[srcI : srcI+8])
				binary.LittleEndian.PutUint64(data64[dstI:dstI+8], uint64(value))
				srcI += 8
				dstI += 8
			}
		}
	default:
		return nil, fmt.Errorf("could not convert to 64-bit label given %d bytes/voxel", bytesPerVoxel)
	}
	return data64, nil
}

// Convert a 32-bit label into a 64-bit label by adding the Z coordinate into high 32 bits.
// Also drops the high byte (alpha channel) since Raveler labels only use 24-bits.
func (d *Data) addLabelZ(geom dvid.Geometry, data32 []uint8, stride int32) ([]byte, error) {
	if len(data32)%4 != 0 {
		return nil, fmt.Errorf("expected 4 byte/voxel alignment but have %d bytes!", len(data32))
	}
	coord := geom.StartPoint()
	if coord.NumDims() < 3 {
		return nil, fmt.Errorf("expected n-d (n >= 3) offset for image.  Got %d dimensions.",
			coord.NumDims())
	}
	superpixelBytes := make([]byte, 8, 8)
	binary.BigEndian.PutUint32(superpixelBytes[0:4], uint32(coord.Value(2)))

	nx := int(geom.Size().Value(0))
	ny := int(geom.Size().Value(1))
	numBytes := nx * ny * 8
	data64 := make([]byte, numBytes, numBytes)
	dstI := 0
	for y := 0; y < ny; y++ {
		srcI := y * int(stride)
		for x := 0; x < nx; x++ {
			if data32[srcI] == 0 && data32[srcI+1] == 0 && data32[srcI+2] == 0 {
				copy(data64[dstI:dstI+8], ZeroBytes())
			} else {
				superpixelBytes[5] = data32[srcI+2]
				superpixelBytes[6] = data32[srcI+1]
				superpixelBytes[7] = data32[srcI]
				copy(data64[dstI:dstI+8], superpixelBytes)
			}
			// NOTE: we skip the 4th byte (alpha) at srcI+3
			//a := uint32(data32[srcI+3])
			//b := uint32(data32[srcI+2])
			//g := uint32(data32[srcI+1])
			//r := uint32(data32[srcI+0])
			//spid := (b << 16) | (g << 8) | r
			srcI += 4
			dstI += 8
		}
	}
	return data64, nil
}

// RavelerSuperpixelBytes returns an encoded slice for Raveler (slice, superpixel) tuple.
// TODO -- Move Raveler-specific functions out of DVID.
func RavelerSuperpixelBytes(slice, superpixel32 uint32) []byte {
	b := make([]byte, 8, 8)
	if superpixel32 != 0 {
		binary.BigEndian.PutUint32(b[0:4], slice)
		binary.BigEndian.PutUint32(b[4:8], superpixel32)
	}
	return b
}

// --- datastore.DataService interface ---------

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	// Send the label voxel blocks
	if err := d.Data.Send(s, roiname, uuid); err != nil {
		return err
	}
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(req datastore.Request, reply *datastore.Response) error {
	switch req.TypeCommand() {
	case "load":
		if len(req.Command) < 5 {
			return fmt.Errorf("Poorly formatted load command.  See command-line help.")
		}
		// Parse the request
		var uuidStr, dataName, cmdStr, offsetStr string
		filenames, err := req.FilenameArgs(1, &uuidStr, &dataName, &cmdStr, &offsetStr)
		if err != nil {
			return err
		}
		if len(filenames) == 0 {
			return fmt.Errorf("Need to include at least one file to add: %s", req)
		}

		// Get offset
		offset, err := dvid.StringToPoint(offsetStr, ",")
		if err != nil {
			return fmt.Errorf("Illegal offset specification: %s: %s", offsetStr, err.Error())
		}

		// Get list of files to add
		var addedFiles string
		if len(filenames) == 1 {
			addedFiles = filenames[0]
		} else {
			addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
		}
		dvid.Debugf(addedFiles + "\n")

		uuid, versionID, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}
		repo, err := datastore.RepoFromUUID(uuid)
		if err != nil {
			return err
		}
		if err = repo.AddToLog(req.Command.String()); err != nil {
			return err
		}
		if err = d.LoadImages(versionID, offset, filenames); err != nil {
			return err
		}
		if err := datastore.SaveRepo(uuid); err != nil {
			return err
		}
		return nil

	case "composite":
		if len(req.Command) < 6 {
			return fmt.Errorf("Poorly formatted composite command.  See command-line help.")
		}
		return d.CreateComposite(req, reply)

	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), req.TypeCommand())
	}
	return nil
}

type Bounds struct {
	VoxelBounds *dvid.Bounds
	BlockBounds *dvid.Bounds
	Exact       bool // All RLEs must respect the voxel bounds.  If false, just screen on blocks.
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	repo, versions, err := datastore.FromContext(ctx)
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

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	switch action {
	case "get":
	case "post":
	default:
		server.BadRequest(w, r, "labelblk only handle GET or POST HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	// Get query strings and possible roi
	var roiptr *imageblk.ROI
	queryValues := r.URL.Query()
	roiname := dvid.InstanceName(queryValues.Get("roi"))
	if len(roiname) != 0 {
		roiptr = new(imageblk.ROI)
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && action == "post" {
		config, err := server.DecodeJSON(r)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := repo.Save(); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		fmt.Fprintf(w, "Changed '%s' based on received configuration:\n%s\n", d.DataName(), config)
		return
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

	case "metadata":
		jsonStr, err := d.NdDataMetadata()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "raw", "isotropic":
		if len(parts) < 7 {
			server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
			return
		}
		var isotropic bool = (parts[3] == "isotropic")
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if action != "get" {
				server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
				return
			}
			rawSlice, err := dvid.Isotropy2D(d.Properties.VoxelSize, slice, isotropic)
			lbl, err := d.NewLabels(rawSlice, nil)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if roiptr != nil {
				roiptr.Iter, err = roi.NewIterator(roiname, versionID, lbl)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			}
			img, err := d.GetImage(versionID, lbl.Voxels, roiptr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if isotropic {
				dstW := int(slice.Size().Value(0))
				dstH := int(slice.Size().Value(1))
				img, err = img.ScaleImage(dstW, dstH)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
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
			throttle := queryStrings.Get("throttle")
			if throttle == "true" || throttle == "on" {
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
				server.BadRequest(w, r, err.Error())
				return
			}
			if action == "get" {
				lbl, err := d.NewLabels(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, lbl)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				data, err := d.GetVolume(versionID, lbl.Voxels, roiptr)
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
			} else {
				if isotropic {
					server.BadRequest(w, r, "can only POST 'raw' not 'isotropic' images")
					return
				}
				// Make sure vox is block-aligned
				if !dvid.BlockAligned(subvol, d.BlockSize()) {
					server.BadRequest(w, r, "cannot store labels in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
					return
				}

				data, err := ioutil.ReadAll(r.Body)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				lbl, err := d.NewLabels(subvol, data)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, lbl)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				if err = d.PutVoxels(versionID, lbl.Voxels, roiptr); err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
			return
		}

	default:
		server.BadRequest(w, r, "Unrecognized API call '%s' for labelblk data '%s'.  See API help.",
			parts[3], d.DataName())
	}
}

// --------- Other functions on labelblk Data -----------------

// GetLabelBytesAtPoint returns the 8 byte slice corresponding to a 64-bit label at a point.
func (d *Data) GetLabelBytesAtPoint(v dvid.VersionID, pt dvid.Point) ([]byte, error) {
	store, err := storage.BigDataStore()
	if err != nil {
		return nil, err
	}

	// Compute the block key that contains the given point.
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("Can't determine block of point %s", pt)
	}
	blockSize := d.BlockSize()
	blockCoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)
	index := dvid.IndexZYX(blockCoord)
	blockIndex := NewIndex(&index)

	// Retrieve the block of labels
	ctx := datastore.NewVersionedContext(d, v)
	serialization, err := store.Get(ctx, blockIndex)
	if err != nil {
		return nil, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), blockCoord)
	}
	labelData, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("Unable to deserialize block %s in '%s': %s\n",
			blockCoord, d.DataName(), err.Error())
	}

	// Retrieve the particular label within the block.
	ptInBlock := coord.PointInChunk(blockSize)
	nx := int64(blockSize.Value(0))
	nxy := nx * int64(blockSize.Value(1))
	i := (int64(ptInBlock.Value(0)) + int64(ptInBlock.Value(1))*nx + int64(ptInBlock.Value(2))*nxy) * 8
	return labelData[i : i+8], nil
}

// GetLabelAtPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	labelBytes, err := d.GetLabelBytesAtPoint(v, pt)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(labelBytes), nil
}
