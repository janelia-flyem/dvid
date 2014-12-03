/*
	Package labels64 tailors the voxels data type for 64-bit labels and allows loading
	of NRGBA images (e.g., Raveler superpixel PNG images) that implicitly use slice Z as
	part of the label index.
*/
package labels64

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labels64"
	TypeName = "labels64"

	// Don't allow requests that will return more than this amount of data.
	MaxDataRequest = 3 * dvid.Giga
)

const HelpMessage = `
API for datatypes derived from labels64 (github.com/janelia-flyem/dvid/datatype/labels64)
=========================================================================

Note: Denormalizations like sparse volumes are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labels64 <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new labels64 superpixels VoxelSize=3.2,3.2,40.0

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "superpixels"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    LabelType      "standard" (default) or "raveler" 
    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")

$ dvid node <UUID> <data name> load <offset> <image glob> <settings...>

    Initializes version node to a set of XY label images described by glob of filenames.
    The DVID server must have access to the named files.  Currently, XY images are required.
    Note that how the loaded data is processed depends on the LabelType of this labels64 data.
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

$ dvid node <UUID> <data name> composite <grayscale8 data name> <new rgba8 data name>

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


GET  <api URL>/node/<UUID>/<data name>/schema

	Retrieves a JSON schema (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=on]
GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?throttle=on]
POST <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=on]

    Retrieves or puts label data as binary blob using schema above.  Binary data is simply
    packed 64-bit data.  See 'voxels' API for discussion of 'raw' versus 'isotropic'

    Example: 

    GET <api URL>/node/3f8c/superpixels/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in binary format.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response will be "application/octet-stream".

    Throttling can be enabled by passing a "throttle=on" query string.  Throttling makes sure
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

GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>

	Returns a sparse volume with voxels of the given label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Payload descriptor:
	               Bit 0 (LSB) - 8-bit grayscale
	               Bit 1 - 16-bit grayscale
	               Bit 2 - 16-bit normal
	               If set to all 0, there is no payload and it's a binary sparse volume.
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

POST <api URL>/node/<UUID>/<data name>/merge

	Merges labels.  Requires JSON in request body using the following format:

	[ [toLabel1, fromLabel1, fromLabel2, fromLabel3, ...],
	  [toLabel2, fromLabel4, fromLabel5, ...], 
	  ... ]

	Each element of the JSON array is another array specifying all the labels that
	should be merged into the label specified by the first element.


POST <api URL>/node/<UUID>/<data name>/split

	Splits a portion of a label's voxels into a new label.  Returns the following JSON:

		{ "label": <new label> }

	This request requires a binary sparse volume in the POSTed body with the following 
	encoded RLE format, which is compatible with the format returned by a GET on the 
	"sparsevol" endpoint described above:

		All integers are in little-endian format.

	    byte     Payload descriptor:
	               Set to 0 to indicate it's a binary sparse volume.
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
`

var (
	dtype        *Type
	encodeFormat dvid.DataValues
)

func init() {
	encodeFormat = dvid.DataValues{
		{
			T:     dvid.T_uint64,
			Label: TypeName,
		},
	}
	interpolable := false
	dtype = &Type{voxels.NewType(encodeFormat, interpolable)}
	dtype.Type.Name = TypeName
	dtype.Type.URL = RepoURL
	dtype.Type.Version = Version

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.Register(dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

func EncodeFormat() dvid.DataValues {
	return encodeFormat
}

// --- Labels64 Datatype -----

// Type just uses voxels data type by composition.
type Type struct {
	*voxels.Type
}

// NewData returns a pointer to labels64 data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (*Data, error) {
	voxelData, err := dtype.Type.NewData(uuid, id, name, c)
	if err != nil {
		return nil, err
	}

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
	dvid.Infof("Creating labels64 '%s' with %s", voxelData.DataName(), labelType)
	data := &Data{
		Data:     voxelData,
		Labeling: labelType,
	}
	return data, nil
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// -------

// GetByUUID returns a pointer to labels64 data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.DataString) (*Data, error) {
	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	source, err := repo.GetDataByName(name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labels64 datatype!", name)
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

// Labels is an image volume that fulfills the voxels.ExtData interface.
type Labels struct {
	*voxels.Voxels
}

func (l *Labels) String() string {
	return fmt.Sprintf("Labels of size %s @ offset %s", l.Size(), l.StartPoint())
}

func (l *Labels) Interpolable() bool {
	return false
}

// Data of labels64 type just uses voxels.Data.
type Data struct {
	*voxels.Data
	Labeling LabelType
	Ready    bool
}

type propertiesT struct {
	voxels.Properties
	Labeling LabelType
	Ready    bool
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
			d.Ready,
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
	if err := dec.Decode(&(d.Ready)); err != nil {
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
	if err := enc.Encode(d.Ready); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- voxels.IntData interface -------------

// NewExtHandler returns a labels64 ExtData given some geometry and optional image data.
// If img is passed in, the function will initialize the ExtData with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
// Unlike the standard voxels NewExtHandler, the labels64 version will modify the
// labels based on the z-coordinate of the given geometry.
func (d *Data) NewExtHandler(geom dvid.Geometry, img interface{}) (voxels.ExtData, error) {
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	stride := geom.Size().Value(0) * bytesPerVoxel
	var data []byte

	if img == nil {
		numVoxels := geom.NumVoxels()
		if numVoxels <= 0 {
			return nil, fmt.Errorf("Illegal geometry requested: %s", geom)
		}
		requestSize := int64(bytesPerVoxel) * numVoxels
		if requestSize > MaxDataRequest {
			return nil, fmt.Errorf("Requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
				requestSize, MaxDataRequest)
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
					return nil, fmt.Errorf("unexpected label type in labels64: %s", d.Labeling)
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
			return nil, fmt.Errorf("unexpected image type given to NewExtHandler(): %T", t)
		}
	}

	labels := &Labels{
		voxels.NewVoxels(geom, d.Properties.Values, data, stride, d.ByteOrder),
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
				d.ByteOrder.PutUint64(data64[dstI:dstI+8], uint64(data[srcI]))
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
				d.ByteOrder.PutUint64(data64[dstI:dstI+8], uint64(value))
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
				d.ByteOrder.PutUint64(data64[dstI:dstI+8], uint64(value))
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
				d.ByteOrder.PutUint64(data64[dstI:dstI+8], uint64(value))
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

	// Send command to do denormalization afterwards if necessary.
	if d.Labeling == RavelerLabel {
		return nil
	}
	params := postProcData{
		Name: d.DataName(),
		UUID: uuid,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(params); err != nil {
		return err
	}
	if err := s.SendPostProc(CommandLabels64Denorm, buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "load":
		if len(request.Command) < 5 {
			return fmt.Errorf("Poorly formatted load command.  See command-line help.")
		}
		// Parse the request
		var uuidStr, dataName, cmdStr, offsetStr string
		filenames, err := request.FilenameArgs(1, &uuidStr, &dataName, &cmdStr, &offsetStr)
		if err != nil {
			return err
		}
		if len(filenames) == 0 {
			return fmt.Errorf("Need to include at least one file to add: %s", request)
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
		if err = repo.AddToLog(request.Command.String()); err != nil {
			return err
		}

		err = voxels.LoadImages(versionID, d, offset, filenames)
		if err != nil {
			return err
		}

		// Perform denormalizations if requested.
		processing, _, err := request.Command.Settings().GetString("proc")
		if err != nil {
			return err
		}
		if d.Labeling != RavelerLabel && processing != "noindex" {
			go d.ProcessSpatially(uuid)
		} else {
			d.Ready = true
			if err := datastore.SaveRepo(uuid); err != nil {
				return err
			}
		}
		return nil

	case "composite":
		if len(request.Command) < 6 {
			return fmt.Errorf("Poorly formatted composite command.  See command-line help.")
		}
		return d.CreateComposite(request, reply)

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

	// Get query strings and possible roi
	var roiptr *voxels.ROI
	queryValues := r.URL.Query()
	roiname := dvid.DataString(queryValues.Get("roi"))
	if len(roiname) != 0 {
		roiptr = new(voxels.ROI)
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && op == voxels.PutOp {
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
			if op == voxels.PutOp {
				if isotropic {
					server.BadRequest(w, r, "can only PUT 'raw' not 'isotropic' images")
					return
				}
				// TODO -- Put in format checks for POSTed image.
				postedImg, _, err := image.Decode(r.Body)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				e, err := d.NewExtHandler(slice, postedImg)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				modsChan := make(voxels.BlockChannel)
				go d.denormFunc(versionID, modsChan)
				var opts voxels.OpOptions
				opts.SetROI(roiptr)
				opts.SetModsChannel(modsChan)
				err = voxels.PutVoxels(storeCtx, d, e, opts)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			} else {
				rawSlice, err := d.HandleIsotropy2D(slice, isotropic)
				e, err := d.NewExtHandler(rawSlice, nil)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				img, err := voxels.GetImage(storeCtx, d, e, roiptr)
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
				server.BadRequest(w, r, err.Error())
				return
			}
			if op == voxels.GetOp {
				e, err := d.NewExtHandler(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				data, err := voxels.GetVolume(storeCtx, d, e, roiptr)
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
					server.BadRequest(w, r, "can only PUT 'raw' not 'isotropic' images")
					return
				}
				data, err := ioutil.ReadAll(r.Body)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				e, err := d.NewExtHandler(subvol, data)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				modsChan := make(voxels.BlockChannel)
				go d.denormFunc(versionID, modsChan)
				var opts voxels.OpOptions
				opts.SetROI(roiptr)
				opts.SetModsChannel(modsChan)
				err = voxels.PutVoxels(storeCtx, d, e, opts)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
			return
		}

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
		data, err := GetSparseVol(storeCtx, label)
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
		data, err := GetSparseVol(storeCtx, label)
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
		label, err := d.GetLabelAtPoint(storeCtx, coord)
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
		jsonStr, err := GetSizeRange(d, versionID, minSize, maxSize)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		timedLog.Infof("HTTP %s: get labels with volume > %d and < %d (%s)", r.Method, minSize, maxSize, r.URL)

	case "split":
		// POST <api URL>/node/<UUID>/<data name>/split
		if action != "post" {
			server.BadRequest(w, r, "Split requests must be POST actions.")
			return
		}
		timedLog.Infof("HTTP split request (%s)", r.URL)

	case "merge":
		// POST <api URL>/node/<UUID>/<data name>/merge
		if action != "post" {
			server.BadRequest(w, r, "Merge requests must be POST actions.")
			return
		}
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, "Bad POSTed data for merge.  Should be JSON.")
			return
		}
		var tuples MergeTuples
		if err := json.Unmarshal(data, &tuples); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Bad merge op JSON: %s", err.Error()))
			return
		}
		if err := d.MergeLabels(storeCtx, tuples); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on merge: %s", err.Error()))
			return
		}
		timedLog.Infof("HTTP merge request (%s)", r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call '%s' for labels64 data '%s'.  See API help.",
			parts[3], d.DataName())
	}
}

// GetLabelBytesAtPoint returns the 8 byte slice corresponding to a 64-bit label at a point.
func (d *Data) GetLabelBytesAtPoint(ctx storage.Context, pt dvid.Point) ([]byte, error) {
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
	blockIndex := voxels.NewVoxelBlockIndex(&index)

	// Retrieve the block of labels
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
func (d *Data) GetLabelAtPoint(ctx storage.Context, pt dvid.Point) (uint64, error) {
	labelBytes, err := d.GetLabelBytesAtPoint(ctx, pt)
	if err != nil {
		return 0, err
	}
	return d.Properties.ByteOrder.Uint64(labelBytes), nil
}

type blockOp struct {
	grayscale *voxels.Data
	composite *voxels.Data
	versionID dvid.VersionID
}

// CreateComposite creates a new rgba8 image by combining hash of labels + the grayscale
func (d *Data) CreateComposite(request datastore.Request, reply *datastore.Response) error {
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, grayscaleName, destName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &grayscaleName, &destName)

	// Get the version
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Get this repo and log request.
	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	if err = repo.AddToLog(request.Command.String()); err != nil {
		return err
	}

	// Get the grayscale data.
	dataservice, err := repo.GetDataByName(dvid.DataString(grayscaleName))
	if err != nil {
		return err
	}
	grayscale, ok := dataservice.(*voxels.Data)
	if !ok {
		return fmt.Errorf("%s is not the name of grayscale8 data", grayscaleName)
	}

	// Create a new rgba8 data.
	var compservice datastore.DataService
	compservice, err = repo.GetDataByName(dvid.DataString(destName))
	if err == nil {
		return fmt.Errorf("Data instance with name %q already exists", destName)
	}
	typeService, err := datastore.TypeServiceByName("rgba8")
	if err != nil {
		return fmt.Errorf("Could not get rgba8 type service from DVID")
	}
	config := dvid.NewConfig()
	compservice, err = repo.NewData(typeService, dvid.DataString(destName), config)
	if err != nil {
		return err
	}
	composite, ok := compservice.(*voxels.Data)
	if !ok {
		return fmt.Errorf("Error: %s was unable to be set to rgba8 data", destName)
	}

	// Iterate through all labels and grayscale chunks incrementally in Z, a layer at a time.
	wg := new(sync.WaitGroup)
	op := &blockOp{grayscale, composite, versionID}
	chunkOp := &storage.ChunkOp{op, wg}

	store, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	ctx := datastore.NewVersionedContext(d, versionID)
	extents := d.Extents()
	blockBeg := voxels.NewVoxelBlockIndex(extents.MinIndex)
	blockEnd := voxels.NewVoxelBlockIndex(extents.MaxIndex)
	err = store.ProcessRange(ctx, blockBeg, blockEnd, chunkOp, d.CreateCompositeChunk)
	wg.Wait()

	// Set new mapped data to same extents.
	composite.Properties.Extents = grayscale.Properties.Extents
	if err := repo.Save(); err != nil {
		dvid.Infof("Could not save new data '%s': %s\n", destName, err.Error())
	}

	timedLog.Infof("Created composite of %s and %s", grayscaleName, destName)
	return nil
}

// CreateCompositeChunk processes each chunk of labels and grayscale data,
// saving the composited result into an rgba8.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) CreateCompositeChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.createCompositeChunk(chunk)
}

var curZ int32
var curZMutex sync.Mutex

func (d *Data) createCompositeChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*blockOp)

	// Get the spatial index associated with this chunk.
	zyx, err := voxels.DecodeVoxelBlockKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error in %s.ChunkApplyMap(): %s", d.Data.DataName(), err.Error())
		return
	}
	blockIndex := voxels.NewVoxelBlockIndex(zyx)

	// Initialize the label buffers.  For voxels, this data needs to be uncompressed and deserialized.
	curZMutex.Lock()
	if zyx[2] > curZ {
		curZ = zyx[2]
		minZ := zyx.MinPoint(d.BlockSize()).Value(2)
		maxZ := zyx.MaxPoint(d.BlockSize()).Value(2)
		dvid.Debugf("Now creating composite blocks for Z %d to %d\n", minZ, maxZ)
	}
	curZMutex.Unlock()

	labelData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}
	blockBytes := len(labelData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}

	// Get the corresponding grayscale block.
	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Unable to retrieve big data store: %s\n", err.Error())
		return
	}
	grayscaleCtx := datastore.NewVersionedContext(op.grayscale, op.versionID)
	blockData, err := bigdata.Get(grayscaleCtx, blockIndex)
	if err != nil {
		dvid.Errorf("Error getting grayscale block for index %s\n", zyx)
		return
	}
	grayscaleData, _, err := dvid.DeserializeData(blockData, true)
	if err != nil {
		dvid.Errorf("Unable to deserialize block in '%s': %s\n", op.grayscale.DataName(), err.Error())
		return
	}

	// Compute the composite block.
	// TODO -- Exploit run lengths, use cache of hash?
	compositeBytes := blockBytes / 2
	compositeData := make([]byte, compositeBytes, compositeBytes)
	compositeI := 0
	labelI := 0
	hashBuf := make([]byte, 4, 4)
	for _, grayscale := range grayscaleData {
		//murmurhash3(labelData[labelI:labelI+8], hashBuf)
		//hashBuf[3] = grayscale
		writePseudoColor(grayscale, labelData[labelI:labelI+8], hashBuf)
		copy(compositeData[compositeI:compositeI+4], hashBuf)
		compositeI += 4
		labelI += 8
	}

	// Store the composite block into the rgba8 data.
	serialization, err := dvid.SerializeData(compositeData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize composite block %s: %s\n", zyx, err.Error())
		return
	}
	compositeCtx := datastore.NewVersionedContext(op.composite, op.versionID)
	err = bigdata.Put(compositeCtx, blockIndex, serialization)
	if err != nil {
		dvid.Errorf("Unable to PUT composite block %s: %s\n", zyx, err.Error())
		return
	}
}

func writePseudoColor(grayscale uint8, in64bits, out32bits []byte) {
	murmurhash3(in64bits, out32bits)
	var t uint64
	t = uint64(out32bits[0]) * uint64(grayscale)
	t >>= 8
	out32bits[0] = uint8(t)
	t = uint64(out32bits[1]) * uint64(grayscale)
	t >>= 8
	out32bits[1] = uint8(t)
	t = uint64(out32bits[2]) * uint64(grayscale)
	t >>= 8
	out32bits[2] = uint8(t)
	out32bits[3] = 255
}

func murmurhash3(in64bits, out32bits []byte) {
	length := len(in64bits)
	var c1, c2 uint32 = 0xcc9e2d51, 0x1b873593
	nblocks := length / 4
	var h, k uint32
	buf := bytes.NewBuffer(in64bits)
	for i := 0; i < nblocks; i++ {
		binary.Read(buf, binary.LittleEndian, &k)
		k *= c1
		k = (k << 15) | (k >> (32 - 15))
		k *= c2
		h ^= k
		h = (h << 13) | (h >> (32 - 13))
		h = (h * 5) + 0xe6546b64
	}
	k = 0
	tailIndex := nblocks * 4
	switch length & 3 {
	case 3:
		k ^= uint32(in64bits[tailIndex+2]) << 16
		fallthrough
	case 2:
		k ^= uint32(in64bits[tailIndex+1]) << 8
		fallthrough
	case 1:
		k ^= uint32(in64bits[tailIndex])
		k *= c1
		k = (k << 15) | (k >> (32 - 15))
		k *= c2
		h ^= k
	}
	h ^= uint32(length)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	binary.BigEndian.PutUint32(out32bits, h)
}
