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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"compress/gzip"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	lz4 "github.com/janelia-flyem/go/golz4"
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

    Sync           Name of labelvol data that should sync with this labelblk data.
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
    				 volumes and size query responses using the loaded labels.

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

    GET <api URL>/node/3f8c/segmentation/info

    Returns JSON with configuration settings that include location in DVID space and
    min/max block indices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET  <api URL>/node/<UUID>/<data name>/metadata

	Retrieves a JSON schema (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/segmentation/isotropic/0_1/512_256/0_0_100/jpg:80

    Returns an isotropic XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    Additional processing is applied based on voxel resolutions to make sure the retrieved image 
    has isotropic pixels.  For example, if an XZ image is requested and the image volume has 
    X resolution 3 nm and Z resolution 40 nm, the returned image's height will be magnified 40/3
    relative to the raw data.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream".

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    roi       	  Name of roi data instance used to mask the requested data.
    compression   Allows retrieval or submission of 3d data in "lz4" and "gzip"
                    compressed format.  The 2d data will ignore this and use
                    the image-based codec.
    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
    				(all API calls that can be throttled) are handled.  If the server can't initiate the API 
    				call right away, a 503 (Service Unavailable) status code is returned.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/segmentation/raw/0_1/512_256/0_0_100/jpg:80

    Returns a raw XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    By "raw", we mean that no additional processing is applied based on voxel
    resolutions to make sure the retrieved image has isotropic pixels.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream". 

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Example: "0_2" is XZ, and "0_1_2" is a 3d subvolume.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    roi           Name of roi data instance used to mask the requested data.
    compression   Allows retrieval or submission of 3d data in "lz4" and "gzip"
                    compressed format.  The 2d data will ignore this and use
                    the image-based codec.
    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
    				(all API calls that can be throttled) are handled.  If the server can't initiate the API 
    				call right away, a 503 (Service Unavailable) status code is returned.


POST <api URL>/node/<UUID>/<data name>/raw/0_1_2/<size>/<offset>[?queryopts]

    Puts block-aligned voxel data using the block sizes defined for  this data instance.  
    For example, if the BlockSize = 32, offset and size must by multiples of 32.

    Example: 

    POST <api URL>/node/3f8c/segmentation/raw/0_1_2/512_256_128/0_0_32

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    roi           Name of roi data instance used to mask the requested data.
    mutate        Default "false" corresponds to ingestion, i.e., the first write of the given block.
                    Use "true" to indicate the POST is a mutation of prior data, which allows any
                    synced data instance to cleanup prior denormalizations.  If "mutate=true", the
                    POST operations will be slower due to a required GET to retrieve past data.
    compression   Allows retrieval or submission of 3d data in "lz4" and "gzip"
                    compressed format.
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be throttled) 
                    are handled.  If the server can't initiate the API call right away, a 503 (Service Unavailable) 
                    status code is returned.

GET  <api URL>/node/<UUID>/<data name>/pseudocolor/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves label data as pseudocolored 2D PNG color images where each label hashed to a different RGB.

    Example: 

    GET <api URL>/node/3f8c/segmentation/pseudocolor/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction.  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    roi       	  Name of roi data instance used to mask the requested data.
    compression   Allows retrieval or submission of 3d data in "lz4" and "gzip"
                    compressed format.
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be throttled) 
                    are handled.  If the server can't initiate the API call right away, a 503 (Service Unavailable) 
                    status code is returned.

GET <api URL>/node/<UUID>/<data name>/label/<coord>

	Returns JSON for the label at the given coordinate:
	{ "Label": 23 }
	
    Arguments:
    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30

POST <api URL>/node/<UUID>/<data name>/labels

	Returns JSON for the labels at a list of coordinates.  Expects JSON in POST body:

	[ [x0, y0, z0], [x1, y1, z1], ...]

	Returns for each POSTed coordinate the corresponding label:

	[ 23, 911, ...]
	
    Arguments:
    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.

DELETE <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>

    Deletes "spanX" blocks of label data along X starting from given block coordinate.
    This will delete both the labelblk as well as any associated labelvol structures within this block.

    Example: 

    GET <api URL>/node/3f8c/segmentation/blocks/10_20_30/8

    Delete 8 blocks where first block has given block coordinate and number
    of blocks returned along x axis is "spanX". 

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    block coord   The block coordinate of the first block in X_Y_Z format.  Block coordinates
                  can be derived from voxel coordinates by dividing voxel coordinates by
                  the block size for a data type.

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
	datastore.Updater
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) ||
		d.Labeling != d2.Labeling {
		return false
	}
	return true
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
		d.Data.Data,
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
				return nil, fmt.Errorf("labels data was %d bytes, expected %d bytes for %s",
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

func (d *Data) DeleteBlocks(ctx *datastore.VersionedCtx, start dvid.ChunkPoint3d, span int) error {
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type labelblk had error initializing store: %v\n", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelblk requires batch-enabled store, which %q is not\n", store)
	}

	indexBeg := dvid.IndexZYX(start)
	end := start
	end[0] += int32(span - 1)
	indexEnd := dvid.IndexZYX(end)
	begTKey := NewTKey(&indexBeg)
	endTKey := NewTKey(&indexEnd)

	iv := dvid.InstanceVersion{d.DataName(), ctx.VersionID()}
	mapping := labels.MergeCache.LabelMap(iv)

	kvs, err := store.GetRange(ctx, begTKey, endTKey)
	if err != nil {
		return err
	}

	batch := batcher.NewBatch(ctx)
	uncompress := true
	for _, kv := range kvs {
		izyx, err := DecodeTKey(kv.K)
		if err != nil {
			return err
		}

		// Delete the labelblk (really tombstones it)
		batch.Delete(kv.K)

		// Send data to delete associated labelvol for labels in this block
		block, _, err := dvid.DeserializeData(kv.V, uncompress)
		if err != nil {
			return fmt.Errorf("Unable to deserialize block, %s (%v): %v", ctx, kv.K, err)
		}
		if mapping != nil {
			n := len(block) / 8
			for i := 0; i < n; i++ {
				orig := binary.LittleEndian.Uint64(block[i*8 : i*8+8])
				mapped, found := mapping.FinalLabel(orig)
				if !found {
					mapped = orig
				}
				binary.LittleEndian.PutUint64(block[i*8:i*8+8], mapped)
			}
		}

		// Notify any subscribers that we've deleted this block.
		evt := datastore.SyncEvent{d.DataName(), labels.DeleteBlockEvent}
		msg := datastore.SyncMessage{ctx.VersionID(), labels.DeleteBlock{izyx, block}}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			return err
		}

	}
	return batch.Commit()
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
			return fmt.Errorf("Illegal offset specification: %s: %v", offsetStr, err)
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
		if err = datastore.AddToNodeLog(uuid, []string{req.Command.String()}); err != nil {
			return err
		}
		if err = d.LoadImages(versionID, offset, filenames); err != nil {
			return err
		}
		if err := datastore.SaveDataByUUID(uuid, d); err != nil {
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

func colorImage(labels *dvid.Image) (image.Image, error) {
	if labels == nil || labels.Which != 3 || labels.NRGBA64 == nil {
		return nil, fmt.Errorf("writePseudoColor can't use labels image with wrong format: %v\n", labels)
	}
	src := labels.NRGBA64
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	dst := image.NewNRGBA(image.Rect(0, 0, srcW, srcH))

	for y := 0; y < srcH; y++ {
		srcI := src.PixOffset(0, y)
		dstI := dst.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			murmurhash3(src.Pix[srcI:srcI+8], dst.Pix[dstI:dstI+4])
			dst.Pix[dstI+3] = 255

			srcI += 8
			dstI += 4
		}
	}
	return dst, nil
}

func sendBinaryData(compression string, data []byte, w http.ResponseWriter) error {
	var err error
	w.Header().Set("Content-type", "application/octet-stream")
	switch compression {
	case "":
		_, err = w.Write(data)
		if err != nil {
			return err
		}
	case "lz4":
		compressed := make([]byte, lz4.CompressBound(data))
		var n, outSize int
		if outSize, err = lz4.Compress(data, compressed); err != nil {
			return err
		}
		compressed = compressed[:outSize]
		if n, err = w.Write(compressed); err != nil {
			return err
		}
		if n != outSize {
			errmsg := fmt.Sprintf("Only able to write %d of %d lz4 compressed bytes\n", n, outSize)
			dvid.Errorf(errmsg)
			return err
		}
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err = gw.Write(data); err != nil {
			return err
		}
		if err = gw.Close(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression type %q", compression)
	}
	return nil
}

func getBinaryData(compression string, in io.ReadCloser, estsize int64) ([]byte, error) {
	var err error
	var data []byte
	switch compression {
	case "":
		tlog := dvid.NewTimeLog()
		data, err = ioutil.ReadAll(in)
		if err != nil {
			return nil, err
		}
		tlog.Debugf("read 3d uncompressed POST")
	case "lz4":
		tlog := dvid.NewTimeLog()
		data, err = ioutil.ReadAll(in)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("received 0 LZ4 compressed bytes")
		}
		tlog.Debugf("read 3d lz4 POST")
		tlog = dvid.NewTimeLog()
		uncompressed := make([]byte, estsize)
		err = lz4.Uncompress(data, uncompressed)
		if err != nil {
			return nil, err
		}
		data = uncompressed
		tlog.Debugf("uncompressed 3d lz4 POST")
	case "gzip":
		tlog := dvid.NewTimeLog()
		gr, err := gzip.NewReader(in)
		if err != nil {
			return nil, err
		}
		data, err = ioutil.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		if err = gr.Close(); err != nil {
			return nil, err
		}
		tlog.Debugf("read and uncompress 3d gzip POST")
	default:
		return nil, fmt.Errorf("unknown compression type %q", compression)
	}
	return data, nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// TODO -- Refactor this method to break it up and make it simpler.  Use the web routing for the endpoints.

	timedLog := dvid.NewTimeLog()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	switch action {
	case "get":
	case "post":
	case "delete":
	default:
		server.BadRequest(w, r, "labelblk only handles GET, POST, and DELETE HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	// Get query strings and possible roi
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && action == "post" {
		config, err := server.DecodeJSON(r)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := datastore.SaveDataByUUID(uuid, d); err != nil {
			server.BadRequest(w, r, err)
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
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "label":
		// GET <api URL>/node/<UUID>/<data name>/label/<coord>
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires coord to follow 'label' command")
			return
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		label, err := d.GetLabelAtPoint(ctx.VersionID(), coord)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		jsonStr := fmt.Sprintf(`{"Label": %d}`, label)
		fmt.Fprintf(w, jsonStr)
		timedLog.Infof("HTTP %s: label at %s (%s)", r.Method, coord, r.URL)

	case "labels":
		// POST <api URL>/node/<UUID>/<data name>/labels
		if action != "post" {
			server.BadRequest(w, r, "Batch labels query must be a POST request")
			return
		}
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, "Bad POSTed data for batch reverse query.  Should be JSON.")
			return
		}
		var coords []dvid.Point3d
		if err := json.Unmarshal(data, &coords); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Bad labels request JSON: %v", err))
			return
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, "[")
		sep := false
		for _, coord := range coords {
			label, err := d.GetLabelAtPoint(ctx.VersionID(), coord)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if sep {
				fmt.Fprintf(w, ",")
			}
			fmt.Fprintf(w, "%d", label)
			sep = true
		}
		fmt.Fprintf(w, "]")
		timedLog.Infof("HTTP batch label-at-point query (%d coordinates)", len(coords))

	case "blocks":
		// DELETE <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		if len(parts) < 6 {
			server.BadRequest(w, r, "DVID requires starting block coord and # of blocks along X to follow 'blocks' endpoint.")
			return
		}
		blockCoord, err := dvid.StringToChunkPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		span, err := strconv.Atoi(parts[5])
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if action != "delete" {
			server.BadRequest(w, r, "DVID currently accepts only DELETE on the 'blocks' endpoint")
			return
		}
		if err := d.DeleteBlocks(ctx, blockCoord, span); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: delete %d blocks from %s (%s)", r.Method, span, blockCoord, r.URL)

	case "pseudocolor":
		if len(parts) < 7 {
			server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
			return
		}
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if action != "get" {
				server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
				return
			}
			lbl, err := d.NewLabels(slice, nil)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			img, err := d.GetImage(ctx.VersionID(), lbl, roiname)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}

			// Convert to pseudocolor
			pseudoColor, err := colorImage(img)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}

			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			var formatStr string
			if len(parts) >= 8 {
				formatStr = parts[7]
			}
			err = dvid.WriteImageHttp(w, pseudoColor, formatStr)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, plane, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports only 2d pseudocolor image requests")
			return
		}

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
			server.BadRequest(w, r, err)
			return
		}
		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if action != "get" {
				server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
				return
			}
			rawSlice, err := dvid.Isotropy2D(d.Properties.VoxelSize, slice, isotropic)
			lbl, err := d.NewLabels(rawSlice, nil)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			img, err := d.GetImage(ctx.VersionID(), lbl, roiname)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if isotropic {
				dstW := int(slice.Size().Value(0))
				dstH := int(slice.Size().Value(1))
				img, err = img.ScaleImage(dstW, dstH)
				if err != nil {
					server.BadRequest(w, r, err)
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
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, plane, r.URL)
		case 3:
			if queryStrings.Get("throttle") == "on" {
				if server.ThrottledHTTP(w) {
					return
				}
				defer server.ThrottledOpDone()
			}
			compression := queryStrings.Get("compression")
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if action == "get" {
				lbl, err := d.NewLabels(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				data, err := d.GetVolume(ctx.VersionID(), lbl, roiname)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				if err := sendBinaryData(compression, data, w); err != nil {
					server.BadRequest(w, r, err)
					return
				}
			} else {
				if isotropic {
					server.BadRequest(w, r, "can only POST 'raw' not 'isotropic' images")
					return
				}
				// Make sure posted subvolume is block-aligned
				if !dvid.BlockAligned(subvol, d.BlockSize()) {
					server.BadRequest(w, r, "cannot store labels in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
					return
				}
				estsize := subvol.NumVoxels() * 8
				data, err := getBinaryData(compression, r.Body, estsize)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				lbl, err := d.NewLabels(subvol, data)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				if queryStrings.Get("mutate") == "true" {
					if err = d.MutateVoxels(ctx.VersionID(), lbl.Voxels, roiname); err != nil {
						server.BadRequest(w, r, err)
						return
					}
				} else {
					if err = d.IngestVoxels(ctx.VersionID(), lbl.Voxels, roiname); err != nil {
						server.BadRequest(w, r, err)
						return
					}
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
			return
		}

	default:
		server.BadRequest(w, r, "Unrecognized API call %q for labelblk data %q.  See API help.",
			parts[3], d.DataName())
	}
}

// --------- Other functions on labelblk Data -----------------

// GetLabelBytesAtPoint returns the 8 byte slice corresponding to a 64-bit label at a point.
func (d *Data) GetLabelBytesAtPoint(v dvid.VersionID, pt dvid.Point) ([]byte, error) {
	store, err := storage.MutableStore()
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

	// Retrieve the block of labels
	ctx := datastore.NewVersionedCtx(d, v)
	serialization, err := store.Get(ctx, NewTKey(&index))
	if err != nil {
		return nil, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), blockCoord)
	}
	if serialization == nil {
		return zeroLabelBytes, nil
	}
	labelData, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("Unable to deserialize block %s in '%s': %v\n", blockCoord, d.DataName(), err)
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
