/*
	Package imageblk implements DVID support for image blocks of various formats (uint8, uint16, rgba8).
    For label data, use labelblk.
*/
package imageblk

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.2"
	RepoURL = "github.com/janelia-flyem/dvid/datatype/imageblk"
)

const HelpMessage = `
API for image block datatype (github.com/janelia-flyem/dvid/datatype/imageblk)
==============================================================================

Note that different data types are available:

    uint8blk
    rgba8blk

Command-line:

$ dvid repo <UUID> new imageblk <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new uint8blk mygrayscale BlockSize=32,32,32 Res=3.2,3.2,40.0

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    type name      Data type name, e.g., "uint8"
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: %s)
    VoxelUnits     Resolution units (default: "nanometers")
    Background     Integer value that signifies background in any element (default: 0)

$ dvid node <UUID> <data name> load <offset> <image glob>

    Initializes version node to a set of XY images described by glob of filenames.  The
    DVID server must have access to the named files.  Currently, XY images are required.

    Example: 

    $ dvid node 3f8c mygrayscale load 0,0,100 data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png

$ dvid node <UUID> <data name> put local  <plane> <offset> <image glob>
$ dvid node <UUID> <data name> put remote <plane> <offset> <image glob>

    Adds image data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").  If possible, use the
    "load" command instead because it is much more efficient.

    Example: 

    $ dvid node 3f8c mygrayscale put local xy 0,0,100 data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png
	

$ dvid node <UUID> <data name> roi <new roi data name> <background values separated by comma> 

    Creates a ROI consisting of all voxel blocks that are non-background.

    Example:

    $ dvid node 3f8c mygrayscale roi grayscale_roi 0,255

    
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

GET <api URL>/node/<UUID>/<data name>/rawkey?x=<block x>&y=<block y>&z=<block z>

    Returns JSON describing hex-encoded binary key used to store a block of data at the given block coordinate:

    {
        "Key": "FF3801AD78BBD4829A3"
    }

    The query options for block x, y, and z must be supplied or this request will return an error.

GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/grayscale/isotropic/0_1/512_256/0_0_100/jpg:80

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

    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
                    (all API calls that can be throttled) are handled.  If the server can't initiate the API 
                    call right away, a 503 (Service Unavailable) status code is returned.

GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/grayscale/raw/0_1/512_256/0_0_100/jpg:80

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
    attenuation   For attenuation n, this reduces the intensity of voxels outside ROI by 2^n.
                  Valid range is n = 1 to n = 7.  Currently only implemented for 8-bit voxels.
                  Default is to zero out voxels outside ROI.
    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
                    (all API calls that can be throttled) are handled.  If the server can't initiate the API 
                    call right away, a 503 (Service Unavailable) status code is returned.

POST <api URL>/node/<UUID>/<data name>/raw/0_1_2/<size>/<offset>[?queryopts]

    Puts block-aligned voxel data using the block sizes defined for  this data instance.  
    For example, if the BlockSize = 32, offset and size must by multiples of 32.

    Example: 

    POST <api URL>/node/3f8c/grayscale/raw/0_1_2/512_256_128/0_0_32

    Throttling can be enabled by passing a "throttle=true" query string.  Throttling makes sure
    only one compute-intense operation (all API calls that can be throttled) is handled.
    If the server can't initiate the API call right away, a 503 (Service Unavailable) status
    code is returned.

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
    throttle      If "true", makes sure only N compute-intense operation 
                    (all API calls that can be throttled) are handled.  If the server can't initiate the API 
                    call right away, a 503 (Service Unavailable) status code is returned.

GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>][?queryopts]

    Retrieves non-orthogonal (arbitrarily oriented planar) image data of named 3d data 
    within a version node.  Returns an image where the top left pixel corresponds to the
    real world coordinate (not in voxel space but in space defined by resolution, e.g.,
    nanometer space).  The real world coordinates are specified in  "x_y_z" format, e.g., "20.3_11.8_109.4".
    The resolution is used to determine the # pixels in the returned image.

    Example: 

    GET <api URL>/node/3f8c/grayscale/arb/100.2_90_80.7/200.2_90_80.7/100.2_190.0_80.7/10.0/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    top left      Real world coordinate (in nanometers) of top left pixel in returned image.
    top right     Real world coordinate of top right pixel.
    bottom left   Real world coordinate of bottom left pixel.
    res           The resolution/pixel that is used to calculate the returned image size in pixels.
    format        "png", "jpg" (default: "png")  
                    jpg allows lossy quality setting, e.g., "jpg:80"

    Query-string Options:

    throttle      If "true", makes sure only N compute-intense operation 
                    (all API calls that can be throttled) are handled.  If the server can't initiate the API 
                    call right away, a 503 (Service Unavailable) status code is returned.

 GET <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
POST <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>

    Retrieves or puts "spanX" blocks of uncompressed voxel data along X starting from given block coordinate.

    Example: 

    GET <api URL>/node/3f8c/grayscale/blocks/10_20_30/8

    Returns blocks where first block has given block coordinate and number
    of blocks returned along x axis is "spanX".  The data is sent in the following format:

    <block 0 byte array>
    <block 1 byte array>
    ... 
    <block N byte array>

    Each byte array iterates in X, then Y, then Z for that block.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    block coord   The block coordinate of the first block in X_Y_Z format.  Block coordinates
                  can be derived from voxel coordinates by dividing voxel coordinates by
                  the block size for a data type.
`

var (
	// DefaultBlockSize specifies the default size for each block of this data type.
	DefaultBlockSize int32 = 32

	DefaultRes float32 = 8

	DefaultUnits = "nanometers"
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(binary.LittleEndian)
	gob.Register(binary.BigEndian)
}

// Type embeds the datastore's Type to create a unique type with voxel functions.
// Refinements of general voxel types can be implemented by embedding this type,
// choosing appropriate # of values and bytes/value, overriding functions as needed,
// and calling datastore.Register().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Type struct {
	datastore.Type

	// values describes the data type/label for each value within a voxel.
	values dvid.DataValues

	// can these values be interpolated?
	interpolable bool
}

// NewType returns a pointer to a new imageblk Type with default values set.
func NewType(values dvid.DataValues, interpolable bool) Type {
	dtype := Type{
		Type: datastore.Type{
			Requirements: &storage.Requirements{Batcher: true},
		},
		values:       values,
		interpolable: interpolable,
	}
	return dtype
}

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Type) NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	var p Properties
	p.setDefault(dtype.values, dtype.interpolable)
	if err := p.setByConfig(c); err != nil {
		return nil, err
	}
	data := &Data{
		Data:       basedata,
		Properties: p,
	}
	return data, nil
}

// --- TypeService interface ---

// NewDataService returns a pointer to a new Voxels with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return dtype.NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

type bulkLoadInfo struct {
	filenames     []string
	versionID     dvid.VersionID
	offset        dvid.Point
	extentChanged dvid.Bool
}

// Voxels represents subvolumes or slices and implements the ExtData interface.
type Voxels struct {
	dvid.Geometry

	values dvid.DataValues

	// The data itself
	data []byte

	// The stride for 2d iteration in bytes between vertically adjacent pixels.
	// For 3d subvolumes, we don't reuse standard Go images but maintain fully
	// packed data slices, so stride isn't necessary.
	stride int32
}

func NewVoxels(geom dvid.Geometry, values dvid.DataValues, data []byte, stride int32) *Voxels {
	return &Voxels{geom, values, data, stride}
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %s @ %s", v.DataShape(), size, v.StartPoint())
}

func (v *Voxels) Values() dvid.DataValues {
	return v.values
}

func (v *Voxels) Data() []byte {
	return v.data
}

func (v *Voxels) Stride() int32 {
	return v.stride
}

func (v *Voxels) BytesPerVoxel() int32 {
	return v.values.BytesPerElement()
}

func (v *Voxels) SetGeometry(geom dvid.Geometry) {
	v.Geometry = geom
}

func (v *Voxels) SetValues(values dvid.DataValues) {
	v.values = values
}

func (v *Voxels) SetStride(stride int32) {
	v.stride = stride
}

func (v *Voxels) SetData(data []byte) {
	v.data = data
}

// -------  ExtData interface implementation -------------

func (v *Voxels) NewChunkIndex() dvid.ChunkIndexer {
	return &dvid.IndexZYX{}
}

func (v *Voxels) Interpolable() bool {
	return true
}

// DownRes downsamples 2d Voxels data by averaging where the down-magnification are
// integers.  If the source image size in Voxels is not an integral multiple of the
// reduction factor, the edge voxels on the right and bottom side are truncated.
// This function modifies the Voxels data.  An error is returned if a non-2d Voxels
// receiver is used.
func (v *Voxels) DownRes(magnification dvid.Point) error {
	if v.DataShape().ShapeDimensions() != 2 {
		return fmt.Errorf("ImageDownres() only supports 2d images at this time.")
	}
	// Calculate new dimensions and allocate.
	srcW := v.Size().Value(0)
	srcH := v.Size().Value(1)
	reduceW, reduceH, err := v.DataShape().GetSize2D(magnification)
	if err != nil {
		return err
	}
	dstW := srcW / reduceW
	dstH := srcH / reduceH

	// Reduce the image.
	img, err := v.GetImage2d()
	if err != nil {
		return err
	}
	img, err = img.ScaleImage(int(dstW), int(dstH))
	if err != nil {
		return err
	}

	// Set data and dimensions to downres data.
	v.data = []byte(img.Data())
	geom, err := dvid.NewOrthogSlice(v.DataShape(), v.StartPoint(), dvid.Point2d{dstW, dstH})
	if err != nil {
		return err
	}
	v.Geometry = geom
	v.stride = dstW * v.values.BytesPerElement()
	return nil
}

// IndexIterator returns an iterator that can move across the voxel geometry,
// generating indices or index spans.
func (v *Voxels) IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error) {
	// Setup traversal
	begVoxel, ok := v.StartPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData StartPoint() cannot handle Chunkable points.")
	}
	endVoxel, ok := v.EndPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData EndPoint() cannot handle Chunkable points.")
	}
	begBlock := begVoxel.Chunk(chunkSize).(dvid.ChunkPoint3d)
	endBlock := endVoxel.Chunk(chunkSize).(dvid.ChunkPoint3d)

	return dvid.NewIndexZYXIterator(begBlock, endBlock), nil
}

// GetImage2d returns a 2d image suitable for use external to DVID.
// TODO -- Create more comprehensive handling of endianness and encoding of
// multibytes/voxel data into appropriate images.
func (v *Voxels) GetImage2d() (*dvid.Image, error) {
	// Make sure each value has same # of bytes or else we can't generate a go image.
	// If so, we need to make another ExtData that knows how to convert the varying
	// values into an appropriate go image.
	valuesPerVoxel := int32(len(v.values))
	if valuesPerVoxel < 1 || valuesPerVoxel > 4 {
		return nil, fmt.Errorf("Standard voxels type can't convert %d values/voxel into image.",
			valuesPerVoxel)
	}
	bytesPerValue := v.values.ValueBytes(0)
	for _, dataValue := range v.values {
		if dvid.DataTypeBytes(dataValue.T) != bytesPerValue {
			return nil, fmt.Errorf("Standard voxels type can't handle varying sized values per voxel.")
		}
	}

	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			valuesPerVoxel, bytesPerValue)
	}

	var img image.Image
	width := v.Size().Value(0)
	height := v.Size().Value(1)
	sliceBytes := width * height * valuesPerVoxel * bytesPerValue
	beg := int32(0)
	end := beg + sliceBytes
	data := v.Data()
	if int(end) > len(data) {
		return nil, fmt.Errorf("Voxels %s has insufficient amount of data to return an image.", v)
	}
	r := image.Rect(0, 0, int(width), int(height))
	switch valuesPerVoxel {
	case 1:
		switch bytesPerValue {
		case 1:
			img = &image.Gray{data[beg:end], 1 * r.Dx(), r}
		case 2:
			bigendian, err := v.littleToBigEndian(data[beg:end])
			if err != nil {
				return nil, err
			}
			img = &image.Gray16{bigendian, 2 * r.Dx(), r}
		case 4:
			img = &image.NRGBA{data[beg:end], 4 * r.Dx(), r}
		case 8:
			img = &image.NRGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			return nil, unsupported()
		}
	case 4:
		switch bytesPerValue {
		case 1:
			img = &image.NRGBA{data[beg:end], 4 * r.Dx(), r}
		case 2:
			img = &image.NRGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			return nil, unsupported()
		}
	default:
		return nil, unsupported()
	}

	return dvid.ImageFromGoImage(img, v.Values(), v.Interpolable())
}

// Properties are additional properties for image block data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Values describes the data type/label for each value within a voxel.
	Values dvid.DataValues

	// Interpolable is true if voxels can be interpolated when resizing.
	Interpolable bool

	// Block size for this repo
	BlockSize dvid.Point

	dvid.Resolution

	dvid.Extents

	// Background value for data
	Background uint8
}

// setDefault sets Voxels properties to default values.
func (p *Properties) setDefault(values dvid.DataValues, interpolable bool) error {
	p.Values = make([]dvid.DataValue, len(values))
	copy(p.Values, values)
	p.Interpolable = interpolable

	dimensions := 3
	size := make([]int32, dimensions)
	for d := 0; d < dimensions; d++ {
		size[d] = DefaultBlockSize
	}
	var err error
	p.BlockSize, err = dvid.NewPoint(size)
	if err != nil {
		return err
	}
	p.Resolution.VoxelSize = make(dvid.NdFloat32, dimensions)
	for d := 0; d < dimensions; d++ {
		p.Resolution.VoxelSize[d] = DefaultRes
	}
	p.Resolution.VoxelUnits = make(dvid.NdString, dimensions)
	for d := 0; d < dimensions; d++ {
		p.Resolution.VoxelUnits[d] = DefaultUnits
	}
	return nil
}

// setByConfig sets Voxels properties based on type-specific keywords in the configuration.
// Any property not described in the config is left as is.  See the Voxels help for listing
// of configurations.
func (p *Properties) setByConfig(config dvid.Config) error {
	s, found, err := config.GetString("BlockSize")
	if err != nil {
		return err
	}
	if found {
		p.BlockSize, err = dvid.StringToPoint(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelSize")
	if err != nil {
		return err
	}
	if found {
		dvid.Infof("Changing resolution of voxels to %s\n", s)
		p.Resolution.VoxelSize, err = dvid.StringToNdFloat32(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelUnits")
	if err != nil {
		return err
	}
	if found {
		p.Resolution.VoxelUnits, err = dvid.StringToNdString(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("Background")
	if err != nil {
		return err
	}
	if found {
		background, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return err
		}
		p.Background = uint8(background)
	}
	return nil
}

type metadataT struct {
	Axes       []axisT
	Properties Properties
}

type axisT struct {
	Label      string
	Resolution float32
	Units      string
	Size       int32
	Offset     int32
}

// NdDataSchema returns the metadata in JSON for this Data
func (p *Properties) NdDataMetadata() (string, error) {
	var err error
	var size, offset dvid.Point

	dims := int(p.BlockSize.NumDims())
	if p.MinPoint == nil || p.MaxPoint == nil {
		zeroPt := make([]int32, dims)
		size, err = dvid.NewPoint(zeroPt)
		if err != nil {
			return "", err
		}
		offset = size
	} else {
		size = p.MaxPoint.Sub(p.MinPoint).AddScalar(1)
		offset = p.MinPoint
	}

	var axesName = []string{"X", "Y", "Z", "t", "c"}
	var metadata metadataT
	metadata.Axes = []axisT{}
	for dim := 0; dim < dims; dim++ {
		metadata.Axes = append(metadata.Axes, axisT{
			Label:      axesName[dim],
			Resolution: p.Resolution.VoxelSize[dim],
			Units:      p.Resolution.VoxelUnits[dim],
			Size:       size.Value(uint8(dim)),
			Offset:     offset.Value(uint8(dim)),
		})
	}
	metadata.Properties = *p

	m, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data
	Properties
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) ||
		!reflect.DeepEqual(d.Properties, d2.Properties) {
		return false
	}
	return true
}

// BlankImage initializes a blank image of appropriate size and depth for the
// current data values.  Returns an error if the geometry is not 2d.
func (d *Data) BlankImage(dstW, dstH int32) (*dvid.Image, error) {
	// Make sure values for this data can be converted into an image.
	valuesPerVoxel := int32(len(d.Properties.Values))
	if valuesPerVoxel < 1 || valuesPerVoxel > 4 {
		return nil, fmt.Errorf("Standard voxels type can't convert %d values/voxel into image.",
			valuesPerVoxel)
	}
	bytesPerValue, err := d.Properties.Values.BytesPerValue()
	if err != nil {
		return nil, err
	}

	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			valuesPerVoxel, bytesPerValue)
	}

	var img image.Image
	stride := int(dstW * valuesPerVoxel * bytesPerValue)
	r := image.Rect(0, 0, int(dstW), int(dstH))
	imageBytes := int(dstH) * stride
	data := make([]uint8, imageBytes, imageBytes)
	switch valuesPerVoxel {
	case 1:
		switch bytesPerValue {
		case 1:
			img = &image.Gray{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 2:
			img = &image.Gray16{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 4:
			img = &image.NRGBA{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 8:
			img = &image.NRGBA64{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		default:
			return nil, unsupported()
		}
	case 4:
		switch bytesPerValue {
		case 1:
			img = &image.NRGBA{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 2:
			img = &image.NRGBA64{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		default:
			return nil, unsupported()
		}
	default:
		return nil, unsupported()
	}

	return dvid.ImageFromGoImage(img, d.Properties.Values, d.Properties.Interpolable)
}

// PutLocal adds image data to a version node, altering underlying blocks if the image
// intersects the block.
//
// The image filename glob MUST BE absolute file paths that are visible to the server.
// This function is meant for mass ingestion of large data files, and it is inappropriate
// to read gigabytes of data just to send it over the network to a local DVID.
func (d *Data) PutLocal(request datastore.Request, reply *datastore.Response) error {
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceStr, planeStr, offsetStr string
	filenames := request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceStr,
		&planeStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
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

	// Get plane
	plane, err := dvid.DataShapeString(planeStr).DataShape()
	if err != nil {
		return err
	}

	// Get Repo and IDs
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Load and PUT each image.
	numSuccessful := 0
	for _, filename := range filenames {
		sliceLog := dvid.NewTimeLog()
		img, _, err := dvid.GoImageFromFile(filename)
		if err != nil {
			return fmt.Errorf("Error after %d images successfully added: %v", numSuccessful, err)
		}
		slice, err := dvid.NewOrthogSlice(plane, offset, dvid.RectSize(img.Bounds()))
		if err != nil {
			return fmt.Errorf("Unable to determine slice: %v", err)
		}

		vox, err := d.NewVoxels(slice, img)
		if err != nil {
			return err
		}
		storage.FileBytesRead <- len(vox.Data())
		if err = d.IngestVoxels(versionID, vox, ""); err != nil {
			return err
		}
		sliceLog.Debugf("%s put local %s", d.DataName(), slice)
		numSuccessful++
		offset = offset.Add(dvid.Point3d{0, 0, 1})
	}

	if err := datastore.AddToNodeLog(uuid, []string{request.Command.String()}); err != nil {
		return err
	}
	timedLog.Infof("RPC put local (%s) completed", addedFiles)
	return nil
}

// NewVoxels returns Voxels with given geometry and optional image data.
// If img is passed in, the function will initialize the Voxels with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
func (d *Data) NewVoxels(geom dvid.Geometry, img interface{}) (*Voxels, error) {
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	stride := geom.Size().Value(0) * bytesPerVoxel

	voxels := &Voxels{
		Geometry: geom,
		values:   d.Properties.Values,
		stride:   stride,
	}

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
		voxels.data = make([]uint8, requestSize)
	} else {
		switch t := img.(type) {
		case image.Image:
			var actualStride int32
			var err error
			voxels.data, _, actualStride, err = dvid.ImageData(t)
			if err != nil {
				return nil, err
			}
			if actualStride < stride {
				return nil, fmt.Errorf("Too little data in input image (expected stride %d)", stride)
			}
			voxels.stride = actualStride
		case []byte:
			voxels.data = t
			actualLen := int64(len(voxels.data))
			expectedLen := int64(bytesPerVoxel) * geom.NumVoxels()
			if actualLen != expectedLen {
				return nil, fmt.Errorf("voxels data was %d bytes, expected %d bytes for %s",
					actualLen, expectedLen, geom)
			}
		default:
			return nil, fmt.Errorf("Unexpected image type given to NewVoxels(): %T", t)
		}
	}
	return voxels, nil
}

func (d *Data) BlockSize() dvid.Point {
	return d.Properties.BlockSize
}

func (d *Data) Extents() *dvid.Extents {
	return &(d.Properties.Extents)
}

func (d *Data) Resolution() dvid.Resolution {
	return d.Properties.Resolution
}

func (d *Data) String() string {
	return string(d.DataName())
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

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

func (d *Data) ModifyConfig(config dvid.Config) error {
	p := &(d.Properties)
	if err := p.setByConfig(config); err != nil {
		return err
	}
	return nil
}

type SendOp struct {
	socket message.Socket
}

// Stub for versioned sync to remote DVID.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	return fmt.Errorf("Versioned inter-DVID syncs not supported yet")
}

// ForegroundROI creates a new ROI by determining all non-background blocks.
func (d *Data) ForegroundROI(req datastore.Request, reply *datastore.Response) error {
	if d.Values.BytesPerElement() != 1 {
		return fmt.Errorf("Foreground ROI command only implemented for 1 byte/voxel data!")
	}

	// Parse the request
	var uuidStr, dataName, cmdStr, destName, backgroundStr string
	req.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &destName, &backgroundStr)

	// Get the version and repo
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	if err = datastore.AddToNodeLog(uuid, []string{req.Command.String()}); err != nil {
		return err
	}

	// Use existing destination data or a new ROI data.
	var dest *roi.Data
	dest, err = roi.GetByUUID(uuid, dvid.InstanceName(destName))
	if err != nil {
		config := dvid.NewConfig()
		typeservice, err := datastore.TypeServiceByName("roi")
		if err != nil {
			return err
		}
		dataservice, err := datastore.NewData(uuid, typeservice, dvid.InstanceName(destName), config)
		if err != nil {
			return err
		}
		var ok bool
		dest, ok = dataservice.(*roi.Data)
		if !ok {
			return fmt.Errorf("Could not create ROI data instance")
		}
	}

	// Asynchronously process the voxels.
	background, err := dvid.StringToPointNd(backgroundStr, ",")
	if err != nil {
		return err
	}
	go d.foregroundROI(versionID, dest, background)

	return nil
}

func (d *Data) foregroundROI(v dvid.VersionID, dest *roi.Data, background dvid.PointNd) {
	dest.Ready = false

	store, err := storage.MutableStore()
	if err != nil {
		dvid.Criticalf("Data type imageblk had error initializing store: %v\n", err)
		return
	}

	timedLog := dvid.NewTimeLog()
	timedLog.Infof("Starting foreground ROI %q for %s", dest.DataName(), d.DataName())

	// Iterate through all voxel blocks, loading and then checking blocks
	// for any foreground voxels.
	ctx := datastore.NewVersionedCtx(d, v)

	backgroundBytes := make([]byte, len(background))
	for i, b := range background {
		backgroundBytes[i] = byte(b)
	}

	const BATCH_SIZE = 1000
	var numBatches int
	var span *dvid.Span
	spans := []dvid.Span{}

	var f storage.ChunkFunc = func(chunk *storage.Chunk) error {
		if chunk == nil || chunk.V == nil {
			return nil
		}
		data, _, err := dvid.DeserializeData(chunk.V, true)
		if err != nil {
			return fmt.Errorf("Error decoding block: %v\n", err)
		}
		numVoxels := d.BlockSize().Prod()
		var foreground bool
		for i := int64(0); i < numVoxels; i++ {
			isBackground := false
			for _, b := range backgroundBytes {
				if data[i] == b {
					isBackground = true
					break
				}
			}
			if !isBackground {
				foreground = true
				break
			}
		}
		if foreground {
			indexZYX, err := DecodeTKey(chunk.K)
			if err != nil {
				return fmt.Errorf("Error decoding voxel block key: %v\n", err)
			}
			x, y, z := indexZYX.Unpack()
			if span == nil {
				span = &dvid.Span{z, y, x, x}
			} else if !span.Extends(x, y, z) {
				spans = append(spans, *span)
				if len(spans) >= BATCH_SIZE {
					init := (numBatches == 0)
					numBatches++
					go func(spans []dvid.Span) {
						if err := dest.PutSpans(v, spans, init); err != nil {
							dvid.Errorf("Error in storing ROI: %v\n", err)
						} else {
							timedLog.Debugf("-- Wrote batch %d of spans for foreground ROI %q", numBatches, dest.DataName())
						}
					}(spans)
					spans = []dvid.Span{}
				}
				span = &dvid.Span{z, y, x, x}
			}
		}
		server.BlockOnInteractiveRequests("voxels [compute foreground ROI]")
		return nil
	}

	minTKey := storage.MinTKey(keyImageBlock)
	maxTKey := storage.MaxTKey(keyImageBlock)

	err = store.ProcessRange(ctx, minTKey, maxTKey, &storage.ChunkOp{}, f)
	if err != nil {
		dvid.Errorf("Error in processing chunks in ROI: %v\n", err)
		return
	}
	if span != nil {
		spans = append(spans, *span)
	}

	// Save new ROI
	if len(spans) > 0 {
		if err := dest.PutSpans(v, spans, numBatches == 0); err != nil {
			dvid.Errorf("Error in storing ROI: %v\n", err)
			return
		}
	}
	timedLog.Infof("Created foreground ROI %q for %s", dest.DataName(), d.DataName())
	dest.Ready = true
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
			hostname, _ := os.Hostname()
			return fmt.Errorf("Couldn't find any files to add.  Are they visible to DVID server on %s?",
				hostname)
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
		return d.LoadImages(versionID, offset, filenames)

	case "put":
		if len(req.Command) < 7 {
			return fmt.Errorf("Poorly formatted put command.  See command-line help.")
		}
		source := req.Command[4]
		switch source {
		case "local":
			return d.PutLocal(req, reply)
		case "remote":
			return fmt.Errorf("put remote not yet implemented")
		default:
			return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
				d.DataName(), d.TypeName(), req.TypeCommand())
		}

	case "roi":
		if len(req.Command) < 6 {
			return fmt.Errorf("Poorly formatted roi command. See command-line help.")
		}
		return d.ForegroundROI(req, reply)

	default:
		return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), req.TypeCommand())
	}
	return nil
}

// Prints RGBA of first n x n pixels of image with header string.
func debugData(img image.Image, message string) {
	data, _, stride, _ := dvid.ImageData(img)
	var n = 3 // neighborhood to write
	fmt.Printf("%s>\n", message)
	for y := 0; y < n; y++ {
		for x := 0; x < n; x++ {
			i := y*int(stride) + x*4
			fmt.Printf("[%3d %3d %3d %3d]  ", data[i], data[i+1], data[i+2], data[i+3])
		}
		fmt.Printf("\n")
	}
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	switch action {
	case "get":
	case "post":
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
	var roiptr *ROI
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))
	if len(roiname) != 0 {
		roiptr = new(ROI)
		attenuationStr := queryStrings.Get("attenuation")
		if len(attenuationStr) != 0 {
			attenuation, err := strconv.Atoi(attenuationStr)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if attenuation < 1 || attenuation > 7 {
				server.BadRequest(w, r, "Attenuation should be from 1 to 7 (divides by 2^n)")
				return
			}
			roiptr.attenuation = uint8(attenuation)
		}
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && action == "put" {
		fmt.Printf("Setting configuration of data '%s'\n", d.DataName())
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
		fmt.Fprintln(w, d.Help())
		return

	case "metadata":
		jsonStr, err := d.NdDataMetadata()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)
		return

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return

	case "rawkey":
		// GET <api URL>/node/<UUID>/<data name>/rawkey?x=<block x>&y=<block y>&z=<block z>
		if len(parts) != 4 {
			server.BadRequest(w, r, "rawkey endpoint should be followed by query strings (x, y, and z) giving block coord")
			return
		}

	case "blocks":
		// GET  <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		// POST <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		if len(parts) < 6 {
			server.BadRequest(w, r, "%q must be followed by block-coord/span-x", parts[3])
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
		if action == "get" {
			data, err := d.GetBlocks(ctx.VersionID(), blockCoord, int32(span))
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/octet-stream")
			_, err = w.Write(data)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
		} else {
			mutate := (queryStrings.Get("mutate") == "true")
			if err := d.PutBlocks(ctx.VersionID(), blockCoord, span, r.Body, mutate); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
		timedLog.Infof("HTTP %s: Blocks (%s)", r.Method, r.URL)

	case "arb":
		// GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>]
		if len(parts) < 8 {
			server.BadRequest(w, r, "%q must be followed by top-left/top-right/bottom-left/res", parts[3])
			return
		}
		if queryStrings.Get("throttle") == "on" {
			if server.ThrottledHTTP(w) {
				return
			}
			defer server.ThrottledOpDone()
		}
		img, err := d.GetArbitraryImage(ctx, parts[4], parts[5], parts[6], parts[7])
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		var formatStr string
		if len(parts) >= 9 {
			formatStr = parts[8]
		}
		err = dvid.WriteImageHttp(w, img.Get(), formatStr)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: Arbitrary image (%s)", r.Method, r.URL)

	case "raw", "isotropic":
		// GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>]
		if len(parts) < 7 {
			server.BadRequest(w, r, "%q must be followed by shape/size/offset", parts[3])
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
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			vox, err := d.NewVoxels(rawSlice, nil)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			img, err := d.GetImage(ctx.VersionID(), vox, roiname)
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
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if action == "get" {
				vox, err := d.NewVoxels(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				data, err := d.GetVolume(ctx.VersionID(), vox, roiname)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				w.Header().Set("Content-type", "application/octet-stream")
				_, err = w.Write(data)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
			} else {
				if isotropic {
					err := fmt.Errorf("can only PUT 'raw' not 'isotropic' images")
					server.BadRequest(w, r, err)
					return
				}
				// Make sure vox is block-aligned
				if !dvid.BlockAligned(subvol, d.BlockSize()) {
					server.BadRequest(w, r, "cannot store voxels in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
					return
				}

				data, err := ioutil.ReadAll(r.Body)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				vox, err := d.NewVoxels(subvol, data)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				mutate := (queryStrings.Get("mutate") == "true")
				if err = d.PutVoxels(ctx.VersionID(), vox, roiname, mutate); err != nil {
					server.BadRequest(w, r, err)
					return
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
		}
	default:
		server.BadRequest(w, r, "Unrecognized API call for voxels %q.  See API help.", d.DataName())
	}
}
