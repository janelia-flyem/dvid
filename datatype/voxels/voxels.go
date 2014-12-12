/*
	Package voxels implements DVID support for data using voxels as elements.
*/
package voxels

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
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.8"
	RepoURL = "github.com/janelia-flyem/dvid/datatype/voxels"

	// Don't allow requests that will return more than this amount of data.
	MaxDataRequest = 3 * dvid.Giga
)

const HelpMessage = `
API for 'voxels' datatype (github.com/janelia-flyem/dvid/datatype/voxels)
=========================================================================

Command-line:

$ dvid repo <UUID> new <type name> <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new grayscale8 mygrayscale BlockSize=32 Res=3.2,3.2,40.0

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    type name      Data type name, e.g., "grayscale8"
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
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
	

$ dvid node <UUID> <data name> roi <new roi data name> <background value> 

    Creates a ROI consisting of all voxel blocks that are non-background.

    Example:

    $ dvid node 3f8c mygrayscale roi grayscale_roi 0

    
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

	Retrieves metadata in JSON format (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=on][?queryopts]
POST <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts voxel data.

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
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    roi       	  Name of roi data instance used to mask the requested data.
    attenuation   (TODO) For attenuation n, this reduces the intensity of voxels outside ROI by 2^n.
    			  Valid range is n = 1 to n = 7.  Currently only implemented for 8-bit voxels.
    			  Default is to zero out voxels outside ROI.

GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?throttle=on]

    Retrieves or puts voxel data.

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
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>]

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

GET <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>

    Retrieves blocks of voxel data.

    Example: 

    GET <api URL>/node/3f8c/grayscale/blocks/10_20_30/8

    Returns blocks where first block has given block coordinate and number
    of blocks returned along x axis is "spanX".  If no "uncompressed=true" query
    string is used, the block data is returned in default compression format, usually
    LZ4.  The data is sent in the following format with all integers in little-endian format:

    <int32: number of blocks, N>
    <int32: block x coord for block 0>
    <int32: block y coord for block 0>
    <int32: block z coord for block 0>
       if compressed:  <int32: length in bytes of block 0>
    <int32: block x coord for block 1>
    ... repeat for all blocks
    <block 0 byte array>
    <block 1 byte array>
    ... repeat for all blocks
    <block N byte array>

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    block coord   Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    uncompressed  If true, causes block data to be uncompressed before sending to client.  This
    			  will lead to all block being the same size.
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

// NewType returns a pointer to a new voxels Type with default values set.
func NewType(values dvid.DataValues, interpolable bool) *Type {
	dtype := &Type{
		values:       values,
		interpolable: interpolable,
	}
	dtype.Type = datastore.Type{
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}
	return dtype
}

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Type) NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (*Data, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	props := new(Properties)
	props.SetDefault(dtype.values, dtype.interpolable)
	if err := props.SetByConfig(c); err != nil {
		return nil, err
	}
	data := &Data{
		Data:       *basedata,
		Properties: *props,
	}
	return data, nil
}

// --- TypeService interface ---

// NewDataService returns a pointer to a new Voxels with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
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

	byteOrder binary.ByteOrder
}

func NewVoxels(geom dvid.Geometry, values dvid.DataValues, data []byte, stride int32,
	byteOrder binary.ByteOrder) *Voxels {

	return &Voxels{geom, values, data, stride, byteOrder}
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %s @ %s", v.DataShape(), size, v.StartPoint())
}

// -------  VoxelGetter interface implementation -------------

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

func (v *Voxels) ByteOrder() binary.ByteOrder {
	return v.byteOrder
}

// -------  VoxelSetter interface implementation -------------

func (v *Voxels) SetGeometry(geom dvid.Geometry) {
	v.Geometry = geom
}

func (v *Voxels) SetValues(values dvid.DataValues) {
	v.values = values
}

func (v *Voxels) SetStride(stride int32) {
	v.stride = stride
}

func (v *Voxels) SetByteOrder(order binary.ByteOrder) {
	v.byteOrder = order
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
			bigendian, err := littleToBigEndian(v, data[beg:end])
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

// Extents holds the extents of a volume in both absolute voxel coordinates
// and lexicographically sorted chunk indices.
type Extents struct {
	MinPoint dvid.Point
	MaxPoint dvid.Point

	MinIndex dvid.ChunkIndexer
	MaxIndex dvid.ChunkIndexer

	pointMu sync.Mutex
	indexMu sync.Mutex
}

// --- dvid.Bounder interface ----

// StartPoint returns the offset to first point of data.
func (ext *Extents) StartPoint() dvid.Point {
	return ext.MinPoint
}

// EndPoint returns the last point.
func (ext *Extents) EndPoint() dvid.Point {
	return ext.MaxPoint
}

// --------

// AdjustPoints modifies extents based on new voxel coordinates in concurrency-safe manner.
func (ext *Extents) AdjustPoints(pointBeg, pointEnd dvid.Point) bool {
	ext.pointMu.Lock()
	defer ext.pointMu.Unlock()

	var changed bool
	if ext.MinPoint == nil {
		ext.MinPoint = pointBeg
		changed = true
	} else {
		ext.MinPoint, changed = ext.MinPoint.Min(pointBeg)
	}
	if ext.MaxPoint == nil {
		ext.MaxPoint = pointEnd
		changed = true
	} else {
		ext.MaxPoint, changed = ext.MaxPoint.Max(pointEnd)
	}
	return changed
}

// AdjustIndices modifies extents based on new block indices in concurrency-safe manner.
func (ext *Extents) AdjustIndices(indexBeg, indexEnd dvid.ChunkIndexer) bool {
	ext.indexMu.Lock()
	defer ext.indexMu.Unlock()

	var changed bool
	if ext.MinIndex == nil {
		ext.MinIndex = indexBeg
		changed = true
	} else {
		ext.MinIndex, changed = ext.MinIndex.Min(indexBeg)
	}
	if ext.MaxIndex == nil {
		ext.MaxIndex = indexEnd
		changed = true
	} else {
		ext.MaxIndex, changed = ext.MaxIndex.Max(indexEnd)
	}
	return changed
}

type Resolution struct {
	// Resolution of voxels in volume
	VoxelSize dvid.NdFloat32

	// Units of resolution, e.g., "nanometers"
	VoxelUnits dvid.NdString
}

func (r Resolution) IsIsotropic() bool {
	if len(r.VoxelSize) <= 1 {
		return true
	}
	curRes := r.VoxelSize[0]
	for _, res := range r.VoxelSize[1:] {
		if res != curRes {
			return false
		}
	}
	return true
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Values describes the data type/label for each value within a voxel.
	Values dvid.DataValues

	// Interpolable is true if voxels can be interpolated when resizing.
	Interpolable bool

	// Block size for this repo
	BlockSize dvid.Point

	// The endianness of this loaded data.
	ByteOrder binary.ByteOrder

	Resolution
	Extents

	// Background value for data
	Background uint8
}

// SetDefault sets Voxels properties to default values.
func (props *Properties) SetDefault(values dvid.DataValues, interpolable bool) error {
	props.Values = make([]dvid.DataValue, len(values))
	copy(props.Values, values)
	props.Interpolable = interpolable

	props.ByteOrder = binary.LittleEndian

	dimensions := 3
	size := make([]int32, dimensions)
	for d := 0; d < dimensions; d++ {
		size[d] = DefaultBlockSize
	}
	var err error
	props.BlockSize, err = dvid.NewPoint(size)
	if err != nil {
		return err
	}
	props.Resolution.VoxelSize = make(dvid.NdFloat32, dimensions)
	for d := 0; d < dimensions; d++ {
		props.Resolution.VoxelSize[d] = DefaultRes
	}
	props.Resolution.VoxelUnits = make(dvid.NdString, dimensions)
	for d := 0; d < dimensions; d++ {
		props.Resolution.VoxelUnits[d] = DefaultUnits
	}
	return nil
}

// SetByConfig sets Voxels properties based on type-specific keywords in the configuration.
// Any property not described in the config is left as is.  See the Voxels help for listing
// of configurations.
func (props *Properties) SetByConfig(config dvid.Config) error {
	s, found, err := config.GetString("BlockSize")
	if err != nil {
		return err
	}
	if found {
		props.BlockSize, err = dvid.StringToPoint(s, ",")
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
		props.Resolution.VoxelSize, err = dvid.StringToNdFloat32(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelUnits")
	if err != nil {
		return err
	}
	if found {
		props.Resolution.VoxelUnits, err = dvid.StringToNdString(s, ",")
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
		props.Background = uint8(background)
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
func (props *Properties) NdDataMetadata() (string, error) {
	var err error
	var size, offset dvid.Point

	dims := int(props.BlockSize.NumDims())
	if props.MinPoint == nil || props.MaxPoint == nil {
		zeroPt := make([]int32, dims)
		size, err = dvid.NewPoint(zeroPt)
		if err != nil {
			return "", err
		}
		offset = size
	} else {
		size = props.MaxPoint.Sub(props.MinPoint).AddScalar(1)
		offset = props.MinPoint
	}

	var axesName = []string{"X", "Y", "Z", "t", "c"}
	var metadata metadataT
	metadata.Axes = []axisT{}
	for dim := 0; dim < dims; dim++ {
		metadata.Axes = append(metadata.Axes, axisT{
			Label:      axesName[dim],
			Resolution: props.Resolution.VoxelSize[dim],
			Units:      props.Resolution.VoxelUnits[dim],
			Size:       size.Value(uint8(dim)),
			Offset:     offset.Value(uint8(dim)),
		})
	}
	metadata.Properties = *props

	m, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	datastore.Data
	Properties
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

// Returns the image size necessary to compute an isotropic slice of the given dimensions.
// If isotropic is false, simply returns the original slice geometry.  If isotropic is true,
// uses the higher resolution dimension.
func (d *Data) HandleIsotropy2D(geom dvid.Geometry, isotropic bool) (dvid.Geometry, error) {
	if !isotropic {
		return geom, nil
	}
	// Get the voxel resolutions for this particular slice orientation
	resX, resY, err := geom.DataShape().GetFloat2D(d.Properties.VoxelSize)
	if err != nil {
		return nil, err
	}
	if resX == resY {
		return geom, nil
	}
	srcW := geom.Size().Value(0)
	srcH := geom.Size().Value(1)
	var dstW, dstH int32
	if resX < resY {
		// Use x resolution for all pixels.
		dstW = srcW
		dstH = int32(float32(srcH)*resX/resY + 0.5)
	} else {
		dstH = srcH
		dstW = int32(float32(srcW)*resY/resX + 0.5)
	}

	// Make altered geometry
	slice, ok := geom.(*dvid.OrthogSlice)
	if !ok {
		return nil, fmt.Errorf("can only handle isotropy for orthogonal 2d slices")
	}
	dstSlice := slice.Duplicate()
	dstSlice.SetSize(dvid.Point2d{dstW, dstH})
	return dstSlice, nil
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

	ctx := datastore.NewVersionedContext(d, versionID)

	// Load and PUT each image.
	numSuccessful := 0
	for _, filename := range filenames {
		sliceLog := dvid.NewTimeLog()
		img, _, err := dvid.GoImageFromFile(filename)
		if err != nil {
			return fmt.Errorf("Error after %d images successfully added: %s",
				numSuccessful, err.Error())
		}
		slice, err := dvid.NewOrthogSlice(plane, offset, dvid.RectSize(img.Bounds()))
		if err != nil {
			return fmt.Errorf("Unable to determine slice: %s", err.Error())
		}

		e, err := d.NewExtHandler(slice, img)
		if err != nil {
			return err
		}
		storage.FileBytesRead <- len(e.Data())
		err = PutVoxels(ctx, d, e, OpOptions{})
		if err != nil {
			return err
		}
		sliceLog.Debugf("%s put local %s", d.DataName(), slice)
		numSuccessful++
		offset = offset.Add(dvid.Point3d{0, 0, 1})
	}

	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	repo.AddToLog(request.Command.String())
	timedLog.Infof("RPC put local (%s) completed", addedFiles)
	return nil
}

// ----- IntData interface implementation ----------

// NewExtHandler returns an ExtData given some geometry and optional image data.
// If img is passed in, the function will initialize the ExtData with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
func (d *Data) NewExtHandler(geom dvid.Geometry, img interface{}) (ExtData, error) {
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	stride := geom.Size().Value(0) * bytesPerVoxel

	voxels := &Voxels{
		Geometry:  geom,
		values:    d.Properties.Values,
		stride:    stride,
		byteOrder: d.ByteOrder,
	}

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
				return nil, fmt.Errorf("PUT data was %d bytes, expected %d bytes for %s",
					actualLen, expectedLen, geom)
			}
		default:
			return nil, fmt.Errorf("Unexpected image type given to NewExtHandler(): %T", t)
		}
	}
	return voxels, nil
}

func (d *Data) BaseData() dvid.Data {
	return d
}

func (d *Data) Values() dvid.DataValues {
	return d.Properties.Values
}

func (d *Data) BlockSize() dvid.Point {
	return d.Properties.BlockSize
}

func (d *Data) Extents() *Extents {
	return &(d.Properties.Extents)
}

func (d *Data) String() string {
	return string(d.DataName())
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
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}
func (d *Data) ModifyConfig(config dvid.Config) error {
	props := &(d.Properties)
	if err := props.SetByConfig(config); err != nil {
		return err
	}
	return nil
}

type SendOp struct {
	socket message.Socket
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
// TODO -- handle versioning of the ROI coming.  For not, only allow root version of ROI.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	//wg := new(sync.WaitGroup)
	server.SpawnGoroutineMutex.Lock()

	// Get the ROI
	var roiIterator *roi.Iterator
	if len(roiname) != 0 {
		versionID, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			return err
		}
		roiIterator, err = roi.NewIterator(dvid.DataString(roiname), versionID, d)
		if err != nil {
			return err
		}
	}

	// Get the entire range of keys for this instance's voxel blocks
	begIndex := NewVoxelBlockIndex(d.Properties.Extents.MinIndex)
	ctx := storage.NewDataContext(d, 0)
	begKey := ctx.ConstructKey(begIndex)

	endIndex := NewVoxelBlockIndex(d.Properties.Extents.MaxIndex)
	ctx = storage.NewDataContext(d, dvid.MaxVersionID)
	endKey := ctx.ConstructKey(endIndex)

	// Send this instance's voxel blocks down the socket
	var blocksTotal, blocksSent int
	chunkOp := &storage.ChunkOp{&SendOp{s}, nil}
	err = db.ProcessRange(nil, begKey, endKey, chunkOp, func(chunk *storage.Chunk) {
		if chunk.KeyValue == nil {
			dvid.Errorf("Received nil keyvalue sending voxel chunks\n")
		}
		blocksTotal++
		indexZYX, err := DecodeVoxelBlockKey(chunk.K)
		if err != nil {
			dvid.Errorf("Error in sending voxel block: %s\n", err.Error())
			return
		}
		if roiIterator != nil && !roiIterator.InsideFast(*indexZYX) {
			return // don't send if this chunk is outside ROI
		}
		blocksSent++
		if err := s.SendKeyValue("voxels", storage.BigData, chunk.KeyValue); err != nil {
			dvid.Errorf("Error sending voxel chunks through nanomsg socket: %s\n", err.Error())
		}
	})
	if err != nil {
		server.SpawnGoroutineMutex.Unlock()
		return fmt.Errorf("Error in voxels %q range query: %s", d.DataName(), err.Error())
	}

	server.SpawnGoroutineMutex.Unlock()
	if err != nil {
		return err
	}
	//wg.Wait()
	if roiIterator == nil {
		dvid.Infof("Sent %d %s voxel blocks\n", blocksTotal, d.DataName())
	} else {
		dvid.Infof("Sent %d %s voxel blocks (out of %d total) within ROI %q\n",
			blocksSent, d.DataName(), blocksTotal, roiname)
	}
	return nil
}

// ForegroundROI creates a new ROI by determining all non-background blocks.
func (d *Data) ForegroundROI(request datastore.Request, reply *datastore.Response) error {
	if d.Values().BytesPerElement() != 1 {
		return fmt.Errorf("Foreground ROI command only implemented for 1 byte/voxel data!")
	}

	// Parse the request
	var uuidStr, dataName, cmdStr, destName, backgroundStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &destName, &backgroundStr)

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

	// Use existing destination data or a new ROI data.
	var dest *roi.Data
	dest, err = roi.GetByUUID(uuid, dvid.DataString(destName))
	if err != nil {
		config := dvid.NewConfig()
		typeservice, err := datastore.TypeServiceByName("roi")
		if err != nil {
			return err
		}
		dataservice, err := repo.NewData(typeservice, dvid.DataString(destName), config)
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
	go d.foregroundROI(uuid, versionID, dest)

	return nil
}

func (d *Data) foregroundROI(uuid dvid.UUID, versionID dvid.VersionID, dest *roi.Data) {
	timedLog := dvid.NewTimeLog()

	// Iterate through all voxel blocks, loading and then checking blocks
	// for any foreground voxels.
	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
		return
	}
	ctx := datastore.NewVersionedContext(d, versionID)

	spans := []roi.Span{}
	minIndex := NewVoxelBlockIndex(&dvid.MinIndexZYX)
	maxIndex := NewVoxelBlockIndex(&dvid.MaxIndexZYX)
	err = bigdata.ProcessRange(ctx, minIndex, maxIndex, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		if chunk == nil || chunk.V == nil {
			return
		}
		data, _, err := dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Error decoding block: %s\n", err.Error())
			return
		}
		numVoxels := d.BlockSize().Prod()
		var foreground bool
		background := byte(d.Background)
		for i := int64(0); i < numVoxels; i++ {
			if data[i] != background {
				foreground = true
				break
			}
		}
		if foreground {

		}
	})

	// Save new ROI
	if err := datastore.SaveRepo(uuid); err != nil {
		dvid.Infof("Could not save new ROI %q, uuid %s: %s", dest.DataName(), uuid, err.Error())
	}
	timedLog.Infof("Created foreground ROI %q for %s\n", dest.DataName(), d.DataName())
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
			hostname, _ := os.Hostname()
			return fmt.Errorf("Couldn't find any files to add.  Are they visible to DVID server on %s?",
				hostname)
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
		return LoadImages(versionID, d, offset, filenames)

	case "put":
		if len(request.Command) < 7 {
			return fmt.Errorf("Poorly formatted put command.  See command-line help.")
		}
		source := request.Command[4]
		switch source {
		case "local":
			return d.PutLocal(request, reply)
		case "remote":
			return fmt.Errorf("put remote not yet implemented")
		default:
			return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
				d.DataName(), d.TypeName(), request.TypeCommand())
		}

	case "roi":
		if len(request.Command) < 6 {
			return fmt.Errorf("Poorly formatted roi command. See command-line help.")
		}
		return d.ForegroundROI(request, reply)

	default:
		return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
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
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	repo, versions, err := datastore.FromContext(requestCtx)
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
	var op OpType
	switch action {
	case "get":
		op = GetOp
	case "post":
		op = PutOp
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
	queryValues := r.URL.Query()
	roiname := dvid.DataString(queryValues.Get("roi"))
	if len(roiname) != 0 {
		roiptr = new(ROI)
		attenuationStr := queryValues.Get("attenuation")
		if len(attenuationStr) != 0 {
			attenuation, err := strconv.Atoi(attenuationStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
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
	if len(parts) == 3 && op == PutOp {
		fmt.Printf("Setting configuration of data '%s'\n", d.DataName())
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
		fmt.Fprintln(w, d.Help())
		return

	case "metadata":
		jsonStr, err := d.NdDataMetadata()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)
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

	case "blocks":
		// GET <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		if len(parts) < 6 {
			server.BadRequest(w, r, "%q must be followed by block-coord/span-x", parts[3])
			return
		}
		queryStrings := r.URL.Query()
		var uncompressed bool
		if queryStrings.Get("uncompressed") == "true" {
			uncompressed = true
		}
		blockCoord, err := dvid.StringToChunkPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		span, err := strconv.Atoi(parts[5])
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := GetBlocks(storeCtx, uncompressed, blockCoord, span)
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
		timedLog.Infof("HTTP %s: Blocks (%s)", r.Method, r.URL)

	case "arb":
		// GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>]
		if len(parts) < 8 {
			server.BadRequest(w, r, "%q must be followed by top-left/top-right/bottom-left/res", parts[3])
			return
		}
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
		img, err := d.GetArbitraryImage(storeCtx, parts[4], parts[5], parts[6], parts[7])
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		var formatStr string
		if len(parts) >= 9 {
			formatStr = parts[8]
		}
		err = dvid.WriteImageHttp(w, img.Get(), formatStr)
		if err != nil {
			server.BadRequest(w, r, err.Error())
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
			if op == PutOp {
				if isotropic {
					err := fmt.Errorf("can only PUT 'raw' not 'isotropic' images")
					server.BadRequest(w, r, err.Error())
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
				err = PutVoxels(storeCtx, d, e, OpOptions{roi: roiptr})
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			} else {
				fmt.Printf("Getting slice: %s\n", slice)
				rawSlice, err := d.HandleIsotropy2D(slice, isotropic)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
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
				img, err := GetImage(storeCtx, d, e, roiptr)
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
			if op == GetOp {
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
				data, err := GetVolume(storeCtx, d, e, roiptr)
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
					err := fmt.Errorf("can only PUT 'raw' not 'isotropic' images")
					server.BadRequest(w, r, err.Error())
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
				err = PutVoxels(storeCtx, d, e, OpOptions{roi: roiptr})
				if err != nil {
					server.BadRequest(w, r, err.Error())
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
