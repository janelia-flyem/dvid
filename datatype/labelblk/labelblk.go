/*
	Package labelblk supports only label volumes.  See labelvol package for support
	of sparse labels.  The labelblk and labelvol datatypes typically are synced to each other.
*/
package labelblk

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync"

	"compress/gzip"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelblk"
	TypeName = "labelblk"
)

const helpMessage = `
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

    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")

$ dvid node <UUID> <data name> load <offset> <image glob> <settings...>

    Initializes version node to a set of XY label images described by glob of filenames.
    The DVID server must have access to the named files.  Currently, XY images are required.

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

POST  <api URL>/node/<UUID>/<data name>/resolution
  
  	Sets the resolution for the image volume. 
  
  	Extents should be in JSON in the following format:
  	[8,8,8]

POST <api URL>/node/<UUID>/<data name>/sync?<options>

    Appends data instances with which this labelblk is synced.  Expects JSON to be POSTed
    with the following format:

    { "sync": "bodies" }

	To delete syncs, pass an empty string of names with query string "replace=true":

	{ "sync": "" }

    The "sync" property should be followed by a comma-delimited list of data instances that MUST
    already exist.  Currently, syncs should be created before any annotations are pushed to
    the server.  If annotations already exist, these are currently not synced.

    The labelblk data type accepts syncs to labelvol and other labelblk data instances.  Syncs to
	labelblk instances automatically create a 2x downsampling compared to the synced labelblk,
	for use in multiscale.

    GET Query-string Options:

    replace    Set to "true" if you want passed syncs to replace and not be appended to current syncs.
			   Default operation is false.


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
    compression   Allows retrieval or submission of 3d data in "lz4","gzip", "google"
		            (neuroglancer compression format), "googlegzip" (google + gzip)
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

GET  <api URL>/node/<UUID>/<data name>/pseudocolor/<dims>/<size>/<offset>[?queryopts]

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

GET <api URL>/node/<UUID>/<data name>/labels[?queryopts]

	Returns JSON for the labels at a list of coordinates.  Expects JSON in GET body:

	[ [x0, y0, z0], [x1, y1, z1], ...]

	Returns for each POSTed coordinate the corresponding label:

	[ 23, 911, ...]
	
    Arguments:
    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.

    Query-string Options:

    hash          MD5 hash of request body content in hexidecimal string format.

GET  <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?queryopts]

    Retrieves blocks corresponding to the extents specified by the size and offset.  The
    subvolume request must be block aligned.  This is the most server-efficient way of
    retrieving labelblk data, where data read from the underlying storage engine
    is written directly to the HTTP connection.

    Example: 

    GET <api URL>/node/3f8c/segmentation/blocks/64_64_64/0_0_0

	If block size is 32x32x32, this call retrieves up to 8 blocks where the first potential
	block is at 0, 0, 0.  The returned byte stream has a list of blocks with a leading block 
	coordinate (3 x int32) plus int32 giving the # of bytes in this block, and  then the 
	bytes for the value.  If blocks are unset within the span, they will not appear in the stream,
	so the returned data will be equal to or less than spanX blocks worth of data.  

    The returned data format has the following format where int32 is in little endian and the bytes of
    block data have been compressed in LZ4 format.

        int32  Block 1 coordinate X (Note that this may not be starting block coordinate if it is unset.)
        int32  Block 1 coordinate Y
        int32  Block 1 coordinate Z
        int32  # bytes for first block (N1)
        byte0  Block N1 serialization using chosen compression format (see "compression" option below)
        byte1
        ...
        byteN1

        int32  Block 2 coordinate X
        int32  Block 2 coordinate Y
        int32  Block 2 coordinate Z
        int32  # bytes for second block (N2)
        byte0  Block N2 serialization using chosen compression format (see "compression" option below)
        byte1
        ...
        byteN2

        ...

    If no data is available for given block span, nothing is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    compression   Allows retrieval of block data in "lz4" (default) or "uncompressed".
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be 
                  throttled) are handled.  If the server can't initiate the API call right away, a 503 
                  (Service Unavailable) status code is returned.
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
	return helpMessage
}

// -------

// GetByDataUUID returns a pointer to labelblk data given a data UUID.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelblk datatype!", source.DataName())
	}
	return data, nil
}

// GetByUUIDName returns a pointer to labelblk data given a UUID and data name.
func GetByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelblk datatype!", name)
	}
	return data, nil
}

// GetByVersionName returns a pointer to labelblk data given a version and data name.
func GetByVersionName(v dvid.VersionID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByVersionName(v, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelblk datatype!", name)
	}
	return data, nil
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
	datastore.Updater

	// unpersisted data: channels for mutations and downres caching.
	syncCh   chan datastore.SyncMessage
	syncDone chan *sync.WaitGroup

	procCh    [numBlockHandlers]chan procMsg // channels into block processors.
	vcache    map[dvid.VersionID]blockCache
	vcache_mu sync.RWMutex
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return true
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties.  Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-labelblk data %q", src.DataName())
	}
	return d.Data.CopyPropertiesFrom(d2.Data, fs)
}

// NewData returns a pointer to labelblk data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	imgblkData, err := dtype.Type.NewData(uuid, id, name, c)
	if err != nil {
		return nil, err
	}

	data := &Data{
		Data: imgblkData,
	}

	return data, nil
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended imageblk.Properties
	}{
		d.Data.Data,
		d.Data.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
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
				data, err = d.convertTo64bit(geom, data, int(inputBytesPerVoxel), int(actualStride))
				if err != nil {
					return nil, err
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

func sendBlockLZ4(w http.ResponseWriter, x, y, z int32, v []byte, compression string) error {
	// Check internal format and see if it's valid with compression choice.
	format, checksum := dvid.DecodeSerializationFormat(dvid.SerializationFormat(v[0]))
	if (compression == "lz4" || compression == "") && format != dvid.LZ4 {
		return fmt.Errorf("Expected internal block data to be LZ4, was %s instead.", format)
	}

	// Send block coordinate and size of data.
	if err := binary.Write(w, binary.LittleEndian, x); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, y); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, z); err != nil {
		return err
	}

	start := 5
	if checksum == dvid.CRC32 {
		start += 4
	}

	// Do any adjustment of sent data based on compression request
	var data []byte
	if compression == "uncompressed" {
		var err error
		data, _, err = dvid.DeserializeData(v, true)
		if err != nil {
			return err
		}
	} else {
		data = v[start:]
	}
	n := len(data)
	if err := binary.Write(w, binary.LittleEndian, int32(n)); err != nil {
		return err
	}

	// Send data itself, skipping the first byte for internal format and next 4 for uncompressed length.
	if written, err := w.Write(data); err != nil || written != n {
		if err != nil {
			return err
		}
		return fmt.Errorf("could not write %d bytes of value: only %d bytes written", n, written)
	}
	return nil
}

// SendBlocks writes all blocks within the given subvolume to the http.ResponseWriter.
func (d *Data) SendBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, subvol *dvid.Subvolume, compression string) error {
	w.Header().Set("Content-type", "application/octet-stream")

	if compression != "uncompressed" && compression != "lz4" && compression != "" {
		return fmt.Errorf("don't understand 'compression' query string value: %s", compression)
	}

	// convert x,y,z coordinates to block coordinates
	blocksize := subvol.Size().Div(d.BlockSize())
	blockoffset := subvol.StartPoint().Div(d.BlockSize())

	timedLog := dvid.NewTimeLog()
	defer timedLog.Infof("SendBlocks %s, span x %d, span y %d, span z %d", blockoffset, blocksize.Value(0), blocksize.Value(1), blocksize.Value(2))

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelblk had error initializing store: %v\n", err)
	}

	// if only one block is requested, avoid the range query
	if blocksize.Value(0) == int32(1) && blocksize.Value(1) == int32(1) && blocksize.Value(2) == int32(1) {
		indexBeg := dvid.IndexZYX(dvid.ChunkPoint3d{blockoffset.Value(0), blockoffset.Value(1), blockoffset.Value(2)})
		keyBeg := NewTKey(&indexBeg)

		value, err := store.Get(ctx, keyBeg)
		if err != nil {
			return err
		}
		if len(value) > 0 {
			return sendBlockLZ4(w, blockoffset.Value(0), blockoffset.Value(1), blockoffset.Value(2), value, compression)
		}
		return nil
	}

	// only do one request at a time, although each request can start many goroutines.
	server.LargeMutationMutex.Lock()
	defer server.LargeMutationMutex.Unlock()

	okv := store.(storage.BufferableOps)
	// extract buffer interface
	req, hasbuffer := okv.(storage.KeyValueRequester)
	if hasbuffer {
		okv = req.NewBuffer(ctx)
	}

	for ziter := int32(0); ziter < blocksize.Value(2); ziter++ {
		for yiter := int32(0); yiter < blocksize.Value(1); yiter++ {
			beginPoint := dvid.ChunkPoint3d{blockoffset.Value(0), blockoffset.Value(1) + yiter, blockoffset.Value(2) + ziter}
			endPoint := dvid.ChunkPoint3d{blockoffset.Value(0) + blocksize.Value(0) - 1, blockoffset.Value(1) + yiter, blockoffset.Value(2) + ziter}

			indexBeg := dvid.IndexZYX(beginPoint)
			sx, sy, sz := indexBeg.Unpack()
			begTKey := NewTKey(&indexBeg)
			indexEnd := dvid.IndexZYX(endPoint)
			endTKey := NewTKey(&indexEnd)

			// Send the entire range of key-value pairs to chunk processor
			err = okv.ProcessRange(ctx, begTKey, endTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
				if c == nil || c.TKeyValue == nil {
					return nil
				}
				kv := c.TKeyValue
				if kv.V == nil {
					return nil
				}

				// Determine which block this is.
				indexZYX, err := DecodeTKey(kv.K)
				if err != nil {
					return err
				}
				x, y, z := indexZYX.Unpack()
				if z != sz || y != sy || x < sx || x >= sx+int32(blocksize.Value(0)) {
					return nil
				}
				if err := sendBlockLZ4(w, x, y, z, kv.V, compression); err != nil {
					return err
				}
				return nil
			})

			if err != nil {
				return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
			}
		}
	}

	if hasbuffer {
		// submit the entire buffer to the DB
		err = okv.(storage.RequestBuffer).Flush()

		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %v", ctx, err)

		}
	}

	return err
}

// --- datastore.DataService interface ---------

// PushData pushes labelblk data to a remote DVID.
func (d *Data) PushData(p *datastore.PushSession) error {
	// Delegate to imageblk's implementation.
	return d.Data.PushData(p)
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

// compressGoogle uses the neuroglancer compression format
func compressGoogle(data []byte, subvol *dvid.Subvolume) ([]byte, error) {
	// TODO: share table between blocks
	subvolsizes := subvol.Size()

	// must <= 32
	BLKSIZE := int32(8)

	xsize := subvolsizes.Value(0)
	ysize := subvolsizes.Value(1)
	zsize := subvolsizes.Value(2)
	gx := subvolsizes.Value(0) / BLKSIZE
	gy := subvolsizes.Value(1) / BLKSIZE
	gz := subvolsizes.Value(2) / BLKSIZE
	if xsize%BLKSIZE > 0 || ysize%BLKSIZE > 0 || zsize%BLKSIZE > 0 {
		return nil, fmt.Errorf("volume must be a multiple of the block size")
	}

	// add initial 4 byte to designate as a header for the compressed data
	// 64 bit headers for each 8x8x8 block and pre-allocate some data based on expected data size
	dword := 4
	globaloffset := dword

	datagoogle := make([]byte, gx*gy*gz*8+int32(globaloffset), xsize*ysize*zsize*8/10)
	datagoogle[0] = byte(globaloffset / dword) // compressed data starts after first 4 bytes

	// everything is written out little-endian
	for gziter := int32(0); gziter < gz; gziter++ {
		for gyiter := int32(0); gyiter < gy; gyiter++ {
			for gxiter := int32(0); gxiter < gx; gxiter++ {
				unique_vals := make(map[uint64]uint32)
				unique_list := make([]uint64, 0)

				currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE) * 8

				// extract unique values in the 8x8x8 block
				for z := int32(0); z < BLKSIZE; z++ {
					for y := int32(0); y < BLKSIZE; y++ {
						for x := int32(0); x < BLKSIZE; x++ {
							if _, ok := unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])]; !ok {
								unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])] = 0
								unique_list = append(unique_list, binary.LittleEndian.Uint64(data[currpos:currpos+8]))
							}
							currpos += 8
						}
						currpos += ((xsize - BLKSIZE) * 8)
					}
					currpos += (xsize*ysize - (xsize * (BLKSIZE))) * 8
				}
				// write out mapping
				for pos, val := range unique_list {
					unique_vals[val] = uint32(pos)
				}

				// write-out compressed data
				encodedBits := uint32(math.Ceil(math.Log2(float64(len(unique_vals)))))
				switch {
				case encodedBits == 0, encodedBits == 1, encodedBits == 2:
				case encodedBits <= 4:
					encodedBits = 4
				case encodedBits <= 8:
					encodedBits = 8
				case encodedBits <= 16:
					encodedBits = 16
				}

				// starting location for writing out data
				currpos2 := len(datagoogle)
				compressstart := len(datagoogle) / dword // in 4-byte units
				// number of bytes to add (encode bytes + table size of 8 byte numbers)
				addedBytes := uint32(encodedBits*uint32(BLKSIZE*BLKSIZE*BLKSIZE)/8) + uint32(len(unique_vals)*8) // will always be a multiple of 4 bytes
				datagoogle = append(datagoogle, make([]byte, addedBytes)...)

				// do not need to write-out anything if there is only one entry
				if encodedBits > 0 {
					currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE) * 8

					for z := uint32(0); z < uint32(BLKSIZE); z++ {
						for y := uint32(0); y < uint32(BLKSIZE); y++ {
							for x := uint32(0); x < uint32(BLKSIZE); x++ {
								mappedval := unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])]
								currpos += 8

								// write out encoding
								startbit := uint32((encodedBits * x) % uint32(8))
								if encodedBits == 16 {
									// write two bytes worth of data
									datagoogle[currpos2] = byte(255 & mappedval)
									currpos2++
									datagoogle[currpos2] = byte(255 & (mappedval >> 8))
									currpos2++
								} else {
									// write bit-shifted data
									datagoogle[currpos2] |= byte(255 & (mappedval << startbit))
								}
								if int(startbit) == (8 - int(encodedBits)) {
									currpos2++
								}

							}
							currpos += ((xsize - BLKSIZE) * 8)
						}
						currpos += (xsize*ysize - (xsize * (BLKSIZE))) * 8
					}
				}
				tablestart := currpos2 / dword // in 4-byte units
				// write-out lookup table
				for _, val := range unique_list {
					for bytespot := uint32(0); bytespot < uint32(8); bytespot++ {
						datagoogle[currpos2] = byte(255 & (val >> (bytespot * 8)))
						currpos2++
					}
				}

				// write-out block header
				// 8 bytes per header entry
				headerpos := (gziter*(gy*gx)+gyiter*gx+gxiter)*8 + int32(globaloffset) // shift start by global offset

				// write out lookup table start
				tablestart -= (globaloffset / dword) // relative to the start of the compressed data
				datagoogle[headerpos] = byte(255 & tablestart)
				headerpos++
				datagoogle[headerpos] = byte(255 & (tablestart >> 8))
				headerpos++
				datagoogle[headerpos] = byte(255 & (tablestart >> 16))
				headerpos++

				// write out number of encoded bits
				datagoogle[headerpos] = byte(255 & encodedBits)
				headerpos++

				// write out block compress start
				compressstart -= (globaloffset / dword) // relative to the start of the compressed data
				datagoogle[headerpos] = byte(255 & compressstart)
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 8))
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 16))
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 24))
			}
		}
	}

	return datagoogle, nil
}

func sendBinaryData(compression string, data []byte, subvol *dvid.Subvolume, w http.ResponseWriter) error {
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
	case "google", "googlegzip": // see neuroglancer for details of compressed segmentation format
		datagoogle, err := compressGoogle(data, subvol)
		if err != nil {
			return err
		}
		if compression == "googlegzip" {
			w.Header().Set("Content-encoding", "gzip")
			gw := gzip.NewWriter(w)
			if _, err = gw.Write(datagoogle); err != nil {
				return err
			}
			if err = gw.Close(); err != nil {
				return err
			}

		} else {
			_, err = w.Write(datagoogle)
			if err != nil {
				return err
			}
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
		if err = lz4.Uncompress(data, uncompressed); err != nil {
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

// SetResolution loads JSON data giving Resolution.
func (d *Data) SetResolution(uuid dvid.UUID, jsonBytes []byte) error {
	config := make(dvid.NdFloat32, 3)
	if err := json.Unmarshal(jsonBytes, &config); err != nil {
		return err
	}
	d.Properties.VoxelSize = config
	if err := datastore.SaveDataByUUID(uuid, d); err != nil {
		return err
	}
	return nil
}

// if hash is not empty, make sure it is hash of data.
func checkContentHash(hash string, data []byte) error {
	if hash == "" {
		return nil
	}
	hexHash := fmt.Sprintf("%x", md5.Sum(data))
	if hexHash != hash {
		return fmt.Errorf("content hash incorrect.  expected %s, got %s", hash, hexHash)
	}
	return nil
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
		jsonStr, err := d.NdDataMetadata(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)

	case "resolution":
		jsonBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.SetResolution(uuid, jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "sync":
		if action != "post" {
			server.BadRequest(w, r, "Only POST allowed to sync endpoint")
			return
		}
		replace := r.URL.Query().Get("replace") == "true"
		if err := datastore.SetSyncByJSON(d, uuid, replace, r.Body); err != nil {
			server.BadRequest(w, r, err)
			return
		}

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
		if action != "get" {
			server.BadRequest(w, r, "Batch labels query must be a GET request")
			return
		}
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, "Bad GET request body for batch query: %v", err)
			return
		}
		hash := queryStrings.Get("hash")
		if err := checkContentHash(hash, data); err != nil {
			server.BadRequest(w, r, err)
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
		// GET <api URL>/node/<UUID>/<data name>/blocks/<coord>/<offset>[?compression=...]
		sizeStr, offsetStr := parts[4], parts[5]

		if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
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

		if subvol.StartPoint().NumDims() != 3 || subvol.Size().NumDims() != 3 {
			server.BadRequest(w, r, "must specify 3D subvolumes", subvol.StartPoint(), subvol.EndPoint())
			return
		}

		// Make sure subvolume gets align with blocks
		if !dvid.BlockAligned(subvol, d.BlockSize()) {
			server.BadRequest(w, r, "cannot use labels via 'block' endpoint in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
			return
		}

		if action == "get" {
			if err := d.SendBlocks(ctx, w, subvol, compression); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		} else {
			server.BadRequest(w, r, "DVID does not accept the %s action on the 'blocks' endpoint", action)
			return
		}

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
			if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
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
				if err := sendBinaryData(compression, data, subvol, w); err != nil {
					server.BadRequest(w, r, err)
					return
				}
			} else {
				if isotropic {
					server.BadRequest(w, r, "can only POST 'raw' not 'isotropic' images")
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
				mutID := d.NewMutationID()
				if queryStrings.Get("mutate") == "true" {
					if err = d.MutateVoxels(ctx.VersionID(), mutID, lbl.Voxels, roiname); err != nil {
						server.BadRequest(w, r, err)
						return
					}
				} else {
					if err = d.IngestVoxels(ctx.VersionID(), mutID, lbl.Voxels, roiname); err != nil {
						server.BadRequest(w, r, err)
						return
					}
				}
				// Let any synced downres instance that we've completed block-level ops.
				d.publishDownresCommit(ctx.VersionID(), mutID)
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
			return
		}

	default:
		server.BadAPIRequest(w, r, d)
	}
}

// --------- Other functions on labelblk Data -----------------

// GetLabelBytes returns a slice of little-endian uint64 corresponding to the block coordinate.
func (d *Data) GetLabelBytes(v dvid.VersionID, bcoord dvid.ChunkPoint3d) ([]byte, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// Retrieve the block of labels
	ctx := datastore.NewVersionedCtx(d, v)
	index := dvid.IndexZYX(bcoord)
	serialization, err := store.Get(ctx, NewTKey(&index))
	if err != nil {
		return nil, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), bcoord)
	}
	if serialization == nil {
		return []byte{}, nil
	}
	labelData, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("Unable to deserialize block %s in '%s': %v\n", bcoord, d.DataName(), err)
	}
	return labelData, nil
}

// GetLabelBytesAtPoint returns the 8 byte slice corresponding to a 64-bit label at a point.
func (d *Data) GetLabelBytesAtPoint(v dvid.VersionID, pt dvid.Point) ([]byte, error) {
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("Can't determine block of point %s", pt)
	}
	blockSize := d.BlockSize()
	bcoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	labelData, err := d.GetLabelBytes(v, bcoord)
	if err != nil {
		return nil, err
	}
	if len(labelData) == 0 {
		return zeroLabelBytes, nil
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
