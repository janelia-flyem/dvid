/*
	Package labelarray tailors the voxels data type for 64-bit labels and allows loading
	of NRGBA images (e.g., Raveler superpixel PNG images) that implicitly use slice Z as
	part of the label index.
*/
package labelarray

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
	"strconv"
	"strings"
	"sync"

	"compress/gzip"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	lz4 "github.com/janelia-flyem/go/golz4"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelarray"
	TypeName = "labelarray"
)

const HelpMessage = `
API for label block data type (github.com/janelia-flyem/dvid/datatype/labelarray)
===============================================================================

Note: Denormalizations like sparse volumes are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labelarray <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new labelarray superpixels VoxelSize=3.2,3.2,40.0

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

    Establishes labelvol data instances with which the annotations are synced.  Expects JSON to be POSTed
    with the following format:

    { "sync": "bodies" }

	To delete syncs, pass an empty string of names with query string "replace=true":

	{ "sync": "" }

    The "sync" property should be followed by a comma-delimited list of data instances that MUST
    already exist.  Currently, syncs should be created before any annotations are pushed to
    the server.  If annotations already exist, these are currently not synced.

    The labelarray data type accepts syncs to labelvol data instances.  It also accepts syncs to
	labelarray instances for multiscale.

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
    retrieving labelarray data, where data read from the underlying storage engine
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
        byte0  Bytes of block data in LZ4-compressed format.
        byte1
        ...
        byteN1

        int32  Block 2 coordinate X
        int32  Block 2 coordinate Y
        int32  Block 2 coordinate Z
        int32  # bytes for second block (N2)
        byte0  Bytes of block data in LZ4-compressed format.
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
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be throttled) 
                    are handled.  If the server can't initiate the API call right away, a 503 (Service Unavailable) 
                    status code is returned.


DELETE <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>

    Deletes "spanX" blocks of label data along X starting from given block coordinate.
    This will delete both the labelarray as well as any associated labelvol structures within this block.

    Example: 

    DELETE <api URL>/node/3f8c/segmentation/blocks/10_20_30/8

    Delete 8 blocks where first block has given block coordinate and number
    of blocks returned along x axis is "spanX". 

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    block coord   The block coordinate of the first block in X_Y_Z format.  Block coordinates
                    can be derived from voxel coordinates by dividing voxel coordinates by
                    the block size for a data type.
    spanX         The number of blocks along X.


GET  <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns a sparse volume with voxels of the given label in encoded RLE format.  The returned
	data can be optionally compressed using the "compression" option below.

	The encoding has the following possible format where integers are little endian and the order
	of data is exactly as specified below:

	Legacy RLEs ("rles") :

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
	        int32   Length of run
	        bytes   Optional payload dependent on first byte descriptor
			  ...
	
	Streaming RLEs ("srles"):

	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
	        int32   Length of run

	Streaming Binary Blocks ("blocks"):

      3 * uint32      values of gx, gy, and gz
      3 * int32       offset of first voxel of Block in DVID space (x, y, z)
      uint64          foreground label

      Stream of gx * gy * gz sub-blocks with the following data:

      byte            0 = background ONLY  (no more data for this sub-block)
                      1 = foreground ONLY  (no more data for this sub-block)
                      2 = both background and foreground so mask data required.
      mask            64 byte bitmask where each voxel is 0 (background) or 1 (foreground)

    GET Query-string Options:

	format  One of the following:
	          "rles" (default) - legacy RLEs with header including # spans.Data
			  "srles" - streaming RLEs with each RLE composed of 4 int32 (16 bytes) for x, y, z, run 
			  "blocks" - binary Block stream

    minx    Spans must be equal to or larger than this minimum x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum z voxel coordinate.
    exact   "false" if RLEs can extend a bit outside voxel bounds within border blocks.
            This will give slightly faster responses. 

    compression   Allows retrieval of data in "lz4" and "gzip"
                  compressed format.


HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns:
		200 (OK) if a sparse volume of the given label exists within any optional bounds.
		204 (No Content) if there is no sparse volume for the given label within any optional bounds.

	Note that for speed, the optional bounds are always expanded to the block-aligned containing
	subvolume, i.e., it's as if exact=false for the corresponding GET.

    GET Query-string Options:

    minx    Spans must be equal to or larger than this minimum x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum z voxel coordinate.


GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>

	Returns a sparse volume with voxels that pass through a given voxel.
	The encoding is described in the "sparsevol" request above.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30


GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>?<options>

	Returns a sparse volume with blocks of the given label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Set to 0
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Blocks [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Block coordinate of run start (dimension 0)
	        int32   Block coordinate of run start (dimension 1)
	        int32   Block coordinate of run start (dimension 2)
			  ...
	        int32   Length of run

	Note that the above format is the RLE encoding of sparsevol, where voxel coordinates
	have been replaced by block coordinates.

    GET Query-string Options:

    minx    Spans must be equal to or larger than this minimum x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum z voxel coordinate.


GET <api URL>/node/<UUID>/<data name>/maxlabel

	GET returns the maximum label for the version of data in JSON form:

		{ "maxlabel": <label #> }


GET <api URL>/node/<UUID>/<data name>/nextlabel
POST <api URL>/node/<UUID>/<data name>/nextlabel

	GET returns the next label for the version of data in JSON form:

		{ "nextlabel": <label #> }

	POST allows the client to request some # of labels that will be reserved.
	This is used if the client wants to introduce new labels.

	The request:

		{ "needed": <# of labels> }

	Response:

		{ "start": <starting label #>, "end": <ending label #> }


POST <api URL>/node/<UUID>/<data name>/merge

	Merges labels.  Requires JSON in request body using the following format:

	[toLabel1, fromLabel1, fromLabel2, fromLabel3, ...]

	The first element of the JSON array specifies the label to be used as the merge result.
	Note that it's computationally more efficient to group a number of merges into the
	same toLabel as a single merge request instead of multiple merge requests.


POST <api URL>/node/<UUID>/<data name>/split/<label>[?splitlabel=X]

	Splits a portion of a label's voxels into a new label or, if "splitlabel" is specified
	as an optional query string, the given split label.  Returns the following JSON:

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

	NOTE 1: The POSTed split sparse volume must be a subset of the given label's voxels.  You cannot
	give an arbitrary sparse volume that may span multiple labels.

	NOTE 2: If a split label is specified, it is the client's responsibility to make sure the given
	label will not create conflict with labels in other versions.  It should primarily be used in
	chain operations like "split-coarse" followed by "split" using voxels, where the new label
	created by the split coarse is used as the split label for the smaller, higher-res "split".


POST <api URL>/node/<UUID>/<data name>/split-coarse/<label>[?splitlabel=X]

	Splits a portion of a label's blocks into a new label or, if "splitlabel" is specified
	as an optional query string, the given split label.  Returns the following JSON:

		{ "label": <new label> }

	This request requires a binary sparse volume in the POSTed body with the following 
	encoded RLE format, which is similar to the "split" request format but uses block
	instead of voxel coordinates:

		All integers are in little-endian format.

	    byte     Payload descriptor:
	               Set to 0 to indicate it's a binary sparse volume.
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Blocks [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
			  ...
	        int32   Length of run

	The Notes for "split" endpoint above are applicable to this "split-coarse" endpoint.
`

var (
	dtype        Type
	encodeFormat dvid.DataValues

	zeroLabelBytes = make([]byte, 8, 8)

	DefaultBlockSize int32   = 64
	DefaultRes       float32 = imageblk.DefaultRes
	DefaultUnits             = imageblk.DefaultUnits
)

// SparseVolFormat indicates the type of encoding used for sparse volume representation.
type SparseVolFormat uint8

const (
	// Legacy RLE encoding with header that gives # spans.
	FormatLegacyRLE SparseVolFormat = iota

	// Streaming RLE encoding
	FormatStreamingRLE

	// Streaming set of binary Blocks
	FormatBinaryBlocks

	// Legacy RLE encoding computed using old system.
	FormatLegacySlowRLE
)

// returns default legacy RLE if no options set.
func svformatFromQueryString(r *http.Request) SparseVolFormat {
	switch r.URL.Query().Get("format") {
	case "srles":
		return FormatStreamingRLE
	case "blocks":
		return FormatBinaryBlocks
	case "old":
		return FormatLegacySlowRLE
	default:
		return FormatLegacyRLE
	}
}

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

type bulkLoadInfo struct {
	filenames     []string
	versionID     dvid.VersionID
	offset        dvid.Point
	extentChanged dvid.Bool
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

// GetByDataUUID returns a pointer to labelarray data given a data UUID.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelarray datatype!", source.DataName())
	}
	return data, nil
}

// GetByUUIDName returns a pointer to labelarray data given a UUID and data name.
func GetByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelarray datatype!", name)
	}
	return data, nil
}

// GetByVersionName returns a pointer to labelarray data given a version and data name.
func GetByVersionName(v dvid.VersionID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByVersionName(v, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelarray datatype!", name)
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

// Data of labelarray type is an extended form of imageblk Data
type Data struct {
	*imageblk.Data
	datastore.Updater

	// The maximum label id found in each version of this instance.
	// Can be unset if no new label was added at that version, in which case
	// you must traverse DAG to find max label of parent.
	MaxLabel map[dvid.VersionID]uint64

	// The maximum label for this instance in the entire repo.  This allows us to do
	// conflict-free merges without any relabeling.  Note that relabeling (rebasing)
	// is required if we move data between repos, e.g., when pushing remote nodes,
	// since we have no control over which labels were created remotely and there
	// could be conflicts between the local and remote repos.  When mutations only
	// occur within a single repo, however, this atomic label allows us to prevent
	// conflict across all versions within this repo.
	MaxRepoLabel uint64

	mlMu sync.RWMutex // For atomic access of MaxLabel and MaxRepoLabel

	// unpersisted data: channels for mutations and downres caching.
	syncCh   chan datastore.SyncMessage
	syncDone chan *sync.WaitGroup

	downresCh [numBlockHandlers]chan procMsg // channels into downres denomralizations.
	mutateCh  [numBlockHandlers]chan procMsg // channels into mutate (merge/split) ops.

	indexCh [numLabelHandlers]chan labelChange // channels into label indexing

	vcache   map[dvid.VersionID]blockCache
	vcacheMu sync.RWMutex
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return true
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-labelarray data %q", src.DataName())
	}
	return d.Data.CopyPropertiesFrom(d2.Data, fs)
}

// NewLabel returns a new label for the given version.
func (d *Data) NewLabel(v dvid.VersionID) (uint64, error) {
	d.mlMu.Lock()
	defer d.mlMu.Unlock()

	// Make sure we aren't trying to increment a label on a locked node.
	locked, err := datastore.LockedVersion(v)
	if err != nil {
		return 0, err
	}
	if locked {
		return 0, fmt.Errorf("can't ask for new label in a locked version id %d", v)
	}

	// Increment and store.
	d.MaxRepoLabel++
	d.MaxLabel[v] = d.MaxRepoLabel

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return 0, err
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.MaxRepoLabel)
	ctx := datastore.NewVersionedCtx(d, v)
	if err := store.Put(ctx, maxLabelTKey, buf); err != nil {
		return 0, err
	}

	ctx2 := storage.NewDataContext(d, 0)
	if err := store.Put(ctx2, maxRepoLabelTKey, buf); err != nil {
		return 0, err
	}

	return d.MaxRepoLabel, nil
}

// NewData returns a pointer to labelarray data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	if _, found := c.Get("BlockSize"); !found {
		c.Set("BlockSize", fmt.Sprintf("%d,%d,%d", DefaultBlockSize, DefaultBlockSize, DefaultBlockSize))
	}
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

// --- datastore.InstanceMutator interface -----

// LoadMutable loads mutable properties of label volumes like the maximum labels
// for each version.  Note that we load these max labels from key-value pairs
// rather than data instance properties persistence, because in the case of a crash,
// the actually stored repo data structure may be out-of-date compared to the guaranteed
// up-to-date key-value pairs for max labels.
func (d *Data) LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (bool, error) {
	ctx := storage.NewDataContext(d, 0)
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return false, fmt.Errorf("Data type labelarray had error initializing store: %v\n", err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	ch := make(chan *storage.KeyValue)

	// Start appropriate migration function if any.
	var saveRequired bool

	switch storedVersion {
	case 0:
		// Need to update all max labels and set repo-level max label.
		saveRequired = true
		dvid.Infof("Migrating old version of labelarray %q to new version\n", d.DataName())
		go d.migrateMaxLabels(root, wg, ch)
	default:
		// Load in each version max label without migration.
		go d.loadMaxLabels(wg, ch)
	}

	// Send the max label data per version
	minKey, err := ctx.MinVersionKey(maxLabelTKey)
	if err != nil {
		return false, err
	}
	maxKey, err := ctx.MaxVersionKey(maxLabelTKey)
	if err != nil {
		return false, err
	}
	keysOnly := false
	if err = store.RawRangeQuery(minKey, maxKey, keysOnly, ch, nil); err != nil {
		return false, err
	}
	wg.Wait()

	dvid.Infof("Loaded max label values for labelarray %q with repo-wide max %d\n", d.DataName(), d.MaxRepoLabel)
	return saveRequired, nil
}

func (d *Data) migrateMaxLabels(root dvid.VersionID, wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Can't initialize store for labelarray %q: %v\n", d.DataName(), err)
	}

	var maxRepoLabel uint64
	d.MaxLabel = make(map[dvid.VersionID]uint64)
	for {
		kv := <-ch
		if kv == nil {
			break
		}
		v, err := ctx.VersionFromKey(kv.K)
		if err != nil {
			dvid.Errorf("Can't decode key when loading mutable data for %s", d.DataName())
			continue
		}
		if len(kv.V) != 8 {
			dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
			continue
		}
		label := binary.LittleEndian.Uint64(kv.V)
		d.MaxLabel[v] = label
		if label > maxRepoLabel {
			maxRepoLabel = label
		}
	}

	// Adjust the MaxLabel data to make sure we correct for any case of child max < parent max.
	d.adjustMaxLabels(store, root)

	// Set the repo-wide max label.
	d.MaxRepoLabel = maxRepoLabel

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, maxRepoLabel)
	store.Put(ctx, maxRepoLabelTKey, buf)

	wg.Done()
	return
}

func (d *Data) adjustMaxLabels(store storage.KeyValueSetter, root dvid.VersionID) error {
	buf := make([]byte, 8)

	parentMax, ok := d.MaxLabel[root]
	if !ok {
		return fmt.Errorf("can't adjust version id %d since none exists in metadata", root)
	}
	childIDs, err := datastore.GetChildrenByVersion(root)
	if err != nil {
		return err
	}
	for _, childID := range childIDs {
		var save bool
		childMax, ok := d.MaxLabel[childID]
		if !ok {
			// set to parent max
			d.MaxLabel[childID] = parentMax
			save = true
		} else if childMax < parentMax {
			d.MaxLabel[childID] = parentMax + childMax + 1
			save = true
		}

		// save the key-value
		if save {
			binary.LittleEndian.PutUint64(buf, d.MaxLabel[childID])
			ctx := datastore.NewVersionedCtx(d, childID)
			store.Put(ctx, maxLabelTKey, buf)
		}

		// recurse for depth-first
		if err := d.adjustMaxLabels(store, childID); err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) loadMaxLabels(wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	var repoMax uint64
	d.MaxLabel = make(map[dvid.VersionID]uint64)
	for {
		kv := <-ch
		if kv == nil {
			break
		}
		v, err := ctx.VersionFromKey(kv.K)
		if err != nil {
			dvid.Errorf("Can't decode key when loading mutable data for %s", d.DataName())
			continue
		}
		if len(kv.V) != 8 {
			dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
			continue
		}
		label := binary.LittleEndian.Uint64(kv.V)
		d.MaxLabel[v] = label
		if label > repoMax {
			repoMax = label
		}
	}

	// Load in the repo-wide max label.
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelarray had error initializing store: %v\n", err)
		return
	}
	data, err := store.Get(ctx, maxRepoLabelTKey)
	if err != nil {
		dvid.Errorf("Error getting repo-wide max label: %v\n", err)
		return
	}
	if data == nil || len(data) != 8 {
		dvid.Errorf("Could not load repo-wide max label for instance %q.  Only got %d bytes, not 64-bit label.\n", d.DataName(), len(data))
		dvid.Errorf("Using max label across versions: %d\n", repoMax)
		d.MaxRepoLabel = repoMax
	} else {
		d.MaxRepoLabel = binary.LittleEndian.Uint64(data)
		if d.MaxRepoLabel < repoMax {
			dvid.Errorf("Saved repo-wide max for instance %q was %d, changed to largest version max %d\n", d.DataName(), d.MaxRepoLabel, repoMax)
			d.MaxRepoLabel = repoMax
		}
	}
	wg.Done()
}

// --- imageblk.IntData interface -------------

func (d *Data) BlockSize() dvid.Point {
	return d.Properties.BlockSize
}

func (d *Data) Extents() *dvid.Extents {
	return &(d.Properties.Extents)
}

// NewLabels returns labelarray Labels, a representation of externally usable subvolume
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

// GetBlocks returns a slice of bytes corresponding to all the blocks along a span in X
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

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data type labelarray had error initializing store: %v\n", err)
	}

	// if only one block is requested, avoid the range query
	if blocksize.Value(0) == int32(1) && blocksize.Value(1) == int32(1) && blocksize.Value(2) == int32(1) {
		indexBeg := dvid.IndexZYX(dvid.ChunkPoint3d{blockoffset.Value(0), blockoffset.Value(1), blockoffset.Value(2)})
		keyBeg := NewBlockTKey(&indexBeg)

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
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

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
			begTKey := NewBlockTKey(&indexBeg)
			indexEnd := dvid.IndexZYX(endPoint)
			endTKey := NewBlockTKey(&indexEnd)

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
				indexZYX, err := DecodeBlockTKey(kv.K)
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

func (d *Data) DeleteBlocks(ctx *datastore.VersionedCtx, start dvid.ChunkPoint3d, span int) error {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data type labelarray had error initializing store: %v\n", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelarray requires batch-enabled store, which %q is not\n", store)
	}

	indexBeg := dvid.IndexZYX(start)
	end := start
	end[0] += int32(span - 1)
	indexEnd := dvid.IndexZYX(end)
	begTKey := NewBlockTKey(&indexBeg)
	endTKey := NewBlockTKey(&indexEnd)

	iv := dvid.InstanceVersion{d.DataUUID(), ctx.VersionID()}
	mapping := labels.LabelMap(iv)

	kvs, err := store.GetRange(ctx, begTKey, endTKey)
	if err != nil {
		return err
	}

	mutID := d.NewMutationID()
	batch := batcher.NewBatch(ctx)
	uncompress := true
	for _, kv := range kvs {
		izyx, err := DecodeBlockTKey(kv.K)
		if err != nil {
			return err
		}

		// Delete the labelarray (really tombstones it)
		batch.Delete(kv.K)

		// Send data to delete associated labelvol for labels in this block
		compressed, _, err := dvid.DeserializeData(kv.V, uncompress)
		if err != nil {
			return fmt.Errorf("Unable to deserialize block, %s (%v): %v", ctx, kv.K, err)
		}
		block, err := labels.Decompress(compressed, d.BlockSize())
		if err != nil {
			return fmt.Errorf("Unable to decompress google compression in %q: %v\n", d.DataName(), err)
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
		evt := datastore.SyncEvent{d.DataUUID(), labels.DeleteBlockEvent}
		msg := datastore.SyncMessage{labels.DeleteBlockEvent, ctx.VersionID(), labels.DeleteBlock{izyx, block, mutID}}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			return err
		}

	}
	if err := batch.Commit(); err != nil {
		return err
	}

	// Notify any downstream downres instance that we're done with this op.
	d.publishDownresCommit(ctx.VersionID(), mutID)
	return nil
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

// PushData pushes labelarray data to a remote DVID.
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

		offset, err := dvid.StringToPoint(offsetStr, ",")
		if err != nil {
			return fmt.Errorf("Illegal offset specification: %s: %v", offsetStr, err)
		}

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

// GetBinaryData returns label data from a potentially compressed ("lz4", "gzip") reader.
func GetBinaryData(compression string, in io.ReadCloser, estsize int64) ([]byte, error) {
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
		tlog.Debugf("read 3d lz4 POST: %d bytes", len(data))
		if len(data) == 0 {
			return nil, fmt.Errorf("received 0 LZ4 compressed bytes")
		}
		tlog = dvid.NewTimeLog()
		uncompressed := make([]byte, estsize)
		err = lz4.Uncompress(data, uncompressed)
		if err != nil {
			return nil, err
		}
		data = uncompressed
		tlog.Debugf("uncompressed 3d lz4 POST: %d bytes", len(data))
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
		tlog.Debugf("read and uncompress 3d gzip POST: %d bytes", len(data))
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
	timedLog := dvid.NewTimeLog()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

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
		d.handleLabel(ctx, w, r, parts)
		timedLog.Infof("HTTP GET label at %s (%s)", parts[4], r.URL)

	case "labels":
		d.handleLabels(ctx, w, r, parts)
		timedLog.Infof("HTTP GET batch label-at-point query (%s)", r.URL)

	case "blocks":
		d.handleBlocks(ctx, w, r, parts)
		timedLog.Infof("HTTP %s blocks at size %s, offset %s (%s)", r.Method, parts[4], parts[5], r.URL)

	case "pseudocolor":
		d.handlePseudocolor(ctx, w, r, parts)
		timedLog.Infof("HTTP GET pseudocolor with shape %s, size %s, offset %s", parts[4], parts[5], parts[6])

	case "raw", "isotropic":
		d.handleDataRequest(ctx, w, r, parts)
		timedLog.Infof("HTTP %s %s with shape %s, size %s, offset %s", r.Method, parts[3], parts[4], parts[5], parts[6])

	// endpoints after this must have LabelIndex key-value pairs initialized. ---------

	case "sparsevol":
		d.handleSparsevol(ctx, w, r, parts)
		timedLog.Infof("HTTP %s: sparsevol on label %s (%s)", r.Method, parts[4], r.URL)

	case "sparsevol-by-point":
		d.handleSparsevolByPoint(ctx, w, r, parts)
		timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, parts[4], r.URL)

	case "sparsevol-coarse":
		d.handleSparsevolCoarse(ctx, w, r, parts)
		timedLog.Infof("HTTP %s: sparsevol-coarse on label %s (%s)", r.Method, parts[4], r.URL)

	case "maxlabel":
		d.handleMaxlabel(ctx, w, r, parts)
		timedLog.Infof("HTTP maxlabel request (%s)", r.URL)

	case "nextlabel":
		d.handleNextlabel(ctx, w, r, parts)
		timedLog.Infof("HTTP maxlabel request (%s)", r.URL)

	case "split":
		d.handleSplit(ctx, w, r, parts)
		timedLog.Infof("HTTP split request (%s)", r.URL)

	case "split-coarse":
		d.handleSplitCoarse(ctx, w, r, parts)
		timedLog.Infof("HTTP split-coarse request (%s)", r.URL)

	case "merge":
		d.handleMerge(ctx, w, r, parts)
		timedLog.Infof("HTTP merge request (%s)", r.URL)

	default:
		server.BadAPIRequest(w, r, d)
	}
}

// --------- Handler functions for HTTP requests --------------

func (d *Data) handleLabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
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
}

func (d *Data) handleLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/labels
	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch labels query must be a GET request")
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad GET request body for batch query: %v", err)
		return
	}
	queryStrings := r.URL.Query()
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
}

func (d *Data) handleBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?compression=...]
	sizeStr, offsetStr := parts[4], parts[5]

	queryStrings := r.URL.Query()
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
		server.BadRequest(w, r, "cannot store labels in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
		return
	}

	if strings.ToLower(r.Method) == "get" {
		if err := d.SendBlocks(ctx, w, subvol, compression); err != nil {
			server.BadRequest(w, r, err)
			return
		}
	} else {
		server.BadRequest(w, r, "DVID does not accept the %s action on the 'blocks' endpoint", r.Method)
		return
	}
}

func (d *Data) handlePseudocolor(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 7 {
		server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
		return
	}
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))

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
		if strings.ToLower(r.Method) != "get" {
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
	default:
		server.BadRequest(w, r, "DVID currently supports only 2d pseudocolor image requests")
		return
	}
}

func (d *Data) handleDataRequest(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
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
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))

	switch plane.ShapeDimensions() {
	case 2:
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if strings.ToLower(r.Method) != "get" {
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
		if strings.ToLower(r.Method) == "get" {
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
			data, err := GetBinaryData(compression, r.Body, estsize)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			mutate := queryStrings.Get("mutate") == "true"
			if err = d.PutLabels(ctx.VersionID(), subvol, data, roiname, mutate); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
	default:
		server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
		return
	}
}

func (d *Data) getSparsevolOptions(r *http.Request) (b dvid.Bounds, compression string, err error) {
	queryStrings := r.URL.Query()
	compression = queryStrings.Get("compression")

	if b.Voxel, err = dvid.OptionalBoundsFromQueryString(r); err != nil {
		err = fmt.Errorf("Error parsing bounds from query string: %v\n", err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	b.Block = b.Voxel.Divide(blockSize)
	b.Exact = true
	if queryStrings.Get("exact") == "false" {
		b.Exact = false
	}
	return
}

func (d *Data) handleSparsevol(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// POST <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
		return
	}
	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	b, compression, err := d.getSparsevolOptions(r)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	switch strings.ToLower(r.Method) {
	case "get":
		w.Header().Set("Content-type", "application/octet-stream")

		var found bool
		format := svformatFromQueryString(r)
		switch format {
		case FormatLegacyRLE, FormatLegacySlowRLE:
			found, err = d.WriteLegacyRLE(ctx, label, b, compression, format, w)
		case FormatBinaryBlocks:
		case FormatStreamingRLE:
			found, err = d.WriteStreamingRLE(ctx, label, b, compression, w)
		}
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}

	case "head":
		w.Header().Set("Content-type", "text/html")
		found, err := d.FoundSparseVol(ctx, label, b)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if found {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return

	case "post":
		server.BadRequest(w, r, "POST of sparsevol not currently implemented\n")
		return
		// if err := d.PutSparseVol(versionID, label, r.Body); err != nil {
		// 	server.BadRequest(w, r, err)
		// 	return
		// }
	default:
		server.BadRequest(w, r, "Unable to handle HTTP action %s on sparsevol endpoint", r.Method)
		return
	}
}

func (d *Data) handleSparsevolByPoint(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
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
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	b, compression, err := d.getSparsevolOptions(r)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")

	format := svformatFromQueryString(r)
	var found bool
	switch format {
	case FormatLegacyRLE, FormatLegacySlowRLE:
		found, err = d.WriteLegacyRLE(ctx, label, b, compression, format, w)
	case FormatBinaryBlocks:
	case FormatStreamingRLE:
		found, err = d.WriteStreamingRLE(ctx, label, b, compression, w)
	}
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (d *Data) handleSparsevolCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label ID to follow 'sparsevol-coarse' command")
		return
	}
	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	var b dvid.Bounds
	b.Voxel, err = dvid.OptionalBoundsFromQueryString(r)
	if err != nil {
		server.BadRequest(w, r, "Error parsing bounds from query string: %v\n", err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		server.BadRequest(w, r, "Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	b.Block = b.Voxel.Divide(blockSize)
	data, err := d.GetSparseCoarseVol(ctx, label, b)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if data == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-type", "application/octet-stream")
	_, err = w.Write(data)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
}

func (d *Data) handleMaxlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/maxlabel
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(r.Method) {
	case "get":
		maxlabel, ok := d.MaxLabel[ctx.VersionID()]
		if !ok {
			server.BadRequest(w, r, "No maximum label found for %s version %d\n", d.DataName(), ctx.VersionID())
			return
		}
		fmt.Fprintf(w, "{%q: %d}", "maxlabel", maxlabel)
	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
}

func (d *Data) handleNextlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/nextlabel
	// POST <api URL>/node/<UUID>/<data name>/nextlabel
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(r.Method) {
	case "get":
		fmt.Fprintf(w, "{%q: %d}", "nextlabel", d.MaxRepoLabel+1)
	case "post":
		server.BadRequest(w, r, "POST on maxlabel is not supported yet.\n")
		return
	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
}

func (d *Data) handleSplit(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/split/<label>[?splitlabel=X]
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Split requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
		return
	}
	fromLabel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if fromLabel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	var splitLabel uint64
	queryStrings := r.URL.Query()
	splitStr := queryStrings.Get("splitlabel")
	if splitStr != "" {
		splitLabel, err = strconv.ParseUint(splitStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "Bad parameter for 'splitlabel' query string (%q).  Must be uint64.\n", splitStr)
		}
	}
	toLabel, err := d.SplitLabels(ctx.VersionID(), fromLabel, splitLabel, r.Body)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %d}", "label", toLabel)
}

func (d *Data) handleSplitCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/split-coarse/<label>[?splitlabel=X]
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Split-coarse requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
		return
	}
	fromLabel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if fromLabel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	var splitLabel uint64
	queryStrings := r.URL.Query()
	splitStr := queryStrings.Get("splitlabel")
	if splitStr != "" {
		splitLabel, err = strconv.ParseUint(splitStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "Bad parameter for 'splitlabel' query string (%q).  Must be uint64.\n", splitStr)
		}
	}
	toLabel, err := d.SplitCoarseLabels(ctx.VersionID(), fromLabel, splitLabel, r.Body)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split-coarse: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %d}", "label", toLabel)
}

func (d *Data) handleMerge(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/merge
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Merge requests must be POST actions.")
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad POSTed data for merge.  Should be JSON.")
		return
	}
	var tuple labels.MergeTuple
	if err := json.Unmarshal(data, &tuple); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad merge op JSON: %v", err))
		return
	}
	mergeOp, err := tuple.Op()
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if err := d.MergeLabels(ctx.VersionID(), mergeOp); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Error on merge: %v", err))
		return
	}
}

// --------- Other functions on labelarray Data -----------------

// GetLabelBlock returns a block of labels corresponding to the block coordinate.
func (d *Data) GetLabelBlock(v dvid.VersionID, blockCoord dvid.ChunkPoint3d) ([]byte, error) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return nil, err
	}

	// Retrieve the block of labels
	ctx := datastore.NewVersionedCtx(d, v)
	index := dvid.IndexZYX(blockCoord)
	serialization, err := store.Get(ctx, NewBlockTKey(&index))
	if err != nil {
		return nil, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), blockCoord)
	}
	if serialization == nil {
		return []byte{}, nil
	}
	compressed, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("Unable to deserialize block %s in '%s': %v\n", blockCoord, d.DataName(), err)
	}
	labelData, err := labels.Decompress(compressed, d.BlockSize())
	if err != nil {
		return nil, fmt.Errorf("Unable to decompress google compression in %q: %v\n", d.DataName(), err)
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
	blockCoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	labelData, err := d.GetLabelBlock(v, blockCoord)
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
