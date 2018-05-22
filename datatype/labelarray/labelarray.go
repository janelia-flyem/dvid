/*
	Package labelarray handles both volumes of label data as well as indexing to
	quickly find and generate sparse volumes of any particular label.
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
	"net/url"
	"strconv"
	"strings"
	"sync"

	"compress/gzip"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelarray"
	TypeName = "labelarray"
)

const helpMessage = `
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

    UUID            Hexidecimal string with enough characters to uniquely identify a version node.
    data name       Name of data to create, e.g., "superpixels"
    settings        Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    BlockSize       Size in pixels  (default: %s)
    VoxelSize       Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits      Resolution units (default: "nanometers")
	IndexedLabels   "false" if no sparse volume support is required (default "true")
	CountLabels     "false" if no voxel counts per label is required (default "true")
	MaxDownresLevel  The maximum down-res level supported.  Each down-res is factor of 2.

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

 POST /api/repo/{uuid}/instance

	Creates a new instance of the labelarray data type.  Expects configuration data in JSON
	as the body of the POST.  Configuration data is a JSON object with each property
	corresponding to a configuration keyword for the particular data type.  

	JSON name/value pairs:

	REQUIRED "typename"         Must be "labelarray"
	REQUIRED "dataname"         Name of the new instance
	OPTIONAL "versioned"        If "false" or "0", the data is unversioned and acts as if 
	                             all UUIDs within a repo become the root repo UUID.  (True by default.)
    OPTIONAL "BlockSize"        Size in pixels  (default: 64,64,64)
    OPTIONAL "VoxelSize"        Resolution of voxels (default: 8.0,8.0,8.0)
    OPTIONAL "VoxelUnits"       Resolution units (default: "nanometers")
	OPTIONAL "IndexedLabels"    "false" if no sparse volume support is required (default "true")
	OPTIONAL "CountLabels"      "false" if no voxel counts per label is required (default "true")
	OPTIONAL "MaxDownresLevel"  The maximum down-res level supported.  Each down-res is factor of 2.
	

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


GET  <api URL>/node/<UUID>/<data name>/specificblocks[?queryopts]

    Retrieves blocks corresponding to those specified in the query string.  This interface
    is useful if the blocks retrieved are not consecutive or if the backend in non ordered.

    TODO: enable arbitrary compression to be specified

    Example: 

    GET <api URL>/node/3f8c/grayscale/specificblocks?blocks=x1,y1,z2,x2,y2,z2,x3,y3,z3
	
	This will fetch blocks at position (x1,y1,z1), (x2,y2,z2), and (x3,y3,z3).
	The returned byte stream has a list of blocks with a leading block 
	coordinate (3 x int32) plus int32 giving the # of bytes in this block, and  then the 
	bytes for the value.  If blocks are unset within the span, they will not appear in the stream,
	so the returned data will be equal to or less than spanX blocks worth of data.  

    The returned data format has the following format where int32 is in little endian and the bytes of
    block data have been compressed in JPEG format.

        int32  Block 1 coordinate X (Note that this may not be starting block coordinate if it is unset.)
        int32  Block 1 coordinate Y
        int32  Block 1 coordinate Z
        int32  # bytes for first block (N1)
        byte0  Bytes of block data in jpeg-compressed format.
        byte1
        ...
        byteN1

        int32  Block 2 coordinate X
        int32  Block 2 coordinate Y
        int32  Block 2 coordinate Z
        int32  # bytes for second block (N2)
        byte0  Bytes of block data in jpeg-compressed format.
        byte1
        ...
        byteN2

        ...

    If no data is available for given block span, nothing is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.

    Query-string Options:

    blocks	  x,y,z... block string
    scale         A number from 0 up to MaxDownresLevel where each level has 1/2 resolution of
	              previous level.  Level 0 (default) is the highest resolution.


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
    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
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
    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
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

GET <api URL>/node/<UUID>/<data name>/label/<coord>[?queryopts]

	Returns JSON for the label at the given coordinate:
	{ "Label": 23 }
	
    Arguments:
    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30

    Query-string Options:

    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.

GET <api URL>/node/<UUID>/<data name>/labels[?queryopts]

	Returns JSON for the labels at a list of coordinates.  Expects JSON in GET body:

	[ [x0, y0, z0], [x1, y1, z1], ...]

	Returns for each POSTed coordinate the corresponding label:

	[ 23, 911, ...]
	
    Arguments:
    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.

    Query-string Options:

    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
    hash          MD5 hash of request body content in hexidecimal string format.

GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?queryopts]

    Gets blocks corresponding to the extents specified by the size and offset.  The
    subvolume request must be block aligned.  This is the most server-efficient way of
    retrieving the labelarray data, where data read from the underlying storage engine is 
	written directly to the HTTP connection possibly after recompression to match the given 
	query-string compression option.  The default labelarray compression 
	is gzip on compressed DVID label Block serialization ("blocks" option).

    Example: 

    GET <api URL>/node/3f8c/segmentation/blocks/64_64_64/0_0_0

	If block size is 32x32x32, this call retrieves up to 8 blocks where the first potential
	block is at 0, 0, 0.  The returned byte stream has a list of blocks with a leading block 
	coordinate (3 x int32) plus int32 giving the # of bytes in this block, and  then the 
	bytes for the value.  If blocks are unset within the span, they will not appear in the stream,
	so the returned data will be equal to or less than spanX blocks worth of data.  

    The returned data format has the following format where int32 is in little endian and the 
	bytes of block data have been compressed in the desired output format.

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

    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
    compression   Allows retrieval of block data in "lz4" (default), "gzip", blocks" (native DVID
	              label blocks) or "uncompressed" (uint64 labels).
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be 
	              throttled) are handled.  If the server can't initiate the API call right away, a 503 
                  (Service Unavailable) status code is returned.


POST <api URL>/node/<UUID>/<data name>/blocks[?queryopts]

    Puts properly-sized blocks for this data instance.  This is the most server-efficient way of
    storing labelarray data, where data read from the HTTP stream is written directly to the 
	underlying storage.  The default (and currently only supported) compression is gzip on compressed 
	DVID label Block serialization.

	Note that maximum label and extents are automatically handled during these calls.

    Example: 

    POST <api URL>/node/3f8c/segmentation/blocks

    The posted data format should be in the following format where int32 is in little endian and 
	the bytes of block data have been compressed in the desired output format.

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

	The Block serialization format is as follows:

      3 * uint32      values of gx, gy, and gz
      uint32          # of labels (N), cannot exceed uint32.
      N * uint64      packed labels in little-endian format.  Label 0 can be used to represent
                          deleted labels, e.g., after a merge operation to avoid changing all
                          sub-block indices.

      ----- Data below is only included if N > 1, otherwise it is a solid block.
            Nsb = # sub-blocks = gx * gy * gz

      Nsb * uint16        # of labels for sub-blocks.  Each uint16 Ns[i] = # labels for sub-block i.
                              If Ns[i] == 0, the sub-block has no data (uninitialized), which
                              is useful for constructing Blocks with sparse data.

      Nsb * Ns * uint32   label indices for sub-blocks where Ns = sum of Ns[i] over all sub-blocks.
                              For each sub-block i, we have Ns[i] label indices of lBits.

      Nsb * values        sub-block indices for each voxel.
                              Data encompasses 512 * ceil(log2(Ns[i])) bits, padded so no two
                              sub-blocks have indices in the same byte.
                              At most we use 9 bits per voxel for up to the 512 labels in sub-block.
                              A value gives the sub-block index which points to the index into
                              the N labels.  If Ns[i] <= 1, there are no values.  If Ns[i] = 0,
                              the 8x8x8 voxels are set to label 0.  If Ns[i] = 1, all voxels
                              are the given label index.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.

    Query-string Options:

    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
	downres       "false" (default) or "true", specifies whether the given blocks should be
	                down-sampled to lower resolution.  If "true", scale must be "0" or absent.
    compression   Specifies compression format of block data: default and only option currently is
                    "blocks" (native DVID label blocks).
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be 
	                throttled) are handled.  If the server can't initiate the API call right away, a 503 
                    (Service Unavailable) status code is returned.


GET <api URL>/node/<UUID>/<data name>/maxlabel

	GET returns the maximum label for the version of data in JSON form:

		{ "maxlabel": <label #> }


-------------------------------------------------------------------------------------------------------
--- The following endpoints require the labelarray data instance to have IndexedLabels set to true. ---
-------------------------------------------------------------------------------------------------------

GET  <api URL>/node/<UUID>/<data name>/sparsevol-size/<label>?<options>

	Returns JSON giving the number of native blocks and the coarse bounding box in DVID
	coordinates (voxel space).

	Example return:

	{ "numblocks": 1081, "minvoxel": [886, 513, 744], "maxvoxel": [1723, 1279, 4855]}

	Note that the minvoxel and maxvoxel coordinates are voxel coordinates that are
	accurate to the block, not the voxel.

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

      3 * uint32      values of gx, gy, and gz -- the # of sub-blocks along each dimension in a Block.
      uint64          foreground label

      Stream of blocks.  Each block has the following data:

		3 * int32       offset of first voxel of Block in DVID space (x, y, z)
		byte            content flag:
						0 = background ONLY  (no more data for this block)
						1 = foreground ONLY  (no more data for this block)
						2 = both background and foreground so stream of sub-blocks required.

		If content is both background and foreground, stream of gx * gy * gz sub-blocks with the following data:

		byte            content flag:
						0 = background ONLY  (no more data for this sub-block)
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

    compression   "lz4" and "gzip" compressed format; only applies to "rles" format for now.
	scale   A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 
	         resolution of previous level.  Level 0 is the highest resolution.


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


GET <api URL>/node/<UUID>/<data name>/sparsevols-coarse/<start label>/<end label>

	Note: this request does not reflect ongoing merges/splits but is meant to be used
	for various batch operations on a static node.

	Returns a stream of sparse volumes with blocks of the given label in encoded RLE format:

		uint64   label
		<coarse sparse vol as given below>

		uint64   label
		<coarse sparse vol as given below>

		...

	The coarse sparse vol has the following format where integers are little endian and the order
	of data is exactly as specified below:

		int32    # Spans
		Repeating unit of:
			int32   Block coordinate of run start (dimension 0)
			int32   Block coordinate of run start (dimension 1)
			int32   Block coordinate of run start (dimension 2)
			int32   Length of run


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

	Kafka JSON message generated by this request:
		{ 
			"Action": "merge",
			"Target": <to label>,
			"Labels": [<to merge label 1>, <to merge label2>, ...],
			"UUID": <UUID on which merge was done>,
			"MutationID": <unique id for mutation>
		}

	After completion of the split op, the following JSON message is published:
		{ 
			"Action": "merge-complete",
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
		}

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

	Kafka JSON message generated by this request:
		{ 
			"Action": "split",
			"Target": <from label>,
			"NewLabel": <to label>,
			"Split": <string for reference to split data in serialized RLE format>,
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
		}
	
	The split reference above can be used to download the split binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}
	
	After completion of the split op, the following JSON message is published:
		{ 
			"Action": "split-complete",
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
		}


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
	// FormatLegacyRLE is Legacy RLE encoding with header that gives # spans.
	FormatLegacyRLE SparseVolFormat = iota

	// FormatStreamingRLE specifies Streaming RLE encoding
	FormatStreamingRLE

	// FormatBinaryBlocks specifies a streaming set of binary Blocks
	FormatBinaryBlocks
)

// returns default legacy RLE if no options set.
func svformatFromQueryString(r *http.Request) SparseVolFormat {
	switch r.URL.Query().Get("format") {
	case "srles":
		return FormatStreamingRLE
	case "blocks":
		return FormatBinaryBlocks
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
	return helpMessage
}

// -------

// GetByDataUUID returns a pointer to labelarray data given a data UUID.  Returns error if not found.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance '%s' is not a labelarray datatype", source.DataName())
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
		return nil, fmt.Errorf("instance '%s' is not a labelarray datatype", name)
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
		return nil, fmt.Errorf("instance '%s' is not a labelarray datatype", name)
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

	// True if sparse volumes (split, merge, sparse volume optimized GET) are supported
	// for this data instance.  (Default true)
	IndexedLabels bool

	// True if we keep track of # voxels per label.  (Default true)
	CountLabels bool

	// Maximum down-resolution level supported.  Each down-res level is 2x scope of
	// the higher level.
	MaxDownresLevel uint8

	updates  []uint32 // tracks updating to each scale of labelarray [0:MaxDownresLevel+1]
	updateMu sync.RWMutex

	mlMu sync.RWMutex // For atomic access of MaxLabel and MaxRepoLabel

	// unpersisted data: channels for mutations
	mutateCh [numMutateHandlers]chan procMsg // channels into mutate (merge/split) ops.
}

// GetMaxDownresLevel returns the number of down-res levels, where level 0 = high-resolution
// and each subsequent level has one-half the resolution.
func (d *Data) GetMaxDownresLevel() uint8 {
	return d.MaxDownresLevel
}

func (d *Data) StartScaleUpdate(scale uint8) {
	d.updateMu.Lock()
	d.updates[scale]++
	d.updateMu.Unlock()
}

func (d *Data) StopScaleUpdate(scale uint8) {
	d.updateMu.Lock()
	d.updates[scale]--
	if d.updates[scale] < 0 {
		dvid.Criticalf("StopScaleUpdate(%d) called more than StartScaleUpdate.", scale)
	}
	d.updateMu.Unlock()
}

func (d *Data) ScaleUpdating(scale uint8) bool {
	d.updateMu.RLock()
	updating := d.updates[scale] > 0
	d.updateMu.RUnlock()
	return updating
}

func (d *Data) AnyScaleUpdating() bool {
	d.updateMu.RLock()
	for scale := uint8(0); scale < d.MaxDownresLevel; scale++ {
		if d.updates[scale] > 0 {
			d.updateMu.RUnlock()
			return true
		}
	}
	d.updateMu.RUnlock()
	return false
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties. Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-labelarray data %q", src.DataName())
	}

	// TODO -- Handle mutable data that could be potentially altered by filter.
	d.MaxLabel = make(map[dvid.VersionID]uint64, len(d2.MaxLabel))
	for k, v := range d2.MaxLabel {
		d.MaxLabel[k] = v
	}
	d.MaxRepoLabel = d2.MaxRepoLabel

	d.IndexedLabels = d2.IndexedLabels
	d.CountLabels = d2.CountLabels
	d.MaxDownresLevel = d2.MaxDownresLevel

	return d.Data.CopyPropertiesFrom(d2.Data, fs)
}

// NewData returns a pointer to labelarray data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	if _, found := c.Get("BlockSize"); !found {
		c.Set("BlockSize", fmt.Sprintf("%d,%d,%d", DefaultBlockSize, DefaultBlockSize, DefaultBlockSize))
	}
	if _, found := c.Get("Compression"); !found {
		c.Set("Compression", "gzip")
	}
	imgblkData, err := dtype.Type.NewData(uuid, id, name, c)
	if err != nil {
		return nil, err
	}

	data := &Data{
		Data: imgblkData,
	}
	indexedLabels := true
	b, found, err := c.GetBool("IndexedLabels")
	if err != nil {
		return nil, err
	}
	if found {
		indexedLabels = b
	}

	countLabels := true
	b, found, err = c.GetBool("CountLabels")
	if err != nil {
		return nil, err
	}
	if found {
		countLabels = b
	}

	var downresLevels uint8
	levels, found, err := c.GetInt("MaxDownresLevel")
	if err != nil {
		return nil, err
	}
	if found {
		if levels < 0 || levels > 255 {
			return nil, fmt.Errorf("illegal number of down-res levels specified (%d): must be 0 <= n <= 255", levels)
		}
		downresLevels = uint8(levels)
	}
	data.updates = make([]uint32, downresLevels+1)

	data.MaxLabel = make(map[dvid.VersionID]uint64)
	data.IndexedLabels = indexedLabels
	data.CountLabels = countLabels
	data.MaxDownresLevel = downresLevels

	data.Initialize()
	return data, nil
}

type propsJSON struct {
	imageblk.Properties
	MaxLabel        map[dvid.VersionID]uint64
	MaxRepoLabel    uint64
	IndexedLabels   bool
	CountLabels     bool
	MaxDownresLevel uint8
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended propsJSON
	}{
		d.Data.Data,
		propsJSON{
			Properties:      d.Data.Properties,
			MaxLabel:        d.MaxLabel,
			MaxRepoLabel:    d.MaxRepoLabel,
			IndexedLabels:   d.IndexedLabels,
			CountLabels:     d.CountLabels,
			MaxDownresLevel: d.MaxDownresLevel,
		},
	})
}

func (d *Data) MarshalJSONExtents(ctx *datastore.VersionedCtx) ([]byte, error) {
	// grab extent property and load
	extents, err := d.GetExtents(ctx)
	if err != nil {
		return nil, err
	}

	var extentsJSON imageblk.ExtentsJSON
	extentsJSON.MinPoint = extents.MinPoint
	extentsJSON.MaxPoint = extents.MaxPoint

	props, err := d.PropertiesWithExtents(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended propsJSON
		Extents  imageblk.ExtentsJSON
	}{
		d.Data.Data,
		propsJSON{
			Properties:      props,
			MaxLabel:        d.MaxLabel,
			MaxRepoLabel:    d.MaxRepoLabel,
			IndexedLabels:   d.IndexedLabels,
			CountLabels:     d.CountLabels,
			MaxDownresLevel: d.MaxDownresLevel,
		},
		extentsJSON,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.IndexedLabels)); err != nil {
		dvid.Errorf("Decoding labelarray %q: no IndexedLabels, setting to true", d.DataName())
		d.IndexedLabels = true
	}
	if err := dec.Decode(&(d.CountLabels)); err != nil {
		dvid.Errorf("Decoding labelarray %q: no CountLabels, setting to true", d.DataName())
		d.CountLabels = true
	}
	if err := dec.Decode(&(d.MaxDownresLevel)); err != nil {
		dvid.Errorf("Decoding labelarray %q: no MaxDownresLevel, setting to 7", d.DataName())
		d.MaxDownresLevel = 7
	}
	d.updates = make([]uint32, d.MaxDownresLevel+1)
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.IndexedLabels); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.CountLabels); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.MaxDownresLevel); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// makes database call for any update
func (d *Data) updateMaxLabel(v dvid.VersionID, label uint64) error {
	var changed bool
	d.mlMu.RLock()
	curMax, found := d.MaxLabel[v]
	d.mlMu.RUnlock()
	if !found || curMax < label {
		changed = true
	}
	if changed {
		d.mlMu.Lock()
		d.MaxLabel[v] = label
		if err := d.persistMaxLabel(v); err != nil {
			return fmt.Errorf("updateMaxLabel of data %q: %v\n", d.DataName(), err)
		}
		if label > d.MaxRepoLabel {
			d.MaxRepoLabel = label
			if err := d.persistMaxRepoLabel(); err != nil {
				return fmt.Errorf("updateMaxLabel of data %q: %v\n", d.DataName(), err)
			}
		}
		d.mlMu.Unlock()
	}
	return nil
}

// makes database call for any update
func (d *Data) updateBlockMaxLabel(v dvid.VersionID, block *labels.Block) {
	var changed bool
	d.mlMu.RLock()
	curMax, found := d.MaxLabel[v]
	d.mlMu.RUnlock()
	if !found {
		curMax = 0
	}
	for _, label := range block.Labels {
		if label > curMax {
			curMax = label
			changed = true
		}
	}
	if changed {
		d.mlMu.Lock()
		d.MaxLabel[v] = curMax
		if err := d.persistMaxLabel(v); err != nil {
			dvid.Errorf("updateBlockMaxLabel of data %q: %v\n", d.DataName(), err)
		}
		if curMax > d.MaxRepoLabel {
			d.MaxRepoLabel = curMax
			if err := d.persistMaxRepoLabel(); err != nil {
				dvid.Errorf("updateBlockMaxLabel of data %q: %v\n", d.DataName(), err)
			}
		}
		d.mlMu.Unlock()
	}
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return true
}

func (d *Data) persistMaxLabel(v dvid.VersionID) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	if len(d.MaxLabel) == 0 {
		return fmt.Errorf("bad attempt to save non-existant max label for version %d\n", v)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.MaxLabel[v])
	ctx := datastore.NewVersionedCtx(d, v)
	return store.Put(ctx, maxLabelTKey, buf)
}

func (d *Data) persistMaxRepoLabel() error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.MaxRepoLabel)
	ctx := storage.NewDataContext(d, 0)
	return store.Put(ctx, maxRepoLabelTKey, buf)
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
	if err := d.persistMaxLabel(v); err != nil {
		return d.MaxRepoLabel, err
	}
	if err := d.persistMaxRepoLabel(); err != nil {
		return d.MaxRepoLabel, err
	}
	return d.MaxRepoLabel, nil
}

// --- datastore.InstanceMutator interface -----

// LoadMutable loads mutable properties of label volumes like the maximum labels
// for each version.  Note that we load these max labels from key-value pairs
// rather than data instance properties persistence, because in the case of a crash,
// the actually stored repo data structure may be out-of-date compared to the guaranteed
// up-to-date key-value pairs for max labels.
func (d *Data) LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (bool, error) {
	ctx := storage.NewDataContext(d, 0)
	store, err := datastore.GetOrderedKeyValueDB(d)
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

const veryLargeLabel = 10000000000 // 10 billion

func (d *Data) migrateMaxLabels(root dvid.VersionID, wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	store, err := datastore.GetOrderedKeyValueDB(d)
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
		var label uint64 = veryLargeLabel
		if len(kv.V) != 8 {
			dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
		} else {
			label = binary.LittleEndian.Uint64(kv.V)
		}
		d.MaxLabel[v] = label
		if label > repoMax {
			repoMax = label
		}
	}

	// Load in the repo-wide max label.
	store, err := datastore.GetOrderedKeyValueDB(d)
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
		if repoMax == 0 {
			repoMax = veryLargeLabel
		}
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

// sendBlocksSpecific writes data to the blocks specified -- best for non-ordered backend
func (d *Data) sendBlocksSpecific(ctx *datastore.VersionedCtx, w http.ResponseWriter, blockstring string, scale uint8) error {
	w.Header().Set("Content-type", "application/octet-stream")
	// extract querey string
	if blockstring == "" {
		return nil
	}
	coordarray := strings.Split(blockstring, ",")
	if len(coordarray)%3 != 0 {
		return fmt.Errorf("block query string should be three coordinates per block")
	}

	// make a finished queue
	finishedRequests := make(chan error, len(coordarray)/3)
	var mutex sync.Mutex

	// get store
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelblk had error initializing store: %v\n", err)
	}

	// iterate through each block and query
	for i := 0; i < len(coordarray); i += 3 {
		xloc, err := strconv.Atoi(coordarray[i])
		if err != nil {
			return err
		}
		yloc, err := strconv.Atoi(coordarray[i+1])
		if err != nil {
			return err
		}
		zloc, err := strconv.Atoi(coordarray[i+2])
		if err != nil {
			return err
		}

		go func(xloc, yloc, zloc int32, finishedRequests chan error, store storage.KeyValueDB) {
			var err error
			defer func() {
				finishedRequests <- err
			}()
			indexBeg := dvid.IndexZYX(dvid.ChunkPoint3d{xloc, yloc, zloc})
			keyBeg := NewBlockTKey(scale, &indexBeg)

			value, err := store.Get(ctx, keyBeg)
			if err != nil {
				return
			}
			if len(value) > 0 {
				// lock shared resource
				mutex.Lock()
				defer mutex.Unlock()
				d.SendBlockSimple(w, xloc, yloc, zloc, value, "")
			}
		}(int32(xloc), int32(yloc), int32(zloc), finishedRequests, store)
	}

	// wait for everything to finish
	for i := 0; i < len(coordarray); i += 3 {
		errjob := <-finishedRequests
		if errjob != nil {
			err = errjob
		}
	}

	return err
}

// returns nil block if no block is at the given block coordinate
func (d *Data) getLabelBlock(ctx *datastore.VersionedCtx, scale uint8, bcoord dvid.IZYXString) (*labels.PositionedBlock, error) {
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("labelarray getLabelBlock() had error initializing store: %v\n", err)
	}
	tk := NewBlockTKeyByCoord(scale, bcoord)
	val, err := store.Get(ctx, tk)
	if err != nil {
		return nil, fmt.Errorf("Error on GET of labelarray %q label block @ %s\n", d.DataName(), bcoord)
	}
	if val == nil {
		return nil, nil
	}
	data, _, err := dvid.DeserializeData(val, true)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize label block in %q: %v\n", d.DataName(), err)
	}
	var block labels.Block
	if err := block.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &labels.PositionedBlock{block, bcoord}, nil
}

func (d *Data) putLabelBlock(ctx *datastore.VersionedCtx, scale uint8, pb *labels.PositionedBlock) error {
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("labelarray putLabelBlock() had error initializing store: %v\n", err)
	}
	tk := NewBlockTKeyByCoord(scale, pb.BCoord)

	data, err := pb.MarshalBinary()
	if err != nil {
		return err
	}

	val, err := dvid.SerializeData(data, d.Compression(), d.Checksum())
	if err != nil {
		return fmt.Errorf("Unable to serialize block %s in %q: %v\n", pb.BCoord, d.DataName(), err)
	}
	return store.Put(ctx, tk, val)
}

func (d *Data) sendBlock(w http.ResponseWriter, x, y, z int32, v []byte, compression string) error {
	formatIn, checksum := dvid.DecodeSerializationFormat(dvid.SerializationFormat(v[0]))

	var start int
	if checksum == dvid.CRC32 {
		start = 5
	} else {
		start = 1
	}

	var outsize uint32
	var out []byte

	switch formatIn {
	case dvid.LZ4:
		outsize = binary.LittleEndian.Uint32(v[start : start+4])
		out = v[start+4:]
		if len(out) != int(outsize) {
			return fmt.Errorf("block (%d,%d,%d) was corrupted lz4: supposed size %d but had %d bytes", x, y, z, outsize, len(out))
		}
	case dvid.Uncompressed, dvid.Gzip:
		outsize = uint32(len(v[start:]))
		out = v[start:]
	default:
		return fmt.Errorf("labelarray data was stored in unknown compressed format: %s\n", formatIn)
	}

	var formatOut dvid.CompressionFormat
	switch compression {
	case "", "lz4":
		formatOut = dvid.LZ4
	case "blocks":
		formatOut = formatIn
	case "gzip":
		formatOut = dvid.Gzip
	case "uncompressed":
		formatOut = dvid.Uncompressed
	default:
		return fmt.Errorf("unknown compression %q requested for blocks", compression)
	}

	// Need to do uncompression/recompression if we are changing compression
	var err error
	var uncompressed, recompressed []byte
	if formatIn != formatOut || compression == "gzip" {
		switch formatIn {
		case dvid.LZ4:
			uncompressed = make([]byte, outsize)
			if err := lz4.Uncompress(out, uncompressed); err != nil {
				return err
			}
		case dvid.Uncompressed:
			uncompressed = out
		case dvid.Gzip:
			gzipIn := bytes.NewBuffer(out)
			zr, err := gzip.NewReader(gzipIn)
			if err != nil {
				return err
			}
			uncompressed, err = ioutil.ReadAll(zr)
			if err != nil {
				return err
			}
			zr.Close()
		}

		var block labels.Block
		if err = block.UnmarshalBinary(uncompressed); err != nil {
			return fmt.Errorf("unable to deserialize label block (%d, %d, %d): %v\n", x, y, z, err)
		}
		uint64array, size := block.MakeLabelVolume()
		expectedSize := d.BlockSize().(dvid.Point3d)
		if !size.Equals(expectedSize) {
			return fmt.Errorf("deserialized label block size %s does not equal data %q block size %s", size, d.DataName(), expectedSize)
		}

		switch formatOut {
		case dvid.LZ4:
			recompressed = make([]byte, lz4.CompressBound(uint64array))
			var size int
			if size, err = lz4.Compress(uint64array, recompressed); err != nil {
				return err
			}
			outsize = uint32(size)
			out = recompressed[:outsize]
		case dvid.Uncompressed:
			out = uint64array
			outsize = uint32(len(uint64array))
		case dvid.Gzip:
			var gzipOut bytes.Buffer
			zw := gzip.NewWriter(&gzipOut)
			if _, err = zw.Write(uint64array); err != nil {
				return err
			}
			zw.Flush()
			zw.Close()
			out = gzipOut.Bytes()
			outsize = uint32(len(out))
		}
	}

	// Send block coordinate, size of data, then data
	if err := binary.Write(w, binary.LittleEndian, x); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, y); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, z); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, outsize); err != nil {
		return err
	}
	if written, err := w.Write(out); err != nil || written != int(outsize) {
		if err != nil {
			return err
		}
		return fmt.Errorf("could not write %d bytes of block (%d,%d,%d): only %d bytes written", outsize, x, y, z, written)
	}
	return nil
}

// SendBlocks returns a series of blocks covering the given block-aligned subvolume.
func (d *Data) SendBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, scale uint8, subvol *dvid.Subvolume, compression string) error {
	w.Header().Set("Content-type", "application/octet-stream")

	switch compression {
	case "", "lz4", "gzip", "blocks", "uncompressed":
	default:
		return fmt.Errorf(`compression must be "lz4" (default), "gzip", "blocks" or "uncompressed"`)
	}

	// convert x,y,z coordinates to block coordinates for this scale
	blocksdims := subvol.Size().Div(d.BlockSize())
	blocksoff := subvol.StartPoint().Div(d.BlockSize())

	timedLog := dvid.NewTimeLog()
	defer timedLog.Infof("SendBlocks %s, span x %d, span y %d, span z %d", blocksoff, blocksdims.Value(0), blocksdims.Value(1), blocksdims.Value(2))

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelarray had error initializing store: %v\n", err)
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

	for ziter := int32(0); ziter < blocksdims.Value(2); ziter++ {
		for yiter := int32(0); yiter < blocksdims.Value(1); yiter++ {
			beginPoint := dvid.ChunkPoint3d{blocksoff.Value(0), blocksoff.Value(1) + yiter, blocksoff.Value(2) + ziter}
			endPoint := dvid.ChunkPoint3d{blocksoff.Value(0) + blocksdims.Value(0) - 1, blocksoff.Value(1) + yiter, blocksoff.Value(2) + ziter}

			indexBeg := dvid.IndexZYX(beginPoint)
			sx, sy, sz := indexBeg.Unpack()
			begTKey := NewBlockTKey(scale, &indexBeg)
			indexEnd := dvid.IndexZYX(endPoint)
			endTKey := NewBlockTKey(scale, &indexEnd)

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
				_, indexZYX, err := DecodeBlockTKey(kv.K)
				if err != nil {
					return err
				}
				x, y, z := indexZYX.Unpack()
				if z != sz || y != sy || x < sx || x >= sx+int32(blocksdims.Value(0)) {
					return nil
				}
				if err := d.sendBlock(w, x, y, z, kv.V, compression); err != nil {
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

func (d *Data) blockChangesExtents(extents *dvid.Extents, bx, by, bz int32) bool {
	blockSize := d.BlockSize().(dvid.Point3d)
	start := dvid.Point3d{bx * blockSize[0], by * blockSize[1], bz * blockSize[2]}
	end := dvid.Point3d{start[0] + blockSize[0] - 1, start[1] + blockSize[1] - 1, start[2] + blockSize[2] - 1}
	return extents.AdjustPoints(start, end)
}

// ReceiveBlocks stores a slice of bytes corresponding to specified blocks
func (d *Data) ReceiveBlocks(ctx *datastore.VersionedCtx, r io.ReadCloser, scale uint8, downscale bool, compression string) error {
	if downscale && scale != 0 {
		return fmt.Errorf("cannot downscale blocks of scale > 0")
	}

	switch compression {
	case "", "blocks":
	default:
		return fmt.Errorf(`compression must be "blocks" (default) at this time`)
	}

	timedLog := dvid.NewTimeLog()
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelarray had error initializing store: %v\n", err)
	}

	// extract buffer interface if it exists
	var putbuffer storage.RequestBuffer
	if req, ok := store.(storage.KeyValueRequester); ok {
		putbuffer = req.NewBuffer(ctx)
	}

	mutID := d.NewMutationID()
	var downresMut *downres.Mutation
	if downscale {
		downresMut = downres.NewMutation(d, ctx.VersionID(), mutID)
	}

	blockCh := make(chan blockChange, 100)
	go d.aggregateBlockChanges(ctx.VersionID(), blockCh)
	var wg sync.WaitGroup

	callback := func(bcoord dvid.IZYXString, block *labels.Block, ready chan error) {
		if ready != nil {
			if resperr := <-ready; resperr != nil {
				dvid.Errorf("Unable to PUT voxel data for block %v: %v\n", bcoord, resperr)
				return
			}
		}
		event := labels.IngestBlockEvent
		ingestBlock := IngestedBlock{mutID, bcoord, block}
		if scale == 0 {
			d.handleBlockIndexing(ctx.VersionID(), blockCh, ingestBlock)
		}
		if downscale {
			if err := downresMut.BlockMutated(bcoord, block); err != nil {
				dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
			}
		}

		evt := datastore.SyncEvent{d.DataUUID(), event}
		msg := datastore.SyncMessage{event, ctx.VersionID(), ingestBlock}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Errorf("Unable to notify subscribers of event %s in %s\n", event, d.DataName())
		}

		wg.Done()
	}

	if d.Compression().Format() != dvid.Gzip {
		return fmt.Errorf("labelarray %q cannot accept GZIP /blocks POST since it internally uses %s", d.DataName(), d.Compression().Format())
	}
	var extentsChanged bool
	extents, err := d.GetExtents(ctx)
	if err != nil {
		return err
	}
	var numBlocks, pos int
	hdrBytes := make([]byte, 16)
	for {
		n, readErr := io.ReadFull(r, hdrBytes)
		if n != 0 {
			pos += n
			if n != 16 {
				return fmt.Errorf("error reading header bytes at byte %d: %v\n", pos, err)
			}
			bx := int32(binary.LittleEndian.Uint32(hdrBytes[0:4]))
			by := int32(binary.LittleEndian.Uint32(hdrBytes[4:8]))
			bz := int32(binary.LittleEndian.Uint32(hdrBytes[8:12]))
			numBytes := int(binary.LittleEndian.Uint32(hdrBytes[12:16]))
			bcoord := dvid.ChunkPoint3d{bx, by, bz}.ToIZYXString()
			tk := NewBlockTKeyByCoord(scale, bcoord)
			compressed := make([]byte, numBytes)
			n, readErr = io.ReadFull(r, compressed)
			if n != numBytes || (readErr != nil && readErr != io.EOF) {
				return fmt.Errorf("error reading %d bytes for block %s: %d read (%v)\n", numBytes, bcoord, n, readErr)
			}

			if scale == 0 {
				if mod := d.blockChangesExtents(&extents, bx, by, bz); mod {
					extentsChanged = true
				}
			}

			serialization, err := dvid.SerializePrecompressedData(compressed, d.Compression(), d.Checksum())
			if err != nil {
				return fmt.Errorf("can't serialize received block %s data: %v\n", bcoord, err)
			}
			pos += n

			gzipIn := bytes.NewBuffer(compressed)
			zr, err := gzip.NewReader(gzipIn)
			if err != nil {
				return fmt.Errorf("can't initiate gzip reader: %v\n", err)
			}
			uncompressed, err := ioutil.ReadAll(zr)
			if err != nil {
				return fmt.Errorf("can't read all %d bytes from gzipped block %s: %v\n", numBytes, bcoord, err)
			}
			if err := zr.Close(); err != nil {
				return fmt.Errorf("error on closing gzip on block read of data %q: %v\n", d.DataName(), err)
			}

			var block labels.Block
			if err = block.UnmarshalBinary(uncompressed); err != nil {
				return fmt.Errorf("unable to deserialize label block %s: %v\n", bcoord, err)
			}
			if scale == 0 {
				go d.updateBlockMaxLabel(ctx.VersionID(), &block)
			}

			if err != nil {
				return fmt.Errorf("Unable to deserialize %d bytes corresponding to block %s: %v\n", n, bcoord, err)
			}
			wg.Add(1)
			if putbuffer != nil {
				ready := make(chan error, 1)
				go callback(bcoord, &block, ready)
				putbuffer.PutCallback(ctx, tk, serialization, ready)
			} else {
				if err := store.Put(ctx, tk, serialization); err != nil {
					return fmt.Errorf("Unable to PUT voxel data for block %s: %v\n", bcoord, err)
				}
				go callback(bcoord, &block, nil)
			}
			numBlocks++
		}
		if readErr == io.EOF {
			break
		}
	}

	wg.Wait()
	close(blockCh)

	if extentsChanged {
		if err := d.PostExtents(ctx, extents.StartPoint(), extents.EndPoint()); err != nil {
			dvid.Criticalf("could not modify extents for labelarray %q: %v\n", d.DataName(), err)
		}
	}

	// if a bufferable op, flush
	if putbuffer != nil {
		putbuffer.Flush()
	}
	if downscale {
		downresMut.Done()
	}
	timedLog.Infof("Received and stored %d blocks for labelarray %q", numBlocks, d.DataName())
	return nil
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
		if err = lz4.Uncompress(data, uncompressed); err != nil {
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

func getScale(queryStrings url.Values) (scale uint8, err error) {
	scaleStr := queryStrings.Get("scale")
	if scaleStr != "" {
		var scaleInt int
		scaleInt, err = strconv.Atoi(scaleStr)
		if err != nil {
			return
		}
		scale = uint8(scaleInt)
	}
	return
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
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

	// Prevent use of APIs that require IndexedLabels when it is not set.
	if !d.IndexedLabels {
		switch parts[3] {
		case "sparsevol", "sparsevol-by-point", "sparsevol-coarse", "maxlabel", "nextlabel", "split", "split-coarse", "merge":
			server.BadRequest(w, r, "data %q is not label indexed (IndexedLabels=false): %q endpoint is not supported", d.DataName(), parts[3])
			return
		}
	}

	// Handle all requests
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
		jsonBytes, err := d.MarshalJSONExtents(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "specificblocks":
		// GET <api URL>/node/<UUID>/<data name>/specificblocks?blocks=x,y,z,x,y,z...
		blocklist := r.URL.Query().Get("blocks")
		scale, err := getScale(r.URL.Query())
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if action == "get" {
			if err := d.sendBlocksSpecific(ctx, w, blocklist, scale); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog := dvid.NewTimeLog()
			timedLog.Infof("HTTP %s: %s", r.Method, r.URL)
		} else {
			server.BadRequest(w, r, "DVID does not accept the %s action on the 'specificblocks' endpoint", action)
			return
		}

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

	case "labels":
		d.handleLabels(ctx, w, r)

	case "blocks":
		d.handleBlocks(ctx, w, r, parts)

	case "pseudocolor":
		d.handlePseudocolor(ctx, w, r, parts)

	case "raw", "isotropic":
		d.handleDataRequest(ctx, w, r, parts)

	// endpoints after this must have data instance IndexedLabels = true

	case "sparsevol-size":
		d.handleSparsevolSize(ctx, w, r, parts)

	case "sparsevol":
		d.handleSparsevol(ctx, w, r, parts)

	case "sparsevol-by-point":
		d.handleSparsevolByPoint(ctx, w, r, parts)

	case "sparsevol-coarse":
		d.handleSparsevolCoarse(ctx, w, r, parts)

	case "sparsevols-coarse":
		d.handleSparsevolsCoarse(ctx, w, r, parts)

	case "maxlabel":
		d.handleMaxlabel(ctx, w, r)

	case "nextlabel":
		d.handleNextlabel(ctx, w, r)

	case "split":
		d.handleSplit(ctx, w, r, parts)

	case "split-coarse":
		d.handleSplitCoarse(ctx, w, r, parts)

	case "merge":
		d.handleMerge(ctx, w, r, parts)

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
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	coord, err := dvid.StringToPoint(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	label, err := d.GetLabelAtScaledPoint(ctx.VersionID(), coord, scale)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-type", "application/json")
	jsonStr := fmt.Sprintf(`{"Label": %d}`, label)
	fmt.Fprintf(w, jsonStr)

	timedLog.Infof("HTTP GET label at %s (%s)", parts[4], r.URL)
}

func (d *Data) handleLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// POST <api URL>/node/<UUID>/<data name>/labels
	timedLog := dvid.NewTimeLog()

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
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
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
		label, err := d.GetLabelAtScaledPoint(ctx.VersionID(), coord, scale)
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

	timedLog.Infof("HTTP GET batch label-at-point query (%s)", r.URL)
}

func (d *Data) handleBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?compression=...]
	// POST <api URL>/node/<UUID>/<data name>/blocks[?compression=...]
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}

	compression := queryStrings.Get("compression")
	downscale := queryStrings.Get("downres") == "true"
	if strings.ToLower(r.Method) == "get" {
		if len(parts) < 6 {
			server.BadRequest(w, r, "must specifiy size and offset with GET /blocks endpoint")
			return
		}
		sizeStr, offsetStr := parts[4], parts[5]
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

		if err := d.SendBlocks(ctx, w, scale, subvol, compression); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP GET blocks at size %s, offset %s (%s)", parts[4], parts[5], r.URL)
	} else {
		if err := d.ReceiveBlocks(ctx, r.Body, scale, downscale, compression); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP POST blocks (%s)", r.URL)
	}
}

func (d *Data) handlePseudocolor(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 7 {
		server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
		return
	}
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
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
		if strings.ToLower(r.Method) != "get" {
			server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
			return
		}
		lbl, err := d.NewLabels(slice, nil)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		img, err := d.GetImage(ctx.VersionID(), lbl, scale, roiname)
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
	timedLog.Infof("HTTP GET pseudocolor with shape %s, size %s, offset %s", parts[4], parts[5], parts[6])
}

func (d *Data) handleDataRequest(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 7 {
		server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
		return
	}
	timedLog := dvid.NewTimeLog()

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

	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
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
		rawSlice, err := dvid.Isotropy2D(d.Properties.VoxelSize, slice, isotropic)
		lbl, err := d.NewLabels(rawSlice, nil)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		img, err := d.GetImage(ctx.VersionID(), lbl, scale, roiname)
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
			data, err := d.GetVolume(ctx.VersionID(), lbl, scale, roiname)
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
	timedLog.Infof("HTTP %s %s with shape %s, size %s, offset %s, scale %d", r.Method, parts[3], parts[4], parts[5], parts[6], scale)
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

func (d *Data) handleSparsevolSize(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-size/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol-size' command")
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
	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "DVID does not support %s on /sparsevol-size endpoint", r.Method)
		return
	}

	meta, lbls, err := GetMappedLabelIndex(d, ctx.VersionID(), label, 0, dvid.Bounds{})
	if err != nil {
		server.BadRequest(w, r, "problem getting block indexing on labels %: %v", lbls, err)
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")
	fmt.Fprintf(w, "{")
	fmt.Fprintf(w, `"numblocks": %d, `, len(meta.Blocks))
	minBlock, maxBlock, err := meta.Blocks.GetBounds()
	if err != nil {
		server.BadRequest(w, r, "problem getting bounds on blocks of label %d: %v", label, err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		server.BadRequest(w, r, "Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	minx := minBlock[0] * blockSize[0]
	miny := minBlock[1] * blockSize[1]
	minz := minBlock[2] * blockSize[2]
	maxx := (maxBlock[0]+1)*blockSize[0] - 1
	maxy := (maxBlock[1]+1)*blockSize[1] - 1
	maxz := (maxBlock[2]+1)*blockSize[2] - 1
	fmt.Fprintf(w, `"minvoxel": [%d, %d, %d], `, minx, miny, minz)
	fmt.Fprintf(w, `"maxvoxel": [%d, %d, %d]`, maxx, maxy, maxz)
	fmt.Fprintf(w, "}")
}

func (d *Data) handleSparsevol(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// POST <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
		return
	}
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
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

	timedLog := dvid.NewTimeLog()
	switch strings.ToLower(r.Method) {
	case "get":
		w.Header().Set("Content-type", "application/octet-stream")

		var found bool
		switch svformatFromQueryString(r) {
		case FormatLegacyRLE:
			found, err = d.writeLegacyRLE(ctx, label, scale, b, compression, w)
		case FormatBinaryBlocks:
			found, err = d.writeBinaryBlocks(ctx, label, scale, b, compression, w)
		case FormatStreamingRLE:
			found, err = d.writeStreamingRLE(ctx, label, scale, b, compression, w)
		}
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if !found {
			dvid.Infof("GET sparsevol on label %d was not found.\n", label)
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

	timedLog.Infof("HTTP %s: sparsevol on label %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolByPoint(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	coord, err := dvid.StringToPoint(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	label, err := d.GetLabelAtScaledPoint(ctx.VersionID(), coord, 0)
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
	case FormatLegacyRLE:
		found, err = d.writeLegacyRLE(ctx, label, 0, b, compression, w)
	case FormatBinaryBlocks:
		found, err = d.writeBinaryBlocks(ctx, label, 0, b, compression, w)
	case FormatStreamingRLE:
		found, err = d.writeStreamingRLE(ctx, label, 0, b, compression, w)
	}
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
	}
	timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label ID to follow 'sparsevol-coarse' command")
		return
	}
	timedLog := dvid.NewTimeLog()

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
	timedLog.Infof("HTTP %s: sparsevol-coarse on label %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolsCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevols-coarse/<start label>/<end label>
	if len(parts) < 6 {
		server.BadRequest(w, r, "DVID requires start and end label ID to follow 'sparsevols-coarse' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	begLabel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	endLabel, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if begLabel == 0 || endLabel == 0 {
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

	w.Header().Set("Content-type", "application/octet-stream")
	if err := d.WriteSparseCoarseVols(ctx, w, begLabel, endLabel, b); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	timedLog.Infof("HTTP %s: sparsevols-coarse on label %s to %s (%s)", r.Method, parts[4], parts[5], r.URL)
}

func (d *Data) handleMaxlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/maxlabel
	timedLog := dvid.NewTimeLog()
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
	timedLog.Infof("HTTP maxlabel request (%s)", r.URL)
}

func (d *Data) handleNextlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/nextlabel
	// POST <api URL>/node/<UUID>/<data name>/nextlabel
	timedLog := dvid.NewTimeLog()
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
	timedLog.Infof("HTTP maxlabel request (%s)", r.URL)
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
	timedLog := dvid.NewTimeLog()

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
		server.BadRequest(w, r, fmt.Sprintf("split label %d -> %d: %v", fromLabel, splitLabel, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %d}", "label", toLabel)

	timedLog.Infof("HTTP split of label %d request (%s)", fromLabel, r.URL)
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
	timedLog := dvid.NewTimeLog()

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

	timedLog.Infof("HTTP split-coarse of label %d request (%s)", fromLabel, r.URL)
}

func (d *Data) handleMerge(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/merge
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Merge requests must be POST actions.")
		return
	}
	timedLog := dvid.NewTimeLog()

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

	timedLog.Infof("HTTP merge request (%s)", r.URL)
}

// --------- Other functions on labelarray Data -----------------

// GetLabelBlock returns a compressed label Block of the given block coordinate.
func (d *Data) GetLabelBlock(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8) (*labels.Block, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// Retrieve the block of labels
	ctx := datastore.NewVersionedCtx(d, v)
	index := dvid.IndexZYX(bcoord)
	serialization, err := store.Get(ctx, NewBlockTKey(scale, &index))
	if err != nil {
		return nil, fmt.Errorf("error getting '%s' block for index %s", d.DataName(), bcoord)
	}
	if serialization == nil {
		blockSize, ok := d.BlockSize().(dvid.Point3d)
		if !ok {
			return nil, fmt.Errorf("block size for data %q should be 3d, not: %s", d.DataName(), d.BlockSize())
		}
		return labels.MakeSolidBlock(0, blockSize), nil
	}
	deserialization, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize block %s in '%s': %v", bcoord, d.DataName(), err)
	}
	var block labels.Block
	if err = block.UnmarshalBinary(deserialization); err != nil {
		return nil, err
	}
	return &block, nil
}

// GetLabelBytesWithScale returns a block of labels in packed little-endian uint64 format at the given scale.
func (d *Data) GetLabelBytesWithScale(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8) ([]byte, error) {
	block, err := d.GetLabelBlock(v, bcoord, scale)
	if err != nil {
		return nil, err
	}
	labelData, _ := block.MakeLabelVolume()
	return labelData, nil
}

// GetLabelBytesAtScaledPoint returns the 8 byte slice corresponding to a 64-bit label at a point.
func (d *Data) GetLabelBytesAtScaledPoint(v dvid.VersionID, pt dvid.Point, scale uint8) ([]byte, error) {
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("Can't determine block of point %s", pt)
	}
	blockSize := d.BlockSize()
	bcoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	labelData, err := d.GetLabelBytesWithScale(v, bcoord, scale)
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

// GetLabelAtScaledPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtScaledPoint(v dvid.VersionID, pt dvid.Point, scale uint8) (uint64, error) {
	labelBytes, err := d.GetLabelBytesAtScaledPoint(v, pt, scale)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(labelBytes), nil
}

// The following functions implement an interface to synced data types like annotation.

// GetLabelBytes returns a block of hi-res labels (scale 0) in packed little-endian uint64 format
func (d *Data) GetLabelBytes(v dvid.VersionID, bcoord dvid.ChunkPoint3d) ([]byte, error) {
	return d.GetLabelBytesWithScale(v, bcoord, 0)
}

// GetLabelAtPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	return d.GetLabelAtScaledPoint(v, pt, 0)
}
