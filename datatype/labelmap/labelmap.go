/*
	Package labelmap handles both volumes of label data as well as indexing to
	quickly find and generate sparse volumes of any particular label.  It differs
	from labelmap in using in-memory label maps to greatly decrease changes
	to underlying label data key-value pairs, instead handling all merges through
	changes in label maps.
*/
package labelmap

import (
	"bytes"
	"compress/gzip"
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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelmap"
	TypeName = "labelmap"
)

const helpMessage = `
API for label block data type (github.com/janelia-flyem/dvid/datatype/labelmap)
===============================================================================

Note: UUIDs referenced below are strings that may either be a unique prefix of a
hexadecimal UUID string (e.g., 3FA22) or a branch leaf specification that adds
a colon (":") followed by the case-dependent branch name.  In the case of a
branch leaf specification, the unique UUID prefix just identifies the repo of
the branch, and the UUID referenced is really the leaf of the branch name.
For example, if we have a DAG with root A -> B -> C where C is the current
HEAD or leaf of the "master" (default) branch, then asking for "B:master" is
the same as asking for "C".  If we add another version so A -> B -> C -> D, then
references to "B:master" now return the data from "D".

----

Denormalizations like sparse volumes are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labelmap <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new labelmap superpixels VoxelSize=3.2,3.2,40.0

    Arguments:

    UUID            Hexadecimal string with enough characters to uniquely identify a version node.
    data name       Name of data to create, e.g., "superpixels"
    settings        Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    BlockSize       Size in voxels  (default: %s) Should be multiples of 16.
    VoxelSize       Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits      Resolution units (default: "nanometers")
	IndexedLabels   "false" if no sparse volume support is required (default "true")
	MaxDownresLevel  The maximum down-res level supported.  Each down-res is factor of 2.

$ dvid node <UUID> <data name> load <offset> <image glob> <settings...>

    Initializes version node to a set of XY label images described by glob of filenames.
    The DVID server must have access to the named files.  Currently, XY images are required.

    Example: 

    $ dvid node 3f8c superpixels load 0,0,100 "data/*.png" proc=noindex

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of label images, preferably in quotes, e.g., "foo-xy-*.png"

$ dvid node <UUID> <data name> composite <uint8 data name> <new rgba8 data name>

    Creates a RGBA8 image where the RGB is a hash of the labels and the A is the
    grayscale intensity.

    Example: 

    $ dvid node 3f8c bodies composite grayscale bodyview

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
	
$ dvid node <UUID> <data name> dump <dump type> <file path>

	Dumps the internal state of the specified version of labelmap data into a space-delimted file.
	The format of the space-delimted files are as follows depending on the <dump type>:

	"svcount": Supervoxel counts in a space-delimted file where each block in a supervoxel has a row:
		<supervoxel id> <block z> <block y> <block x> <# voxels>
	
	"mappings": Supervoxel to agglomerated label mappings in a space-delimted file where each supervoxel has a row:
		<supervoxel id> <label>
		Unlike the GET /mappings endpoint, the command-line version is consistent by default and will hold lock at
		possible detriment to other users.
	
	"indices": Label indices in a space-delimted file where each supervoxel has a row with its agglomerated label:
		<label> <supervoxel id> <block z> <block y> <block x> <# voxels>
	
	Note that the last two dumps can be used by a program to ingest all but the actual voxel labels,
	using the POST /mappings and POST /indices endpoints.  All three dumps can be used for quality
	control.  Sorting can be done after the dump by the linux "sort" command:
	    % sort -g -k1,1 -k2,2 -k3,3 -k4,4 svcount.csv > sorted-svcount.csv

    Example: 

    $ dvid node 3f8c segmentation dump svcount /path/to/counts.csv

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	dump type     One of "svcount", "mappings", or "indices".
	file path     Absolute path to a writable file that the dvid server has write privileges to.
	
	
    ------------------

HTTP API (Level 2 REST):

 POST /api/repo/{uuid}/instance

	Creates a new instance of the labelmap data type.  Expects configuration data in JSON
	as the body of the POST.  Configuration data is a JSON object with each property
	corresponding to a configuration keyword for the particular data type.  

	JSON name/value pairs:

	REQUIRED "typename"         Must be "labelmap"
	REQUIRED "dataname"         Name of the new instance
	OPTIONAL "versioned"        If "false" or "0", the data is unversioned and acts as if 
	                             all UUIDs within a repo become the root repo UUID.  (True by default.)
    OPTIONAL "BlockSize"        Size in pixels  (default: 64,64,64 and should be multiples of 16)
    OPTIONAL "VoxelSize"        Resolution of voxels (default: 8.0,8.0,8.0)
    OPTIONAL "VoxelUnits"       Resolution units (default: "nanometers")
	OPTIONAL "IndexedLabels"    "false" if no sparse volume support is required (default "true")
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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.

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

    The labelmap data type accepts syncs to labelvol data instances.  It also accepts syncs to
	labelmap instances for multiscale.

    Query-string Options:

    replace    Set to "true" if you want passed syncs to replace and not be appended to current syncs.
			   Default operation is false.

GET <api URL>/node/<UUID>/<data name>/tags
POST <api URL>/node/<UUID>/<data name>/tags?<options>

	GET retrieves JSON of tags for this instance.
	POST appends or replaces tags provided in POST body.  Expects JSON to be POSTed
	with the following format:

	{ "tag1": "anything you want", "tag2": "something else" }

	To delete tags, pass an empty object with query string "replace=true".

	POST Query-string Options:

	replace   Set to "true" if you want passed tags to replace and not be appended to current tags.
				Default operation is false (append).
			   
GET  <api URL>/node/<UUID>/<data name>/metadata

	Retrieves a JSON schema (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/specificblocks[?queryopts]

    Retrieves blocks corresponding to those specified in the query string.  This interface
    is useful if the blocks retrieved are not consecutive or if the backend in non ordered.

    Example: 

    GET <api URL>/node/3f8c/grayscale/specificblocks?blocks=x1,y1,z2,x2,y2,z2,x3,y3,z3
	
	This will fetch blocks at position (x1,y1,z1), (x2,y2,z2), and (x3,y3,z3).
	The returned byte stream has a list of blocks with a leading block 
	coordinate (3 x int32) plus int32 giving the # of bytes in this block, and  then the 
	bytes for the value.  If blocks are unset within the span, they will not appear in the stream,
	so the returned data will be equal to or less than spanX blocks worth of data.  

	The returned data format has the following format where int32 is in little endian and the bytes 
	of block data have been compressed in the desired output format, according to the specification 
	in "compression" query string.

        int32  Block 1 coordinate X (Note that this may not be starting block coordinate if it is unset.)
        int32  Block 1 coordinate Y
        int32  Block 1 coordinate Z
        int32  # bytes for first block (N1)
        byte0  Bytes of block data in compressed format.
        byte1
        ...
        byteN1

        int32  Block 2 coordinate X
        int32  Block 2 coordinate Y
        int32  Block 2 coordinate Z
        int32  # bytes for second block (N2)
        byte0  Bytes of block data in compressed format.
        byte1
        ...
        byteN2

        ...

    If a block is not available, no data will be returned for it.

    Arguments:

	supervoxels   If "true", returns unmapped supervoxels, disregarding any kind of merges.
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.

    Query-string Options:

    blocks		  x,y,z... block string
    scale         A number from 0 up to MaxDownresLevel where each level has 1/2 resolution of
	              previous level.  Level 0 (default) is the highest resolution.
	compression   Allows retrieval of block data in "lz4", "gzip", "blocks" (native DVID
				  label blocks), or "uncompressed" (uint64 labels). Default is "blocks".
  

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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
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
	The 3d binary data response has "Content-type" set to "application/octet-stream" and is a packed
	array of voxel values (little-endian uint64 per voxel) in ZYX order (X iterates most rapidly).

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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
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

	supervoxels   If "true", returns unmapped supervoxels, disregarding any kind of merges.
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

    Ingests block-aligned supervoxel data using the block sizes defined for this data instance.  
	For example, if the BlockSize = 32, offset and size must be multiples of 32.
	The POST body should be a packed array of voxel values (little-endian uint64 per voxel) in ZYX order 
	(X iterates most rapidly).

    Example: 

    POST <api URL>/node/3f8c/segmentation/raw/0_1_2/512_256_128/0_0_32

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

    roi           Name of roi data instance used to mask the requested data.
    compression   Allows retrieval or submission of 3d data in "lz4" and "gzip"
                    compressed format.
	throttle      If "true", makes sure only N compute-intense operation (all API calls that can 
					be throttled) are handled.  If the server can't initiate the API call right away, 
					a 503 (Service Unavailable) status code is returned.

GET  <api URL>/node/<UUID>/<data name>/pseudocolor/<dims>/<size>/<offset>[?queryopts]

    Retrieves label data as pseudocolored 2D PNG color images where each label hashed to a different RGB.

    Example: 

    GET <api URL>/node/3f8c/segmentation/pseudocolor/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
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
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30

    Query-string Options:

	supervoxels   If "true", returns unmapped supervoxel label, disregarding any kind of merges.
    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.

GET <api URL>/node/<UUID>/<data name>/labels[?queryopts]

	Returns JSON for the labels at a list of coordinates.  Expects JSON in GET body:

	[ [x0, y0, z0], [x1, y1, z1], ...]

	Returns for each POSTed coordinate the corresponding label:

	[ 23, 911, ...]
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.

    Query-string Options:

	supervoxels   If "true", returns unmapped supervoxel label, disregarding any kind of merges.
    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
    hash          MD5 hash of request body content in hexidecimal string format.

GET <api URL>/node/<UUID>/<data name>/history/<label>/<from UUID>/<to UUID>

	Returns JSON for the all mutations pertinent to the label in the region of versions.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
	label     	  The label ID as exists in the later version specified by <to UUID>.
	from UUID     The UUID of the earlier version in time range.
	to UUID       The UUID of the later version in time range.


GET <api URL>/node/<UUID>/<data name>/mapping[?queryopts]

	Returns JSON for mapped labels given a list of supervoxels.  Expects JSON in GET body:

	[ supervoxel1, supervoxel2, supervoxel3, ...]

	Returns for each POSTed supervoxel the corresponding mapped label, which may be 0 if the
	supervoxel no longer exists, e.g., has been split:

	[ 23, 0, 911, ...]

	In the example above, supervoxel2 had been split and no longer exists.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.

    Query-string Options:

	nolookup      if "true", dvid won't verify that a supervoxel actually exists by looking up
	                the label indices.  Only use this if supervoxels were known to exist at some time.
    hash          MD5 hash of request body content in hexidecimal string format.

GET <api URL>/node/<UUID>/<data name>/supervoxel-splits

	Returns JSON for all supervoxel splits that have occured up to this version of the
	labelmap instance.  The returned JSON is of format:

		[
			"abc123",
			[[<mutid>, <old>, <remain>, <split>],
			[<mutid>, <old>, <remain>, <split>],
			[<mutid>, <old>, <remain>, <split>]],
			"bcd234",
			[[<mutid>, <old>, <remain>, <split>],
			[<mutid>, <old>, <remain>, <split>],
			[<mutid>, <old>, <remain>, <split>]]
		]
	
	The UUID examples above, "abc123" and "bcd234", would be full UUID strings and are in order
	of proximity to the given UUID.  So the first UUID would be the version of interest, then
	its parent, and so on.
		  
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of label data.


GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?queryopts]

    Gets blocks corresponding to the extents specified by the size and offset.  The
    subvolume request must be block aligned.  This is the most server-efficient way of
    retrieving the labelmap data, where data read from the underlying storage engine is 
	written directly to the HTTP connection possibly after recompression to match the given 
	query-string compression option.  The default labelmap compression 
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

	If a block is not available, no data will be returned for it.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.

    Query-string Options:

	supervoxels   If "true", returns unmapped supervoxels, disregarding any kind of merges.
	scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
    compression   Allows retrieval of block data in "lz4" (default), "gzip", blocks" (native DVID
	              label blocks) or "uncompressed" (uint64 labels).
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be 
	              throttled) are handled.  If the server can't initiate the API call right away, a 503 
                  (Service Unavailable) status code is returned.


POST <api URL>/node/<UUID>/<data name>/blocks[?queryopts]

    Puts properly-sized supervoxel block data.  This is the most server-efficient way of
    storing labelmap data, where data read from the HTTP stream is written directly to the 
	underlying storage.  The default (and currently only supported) compression is gzip on compressed 
	DVID label Block serialization.

	Note that maximum label and extents are automatically handled during these calls.
	If the optional "scale" query is greater than 0, these ingestions will not trigger
	syncing with associated annotations, etc.

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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.

    Query-string Options:

    scale         A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 resolution
	                of previous level.  Level 0 is the highest resolution.
	downres       "false" (default) or "true", specifies whether the given blocks should be
	                down-sampled to lower resolution.  If "true", scale must be "0" or absent.
    compression   Specifies compression format of block data: default and only option currently is
					"blocks" (native DVID label blocks).
	noindexing	  If "true" (default "false"), will not compute label indices from the received voxel data.  
	                Use this in conjunction with POST /index and /affinities endpoint for faster ingestion.
    throttle      If "true", makes sure only N compute-intense operation (all API calls that can be 
	                throttled) are handled.  If the server can't initiate the API call right away, a 503 
                    (Service Unavailable) status code is returned.


GET <api URL>/node/<UUID>/<data name>/maxlabel

	GET returns the maximum label for the version of data in JSON form:

		{ "maxlabel": <label #> }

POST <api URL>/node/<UUID>/<data name>/maxlabel/<max label>

	Sets the maximum label for the version of data specified by the UUID.  This maximum label will be 
	ignored if it is not greater than the current maximum label.  This value is purely informative
	(i.e., not used for establishing new labels on split) and can be used to distinguish new labels
	in remote stores that may collide with local ones.
	
	If Kafka is enabled, a log message will be posted:
	{
		"Action":     "post-maxlabel",
		"Max Label":  label,
		"UUID":       uuid,
		"Timestamp":  time.Now().String(),
	}

	GET <api URL>/node/<UUID>/<data name>/nextlabel

	GET returns what would be a new label for the version of data in JSON form assuming the version
	has not been committed:

		{ "nextlabel": <label #> }
	
POST <api URL>/node/<UUID>/<data name>/nextlabel/<desired # of labels>

	POST allows the client to request some # of labels that will be reserved.
	This is used if the client wants to introduce new labels.

	Response:

		{ "start": <starting label #>, "end": <ending label #> }

	Unlike POST /maxlabel, which can set the maximum label arbitrarily high, this
	endpoint gives incremental new label ids.

	If Kafka is enabled, a log message will be posted:
	{
		"Action":      "post-nextlabel",
		"Start Label": start,
		"End Label":   end,
		"UUID":        uuid,
		"Timestamp":   time.Now().String(),
	}


-------------------------------------------------------------------------------------------------------
--- The following endpoints require the labelmap data instance to have IndexedLabels set to true. ---
-------------------------------------------------------------------------------------------------------

GET <api URL>/node/<UUID>/<data name>/lastmod/<label>

	Returns last modification metadata for a label in JSON:

	{ "mutation id": 2314, "last mod user": "johndoe", "last mod time": "2000-02-01 12:13:14 +0000 UTC", "last mod app": "Neu3" }
	
	Time is returned in RFC3339 string format. Returns a status code 404 (Not Found)
    if label does not exist.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    label     	  A 64-bit integer label id

GET <api URL>/node/<UUID>/<data name>/supervoxels/<label>

	Returns JSON for the supervoxels that have been agglomerated into the given label:

	[ 23, 911, ...]

	Returns a status code 404 (Not Found) if label does not exist.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    label     	  A 64-bit integer label id

GET <api URL>/node/<UUID>/<data name>/size/<label>[?supervoxels=true]

	Returns the size in voxels for the given label (or supervoxel) in JSON:

	{ "voxels": 2314 }
	
	Returns a status code 404 (Not Found) if label does not exist.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    label     	  A 64-bit integer label id

    Query-string Options:

	supervoxels   If "true", interprets the given label as a supervoxel id, not a possibly merged label.

GET <api URL>/node/<UUID>/<data name>/sizes[?supervoxels=true]

	Returns the sizes in voxels for a list of labels (or supervoxels) in JSON.  Expects JSON
	for the list of labels (or supervoxels) in the body of the request:

	[ 1, 2, 3, ... ]

	Returns JSON of the sizes for each of the above labels:

	[ 19381, 308, 586, ... ]
	
	Returns a size of 0 if label does not exist.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.

    Query-string Options:

	supervoxels   If "true", interprets the given labels as a supervoxel ids.
    hash          MD5 hash of request body content in hexidecimal string format.

GET  <api URL>/node/<UUID>/<data name>/sparsevol-size/<label>[?supervoxels=true]

	Returns JSON giving the number of voxels, number of native blocks and the coarse bounding box in DVID
	coordinates (voxel space):

	{ "voxels": 231387, numblocks": 1081, "minvoxel": [0, 11, 23], "maxvoxel": [1723, 1279, 4855]}

	Returns a status code 404 (Not Found) if label does not exist.

	Note that the minvoxel and maxvoxel coordinates are voxel coordinates that are
	accurate to the block, not the voxel.

    Query-string Options:

	supervoxels   If "true", interprets the given label as a supervoxel id, not a possibly merged label.

GET  <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns a sparse volume with voxels of the given label in encoded RLE format.  The returned
	data can be optionally compressed using the "compression" option below.

	Returns a status code 404 (Not Found) if label does not exist.
	
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

    minx    Spans must be equal to or larger than this minimum hi-res (scale 0) x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum hi-res (scale 0) x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum hi-res (scale 0) y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum hi-res (scale 0) y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum hi-res (scale 0) z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum hi-res (scale 0) z voxel coordinate.
    exact   "false" if RLEs can extend a bit outside voxel bounds within border blocks.
             This will give slightly faster responses. 

    compression  "lz4" and "gzip" compressed format; only applies to "rles" format for now.
	scale        A number from 0 up to MaxDownresLevel where each level beyond 0 has 1/2 
                   resolution of previous level.  Level 0 is the highest resolution.
	supervoxels   If "true", interprets the given label as a supervoxel id.


HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>[?supervoxels=true]

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

    Query-string Options:

	supervoxels   If "true", interprets the given label as a supervoxel id, not a possibly merged label.

GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>[?supervoxels=true]

	Returns a sparse volume with voxels that pass through a given voxel.
	The encoding is described in the "sparsevol" request above.
	
    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30

    Query-string Options:

	supervoxels   If "true", returns the sparsevol of the supervoxel designated by the point.

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

	Returns a status code 404 (Not Found) if label does not exist.
	
	GET Query-string Options:

	supervoxels   If "true", interprets the given label as a supervoxel id.

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


POST <api URL>/node/<UUID>/<data name>/merge

	Merges labels (not supervoxels).  Requires JSON in request body using the 
	following format:

	[toLabel1, fromLabel1, fromLabel2, fromLabel3, ...]

	Returns JSON:
	{
		"MutationID": <unique id for mutation>
	}

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

POST <api URL>/node/<UUID>/<data name>/cleave/<label>

	Cleaves a label given supervoxels to be cleaved.  Requires JSON in request body 
	using the following format:

	[supervoxel1, supervoxel2, ...]

	Each element of the JSON array is a supervoxel to be cleaved from the label and is given
	a new unique label that's provided in the returned JSON.  Note that unlike the 
	target label to be cleved, the POSTed data should be supervoxel IDs, not potentially 
	merged labels.
	
	A bad request error (status 400) will be returned if you attempt to cleve on a 
	non-existent body or attempt to cleve all the supervoxels from a label, i.e., you 
	are not allowed to create empty labels from the cleave operation.

	Returns the following JSON:

		{ 
			"CleavedLabel": <new or optionally assigned label of cleaved portion>,
			"MutationID": <unique id for mutation>
		}


	Kafka JSON message generated by this request at its initiation:
		{ 
			"Action": "cleave",
			"OrigLabel": <original label>,  
			"CleavedLabel": <cleaved label>,
			"CleavedSupervoxels": [<supervoxel 1>, <supervoxel 2>, ...],
			"UUID": <UUID on which cleave was done>,
			"MutationID": <unique id for mutation>
		}

	After the cleave is successfully completed, the following JSON message is logged with Kafka:
	{ 
		"Action": "cleave-complete",
		"UUID": <UUID on which cleave was done>,
		"MutationID": <unique id for mutation>
	}


POST <api URL>/node/<UUID>/<data name>/split-supervoxel/<supervoxel>?<options>

	Splits a portion of a supervoxel into two new supervoxels, both of which will still be mapped
	to the original label.  Returns the following JSON:

		{ 
			"SplitSupervoxel": <new label of split portion>,
			"RemainSupervoxel": <new label of remainder>,
			"MutationID": <unique id for mutation>
		}

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

	NOTE: The POSTed split sparse volume should be a subset of the given supervoxel.  Any
	region that falls out of the given supervoxel will be ignored.

	Kafka JSON message generated by this request:
		{ 
			"Action": "split-supervoxel",
			"Supervoxel": <supervoxel label>,
			"SplitSupervoxel": <new label of split portion>,
			"RemainSupervoxel": <new label of remainder> 
			"Split": <string for reference to split data in serialized RLE format>,
			"UUID": <UUID on which split was done>
		}
	
	The split reference above can be used to download the split binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}
	
	After completion of the split op, since it can take some time to process all blocks, 
	the following JSON message is published:
		{ 
			"Action": "split-supervoxel-complete",
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
		}

	
	POST Query-string Options:

	split  	Label id that should be used for split voxels.
	remain  Label id that should be used for remaining (unsplit) voxels.
	downres Defaults to "true" where all lower-res scales will be computed.
	          Use "false" if you plan on supplying lower-res scales via POST /blocks.

POST <api URL>/node/<UUID>/<data name>/split/<label>

	Splits a portion of a label's voxels into a new supervoxel with a new label.  
	Returns the following JSON:

		{ 
			"label": <new label>,
			"MutationID": <unique id for mutation>
		}

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

	NOTE: The POSTed split sparse volume must be a subset of the given label's voxels.  You cannot
	give an arbitrary sparse volume that may span multiple labels.  An error will be returned if
	the POSTed split sparsevol isn't contained within the split label.
	
	Kafka JSON message generated by this request:
		{ 
			"Action": "split",
			"Target": <from label>,
			"NewLabel": <to label>,
			"Split": <string for reference to split data in serialized RLE format>,
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
			"SVSplit": {23918: {"Remain": 481273, "Split": 481274}, 1839109: {"Remain":...}}
		}
	
	The SVSplit field above gives the new remain and split labels for any given split supervoxel.
	The split reference above can be used to download the split binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}
	
	After completion of the split op, the following JSON message is published:
		{ 
			"Action": "split-complete",
			"MutationID": <unique id for mutation>
			"UUID": <UUID on which split was done>
		}


GET  <api URL>/node/<UUID>/<data name>/index/<label>
POST <api URL>/node/<UUID>/<data name>/index/<label>

	Allows direct retrieval or storing of an index (blocks per supervoxel and their voxel count) 
	for a label.  Typically, these indices are computed on-the-fly during ingestion of
	of blocks of label voxels.  If there are cluster systems capable of computing label
	blocks, indices, and affinities directly, though, it's more efficient to simply POST
	them into dvid.

	The returned (GET) or sent (POST) protobuf serialization of a LabelIndex message is defined by:

	message SVCount {
		map<uint64, uint32> counts = 1;
	}

	message LabelIndex {
		map<uint64, SVCount> blocks = 1;  // key is encoded block coord ZYX (packed little-endian 21-bit numbers where MSB is sign flag)
		uint64 label = 2;
		uint64 last_mutid = 3;
		string last_mod_time = 4;  // string is time in RFC 3339 format
		string last_mod_user = 5;
	}

	If the blocks map is empty on a POST, the label index is deleted.
	
GET <api URL>/node/<UUID>/<data name>/indices

	Allows bulk GET of indices (blocks per supervoxel and their voxel count) by
	including in the GET body a JSON array of requested labels (max 50,000 labels):

		[ 1028193, 883177046, ... ]

	The GET returns a protobuf serialization of a LabelIndices message defined by:
	
	message LabelIndices {
		repeated LabelIndex indices = 1;
	}

	where LabelIndex is defined by the protobuf above in the /index documentation.

POST <api URL>/node/<UUID>/<data name>/indices

	Allows bulk storage of indices (blocks per supervoxel and their voxel count) for any
	particular label.  Typically, these indices are computed on-the-fly during ingestion of
	of blocks of label voxels.  If there are cluster systems capable of computing label
	blocks, indices, and affinities directly, though, it's more efficient to simply load
	them into dvid.

	The POST expects a protobuf serialization of a LabelIndices message defined by:
	
	message LabelIndices {
		repeated LabelIndex indices = 1;
	}

	where LabelIndex is defined by the protobuf in the /index endpoint documentation.
	A label index can be deleted as per the POST /index documentation by having an empty
	blocks map.

GET <api URL>/node/<UUID>/<data name>/mappings[?queryopts]

	Streams space-delimited mappings for the given UUID, one mapping per line:

		supervoxel0 mappedTo0
		supervoxel1 mappedTo1
		supervoxel2 mappedTo2
		...
		supervoxelN mappedToN
	
	Note that only non-identity mappings are transmitted.

	Query-string Options:

		format: If format=binary, the data is returned as little-endian binary uint64 pairs,
				in the same order as shown in the CSV format above.
		consistent: default is "false", use "true" if you want a lock to prevent any
				concurrent mutations on the mappings.  For example, under default behavior,
				the mappings endpoint will periodically release the lock to play nice with
				other requesters, which allows a concurrent mutation to cause an inconsistent
				mapping.

POST <api URL>/node/<UUID>/<data name>/mappings

	Allows direct storing of merge maps for a particular UUID.  Typically, merge
	maps are stored as part of POST /merge calls, which actually modify label
	index data.  If there are cluster systems capable of computing label
	blocks, indices, mappings (to represent agglomerations) and affinities directly,
	though, it's more efficient to simply load them into dvid. 

	The POST expects a protobuf serialization of a MergeOps message defined by:

	message MappingOp {
		uint64 mutid = 1;
		uint64 mapped = 2;
		repeated uint64 original = 3;
	}
	
	message MappingOps {
		repeated MappingOp mappings = 1;
	}
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

// GetByDataUUID returns a pointer to labelmap data given a data UUID.  Returns error if not found.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance '%s' is not a labelmap datatype", source.DataName())
	}
	return data, nil
}

// GetByUUIDName returns a pointer to labelmap data given a UUID and data name.
func GetByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance '%s' is not a labelmap datatype", name)
	}
	return data, nil
}

// GetByVersionName returns a pointer to labelmap data given a version and data name.
func GetByVersionName(v dvid.VersionID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByVersionName(v, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance '%s' is not a labelmap datatype", name)
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

// Data of labelmap type is an extended form of imageblk Data
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

	// Maximum down-resolution level supported.  Each down-res level is 2x scope of
	// the higher level.
	MaxDownresLevel uint8

	updates  []uint32 // tracks updating to each scale of labelmap [0:MaxDownresLevel+1]
	updateMu sync.RWMutex

	mlMu sync.RWMutex // For atomic access of MaxLabel and MaxRepoLabel

	voxelMu sync.Mutex // Only allow voxel-level label mutation ops sequentially.
}

// --- LogReadable interface ---

func (d *Data) ReadLogRequired() bool {
	return true
}

// --- LogWritable interface ---

func (d *Data) WriteLogRequired() bool {
	return true
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
		return fmt.Errorf("unable to copy properties from non-labelmap data %q", src.DataName())
	}

	// TODO -- Handle mutable data that could be potentially altered by filter.
	d.MaxLabel = make(map[dvid.VersionID]uint64, len(d2.MaxLabel))
	for k, v := range d2.MaxLabel {
		d.MaxLabel[k] = v
	}
	d.MaxRepoLabel = d2.MaxRepoLabel

	d.IndexedLabels = d2.IndexedLabels
	d.MaxDownresLevel = d2.MaxDownresLevel

	return d.Data.CopyPropertiesFrom(d2.Data, fs)
}

// NewData returns a pointer to labelmap data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	blockSizeStr, found, err := c.GetString("BlockSize")
	if err != nil {
		return nil, err
	}
	if found {
		var nx, ny, nz int
		fmt.Sscanf(blockSizeStr, "%d,%d,%d", &nx, &ny, &nz)
		if nx%16 != 0 || ny%16 != 0 || nz%16 != 0 {
			return nil, fmt.Errorf("BlockSize must be multiples of 16")
		}
	} else {
		c.Set("BlockSize", fmt.Sprintf("%d,%d,%d", DefaultBlockSize, DefaultBlockSize, DefaultBlockSize))
	}
	if _, found := c.Get("Compression"); !found {
		c.Set("Compression", "gzip")
	}
	// Note that labelmap essentially piggybacks off the associated imageblk Data.
	imgblkData, err := dtype.Type.NewData(uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	if imgblkData.GetWriteLog() == nil {
		return nil, fmt.Errorf("labelmap %q instance needs a writable log yet has none assigned", name)
	}
	if imgblkData.GetReadLog() == nil {
		return nil, fmt.Errorf("labelmap %q instance needs a readable log yet has none assigned", name)
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
	data.MaxDownresLevel = downresLevels

	data.Initialize()
	return data, nil
}

type propsJSON struct {
	imageblk.Properties
	MaxLabel        map[dvid.VersionID]uint64
	MaxRepoLabel    uint64
	IndexedLabels   bool
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
		dvid.Errorf("Decoding labelmap %q: no IndexedLabels, setting to true", d.DataName())
		d.IndexedLabels = true
	}
	if err := dec.Decode(&(d.MaxDownresLevel)); err != nil {
		dvid.Errorf("Decoding labelmap %q: no MaxDownresLevel, setting to 7", d.DataName())
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
	if err := enc.Encode(d.MaxDownresLevel); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// makes database call for any update
func (d *Data) updateMaxLabel(v dvid.VersionID, label uint64) (changed bool, err error) {
	d.mlMu.RLock()
	curMax, found := d.MaxLabel[v]
	d.mlMu.RUnlock()
	if !found || curMax < label {
		changed = true
	}
	if changed {
		d.mlMu.Lock()
		d.MaxLabel[v] = label
		if err = d.persistMaxLabel(v); err != nil {
			err = fmt.Errorf("updateMaxLabel of data %q: %v", d.DataName(), err)
			return
		}
		if label > d.MaxRepoLabel {
			d.MaxRepoLabel = label
			if err = d.persistMaxRepoLabel(); err != nil {
				err = fmt.Errorf("updateMaxLabel of data %q: %v", d.DataName(), err)
				return
			}
		}
		d.mlMu.Unlock()
	}
	return
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
		return fmt.Errorf("bad attempt to save non-existent max label for version %d", v)
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

// newLabel returns a new label for the given version.
func (d *Data) newLabel(v dvid.VersionID) (uint64, error) {
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

// newLabels returns a span of new labels for the given version
func (d *Data) newLabels(v dvid.VersionID, numLabels uint64) (begin, end uint64, err error) {
	if numLabels <= 0 {
		err = fmt.Errorf("cannot request %d new labels, must be 1 or more", numLabels)
	}
	d.mlMu.Lock()
	defer d.mlMu.Unlock()

	// Make sure we aren't trying to add labels on a locked node.
	var locked bool
	if locked, err = datastore.LockedVersion(v); err != nil {
		return
	}
	if locked {
		err = fmt.Errorf("can't ask for new labels in a locked version id %d", v)
		return
	}

	// Increment and store.
	begin = d.MaxRepoLabel + 1
	end = d.MaxRepoLabel + numLabels
	d.MaxRepoLabel = end
	d.MaxLabel[v] = d.MaxRepoLabel
	if err = d.persistMaxLabel(v); err != nil {
		return
	}
	if err = d.persistMaxRepoLabel(); err != nil {
		return
	}
	return
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
		return false, fmt.Errorf("Data type labelmap had error initializing store: %v", err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	ch := make(chan *storage.KeyValue)

	// Start appropriate migration function if any.
	var saveRequired bool
	go d.loadMaxLabels(wg, ch)

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

	dvid.Infof("Loaded max label values for labelmap %q with repo-wide max %d\n", d.DataName(), d.MaxRepoLabel)
	return saveRequired, nil
}

const veryLargeLabel = 10000000000 // 10 billion

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
		dvid.Errorf("Data type labelmap had error initializing store: %v\n", err)
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

// NewLabels returns labelmap Labels, a representation of externally usable subvolume
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

type blockSend struct {
	bcoord dvid.ChunkPoint3d
	value  []byte
	err    error
}

func writeBlock(w http.ResponseWriter, bcoord dvid.ChunkPoint3d, out []byte) error {
	if err := binary.Write(w, binary.LittleEndian, bcoord[0]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bcoord[1]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bcoord[2]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(out))); err != nil {
		return err
	}
	if written, err := w.Write(out); err != nil || written != int(len(out)) {
		if err != nil {
			dvid.Errorf("error writing value: %v\n", err)
			return err
		}
		return fmt.Errorf("could not write %d bytes of block %s: only %d bytes written", len(out), bcoord, written)
	}
	return nil
}

// sendBlocksSpecific writes data to the blocks specified -- best for non-ordered backend
func (d *Data) sendBlocksSpecific(ctx *datastore.VersionedCtx, w http.ResponseWriter, supervoxels bool, compression, blockstring string, scale uint8) (numBlocks int, err error) {
	timedLog := dvid.NewTimeLog()
	switch compression {
	case "":
		compression = "blocks"
	case "lz4", "gzip", "blocks", "uncompressed":
		break
	default:
		err = fmt.Errorf(`compression must be "lz4" (default), "gzip", "blocks" or "uncompressed"`)
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")
	// extract querey string
	if blockstring == "" {
		return
	}
	coordarray := strings.Split(blockstring, ",")
	if len(coordarray)%3 != 0 {
		return 0, fmt.Errorf("block query string should be three coordinates per block")
	}

	var store storage.KeyValueDB
	store, err = datastore.GetKeyValueDB(d)
	if err != nil {
		return
	}

	// launch goroutine that will stream blocks to client
	numBlocks = len(coordarray) / 3
	wg := new(sync.WaitGroup)

	ch := make(chan blockSend, numBlocks)
	var sendErr error
	var startBlock dvid.ChunkPoint3d
	var readT, transcodeT, writeT time.Duration
	var readNum, transcodeNum int64
	go func() {
		for data := range ch {
			if data.err != nil && sendErr == nil {
				sendErr = data.err
			} else if len(data.value) > 0 {
				t0 := time.Now()
				err := writeBlock(w, data.bcoord, data.value)
				if err != nil && sendErr == nil {
					sendErr = err
				}
				writeT += time.Now().Sub(t0)
			}
			wg.Done()
		}
		timedLog.Infof("labelmap %q specificblocks - finished sending %d blocks starting with %s", d.DataName(), numBlocks, startBlock)
		if transcodeNum == 0 {
			transcodeNum = 1
		}
		if readNum == 0 {
			readNum = 1
		}
		dvid.Infof("labelmap %q specificblocks - %d blocks starting with %s: read %s (%s), transcode %s (%s), write %s (%s)\n", d.DataName(), numBlocks, startBlock,
			readT, readT/time.Duration(readNum), transcodeT, transcodeT/time.Duration(transcodeNum), writeT, writeT/time.Duration(readNum))
	}()

	var timeMutex sync.Mutex
	// iterate through each block, get data from store, and transcode based on request parameters
	for i := 0; i < len(coordarray); i += 3 {
		var xloc, yloc, zloc int
		xloc, err = strconv.Atoi(coordarray[i])
		if err != nil {
			return
		}
		yloc, err = strconv.Atoi(coordarray[i+1])
		if err != nil {
			return
		}
		zloc, err = strconv.Atoi(coordarray[i+2])
		if err != nil {
			return
		}
		bcoord := dvid.ChunkPoint3d{int32(xloc), int32(yloc), int32(zloc)}
		if i == 0 {
			startBlock = bcoord
		}
		wg.Add(1)
		go func(bcoord dvid.ChunkPoint3d) {
			t0 := time.Now()
			indexBeg := dvid.IndexZYX(bcoord)
			keyBeg := NewBlockTKey(scale, &indexBeg)

			value, err := store.Get(ctx, keyBeg)
			timeMutex.Lock()
			readNum++
			readT += time.Now().Sub(t0)
			timeMutex.Unlock()
			if err != nil {
				ch <- blockSend{err: err}
				return
			}
			if len(value) > 0 {
				b := blockData{
					bcoord:      bcoord,
					v:           ctx.VersionID(),
					data:        value,
					compression: compression,
					supervoxels: supervoxels,
				}
				t0 := time.Now()
				out, err := d.transcodeBlock(b)
				timeMutex.Lock()
				transcodeNum++
				transcodeT += time.Now().Sub(t0)
				timeMutex.Unlock()
				ch <- blockSend{bcoord: bcoord, value: out, err: err}
				return
			}
			ch <- blockSend{value: nil}
		}(bcoord)
	}
	timedLog.Infof("labelmap %q specificblocks - launched concurrent reads of %d blocks starting with %s", d.DataName(), numBlocks, startBlock)
	wg.Wait()
	close(ch)
	return numBlocks, sendErr
}

// returns nil block if no block is at the given block coordinate
func (d *Data) getLabelBlock(ctx *datastore.VersionedCtx, scale uint8, bcoord dvid.IZYXString) (*labels.PositionedBlock, error) {
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("labelmap getLabelBlock() had error initializing store: %v", err)
	}
	tk := NewBlockTKeyByCoord(scale, bcoord)
	val, err := store.Get(ctx, tk)
	if err != nil {
		return nil, fmt.Errorf("Error on GET of labelmap %q label block @ %s", d.DataName(), bcoord)
	}
	if val == nil {
		return nil, nil
	}
	data, _, err := dvid.DeserializeData(val, true)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize label block in %q: %v", d.DataName(), err)
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
		return fmt.Errorf("labelmap putLabelBlock() had error initializing store: %v", err)
	}
	tk := NewBlockTKeyByCoord(scale, pb.BCoord)

	data, err := pb.MarshalBinary()
	if err != nil {
		return err
	}

	val, err := dvid.SerializeData(data, d.Compression(), d.Checksum())
	if err != nil {
		return fmt.Errorf("Unable to serialize block %s in %q: %v", pb.BCoord, d.DataName(), err)
	}
	return store.Put(ctx, tk, val)
}

type blockData struct {
	bcoord      dvid.ChunkPoint3d
	compression string
	supervoxels bool
	v           dvid.VersionID
	data        []byte
}

// transcodes a block of data by doing any data modifications necessary to meet requested
// compression compared to stored compression as well as raw supervoxels versus mapped labels.
func (d *Data) transcodeBlock(b blockData) (out []byte, err error) {
	formatIn, checksum := dvid.DecodeSerializationFormat(dvid.SerializationFormat(b.data[0]))

	var start int
	if checksum == dvid.CRC32 {
		start = 5
	} else {
		start = 1
	}

	var outsize uint32

	switch formatIn {
	case dvid.LZ4:
		outsize = binary.LittleEndian.Uint32(b.data[start : start+4])
		out = b.data[start+4:]
		if len(out) != int(outsize) {
			err = fmt.Errorf("block %s was corrupted lz4: supposed size %d but had %d bytes", b.bcoord, outsize, len(out))
			return
		}
	case dvid.Uncompressed, dvid.Gzip:
		outsize = uint32(len(b.data[start:]))
		out = b.data[start:]
	default:
		err = fmt.Errorf("labelmap data was stored in unknown compressed format: %s", formatIn)
		return
	}

	var formatOut dvid.CompressionFormat
	switch b.compression {
	case "", "lz4":
		formatOut = dvid.LZ4
	case "blocks":
		formatOut = formatIn
	case "gzip":
		formatOut = dvid.Gzip
	case "uncompressed":
		formatOut = dvid.Uncompressed
	default:
		err = fmt.Errorf("unknown compression %q requested for blocks", b.compression)
		return
	}

	var doMapping bool
	var mapping *SVMap
	if !b.supervoxels {
		if mapping, err = getMapping(d, b.v); err != nil {
			return
		}
		if mapping != nil && mapping.exists(b.v) {
			doMapping = true
		}
	}

	// Need to do uncompression/recompression if we are changing compression or mapping
	var uncompressed, recompressed []byte
	if formatIn != formatOut || b.compression == "gzip" || doMapping {
		switch formatIn {
		case dvid.LZ4:
			uncompressed = make([]byte, outsize)
			if err = lz4.Uncompress(out, uncompressed); err != nil {
				return
			}
		case dvid.Uncompressed:
			uncompressed = out
		case dvid.Gzip:
			gzipIn := bytes.NewBuffer(out)
			var zr *gzip.Reader
			zr, err = gzip.NewReader(gzipIn)
			if err != nil {
				return
			}
			uncompressed, err = ioutil.ReadAll(zr)
			if err != nil {
				return
			}
			zr.Close()
		}

		var block labels.Block
		if err = block.UnmarshalBinary(uncompressed); err != nil {
			err = fmt.Errorf("unable to deserialize label block %s: %v", b.bcoord, err)
			return
		}

		if doMapping {
			modifyBlockMapping(b.v, &block, mapping)
		}

		if b.compression == "blocks" { // send native DVID block compression with gzip
			out, err = block.CompressGZIP()
			if err != nil {
				return nil, err
			}
			outsize = uint32(len(out))
		} else { // we are sending raw block data
			uint64array, size := block.MakeLabelVolume()
			expectedSize := d.BlockSize().(dvid.Point3d)
			if !size.Equals(expectedSize) {
				err = fmt.Errorf("deserialized label block size %s does not equal data %q block size %s", size, d.DataName(), expectedSize)
				return
			}

			switch formatOut {
			case dvid.LZ4:
				recompressed = make([]byte, lz4.CompressBound(uint64array))
				var size int
				if size, err = lz4.Compress(uint64array, recompressed); err != nil {
					return nil, err
				}
				outsize = uint32(size)
				out = recompressed[:outsize]
			case dvid.Uncompressed:
				out = uint64array
			case dvid.Gzip:
				var gzipOut bytes.Buffer
				zw := gzip.NewWriter(&gzipOut)
				if _, err = zw.Write(uint64array); err != nil {
					return nil, err
				}
				zw.Flush()
				zw.Close()
				out = gzipOut.Bytes()
			}
		}
	}
	return
}

// sends a block of data through http.ResponseWriter while handling any data modifications
// necessary to meet requested compression compared to stored compression as well as raw
// supervoxels versus mapped labels.
func (d *Data) sendBlock(w http.ResponseWriter, b blockData) error {
	out, err := d.transcodeBlock(b)
	if err != nil {
		return err
	}
	return writeBlock(w, b.bcoord, out)
}

// SendBlocks returns a series of blocks covering the given block-aligned subvolume.
func (d *Data) SendBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, supervoxels bool, scale uint8, subvol *dvid.Subvolume, compression string) error {
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

	numBlocks := int(blocksdims.Prod())
	wg := new(sync.WaitGroup)

	// launch goroutine that will stream blocks to client
	ch := make(chan blockSend, numBlocks)
	var sendErr error
	go func() {
		for data := range ch {
			if data.err != nil && sendErr == nil {
				sendErr = data.err
			} else {
				err := writeBlock(w, data.bcoord, data.value)
				if err != nil && sendErr == nil {
					sendErr = err
				}
			}
			wg.Done()
		}
	}()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelmap had error initializing store: %v", err)
	}

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
				b := blockData{
					bcoord:      dvid.ChunkPoint3d{x, y, z},
					compression: compression,
					supervoxels: supervoxels,
					v:           ctx.VersionID(),
					data:        kv.V,
				}
				wg.Add(1)
				go func(b blockData) {
					out, err := d.transcodeBlock(b)
					ch <- blockSend{bcoord: b.bcoord, value: out, err: err}
				}(b)
				return nil
			})

			if err != nil {
				return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
			}
		}
	}

	wg.Wait()
	close(ch)

	if hasbuffer {
		// submit the entire buffer to the DB
		err = okv.(storage.RequestBuffer).Flush()
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %v", ctx, err)
		}
	}

	return sendErr
}

func (d *Data) blockChangesExtents(extents *dvid.Extents, bx, by, bz int32) bool {
	blockSize := d.BlockSize().(dvid.Point3d)
	start := dvid.Point3d{bx * blockSize[0], by * blockSize[1], bz * blockSize[2]}
	end := dvid.Point3d{start[0] + blockSize[0] - 1, start[1] + blockSize[1] - 1, start[2] + blockSize[2] - 1}
	return extents.AdjustPoints(start, end)
}

// ReceiveBlocks stores a slice of bytes corresponding to specified blocks
func (d *Data) ReceiveBlocks(ctx *datastore.VersionedCtx, r io.ReadCloser, scale uint8, downscale bool, compression string, indexing bool) error {
	if r == nil {
		return fmt.Errorf("no data blocks POSTed")
	}

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
		return fmt.Errorf("Data type labelmap had error initializing store: %v", err)
	}

	// Only do voxel-based mutations one at a time.  This lets us remove handling for block-level concurrency.
	d.voxelMu.Lock()
	defer d.voxelMu.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

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

	svmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return fmt.Errorf("ReceiveBlocks couldn't get mapping for data %q, version %d: %v", d.DataName(), ctx.VersionID(), err)
	}
	var blockCh chan blockChange
	var putWG, processWG sync.WaitGroup
	if indexing {
		blockCh = make(chan blockChange, 100)
		processWG.Add(1)
		go func() {
			d.aggregateBlockChanges(ctx.VersionID(), svmap, blockCh)
			processWG.Done()
		}()
	}

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
			if indexing {
				d.handleBlockIndexing(ctx.VersionID(), blockCh, ingestBlock)
			}
			go d.updateBlockMaxLabel(ctx.VersionID(), ingestBlock.Data)
			evt := datastore.SyncEvent{d.DataUUID(), event}
			msg := datastore.SyncMessage{event, ctx.VersionID(), ingestBlock}
			if err := datastore.NotifySubscribers(evt, msg); err != nil {
				dvid.Errorf("Unable to notify subscribers of event %s in %s\n", event, d.DataName())
			}
			if downscale {
				if err := downresMut.BlockMutated(bcoord, block); err != nil {
					dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
				}
			}
		}

		putWG.Done()
	}

	if d.Compression().Format() != dvid.Gzip {
		return fmt.Errorf("labelmap %q cannot accept GZIP /blocks POST since it internally uses %s", d.DataName(), d.Compression().Format())
	}
	var extentsChanged bool
	extents, err := d.GetExtents(ctx)
	if err != nil {
		return err
	}
	var numBlocks int
	for {
		block, compressed, bx, by, bz, err := readStreamedBlock(r, scale)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bcoord := dvid.ChunkPoint3d{bx, by, bz}.ToIZYXString()
		tk := NewBlockTKeyByCoord(scale, bcoord)
		if scale == 0 {
			if mod := d.blockChangesExtents(&extents, bx, by, bz); mod {
				extentsChanged = true
			}
			go d.updateBlockMaxLabel(ctx.VersionID(), block)
		}
		serialization, err := dvid.SerializePrecompressedData(compressed, d.Compression(), d.Checksum())
		if err != nil {
			return fmt.Errorf("can't serialize received block %s data: %v", bcoord, err)
		}
		putWG.Add(1)
		if putbuffer != nil {
			ready := make(chan error, 1)
			go callback(bcoord, block, ready)
			putbuffer.PutCallback(ctx, tk, serialization, ready)
		} else {
			if err := store.Put(ctx, tk, serialization); err != nil {
				return fmt.Errorf("Unable to PUT voxel data for block %s: %v", bcoord, err)
			}
			go callback(bcoord, block, nil)
		}
		numBlocks++
	}

	putWG.Wait()
	if blockCh != nil {
		close(blockCh)
	}
	processWG.Wait()

	if extentsChanged {
		if err := d.PostExtents(ctx, extents.StartPoint(), extents.EndPoint()); err != nil {
			dvid.Criticalf("could not modify extents for labelmap %q: %v\n", d.DataName(), err)
		}
	}

	// if a bufferable op, flush
	if putbuffer != nil {
		putbuffer.Flush()
	}
	if downscale {
		if err := downresMut.Execute(); err != nil {
			return err
		}
	}
	timedLog.Infof("Received and stored %d blocks for labelmap %q", numBlocks, d.DataName())
	return nil
}

// --- datastore.DataService interface ---------

// PushData pushes labelmap data to a remote DVID.
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
		go func() {
			if err = d.LoadImages(versionID, offset, filenames); err != nil {
				dvid.Errorf("Cannot load images into data instance %q @ node %s: %v\n", dataName, uuidStr, err)
			}
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				dvid.Errorf("Could not store metadata changes into data instance %q @ node %s: %v\n", dataName, uuidStr, err)
			}
		}()
		reply.Text = fmt.Sprintf("Asynchronously loading %d files into data instance %q @ node %s (errors will be printed in server log) ...\n", len(filenames), dataName, uuidStr)
		return nil

	case "composite":
		if len(req.Command) < 6 {
			return fmt.Errorf("poorly formatted composite command.  See command-line help")
		}
		return d.createComposite(req, reply)

	case "dump":
		if len(req.Command) < 6 {
			return fmt.Errorf("poorly formatted dump command.  See command-line help")
		}
		// Parse the request
		var uuidStr, dataName, cmdStr, dumpType, outPath string
		req.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &dumpType, &outPath)

		uuid, v, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}

		// Setup output file
		f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			return err
		}
		switch dumpType {
		case "svcount":
			go d.writeSVCounts(f, outPath, v)
			reply.Text = fmt.Sprintf("Asynchronously writing supervoxel counts for data %q, uuid %s to file: %s\n", d.DataName(), uuid, outPath)
		case "mappings":
			go d.writeFileMappings(f, outPath, v)
			reply.Text = fmt.Sprintf("Asynchronously writing mappings for data %q, uuid %s to file: %s\n", d.DataName(), uuid, outPath)
		case "indices":
			go d.writeIndices(f, outPath, v)
			reply.Text = fmt.Sprintf("Asynchronously writing label indices for data %q, uuid %s to file: %s\n", d.DataName(), uuid, outPath)
		default:
		}
		return nil

	default:
		return fmt.Errorf("unknown command.  Data type '%s' [%s] does not support '%s' command",
			d.DataName(), d.TypeName(), req.TypeCommand())
	}
}

func colorImage(labels *dvid.Image) (image.Image, error) {
	if labels == nil || labels.Which != 3 || labels.NRGBA64 == nil {
		return nil, fmt.Errorf("writePseudoColor can't use labels image with wrong format: %v", labels)
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
	return datastore.SaveDataByUUID(uuid, d)
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
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
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
		case "sparsevol", "sparsevol-by-point", "sparsevol-coarse", "maxlabel", "nextlabel", "split-supervoxel", "cleave", "merge":
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

	case "tags":
		if action == "post" {
			replace := r.URL.Query().Get("replace") == "true"
			if err := datastore.SetTagsByJSON(d, uuid, replace, r.Body); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		} else {
			jsonBytes, err := d.MarshalJSONTags()
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
		}

	case "specificblocks":
		// GET <api URL>/node/<UUID>/<data name>/specificblocks?blocks=x,y,z,x,y,z...
		queryStrings := r.URL.Query()
		blocklist := queryStrings.Get("blocks")
		supervoxels := queryStrings.Get("supervoxels") == "true"
		compression := queryStrings.Get("compression")
		scale, err := getScale(queryStrings)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if action == "get" {
			timedLog := dvid.NewTimeLog()
			numBlocks, err := d.sendBlocksSpecific(ctx, w, supervoxels, compression, blocklist, scale)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: %s", r.Method, r.URL)
			activity = map[string]interface{}{
				"num_blocks": numBlocks,
			}
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

	case "mapping":
		d.handleMapping(ctx, w, r)

	case "supervoxel-splits":
		d.handleSupervoxelSplits(ctx, w, r)

	case "blocks":
		d.handleBlocks(ctx, w, r, parts)

	case "pseudocolor":
		d.handlePseudocolor(ctx, w, r, parts)

	case "raw", "isotropic":
		d.handleDataRequest(ctx, w, r, parts)

	// endpoints after this must have data instance IndexedLabels = true

	case "lastmod":
		d.handleLabelmod(ctx, w, r, parts)

	case "supervoxels":
		d.handleSupervoxels(ctx, w, r, parts)

	case "size":
		d.handleSize(ctx, w, r, parts)

	case "sizes":
		d.handleSizes(ctx, w, r)

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
		d.handleMaxlabel(ctx, w, r, parts)

	case "nextlabel":
		d.handleNextlabel(ctx, w, r, parts)

	case "split-supervoxel":
		d.handleSplitSupervoxel(ctx, w, r, parts)

	case "cleave":
		d.handleCleave(ctx, w, r, parts)

	case "split":
		d.handleSplit(ctx, w, r, parts)

	case "merge":
		d.handleMerge(ctx, w, r, parts)

	case "index":
		d.handleIndex(ctx, w, r, parts)

	case "indices":
		d.handleIndices(ctx, w, r)

	case "mappings":
		d.handleMappings(ctx, w, r)

	case "history":
		d.handleHistory(ctx, w, r, parts)

	default:
		server.BadAPIRequest(w, r, d)
	}
	return
}

// --------- Handler functions for HTTP requests --------------

func (d *Data) handleLabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/label/<coord>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires coord to follow 'label' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	coord, err := dvid.StringToPoint3d(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	labels, err := d.GetLabelPoints(ctx.VersionID(), []dvid.Point3d{coord}, scale, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-type", "application/json")
	jsonStr := fmt.Sprintf(`{"Label": %d}`, labels[0])
	fmt.Fprintf(w, jsonStr)

	timedLog.Infof("HTTP GET label at %s (%s)", parts[4], r.URL)
}

func (d *Data) handleLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/labels
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
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
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
	labels, err := d.GetLabelPoints(ctx.VersionID(), coords, scale, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, label := range labels {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", label)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch label-at-point query of %d points (%s)", len(coords), r.URL)
}

func (d *Data) handleMapping(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/mapping
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch mapping query must be a GET request")
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
	var supervoxels []uint64
	if err := json.Unmarshal(data, &supervoxels); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad mapping request JSON: %v", err))
		return
	}
	svmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, "couldn't get mapping for data %q, version %d: %v", d.DataName(), ctx.VersionID(), err)
		return
	}
	labels, found, err := svmap.MappedLabels(ctx.VersionID(), supervoxels)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if queryStrings.Get("nolookup") != "true" {
		labels, err = d.verifyMappings(ctx, supervoxels, labels, found)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, label := range labels {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", label)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch mapping query of %d labels (%s)", len(labels), r.URL)
}

func (d *Data) handleSupervoxelSplits(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/supervoxel-splits
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "The /supervoxel-splits endpoint is GET only")
		return
	}
	svmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, "couldn't get mapping for data %q, version %d: %v", d.DataName(), ctx.VersionID(), err)
		return
	}
	splitsJSON, err := svmap.SupervoxelSplitsJSON(ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, splitsJSON)

	timedLog.Infof("HTTP GET supervoxel splits query (%s)", r.URL)
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
	supervoxels := queryStrings.Get("supervoxels") == "true"

	if strings.ToLower(r.Method) == "get" {
		if len(parts) < 6 {
			server.BadRequest(w, r, "must specify size and offset with GET /blocks endpoint")
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
			server.BadRequest(w, r, "cannot use labels via 'blocks' endpoint in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
			return
		}

		if err := d.SendBlocks(ctx, w, supervoxels, scale, subvol, compression); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP GET blocks at size %s, offset %s (%s)", parts[4], parts[5], r.URL)
	} else {
		var indexing bool
		if queryStrings.Get("noindexing") != "true" {
			indexing = true
		}
		if err := d.ReceiveBlocks(ctx, r.Body, scale, downscale, compression, indexing); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP POST blocks, indexing = %t, downscale = %t (%s)", indexing, downscale, r.URL)
	}
}

func (d *Data) handleIndex(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET  <api URL>/node/<UUID>/<data name>/index/<label>
	// POST <api URL>/node/<UUID>/<data name>/index/<label>
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "there is no index for protected label 0")
		return
	}

	switch strings.ToLower(r.Method) {
	case "post":
		if r.Body == nil {
			server.BadRequest(w, r, fmt.Errorf("no data POSTed"))
			return
		}
		serialization, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		idx := new(labels.Index)
		if err := idx.Unmarshal(serialization); err != nil {
			server.BadRequest(w, r, err)
		}
		if idx.Label != label {
			server.BadRequest(w, r, "serialized Index was for label %d yet was POSTed to label %d", idx.Label, label)
			return
		}
		if len(idx.Blocks) == 0 {
			if err := deleteLabelIndex(ctx, label); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP POST index for label %d (%s) -- empty index so deleted index", label, r.URL)
			return
		}
		if err := putCachedLabelIndex(d, ctx.VersionID(), idx); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "get":
		idx, err := getCachedLabelIndex(d, ctx.VersionID(), label)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if idx == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		serialization, err := idx.Marshal()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		n, err := w.Write(serialization)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if n != len(serialization) {
			server.BadRequest(w, r, "unable to write all %d bytes of serialized label %d index: only %d bytes written", len(serialization), label, n)
			return
		}

	default:
		server.BadRequest(w, r, "only POST or GET action allowed for /index endpoint")
		return
	}

	timedLog.Infof("HTTP %s index for label %d (%s)", r.Method, label, r.URL)
}

func (d *Data) handleIndices(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/indices
	// POST <api URL>/node/<UUID>/<data name>/indices
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	method := strings.ToLower(r.Method)
	switch method {
	case "post":
	case "get":
	default:
		server.BadRequest(w, r, "only GET and POST actions allowed for /indices endpoint")
		return
	}
	if r.Body == nil {
		server.BadRequest(w, r, fmt.Errorf("expected data to be sent for /indices request"))
		return
	}
	dataIn, err := ioutil.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, err)
	}
	var numLabels int
	if method == "post" {
		indices := new(proto.LabelIndices)
		if err := indices.Unmarshal(dataIn); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		numLabels = len(indices.Indices)
		var numDeleted int
		for i, protoIdx := range indices.Indices {
			if protoIdx == nil {
				server.BadRequest(w, r, "indices included a nil index in position %d", i)
				return
			}
			if protoIdx.Label == 0 {
				server.BadRequest(w, r, "index %d had label 0, which is a reserved label", i)
				return
			}
			if len(protoIdx.Blocks) == 0 {
				if err := deleteLabelIndex(ctx, protoIdx.Label); err != nil {
					server.BadRequest(w, r, err)
					return
				}
				numDeleted++
				continue
			}
			idx := labels.Index{LabelIndex: *protoIdx}
			if err := putCachedLabelIndex(d, ctx.VersionID(), &idx); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
		if numDeleted > 0 {
			timedLog.Infof("HTTP POST indices for %d labels, %d deleted (%s)", len(indices.Indices), numDeleted, r.URL)
			return
		}
	} else { // GET
		var labelList []uint64
		if err := json.Unmarshal(dataIn, &labelList); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("expected JSON label list for GET /indices: %v", err))
			return
		}
		if len(labelList) > 50000 {
			server.BadRequest(w, r, fmt.Sprintf("only 50,000 label indices can be returned at a time, %d given", len(labelList)))
			return
		}
		var indices proto.LabelIndices
		indices.Indices = make([]*proto.LabelIndex, len(labelList))
		for i, label := range labelList {
			idx, err := getCachedLabelIndex(d, ctx.VersionID(), label)
			if err != nil {
				server.BadRequest(w, r, "could not get label %d index in position %d: %v", label, i, err)
				return
			}
			fmt.Printf("indices.Indices[%d]: %v\n", i, idx)
			if idx != nil {
				indices.Indices[i] = &(idx.LabelIndex)
			} else {
				index := proto.LabelIndex{
					Label: label,
				}
				indices.Indices[i] = &index
			}
		}
		dataOut, err := indices.Marshal()
		if err != nil {
			server.BadRequest(w, r, "could not serialize %d label indices: %v", len(labelList), err)
			return
		}
		requestSize := int64(len(dataOut))
		if requestSize > server.MaxDataRequest {
			server.BadRequest(w, r, "requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
				requestSize, server.MaxDataRequest)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		n, err := w.Write(dataOut)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if n != len(dataOut) {
			server.BadRequest(w, r, "unable to write all %d bytes of serialized label indices: only %d bytes written", len(dataOut), n)
			return
		}
	}
	timedLog.Infof("HTTP %s indices for %d labels (%s)", method, numLabels, r.URL)
}

func (d *Data) handleMappings(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// POST <api URL>/node/<UUID>/<data name>/mappings
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	format := queryStrings.Get("format")
	var consistent bool
	if queryStrings.Get("consistent") == "true" {
		consistent = true
	}

	switch strings.ToLower(r.Method) {
	case "post":
		if r.Body == nil {
			server.BadRequest(w, r, fmt.Errorf("no data POSTed"))
			return
		}
		serialization, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
		}
		var mappings proto.MappingOps
		if err := mappings.Unmarshal(serialization); err != nil {
			server.BadRequest(w, r, err)
		}
		if err := d.ingestMappings(ctx, mappings); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP POST %d merges (%s)", len(mappings.Mappings), r.URL)

	case "get":
		if err := d.writeMappings(w, ctx.VersionID(), (format == "binary"), consistent); err != nil {
			server.BadRequest(w, r, "unable to write mappings: %v", err)
		}
		timedLog.Infof("HTTP GET mappings (%s)", r.URL)

	default:
		server.BadRequest(w, r, "only POST action allowed for /mappings endpoint")
	}
}

func (d *Data) handleHistory(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/history/<label>/<from UUID>/<to UUID>
	if len(parts) < 7 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID, 'from' UUID, and 'to' UUID to follow 'history' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "only GET action allowed for /history endpoint")
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
	fromUUID, _, err := datastore.MatchingUUID(parts[5])
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	toUUID, _, err := datastore.MatchingUUID(parts[6])
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	if err := d.GetMutationHistory(w, fromUUID, toUUID, label); err != nil {
		server.BadRequest(w, r, "unable to get mutation history: %v", err)
	}
	timedLog.Infof("HTTP GET history (%s)", r.URL)
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
		img, err := d.GetImage(ctx.VersionID(), lbl, false, scale, roiname)
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

	var isotropic = (parts[3] == "isotropic")
	shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))
	supervoxels := queryStrings.Get("supervoxels") == "true"

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
		img, err := d.GetImage(ctx.VersionID(), lbl, supervoxels, scale, roiname)
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
			data, err := d.GetVolume(ctx.VersionID(), lbl, supervoxels, scale, roiname)
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
		err = fmt.Errorf("error parsing bounds from query string: %v", err)
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

// GetSupervoxels returns the set of supervoxel ids that compose the given label
func (d *Data) GetSupervoxels(v dvid.VersionID, label uint64) (labels.Set, error) {
	idx, err := GetLabelIndex(d, v, label, false)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, err
	}
	return idx.GetSupervoxels(), nil
}

func (d *Data) handleSupervoxels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/supervoxels/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'supervoxels' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}

	supervoxels, err := d.GetSupervoxels(ctx.VersionID(), label)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if len(supervoxels) == 0 {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, "[")
		i := 0
		for supervoxel := range supervoxels {
			fmt.Fprintf(w, "%d", supervoxel)
			i++
			if i < len(supervoxels) {
				fmt.Fprintf(w, ",")
			}
		}
		fmt.Fprintf(w, "]")
	}

	timedLog.Infof("HTTP GET supervoxels for label %d (%s)", label, r.URL)
}

func (d *Data) handleLabelmod(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/lastmod/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'lastmod' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}

	idx, err := GetLabelIndex(d, ctx.VersionID(), label, false)
	if err != nil {
		server.BadRequest(w, r, "unable to get label %d index: %v", label, err)
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, `{`)
	fmt.Fprintf(w, `"mutation id": %d, `, idx.LastMutId)
	fmt.Fprintf(w, `"last mod user": %q, `, idx.LastModUser)
	fmt.Fprintf(w, `"last mod app": %q, `, idx.LastModApp)
	fmt.Fprintf(w, `"last mod time": %q `, idx.LastModTime)
	fmt.Fprintf(w, `}`)

	timedLog.Infof("HTTP GET lastmod for label %d (%s)", label, r.URL)
}

func (d *Data) handleSize(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/size/<label>[?supervoxels=true]
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'size' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}
	queryStrings := r.URL.Query()
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	size, err := GetLabelSize(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "unable to get label %d size: %v", label, err)
		return
	}
	if size == 0 {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, `{"voxels": %d}`, size)
	}

	timedLog.Infof("HTTP GET size for label %d, supervoxels=%t (%s)", label, isSupervoxel, r.URL)
}

func (d *Data) handleSizes(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/sizes
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch sizes query must be a GET request")
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad GET request body for batch sizes query: %v", err)
		return
	}
	queryStrings := r.URL.Query()
	hash := queryStrings.Get("hash")
	if err := checkContentHash(hash, data); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	var labelList []uint64
	if err := json.Unmarshal(data, &labelList); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad mapping request JSON: %v", err))
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	sizes, err := GetLabelSizes(d, ctx.VersionID(), labelList, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "unable to get label sizes: %v", err)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, size := range sizes {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", size)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch sizes query of %d labels (%s)", len(sizes), r.URL)
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

	queryStrings := r.URL.Query()
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "problem getting label set idx on label %: %v", label, err)
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if isSupervoxel {
		idx, err = idx.LimitToSupervoxel(label)
		if err != nil {
			server.BadRequest(w, r, "error limiting label %d index to supervoxel %d: %v\n", idx.Label, label, err)
			return
		}
		if idx == nil || len(idx.Blocks) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "{")
	fmt.Fprintf(w, `"voxels": %d, `, idx.NumVoxels())
	fmt.Fprintf(w, `"numblocks": %d, `, len(idx.Blocks))
	minBlock, maxBlock, err := idx.GetBlockIndices().GetBounds()
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
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

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
			found, err = d.writeLegacyRLE(ctx, label, scale, b, compression, isSupervoxel, w)
		case FormatBinaryBlocks:
			found, err = d.writeBinaryBlocks(ctx, label, scale, b, compression, isSupervoxel, w)
		case FormatStreamingRLE:
			found, err = d.writeStreamingRLE(ctx, label, scale, b, compression, isSupervoxel, w)
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
		found, err := d.FoundSparseVol(ctx, label, b, isSupervoxel)
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

	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	coord, err := dvid.StringToPoint(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	label, err := d.GetLabelAtScaledPoint(ctx.VersionID(), coord, scale, isSupervoxel)
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
		found, err = d.writeLegacyRLE(ctx, label, 0, b, compression, isSupervoxel, w)
	case FormatBinaryBlocks:
		found, err = d.writeBinaryBlocks(ctx, label, 0, b, compression, isSupervoxel, w)
	case FormatStreamingRLE:
		found, err = d.writeStreamingRLE(ctx, label, 0, b, compression, isSupervoxel, w)
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
	queryStrings := r.URL.Query()
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
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
	data, err := d.GetSparseCoarseVol(ctx, label, b, isSupervoxel)
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

func (d *Data) handleMaxlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/maxlabel
	// POST <api URL>/node/<UUID>/<data name>/maxlabel/<max label>
	timedLog := dvid.NewTimeLog()
	switch strings.ToLower(r.Method) {
	case "get":
		w.Header().Set("Content-Type", "application/json")
		maxlabel, ok := d.MaxLabel[ctx.VersionID()]
		if !ok {
			server.BadRequest(w, r, "No maximum label found for %s version %d\n", d.DataName(), ctx.VersionID())
			return
		}
		fmt.Fprintf(w, "{%q: %d}", "maxlabel", maxlabel)

	case "post":
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires max label ID to follow POST /maxlabel")
			return
		}
		maxlabel, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		changed, err := d.updateMaxLabel(ctx.VersionID(), maxlabel)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if changed {
			versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
			msginfo := map[string]interface{}{
				"Action":    "post-maxlabel",
				"MaxLabel":  maxlabel,
				"UUID":      string(versionuuid),
				"Timestamp": time.Now().String(),
			}
			jsonmsg, _ := json.Marshal(msginfo)
			if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
				dvid.Errorf("error on sending split op to kafka: %v", err)
			}
		}

	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
	timedLog.Infof("HTTP maxlabel request (%s)", r.URL)
}

func (d *Data) handleNextlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/nextlabel
	// POST <api URL>/node/<UUID>/<data name>/nextlabel/<number of labels>
	timedLog := dvid.NewTimeLog()
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(r.Method) {
	case "get":
		fmt.Fprintf(w, `{"nextlabel": %d}`, d.MaxRepoLabel+1)
	case "post":
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires number of requested labels to follow POST /nextlabel")
			return
		}
		numLabels, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		start, end, err := d.newLabels(ctx.VersionID(), numLabels)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		fmt.Fprintf(w, `{"start": %d, "end": %d}`, start, end)
		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":      "post-nextlabel",
			"Start Label": start,
			"End Label":   end,
			"UUID":        string(versionuuid),
			"Timestamp":   time.Now().String(),
		}
		jsonmsg, _ := json.Marshal(msginfo)
		if err = d.ProduceKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending split op to kafka: %v", err)
		}
		return
	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
	timedLog.Infof("HTTP maxlabel request (%s)", r.URL)
}

func (d *Data) handleSplitSupervoxel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/split-supervoxel/<supervoxel>?<options>
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Split requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
		return
	}
	queryStrings := r.URL.Query()
	splitStr := queryStrings.Get("split")
	remainStr := queryStrings.Get("remain")
	downscale := true
	if queryStrings.Get("downres") == "false" {
		downscale = false
	}
	var err error
	var split, remain uint64
	if splitStr != "" {
		split, err = strconv.ParseUint(splitStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "bad split query string provided: %s", splitStr)
			return
		}
	}
	if remainStr != "" {
		remain, err = strconv.ParseUint(remainStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "bad remain query string provided: %s", remainStr)
			return
		}
	}

	timedLog := dvid.NewTimeLog()

	supervoxel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if supervoxel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as split target\n")
		return
	}
	info := dvid.GetModInfo(r)
	splitSupervoxel, remainSupervoxel, mutID, err := d.SplitSupervoxel(ctx.VersionID(), supervoxel, split, remain, r.Body, info, downscale)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split supervoxel %d -> %d, %d: %v", supervoxel, splitSupervoxel, remainSupervoxel, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"SplitSupervoxel": %d, "RemainSupervoxel": %d, "MutationID": %d}`, splitSupervoxel, remainSupervoxel, mutID)

	timedLog.Infof("HTTP split supervoxel of supervoxel %d request (%s)", supervoxel, r.URL)
}

func (d *Data) handleCleave(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/cleave/<label>
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Cleave requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'cleave' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as cleave target\n")
		return
	}
	modInfo := dvid.GetModInfo(r)
	cleaveLabel, mutID, err := d.CleaveLabel(ctx.VersionID(), label, modInfo, r.Body)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"CleavedLabel": %d, "MutationID": %d}`, cleaveLabel, mutID)

	timedLog.Infof("HTTP cleave of label %d request (%s)", label, r.URL)
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
	info := dvid.GetModInfo(r)
	toLabel, mutID, err := d.SplitLabels(ctx.VersionID(), fromLabel, r.Body, info)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split label %d: %v", fromLabel, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"label": %d, "MutationID": %d}`, toLabel, mutID)

	timedLog.Infof("HTTP split of label %d request (%s)", fromLabel, r.URL)
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
	info := dvid.GetModInfo(r)
	mutID, err := d.MergeLabels(ctx.VersionID(), mergeOp, info)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Error on merge: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"MutationID": %d}`, mutID)

	timedLog.Infof("HTTP merge request (%s)", r.URL)
}

// --------- Other functions on labelmap Data -----------------

// GetSupervoxelBlock returns a compressed supervoxel Block of the given block coordinate.
func (d *Data) GetSupervoxelBlock(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8) (*labels.Block, error) {
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

// GetLabelBytesWithScale returns a block of labels at given scale in packed little-endian uint64 format.
func (d *Data) GetLabelBytesWithScale(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8, supervoxels bool) ([]byte, error) {
	block, err := d.GetSupervoxelBlock(v, bcoord, scale)
	if err != nil {
		return nil, err
	}
	var mapping *SVMap
	if !supervoxels {
		if mapping, err = getMapping(d, v); err != nil {
			return nil, err
		}
	}
	if mapping != nil && mapping.exists(v) {
		err = modifyBlockMapping(v, block, mapping)
		if err != nil {
			return nil, fmt.Errorf("unable to modify block %s mapping: %v", bcoord, err)
		}
	}
	labelData, _ := block.MakeLabelVolume()
	return labelData, nil
}

// GetLabelAtScaledPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtScaledPoint(v dvid.VersionID, pt dvid.Point, scale uint8, supervoxels bool) (uint64, error) {
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return 0, fmt.Errorf("Can't determine block of point %s", pt)
	}
	blockSize := d.BlockSize()
	bcoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	labelData, err := d.GetLabelBytesWithScale(v, bcoord, scale, supervoxels)
	if err != nil {
		return 0, err
	}
	if len(labelData) == 0 {
		return 0, nil
	}

	// Retrieve the particular label within the block.
	ptInBlock := coord.PointInChunk(blockSize)
	nx := int64(blockSize.Value(0))
	nxy := nx * int64(blockSize.Value(1))
	i := (int64(ptInBlock.Value(0)) + int64(ptInBlock.Value(1))*nx + int64(ptInBlock.Value(2))*nxy) * 8

	return binary.LittleEndian.Uint64(labelData[i : i+8]), nil
}

// The following functions implement an interface to synced data types like annotation.

// GetLabelBytes returns a block of hi-res (body) labels (scale 0) in packed little-endian uint64 format
func (d *Data) GetLabelBytes(v dvid.VersionID, bcoord dvid.ChunkPoint3d) ([]byte, error) {
	return d.GetLabelBytesWithScale(v, bcoord, 0, false)
}

// GetLabelAtPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	return d.GetLabelAtScaledPoint(v, pt, 0, false)
}

// GetSupervoxelAtPoint returns the 64-bit unsigned int supervoxel id for a given point.
func (d *Data) GetSupervoxelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	return d.GetLabelAtScaledPoint(v, pt, 0, true)
}

type ptsIndex struct {
	pts     []dvid.Point3d
	indices []int
}

type blockPtsI struct {
	*labels.Block
	ptsIndex
}

func (d *Data) partitionPoints(pts []dvid.Point3d) map[dvid.IZYXString]ptsIndex {
	blockSize := d.BlockSize().(dvid.Point3d)
	blockPts := make(map[dvid.IZYXString]ptsIndex)
	for i, pt := range pts {
		x := pt[0] / blockSize[0]
		y := pt[1] / blockSize[1]
		z := pt[2] / blockSize[2]
		bx := pt[0] % blockSize[0]
		by := pt[1] % blockSize[1]
		bz := pt[2] % blockSize[2]
		bpt := dvid.Point3d{bx, by, bz}
		bcoord := dvid.ChunkPoint3d{x, y, z}.ToIZYXString()
		ptsi, found := blockPts[bcoord]
		if found {
			ptsi.pts = append(ptsi.pts, bpt)
			ptsi.indices = append(ptsi.indices, i)
		} else {
			ptsi.pts = []dvid.Point3d{bpt}
			ptsi.indices = []int{i}
		}
		blockPts[bcoord] = ptsi
	}
	return blockPts
}

// GetLabelPoints returns labels or supervoxels corresponding to given 3d points.
func (d *Data) GetLabelPoints(v dvid.VersionID, pts []dvid.Point3d, scale uint8, useSupervoxels bool) (mapped []uint64, err error) {
	if len(pts) == 0 {
		return
	}
	timedLog := dvid.NewTimeLog()

	mapped = make([]uint64, len(pts))
	blockPts := d.partitionPoints(pts)

	// Get mapping.
	var mapping *SVMap
	var ancestry []uint8
	if !useSupervoxels {
		if mapping, err = getMapping(d, v); err != nil {
			return nil, err
		}
		if mapping != nil && mapping.exists(v) {
			ancestry, err = mapping.getAncestry(v)
			if err != nil {
				err = fmt.Errorf("unable to get ancestry for version %d: %v", v, err)
				return
			}
		}
	}

	// Iterate through blocks and get labels without inflating blocks and concurrently process data.
	var wg sync.WaitGroup
	var labelsMu sync.Mutex
	concurrency := len(blockPts) / 10
	if concurrency < 1 {
		concurrency = 1
	}
	ch := make(chan blockPtsI, len(blockPts))
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			for bptsI := range ch {
				if len(ancestry) > 0 {
					mapping.ApplyMappingToBlock(ancestry, bptsI.Block)
				}
				blockLabels := bptsI.Block.GetPointLabels(bptsI.pts)
				labelsMu.Lock()
				for i, index := range bptsI.indices {
					mapped[index] = blockLabels[i]
				}
				labelsMu.Unlock()
			}
			wg.Done()
		}()
	}
	for bcoord, ptsi := range blockPts {
		chunkPt3d, err := bcoord.ToChunkPoint3d()
		if err != nil {
			close(ch)
			return nil, err
		}
		block, err := d.GetSupervoxelBlock(v, chunkPt3d, scale)
		if err != nil {
			close(ch)
			return nil, err
		}
		ch <- blockPtsI{block, ptsi}
	}
	close(ch)
	wg.Wait()

	if len(blockPts) > 10 {
		timedLog.Infof("Larger label query for annotation %q at %d points -> %d blocks with %d goroutines", d.DataName(), len(pts), len(blockPts), concurrency)
	}
	return
}

// GetPointsInSupervoxels returns the 3d points that fall within given supervoxels that are
// assumed to be mapped to one label.  If supervoxels are assigned to more than one label,
// or a mapping is not available, an error is returned.  The label index is not used so
// this function will use immutable data underneath if there are no supervoxel splits.
func (d *Data) GetPointsInSupervoxels(v dvid.VersionID, pts []dvid.Point3d, supervoxels []uint64) (inSupervoxels []bool, err error) {
	inSupervoxels = make([]bool, len(pts))
	if len(pts) == 0 || len(supervoxels) == 0 {
		return
	}
	timedLog := dvid.NewTimeLog()

	supervoxelSet := make(labels.Set)
	for _, supervoxel := range supervoxels {
		supervoxelSet[supervoxel] = struct{}{}
	}
	ptsByBlock := d.partitionPoints(pts)

	// Launch the block processing goroutines
	var wg sync.WaitGroup
	concurrency := len(ptsByBlock) / 10
	if concurrency < 1 {
		concurrency = 1
	}
	ch := make(chan blockPtsI, len(ptsByBlock))
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			for bptsI := range ch {
				blockLabels := bptsI.Block.GetPointLabels(bptsI.pts)
				for i, index := range bptsI.indices {
					supervoxel := blockLabels[i]
					if _, found := supervoxelSet[supervoxel]; found {
						inSupervoxels[index] = true
					}
				}
			}
			wg.Done()
		}()
	}

	// Send the blocks spanning the given supervoxels.
	var block *labels.Block
	for bcoord, bptsI := range ptsByBlock {
		var chunkPt3d dvid.ChunkPoint3d
		if chunkPt3d, err = bcoord.ToChunkPoint3d(); err != nil {
			close(ch)
			return
		}
		if block, err = d.GetSupervoxelBlock(v, chunkPt3d, 0); err != nil {
			close(ch)
			return nil, err
		}
		ch <- blockPtsI{block, bptsI}
	}
	close(ch)
	wg.Wait()

	timedLog.Infof("Annotation %q point check for %d supervoxels, %d points -> %d blocks (%d goroutines)", d.DataName(), len(supervoxels), len(pts), len(ptsByBlock), concurrency)
	return
}
