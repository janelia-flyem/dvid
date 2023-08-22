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
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelmap"
	TypeName = "labelmap"
)

const helpMessage = `
API for label map data type (github.com/janelia-flyem/dvid/datatype/labelmap)
=============================================================================

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
	
$ dvid node <UUID> <data name> set-nextlabel <label>

	Sets the counter for new labels repo-wide for the given labelmap instance.
	Note that the next label will be one more than the given label, and the given
	label must be 1 or more.  If label is 0, then this next label setting will
	be ignored and future labels will be determined by the repo-wide max label
	as is the default.
	
	This is a dangerous command if you set the next label to a low value because
	it will not check if it starts to encroach higher label values, so use with
	caution.

    Example: 

	$ dvid node 3f8c segmentation set-nextlabel 999
	
	The next new label, for example in a cleave, will be 1000.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	label     	  A uint64 label ID
	
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

    Returns or posts JSON of configuration settings with the following optional fields:

     "BlockSize"        Size in pixels  (default: 64,64,64 and should be multiples of 16)
     "VoxelSize"        Resolution of voxels (default: 8.0,8.0,8.0)
     "VoxelUnits"       Resolution units (default: "nanometers")
	 "MinPoint"         Minimum voxel coordinate as 3d point
	 "MaxPoint"         Maximum voxel coordinate as 3d point
	 "GridStore"        Store identifier in TOML config file that specifies precomputed store.
	 "MaxDownresLevel"  The maximum down-res level supported.  Each down-res is factor of 2.

    Arguments:

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.

POST <api URL>/node/<UUID>/<data name>/extents
  
	Sets the extents for the image volume.  This is primarily used when POSTing from multiple
  	DVID servers not sharing common metadata to a shared backend.

	Extents should be in JSON in the following format:
	{
		"MinPoint": [0,0,0],
		"MaxPoint": [300,400,500]
	}

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

	Returns JSON for mutations involving labels in "from UUID" version that correspond to the
	supervoxels in the "to UUID" target label.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
	label     	  The label ID as exists in the later version specified by <to UUID>.
	from UUID     The UUID of the earlier version in time range.
	to UUID       The UUID of the later version in time range.


GET <api URL>/node/<UUID>/<data name>/mapping[?queryopts]

	Returns JSON for mapped uint64 identifiers (labels). The mapping holds not only the
	unique IDs of supervoxels but also newly created IDs for renumbered & cleaved bodies
	that will never overlap with supervoxel IDs. 
	
	Expects JSON in GET body:

	[ label1, label2, label3, ...]

	Returns for each POSTed label the corresponding mapped label:

	[ 23, 0, 911, ...]

	The mapped label can be 0 in the following circumstances:
	* The label was a supervoxel ID that was split into two different unique IDs.
	* The label is used for a newly generated ID that will be a new renumbered label.
	* The label is used for a newly generated ID that will represent a cleaved body ID.
	
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
    storing labelmap data if you want DVID to also handle indexing and downres computation.
	If you are calculating indices and downres supervoxel blocks offline for ingesting into
	DVID, use the "POST /ingest-supervoxels" endpoint, since it is even faster.

	It's suggested that downres supervoxel blocks should be calculated outside DVID and then
	ingested for anything larger than small (Gigavoxel) volumes. Currently, the downres computation 
	is not robust for non-cubic chunk sizes and because this endpoint must consider parallel 
	requests using overlapping blocks, a mutex is employed that limits the overall throughput.  
	Still data read from the HTTP stream is written directly to the underlying storage.  The
	default (and currently only supported) compression is gzip on compressed DVID label Block 
	serialization.

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


POST <api URL>/node/<UUID>/<data name>/ingest-supervoxels[?queryopts]

    Ingests properly-sized supervoxel block data under the assumption that parallel calls to this 
	endpoint do not use overlapping blocks.  This is the most server-efficient way of storing labelmap data, 
	where data read from the HTTP stream is written directly to the underlying storage.  The default 
	(and currently only supported) compression is gzip on compressed DVID label Block serialization.
	Unlike the "POST /blocks" endpoint, this endpoint assumes that the following operations will be done separately:
	    * label indexing
		* syncing to other instances (e.g., annotations)
		* calculation and POST /maxlabel
		* calculation and POST /extents
		* downres calculations of different scales and the POST of the downres supervoxel block data.

	This endpoint maximizes write throughput and assumes parallel POST requests will not use overlapping blocks.
	No goroutines are spawned so the number of write goroutines is directly related to the number of parallel
	calls to this endpoint.

    Example: 

    POST <api URL>/node/3f8c/segmentation/ingest-supervoxels?scale=1

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
	                of previous level.  Level 0 (default) is the highest resolution.


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

GET <api URL>/node/<UUID>/<data name>/supervoxel-sizes/<label>

	Returns the supervoxels and their sizes for the given label in JSON.
	Although the position in the lists will match supervoxel label and size,
	the supervoxels may not be ordered:
	{
		"supervoxels": [1,2,4,3,...],
		"sizes": [100,200,400,300,...]
	}

	Returns a status code 404 (Not Found) if label does not exist.
	
    Arguments:
    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmap instance.
    label     	  A 64-bit integer label id


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

    minx    Spans must be >= this minimum x voxel coordinate at given scale
    maxx    Spans must be <= this maximum x voxel coordinate at given scale.
    miny    Spans must be >= this minimum y voxel coordinate at given scale.
    maxy    Spans must be <= this maximum y voxel coordinate at given scale.
    minz    Spans must be >= this minimum z voxel coordinate at given scale.
    maxz    Spans must be <= this maximum z voxel coordinate at given scale.
    exact   "false" if RLEs can extend a bit outside voxel bounds within border blocks.
             This will give slightly faster responses. 

    compression  "lz4" and "gzip" compressed format; only applies to "rles" format for now.
	scale        A number from 0 (default highest res) to MaxDownresLevel where each level 
				   beyond 0 has 1/2 resolution of previous level.
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


POST <api URL>/node/<UUID>/<data name>/renumber

	Renumbers labels.  Requires JSON in request body using the following format:

	[newLabel1, oldLabel1, newLabel2, oldLabel2, ...]

	For each pair of numbers passed in, the oldLabel is renumbered to the newLabel.
	So if we pass [21, 1, 22, 2] then label 1 is renumbered to 21, and label 2 is
	renumbered to 22.

	Kafka JSON message generated by each renumbering pair in this request:
		{ 
			"Action": "renumber",
			"NewLabel": <new label>,
			"OrigLabel": <old label>,
			"UUID": <UUID on which renumber was done>,
			"MutationID": <unique id for mutation>,
			"Timestamp": <time at start of operation>
		}

	After completion of each renumbering op, the following Kafka JSON message is published:
		{ 
			"Action": "renumber-complete",
			"NewLabel": <new label>,
			"OrigLabel": <old label>,
			"UUID": <UUID on which renumber was done>
			"MutationID": <unique id for mutation>,
			"Timestamp": <time at end of operation>
		}
		
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
			"MutationID": <unique id for mutation>,
			"Timestamp": <time at start of operation>
		}

	After completion of the merge op, the following JSON message is published:
		{ 
			"Action": "merge-complete",
			"Target": <to label>,
			"Labels": [<to merge label 1>, <to merge label2>, ...],
			"UUID": <UUID on which merge was done>
			"MutationID": <unique id for mutation>,
			"Timestamp": <time at end of operation>
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
			"MutationID": <unique id for mutation>,
			"Timestamp": <time at start of operation>
		}

	After the cleave is successfully completed, the following JSON message is logged with Kafka:
	{ 
		"Action": "cleave-complete",
		"OrigLabel": <original label>,  
		"CleavedLabel": <cleaved label>,
		"CleavedSupervoxels": [<supervoxel 1>, <supervoxel 2>, ...],
		"CleavedSize": <number of voxels cleaved>,
		"RemainSize": <number of voxels remaining>,
		"UUID": <UUID on which cleave was done>,
		"MutationID": <unique id for mutation>,
		"Timestamp": <time at end of operation>
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
			"Body": <label of body containing the split supervoxel>,
			"SplitSupervoxel": <new label of split portion>,
			"RemainSupervoxel": <new label of remainder> 
			"Split": <string for reference to split data in serialized RLE format>,
			"UUID": <UUID on which split was done>,
			"MutationID": <unique mutation ID corresponding to this operation>,
			"Timestamp": <time at start of operation>
		}
	
	The split reference above can be used to download the split binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}
	
	After completion of the split op, since it can take some time to process all blocks, 
	the following JSON message is published:
		{ 
			"Action": "split-supervoxel-complete",
			"Supervoxel": <supervoxel label>,
			"Body": <label of body containing the split supervoxel>,
			"SplitSupervoxel": <new label of split portion>,
			"RemainSupervoxel": <new label of remainder> 
			"Split": <string for reference to split data in serialized RLE format>,
			"SplitSize": <size in voxels of the split supervoxel>,
			"RemainSize": <size in voxels of the remaining supervoxel>,
			"MutationID": <unique id for mutation>,
			"UUID": <UUID on which split was done>,
			"Timestamp": <time at end of operation>
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
			"MutationID": <unique id for mutation>,
			"UUID": <UUID on which split was done>,
			"SVSplit": {23918: {"Remain": 481273, "Split": 481274}, 1839109: {"Remain":...}},
			"Timestamp": <time at start of operation>
		}
	
	The SVSplit field above gives the new remain and split labels for any given split supervoxel.
	The split reference above can be used to download the split binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}
	
	After completion of the split op, the following JSON message is published:
		{ 
			"Action": "split-complete",
			"Target": <from label>,
			"NewLabel": <to label>,
			"Split": <string for reference to split data in serialized RLE format>,
			"MutationID": <unique id for mutation>,
			"UUID": <UUID on which split was done>,
			"SVSplit": {23918: {"Remain": 481273, "Split": 481274}, 1839109: {"Remain":...}},
			"Timestamp": <time at end of operation>
		}

GET <api URL>/node/<UUID>/<data name>/proximity/<label 1 (target)>,<label 2a>,<label 2b>,...

	Determines proximity of a number of labels with a target label returning the 
	following JSON giving blocks in which they intersect and the min euclidean 
	voxel distance between the labels within that block:
		[
			{ 
				"Block": [<x1>, <y1>, <z1>],
				"Label": <label 2a>,
				"Distance": <min euclidean voxel distance in block 1 to label 2a>
			}, 
			{ 
				"Block": [<x1>, <y1>, <z1>],
				"Label": <label 2b>
				"Distance": <min euclidean voxel distance in block 1 to label 2b>
			},
			...
		]
	If only two labels are specified, the "Label" property is omitted.

	Note that this is an approximation in that we assume two labels aren't perfectly
	separated across block boundaries. This could be the case for supervoxels that
	are artificially split along block boundaries but won't be true for agglomerated
	labels.
	
GET  <api URL>/node/<UUID>/<data name>/index/<label>?mutid=<uint64>
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
		uint64 last_mut_id = 3;
		string last_mod_time = 4;  // string is time in RFC 3339 format
		string last_mod_user = 5;
		string last_mod_app = 6;
	}

	If the blocks map is empty on a POST, the label index is deleted.

	Query-string options:

		metadata-only: if "true" (default "false"), returns JSON object of num_voxels, last_mutid, last_mod_time, last_mod_user.
		mutid: (Read only) Returns the label index prior to completing the given mutation id in the current branch. If
		   no cached label index is found prior to the given mutation id in the branch of the specified UUID, 
		   a 404 not found is returned.

	
GET <api URL>/node/<UUID>/<data name>/listlabels[?queryopts]

	Streams labels and optionally their voxel counts in numerical order up to a maximum
	of 10 million labels.  The GET returns a stream of little-endian uint64.  If label sizes are
	requested, the stream is <label id i> <voxels in label i> <label id i+1> <voxels in label i+1> ...
	If label sizes aren't requested, the stream is simply <label id i> <label id i+1> ...

	Note that because the data is streamed over HTTP, an error code cannot be sent once data is 
	in flight, so any error is marked by four consecutive uint64 with value 0.

	The query strings allow you to page through vast amounts of labels by changing the start. 
	For example:
	   GET /api/node/37af8/segmentation/listlabels
	      which is equivalent to
	   GET /api/node/37af8/segmentation/listlabels?start=0    --> say it returns up to label 10,281,384 due to some gaps in labeling.
	      and then you can call
	   GET /api/node/37af8/segmentation/listlabels?start=10281385   --> to return next batch, start with last received label + 1
	
	Note that the start is a label identifier and not a position or index.  Since labels are
	stored consecutively, you can omit the start query string (default is start=0) to guarantee 
	you will get the smallest label identifier, which should be non-zero since 0 is used only for background.
	
	Query-string options:

		start: starting label id (use 0 to start at very beginning since labels are stored consecutively)
		number: number of labels to return (if not specified, returns maximum of 10,000,000 labels)
		sizes: if "true", returns the number of voxels for each label.

GET <api URL>/node/<UUID>/<data name>/indices

	Note: For more optimized bulk index retrieval, see /indices-compressed.
	Allows bulk GET of indices (blocks per supervoxel and their voxel count) by
	including in the GET body a JSON array of requested labels (max 50,000 labels):

		[ 1028193, 883177046, ... ]

	The GET returns a protobuf serialization of a LabelIndices message defined by:
	
	message LabelIndices {
		repeated LabelIndex indices = 1;
	}

	where LabelIndex is defined by the protobuf above in the /index documentation.

GET <api URL>/node/<UUID>/<data name>/indices-compressed

	The fastest and most efficient way for bulk index retrieval.  Streams lz4 
	compressed LabelIndex protobuf messages with minimal processing server-side.  
	This allows lz4 uncompression to be done in parallel on clients as well as
	immediate processing of each label index as it is received.
	Up to 50,000 labels may be requested at once by sending a JSON array of 
	requested labels in GET body:

		[ 1028193, 883177046, ... ]

	The GET returns a stream of data with the following format:

	first label index compressed size in bytes (uint64 little endian)
	first label id (uint64 little endian)
	first label index protobuf (see definition in /index doc), lz4 compressed
	second label index compressed size in bytes (uint64 little endian)
	second label id (uint64 little endian)
	second label index protobuf, lz4 compressed
	...

	Although the label id is included in the protobuf, the stream includes the label
	of the record in case there is corruption of the record or other issues.  So this
	call allows verifying the records are properly stored.  

	Missing labels will have 0 size and no bytes for the protobuf data.

	If an error occurs, zeros will be transmitted for a label index size and a label id.
	
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

GET <api URL>/node/<UUID>/<data name>/mutations[?queryopts]

	Returns JSON of the successfully completed mutations for the given version 
	for this data instance.  The format is equivalent to the JSON provided to 
	the Kafka mutation log.  For example, a merge mutation would be:

	{
		"Action": "merge",
		"Target": <label ID>,
		"Labels": [<merged label 1>, <merged label 2>, ...],
		"UUID": "28841c8277e044a7b187dda03e18da13",
		"MutationID": <uint64>,
		"Timestamp": <string>,
		"User": <user id string if supplied during mutation>,
		"App": <app id string if supplied during mutation>
	}

	Query-string options:

		userid:  Limit returned mutations to the given User ID.  Note that
				 this should be distinguished from the "u" query string which
				 is the requester's User ID (not necessarily the same as the
				 User ID whose mutations are being requested).

GET <api URL>/node/<UUID>/<data name>/mappings[?queryopts]

	Streams space-delimited mappings for the given UUID, one mapping per line:

		supervoxel0 mappedTo0
		supervoxel1 mappedTo1
		supervoxel2 mappedTo2
		...
		supervoxelN mappedToN
	
	Note that only non-identity mappings are presented unless the mapping is 
	associated with some kind of mutation.  The mapping contains not only the 
	unique IDs of supervoxels but also newly created IDs for renumbered & 
	cleaved bodies that will never overlap with supervoxel IDs. 
	
	The mapped label can be 0 in the following circumstances:
	* The label was a supervoxel ID that was split into two different unique IDs.
	* The label is used for a newly generated ID that will be a new renumbered label.
	* The label is used for a newly generated ID that will represent a cleaved body ID.

	Query-string Options:

		format: If format=binary, the data is returned as little-endian binary uint64 pairs,
				in the same order as shown in the CSV format above.

POST <api URL>/node/<UUID>/<data name>/mappings

	Allows direct storing of merge maps for a particular UUID.  Typically, merge
	maps are stored as part of POST /merge calls, which actually modify label
	index data.  If there are cluster systems capable of computing label
	blocks, indices, mappings (to represent agglomerations) and affinities directly,
	though, it's more efficient to simply load them into dvid. 

	See the description of mappings in GET /mapping and /mappings.

	The POST expects a protobuf serialization of a MergeOps message defined by:

	message MappingOp {
		uint64 mutid = 1;
		uint64 mapped = 2;
		repeated uint64 original = 3;
	}
	
	message MappingOps {
		repeated MappingOp mappings = 1;
	}

GET  <api URL>/node/<UUID>/<data name>/map-stats
	
	Returns JSON describing in-memory mapping stats.
`

var (
	dtype        Type
	encodeFormat dvid.DataValues

	zeroLabelBytes = make([]byte, 8)

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

	// The next label ID to be returned for new labels.  This can override MaxRepoLabel
	// if it is non-zero.  If zero, it is not used.  This was added to allow new
	// labels to be assigned that have lower IDs than existing labels.
	NextLabel uint64

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

// --- Override of DataService interface ---

func (d *Data) modifyConfig(config dvid.Config) error {
	if err := d.Data.ModifyConfig(config); err != nil {
		return err
	}
	s, found, err := config.GetString("MaxDownresLevel")
	if err != nil {
		return err
	}
	if found {
		maxDownresLevel, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return err
		}
		d.MaxDownresLevel = uint8(maxDownresLevel)
	}
	return nil
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
	if d.updates[scale] == 0 {
		dvid.Criticalf("StopScaleUpdate(%d) called more than StartScaleUpdate.\n", scale)
	}
	d.updates[scale]--
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
	d.NextLabel = d2.NextLabel

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
	NextLabel       uint64
	IndexedLabels   bool
	MaxDownresLevel uint8
}

func (d *Data) MarshalJSON() ([]byte, error) {
	vctx, err := datastore.NewVersionedCtxMasterLeaf(d)
	if err != nil {
		return json.Marshal(struct {
			Base     *datastore.Data
			Extended propsJSON
		}{
			d.Data.Data,
			propsJSON{
				Properties:      d.Data.Properties,
				MaxLabel:        d.MaxLabel,
				MaxRepoLabel:    d.MaxRepoLabel,
				NextLabel:       d.NextLabel,
				IndexedLabels:   d.IndexedLabels,
				MaxDownresLevel: d.MaxDownresLevel,
			},
		})
	}
	return d.MarshalJSONExtents(vctx)
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
			NextLabel:       d.NextLabel,
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

// setResolution loads JSON data to set VoxelSize property.
func (d *Data) setResolution(uuid dvid.UUID, jsonBytes []byte) error {
	config := make(dvid.NdFloat32, 3)
	if err := json.Unmarshal(jsonBytes, &config); err != nil {
		return err
	}
	d.Properties.VoxelSize = config
	return datastore.SaveDataByUUID(uuid, d)
}

// makes database call for any update
func (d *Data) updateMaxLabel(v dvid.VersionID, label uint64) (changed bool, err error) {
	d.mlMu.RLock()
	curMax, found := d.MaxLabel[v]
	if !found || curMax < label {
		changed = true
	}
	d.mlMu.RUnlock()
	if !changed {
		return
	}

	d.mlMu.Lock()
	defer d.mlMu.Unlock()

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
	return d.Data.Equals(d2.Data)
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

func (d *Data) persistNextLabel() error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.NextLabel)
	ctx := storage.NewDataContext(d, 0)
	return store.Put(ctx, nextLabelTKey, buf)
}

// newLabel returns a new label for the given version.
func (d *Data) newLabel(v dvid.VersionID) (uint64, error) {
	d.mlMu.Lock()
	defer d.mlMu.Unlock()

	// Increment and store if we don't have an ephemeral new label start ID.
	if d.NextLabel != 0 {
		d.NextLabel++
		if err := d.persistNextLabel(); err != nil {
			return d.NextLabel, err
		}
		return d.NextLabel, nil
	}
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

	// Increment and store.
	if d.NextLabel != 0 {
		begin = d.NextLabel + 1
		end = d.NextLabel + numLabels
		d.NextLabel = end
		if err = d.persistNextLabel(); err != nil {
			return
		}
		return
	}
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

// SetNextLabelStart sets the next label ID for this labelmap instance across
// the entire repo.
func (d *Data) SetNextLabelStart(nextLabelID uint64) error {
	d.NextLabel = nextLabelID
	if err := d.persistNextLabel(); err != nil {
		return err
	}
	return nil
}

// --- datastore.InstanceMutator interface -----

// LoadMutable loads mutable properties of label volumes like the maximum labels
// for each version.  Note that we load these max and next labels from key-value pairs
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
	go d.loadLabelIDs(wg, ch)

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
	if d.NextLabel != 0 {
		dvid.Infof("Loaded next label id for labelmap %q: %d\n", d.DataName(), d.NextLabel)
	}
	return saveRequired, nil
}

const veryLargeLabel = 10000000000 // 10 billion

func (d *Data) loadLabelIDs(wg *sync.WaitGroup, ch chan *storage.KeyValue) {
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

	// Load in next label if set.
	data, err = store.Get(ctx, nextLabelTKey)
	if err != nil {
		dvid.Errorf("Error getting repo-wide next label: %v\n", err)
		return
	}
	if data == nil || len(data) != 8 {
		dvid.Errorf("Could not load repo-wide next label for instance %q.  No next label override of max label.\n", d.DataName())
	} else {
		d.NextLabel = binary.LittleEndian.Uint64(data)
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
			return nil, fmt.Errorf("illegal geometry requested: %s", geom)
		}
		requestSize := int64(bytesPerVoxel) * numVoxels
		if requestSize > server.MaxDataRequest {
			return nil, fmt.Errorf("requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
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
	data64 := make([]byte, numBytes)

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

// --- datastore.DataService interface ---------

// PushData pushes labelmap data to a remote DVID.
func (d *Data) PushData(p *datastore.PushSession) error {
	// Delegate to imageblk's implementation.
	return d.Data.PushData(p)
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(req datastore.Request, reply *datastore.Response) error {
	switch req.TypeCommand() {
	case "set-nextlabel":
		if len(req.Command) < 5 {
			return fmt.Errorf("poorly formatted set-nextlabel command, see command-line help")
		}

		// Parse the request
		var uuidStr, dataName, cmdStr, labelStr string
		req.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &labelStr)

		uuid, _, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}

		dataservice, err := datastore.GetDataByUUIDName(uuid, dvid.InstanceName(dataName))
		if err != nil {
			return err
		}
		lmData, ok := dataservice.(*Data)
		if !ok {
			return fmt.Errorf("instance %q of uuid %s was not a labelmap instance", dataName, uuid)
		}

		nextLabelID, err := strconv.ParseUint(labelStr, 10, 64)
		if err != nil {
			return err
		}

		if err := lmData.SetNextLabelStart(nextLabelID); err != nil {
			return err
		}

		reply.Text = fmt.Sprintf("Set next label ID to %d.\n", nextLabelID)
		return nil

	case "load":
		if len(req.Command) < 5 {
			return fmt.Errorf("poorly formatted load command, see command-line help")
		}
		// Parse the request
		var uuidStr, dataName, cmdStr, offsetStr string
		filenames, err := req.FilenameArgs(1, &uuidStr, &dataName, &cmdStr, &offsetStr)
		if err != nil {
			return err
		}
		if len(filenames) == 0 {
			return fmt.Errorf("need to include at least one file to add: %s", req)
		}

		offset, err := dvid.StringToPoint(offsetStr, ",")
		if err != nil {
			return fmt.Errorf("illegal offset specification: %s: %v", offsetStr, err)
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

	case "extents":
		jsonBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.SetExtents(ctx, uuid, jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "resolution":
		jsonBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.setResolution(uuid, jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "map-stats":
		if action != "get" {
			server.BadRequest(w, r, "only GET available for endpoint /map-stats")
			return
		}
		jsonBytes, err := d.GetMapStats(ctx)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(jsonBytes))

	case "info":
		if action == "post" {
			config, err := server.DecodeJSON(r)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if err := d.modifyConfig(config); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if err := datastore.SaveDataByUUID(uuid, d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			fmt.Fprintf(w, "Changed '%s' based on received configuration:\n%s\n", d.DataName(), config)
			return
		} else {
			jsonBytes, err := d.MarshalJSONExtents(ctx)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, string(jsonBytes))
		}

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
			fmt.Fprint(w, string(jsonBytes))
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

	case "listlabels":
		d.handleListLabels(ctx, w, r)

	case "mapping":
		d.handleMapping(ctx, w, r)

	case "supervoxel-splits":
		d.handleSupervoxelSplits(ctx, w, r)

	case "blocks":
		d.handleBlocks(ctx, w, r, parts)

	case "ingest-supervoxels":
		d.handleIngest(ctx, w, r)

	case "pseudocolor":
		d.handlePseudocolor(ctx, w, r, parts)

	case "raw", "isotropic":
		d.handleDataRequest(ctx, w, r, parts)

	// endpoints after this must have data instance IndexedLabels = true

	case "lastmod":
		d.handleLabelmod(ctx, w, r, parts)

	case "supervoxels":
		d.handleSupervoxels(ctx, w, r, parts)

	case "supervoxel-sizes":
		d.handleSupervoxelSizes(ctx, w, r, parts)

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

	case "set-nextlabel":
		d.handleSetNextlabel(ctx, w, r, parts)

	case "split-supervoxel":
		d.handleSplitSupervoxel(ctx, w, r, parts)

	case "cleave":
		d.handleCleave(ctx, w, r, parts)

	case "split":
		d.handleSplit(ctx, w, r, parts)

	case "merge":
		d.handleMerge(ctx, w, r, parts)

	case "renumber":
		d.handleRenumber(ctx, w, r)

	case "proximity":
		d.handleProximity(ctx, w, r, parts)

	case "index":
		d.handleIndex(ctx, w, r, parts)

	case "indices":
		d.handleIndices(ctx, w, r)

	case "indices-compressed":
		d.handleIndicesCompressed(ctx, w, r)

	case "mappings":
		d.handleMappings(ctx, w, r)

	case "history":
		d.handleHistory(ctx, w, r, parts)

	case "mutations":
		d.handleMutations(ctx, w, r)

	default:
		server.BadAPIRequest(w, r, d)
	}
	return
}
