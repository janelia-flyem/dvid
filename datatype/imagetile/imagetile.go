/*
	Package imagetile implements DVID support for imagetiles in XY, XZ, and YZ orientation.
	All raw tiles are stored as PNG images that are by default gzipped.  This allows raw
	tile gets to be already compressed at the cost of more expensive uncompression to
	retrieve arbitrary image sizes.
*/
package imagetile

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/imagetile"
	TypeName = "imagetile"
)

const helpMessage = `
API for datatypes derived from imagetile (github.com/janelia-flyem/dvid/datatype/imagetile)
=====================================================================================

Command-line:

$ dvid repo <UUID> new imagetile <data name> <settings...>

	Adds multiresolution XY, XZ, and YZ imagetile from Source to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new imagetile myimagetile source=mygrayscale format=jpg

	Arguments:

	UUID           Hexadecimal string with enough characters to uniquely identify a version node.
	data name      Name of data to create, e.g., "mygrayscale"
	settings       Configuration settings in "key=value" format separated by spaces.

	Configuration Settings (case-insensitive keys)

	Format         "lz4", "jpg", or "png" (default).  In the case of "lz4", decoding/encoding is done at
					  tile request time and is a better choice if you primarily ask for arbitrary sized images
					  (via GET .../raw/... or .../isotropic/...) instead of tiles (via GET .../tile/...)
	Versioned      "true" or "false" (default)
	Source         Name of uint8blk data instance if using the tile "generate" command below.
	Placeholder    Bool ("false", "true", "0", or "1").  Return placeholder tile if missing.


$ dvid node <UUID> <data name> generate [settings]
$ dvid -stdin node <UUID> <data name> generate [settings] < config.json

	Generates multiresolution XY, XZ, and YZ imagetile from Source to repo with specified UUID.
	The resolutions at each scale and the dimensions of the tiles are passed in the configuration
	JSON.  Only integral multiplications of original resolutions are allowed for scale.  If you
	want more sophisticated processing, post the imagetile tiles directly via HTTP.  Note that
	the generated tiles are aligned in a grid having (0,0,0) as a top left corner of a tile, not
	tiles that start from the corner of present data since the data can expand.

	If not tile spec file is used, a default tile spec is generated that will cover the 
	extents of the source data.

	Example:

	$ dvid repo 3f8c myimagetile generate /path/to/config.json
	$ dvid -stdin repo 3f8c myimagetile generate xrange=100,700 planes="yz;0,1" < /path/to/config.json 

	Arguments:

	UUID            Hexadecimal string with enough characters to uniquely identify a version node.
	data name       Name of data to create, e.g., "mygrayscale".
	settings        Optional file name of tile specifications for tile generation.

	Configuration Settings (case-insensitive keys)

	planes          List of one or more planes separated by semicolon.  Each plane can be
					   designated using either axis number ("0,1") or xyz nomenclature ("xy").
					   Example:  planes="0,1;yz"
	zrange			Only render XY tiles containing pixels between this minimum and maximum in z.
	yrange			Only render XZ tiles containing pixels between this minimum and maximum in y.
	xrange			Only render YZ tiles containing pixels between this minimum and maximum in x.
	filename        Filename of JSON file specifying multiscale tile resolutions as below.

	Sample config.json:

	{
		"0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
		"1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
		"2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
		"3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
	}
	
 $ dvid repo <UUID> push <remote DVID address> <settings...>
 
		Push tiles to remote DVID.

		where <settings> are optional "key=value" strings:

		data=<data1>[,<data2>[,<data3>...]]
		
			If supplied, the transmitted data will be limited to the listed
			data instance names.
				
		filter=roi:<roiname>,<uuid>/tile:<plane>,<plane>
        
            Example: filter=roi:seven_column,38af/tile:xy,xz
		
			There are two usable filters for imagetile:
            The "roi" filter is followed by an roiname and a UUID for that ROI.
            The "tile" filter is followed by one or more plane specifications (xy, xz, yz).  
            If omitted all planes are pushed.
		
		transmit=[all | branch | flatten]

			The default transmit "all" sends all versions necessary to 
			make the remote equivalent or a superset of the local repo.
			
			A transmit "flatten" will send just the version specified and
			flatten the key/values so there is no history.
            
            A transmit "branch" will send just the ancestor path of the
            version specified.


	------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info

	Retrieves characteristics of this tile data like the tile size and number of scales present.

	Example: 

	GET <api URL>/node/3f8c/myimagetile/info

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of imagetile data.


GET  <api URL>/node/<UUID>/<data name>/metadata

	Gets the resolution and expected tile sizes for stored tiles.   See the POST action on this endpoint
	for further documentation.

POST <api URL>/node/<UUID>/<data name>/metadata

	Sets the resolution and expected tile sizes for stored tiles.   This should be used in conjunction
	with POST to the tile endpoints to populate an imagetile data instance with externally generated
	data.  For example POST payload, see the sample config.json above in "generate" command line.

	Note that until metadata is set, any call to the "raw" or "isotropic" endpoints will return
	a status code 400 (Bad Request) and a message that the metadata needs to be set.

	Metadata should be JSON in the following format:
	{
		"MinTileCoord": [0, 0, 0],
		"MaxTileCoord": [5, 5, 4],
		"Levels": {
			"0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
			"1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
			"2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
			"3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
		}
	}

	where "MinTileCoord" and "MaxTileCoord" are the minimum and maximum tile coordinates,
	thereby defining the extent of the tiled volume when coupled with level "0" tile sizes.


GET  <api URL>/node/<UUID>/<data name>/tile/<dims>/<scaling>/<tile coord>[?noblanks=true]
POST
	Retrieves or adds tile of named data within a version node.  This GET call should be the fastest
	way to retrieve image data since internally it has already been stored in a pre-computed, optionally
	compression format, whereas arbitrary geometry calls require the DVID server to stitch images
	together.

	The returned image format is dictated by the imagetile encoding.  PNG tiles are returned
	if internal encoding is either lz4 or png.  JPG tiles are returned if internal encoding is JPG.
	The only reason to use lz4 for internal encoding is if the majority of endpoint use for
	the data instance is via the "raw" endpoint where many tiles need to be stitched before
	sending the requested image back.

	Note on POSTs: The data of the body in the POST is assumed to match the data instance's 
	chosen compression and tile sizes.  Currently, no checks are performed to make sure the
	POSTed data meets the specification.

	Example: 

	GET  <api URL>/node/3f8c/myimagetile/tile/xy/0/10_10_20
	POST <api URL>/node/3f8c/myimagetile/tile/xy/0/10_10_20

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
					Slice strings ("xy", "xz", or "yz") are also accepted.
	scaling       Value from 0 (original resolution) to N where each step is downres by 2.
	tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.

  	Query-string options:

  	noblanks	  (only GET) If true, any tile request for tiles outside the currently stored extents
  				  will return a blank image.


GET  <api URL>/node/<UUID>/<data name>/tilekey/<dims>/<scaling>/<tile coord>

	Retrieves the internal key for a tile of named data within a version node.  
	This lets external systems bypass DVID and store tiles directly into immutable stores.

	Returns JSON with "key" key and a hexadecimal string giving binary key.

	Example: 

	GET  <api URL>/node/3f8c/myimagetile/tilekey/xy/0/10_10_20

	Returns:
	{ "key": <hexadecimal string of key> }

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
					Slice strings ("xy", "xz", or "yz") are also accepted.
	scaling       Value from 0 (original resolution) to N where each step is downres by 2.
	tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>]

	Retrieves raw image of named data within a version node using the precomputed imagetile.
	By "raw", we mean that no additional processing is applied based on voxel resolutions
	to make sure the retrieved image has isotropic pixels.  For example, if an XZ image
	is requested and the image volume has X resolution 3 nm and Z resolution 40 nm, the
	returned image will be heavily anisotropic and should be scaled by 40/3 in Y by client.

	Example: 

	GET <api URL>/node/3f8c/myimagetile/raw/xy/512_256/0_0_100/jpg:80

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
					Slice strings ("xy", "xz", or "yz") are also accepted.
					Note that only 2d images are returned for imagetiles.
	size          Size in voxels along each dimension specified in <dims>.
	offset        Gives coordinate of first voxel using dimensionality of data.
	format        "png", "jpg" (default: "png")
					jpg allows lossy quality setting, e.g., "jpg:80"

GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>]

	Retrieves isotropic image of named data within a version node using the precomputed imagetile.
	Additional processing is applied based on voxel resolutions to make sure the retrieved image 
	has isotropic pixels.  For example, if an XZ image is requested and the image volume has 
	X resolution 3 nm and Z resolution 40 nm, the returned image's height will be magnified 40/3
	relative to the raw data.

	Example: 

	GET <api URL>/node/3f8c/myimagetile/isotropic/xy/512_256/0_0_100/jpg:80

	Arguments:

	UUID          Hexadecimal string with enough characters to uniquely identify a version node.
	data name     Name of data to add.
	dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
					Slice strings ("xy", "xz", or "yz") are also accepted.
					Note that only 2d images are returned for imagetiles.
	size          Size in voxels along each dimension specified in <dims>.
	offset        Gives coordinate of first voxel using dimensionality of data.
	format        "png", "jpg" (default: "png")
					jpg allows lossy quality setting, e.g., "jpg:80"

`

var (
	ErrNoMetadataSet = errors.New("Tile metadata has not been POSTed yet.  GET requests require metadata to be POST.")
)

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// Type embeds the datastore's Type to create a unique type with tile functions.
// Refinements of general tile types can be implemented by embedding this type,
// choosing appropriate # of channels and bytes/voxel, overriding functions as
// needed, and calling datastore.Register().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Type struct {
	datastore.Type
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewType() *Type {
	return &Type{
		datastore.Type{
			Name:    "imagetile",
			URL:     "github.com/janelia-flyem/dvid/datatype/imagetile",
			Version: "0.1",
			Requirements: &storage.Requirements{
				Batcher: true,
			},
		},
	}
}

// --- TypeService interface ---

// NewData returns a pointer to new tile data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	// See if we have a valid DataService source
	sourcename, found, err := c.GetString("Source")
	if err != nil {
		return nil, err
	}

	// See if we want placeholder imagetile.
	placeholder, found, err := c.GetBool("Placeholder")
	if err != nil {
		return nil, err
	}

	// Determine encoding for tile storage and this dictates what kind of compression we use.
	encoding, found, err := c.GetString("Format")
	if err != nil {
		return nil, err
	}
	format := PNG
	if found {
		switch strings.ToLower(encoding) {
		case "lz4":
			format = LZ4
		case "png":
			format = PNG
		case "jpg":
			format = JPG
		default:
			return nil, fmt.Errorf("Unknown encoding specified: '%s' (should be 'lz4', 'png', or 'jpg'", encoding)
		}
	}

	// Compression is determined by encoding.  Inform user if there's a discrepancy.
	var compression string
	switch format {
	case LZ4:
		compression = "lz4"
	case PNG:
		compression = "none"
	case JPG:
		compression = "none"
	}
	compressConfig, found, err := c.GetString("Compression")
	if err != nil {
		return nil, err
	}
	if found && strings.ToLower(compressConfig) != compression {
		return nil, fmt.Errorf("Conflict between specified compression '%s' and format '%s'.  Suggest not dictating compression.",
			compressConfig, encoding)
	}
	c.Set("Compression", compression)

	// Initialize the imagetile data
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data: basedata,
		Properties: Properties{
			Source:      dvid.InstanceName(sourcename),
			Placeholder: placeholder,
			Encoding:    format,
		},
	}
	return data, nil
}

func (dtype *Type) Help() string {
	return helpMessage
}

// Scaling describes the scale level where 0 = original data resolution and
// higher levels have been downsampled.
type Scaling uint8

type LevelSpec struct {
	Resolution dvid.NdFloat32
	TileSize   dvid.Point3d
}

func (spec LevelSpec) Duplicate() LevelSpec {
	var out LevelSpec
	out.Resolution = make(dvid.NdFloat32, 3)
	copy(out.Resolution, spec.Resolution)
	out.TileSize = spec.TileSize
	return out
}

// TileScaleSpec is a slice of tile resolution & size for each dimensions.
type TileScaleSpec struct {
	LevelSpec

	levelMag dvid.Point3d // Magnification from this one to the next level.
}

// TileSpec specifies the resolution & size of each dimension at each scale level.
type TileSpec map[Scaling]TileScaleSpec

// MarshalJSON returns the JSON of the imagetile specifications for each scale level.
func (tileSpec TileSpec) MarshalJSON() ([]byte, error) {
	serializable := make(specJSON, len(tileSpec))
	for scaling, levelSpec := range tileSpec {
		key := fmt.Sprintf("%d", scaling)
		serializable[key] = LevelSpec{levelSpec.Resolution, levelSpec.TileSize}
	}
	return json.Marshal(serializable)
}

type specJSON map[string]LevelSpec

// LoadTileSpec loads a TileSpec from JSON data.
// JSON data should look like:
// {
//    "0": { "Resolution": [3.1, 3.1, 40.0], "TileSize": [512, 512, 40] },
//    "1": { "Resolution": [6.2, 6.2, 40.0], "TileSize": [512, 512, 80] },
//    ...
// }
// Each line is a scale with a n-D resolution/voxel and a n-D tile size in voxels.
func LoadTileSpec(jsonBytes []byte) (TileSpec, error) {
	var config specJSON
	err := json.Unmarshal(jsonBytes, &config)
	if err != nil {
		return nil, err
	}
	return parseTileSpec(config)
}

type metadataJSON struct {
	MinTileCoord dvid.Point3d
	MaxTileCoord dvid.Point3d
	Levels       specJSON
}

// SetMetadata loads JSON data giving MinTileCoord, MaxTileCoord, and tile level specifications.
func (d *Data) SetMetadata(uuid dvid.UUID, jsonBytes []byte) error {
	var config metadataJSON
	if err := json.Unmarshal(jsonBytes, &config); err != nil {
		return err
	}
	tileSpec, err := parseTileSpec(config.Levels)
	if err != nil {
		return err
	}

	d.Levels = tileSpec
	d.MinTileCoord = config.MinTileCoord
	d.MaxTileCoord = config.MaxTileCoord
	if err := datastore.SaveDataByUUID(uuid, d); err != nil {
		return err
	}
	return nil
}

func parseTileSpec(config specJSON) (TileSpec, error) {
	// Allocate the tile specs
	numLevels := len(config)
	specs := make(TileSpec, numLevels)
	dvid.Infof("Found %d scaling levels for imagetile specification.\n", numLevels)

	// Store resolution and tile sizes per level.
	var hires, lores float64
	for scaleStr, levelSpec := range config {
		dvid.Infof("scale %s, levelSpec %v\n", scaleStr, levelSpec)
		scaleLevel, err := strconv.Atoi(scaleStr)
		if err != nil {
			return nil, fmt.Errorf("Scaling '%s' needs to be a number for the scale level.", scaleStr)
		}
		if scaleLevel >= numLevels {
			return nil, fmt.Errorf("Tile levels must be consecutive integers from [0,Max]: Got scale level %d > # levels (%d)\n",
				scaleLevel, numLevels)
		}
		specs[Scaling(scaleLevel)] = TileScaleSpec{LevelSpec: levelSpec}
	}

	// Compute the magnification between each level.
	for scaling := Scaling(0); scaling < Scaling(numLevels-1); scaling++ {
		levelSpec, found := specs[scaling]
		if !found {
			return nil, fmt.Errorf("Could not find tile spec for level %d", scaling)
		}
		nextSpec, found := specs[scaling+1]
		if !found {
			return nil, fmt.Errorf("Could not find tile spec for level %d", scaling+1)
		}
		var levelMag dvid.Point3d
		for i, curRes := range levelSpec.Resolution {
			hires = float64(curRes)
			lores = float64(nextSpec.Resolution[i])
			rem := math.Remainder(lores, hires)
			if rem > 0.001 {
				return nil, fmt.Errorf("Resolutions between scale %d and %d aren't integral magnifications!",
					scaling, scaling+1)
			}
			mag := lores / hires
			if mag < 0.99 {
				return nil, fmt.Errorf("A resolution between scale %d and %d actually increases!",
					scaling, scaling+1)
			}
			mag += 0.5
			levelMag[i] = int32(mag)
		}
		levelSpec.levelMag = levelMag
		specs[scaling] = levelSpec
	}
	return specs, nil
}

// --- Tile Data ----

// SourceData is the source of the tile data and should be voxels or voxels-derived data.
type SourceData interface{}

type Format uint8

const (
	LZ4 Format = iota
	PNG
	JPG
)

func (f Format) String() string {
	switch f {
	case LZ4:
		return "lz4"
	case PNG:
		return "png"
	case JPG:
		return "jpeg"
	default:
		return fmt.Sprintf("format %d", f)
	}
}

var DefaultTileSize = dvid.Point3d{512, 512, 512}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Source of the data for these imagetile.  Could be blank if tiles are not generated from
	// an associated grayscale data instance, but were loaded directly from external processing.
	Source dvid.InstanceName

	// MinTileCoord gives the minimum tile coordinate.
	MinTileCoord dvid.Point3d

	// MaxTileCoord gives the maximum tile coordinate.
	MaxTileCoord dvid.Point3d

	// Levels describe the resolution and tile sizes at each level of resolution.
	Levels TileSpec

	// Placeholder, when true (false by default), will generate fake tile images if a tile cannot
	// be found.  This is useful in testing clients.
	Placeholder bool

	// Encoding describes encoding of the stored tile.  See imagetile.Format
	Encoding Format

	// Quality is optional quality of encoding for jpeg, 1-100, higher is better.
	Quality int
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data
	Properties
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties. Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-imagetile data %q", src.DataName())
	}
	d.Properties.copyImmutable(&(d2.Properties))

	// TODO -- Handle mutable data that could be potentially altered by filter.
	d.MinTileCoord = d2.MinTileCoord
	d.MaxTileCoord = d2.MaxTileCoord

	return nil
}

func (p *Properties) copyImmutable(p2 *Properties) {
	p.Source = p2.Source

	p.Levels = make(TileSpec, len(p2.Levels))
	for scale, spec := range p2.Levels {
		p.Levels[scale] = TileScaleSpec{spec.LevelSpec.Duplicate(), spec.levelMag}
	}
	p.Placeholder = p2.Placeholder
	p.Encoding = p2.Encoding
	p.Quality = p2.Quality
}

// Returns the bounds in voxels for a given tile.
func (d *Data) computeVoxelBounds(tileCoord dvid.ChunkPoint3d, plane dvid.DataShape, scale Scaling) (dvid.Extents3d, error) {
	// Get magnification at the given scale of the tile sizes.
	mag := dvid.Point3d{1, 1, 1}
	var tileSize dvid.Point3d
	for s := Scaling(0); s <= scale; s++ {
		spec, found := d.Properties.Levels[s]
		if !found {
			return dvid.Extents3d{}, fmt.Errorf("no tile spec for scale %d", scale)
		}
		tileSize = spec.TileSize.Mult(mag).(dvid.Point3d)
		// mag = mag.Mult(spec.levelMag).(dvid.Point3d)
		// TODO -- figure out why mag level not working for some imagetile instances.
		mag[0] *= 2
		mag[1] *= 2
		mag[2] *= 2
	}
	return dvid.GetTileExtents(tileCoord, plane, tileSize)
}

// DefaultTileSpec returns the default tile spec that will fully cover the source extents and
// scaling 0 uses the original voxel resolutions with each subsequent scale causing a 2x zoom out.
func (d *Data) DefaultTileSpec(uuidStr string) (TileSpec, error) {
	uuid, _, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return nil, err
	}
	source, err := datastore.GetDataByUUIDName(uuid, d.Source)
	if err != nil {
		return nil, err
	}
	var ok bool
	var src *imageblk.Data
	src, ok = source.(*imageblk.Data)
	if !ok {
		return nil, fmt.Errorf("Cannot construct tile spec for non-voxels data: %s", d.Source)
	}

	// Set scaling 0 based on extents and resolution of source.
	//extents := src.Extents()
	resolution := src.Properties.Resolution.VoxelSize

	if len(resolution) != 3 {
		return nil, fmt.Errorf("Cannot construct tile spec for non-3d data: voxel is %d-d",
			len(resolution))
	}

	// Expand min and max points to coincide with full tile boundaries of highest resolution.
	minTileCoord := src.MinPoint.(dvid.Chunkable).Chunk(DefaultTileSize)
	maxTileCoord := src.MaxPoint.(dvid.Chunkable).Chunk(DefaultTileSize)
	minTiledPt := minTileCoord.MinPoint(DefaultTileSize)
	maxTiledPt := maxTileCoord.MaxPoint(DefaultTileSize)
	sizeVolume := maxTiledPt.Sub(minTiledPt).AddScalar(1)

	dvid.Infof("Creating default multiscale tile spec for volume of size %s\n", sizeVolume)

	// For each dimension, calculate the number of scaling levels necessary to cover extent,
	// assuming we use the raw resolution at scaling 0.
	numScales := make([]int, 3)
	var maxScales int
	var dim uint8
	for dim = 0; dim < 3; dim++ {
		numPixels := float64(sizeVolume.Value(dim))
		tileSize := float64(DefaultTileSize.Value(dim))
		if numPixels <= tileSize {
			numScales[dim] = 1
		} else {
			numScales[dim] = int(math.Ceil(math.Log2(numPixels/tileSize))) + 1
		}
		if numScales[dim] > maxScales {
			maxScales = numScales[dim]
		}
	}

	// Initialize the tile level specification
	specs := make(TileSpec, maxScales)
	curRes := resolution
	levelMag := dvid.Point3d{2, 2, 2}
	var scaling Scaling
	for scaling = 0; scaling < Scaling(maxScales); scaling++ {
		for dim = 0; dim < 3; dim++ {
			if scaling >= Scaling(numScales[dim]) {
				levelMag[dim] = 1
			}
		}
		specs[scaling] = TileScaleSpec{
			LevelSpec{curRes, DefaultTileSize},
			levelMag,
		}
		curRes = curRes.MultScalar(2.0)
	}
	return specs, nil
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
	return helpMessage
}

// DoRPC handles the 'generate' command.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "generate" {
		return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	var uuidStr, dataName, cmdStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr)

	// Get the imagetile generation configuration from a file or stdin.
	var err error
	var tileSpec TileSpec
	if request.Input != nil {
		tileSpec, err = LoadTileSpec(request.Input)
		if err != nil {
			return err
		}
	} else {
		config := request.Settings()
		filename, found, err := config.GetString("filename")
		if err != nil {
			return err
		}
		if found {
			configData, err := storage.DataFromFile(filename)
			if err != nil {
				return err
			}
			tileSpec, err = LoadTileSpec(configData)
			if err != nil {
				return err
			}
			dvid.Infof("Using tile spec file: %s\n", filename)
		} else {
			dvid.Infof("Using default tile generation method since no tile spec file was given...\n")
			tileSpec, err = d.DefaultTileSpec(uuidStr)
			if err != nil {
				return err
			}
		}
	}
	reply.Text = fmt.Sprintf("Tiling data instance %q @ node %s...\n", dataName, uuidStr)
	go func() {
		err := d.ConstructTiles(uuidStr, tileSpec, request)
		if err != nil {
			dvid.Errorf("Cannot construct tiles for data instance %q @ node %s: %v\n", dataName, uuidStr, err)
		}
	}()
	return nil
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
	timedLog := dvid.NewTimeLog()

	action := strings.ToLower(r.Method)
	switch action {
	case "get", "post":
		// Acceptable
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
	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API request")
		return
	}

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "metadata":
		switch action {
		case "post":
			jsonBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if err := d.SetMetadata(uuid, jsonBytes); err != nil {
				server.BadRequest(w, r, err)
				return
			}

		case "get":
			if d.Levels == nil || len(d.Levels) == 0 {
				server.BadRequest(w, r, "tile metadata for imagetile %q was not set\n", d.DataName())
				return
			}
			metadata := struct {
				MinTileCoord dvid.Point3d
				MaxTileCoord dvid.Point3d
				Levels       TileSpec
			}{
				d.MinTileCoord,
				d.MaxTileCoord,
				d.Levels,
			}
			jsonBytes, err := json.Marshal(metadata)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
		}
		timedLog.Infof("HTTP %s: metadata (%s)", r.Method, r.URL)

	case "tile":
		switch action {
		case "post":
			err := d.PostTile(ctx, w, r, parts)
			if err != nil {
				server.BadRequest(w, r, "Error in posting tile with URL %q: %v\n", url, err)
				return
			}
		case "get":
			if err := d.ServeTile(ctx, w, r, parts); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
		timedLog.Infof("HTTP %s: tile (%s)", r.Method, r.URL)

	case "tilekey":
		switch action {
		case "get":
			var err error
			var hexkey string
			if hexkey, err = d.getTileKey(ctx, w, r, parts); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"key": "%s"}`, hexkey)
			timedLog.Infof("HTTP %s: tilekey (%s) returns %s", r.Method, r.URL, hexkey)
		default:
			server.BadRequest(w, r, fmt.Errorf("Cannot use HTTP %s for tilekey endpoint", action))
			return
		}

	case "raw", "isotropic":
		if action == "post" {
			server.BadRequest(w, r, "imagetile '%s' can only PUT tiles not images", d.DataName())
			return
		}
		if len(parts) < 7 {
			server.BadRequest(w, r, "%q must be followed by shape/size/offset", parts[3])
			return
		}
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if plane.ShapeDimensions() != 2 {
			server.BadRequest(w, r, "Quadtrees can only return 2d images not %s", plane)
			return
		}
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		source, err := datastore.GetDataByUUIDName(uuid, d.Source)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		src, ok := source.(*imageblk.Data)
		if !ok {
			server.BadRequest(w, r, "Cannot construct imagetile for non-voxels data: %s", d.Source)
			return
		}
		img, err := d.GetImage(ctx, src, slice, parts[3] == "isotropic")
		if err != nil {
			server.BadRequest(w, r, err)
			return
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
		timedLog.Infof("HTTP %s: tile-accelerated %s %s (%s)", r.Method, planeStr, parts[3], r.URL)
	default:
		server.BadAPIRequest(w, r, d)
	}
	return
}

// GetImage returns an image given a 2d orthogonal image description.  Since imagetile tiles
// have precomputed XY, XZ, and YZ orientations, reconstruction of the desired image should
// be much faster than computing the image from voxel blocks.
func (d *Data) GetImage(ctx storage.Context, src *imageblk.Data, geom dvid.Geometry, isotropic bool) (*dvid.Image, error) {
	// Iterate through tiles that intersect our geometry.
	if d.Levels == nil || len(d.Levels) == 0 {
		return nil, ErrNoMetadataSet
	}
	levelSpec := d.Levels[0]
	minSlice, err := dvid.Isotropy2D(src.VoxelSize, geom, isotropic)
	if err != nil {
		return nil, err
	}

	// Create an image of appropriate size and type using source's ExtData creation.
	dstW := minSlice.Size().Value(0)
	dstH := minSlice.Size().Value(1)
	dst, err := src.BlankImage(dstW, dstH)
	if err != nil {
		return nil, err
	}

	// Read each tile that intersects the geometry and store into final image.
	slice := minSlice.DataShape()
	tileW, tileH, err := slice.GetSize2D(levelSpec.TileSize)
	if err != nil {
		return nil, err
	}
	tileSize := dvid.Point2d{tileW, tileH}
	minPtX, minPtY, err := slice.GetSize2D(minSlice.StartPoint())
	if err != nil {
		return nil, err
	}

	wg := new(sync.WaitGroup)
	topLeftGlobal := dvid.Point2d{minPtX, minPtY}
	tilePt := topLeftGlobal.Chunk(tileSize)
	bottomRightGlobal := tilePt.MaxPoint(tileSize).(dvid.Point2d)
	y0 := int32(0)
	y1 := bottomRightGlobal[1] - minPtY + 1
	for y0 < dstH {
		x0 := int32(0)
		x1 := bottomRightGlobal[0] - minPtX + 1
		for x0 < dstW {
			wg.Add(1)
			go func(x0, y0, x1, y1 int32) {
				defer wg.Done()

				// Get this tile from datastore
				tileCoord, err := slice.PlaneToChunkPoint3d(x0, y0, minSlice.StartPoint(), levelSpec.TileSize)
				goImg, err := d.getTileImage(ctx, TileReq{tileCoord, slice, 0})
				if err != nil || goImg == nil {
					return
				}

				// Get tile space coordinate for top left.
				curStart := dvid.Point2d{x0 + minPtX, y0 + minPtY}
				p := curStart.PointInChunk(tileSize)
				ptInTile := image.Point{int(p.Value(0)), int(p.Value(1))}

				// Paste the pertinent rectangle from this tile into our destination.
				r := image.Rect(int(x0), int(y0), int(x1), int(y1))
				draw.Draw(dst.GetDrawable(), r, goImg, ptInTile, draw.Src)
			}(x0, y0, x1, y1)
			x0 = x1
			x1 += tileW
		}
		y0 = y1
		y1 += tileH
	}
	wg.Wait()

	if isotropic {
		dstW := int(geom.Size().Value(0))
		dstH := int(geom.Size().Value(1))
		dst, err = dst.ScaleImage(dstW, dstH)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// PostTile stores a tile.
func (d *Data) PostTile(ctx storage.Context, w http.ResponseWriter, r *http.Request, parts []string) error {
	req, err := d.ParseTileReq(r, parts)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Cannot open imagetile store: %v\n", err)
	}
	tk, err := NewTKeyByTileReq(req)
	if err != nil {
		return err
	}
	return db.Put(ctx, tk, data)
}

// ServeTile returns a tile with appropriate Content-Type set.
func (d *Data) ServeTile(ctx storage.Context, w http.ResponseWriter, r *http.Request, parts []string) error {

	if d.Levels == nil || len(d.Levels) == 0 {
		return ErrNoMetadataSet
	}
	tileReq, err := d.ParseTileReq(r, parts)

	queryStrings := r.URL.Query()
	noblanksStr := dvid.InstanceName(queryStrings.Get("noblanks"))
	var noblanks bool
	if noblanksStr == "true" {
		noblanks = true
	}
	var formatStr string
	if len(parts) >= 8 {
		formatStr = parts[7]
	}

	data, err := d.getTileData(ctx, tileReq)
	if err != nil {
		server.BadRequest(w, r, err)
		return err
	}
	if len(data) == 0 {
		if noblanks {
			http.NotFound(w, r)
			return nil
		}
		img, err := d.getBlankTileImage(tileReq)
		if err != nil {
			return err
		}
		return dvid.WriteImageHttp(w, img, formatStr)
	}

	switch d.Encoding {
	case LZ4:
		var img dvid.Image
		if err := img.Deserialize(data); err != nil {
			return err
		}
		data, err = img.GetPNG()
		w.Header().Set("Content-type", "image/png")
	case PNG:
		w.Header().Set("Content-type", "image/png")
	case JPG:
		w.Header().Set("Content-type", "image/jpeg")
	}
	if err != nil {
		server.BadRequest(w, r, err)
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

// getTileKey returns the internal key as a hexadecimal string
func (d *Data) getTileKey(ctx storage.Context, w http.ResponseWriter, r *http.Request, parts []string) (string, error) {
	req, err := d.ParseTileReq(r, parts)
	if err != nil {
		return "", err
	}
	tk, err := NewTKeyByTileReq(req)
	if err != nil {
		return "", err
	}
	key := ctx.ConstructKey(tk)
	return fmt.Sprintf("%x", key), nil
}

// getTileImage returns a 2d tile image or a placeholder, useful for further stitching before
// delivery of a final image.
func (d *Data) getTileImage(ctx storage.Context, req TileReq) (image.Image, error) {
	if d.Levels == nil || len(d.Levels) == 0 {
		return nil, ErrNoMetadataSet
	}
	data, err := d.getTileData(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		if d.Placeholder {
			if req.scale < 0 || req.scale >= Scaling(len(d.Levels)) {
				return nil, fmt.Errorf("Could not find tile specification at given scale %d", req.scale)
			}
			message := fmt.Sprintf("%s Tile coord %s @ scale %d", req.plane, req.tile, req.scale)
			return dvid.PlaceholderImage(req.plane, d.Levels[req.scale].TileSize, message)
		}
		return nil, nil // Not found
	}

	var goImg image.Image
	switch d.Encoding {
	case LZ4:
		var img dvid.Image
		err = img.Deserialize(data)
		if err != nil {
			return nil, err
		}
		goImg = img.Get()
	case PNG:
		pngBuffer := bytes.NewBuffer(data)
		goImg, err = png.Decode(pngBuffer)
	case JPG:
		jpgBuffer := bytes.NewBuffer(data)
		goImg, err = jpeg.Decode(jpgBuffer)
	default:
		return nil, fmt.Errorf("Unknown tile encoding: %s", d.Encoding)
	}
	return goImg, err
}

// getTileData returns 2d tile data straight from storage without decoding.
func (d *Data) getTileData(ctx storage.Context, req TileReq) ([]byte, error) {
	// Don't allow negative tile coordinates for now.
	// TODO: Fully check negative coordinates for both tiles and voxels.
	if req.tile.Value(0) < 0 || req.tile.Value(1) < 0 || req.tile.Value(2) < 0 {
		return nil, nil
	}

	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// Retrieve the tile data from datastore
	tk, err := NewTKeyByTileReq(req)
	if err != nil {
		return nil, err
	}
	data, err := db.Get(ctx, tk)
	if err != nil {
		return nil, fmt.Errorf("Error trying to GET from datastore: %v", err)
	}
	return data, nil
}

// getBlankTileData returns zero 2d tile image.
func (d *Data) getBlankTileImage(req TileReq) (image.Image, error) {
	levelSpec, found := d.Levels[req.scale]
	if !found {
		return nil, fmt.Errorf("Could not extract tiles for unspecified scale level %d", req.scale)
	}
	tileW, tileH, err := req.plane.GetSize2D(levelSpec.TileSize)
	if err != nil {
		return nil, err
	}
	img := image.NewGray(image.Rect(0, 0, int(tileW), int(tileH)))
	return img, nil
}

// pow2 returns the power of 2 with the passed exponent.
func pow2(exp uint8) int {
	pow := 1
	for i := uint8(1); i <= exp; i++ {
		pow *= 2
	}
	return pow
}

// log2 returns the power of 2 necessary to cover the given value.
func log2(value int32) uint8 {
	var exp uint8
	pow := int32(1)
	for {
		if pow >= value {
			return exp
		}
		pow *= 2
		exp++
	}
}

type outFunc func(TileReq, *dvid.Image) error

// Construct all tiles for an image with offset and send to out function.  extractTiles assumes
// the image and offset are in the XY plane.
func (d *Data) extractTiles(v *imageblk.Voxels, offset dvid.Point, scale Scaling, outF outFunc) error {
	if d.Levels == nil || scale < 0 || scale >= Scaling(len(d.Levels)) {
		return fmt.Errorf("Bad scaling level specified: %d", scale)
	}
	levelSpec, found := d.Levels[scale]
	if !found {
		return fmt.Errorf("No scaling specs available for scaling level %d", scale)
	}
	srcW := v.Size().Value(0)
	srcH := v.Size().Value(1)

	tileW, tileH, err := v.DataShape().GetSize2D(levelSpec.TileSize)
	if err != nil {
		return err
	}

	// Split image into tiles and store into datastore.
	src, err := v.GetImage2d()
	if err != nil {
		return err
	}
	var x0, y0, x1, y1 int32
	y1 = tileH
	for y0 = 0; y0 < srcH; y0 += tileH {
		x1 = tileW
		for x0 = 0; x0 < srcW; x0 += tileW {
			tileRect := image.Rect(int(x0), int(y0), int(x1), int(y1))
			tile, err := src.SubImage(tileRect)
			if err != nil {
				return err
			}
			tileCoord, err := v.DataShape().PlaneToChunkPoint3d(x0, y0, offset, levelSpec.TileSize)
			// fmt.Printf("Tile Coord: %s > %s\n", tileCoord, tileRect)
			req := NewTileReq(tileCoord, v.DataShape(), scale)
			if err = outF(req, tile); err != nil {
				return err
			}
			x1 += tileW
		}
		y1 += tileH
	}
	return nil
}

// Returns function that stores a tile as an optionally compressed PNG image.
func (d *Data) putTileFunc(versionID dvid.VersionID) (outFunc, error) {
	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("Cannot open imagetile store: %v\n", err)
	}
	ctx := datastore.NewVersionedCtx(d, versionID)

	return func(req TileReq, tile *dvid.Image) error {
		var err error
		var data []byte

		switch d.Encoding {
		case LZ4:
			var compression dvid.Compression
			compression, err = dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
			if err != nil {
				return err
			}
			data, err = tile.Serialize(compression, d.Checksum())
		case PNG:
			data, err = tile.GetPNG()
		case JPG:
			data, err = tile.GetJPEG(d.Quality)
		}
		if err != nil {
			return err
		}
		var tk storage.TKey
		tk, err = NewTKeyByTileReq(req)
		if err != nil {
			return err
		}
		return db.Put(ctx, tk, data)
	}, nil
}

func (d *Data) ConstructTiles(uuidStr string, tileSpec TileSpec, request datastore.Request) error {
	config := request.Settings()
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	if err = datastore.AddToNodeLog(uuid, []string{request.Command.String()}); err != nil {
		return err
	}

	source, err := datastore.GetDataByUUIDName(uuid, d.Source)
	if err != nil {
		return fmt.Errorf("Cannot get source %q for %q tile construction: %v", d.Source, d.DataName(), err)
	}
	src, ok := source.(*imageblk.Data)
	if !ok {
		return fmt.Errorf("Cannot construct imagetile for non-voxels data: %s", d.Source)
	}

	// Get size of tile at lowest resolution.
	lastLevel := Scaling(len(tileSpec) - 1)
	loresSpec, found := tileSpec[lastLevel]
	if !found {
		return fmt.Errorf("Illegal tile spec.  Should have levels 0 to absent %d.", lastLevel)
	}
	var loresSize [3]float64
	for i := 0; i < 3; i++ {
		loresSize[i] = float64(loresSpec.Resolution[i]) * float64(DefaultTileSize[i])
	}
	loresMag := dvid.Point3d{1, 1, 1}
	for i := Scaling(0); i < lastLevel; i++ {
		levelMag := tileSpec[i].levelMag
		loresMag[0] *= levelMag[0]
		loresMag[1] *= levelMag[1]
		loresMag[2] *= levelMag[2]
	}

	// Get min and max points in terms of distance.
	var minPtDist, maxPtDist [3]float64
	for i := uint8(0); i < 3; i++ {
		minPtDist[i] = float64(src.MinPoint.Value(i)) * float64(src.VoxelSize[i])
		maxPtDist[i] = float64(src.MaxPoint.Value(i)) * float64(src.VoxelSize[i])
	}

	// Adjust min and max points for the tileable surface at lowest resolution.
	var minTiledPt, maxTiledPt dvid.Point3d
	for i := 0; i < 3; i++ {
		minInt, _ := math.Modf(minPtDist[i] / loresSize[i])
		maxInt, _ := math.Modf(maxPtDist[i] / loresSize[i])
		d.MinTileCoord[i] = int32(minInt)
		d.MaxTileCoord[i] = int32(maxInt)
		minTiledPt[i] = d.MinTileCoord[i] * DefaultTileSize[i] * loresMag[i]
		maxTiledPt[i] = (d.MaxTileCoord[i]+1)*DefaultTileSize[i]*loresMag[i] - 1
	}
	sizeVolume := maxTiledPt.Sub(minTiledPt).AddScalar(1)

	// Save the current tile specification
	d.Levels = tileSpec
	if err := datastore.SaveDataByUUID(uuid, d); err != nil {
		return err
	}

	// Setup swappable ExtData buffers (the stitched slices) so we can be generating tiles
	// at same time we are reading and stitching them.
	var bufferLock [2]sync.Mutex
	var sliceBuffers [2]*imageblk.Voxels
	var bufferNum int

	// Get the planes we should tile.
	planes, err := config.GetShapes("planes", ";")
	if err != nil {
		return err
	}
	if planes == nil {
		// If no planes are specified, construct imagetile for 3 orthogonal planes.
		planes = []dvid.DataShape{dvid.XY, dvid.XZ, dvid.YZ}
	}

	outF, err := d.putTileFunc(versionID)
	if err != nil {
		return err
	}

	// Get bounds for tiling if specified
	minx, maxx, err := config.GetRange("xrange", ",")
	if err != nil {
		return err
	}
	miny, maxy, err := config.GetRange("yrange", ",")
	if err != nil {
		return err
	}
	minz, maxz, err := config.GetRange("zrange", ",")
	if err != nil {
		return err
	}

	// sort the tile spec keys to iterate from highest to lowest resolution
	var sortedKeys []int
	for scaling, _ := range tileSpec {
		sortedKeys = append(sortedKeys, int(scaling))
	}
	sort.Ints(sortedKeys)

	for _, plane := range planes {
		timedLog := dvid.NewTimeLog()
		offset := minTiledPt.Duplicate()

		switch {

		case plane.Equals(dvid.XY):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Debugf("Tiling XY image %d x %d pixels\n", width, height)
			z0 := src.MinPoint.Value(2)
			z1 := src.MaxPoint.Value(2)
			if minz != nil && z0 < *minz {
				z0 = *minz
			}
			if maxz != nil && z1 > *maxz {
				z1 = *maxz
			}
			for z := z0; z <= z1; z++ {
				server.BlockOnInteractiveRequests("imagetile.ConstructTiles [xy]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{2: z})
				slice, err := dvid.NewOrthogSlice(dvid.XY, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewVoxels(slice, nil)
				if err != nil {
					return err
				}
				if err = src.GetVoxels(versionID, sliceBuffers[bufferNum], ""); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for _, key := range sortedKeys {
						scaling := Scaling(key)
						levelSpec := tileSpec[scaling]
						if err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %v\n", err)
								return
							}
						}
					}
					timedLog.Debugf("Tiled XY Tile using buffer %d", bufferNum)
				}(bufferNum, offset)

				sliceLog.Infof("Read XY Tile @ Z = %d, now tiling...", z)
				bufferNum = (bufferNum + 1) % 2
			}
			timedLog.Infof("Total time to generate XY Tiles")

		case plane.Equals(dvid.XZ):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Debugf("Tiling XZ image %d x %d pixels\n", width, height)
			y0 := src.MinPoint.Value(1)
			y1 := src.MaxPoint.Value(1)
			if miny != nil && y0 < *miny {
				y0 = *miny
			}
			if maxy != nil && y1 > *maxy {
				y1 = *maxy
			}
			for y := y0; y <= y1; y++ {
				server.BlockOnInteractiveRequests("imagetile.ConstructTiles [xz]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{1: y})
				slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewVoxels(slice, nil)
				if err != nil {
					return err
				}
				if err = src.GetVoxels(versionID, sliceBuffers[bufferNum], ""); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for _, key := range sortedKeys {
						scaling := Scaling(key)
						levelSpec := tileSpec[scaling]
						if err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %v\n", err)
								return
							}
						}
					}
					timedLog.Debugf("Tiled XZ Tile using buffer %d", bufferNum)
				}(bufferNum, offset)

				sliceLog.Infof("Read XZ Tile @ Y = %d, now tiling...", y)
				bufferNum = (bufferNum + 1) % 2
			}
			timedLog.Infof("Total time to generate XZ Tiles")

		case plane.Equals(dvid.YZ):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Debugf("Tiling YZ image %d x %d pixels\n", width, height)
			x0 := src.MinPoint.Value(0)
			x1 := src.MaxPoint.Value(0)
			if minx != nil && x0 < *minx {
				x0 = *minx
			}
			if maxz != nil && x1 > *maxx {
				x1 = *maxx
			}
			for x := x0; x <= x1; x++ {
				server.BlockOnInteractiveRequests("imagetile.ConstructTiles [yz]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{0: x})
				slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewVoxels(slice, nil)
				if err != nil {
					return err
				}
				if err = src.GetVoxels(versionID, sliceBuffers[bufferNum], ""); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for _, key := range sortedKeys {
						scaling := Scaling(key)
						levelSpec := tileSpec[scaling]
						outF, err := d.putTileFunc(versionID)
						if err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %v\n", err)
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %v\n", err)
								return
							}
						}
					}
					timedLog.Debugf("Tiled YZ Tile using buffer %d", bufferNum)
				}(bufferNum, offset)

				sliceLog.Debugf("Read YZ Tile @ X = %d, now tiling...", x)
				bufferNum = (bufferNum + 1) % 2
			}
			timedLog.Infof("Total time to generate YZ Tiles")

		default:
			dvid.Infof("Skipping request to tile '%s'.  Unsupported.", plane)
		}
	}
	return nil
}
