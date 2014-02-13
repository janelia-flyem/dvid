/*
	Package quadtree implements DVID support for quadtrees in XY, XZ, and YZ orientation.
*/
package quadtree

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/quadtree"
)

var (
	Compression dvid.Compression = dvid.LZ4
)

const HelpMessage = `
API for datatypes derived from quadtree (github.com/janelia-flyem/dvid/datatype/quadtree)
=====================================================================================

Command-line:

$ dvid dataset <UUID> new quadtree <data name> <settings...>

	Adds multiresolution XY, XZ, and YZ quadtree from Source to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c new quadtree myquadtree source=mygrayscale versioned=true

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    Source         Name of data source (required)
    TileSize       Size in pixels  (default: %s)
    Placeholder    Bool ("false", "true", "0", or "1").  Return placeholder tile if missing.


$ dvid node <UUID> <data name> generate <config JSON file name> <settings...>
$ dvid -stdin node <UUID> <data name> generate <settings...> < config.json

	Generates multiresolution XY, XZ, and YZ quadtree from Source to dataset with specified UUID.
	The resolutions at each scale and the dimensions of the tiles are passed in the configuration
	JSON.  Only integral multiplications of original resolutions are allowed for scale.  If you
	want more sophisticated processing, post the quadtree tiles directly via HTTP.

	Example:

	$ dvid dataset 3f8c myquadtree generate /path/to/config.json
	$ dvid -stdin dataset 3f8c myquadtree generate planes=yz;0,1 < /path/to/config.json 

    Arguments:

    UUID            Hexidecimal string with enough characters to uniquely identify a version node.
    data name       Name of data to create, e.g., "mygrayscale".
    settings        Optional specification of tiles to generate.

    Configuration Settings (case-insensitive keys)

    planes          List of one or more planes separated by semicolon.  Each plane can be
                       designated using either axis number ("0,1") or xyz nomenclature ("xy").
                       Example:  planes=0,1;yz

    ------------------

HTTP API (Level 2 REST):

GET  /api/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  /api/node/<UUID>/<data name>/info

    Retrieves characteristics of this tile data like the tile size and number of scales present.

    Example: 

    GET /api/node/3f8c/myquadtree/info

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of quadtree data.


GET  /api/node/<UUID>/<data name>/tile/<dims>/<scaling>/<tile coord>[/<format>]
(TODO) POST
    Retrieves tile of named data within a version node.

    Example: 

    GET /api/node/3f8c/myquadtree/tile/xy/0/10_10_20/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    scaling       Value from 0 (original resolution) to N where each step is downres by 2.
    tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"


GET  /api/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>]

    Retrieves raw image of named data within a version node using the precomputed quadtree.
    By "raw", we mean that no additional processing is applied based on voxel resolutions
    to make sure the retrieved image has isotropic pixels.  For example, if an XZ image
    is requested and the image volume has X resolution 3 nm and Z resolution 40 nm, the
    returned image will be heavily anisotropic and should be scaled by 40/3 in Y by client.

    Example: 

    GET /api/node/3f8c/myquadtree/raw/xy/512_256/0_0_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Note that only 2d images are returned for quadtrees.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

GET  /api/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>]

    Retrieves isotropic image of named data within a version node using the precomputed quadtree.
    Additional processing is applied based on voxel resolutions to make sure the retrieved image 
    has isotropic pixels.  For example, if an XZ image is requested and the image volume has 
    X resolution 3 nm and Z resolution 40 nm, the returned image's height will be magnified 40/3
    relative to the raw data.

    Example: 

    GET /api/node/3f8c/myquadtree/isotropic/xy/512_256/0_0_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Note that only 2d images are returned for quadtrees.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

`

func init() {
	quadtree := NewDatatype()
	quadtree.DatatypeID = &datastore.DatatypeID{
		Name:    "quadtree",
		Url:     "github.com/janelia-flyem/dvid/datatype/quadtree",
		Version: "0.1",
	}
	datastore.RegisterDatatype(quadtree)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&IndexTile{})
}

// Scaling describes the scale level where 0 = original data resolution and
// higher levels have been downsampled.
type Scaling uint8

type LevelSpec struct {
	Resolution dvid.NdFloat32
	TileSize   dvid.Point3d
}

// TileScaleSpec is a slice of tile resolution & size for each dimensions.
type TileScaleSpec struct {
	LevelSpec

	levelMag dvid.Point3d // Magnification from this one to the next level.
}

// TileSpec specifies the resolution & size of each dimension at each scale level.
type TileSpec map[Scaling]TileScaleSpec

// MarshalJSON returns the JSON of the quadtree specifications for each scale level.
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
func LoadTileSpec(data []byte) (TileSpec, error) {
	var config specJSON
	err := json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	// Allocate the tile specs
	specs := make(TileSpec, len(config))
	dvid.Log(dvid.Debug, "Found %d scaling levels for quadtree specification.\n", len(config))

	// Store resolution and tile sizes per level.
	firstLevel := true
	var scaling Scaling
	var hires, lores float64
	for scaleStr, levelSpec := range config {
		scaleLevel, err := strconv.Atoi(scaleStr)
		if err != nil {
			return nil, fmt.Errorf("Scaling '%s' needs to be a number for the scale level.", scaleStr)
		}
		if firstLevel {
			if scaleLevel != 0 {
				return nil, fmt.Errorf("Tile levels should start with '0' then specify '1', etc.")
			}
			firstLevel = false
		} else {
			if int(scaling+1) != scaleLevel {
				return nil, fmt.Errorf("Tile levels need to be incremental.  Jumps from %d to %d!",
					scaling, scaleLevel)
			}
		}
		scaling = Scaling(scaleLevel)
		specs[scaling] = TileScaleSpec{LevelSpec: levelSpec}
	}

	// Compute the magnification between each level.
	for scaling, levelSpec := range specs {
		if int(scaling+1) <= len(specs)-1 {
			nextSpec := specs[scaling+1]
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
	}
	return specs, nil
}

func getSourceVoxels(uuid dvid.UUID, name dvid.DataString) (*voxels.Data, error) {
	service := server.DatastoreService()
	source, err := service.DataService(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*voxels.Data)
	if !ok {
		return nil, fmt.Errorf("Cannot construct quadtree for non-voxels data: %s", name)
	}
	return data, nil
}

// Datatype embeds the datastore's Datatype to create a unique type
// with tile functions.  Refinements of general tile types can be implemented
// by embedding this type, choosing appropriate # of channels and bytes/voxel,
// overriding functions as needed, and calling datastore.RegisterDatatype().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Datatype struct {
	datastore.Datatype
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewDatatype() (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to new tile data with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, config dvid.Config) (
	datastore.DataService, error) {

	// Make sure we have a valid DataService source
	name, found, err := config.GetString("Source")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make quadtree data without valid 'Source' setting.")
	}
	sourcename := dvid.DataString(name)

	// See if we want placeholder quadtree.
	placeholder, found, err := config.GetBool("Placeholder")
	if err != nil {
		return nil, err
	}

	// Initialize the quadtree data
	basedata, err := datastore.NewDataService(id, dtype, config)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data:        basedata,
		Source:      sourcename,
		Placeholder: placeholder,
	}
	return data, nil
}

func (dtype *Datatype) Help() string {
	return HelpMessage
}

// --- Tile Data ----

// SourceData is the source of the tile data and should be voxels or voxels-derived data.
type SourceData interface{}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data

	// Source of the data for these quadtree.
	Source dvid.DataString

	// Levels describe the resolution and tile sizes at each level of resolution.
	Levels TileSpec

	// Placeholder, when true (false by default), will generate fake tile images if a tile cannot
	// be found.  This is useful in testing clients.
	Placeholder bool
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// --- DataService interface ---

// DoRPC handles the 'generate' command.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "generate" {
		return d.UnknownCommand(request)
	}
	var uuidStr, dataName, cmdStr string
	filenames := request.CommandArgs(1, &uuidStr, &dataName, &cmdStr)

	// Get the quadtree generation configuration from a file or stdin.
	var configData []byte
	if request.Input != nil {
		configData = request.Input
	} else {
		if len(filenames) == 0 {
			return fmt.Errorf("Must specify either a configuration JSON file name or use -stdin")
		}
		var err error
		configData, err = storage.DataFromFile(filenames[0])
		if err != nil {
			return err
		}
	}
	tileSpec, err := LoadTileSpec(configData)
	if err != nil {
		return err
	}
	return d.ConstructTiles(uuidStr, tileSpec, request.Settings())
}

// DoHTTP handles all incoming HTTP requests for this data.
func (d *Data) DoHTTP(uuid dvid.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	switch action {
	case "get", "post":
		// Acceptable
	default:
		return fmt.Errorf("Can only handle GET or POST HTTP verbs")
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")

	// Get the running datastore service from this DVID instance.
	service := server.DatastoreService()

	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		return err
	}

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil
	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil
	case "tile":
		planeStr, scalingStr, coordStr := parts[4], parts[5], parts[6]
		if action == "post" {
			return fmt.Errorf("DVID does not yet support POST of quadtree")
		} else {
			img, err := d.GetTile(versionID, planeStr, scalingStr, coordStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			if img == nil {
				http.NotFound(w, r)
				return nil
			}
			var formatStr string
			if len(parts) >= 8 {
				formatStr = parts[7]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img, formatStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return err
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: tile %s", r.Method, planeStr)
		}

	case "raw", "anisotropic":
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			return err
		}
		if plane.ShapeDimensions() != 2 {
			return fmt.Errorf("Quadtrees can only return 2d images not %s", plane)
		}
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			return err
		}
		img, err := d.GetImage(uuid, slice, parts[3])
		if err != nil {
			return err
		}
		var formatStr string
		if len(parts) >= 8 {
			formatStr = parts[7]
		}
		err = dvid.WriteImageHttp(w, img, formatStr)
		if err != nil {
			return err
		}
	default:
		err = fmt.Errorf("Illegal request for quadtree data.  See 'help' for REST API")
	}
	if err != nil {
		server.BadRequest(w, r, err.Error())
		return err
	}
	return nil
}

// GetImage returns an image given a 2d orthogonal image description.  Since quadtrees
// have precomputed XY, XZ, and YZ orientations, reconstruction of the desired image should
// be much faster than computing the image from voxel blocks.
// The 'proc' option should be either the string "raw" or "isotropic".  If the latter,
// the returned image will be processed to be isotropic using the data's resolution.
func (d *Data) GetImage(uuid dvid.UUID, slice dvid.Geometry, proc string) (image.Image, error) {
	/*
		// Iterate through tiles that intersect our geometry.
		levelSpec, found := d.Levels[0]
		if !found {
			return nil, fmt.Errorf("%s has no specification for tiles at highest resolution",
				d.DataName())
		}
		src, err := getSourceVoxels(uuid, d.Source)
		if err != nil {
			return nil, err
		}
		_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
		if err != nil {
			return nil, err
		}
		dstW := slice.Size().Value(0)
		dstH := slice.Size().Value(1)

		// Create an image of appropriate size and type using source's ExtHandler creation.
		v, err := src.NewExtHandler(slice, nil)
		if err != nil {
			return nil, err
		}

		// Read each tile that intersects the geometry and store into final image.
		tileW, tileH, err := slice.DataShape().GetSize2D(levelSpec.TileSize)
		if err != nil {
			return nil, err
		}

		// Out of the stitched tiles, extract sub-image of interest.

		// If isotropic pixels are requested, scale by the voxel resolutions.

		return v.GoImage()
	*/
	return nil, nil
}

// GetTile retrieves a tile.
func (d *Data) GetTile(versionID dvid.VersionLocalID, planeStr, scalingStr, coordStr string) (
	image.Image, error) {

	if d.Levels == nil {
		return nil, fmt.Errorf("Tiles have not been generated.")
	}

	db, err := server.KeyValueGetter()
	if err != nil {
		return nil, err
	}

	// Construct the index for this tile
	plane := dvid.DataShapeString(planeStr)
	shape, err := plane.DataShape()
	if err != nil {
		return nil, fmt.Errorf("Illegal tile plane: %s (%s)", planeStr, err.Error())
	}
	scaling, err := strconv.ParseUint(scalingStr, 10, 8)
	if err != nil {
		return nil, fmt.Errorf("Illegal tile scale: %s (%s)", scalingStr, err.Error())
	}
	tileCoord, err := dvid.StringToPoint(coordStr, "_")
	if err != nil {
		return nil, fmt.Errorf("Illegal tile coordinate: %s (%s)", coordStr, err.Error())
	}
	indexZYX := dvid.IndexZYX{tileCoord.Value(0), tileCoord.Value(1), tileCoord.Value(2)}
	index := &IndexTile{indexZYX, shape, Scaling(scaling)}

	// Retrieve the tile from datastore
	key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	data, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Error trying to GET from datastore: %s", err.Error())
	}
	if data == nil {
		if d.Placeholder {
			scaleSpec, ok := d.Levels[Scaling(scaling)]
			if !ok {
				return nil, fmt.Errorf("Could not find tiles at given scale %d", scaling)
			}
			message := fmt.Sprintf("%s Tile coord %s @ scale %d", shape, tileCoord, scaling)
			return dvid.PlaceholderImage(shape, scaleSpec.TileSize, message)
		}
		return nil, nil // Not found
	}
	//fmt.Printf("Retrieved tile for key %s: %d bytes\n", key, len(data))
	img := new(dvid.Image)
	err = img.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("Error deserializing tile: %s", err.Error())
	}
	return img.Get(), nil
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

type outFunc func(index IndexTile, img *dvid.Image) error

// Construct all tiles for an image with offset and send to out function.  extractTiles assumes
// the image and offset are in the XY plane.
func (d *Data) extractTiles(v voxels.ExtHandler, offset dvid.Point, scaling Scaling, outF outFunc) error {

	levelSpec, found := d.Levels[scaling]
	if !found {
		return fmt.Errorf("Could not extract tiles for unspecified scale level %d", scaling)
	}
	srcW := v.Size().Value(0)
	srcH := v.Size().Value(1)

	tileW, tileH, err := v.DataShape().GetSize2D(levelSpec.TileSize)
	if err != nil {
		return err
	}

	// Split image into tiles and store into datastore.
	src := new(dvid.Image)
	img, err := v.GoImage()
	if err != nil {
		return err
	}
	src.Set(img)
	var x0, y0, x1, y1 int32
	y1 = tileH - 1
	for y0 = 0; y0 < srcH; y0 += tileH {
		x1 = tileW - 1
		for x0 = 0; x0 < srcW; x0 += tileW {
			tileRect := image.Rect(int(x0), int(y0), int(x1), int(y1))
			tile, err := src.SubImage(tileRect)
			if err != nil {
				return err
			}
			tileCoord, err := v.DataShape().PlaneToChunkPoint3d(x0, y0, offset, levelSpec.TileSize)
			// fmt.Printf("Tile Coord: %s > %s\n", tileCoord, tileRect)
			tileIndex := NewIndexTile(dvid.IndexZYX(tileCoord), v.DataShape(), scaling)
			err = outF(tileIndex, tile)
			if err != nil {
				return err
			}
			x1 += tileW
		}
		y1 += tileH
	}
	return nil
}

func (d *Data) getXYPutFunc(versionID dvid.VersionLocalID, z int32) (outFunc, error) {
	db, err := server.KeyValueSetter()
	if err != nil {
		return nil, err
	}
	return func(index IndexTile, tile *dvid.Image) error {
		serialization, err := tile.Serialize(Compression, dvid.CRC32)
		if err != nil {
			return err
		}
		key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
		return db.Put(key, serialization)
	}, nil
}

func (d *Data) getXZPutFunc(versionID dvid.VersionLocalID, y int32) (outFunc, error) {
	db, err := server.KeyValueSetter()
	if err != nil {
		return nil, err
	}
	return func(index IndexTile, tile *dvid.Image) error {
		serialization, err := tile.Serialize(Compression, dvid.CRC32)
		if err != nil {
			return err
		}
		key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
		return db.Put(key, serialization)
	}, nil
}

func (d *Data) getYZPutFunc(versionID dvid.VersionLocalID, x int32) (outFunc, error) {
	db, err := server.KeyValueSetter()
	if err != nil {
		return nil, err
	}
	return func(index IndexTile, tile *dvid.Image) error {
		serialization, err := tile.Serialize(Compression, dvid.CRC32)
		if err != nil {
			return err
		}
		key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
		return db.Put(key, serialization)
	}, nil
}

func (d *Data) ConstructTiles(uuidStr string, tileSpec TileSpec, config dvid.Config) error {

	// Save the current tile specification
	service := server.DatastoreService()
	uuid, _, versionID, err := service.NodeIDFromString(uuidStr)
	if err != nil {
		return err
	}
	d.Levels = tileSpec
	if err := service.SaveDataset(uuid); err != nil {
		return err
	}
	src, err := getSourceVoxels(uuid, d.Source)
	if err != nil {
		return err
	}

	// Expand min and max points to coincide with full tile boundaries of highest resolution.
	hiresSpec := tileSpec[Scaling(0)]
	minTileCoord := src.MinPoint.(dvid.Chunkable).Chunk(hiresSpec.TileSize)
	maxTileCoord := src.MaxPoint.(dvid.Chunkable).Chunk(hiresSpec.TileSize)
	minPt := minTileCoord.MinPoint(hiresSpec.TileSize)
	maxPt := maxTileCoord.MaxPoint(hiresSpec.TileSize)
	sizeVolume := maxPt.Sub(minPt).AddScalar(1)

	// Get the planes we should tile.
	planes, err := config.GetShapes("planes", ";")
	if planes == nil {
		// If no planes are specified, construct quadtree for 3 orthogonal planes.
		planes = []dvid.DataShape{dvid.XY, dvid.XZ, dvid.YZ}
	}

	for _, plane := range planes {
		startTime := time.Now()
		offset := minPt.Duplicate()

		switch {

		case plane.Equals(dvid.XY):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Log(dvid.Debug, "Tiling XY image %d x %d pixels\n", width, height)
			for z := src.MinPoint.Value(2); z <= src.MaxPoint.Value(2); z++ {
				sliceTime := time.Now()
				offset = offset.Modify(map[uint8]int32{2: z})
				slice, err := dvid.NewOrthogSlice(dvid.XY, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				v, err := src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.SetVoxels(uuid, src, v); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				for scaling, levelSpec := range tileSpec {
					outF, err := d.getXYPutFunc(versionID, z)
					if err != nil {
						return err
					}
					if err := d.extractTiles(v, offset, scaling, outF); err != nil {
						return err
					}
					if int(scaling) < len(tileSpec)-1 {
						if err := v.DownRes(levelSpec.levelMag); err != nil {
							return err
						}
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "XY Tile @ Z = %d", z)
			}
			dvid.ElapsedTime(dvid.Normal, startTime, "Total time to generate XY Tiles")

		case plane.Equals(dvid.XZ):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Log(dvid.Debug, "Tiling XZ image %d x %d pixels\n", width, height)
			for y := src.MinPoint.Value(1); y <= src.MaxPoint.Value(1); y++ {
				sliceTime := time.Now()
				offset = offset.Modify(map[uint8]int32{1: y})
				slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				v, err := src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.SetVoxels(uuid, src, v); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				for scaling, levelSpec := range tileSpec {
					outF, err := d.getXZPutFunc(versionID, y)
					if err != nil {
						return err
					}
					if err := d.extractTiles(v, offset, scaling, outF); err != nil {
						return err
					}
					if int(scaling) < len(tileSpec)-1 {
						if err := v.DownRes(levelSpec.levelMag); err != nil {
							return err
						}
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "XZ Tile @ Y = %d", y)
			}
			dvid.ElapsedTime(dvid.Normal, startTime, "Total time to generate XZ Tiles")

		case plane.Equals(dvid.YZ):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Log(dvid.Debug, "Tiling YZ image %d x %d pixels\n", width, height)
			for x := src.MinPoint.Value(0); x <= src.MaxPoint.Value(0); x++ {
				sliceTime := time.Now()
				offset = offset.Modify(map[uint8]int32{0: x})
				slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				v, err := src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.SetVoxels(uuid, src, v); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				for scaling, levelSpec := range tileSpec {
					outF, err := d.getYZPutFunc(versionID, x)
					if err != nil {
						return err
					}
					if err := d.extractTiles(v, offset, scaling, outF); err != nil {
						return err
					}
					if int(scaling) < len(tileSpec)-1 {
						if err := v.DownRes(levelSpec.levelMag); err != nil {
							return err
						}
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "YZ Tile @ X = %d", x)
			}
			dvid.ElapsedTime(dvid.Normal, startTime, "Total time to generate YZ Tiles")

		default:
			dvid.Log(dvid.Normal, "Skipping request to tile '%s'.  Unsupported.", plane)
		}
	}
	return nil
}
