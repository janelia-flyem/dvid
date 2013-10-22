/*
	Package tiles implements DVID support for multiscale tiles in XY, XZ, and YZ orientation
	that can sync with datatypes based on the voxels package.
*/
package tiles

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/resize"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/tiles"
)

const HelpMessage = `
API for datatypes derived from tiles (github.com/janelia-flyem/dvid/datatype/tiles)
=====================================================================================

Command-line:

$ dvid dataset <UUID> new tiles <data name> <settings...>

	Adds multiresolution XY, XZ, and YZ tiles from Source to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c new tiles mytiles source=mygrayscale versioned=true

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    Source         Name of data source (required)
    TileSize       Size in pixels  (default: %s)


$ dvid node <UUID> <data name> generate <settings>

	Generates multiresolution XY, XZ, and YZ tiles from Source to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c generate tiles mytiles

    Arguments:

    UUID            Hexidecimal string with enough characters to uniquely identify a version node.
    data name       Name of data to create, e.g., "mygrayscale"
    settings        Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    planes          List of one or more planes separated by semicolon.  Each plane can be
                       designated using either axis number ("0,1") or xyz nomenclature ("xy").
                       Example:  planes=0,1;yz

	interpolation   One of the following methods of interpolation:
	                   NearestNeighbor (Nearest-Neighbor)
	                   Bilinear
	                   Bicubic
	                   MitchellNetravali (Mitchell-Netravali)
	                   Lanczos2Lut (Lanczos resampling with a=2 using a look-up table)
	                   Lanczos2  (Same as above but without look-up table for fast computation)
	                   Lanczos3Lut (Lanczos resampling with a=3 using a look-up table)
	                   Lanczos3  (Same as above but without look-up table for fast computation)

    ------------------

HTTP API (Level 2 REST):

GET  /api/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  /api/node/<UUID>/<data name>/info

    Retrieves characteristics of this tile data like the tile size and number of scales present.

    Example: 

    GET /api/node/3f8c/mytiles/info

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of tiles data.


GET  /api/node/<UUID>/<data name>/tile/<dims>/<scaling>/<tile coord>[/<format>]

    Retrieves tile of named data within a version node.

    Example: 

    GET /api/node/3f8c/mytiles/tile/xy/0/10_10_20/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    scaling       Value from 0 (original resolution) to N where each step is downres by 2.
    tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"


(TODO)
GET  /api/node/<UUID>/<data name>/image/<dims>/<size>/<offset>[/<format>]

    Retrieves image of named data within a version node using the precomputed tiles.

    Example: 

    GET /api/node/3f8c/mytiles/image/xy/512_256/0_0_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

`

const DefaultTileSize = 512

func init() {
	tiles := NewDatatype()
	tiles.DatatypeID = &datastore.DatatypeID{
		Name:    "tiles",
		Url:     "github.com/janelia-flyem/dvid/datatype/tiles",
		Version: "0.1",
	}
	datastore.RegisterDatatype(tiles)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&IndexTile{})
}

// Operation holds Voxel-specific data for processing chunks.
type Operation struct {
	voxels.VoxelHandler
	OpType
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
)

func (o OpType) String() string {
	switch o {
	case GetOp:
		return "Get Op"
	case PutOp:
		return "Put Op"
	default:
		return "Illegal Op"
	}
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
func (dtype *Datatype) NewDataService(dset *datastore.Dataset, id *datastore.DataID,
	config dvid.Config) (datastore.DataService, error) {

	// Make sure we have a valid DataService source
	s, found, e := config.GetString("Source")
	if e != nil || !found {
		return nil, fmt.Errorf("Cannot make tiles data without valid 'Source' setting")
	}
	sourcename := datastore.DataString(s)
	source, err := dset.DataService(sourcename)
	if err != nil {
		return nil, err
	}

	// Initialize the tiles data
	basedata, err := datastore.NewDataService(id, dtype, config)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data:   basedata,
		Source: source,
	}
	tilesize, found, err := config.GetInt("TileSize")
	if err != nil {
		dvid.Log(dvid.Normal, "Error in trying to set TileSize: %s\n", e.Error())
		data.Size = DefaultTileSize
	} else if !found {
		data.Size = DefaultTileSize
	} else {
		data.Size = int32(tilesize)
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

	// Source of the data for these tiles.
	Source datastore.DataService

	// Size in pixels.  All tiles are square.
	Size int32

	// MaxScale is the maximum scaling computed for the tiles.  The maximum scaling
	// is sufficient to show the longest dimension as one tile.
	MaxScale uint8
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
	var uuidStr string
	request.Command.CommandArgs(1, &uuidStr)
	service := server.DatastoreService()
	_, _, versionID, err := service.NodeIDFromString(uuidStr)
	if err != nil {
		return err
	}
	config := request.Settings()
	return d.ConstructTiles(versionID, config)
}

// DoHTTP handles all incoming HTTP requests for this dataset.
func (d *Data) DoHTTP(uuid datastore.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

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
		if op == PutOp {
			return fmt.Errorf("DVID does not yet support POST of tiles")
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

	case "image":
		err = fmt.Errorf("DVID does not yet support stitched images from tiles.")
	default:
		err = fmt.Errorf("Illegal request for tiles data.  See 'help' for REST API")
	}
	if err != nil {
		server.BadRequest(w, r, err.Error())
		return err
	}
	return nil
}

// GetTile retrieves a tile.
func (d *Data) GetTile(versionID datastore.VersionLocalID, planeStr, scalingStr, coordStr string) (
	image.Image, error) {

	db := server.StorageEngine()
	if db == nil {
		return nil, fmt.Errorf("Did not find a working key-value datastore to get image!")
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
	point, err := dvid.StringToPoint(coordStr, "_")
	if err != nil {
		return nil, fmt.Errorf("Illegal tile coordinate: %s (%s)", coordStr, err.Error())
	}
	index := &IndexTile{shape, uint8(scaling), point}
	//fmt.Printf("Point %s, Index: %s\n", point, index)

	// Retrieve the tile from datastore
	key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	data, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Could not find tile in datastore: %s", err.Error())
	}
	if data == nil {
		return nil, nil // Not found
	}
	//fmt.Printf("Retrieved tile for key %s: %d bytes\n", key, len(data))
	var img dvid.Image
	err = dvid.Deserialize(data, &img)
	if err != nil {
		return nil, fmt.Errorf("Error deserializing tile: %s", err.Error())
	}
	return img.Get(), nil
}

func interpConfig(config dvid.Config) (resize.InterpolationFunction, error) {
	s, found, err := config.GetString("interpolation")
	if err != nil {
		return nil, err
	}
	if !found {
		return resize.Bicubic, nil
	}
	switch s {
	case "NearestNeighbor":
		return resize.NearestNeighbor, nil
	case "Bilinear":
		return resize.Bilinear, nil
	case "Bicubic":
		return resize.Bicubic, nil
	case "MitchellNetravali":
		return resize.MitchellNetravali, nil
	case "Lanczos2Lut":
		return resize.Lanczos2Lut, nil
	case "Lanczos2":
		return resize.Lanczos2, nil
	case "Lanczos3Lut":
		return resize.Lanczos3Lut, nil
	case "Lanczos3":
		return resize.Lanczos3, nil
	default:
		return nil, fmt.Errorf("Unrecognized interpolation specified '%s'", s)
	}
}

type keyFunc func(scaling uint8, tileX, tileY int32) *datastore.DataKey

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

// Construct all tiles for an image with offset and put in datastore.  This function assumes
// the image and offset are in the XY plane.  It also assumes that img has dimensions that
// are a multiple of tile size so generated tiles are full-sized even along edges.
// Returns the # of extracted tiles.
func (d *Data) extractTiles(img image.Image, interp resize.InterpolationFunction,
	off dvid.Point2d, f keyFunc, scaling uint8) error {

	db := server.StorageEngine()

	// The reduction factor is 2^scaling.
	reduction := pow2(scaling)
	var downres image.Image
	if scaling == 0 {
		downres = img
	} else {
		width := uint(img.Bounds().Dx() / reduction)
		height := uint(img.Bounds().Dy() / reduction)
		downres = resize.Resize(width, height, img, interp)
	}

	// Determine the bounds in tile space for this scale.
	offset := dvid.Point2d{
		off[0] / int32(reduction),
		off[1] / int32(reduction),
	}
	imgSize := dvid.RectSize(downres.Bounds())
	lastPt := dvid.Point2d{offset[0] + imgSize[0] - 1, offset[1] + imgSize[1] - 1}
	tileBegX := offset[0] / d.Size
	tileEndX := lastPt[0] / d.Size
	tileBegY := offset[1] / d.Size
	tileEndY := lastPt[1] / d.Size

	//fmt.Printf("Image %d x %d, downres %d x %d\n",
	//	img.Bounds().Dx(), img.Bounds().Dy(), downres.Bounds().Dx(), downres.Bounds().Dy())
	//fmt.Printf("Tiling at scaling %d, offset %s (reduced %d): tile %d,%d -> %d,%d\n",
	//	scaling, off, offset, tileBegX, tileBegY, tileEndX, tileEndY)

	// Split image into tiles and store into datastore.
	src := new(dvid.Image)
	src.Set(downres)
	y0 := tileBegY * d.Size
	y1 := y0 + d.Size
	for ty := tileBegY; ty <= tileEndY; ty++ {
		x0 := tileBegX * d.Size
		x1 := x0 + d.Size
		for tx := tileBegX; tx <= tileEndX; tx++ {
			tileRect := image.Rect(int(x0), int(y0), int(x1), int(y1))
			tile, err := src.SubImage(tileRect)
			if err != nil {
				return err
			}
			key := f(scaling, tx, ty)
			serialization, err := dvid.Serialize(tile, dvid.Snappy, dvid.CRC32)
			if err != nil {
				return err
			}
			//fmt.Printf("Writing tile %s for scaling %d (%d,%d) (%s): %d bytes\n",
			//	tileRect, scaling, tx, ty, key, len(serialization))
			err = db.Put(key, serialization)
			if err != nil {
				return err
			}

			x0 += d.Size
			x1 += d.Size
		}
		y0 += d.Size
		y1 += d.Size
	}
	return nil
}

func (d *Data) getXYKeyFunc(versionID datastore.VersionLocalID, z int32) keyFunc {
	return func(scaling uint8, tileX, tileY int32) *datastore.DataKey {
		index := IndexTile{dvid.XY, scaling, dvid.Point3d{tileX, tileY, z}}
		return &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	}
}

func (d *Data) getXZKeyFunc(versionID datastore.VersionLocalID, y int32) keyFunc {
	return func(scaling uint8, tileX, tileY int32) *datastore.DataKey {
		index := IndexTile{dvid.XZ, scaling, dvid.Point3d{tileX, y, tileY}}
		return &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	}
}

func (d *Data) getYZKeyFunc(versionID datastore.VersionLocalID, x int32) keyFunc {
	return func(scaling uint8, tileX, tileY int32) *datastore.DataKey {
		index := IndexTile{dvid.YZ, scaling, dvid.Point3d{x, tileX, tileY}}
		return &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	}
}

func (d *Data) ConstructTiles(versionID datastore.VersionLocalID, config dvid.Config) error {
	// Make sure the source is valid voxels data.
	src, ok := d.Source.(*voxels.Data)
	if !ok {
		return fmt.Errorf("Cannot construct tiles for non-voxels data: %s", src.DataName())
	}

	// Get voxel extents of volume.
	minPt := dvid.Point3d{
		src.MinIndex.Value(0) * src.BlockSize.Value(0),
		src.MinIndex.Value(1) * src.BlockSize.Value(1),
		src.MinIndex.Value(2) * src.BlockSize.Value(2),
	}
	maxPt := dvid.Point3d{
		(src.MaxIndex.Value(0)+1)*src.BlockSize.Value(0) - 1,
		(src.MaxIndex.Value(1)+1)*src.BlockSize.Value(1) - 1,
		(src.MaxIndex.Value(2)+1)*src.BlockSize.Value(2) - 1,
	}

	// Determine covering volume size that is multiple of tile size.
	tileSize := dvid.Point3d{d.Size, d.Size, d.Size}
	tileMinPt := minPt.Div(tileSize)
	tileMaxPt := maxPt.Div(tileSize)
	coverMinPt := tileMinPt.Mult(tileSize)
	coverMaxPt := dvid.Point3d{
		(tileMaxPt.Value(0)+1)*d.Size - 1,
		(tileMaxPt.Value(1)+1)*d.Size - 1,
		(tileMaxPt.Value(2)+1)*d.Size - 1,
	}

	// Determine maximum scale levels based on the longest dimension.
	tilesInX := tileMaxPt.Value(0) - tileMinPt.Value(0) + 1
	tilesInY := tileMaxPt.Value(1) - tileMinPt.Value(1) + 1
	tilesInZ := tileMaxPt.Value(2) - tileMinPt.Value(2) + 1

	maxAnyDim := tilesInX
	if maxAnyDim < tilesInY {
		maxAnyDim = tilesInY
	}
	if maxAnyDim < tilesInZ {
		maxAnyDim = tilesInZ
	}
	d.MaxScale = log2(maxAnyDim)

	// Get type of interpolation
	interp, err := interpConfig(config)
	if err != nil {
		return err
	}

	// Get the planes we should tile.
	planes, err := config.GetShapes("planes", ";")
	if planes == nil {
		// If no planes are specified, construct tiles for 3 orthogonal planes.
		planes = []dvid.DataShape{dvid.XY, dvid.XZ, dvid.YZ}
	}

	for _, plane := range planes {
		var img image.Image
		startTime := time.Now()
		offset := coverMinPt.Duplicate().(dvid.Point3d)

		switch {

		case plane.Equals(dvid.XY):
			size := dvid.Point2d{
				coverMaxPt[0] - offset[0] + 1,
				coverMaxPt[1] - offset[1] + 1,
			}
			for z := minPt[2]; z <= maxPt[2]; z++ {
				sliceTime := time.Now()
				offset[2] = z
				slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
				if err != nil {
					return err
				}
				v, err := src.NewVoxelHandler(slice, nil)
				if err != nil {
					return err
				}
				img, err = src.GetImage(versionID, v)
				if err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				extractOffset := dvid.Point2d{offset[0], offset[1]}
				keyF := d.getXYKeyFunc(versionID, z)
				for scaling := uint8(0); scaling <= d.MaxScale; scaling++ {
					err := d.extractTiles(img, interp, extractOffset, keyF, scaling)
					if err != nil {
						return err
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "XY Tile @ Z = %d", z)
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "Total time to generate XY Tiles")

		case plane.Equals(dvid.XZ):
			size := dvid.Point2d{
				coverMaxPt[0] - offset[0] + 1,
				coverMaxPt[2] - offset[2] + 1,
			}
			for y := minPt[1]; y <= maxPt[1]; y++ {
				sliceTime := time.Now()
				offset[1] = y
				slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, size)
				if err != nil {
					return err
				}
				v, err := src.NewVoxelHandler(slice, nil)
				if err != nil {
					return err
				}
				img, err = src.GetImage(versionID, v)
				if err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				extractOffset := dvid.Point2d{offset[0], offset[2]}
				keyF := d.getXZKeyFunc(versionID, y)
				for scaling := uint8(0); scaling <= d.MaxScale; scaling++ {
					err := d.extractTiles(img, interp, extractOffset, keyF, scaling)
					if err != nil {
						return err
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "XZ Tile @ Y = %d", y)
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "Total time to generate XZ Tiles")

		case plane.Equals(dvid.YZ):
			size := dvid.Point2d{
				coverMaxPt[1] - offset[1] + 1,
				coverMaxPt[2] - offset[2] + 1,
			}
			for x := minPt[0]; x <= maxPt[0]; x++ {
				sliceTime := time.Now()
				offset[0] = x
				slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, size)
				if err != nil {
					return err
				}
				v, err := src.NewVoxelHandler(slice, nil)
				if err != nil {
					return err
				}
				img, err = src.GetImage(versionID, v)
				if err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				extractOffset := dvid.Point2d{offset[1], offset[2]}
				keyF := d.getYZKeyFunc(versionID, x)
				for scaling := uint8(0); scaling <= d.MaxScale; scaling++ {
					err := d.extractTiles(img, interp, extractOffset, keyF, scaling)
					if err != nil {
						return err
					}
				}
				dvid.ElapsedTime(dvid.Debug, sliceTime, "YZ Tile @ X = %d", x)
			}
			dvid.ElapsedTime(dvid.Debug, startTime, "Total time to generate YZ Tiles")

		default:
			dvid.Log(dvid.Normal, "Skipping request to tile '%s'.  Unsupported.", plane)
		}
	}
	return nil
}
