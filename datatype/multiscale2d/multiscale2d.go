/*
	Package multiscale2d implements DVID support for multiscale2ds in XY, XZ, and YZ orientation.
	All raw tiles are stored as PNG images that are by default gzipped.  This allows raw
	tile gets to be already compressed at the cost of more expensive uncompression to
	retrieve arbitrary image sizes.
*/
package multiscale2d

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/multiscale2d"
	TypeName = "multiscale2d"
)

const HelpMessage = `
API for datatypes derived from multiscale2d (github.com/janelia-flyem/dvid/datatype/multiscale2d)
=====================================================================================

Command-line:

$ dvid repo <UUID> new multiscale2d <data name> <settings...>

	Adds multiresolution XY, XZ, and YZ multiscale2d from Source to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new multiscale2d mymultiscale2d source=mygrayscale format=jpg

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Format         "lz4" (default), "jpg", or "png".  In the case of "lz4", decoding/encoding is done at
                      tile request time and is a better choice if you primarily ask for arbitrary sized images
                      (via GET .../raw/... or .../isotropic/...) instead of tiles (via GET .../tile/...)
    Versioned      "true" or "false" (default)
    Source         Name of data source (required)
    TileSize       Size in pixels
    Placeholder    Bool ("false", "true", "0", or "1").  Return placeholder tile if missing.


$ dvid node <UUID> <data name> generate <settings...>
$ dvid -stdin node <UUID> <data name> generate <settings...> < config.json

	Generates multiresolution XY, XZ, and YZ multiscale2d from Source to repo with specified UUID.
	The resolutions at each scale and the dimensions of the tiles are passed in the configuration
	JSON.  Only integral multiplications of original resolutions are allowed for scale.  If you
	want more sophisticated processing, post the multiscale2d tiles directly via HTTP.  Note that
	the generated tiles are aligned in a grid having (0,0,0) as a top left corner of a tile, not
	tiles that start from the corner of present data since the data can expand.

	Example:

	$ dvid repo 3f8c mymultiscale2d generate /path/to/config.json
	$ dvid -stdin repo 3f8c mymultiscale2d generate planes="yz;0,1" < /path/to/config.json 

    Arguments:

    UUID            Hexidecimal string with enough characters to uniquely identify a version node.
    data name       Name of data to create, e.g., "mygrayscale".
    settings        Optional specification of tiles to generate.

    Configuration Settings (case-insensitive keys)

    planes          List of one or more planes separated by semicolon.  Each plane can be
                       designated using either axis number ("0,1") or xyz nomenclature ("xy").
                       Example:  planes="0,1;yz"
    filename        Filename of JSON file specifying multiscale tile resolutions as below.

    Sample config.json:

	{
	    "0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
	    "1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
	    "2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
	    "3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
	}

    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info

    Retrieves characteristics of this tile data like the tile size and number of scales present.

    Example: 

    GET <api URL>/node/3f8c/mymultiscale2d/info

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of multiscale2d data.


GET  <api URL>/node/<UUID>/<data name>/tile/<dims>/<scaling>/<tile coord>[?noblanks=true]
(TODO) POST
    Retrieves PNG tile of named data within a version node.  This GET call should be the fastest
    way to retrieve image data since internally it has already been stored as a compressed PNG.

    Example: 

    GET <api URL>/node/3f8c/mymultiscale2d/tile/xy/0/10_10_20

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    scaling       Value from 0 (original resolution) to N where each step is downres by 2.
    tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.

  	Query-string options:

  	noblanks	  If true, any tile request for tiles outside the currently stored extents
  				  will return a 


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>]

    Retrieves raw image of named data within a version node using the precomputed multiscale2d.
    By "raw", we mean that no additional processing is applied based on voxel resolutions
    to make sure the retrieved image has isotropic pixels.  For example, if an XZ image
    is requested and the image volume has X resolution 3 nm and Z resolution 40 nm, the
    returned image will be heavily anisotropic and should be scaled by 40/3 in Y by client.

    Example: 

    GET <api URL>/node/3f8c/mymultiscale2d/raw/xy/512_256/0_0_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Note that only 2d images are returned for multiscale2ds.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>]

    Retrieves isotropic image of named data within a version node using the precomputed multiscale2d.
    Additional processing is applied based on voxel resolutions to make sure the retrieved image 
    has isotropic pixels.  For example, if an XZ image is requested and the image volume has 
    X resolution 3 nm and Z resolution 40 nm, the returned image's height will be magnified 40/3
    relative to the raw data.

    Example: 

    GET <api URL>/node/3f8c/mymultiscale2d/isotropic/xy/512_256/0_0_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form i_j.  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Note that only 2d images are returned for multiscale2ds.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(&IndexTile{})
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
			Name:    "multiscale2d",
			URL:     "github.com/janelia-flyem/dvid/datatype/multiscale2d",
			Version: "0.1",
			Requirements: &storage.Requirements{
				Batcher: true,
			},
		},
	}
}

// --- TypeService interface ---

// NewData returns a pointer to new tile data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	// Make sure we have a valid DataService source
	sourcename, found, err := c.GetString("Source")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make multiscale2d data without valid 'Source' setting.")
	}

	// See if we want placeholder multiscale2d.
	placeholder, found, err := c.GetBool("Placeholder")
	if err != nil {
		return nil, err
	}

	// Determine encoding for tile storage and this dictates what kind of compression we use.
	encoding, found, err := c.GetString("Format")
	if err != nil {
		return nil, err
	}
	format := LZ4
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

	// Initialize the multiscale2d data
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data: basedata,
		Properties: Properties{
			Source:      dvid.DataString(sourcename),
			Placeholder: placeholder,
			Encoding:    format,
		},
	}
	return data, nil
}

func (dtype *Type) Help() string {
	return HelpMessage
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
type TileSpec []TileScaleSpec

// MarshalJSON returns the JSON of the multiscale2d specifications for each scale level.
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
	dvid.Debugf("Found %d scaling levels for multiscale2d specification.\n", len(config))

	// Store resolution and tile sizes per level.
	var hires, lores float64
	for scaleStr, levelSpec := range config {
		fmt.Printf("scale %s, levelSpec %v\n", scaleStr, levelSpec)
		scaleLevel, err := strconv.Atoi(scaleStr)
		if err != nil {
			return nil, fmt.Errorf("Scaling '%s' needs to be a number for the scale level.", scaleStr)
		}
		if scaleLevel >= len(specs) {
			return nil, fmt.Errorf("Tile levels must be consecutive integers from [0,Max]: Got scale level %d > # levels (%d)\n",
				scaleLevel, len(specs))
		}
		specs[scaleLevel] = TileScaleSpec{LevelSpec: levelSpec}
	}

	// Compute the magnification between each level.
	for scaleLevel, levelSpec := range specs {
		if int(scaleLevel+1) <= len(specs)-1 {
			nextSpec := specs[scaleLevel+1]
			var levelMag dvid.Point3d
			for i, curRes := range levelSpec.Resolution {
				hires = float64(curRes)
				lores = float64(nextSpec.Resolution[i])
				rem := math.Remainder(lores, hires)
				if rem > 0.001 {
					return nil, fmt.Errorf("Resolutions between scale %d and %d aren't integral magnifications!",
						scaleLevel, scaleLevel+1)
				}
				mag := lores / hires
				if mag < 0.99 {
					return nil, fmt.Errorf("A resolution between scale %d and %d actually increases!",
						scaleLevel, scaleLevel+1)
				}
				mag += 0.5
				levelMag[i] = int32(mag)
			}
			levelSpec.levelMag = levelMag
			specs[scaleLevel] = levelSpec
		}
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

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Source of the data for these multiscale2d.
	Source dvid.DataString

	// Levels describe the resolution and tile sizes at each level of resolution.
	Levels TileSpec

	// Placeholder, when true (false by default), will generate fake tile images if a tile cannot
	// be found.  This is useful in testing clients.
	Placeholder bool

	// Encoding describes encoding of the stored tile.  See multiscale2d.Format
	Encoding Format

	// Quality is optional quality of encoding for jpeg, 1-100, higher is better.
	Quality int
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data
	Properties
}

// Returns the default tile spec for
func (d *Data) DefaultTileSpec() TileSpec {
	return nil
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
	return HelpMessage
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("multiscale2d.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC handles the 'generate' command.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "generate" {
		return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	var uuidStr, dataName, cmdStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr)

	// Get the multiscale2d generation configuration from a file or stdin.
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
		} else {
			dvid.Infof("Using default tile generation method since no tile spec file was given...\n")
			tileSpec = d.DefaultTileSpec()
		}
	}
	return d.ConstructTiles(uuidStr, tileSpec, request)
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	repo, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n",
			d.DataName(), err.Error())
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
	// All HTTP requests are interactive so let server tally request.
	// TODO: This command should be moved to web server handling when better
	// framework for datatype-specific API is implemented, allowing type-specific
	// logging of API calls, etc.
	server.GotInteractiveRequest()

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
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "tile":
		if action == "post" {
			server.BadRequest(w, r, "DVID does not yet support POST of multiscale2d")
			return
		}
		if err := d.ServeTile(repo, storeCtx, w, r, parts); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: tile (%s)", r.Method, r.URL)

	case "raw", "isotropic":
		if action == "post" {
			server.BadRequest(w, r, "multiscale2d '%s' can only PUT tiles not images", d.DataName())
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
			server.BadRequest(w, r, err.Error())
			return
		}
		if plane.ShapeDimensions() != 2 {
			server.BadRequest(w, r, "Quadtrees can only return 2d images not %s", plane)
			return
		}
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		source, err := repo.GetDataByName(d.Source)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		src, ok := source.(*voxels.Data)
		if !ok {
			server.BadRequest(w, r, "Cannot construct multiscale2d for non-voxels data: %s", d.Source)
			return
		}
		img, err := d.GetImage(storeCtx, src, slice, parts[3] == "isotropic")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
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
		timedLog.Infof("HTTP %s: tile-accelerated %s %s (%s)", r.Method, planeStr, parts[3], r.URL)
	default:
		server.BadRequest(w, r, "Illegal request for multiscale2d data.  See 'help' for REST API")
	}
}

// GetImage returns an image given a 2d orthogonal image description.  Since multiscale2d tiles
// have precomputed XY, XZ, and YZ orientations, reconstruction of the desired image should
// be much faster than computing the image from voxel blocks.
func (d *Data) GetImage(ctx storage.Context, src *voxels.Data, geom dvid.Geometry, isotropic bool) (*dvid.Image, error) {
	// Iterate through tiles that intersect our geometry.
	if d.Levels == nil || len(d.Levels) == 0 {
		return nil, fmt.Errorf("%s has no specification for tiles at highest resolution",
			d.DataName())
	}
	levelSpec := d.Levels[0]
	minSlice, err := src.HandleIsotropy2D(geom, isotropic)
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
				goImg, err := d.GetTile(ctx, slice, 0, dvid.IndexZYX(tileCoord))
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

// ServeTile returns a tile with appropriate Content-Type set.
func (d *Data) ServeTile(repo datastore.Repo, ctx storage.Context, w http.ResponseWriter,
	r *http.Request, parts []string) error {

	if len(parts) < 7 {
		return fmt.Errorf("'tile' request must be following by plane, scale level, and tile coordinate")
	}
	planeStr, scalingStr, coordStr := parts[4], parts[5], parts[6]
	queryValues := r.URL.Query()
	noblanksStr := dvid.DataString(queryValues.Get("noblanks"))
	var noblanks bool
	if noblanksStr == "true" {
		noblanks = true
	}
	var formatStr string
	if len(parts) >= 8 {
		formatStr = parts[7]
	}

	// Construct the index for this tile
	plane := dvid.DataShapeString(planeStr)
	shape, err := plane.DataShape()
	if err != nil {
		err = fmt.Errorf("Illegal tile plane: %s (%s)", planeStr, err.Error())
		server.BadRequest(w, r, err.Error())
		return err
	}
	scaling, err := strconv.ParseUint(scalingStr, 10, 8)
	if err != nil {
		err = fmt.Errorf("Illegal tile scale: %s (%s)", scalingStr, err.Error())
		server.BadRequest(w, r, err.Error())
		return err
	}
	tileCoord, err := dvid.StringToPoint(coordStr, "_")
	if err != nil {
		err = fmt.Errorf("Illegal tile coordinate: %s (%s)", coordStr, err.Error())
		server.BadRequest(w, r, err.Error())
		return err
	}
	indexZYX := dvid.IndexZYX{tileCoord.Value(0), tileCoord.Value(1), tileCoord.Value(2)}
	data, err := d.getTileData(ctx, shape, Scaling(scaling), indexZYX)
	if err != nil {
		server.BadRequest(w, r, err.Error())
		return err
	}
	if data == nil {
		if noblanks {
			http.NotFound(w, r)
			return nil
		}
		img, err := d.getBlankTileImage(repo, shape, Scaling(scaling))
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
		server.BadRequest(w, r, err.Error())
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

// GetTile returns a 2d tile image or a placeholder
func (d *Data) GetTile(ctx storage.Context, shape dvid.DataShape, scaling int, index dvid.IndexZYX) (image.Image, error) {
	data, err := d.getTileData(ctx, shape, Scaling(scaling), index)
	if err != nil {
		return nil, err
	}
	tileIndex := &IndexTile{&index, shape, Scaling(scaling)}

	if data == nil {
		if d.Placeholder {
			if d.Levels == nil || scaling < 0 || scaling >= len(d.Levels) {
				return nil, fmt.Errorf("Could not find tile specification at given scale %d", scaling)
			}
			message := fmt.Sprintf("%s Tile coord %s @ scale %d", shape, tileIndex, scaling)
			return dvid.PlaceholderImage(shape, d.Levels[scaling].TileSize, message)
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
func (d *Data) getTileData(ctx storage.Context, shape dvid.DataShape, scaling Scaling, index dvid.IndexZYX) ([]byte, error) {
	if d.Levels == nil {
		return nil, fmt.Errorf("Tiles have not been generated.")
	}
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return nil, err
	}

	// Retrieve the tile data from datastore
	tileIndex := &IndexTile{&index, shape, scaling}
	data, err := bigdata.Get(ctx, tileIndex.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Error trying to GET from datastore: %s", err.Error())
	}
	return data, nil
}

// getBlankTileData returns zero 2d tile data with a given scaling and format.
func (d *Data) getBlankTileImage(repo datastore.Repo, shape dvid.DataShape, scaling Scaling) (image.Image, error) {
	levelSpec, found := d.Levels[scaling]
	if !found {
		return nil, fmt.Errorf("Could not extract tiles for unspecified scale level %d", scaling)
	}
	tileW, tileH, err := shape.GetSize2D(levelSpec.TileSize)
	if err != nil {
		return nil, err
	}
	source, err := repo.GetDataByName(d.Source)
	if err != nil {
		return nil, err
	}
	src, ok := source.(*voxels.Data)
	if !ok {
		return nil, fmt.Errorf("Data instance %q for uuid %q is not voxels.Data", d.Source,
			repo.RootUUID())
	}
	bytesPerVoxel := src.Values().BytesPerElement()
	switch bytesPerVoxel {
	case 1, 2, 4, 8:
		numBytes := tileW * tileH * bytesPerVoxel
		data := make([]byte, numBytes, numBytes)
		return dvid.ImageFromData(data, int(tileW), int(tileH))
	default:
		return nil, fmt.Errorf("Cannot construct blank tile for data %q with %d bytes/voxel",
			d.Source, src.Values().BytesPerElement())
	}
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

type outFunc func(index *IndexTile, img *dvid.Image) error

// Construct all tiles for an image with offset and send to out function.  extractTiles assumes
// the image and offset are in the XY plane.
func (d *Data) extractTiles(v voxels.ExtData, offset dvid.Point, scaling int, outF outFunc) error {
	if d.Levels == nil || scaling < 0 || scaling >= len(d.Levels) {
		return fmt.Errorf("Bad scaling level specified: %d", scaling)
	}
	levelSpec := d.Levels[scaling]
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
			tileIndex := NewIndexTile(dvid.IndexZYX(tileCoord), v.DataShape(), Scaling(scaling))
			if err = outF(tileIndex, tile); err != nil {
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
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return nil, fmt.Errorf("Cannot open big data store: %s\n", err.Error())
	}
	ctx := datastore.NewVersionedContext(d, versionID)

	return func(index *IndexTile, tile *dvid.Image) error {
		var err error
		var data []byte

		switch d.Encoding {
		case LZ4:
			compression, err := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
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
		return bigdata.Put(ctx, index.Bytes(), data)
	}, nil
}

func (d *Data) ConstructTiles(uuidStr string, tileSpec TileSpec, request datastore.Request) error {
	config := request.Settings()
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

	source, err := repo.GetDataByName(d.Source)
	if err != nil {
		return err
	}
	src, ok := source.(*voxels.Data)
	if !ok {
		return fmt.Errorf("Cannot construct multiscale2d for non-voxels data: %s", d.Source)
	}

	// Save the current tile specification
	d.Levels = tileSpec
	if err := repo.Save(); err != nil {
		return err
	}

	// Expand min and max points to coincide with full tile boundaries of highest resolution.
	hiresSpec := tileSpec[Scaling(0)]
	minTileCoord := src.MinPoint.(dvid.Chunkable).Chunk(hiresSpec.TileSize)
	maxTileCoord := src.MaxPoint.(dvid.Chunkable).Chunk(hiresSpec.TileSize)
	minPt := minTileCoord.MinPoint(hiresSpec.TileSize)
	maxPt := maxTileCoord.MaxPoint(hiresSpec.TileSize)
	sizeVolume := maxPt.Sub(minPt).AddScalar(1)

	// Setup swappable ExtData buffers (the stitched slices) so we can be generating tiles
	// at same time we are reading and stitching them.
	var bufferLock [2]sync.Mutex
	var sliceBuffers [2]voxels.ExtData
	var bufferNum int

	// Get the planes we should tile.
	planes, err := config.GetShapes("planes", ";")
	if planes == nil {
		// If no planes are specified, construct multiscale2d for 3 orthogonal planes.
		planes = []dvid.DataShape{dvid.XY, dvid.XZ, dvid.YZ}
	}

	voxelsCtx := datastore.NewVersionedContext(src, versionID)
	outF, err := d.putTileFunc(versionID)

	for _, plane := range planes {
		timedLog := dvid.NewTimeLog()
		offset := minPt.Duplicate()

		switch {

		case plane.Equals(dvid.XY):
			width, height, err := plane.GetSize2D(sizeVolume)
			if err != nil {
				return err
			}
			dvid.Debugf("Tiling XY image %d x %d pixels\n", width, height)
			for z := src.MinPoint.Value(2); z <= src.MaxPoint.Value(2); z++ {
				server.BlockOnInteractiveRequests("multiscale2d.ConstructTiles [xy]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{2: z})
				slice, err := dvid.NewOrthogSlice(dvid.XY, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.GetVoxels(voxelsCtx, src, sliceBuffers[bufferNum], nil); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for scaling, levelSpec := range tileSpec {
						if err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %s\n", err.Error())
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
			for y := src.MinPoint.Value(1); y <= src.MaxPoint.Value(1); y++ {
				server.BlockOnInteractiveRequests("multiscale2d.ConstructTiles [xz]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{1: y})
				slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.GetVoxels(voxelsCtx, src, sliceBuffers[bufferNum], nil); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for scaling, levelSpec := range tileSpec {
						if err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %s\n", err.Error())
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
			for x := src.MinPoint.Value(0); x <= src.MaxPoint.Value(0); x++ {
				server.BlockOnInteractiveRequests("multiscale2d.ConstructTiles [yz]")

				sliceLog := dvid.NewTimeLog()
				offset = offset.Modify(map[uint8]int32{0: x})
				slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, dvid.Point2d{width, height})
				if err != nil {
					return err
				}
				bufferLock[bufferNum].Lock()
				sliceBuffers[bufferNum], err = src.NewExtHandler(slice, nil)
				if err != nil {
					return err
				}
				if err = voxels.GetVoxels(voxelsCtx, src, sliceBuffers[bufferNum], nil); err != nil {
					return err
				}
				// Iterate through the different scales, extracting tiles at each resolution.
				go func(bufferNum int, offset dvid.Point) {
					defer bufferLock[bufferNum].Unlock()
					timedLog := dvid.NewTimeLog()
					for scaling, levelSpec := range tileSpec {
						outF, err := d.putTileFunc(versionID)
						if err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if err := d.extractTiles(sliceBuffers[bufferNum], offset, scaling, outF); err != nil {
							dvid.Errorf("Error in tiling: %s\n", err.Error())
							return
						}
						if int(scaling) < len(tileSpec)-1 {
							if err := sliceBuffers[bufferNum].DownRes(levelSpec.levelMag); err != nil {
								dvid.Errorf("Error in tiling: %s\n", err.Error())
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
