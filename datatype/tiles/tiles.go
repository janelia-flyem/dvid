/*
	Package tiles implements DVID support for multiscale tiles in XY, XZ, and YZ orientation
	that can sync with datatypes based on the voxels package.
*/
package tiles

import (
	"encoding/binary"
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


$ dvid node <UUID> <data name> generate

	Generates multiresolution XY, XZ, and YZ tiles from Source to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c generate tiles mytiles

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    Source         Name of data source (required)
    TileSize       Size in pixels  (default: %s)
	
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

    GET /api/node/3f8c/mytiles/tile/xy/0/10,10,20/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    scaling       Value from 0 (original resolution) to N where each step is downres by 2.
    tile coord    The tile coordinate in "x,y,z" format.  See discussion of scaling above.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"


GET  /api/node/<UUID>/<data name>/image/<dims>/<size>/<offset>[/<format>]

    Retrieves image of named data within a version node using the precomputed tiles.

    Example: 

    GET /api/node/3f8c/mytiles/image/xy/512,256/0,0,100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    tile coord    The tile coordinate in "x,y,z" format.  See discussion of scaling above.
    format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

`

const DefaultTileSize = 256

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
	config dvid.Config) (dataservice datastore.DataService, err error) {

	// Make sure we have a valid DataService source
	s, found, e := config.GetString("Source")
	if e != nil || !found {
		err = fmt.Errorf("Cannot make tiles data without valid 'Source' setting")
		return
	}
	sourcename := datastore.DataString(s)
	var source datastore.DataService
	source, err = dset.DataService(sourcename)
	if err != nil {
		return
	}

	// Initialize the tiles data
	var basedata *datastore.Data
	basedata, err = datastore.NewDataService(id, dtype, config)
	if err != nil {
		return
	}
	data := &Data{
		Data:   basedata,
		Source: source,
	}
	data.Size, found, e = config.GetInt32("TileSize")
	if e != nil || !found {
		data.Size = DefaultTileSize
	}
	dataservice = data
	return
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
	return d.ConstructTiles(versionID)
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
	img image.Image, err error) {

	db := server.KeyValueDB()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
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

	// Retrieve the tile from datastore
	key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	err = dvid.Deserialize(data, img)
	return
}

// Subvolume is a tile-sized 3d subvolume that allows us to efficiently reconstruct tiles.
// This struct should fulfill the VoxelHandler interface.
type Subvolume struct {
	dvid.Geometry

	source *voxels.Data
	data   []uint8
}

func (v *Subvolume) String() string {
	return fmt.Sprintf("Subvolume %s of size %s @ offset %s", v.DataShape(), v.Size(), v.StartPoint())
}

func (v *Subvolume) Data() []uint8 {
	return v.data
}

func (v *Subvolume) Stride() int32 {
	return v.Size().Value(0) * v.BytesPerVoxel()
}

func (v *Subvolume) PointIndexer(p dvid.Point) dvid.PointIndexer {
	index := dvid.IndexZYX(p.(dvid.Point3d))
	return &index
}

func (v *Subvolume) VoxelFormat() (channels, bytesPerChannel int32) {
	return v.source.ValuesPerVoxel, v.source.BytesPerValue
}

func (v *Subvolume) BytesPerVoxel() int32 {
	return v.source.ValuesPerVoxel * v.source.BytesPerValue
}

func (v *Subvolume) ByteOrder() binary.ByteOrder {
	return v.source.ByteOrder
}

func (d *Data) ConstructTiles(versionID datastore.VersionLocalID) error {
	// Make sure the source is valid voxels data.
	src, ok := d.Source.(*voxels.Data)
	if !ok {
		return fmt.Errorf("Cannot construct tiles for non-voxels data: %s", src.DataName())
	}

	// Iterate through tile-sized subvolume cubes that span the known extents
	// of the source data.
	subvolXMin := src.MinIndex.X() * src.BlockSize[0] / d.Size
	subvolXMax := ((src.MaxIndex.X()+1)*src.BlockSize[0] - 1) / d.Size
	subvolYMin := src.MinIndex.Y() * src.BlockSize[1] / d.Size
	subvolYMax := ((src.MaxIndex.Y()+1)*src.BlockSize[1] - 1) / d.Size
	subvolZMin := src.MinIndex.Z() * src.BlockSize[2] / d.Size
	subvolZMax := ((src.MaxIndex.Z()+1)*src.BlockSize[2] - 1) / d.Size

	fmt.Printf("Source extents: (%d,%d,%d) -> (%d,%d,%d)\n",
		src.MinIndex.X(), src.MinIndex.Y(), src.MinIndex.Z(),
		src.MaxIndex.X(), src.MaxIndex.Y(), src.MaxIndex.Z())

	for z := subvolZMin; z <= subvolZMax; z++ {
		for y := subvolYMin; y <= subvolYMax; y++ {
			for x := subvolXMin; x <= subvolXMax; x++ {
				err := d.convertSubvol(src, versionID, [3]int32{x, y, z})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// convertSubvol initializes scale-level 0 tiles (source voxels) from source data
// efficiently.  Conversion occurs by reading in all blocks for a tile-sized 3d subvolume.
// This assumes that we are constructing tiles in the 3 orthogonal orientations where
// each orientation has the same TileSize x TileSize pixels.
func (d *Data) convertSubvol(src *voxels.Data, versionID datastore.VersionLocalID, v [3]int32) error {
	startTime := time.Now()

	db := server.KeyValueDB()

	// Determine the voxel extents encompassed by this subvolume.
	tileSize := int32(d.Size)
	x0, y0, z0 := v[0]*tileSize, v[1]*tileSize, v[2]*tileSize
	x1, y1, z1 := x0+tileSize-1, y0+tileSize-1, z0+tileSize-1
	startVoxel := voxels.Coord{x0, y0, z0}
	endVoxel := voxels.Coord{x1, y1, z1}

	// Allocate the subvolume buffer
	geom := voxels.NewSubvolume(startVoxel, voxels.Point3d{tileSize, tileSize, tileSize})
	subvolume := &Subvolume{
		Geometry: geom,
		source:   src,
	}
	bytesPerVoxel := subvolume.BytesPerVoxel()
	blockBytes := int(src.BlockSize[0] * src.BlockSize[1] * src.BlockSize[2] * bytesPerVoxel)
	totalBytes := geom.NumVoxels() * int64(bytesPerVoxel)
	subvolume.data = make([]uint8, totalBytes, totalBytes)

	dataNumX := tileSize * bytesPerVoxel
	dataNumXY := tileSize * dataNumX

	blockNumX := src.BlockSize[0] * bytesPerVoxel
	blockNumXY := src.BlockSize[1] * blockNumX

	fmt.Printf("Buffer bytesPerVoxel %d, totalBytes %d\n", bytesPerVoxel, totalBytes)
	fmt.Printf("convertSubvol: %s\n", geom)

	// Create a subvolume data that handles the VoxelHandler interface
	// Be as efficient as possible in range queries, getting values for subvolume.
	// Map: Iterate in x, then y, then z
	buffer := []byte(subvolume.data)
	startBlockCoord := startVoxel.BlockCoord(src.BlockSize)
	endBlockCoord := endVoxel.BlockCoord(src.BlockSize)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			// might not be true if we go to Z-curve.
			i0 := subvolume.PointIndexer(dvid.Point3d{startBlockCoord[0], y, z})
			i1 := subvolume.PointIndexer(dvid.Point3d{endBlockCoord[0], y, z})
			startKey := &datastore.DataKey{src.DsetID, src.ID, versionID, i0}
			endKey := &datastore.DataKey{src.DsetID, src.ID, versionID, i1}

			// GET all the chunks for this range.
			keyvalues, err := db.GetRange(startKey, endKey)
			if err != nil {
				return fmt.Errorf("Error in reading data during ConstructTiles %s: %s",
					src.DataName(), err.Error())
			}

			// Store the values in subvolume buffer.
			for _, kv := range keyvalues {
				// Get the block's voxel extent
				datakey, ok := kv.K.(*datastore.DataKey)
				if !ok {
					return fmt.Errorf("Illegal key (not DataKey) returned for data %s",
						src.DataName())
				}
				zyxIndex, ok := datakey.Index.(voxels.PointIndexer)
				if !ok {
					return fmt.Errorf("Illegal index (not PointIndexer) returned for data %s",
						src.DataName())
				}
				minBlockVoxel := zyxIndex.OffsetToBlock(src.BlockSize)
				maxBlockVoxel := minBlockVoxel.Add(src.BlockSize.Sub(Point3d{1, 1, 1}))

				// Compute the bound voxel coordinates for the subvolume and adjust
				// to our block bounds.
				begVolCoord := startVoxel.Max(minBlockVoxel)
				endVolCoord := endVoxel.Min(maxBlockVoxel)
				blockBeg := begVolCoord.Sub(minBlockVoxel)

				// Adjust the voxel coordinates for the data so that (0,0,0)
				// is where we expect this subvolume's data to begin.
				beg := begVolCoord.Sub(startVoxel)
				end := endVolCoord.Sub(startVoxel)

				// Deserialize the data
				data, _, err := dvid.DeserializeData(kv.V, true)
				if err != nil {
					return fmt.Errorf("Unable to deserialize chunk from dataset '%s': %s\n",
						d.DataName(), err.Error())
				}
				block := []uint8(data)
				if len(block) != blockBytes {
					return fmt.Errorf("Retrieved block for '%s' is %d bytes, not block size of %d!\n",
						d.DataName(), len(block), blockBytes)
				}

				// Store the data into the buffer.
				blockZ := blockBeg[2]
				for dataZ := beg[2]; dataZ <= end[2]; dataZ++ {
					blockY := blockBeg[1]
					for dataY := beg[1]; dataY <= end[1]; dataY++ {
						blockI := blockZ*blockNumXY + blockY*blockNumX + blockBeg[0]*bytesPerVoxel
						dataI := dataZ*dataNumXY + dataY*dataNumX + beg[0]*bytesPerVoxel
						run := end[0] - beg[0] + 1
						bytes := run * bytesPerVoxel
						copy(buffer[dataI:dataI+bytes], block[blockI:blockI+bytes])
						blockY++
					}
					blockZ++
				}
			}
		}
	} // Block iteration in z over the subvolume.

	// Store all the XY images in this subvolume.
	// nx := x1 - x0 + 1
	// ny := y1 - y0 + 1
	nz := z1 - z0 + 1
	plane := voxels.XY
	scaling := uint8(0)
	for z := int32(0); z < nz; z++ {
		img, err := src.SliceImage(subvolume, z)
		if err != nil {
			return err
		}
		index := &IndexTile{plane, scaling, []int32{v[0], v[1], z}}
		key := &datastore.DataKey{d.DatasetID(), d.ID, versionID, index}
		serialization, err := dvid.Serialize(img, dvid.Snappy, dvid.CRC32)
		if err != nil {
			return err
		}
		err = db.Put(key, serialization)
		if err != nil {
			return err
		}
	}

	// Store all the XZ images in this subvolume.

	// Store all the YZ images in this subvolume.

	dvid.ElapsedTime(dvid.Debug, startTime, "Generate tiles in subvolume (%d,%d,%d)",
		v[0], v[1], v[2])
	return nil
}
