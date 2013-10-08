/*
	Package voxels implements DVID support for data using voxels as elements.
*/
package voxels

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.8"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/voxels"
)

const HelpMessage = `
API for 'voxels' datatype (github.com/janelia-flyem/dvid/datatype/voxels)
=========================================================================

Command-line:

$ dvid dataset <UUID> new <type name> <data name> <settings...>

	Adds newly named data of the 'type name' to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c new grayscale8 mygrayscale BlockSize=32 VoxelRes=1.5,1.0,1.5

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    type name      Data type name, e.g., "grayscale8"
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %s)
    VoxelRes       Resolution of voxels (default: 1.0, 1.0, 1.0)
    VoxelResUnits  String of units (default: "nanometers")

$ dvid node <UUID> <data name> load local  <plane> <offset> <image glob>
$ dvid node <UUID> <data name> load remote <plane> <offset> <image glob>

    Adds image data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").

    Example: 

    $ dvid node 3f8c mygrayscale load local xy 0,0,100 data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png
	
    ------------------

HTTP API (Level 2 REST):

GET  /api/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  /api/node/<UUID>/<data name>/info
POST /api/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET /api/node/3f8c/grayscale/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]
POST /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts voxel data.

    Example: 

    GET /api/node/3f8c/grayscale/0,1/512,256/0,0,100/jpg:80

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD thrift-encoded data will be "application/x-thrift".

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: "thrift" (default: "thrift")

(TO DO)

GET  /api/node/<UUID>/<data name>/arb/<center>/<normal>/<size>[/<format>]

    Retrieves non-orthogonal (arbitrarily oriented planar) image data of named 3d data 
    within a version node.

    Example: 

    GET /api/node/3f8c/grayscale/arb/200,200/2.0,1.3,1/100,100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    center        3d coordinate in the format "x,y,z".  Gives 3d coord of center pixel.
    normal        3d vector in the format "nx,ny,nz".  Gives normal vector of image.
    size          Size in pixels in the format "dx,dy".
    format        "png", "jpg" (default: "png")  
                    jpg allows lossy quality setting, e.g., "jpg:80"
`

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax Point3d = Point3d{16, 16, 16}

func init() {
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
}

// Operation holds Voxel-specific data for processing chunks.
type Operation struct {
	VoxelHandler
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

// Voxels provide the shape and data of a set of voxels as well as some 3d indexing.
type VoxelHandler interface {
	Geometry

	VoxelFormat() (channels, bytesPerChannel int32)

	BytesPerVoxel() int32

	ByteOrder() binary.ByteOrder

	Data() []uint8

	Stride() int32

	BlockIndex(blockX, blockY, blockZ int32) ZYXIndexer
}

// -------  VoxelHandler interface implementation -------------

// Voxels represents subvolumes or slices.
type Voxels struct {
	Geometry

	channels        int32
	bytesPerChannel int32

	// The data itself
	data []uint8

	// The stride for 2d iteration.  For 3d subvolumes, we don't reuse standard Go
	// images but maintain fully packed data slices, so stride isn't necessary.
	stride int32

	byteOrder binary.ByteOrder
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %d x %d x %d @ %s",
		v.DataShape(), size[0], size[1], size[2], v.StartVoxel())
}

func (v *Voxels) Data() []uint8 {
	return v.data
}

func (v *Voxels) Stride() int32 {
	return v.stride
}

func (v *Voxels) BlockIndex(x, y, z int32) ZYXIndexer {
	return &IndexZYX{x, y, z}
}

func (v *Voxels) VoxelFormat() (channels, bytesPerChannel int32) {
	return v.channels, v.bytesPerChannel
}

func (v *Voxels) BytesPerVoxel() int32 {
	return v.channels * v.bytesPerChannel
}

func (v *Voxels) ByteOrder() binary.ByteOrder {
	return v.byteOrder
}

// Datatype embeds the datastore's Datatype to create a unique type
// with voxel functions.  Refinements of general voxel types can be implemented
// by embedding this type, choosing appropriate # of channels and bytes/voxel,
// overriding functions as needed, and calling datastore.RegisterDatatype().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Datatype struct {
	datastore.Datatype

	// channels specifies the # channels interleaved within a voxel.
	channels int32

	// bytesPerChannel gives the # of bytes/channel/voxel
	bytesPerChannel int32
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewDatatype(channelsPerVoxel, bytesPerChannel int32) (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.channels = channelsPerVoxel
	dtype.bytesPerChannel = bytesPerChannel
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Datatype) NewDataService(dset *datastore.Dataset, id *datastore.DataID,
	config dvid.Config) (service datastore.DataService, err error) {

	var basedata *datastore.Data
	basedata, err = datastore.NewDataService(id, dtype, config)
	if err != nil {
		return
	}
	data := &Data{
		Data:             basedata,
		ChannelsPerVoxel: dtype.channels,
		BytesPerChannel:  dtype.bytesPerChannel,
	}

	data.BlockSize = DefaultBlockMax
	var s string
	var found bool
	s, found, err = config.GetString("BlockSize")
	if err != nil {
		return
	}
	if found {
		data.BlockSize, err = PointStr(s).Point3d()
		if err != nil {
			err = fmt.Errorf("BlockSize setting is not a 3d point: %s [%s]", s, err.Error())
			return
		}
	}
	data.VoxelRes = VoxelResolution{1.0, 1.0, 1.0}
	s, found, err = config.GetString("VoxelRes")
	if err != nil {
		return
	}
	if found {
		data.VoxelRes, err = VectorStr(s).VoxelResolution()
		if err != nil {
			err = fmt.Errorf("VoxelRes setting is not a 3d vector: %s [%s]", s, err.Error())
			return
		}
	}
	data.VoxelResUnits = "nanometers"
	s, found, err = config.GetString("VoxelResUnits")
	if err != nil {
		return
	}
	if found {
		data.VoxelResUnits = VoxelResolutionUnits(s)
	}
	service = data
	return
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockMax)
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data

	// ChannelsPerVoxel specifies the # channels interleaved within a voxel.
	ChannelsPerVoxel int32

	// BytesPerChannel gives the # of bytes/channel/voxel
	BytesPerChannel int32

	// Block size for this dataset
	BlockSize Point3d

	// Relative resolution of voxels in volume
	VoxelRes VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits VoxelResolutionUnits

	// The endianness of this loaded data.
	ByteOrder binary.ByteOrder

	// Maximum extents of data stored
	MinIndex ZYXIndexer
	MaxIndex ZYXIndexer
}

// MarshalJSON returns the JSON for this Data's configuration
func (d *Data) MarshalJSON() (m []byte, err error) {
	return json.Marshal(d)
}

// If img is passed in, newVoxels will initialize the VoxelHandler with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
func (d *Data) newVoxels(geom Geometry, img image.Image) (VoxelHandler, error) {
	bytesPerVoxel := d.ChannelsPerVoxel * d.BytesPerChannel
	stride := geom.Width() * bytesPerVoxel

	voxels := &Voxels{
		Geometry:        geom,
		channels:        d.ChannelsPerVoxel,
		bytesPerChannel: d.BytesPerChannel,
		stride:          stride,
	}

	if img == nil {
		voxels.data = make([]uint8, int64(bytesPerVoxel)*geom.NumVoxels())
	} else {
		var actualStride int32
		var err error
		voxels.data, actualStride, err = dvid.ImageData(img)
		if err != nil {
			return nil, err
		}
		if actualStride < stride {
			return nil, fmt.Errorf("Too little data in input image (%d stride bytes)", stride)
		}
	}
	return voxels, nil
}

// --- DataService interface ---

// Do acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "load" {
		return d.UnknownCommand(request)
	}
	if len(request.Command) < 7 {
		return fmt.Errorf("Poorly formatted load command.  See command-line help.")
	}
	source := request.Command[4]
	switch source {
	case "local":
		return d.LoadLocal(request, reply)
	case "remote":
		return fmt.Errorf("load remote not yet implemented")
	default:
		return d.UnknownCommand(request)
	}
	return nil
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

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil
	case "info":
		m, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(m))
		return nil
	default:
	}

	// Get the data shape.
	shapeStr := DataShapeString(parts[3])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape {
	case XY, XZ, YZ:
		sizeStr, offsetStr := parts[4], parts[5]
		slice, err := NewSliceFromStrings(shapeStr, offsetStr, sizeStr)
		if err != nil {
			return err
		}
		if op == PutOp {
			// TODO -- Put in format checks for POSTed image.
			postedImg, _, err := dvid.ImageFromPost(r, "image")
			if err != nil {
				return err
			}
			v, err := d.newVoxels(slice, postedImg)
			if err != nil {
				return err
			}
			err = d.PutImage(versionID, v)
			if err != nil {
				return err
			}
		} else {
			v, err := d.newVoxels(slice, nil)
			if err != nil {
				return err
			}
			img, err := d.GetImage(versionID, v)
			if err != nil {
				return err
			}
			var formatStr string
			if len(parts) >= 7 {
				formatStr = parts[6]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img, formatStr)
			if err != nil {
				return err
			}
		}
	case Vol:
		sizeStr, offsetStr := parts[4], parts[5]
		_, err := NewSubvolumeFromStrings(offsetStr, sizeStr)
		if err != nil {
			return err
		}
		if op == GetOp {
			return fmt.Errorf("DVID does not yet support GET of thrift-encoded volume data")
			/*
				if data, err := d.GetVolume(uuidStr, subvol); err != nil {
					return err
				} else {
					w.Header().Set("Content-type", "application/x-protobuf")
					_, err = w.Write(data)
					if err != nil {
						return err
					}
				}
			*/
		} else {
			return fmt.Errorf("DVID does not yet support POST of thrift-encoded volume data")
		}
	case Arb:
		return fmt.Errorf("DVID does not yet support arbitrary planes.")
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s", r.Method, dataShape)
	return nil
}

// SliceImage returns an image.Image for the z-th slice of the voxel data.
// TODO -- Create more comprehensive handling of endianness and encoding of
// multibytes/voxel data into appropriate images.  Suggest using Thrift or
// cross-platform serialization than standard image formats for anything above
// 16-bits/voxel.
func (d *Data) SliceImage(v VoxelHandler, z int) (img image.Image, err error) {
	channels, bytesPerChannel := v.VoxelFormat()
	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			channels, bytesPerChannel)
	}
	sliceBytes := int(v.Width() * v.Height() * channels * bytesPerChannel)
	beg := z * sliceBytes
	end := beg + sliceBytes
	data := v.Data()
	if end > len(data) {
		err = fmt.Errorf("SliceImage() called with z = %d greater than %s", z, v)
		return
	}
	r := image.Rect(0, 0, int(v.Width()), int(v.Height()))
	switch channels {
	case 1:
		switch bytesPerChannel {
		case 1:
			img = &image.Gray{data[beg:end], 1 * r.Dx(), r}
		case 2:
			bigendian, err := littleToBigEndian(v, data[beg:end])
			if err != nil {
				return nil, err
			}
			img = &image.Gray16{bigendian, 2 * r.Dx(), r}
		case 4:
			img = &image.RGBA{data[beg:end], 4 * r.Dx(), r}
		case 8:
			img = &image.RGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			err = unsupported()
		}
	case 4:
		switch bytesPerChannel {
		case 1:
			img = &image.RGBA{data[beg:end], 4 * r.Dx(), r}
		case 2:
			img = &image.RGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			err = unsupported()
		}
	default:
		err = unsupported()
	}
	return
}

// LoadLocal adds image data to a version node.  Command-line usage is as follows:
//
// $ dvid node <UUID> <data name> load local  <plane> <offset> <image glob>
// $ dvid node <UUID> <data name> load remote <plane> <offset> <image glob>
//
//     Adds image data to a version node when the server can see the local files ("local")
//     or when the server must be sent the files via rpc ("remote").
//
//     Example:
//
//     $ dvid node 3f8c mygrayscale load local xy 0,0,100 data/*.png
//
//     Arguments:
//
//     UUID          Hexidecimal string with enough characters to uniquely identify a version node.
//     data name     Name of data to add.
//     plane         One of "xy", "xz", or "yz".
//     offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
//     image glob    Filenames of images, e.g., foo-xy-*.png
//
// The image filename glob MUST BE absolute file paths that are visible to the server.
// This function is meant for mass ingestion of large data files, and it is inappropriate
// to read gigabytes of data just to send it over the network to a local DVID.
func (d *Data) LoadLocal(request datastore.Request, reply *datastore.Response) error {
	startTime := time.Now()

	// Get the running datastore service from this DVID instance.
	service := server.DatastoreService()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceStr, planeStr, offsetStr string
	filenames := request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceStr,
		&planeStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
	}

	// Get the version ID from a uniquely identifiable string
	_, _, versionID, err := service.NodeIDFromString(uuidStr)
	if err != nil {
		return fmt.Errorf("Could not find node with UUID %s: %s", uuidStr, err.Error())
	}

	// Get origin
	offset, err := PointStr(offsetStr).Coord()
	if err != nil {
		return fmt.Errorf("Illegal offset specification: %s: %s", offsetStr, err.Error())
	}

	// Get list of files to add
	var addedFiles string
	if len(filenames) == 1 {
		addedFiles = filenames[0]
	} else {
		addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}
	dvid.Log(dvid.Debug, addedFiles+"\n")

	// Get plane
	plane, err := DataShapeString(planeStr).DataShape()
	if err != nil {
		fmt.Println("GetPlane")
		return err
	}

	// Load and PUT each image.
	numSuccessful := 0
	for _, filename := range filenames {
		sliceTime := time.Now()
		img, _, err := dvid.ImageFromFile(filename)
		if err != nil {
			return fmt.Errorf("Error after %d images successfully added: %s", err.Error())
		}
		size := SizeFromRect(img.Bounds())
		slice, err := NewSlice(plane, offset, size)
		if err != nil {
			return fmt.Errorf("Unable to determine slice: %s", err.Error())
		}
		v, err := d.newVoxels(slice, img)
		if err != nil {
			return err
		}
		err = d.PutImage(versionID, v)
		if err != nil {
			return err
		}
		dvid.ElapsedTime(dvid.Debug, sliceTime, "%s load local %s", d.DataName(), slice)
		numSuccessful++
		offset = offset.Add(Coord{0, 0, 1})
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "RPC load local (%s) completed", addedFiles)
	return nil
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetImage(versionID datastore.VersionLocalID, v VoxelHandler) (img image.Image, err error) {
	db := server.KeyValueDB()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
	}

	op := Operation{v, GetOp}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&op, wg}

	// Setup traversal
	startVoxel := v.StartVoxel()
	endVoxel := v.EndVoxel()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(d.BlockSize)
	endBlockCoord := endVoxel.BlockCoord(d.BlockSize)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := v.BlockIndex(startBlockCoord[0], y, z)
			i1 := v.BlockIndex(endBlockCoord[0], y, z)
			startKey := &datastore.DataKey{d.DsetID, d.ID, versionID, i0}
			endKey := &datastore.DataKey{d.DsetID, d.ID, versionID, i1}

			// Send the entire range of key/value pairs to ProcessChunk()
			err = db.ProcessRange(startKey, endKey, chunkOp, d.ProcessChunk)
			if err != nil {
				err = fmt.Errorf("Unable to GET data %s: %s", d.DataName(), err.Error())
				return
			}
		}
	}

	// Reduce: Grab the resulting 2d image.
	wg.Wait()
	img, err = d.SliceImage(v, 0)
	return
}

// PutImage adds a 2d image within given geometry to a version node.   Since chunk sizes
// are larger than a 2d slice, this also requires integrating this image into current
// chunks before writing result back out, so it's a PUT for nonexistant keys and GET/PUT
// for existing keys.
func (d *Data) PutImage(versionID datastore.VersionLocalID, v VoxelHandler) error {
	db := server.KeyValueDB()
	if db == nil {
		return fmt.Errorf("Did not find a working key-value datastore to put image!")
	}

	op := Operation{v, PutOp}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&op, wg}

	// Setup traversal
	startVoxel := v.StartVoxel()
	endVoxel := v.EndVoxel()

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	versionMutex := datastore.VersionMutex(d, versionID)
	versionMutex.Lock()
	defer versionMutex.Unlock()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(d.BlockSize)
	endBlockCoord := endVoxel.BlockCoord(d.BlockSize)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := v.BlockIndex(startBlockCoord[0], y, z)
			i1 := v.BlockIndex(endBlockCoord[0], y, z)
			startKey := &datastore.DataKey{d.DsetID, d.ID, versionID, i0}
			endKey := &datastore.DataKey{d.DsetID, d.ID, versionID, i1}

			// Expand stored extents if necessary.
			if d.MinIndex == nil {
				d.MinIndex = i0
			} else {
				d.MinIndex.ExtendMin(i0)
			}
			if d.MaxIndex == nil {
				d.MaxIndex = i1
			} else {
				d.MaxIndex.ExtendMax(i1)
			}

			// GET all the chunks for this range.
			keyvalues, err := db.GetRange(startKey, endKey)
			if err != nil {
				return fmt.Errorf("Error in reading data during PUT %s: %s",
					d.DataName(), err.Error())
			}

			// Send all data to chunk handlers for this range.
			var kv, oldkv storage.KeyValue
			numOldkv := len(keyvalues)
			oldI := 0
			if numOldkv > 0 {
				oldkv = keyvalues[oldI]
			}
			wg.Add(int(endBlockCoord[0]-startBlockCoord[0]) + 1)
			for x := startBlockCoord[0]; x <= endBlockCoord[0]; x++ {
				i := v.BlockIndex(x, y, z)
				key := &datastore.DataKey{d.DsetID, d.ID, versionID, i}
				// Check for this key among old key-value pairs and if so,
				// send the old value into chunk handler.
				if oldkv.K != nil {
					zyx, err := KeyToZYXIndexer(oldkv.K)
					if err != nil {
						return err
					}
					if zyx.X() == x {
						kv = oldkv
						oldI++
						if oldI < numOldkv {
							oldkv = keyvalues[oldI]
						} else {
							oldkv.K = nil
						}
					} else {
						kv = storage.KeyValue{K: key}
					}
				} else {
					kv = storage.KeyValue{K: key}
				}
				// TODO -- Pass batch write via chunkOp and group all PUTs
				// together at once.  Should increase write speed, particularly
				// since the PUTs are using mostly sequential keys.
				d.ProcessChunk(&storage.Chunk{chunkOp, kv})
			}
		}
	}
	wg.Wait()

	return nil
}

/*
func (d *Data) GetVolume(versionID dvid.LocalID, vol Geometry) (data []byte, err error) {
	startTime := time.Now()

	bytesPerVoxel := d.BytesPerVoxel()
	numBytes := int64(bytesPerVoxel) * vol.NumVoxels()
	voldata := make([]uint8, numBytes, numBytes)
	operation := d.makeOp(&Voxels{vol, voldata, 0}, versionID, GetOp)

	// Perform operation using mapping
	err = operation.Map()
	if err != nil {
		return
	}
	operation.Wait()

	// server.Subvolume is a thrift-defined data structure
	encodedVol := &server.Subvolume{
		Data:    proto.String(string(d.DataName())),
		OffsetX: proto.Int32(operation.data.Geometry.StartVoxel()[0]),
		OffsetY: proto.Int32(operation.data.Geometry.StartVoxel()[1]),
		OffsetZ: proto.Int32(operation.data.Geometry.StartVoxel()[2]),
		SizeX:   proto.Uint32(uint32(operation.data.Geometry.Size()[0])),
		SizeY:   proto.Uint32(uint32(operation.data.Geometry.Size()[1])),
		SizeZ:   proto.Uint32(uint32(operation.data.Geometry.Size()[2])),
		Data:    []byte(operation.data.data),
	}
	data, err = proto.Marshal(encodedVol)

	dvid.ElapsedTime(dvid.Normal, startTime, "%s %s (%s) %s", GetOp, operation.DataName(),
		operation.DatatypeName(), operation.data.Geometry)

	return
}
*/

// ProcessChunk processes a chunk of data as part of a mapped operation.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc).
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) ProcessChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.processChunk(chunk)
}

func (d *Data) processChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1
	}()

	//dvid.PrintNonZero("processChunk", chunk.V)

	op, ok := chunk.Op.(*Operation)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}
	index, err := KeyToZYXIndexer(chunk.K)
	if err != nil {
		log.Fatalf("Data %s: %s\n", d.DataName(), err.Error())
	}

	// Compute the bounding voxel coordinates for this block.
	blockSize := d.BlockSize
	minBlockVoxel := index.OffsetToBlock(blockSize)
	maxBlockVoxel := minBlockVoxel.AddSize(blockSize)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := op.StartVoxel()
	maxDataVoxel := op.EndVoxel()
	begVolCoord := minDataVoxel.Max(minBlockVoxel)
	endVolCoord := maxDataVoxel.Min(maxBlockVoxel)

	// Calculate the strides
	bytesPerVoxel := op.BytesPerVoxel()
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * bytesPerVoxel)

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel)

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var block []uint8
	if chunk == nil || chunk.V == nil {
		block = make([]uint8, blockBytes)
	} else {
		// Deserialize
		data, _, err := dvid.DeserializeData(chunk.V, true)
		if err != nil {
			log.Fatalf("Unable to deserialize chunk from dataset '%s': %s\n",
				d.DataName(), err.Error())
		}
		block = []uint8(data)
		if len(block) != blockBytes {
			log.Fatalf("Retrieved block for dataset '%s' is %d bytes, not %d block size!\n",
				d.DataName(), len(block), blockBytes)
		}
	}

	// Compute index into the block byte buffer, blockI
	blockNumX := blockSize[0] * bytesPerVoxel
	blockNumXY := blockSize[1] * blockNumX

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	beg := begVolCoord.Sub(op.StartVoxel())
	end := endVolCoord.Sub(op.StartVoxel())

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.
	data := op.Data()

	//fmt.Printf("Data %s -> %s, Orig %s -> %s\n", beg, end, begVolCoord, endVolCoord)
	//fmt.Printf("Block start: %s\n", blockBeg)
	//fmt.Printf("Block buffer size: %d bytes\n", len(block))
	//fmt.Printf("Data buffer size: %d bytes\n", len(data))

	switch op.DataShape() {
	case XY:
		//fmt.Printf("XY Block: %s->%s, blockXY %d, blockX %d, blockBeg %s\n",
		//	begVolCoord, endVolCoord, blockNumXY, blockNumX, blockBeg)
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[1]*op.Stride() + beg[0]*bytesPerVoxel
		for y := beg[1]; y <= end[1]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch op.OpType {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * bytesPerVoxel
			dataI += op.Stride()
		}
		//dvid.PrintNonZero("After copy", data)
	case XZ:
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[2]*op.Stride() + beg[0]*bytesPerVoxel
		for y := beg[2]; y <= end[2]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch op.OpType {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * blockSize[1] * bytesPerVoxel
			dataI += op.Stride()
		}
	case YZ:
		bx, bz := blockBeg[0], blockBeg[2]
		for y := beg[2]; y <= end[2]; y++ {
			dataI := y*op.Stride() + beg[1]*bytesPerVoxel
			blockI := bz*blockNumXY + blockBeg[1]*blockNumX + bx*bytesPerVoxel
			for x := beg[1]; x <= end[1]; x++ {
				switch op.OpType {
				case GetOp:
					copy(data[dataI:dataI+bytesPerVoxel], block[blockI:blockI+bytesPerVoxel])
				case PutOp:
					copy(block[blockI:blockI+bytesPerVoxel], data[dataI:dataI+bytesPerVoxel])
				}
				blockI += blockNumX
				dataI += bytesPerVoxel
			}
			bz++
		}
	case Vol:
		dataNumX := op.Width() * bytesPerVoxel
		dataNumXY := op.Height() * dataNumX
		blockZ := blockBeg[2]
		for dataZ := beg[2]; dataZ <= end[2]; dataZ++ {
			blockY := blockBeg[1]
			for dataY := beg[1]; dataY <= end[1]; dataY++ {
				blockI := blockZ*blockNumXY + blockY*blockNumX + blockBeg[0]*bytesPerVoxel
				dataI := dataZ*dataNumXY + dataY*dataNumX + beg[0]*bytesPerVoxel
				run := end[0] - beg[0] + 1
				bytes := run * bytesPerVoxel
				switch op.OpType {
				case GetOp:
					copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
				case PutOp:
					copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
				}
				blockY++
			}
			blockZ++
		}

	default:
	}

	//dvid.PrintNonZero(op.OpType.String(), block)

	// If this is a PUT, place the modified block data into the database.
	if op.OpType == PutOp {
		db := server.KeyValueDB()
		serialization, err := dvid.SerializeData([]byte(block), dvid.Snappy, dvid.CRC32)
		if err != nil {
			fmt.Printf("Unable to serialize block: %s\n", err.Error())
		}
		db.Put(chunk.K, serialization)
	}

	// Notify the requestor that this chunk is done.
	if chunk.Wg != nil {
		chunk.Wg.Done()
	}
}
