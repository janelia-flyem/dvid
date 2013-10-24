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

    GET /api/node/3f8c/grayscale/0_1/512_256/0_0_100/jpg:80

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD thrift-encoded data will be "application/x-thrift".

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
                  nD: "thrift" (default: "thrift")

(TO DO)

GET  /api/node/<UUID>/<data name>/arb/<center>/<normal>/<size>[/<format>]

    Retrieves non-orthogonal (arbitrarily oriented planar) image data of named 3d data 
    within a version node.

    Example: 

    GET /api/node/3f8c/grayscale/arb/200_200/2.0_1.3_1/100_100/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    center        3d coordinate in the format "x_y_z".  Gives 3d coord of center pixel.
    normal        3d vector in the format "nx_ny_nz".  Gives normal vector of image.
    size          Size in pixels in the format "dx_dy".
    format        "png", "jpg" (default: "png")  
                    jpg allows lossy quality setting, e.g., "jpg:80"
`

var (
	// DefaultBlockSize specifies the default size for each block of this data type.
	DefaultBlockSize = dvid.Point3d{16, 16, 16}

	DefaultVoxelRes = dvid.NdFloat32{10.0, 10.0, 10.0}

	DefaultVoxelResUnits = dvid.NdString{"nanometers", "nanometers", "nanometers"}
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
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
	dvid.Geometry

	VoxelFormat() (channels, bytesPerValue int32)

	BytesPerVoxel() int32

	ByteOrder() binary.ByteOrder

	Data() []uint8

	Stride() int32

	Index(p dvid.Point) dvid.Index

	IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error)
}

// Handler conversion of little to big endian for voxels larger than 1 byte.
func littleToBigEndian(v VoxelHandler, data []uint8) (bigendian []uint8, err error) {
	if v.ByteOrder() == nil || v.ByteOrder() == binary.BigEndian || v.BytesPerVoxel() == 1 {
		return data, nil
	}
	bigendian = make([]uint8, len(data))
	switch v.BytesPerVoxel() {
	case 2:
		for beg := 0; beg < len(data)-1; beg += 2 {
			bigendian[beg], bigendian[beg+1] = data[beg+1], data[beg]
		}
	case 4:
		for beg := 0; beg < len(data)-3; beg += 4 {
			value := binary.LittleEndian.Uint32(data[beg : beg+4])
			binary.BigEndian.PutUint32(bigendian[beg:beg+4], value)
		}
	case 8:
		for beg := 0; beg < len(data)-7; beg += 8 {
			value := binary.LittleEndian.Uint64(data[beg : beg+8])
			binary.BigEndian.PutUint64(bigendian[beg:beg+8], value)
		}
	}
	return
}

// -------  VoxelHandler interface implementation -------------

// Voxels represents subvolumes or slices.
type Voxels struct {
	dvid.Geometry

	valuesPerVoxel int32
	bytesPerValue  int32

	// The data itself
	data []uint8

	// The stride for 2d iteration.  For 3d subvolumes, we don't reuse standard Go
	// images but maintain fully packed data slices, so stride isn't necessary.
	stride int32

	byteOrder binary.ByteOrder
}

// NewVoxels returns a Voxels struct from given parameters.
func NewVoxels(geom dvid.Geometry, valuesPerVoxel, bytesPerValue int32, data []uint8,
	byteOrder binary.ByteOrder) *Voxels {

	bytesPerVoxel := valuesPerVoxel * bytesPerValue
	stride := geom.Size().Value(0) * bytesPerVoxel
	return &Voxels{
		Geometry:       geom,
		valuesPerVoxel: valuesPerVoxel,
		bytesPerValue:  bytesPerValue,
		data:           data,
		stride:         stride,
		byteOrder:      byteOrder,
	}
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %s @ %s", v.DataShape(), size, v.StartPoint())
}

func (v *Voxels) Data() []uint8 {
	return v.data
}

func (v *Voxels) Stride() int32 {
	return v.stride
}

func (v *Voxels) VoxelFormat() (valuesPerVoxel, bytesPerValue int32) {
	return v.valuesPerVoxel, v.bytesPerValue
}

func (v *Voxels) BytesPerVoxel() int32 {
	return v.valuesPerVoxel * v.bytesPerValue
}

func (v *Voxels) ByteOrder() binary.ByteOrder {
	return v.byteOrder
}

func (v *Voxels) Index(p dvid.Point) dvid.Index {
	return dvid.IndexZYX(p.(dvid.Point3d))
}

// IndexIterator returns an iterator that can move across the voxel geometry,
// generating indices or index spans.
func (v *Voxels) IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error) {
	// Setup traversal
	begVoxel, ok := v.StartPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("VoxelHandler StartPoint() cannot handle Chunkable points.")
	}
	endVoxel, ok := v.EndPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("VoxelHandler EndPoint() cannot handle Chunkable points.")
	}
	begBlock := begVoxel.Chunk(chunkSize).(dvid.Point3d)
	endBlock := endVoxel.Chunk(chunkSize).(dvid.Point3d)

	return dvid.NewIndexZYXIterator(v.Geometry, begBlock, endBlock), nil
}

// Datatype embeds the datastore's Datatype to create a unique type
// with voxel functions.  Refinements of general voxel types can be implemented
// by embedding this type, choosing appropriate # of values and bytes/value,
// overriding functions as needed, and calling datastore.RegisterDatatype().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Datatype struct {
	datastore.Datatype

	// The number of values associated with a voxel.
	// For example, a RGBA color has four values.
	valuesPerVoxel int32

	// bytesPerValue gives the # of bytes/value/voxel
	bytesPerValue int32
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewDatatype(valuesPerVoxel, bytesPerValue int32) (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.valuesPerVoxel = valuesPerVoxel
	dtype.bytesPerValue = bytesPerValue
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, config dvid.Config) (
	service datastore.DataService, err error) {

	var basedata *datastore.Data
	basedata, err = datastore.NewDataService(id, dtype, config)
	if err != nil {
		return
	}
	data := &Data{
		Data:           basedata,
		ValuesPerVoxel: dtype.valuesPerVoxel,
		BytesPerValue:  dtype.bytesPerValue,
	}

	var s string
	var found bool
	s, found, err = config.GetString("BlockSize")
	if err != nil {
		return
	}
	if found {
		data.BlockSize, err = dvid.StringToPoint(s, ",")
		if err != nil {
			return
		}
	} else {
		data.BlockSize = DefaultBlockSize
	}
	s, found, err = config.GetString("VoxelRes")
	if err != nil {
		return
	}
	if found {
		data.VoxelRes, err = dvid.StringToNdFloat32(s, ",")
		if err != nil {
			return
		}
	} else {
		data.VoxelRes = DefaultVoxelRes
	}
	s, found, err = config.GetString("VoxelResUnits")
	if err != nil {
		return
	}
	if found {
		data.VoxelResUnits, err = dvid.StringToNdString(s, ",")
		if err != nil {
			return
		}
	} else {
		data.VoxelResUnits = DefaultVoxelResUnits
	}
	service = data
	return
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data

	// ValuesPerVoxel specifies the # values interleaved within a voxel.
	ValuesPerVoxel int32

	// BytesPerValue gives the # of bytes/value/voxel
	BytesPerValue int32

	// Block size for this dataset
	BlockSize dvid.Point

	// Resolution of voxels in volume
	VoxelRes dvid.NdFloat32

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.NdString

	// The endianness of this loaded data.
	ByteOrder binary.ByteOrder

	// Maximum extents of data stored
	MinIndex dvid.PointIndexer
	MaxIndex dvid.PointIndexer
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// If img is passed in, newVoxels will initialize the VoxelHandler with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
func (d *Data) NewVoxelHandler(geom dvid.Geometry, img image.Image) (VoxelHandler, error) {
	bytesPerVoxel := d.ValuesPerVoxel * d.BytesPerValue
	stride := geom.Size().Value(0) * bytesPerVoxel

	voxels := &Voxels{
		Geometry:       geom,
		valuesPerVoxel: d.ValuesPerVoxel,
		bytesPerValue:  d.BytesPerValue,
		stride:         stride,
		byteOrder:      d.ByteOrder,
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

// DoRPC acts as a switchboard for RPC commands.
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

// DoHTTP handles all incoming HTTP requests for this data.
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

	// Process help and info.
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
	default:
	}

	// Get the data shape.
	shapeStr := dvid.DataShapeString(parts[3])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape.ShapeDimensions() {
	case 2:
		sizeStr, offsetStr := parts[4], parts[5]
		slice, err := dvid.NewSliceFromStrings(shapeStr, offsetStr, sizeStr, "_")
		if err != nil {
			return err
		}
		if op == PutOp {
			// TODO -- Put in format checks for POSTed image.
			postedImg, _, err := dvid.ImageFromPost(r, "image")
			if err != nil {
				return err
			}
			v, err := d.NewVoxelHandler(slice, postedImg)
			if err != nil {
				return err
			}
			err = d.PutImage(uuid, v)
			if err != nil {
				return err
			}
		} else {
			v, err := d.NewVoxelHandler(slice, nil)
			if err != nil {
				return err
			}
			img, err := d.GetImage(uuid, v)
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
	case 3:
		sizeStr, offsetStr := parts[4], parts[5]
		_, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
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
	default:
		return fmt.Errorf("DVID currently supports shapes of only 2 and 3 dimensions")
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s", r.Method, dataShape)
	return nil
}

// SliceImage returns an image.Image for the z-th slice of the voxel data.
// TODO -- Create more comprehensive handling of endianness and encoding of
// multibytes/voxel data into appropriate images.  Suggest using Thrift or
// cross-platform serialization than standard image formats for anything above
// 16-bits/voxel.
func (d *Data) SliceImage(v VoxelHandler, z int32) (img image.Image, err error) {
	channels, bytesPerValue := v.VoxelFormat()
	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			channels, bytesPerValue)
	}
	width := v.Size().Value(0)
	height := v.Size().Value(1)
	sliceBytes := width * height * channels * bytesPerValue
	beg := z * sliceBytes
	end := beg + sliceBytes
	data := v.Data()
	if int(end) > len(data) {
		err = fmt.Errorf("SliceImage() called with z = %d greater than %s", z, v)
		return
	}
	r := image.Rect(0, 0, int(width), int(height))
	switch channels {
	case 1:
		switch bytesPerValue {
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
		switch bytesPerValue {
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
	uuid, _, _, err := service.NodeIDFromString(uuidStr)
	if err != nil {
		return fmt.Errorf("Could not find node with UUID %s: %s", uuidStr, err.Error())
	}

	// Get offset
	offset, err := dvid.StringToPoint(offsetStr, ",")
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
	plane, err := dvid.DataShapeString(planeStr).DataShape()
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
		slice, err := dvid.NewOrthogSlice(plane, offset, dvid.RectSize(img.Bounds()))
		if err != nil {
			return fmt.Errorf("Unable to determine slice: %s", err.Error())
		}
		v, err := d.NewVoxelHandler(slice, img)
		if err != nil {
			return err
		}
		err = d.PutImage(uuid, v)
		if err != nil {
			return err
		}
		dvid.ElapsedTime(dvid.Debug, sliceTime, "%s load local %s", d.DataName(), slice)
		numSuccessful++
		offset = offset.Add(dvid.Point3d{0, 0, 1})
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "RPC load local (%s) completed", addedFiles)
	return nil
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetImage(uuid datastore.UUID, v VoxelHandler) (img image.Image, err error) {
	db := server.StorageEngine()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
	}

	op := Operation{v, GetOp}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&op, wg}

	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)

	for it, err := v.IndexIterator(d.BlockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return nil, err
		}
		startKey := &datastore.DataKey{d.DsetID, d.ID, versionID, indexBeg}
		endKey := &datastore.DataKey{d.DsetID, d.ID, versionID, indexEnd}

		// Send the entire range of key/value pairs to ProcessChunk()
		err = db.ProcessRange(startKey, endKey, chunkOp, d.ProcessChunk)
		if err != nil {
			return nil, fmt.Errorf("Unable to GET data %s: %s", d.DataName(), err.Error())
		}
	}
	if err != nil {
		return
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
func (d *Data) PutImage(uuid datastore.UUID, v VoxelHandler) error {
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		return err
	}

	db := server.StorageEngine()
	if db == nil {
		return fmt.Errorf("Did not find a working key-value datastore to put image!")
	}

	op := Operation{v, PutOp}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&op, wg}

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	versionMutex := datastore.VersionMutex(d, versionID)
	versionMutex.Lock()
	defer versionMutex.Unlock()

	// Keep track of changing extents and mark dataset as dirty if changed.
	var extentChanged bool
	defer func() {
		if extentChanged {
			err := service.SaveDataset(uuid)
			if err != nil {
				dvid.Log(dvid.Normal, "Error in trying to save dataset on change: %s\n", err.Error())
			}
		}
	}()

	// Iterate through index space for this data.
	for it, err := v.IndexIterator(d.BlockSize); err == nil && it.Valid(); it.NextSpan() {
		i0, i1, err := it.IndexSpan()
		if err != nil {
			return err
		}
		indexBeg := i0.Duplicate().(dvid.PointIndexer)
		indexEnd := i1.Duplicate().(dvid.PointIndexer)

		begX := indexBeg.Value(0)
		endX := indexEnd.Value(0)

		// Expand stored extents if necessary.
		var changed bool
		if d.MinIndex == nil {
			d.MinIndex = indexBeg
			extentChanged = true
		} else {
			d.MinIndex, changed = d.MinIndex.Min(indexBeg)
			if changed {
				extentChanged = true
			}
		}
		if d.MaxIndex == nil {
			d.MaxIndex = indexEnd
			extentChanged = true
		} else {
			d.MaxIndex, changed = d.MaxIndex.Max(indexEnd)
			if changed {
				extentChanged = true
			}
		}

		startKey := &datastore.DataKey{d.DsetID, d.ID, versionID, indexBeg}
		endKey := &datastore.DataKey{d.DsetID, d.ID, versionID, indexEnd}

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
		wg.Add(int(endX-begX) + 1)
		p := dvid.Point3d{begX, indexBeg.Value(1), indexBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			p[0] = x
			key := &datastore.DataKey{d.DsetID, d.ID, versionID, v.Index(p)}
			// Check for this key among old key-value pairs and if so,
			// send the old value into chunk handler.
			if oldkv.K != nil {
				indexer, err := datastore.KeyToPointIndexer(oldkv.K)
				if err != nil {
					return err
				}
				if indexer.Value(0) == x {
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
		OffsetX: proto.Int32(operation.data.Geometry.StartPoint()[0]),
		OffsetY: proto.Int32(operation.data.Geometry.StartPoint()[1]),
		OffsetZ: proto.Int32(operation.data.Geometry.StartPoint()[2]),
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
	index, err := datastore.KeyToPointIndexer(chunk.K)
	if err != nil {
		log.Fatalf("Data %s: %s\n", d.DataName(), err.Error())
	}

	// Compute the bounding voxel coordinates for this block.
	blockSize := d.BlockSize.(dvid.Point3d)
	minBlockVoxel := index.PointInChunk(blockSize)
	maxBlockVoxel := minBlockVoxel.Add(blockSize.Sub(dvid.Point3d{1, 1, 1}))
	//fmt.Printf("MinBlockVoxel: %s, MaxBlockVoxel: %s\n", minBlockVoxel, maxBlockVoxel)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := op.StartPoint()
	maxDataVoxel := op.EndPoint()
	begVolCoord := minDataVoxel.Max(minBlockVoxel)
	endVolCoord := maxDataVoxel.Min(maxBlockVoxel)
	//fmt.Printf("minDataVoxel: %s, maxDataVoxel: %s\n", minDataVoxel, maxDataVoxel)
	//fmt.Printf("begVolCoord: %s, endVolCoord: %s\n", begVolCoord, endVolCoord)

	// Calculate the strides
	bytesPerVoxel := op.BytesPerVoxel()
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * bytesPerVoxel)

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel).(dvid.Point3d)

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
	beg := begVolCoord.Sub(op.StartPoint()).(dvid.Point3d)
	end := endVolCoord.Sub(op.StartPoint()).(dvid.Point3d)

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.
	data := op.Data()

	//fmt.Printf("Data %s -> %s, Orig %s -> %s\n", beg, end, begVolCoord, endVolCoord)
	//fmt.Printf("Block start: %s\n", blockBeg)
	//fmt.Printf("Block buffer size: %d bytes\n", len(block))
	//fmt.Printf("Data buffer size: %d bytes\n", len(data))

	switch {
	case op.DataShape().Equals(dvid.XY):
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
	case op.DataShape().Equals(dvid.XZ):
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
	case op.DataShape().Equals(dvid.YZ):
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
	case op.DataShape().Equals(dvid.Vol3d):
		dataNumX := op.Size().Value(0) * bytesPerVoxel
		dataNumXY := op.Size().Value(1) * dataNumX
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
		db := server.StorageEngine()
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
