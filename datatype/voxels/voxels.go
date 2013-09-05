/*
	Package voxels implements DVID support for data using voxels as elements.
	A number of data types will embed this package and customize it using the
	"NumChannels" and "BytesPerVoxel" fields.
*/
package voxels

import (
	"encoding/gob"
	"fmt"
	"image"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	// "github.com/janelia-flyem/go/goprotobuf/proto"
)

const Version = "0.7"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/voxels"

const HelpMessage = `
Server-side commands for datasets with voxels data type:

1) Add local files to an image version

    <dataset name> add <uuid> <origin> <image filename glob> [plane=<plane>]

    Example: 

    $ dvid mygrayscale add 3f8c 0,0,100 data/*.png 

Arguments:

    dataset name: the name of a dataset created using the "dataset" command.
    uuid: hexidecimal string with enough characters to uniquely identify a version node.
    image filename glob: filenames of images, e.g., foo-xy-*.png
    origin: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    plane: xy (default), xz, or yz.

2) Add files accessible to server to an image version

    <dataset name> server-add <uuid> <origin> <image filename glob> [plane=<plane>]

    Example: 

    $ dvid mygrayscale server-add 3f8c 0,0,100 /absolute/path/to/data/*.png 

Arguments: same as #1

    NOTE: The image filename glob MUST BE absolute file paths that are visible to
    the server.  The 'server-add' command is meant for mass ingestion of large data files, 
    and it is inappropriate to read gigabytes of data just to send it over the network to
    a local DVID.
	
    ------------------

HTTP API for datasets with voxels data type:

The voxel data type supports the following Level 2 REST HTTP API calls.

    1) Adding different voxel data sets
	
        POST /api/data/<voxel type>/<data set name>

    Parameters:
    <voxel type> = the abbreviated data type name, e.g., "grayscale8"
    <data set name> = name of a data set of this type, e.g., "mygrayscale"

    Examples:

        POST /api/data/grayscale8/mygrayscale

    Adds a data set name "rgb8" that uses the voxels data type at 8 bits/voxel
    and with 3 channels.

    2) Storing and retrieving voxel data for a particular voxel data set

    If <data shape> is "xy", "xz", "yz", or "vol"

        POST /api/<data set name>/<uuid>/<data shape>/<offset>/<size>
        GET  /api/<data set name>/<uuid>/<data shape>/<offset>/<size>[/<format>]

    If <data shape> is "arb":

        GET  /api/<data set name>/<uuid>/<data shape>/<offset>/<tr>/<bl>[/<format>]

    Parameters:
    <data set name> = Data set name that is of type voxel, e.g., "rgb8" or "labels16"
    <uuid> = Hexadecimal number, as many digits as necessary to uniquely identify
       the UUID among nodes in this database.
    <data shape> = "xy", "xz", "yz", "arb", "vol"
    <offset> = x,y,z
    <size> = dx,dy,dz (if <volume> is "vol")
             dx,dy    (if <volume> is not "vol")
	<format> = "png", "jpg", "hdf5" (default: "png")  
       Jpeg allows lossy quality setting, e.g., "jpg:80"
	<tr> = x,y,z of top right corner of arbitrary slice
	<bl> = x,y,z of bottom left corner of arbitrary slice

    Examples:

        GET /api/myvoxels/c66e0/xy/0,125,135/250,240

    Returns a PNG image with width 250 pixels, height 240 pixels that were taken
    from the XY plane with the upper left pixel sitting at (0,125,135) in 
    the image volume space.  The # bits/voxel and the # of channels are determined
    by the type of data associated with the data set name 'myvoxels'
`

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax Point3d = Point3d{16, 16, 16}

func init() {
	gob.Register(&Datatype{})
	gob.Register(&Data{})
}

/// Datatype embeds the datastore's Datatype to create a unique type
// with voxel functions.  Refinements of general voxel types can be implemented
// by embedding this type, choosing appropriate # of channels and bytes/voxel,
// overriding functions as needed, and calling datastore.RegisterDatatype().
type Datatype struct {
	datastore.Datatype
	NumChannels   int
	BytesPerVoxel int
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewDatatype() (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.NumChannels = 1
	dtype.BytesPerVoxel = 1
	dtype.Requirements = storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    false,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Datatype) NewData(id datastore.DataID, config dvid.Config) datastore.DataService {
	data := &Data{
		Data: datastore.NewData(id, dtype),
	}
	data.BlockSize = DefaultBlockMax
	if obj, found := config["BlockSize"]; found {
		if blockSize, ok := obj.(Point3d); ok {
			data.BlockSize = blockSize
		}
	}
	data.VoxelRes = VoxelResolution{1.0, 1.0, 1.0}
	if obj, found := config["VoxelRes"]; found {
		if voxelRes, ok := obj.(VoxelResolution); ok {
			data.VoxelRes = voxelRes
		}
	}
	data.VoxelResUnits = "nanometers"
	if obj, found := config["VoxelResUnits"]; found {
		if res, ok := obj.(VoxelResolutionUnits); ok {
			data.VoxelResUnits = res
		}
	}
	data.StartChunkHandlers()
	return data
}

func (dtype *Datatype) Help() string {
	return HelpMessage
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data

	// Block size for this dataset
	BlockSize Point3d

	// Relative resolution of voxels in volume
	VoxelRes VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits VoxelResolutionUnits
}

// Returns total number of bytes across all channels for a voxel.
func (d *Data) BytesPerVoxel() int32 {
	// Make sure the dataset here has a pointer to a Datatype of this package.
	dtype, ok := d.Datatype.(*Datatype)
	if !ok {
		log.Fatalf("Illegal datatype %s for dataset %s",
			d.Datatype, d.DataName())
	}
	return int32(dtype.BytesPerVoxel * dtype.NumChannels)
}

func (d *Data) TypeService() datastore.TypeService {
	return d.Data.Datatype
}

// --- DataService interface ---

// Do acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "server-add":
		return d.ServerAdd(request, reply)
	//	case "get":
	//		return dtype.Get(request, reply)
	case "help":
		reply.Text = HelpMessage
	default:
		return d.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this dataset.
func (d *Data) DoHTTP(w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

	// Get the running datastore service from this DVID instance.
	dataService := server.DataService()

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

	// Get the version ID from a uniquely identifiable string
	uuidStr := parts[1]
	versionID, err := dataService.VersionIDFromString(uuidStr)
	if err != nil {
		return err
	}

	// Get the data shape.
	shapeStr := DataShapeString(parts[2])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape {
	case XY, XZ, YZ:
		slice, err := NewSliceFromStrings(parts[2], parts[3], parts[4])
		if err != nil {
			return err
		}
		if op == PutOp {
			// TODO -- Put in format checks for POSTed image.
			postedImg, _, err := dvid.ImageFromPost(r, "image")
			if err != nil {
				return err
			}
			err = d.PutImage(versionID, postedImg, slice)
			if err != nil {
				return err
			}
		} else {
			img, err := d.GetImage(versionID, slice)
			data, _, _ := dvid.ImageData(img)
			dvid.PrintNonZero("GetImage", data)

			var formatStr string
			if len(parts) >= 6 {
				formatStr = parts[5]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img, formatStr)
			if err != nil {
				return err
			}
		}
	case Vol:
		_, err := NewSubvolumeFromStrings(parts[3], parts[4])
		if err != nil {
			return err
		}
		if op == GetOp {
			/*
				if data, err := d.GetVolume(versionID, subvol); err != nil {
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
			return fmt.Errorf("DVID does not yet support POST of subvolume data.")
		}
	case Arb:
		return fmt.Errorf("DVID does not yet support arbitrary planes.")
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s", r.Method, dataShape)
	return nil
}

// Voxels represents subvolumes or slices.
type Voxels struct {
	Geometry

	// The data itself
	data []uint8

	// The stride for 2d iteration.  For 3d subvolumes, we don't reuse standard Go
	// images but maintain fully packed data slices, so stride isn't necessary.
	stride int32
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %d x %d x %d @ %s",
		v.DataShape(), size[0], size[1], size[2], v.Origin())
}

// Operation holds Voxel-specific data for processing chunks.
type Operation struct {
	data *Voxels
	vID  dvid.LocalID
	op   OpType
	wg   *sync.WaitGroup
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
)

// SliceImage returns an image.Image for the z-th slice of the volume.
func (d *Data) SliceImage(v *Voxels, z int) (img image.Image, err error) {
	dtype, ok := d.Datatype.(*Datatype)
	if !ok {
		err = fmt.Errorf("Data %s does not have type of voxels.Datatype!", d.DataName())
	}
	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images with %d bytes/voxel and %d channels",
			dtype.BytesPerVoxel, dtype.NumChannels)
	}
	sliceBytes := int(v.Width() * v.Height() * int32(d.BytesPerVoxel()))
	beg := z * sliceBytes
	end := beg + sliceBytes
	if end > len(v.data) {
		err = fmt.Errorf("SliceImage() called with z = %d greater than %s", z, v)
		return
	}
	r := image.Rect(0, 0, int(v.Width()), int(v.Height()))
	switch dtype.NumChannels {
	case 1:
		switch dtype.BytesPerVoxel {
		case 1:
			img = &image.Gray{v.data[beg:end], 1 * r.Dx(), r}
		case 2:
			img = &image.Gray16{v.data[beg:end], 2 * r.Dx(), r}
		case 4:
			img = &image.RGBA{v.data[beg:end], 4 * r.Dx(), r}
		case 8:
			img = &image.RGBA64{v.data[beg:end], 8 * r.Dx(), r}
		default:
			err = unsupported()
		}
	case 4:
		switch dtype.BytesPerVoxel {
		case 1:
			img = &image.RGBA{v.data[beg:end], 4 * r.Dx(), r}
		case 2:
			img = &image.RGBA64{v.data[beg:end], 8 * r.Dx(), r}
		default:
			err = unsupported()
		}
	default:
		err = unsupported()
	}
	return
}

// ServerAdd adds a sequence of image files to an image version.  The request
// contains arguments as follows:
//
// <dataset name> server-add <uuid> <origin> <image filename glob> [plane=<plane>]
//
// Example request string: "mygrayscale server-add 3f8c 0,0,100 /absolute/path/to/data/*.png"
//
//	dataset name: the name of a dataset created using the "dataset" command.
//	uuid: hexidecimal string with enough characters to uniquely identify a version node.
//	origin: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
//	image filename glob: filenames of images, e.g., foo-xy-*.png
//	plane: xy (default), xz, or yz.
//
// The image filename glob MUST BE absolute file paths that are visible to the server.
// This function is meant for mass ingestion of large data files, and it is inappropriate
// to read gigabytes of data just to send it over the network to a local DVID.
func (d *Data) ServerAdd(request datastore.Request, reply *datastore.Response) error {

	startTime := time.Now()

	// Get the running datastore service from this DVID instance.
	service := server.DataService()

	// Parse the request
	var setName, cmdStr, uuidStr, offsetStr string
	filenames := request.CommandArgs(0, &setName, &cmdStr, &uuidStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
	}

	// Get the version ID from a uniquely identifiable string
	versionID, err := service.VersionIDFromString(uuidStr)
	if err != nil {
		return err
	}

	// Get origin
	offset, err := PointStr(offsetStr).Coord()
	if err != nil {
		return err
	}

	// Get list of files to add
	var addedFiles string
	if len(filenames) == 1 {
		addedFiles = filenames[0]
	} else {
		addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}
	dvid.Log(dvid.Debug, addedFiles+"\n")

	// Get optional plane
	shapeStr, found := request.Parameter(dvid.KeyPlane)
	if !found {
		shapeStr = "xy"
	}
	dataShape, err := DataShapeString(shapeStr).DataShape()
	if err != nil {
		return err
	}

	// Load each image and do map/reduce computation on blocks of the image.

	numSuccessful := 0
	var lastErr error
	for _, filename := range filenames {
		sliceTime := time.Now()
		img, _, err := dvid.ImageFromFile(filename)
		if err != nil {
			lastErr = err
		} else {
			size := SizeFromRect(img.Bounds())
			slice, err := NewSlice(dataShape, offset, size)

			err = d.PutImage(versionID, img, slice)

			dvid.ElapsedTime(dvid.Debug, sliceTime, "%s server-add %s", d.DataName(), slice)
			if err == nil {
				numSuccessful++
			} else {
				lastErr = err
			}
		}
		offset = offset.Add(Coord{0, 0, 1})
	}
	if lastErr != nil {
		return fmt.Errorf("Error: %d of %d images successfully added [%s]\n",
			numSuccessful, len(filenames), lastErr.Error())
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetImage(vID dvid.LocalID, slice Geometry) (img image.Image, err error) {
	db := server.KeyValueDB()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
	}

	bytesPerVoxel := d.BytesPerVoxel()
	stride := slice.Width() * bytesPerVoxel

	numBytes := int64(bytesPerVoxel) * slice.NumVoxels()
	data := make([]uint8, numBytes, numBytes)
	dataID := d.DataLocalID()

	voxels := Voxels{slice, data, stride}

	wg := new(sync.WaitGroup)
	op := Operation{
		data: &voxels,
		vID:  vID,
		op:   PutOp,
		wg:   wg,
	}
	chunkOp := storage.ChunkOp{&op, wg}
	mapOp := storage.MapOp{chunkOp, d.Channels}

	blockSize := d.BlockSize

	// Setup traversal
	startVoxel := slice.Origin()
	endVoxel := slice.EndVoxel()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := IndexZYX{startBlockCoord[0], y, z}
			i1 := IndexZYX{endBlockCoord[0], y, z}
			startKey := &storage.Key{dataID, vID, i0}
			endKey := &storage.Key{dataID, vID, i1}

			// Send the entire range of key/value pairs to ProcessChunk()
			err = db.SendRange(startKey, endKey, &mapOp)
		}
	}

	// Reduce: Grab the resulting 2d image.
	wg.Wait()
	img, err = d.SliceImage(&voxels, 0)
	return
}

// PutImage adds a 2d image within given geometry to a version node.
func (d *Data) PutImage(vID dvid.LocalID, img image.Image, slice Geometry) error {
	db := server.KeyValueDB()
	if db == nil {
		return fmt.Errorf("Did not find a working key-value datastore to put image!")
	}

	bytesPerVoxel := d.BytesPerVoxel()
	stride := slice.Width() * bytesPerVoxel

	data, actualStride, err := dvid.ImageData(img)
	if err != nil {
		return err
	}
	if actualStride < stride {
		return fmt.Errorf("Too little data in input image (%d stride bytes)", stride)
	}
	dataID := d.DataLocalID()
	voxels := Voxels{slice, data, stride}

	op := Operation{
		data: &voxels,
		vID:  vID,
		op:   PutOp,
		wg:   nil,
	}
	chunkOp := storage.ChunkOp{&op, nil}
	mapOp := storage.MapOp{chunkOp, d.Channels}

	blockSize := d.BlockSize

	// Setup traversal
	startVoxel := slice.Origin()
	endVoxel := slice.EndVoxel()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := IndexZYX{startBlockCoord[0], y, z}
			i1 := IndexZYX{endBlockCoord[0], y, z}
			startKey := &storage.Key{dataID, vID, i0}
			endKey := &storage.Key{dataID, vID, i1}

			// Send the entire range of key/value pairs to ProcessChunk()
			err := db.SendRange(startKey, endKey, &mapOp)
			if err != nil {
				return fmt.Errorf("Unable to PUT to data %s", d.DataName())
			}
		}
	}
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
		OffsetX: proto.Int32(operation.data.Geometry.Origin()[0]),
		OffsetY: proto.Int32(operation.data.Geometry.Origin()[1]),
		OffsetZ: proto.Int32(operation.data.Geometry.Origin()[2]),
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
// ---- ChunkHandler interface ----

const ChannelBufferSize = 1000

// ChannelSpecs returns the number of chunk channels/handlers and
// the buffer size of a channel for Voxel data types.
func (d *Data) ChannelSpecs() (number, bufferSize int) {
	return runtime.NumCPU(), ChannelBufferSize
}

// StartChunkHandlers and StopChunkHandlers are inherited from embedded datastore.Data.

// ProcessChunk processes a chunk of data as part of a mapped operation.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc)
func (d *Data) ProcessChunk(chunk *storage.Chunk) {

	operation, ok := chunk.Op.(*Operation)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}
	voxels := operation.data
	index, ok := chunk.GetKey().Index.(IndexZYX)
	if !ok {
		log.Fatalf("Indexing for Voxel Chunk was not IndexZYX in data %s!\n", d.DataName())
	}

	// Compute the bounding voxel coordinates for this block.
	blockSize := d.BlockSize
	minBlockVoxel := index.OffsetToBlock(blockSize)
	maxBlockVoxel := minBlockVoxel.AddSize(blockSize)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := voxels.Origin()
	maxDataVoxel := voxels.EndVoxel()
	begVolCoord := minDataVoxel.Max(minBlockVoxel)
	endVolCoord := maxDataVoxel.Min(maxBlockVoxel)

	// Calculate the strides
	dtype := d.TypeService().(*Datatype)
	bytesPerVoxel := int32(dtype.NumChannels * dtype.BytesPerVoxel)

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel)

	// Allocate the block buffer if PUT or use passed-in chunk if GET
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * bytesPerVoxel)
	var block []uint8
	if chunk != nil {
		block = []uint8(chunk.GetValue())
		if len(block) != blockBytes {
			log.Fatalf("Retrieved block for dataset '%s' is %d bytes, not %d block size!\n",
				d.DataName(), len(block), blockBytes)
		}
	} else {
		block = make([]uint8, blockBytes)
	}

	// Compute index into the block byte buffer, blockI
	blockNumX := blockSize[0] * bytesPerVoxel
	blockNumXY := blockSize[1] * blockNumX

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	beg := begVolCoord.Sub(voxels.Origin())
	end := endVolCoord.Sub(voxels.Origin())

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.
	data := voxels.data

	// fmt.Printf("Data %s -> %s, Orig %s -> %s\n", beg, end, begVolCoord, endVolCoord)
	//fmt.Printf("Block start: %s\n", blockBeg)
	//fmt.Printf("Block buffer size: %d bytes\n", len(block))
	//fmt.Printf("Data buffer size: %d bytes\n", len(data))

	switch voxels.DataShape() {
	case XY:
		//fmt.Printf("XY Block: %s->%s, blockXY %d, blockX %d, blockBeg %s\n",
		//	begVolCoord, endVolCoord, blockNumXY, blockNumX, blockBeg)
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[1]*voxels.stride + beg[0]*bytesPerVoxel
		for y := beg[1]; y <= end[1]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch operation.op {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * bytesPerVoxel
			dataI += voxels.stride
		}
		//dvid.PrintNonZero("After copy", data)
	case XZ:
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[2]*voxels.stride + beg[0]*bytesPerVoxel
		for y := beg[2]; y <= end[2]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch operation.op {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * blockSize[1] * bytesPerVoxel
			dataI += voxels.stride
		}
	case YZ:
		bx, bz := blockBeg[0], blockBeg[2]
		for y := beg[2]; y <= end[2]; y++ {
			dataI := y*voxels.stride + beg[1]*bytesPerVoxel
			blockI := bz*blockNumXY + blockBeg[1]*blockNumX + bx*bytesPerVoxel
			for x := beg[1]; x <= end[1]; x++ {
				switch operation.op {
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
		dataNumX := voxels.Width() * bytesPerVoxel
		dataNumXY := voxels.Height() * dataNumX
		blockZ := blockBeg[2]
		for dataZ := beg[2]; dataZ <= end[2]; dataZ++ {
			blockY := blockBeg[1]
			for dataY := beg[1]; dataY <= end[1]; dataY++ {
				blockI := blockZ*blockNumXY + blockY*blockNumX + blockBeg[0]*bytesPerVoxel
				dataI := dataZ*dataNumXY + dataY*dataNumX + beg[0]*bytesPerVoxel
				run := end[0] - beg[0] + 1
				bytes := run * bytesPerVoxel
				switch operation.op {
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

	// If this is a PUT, place the modified block data into the database.
	if operation.op == PutOp {
		db := server.KeyValueDB()
		serialization, err := dvid.SerializeData([]byte(block), dvid.Snappy, dvid.CRC32)
		if err != nil {
			fmt.Printf("Unable to serialize block: %s\n", err.Error())
		}
		db.Put(chunk.GetKey(), serialization)
	}

	// Notify the requestor that this block is done.
	if operation.wg != nil {
		operation.wg.Done()
	}
}
