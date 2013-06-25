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
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/goprotobuf/proto"
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

// DefaultNumChunkHandlers specifies the number of chunk handlers for each dataset
// of this data type.
const DefaultNumChunkHandlers = 8

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

func init() {
	gob.Register(&Datatype{})
	gob.Register(&Dataset{})
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
		Iterator:      true,
		JSONDatastore: false,
		BulkIniter:    false,
		BulkLoader:    false,
		Batcher:       false,
	}
	return
}

// --- TypeService interface ---

// NewDataset returns a pointer to a new voxels Dataset with default values.
func (dtype *Datatype) NewDataset(id datastore.DatasetID, config dvid.Config) datastore.DatasetService {
	dataset := &Dataset{
		Dataset: datastore.NewDataset(id, dtype),
	}
	dataset.BlockSize = DefaultBlockMax
	if obj, found := config["BlockSize"]; found {
		if blockSize, ok := obj.(dvid.Point3d); ok {
			dataset.BlockSize = blockSize
		}
	}
	dataset.VoxelRes = dvid.VoxelResolution{1.0, 1.0, 1.0}
	if obj, found := config["VoxelRes"]; found {
		if voxelRes, ok := obj.(dvid.VoxelResolution); ok {
			dataset.VoxelRes = voxelRes
		}
	}
	dataset.VoxelResUnits = "nanometers"
	if obj, found := config["VoxelResUnits"]; found {
		if res, ok := obj.(dvid.VoxelResolutionUnits); ok {
			dataset.VoxelResUnits = res
		}
	}
	datastore.StartDatasetChunkHandlers(dataset)
	return dataset
}

func (dtype *Datatype) Help() string {
	return HelpMessage
}

// Dataset embeds the datastore's Dataset and extends it with voxel-specific properties.
type Dataset struct {
	*datastore.Dataset

	// Block size for this dataset
	BlockSize dvid.Point3d

	// Relative resolution of voxels in volume
	VoxelRes dvid.VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.VoxelResolutionUnits
}

// Returns total number of bytes across all channels for a voxel.
func (dset *Dataset) BytesPerVoxel() int32 {
	// Make sure the dataset here has a pointer to a Datatype of this package.
	dtype, ok := dset.Datatype.(*Datatype)
	if !ok {
		log.Fatalf("Illegal datatype %s for dataset %s",
			dset.Datatype, dset.DatasetName())
	}
	return int32(dtype.BytesPerVoxel * dtype.NumChannels)
}

func (dset *Dataset) TypeService() datastore.TypeService {
	return dset.Dataset.Datatype
}

// --- DatasetService interface ---

func (dset *Dataset) NumChunkHandlers() int {
	return DefaultNumChunkHandlers
}

// Do acts as a switchboard for RPC commands.
func (dset *Dataset) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "server-add":
		return dset.ServerAdd(request, reply)
	//	case "get":
	//		return dtype.Get(request, reply)
	case "help":
		reply.Text = HelpMessage
	default:
		return dset.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this dataset.
func (dset *Dataset) DoHTTP(w http.ResponseWriter, r *http.Request) error {
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
	shapeStr := dvid.DataShapeString(parts[2])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape {
	case dvid.XY, dvid.XZ, dvid.YZ:
		slice, err := dvid.NewSliceFromStrings(parts[2], parts[3], parts[4])
		if err != nil {
			return err
		}
		if op == PutOp {
			// TODO -- Put in format checks for POSTed image.
			postedImg, _, err := dvid.ImageFromPost(r, "image")
			if err != nil {
				return err
			}
			err = dset.PutImage(versionID, postedImg, slice)
			if err != nil {
				return err
			}
		} else {
			img, err := dset.GetImage(versionID, slice)
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
	case dvid.Vol:
		subvol, err := dvid.NewSubvolumeFromStrings(parts[3], parts[4])
		if err != nil {
			return err
		}
		if op == GetOp {
			if data, err := dset.GetVolume(versionID, subvol); err != nil {
				return err
			} else {
				w.Header().Set("Content-type", "application/x-protobuf")
				_, err = w.Write(data)
				if err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("DVID does not yet support POST of subvolume data.")
		}
	case dvid.Arb:
		return fmt.Errorf("DVID does not yet support arbitrary planes.")
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s", r.Method, dataShape)
	return nil
}

// NewIndexIterator returns an IndexIterator closure.  The closure for this voxels data
// type uses VoxelCoord, BlockCoord, and returns an IndexZYX.
func (dset *Dataset) NewIndexIterator(extents interface{}) datastore.IndexIterator {
	data, ok := extents.(dvid.Geometry)
	if !ok {
		log.Fatalf("Received bad extents (%s) for NewIndexIterator for %s", extents, *dset)
	}
	blockSize := dset.BlockSize

	// Setup traversal
	startVoxel := data.Origin()
	endVoxel := data.EndVoxel()

	// Returns a closure that iterates in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	z := startBlockCoord[2]
	y := startBlockCoord[1]
	x := startBlockCoord[0]
	return func() datastore.Index {
		if z > endBlockCoord[2] {
			return nil
		}
		// Convert an n-D block coordinate into a 1-D index
		index, err := datastore.BlockZYX{dvid.BlockCoord{x, y, z}}.MakeIndex()
		if err != nil {
			log.Fatalf("Bad BlockZYX.MakeIndex() for block coord (%d,%d,%d)", x, y, z)
		}
		x++
		if x > endBlockCoord[0] {
			x = startBlockCoord[0]
			y++
		}
		if y > endBlockCoord[1] {
			y = startBlockCoord[1]
			z++
		}
		return index
	}
}

// Voxels represents subvolumes or slices.
type Voxels struct {
	dvid.Geometry

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

// SliceImage returns an image.Image for the z-th slice of the volume.
func (dset *Dataset) SliceImage(v *Voxels, z int) (img image.Image, err error) {
	dtype, ok := dset.Datatype.(*Datatype)
	if !ok {
		err = fmt.Errorf("Dataset %s does not have type of voxels.Datatype!", dset.DatasetName())
	}
	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images with %d bytes/voxel and %d channels",
			dtype.BytesPerVoxel, dtype.NumChannels)
	}
	sliceBytes := int(v.Width() * v.Height() * int32(dset.BytesPerVoxel()))
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

// --- datastore.Operation interface ----

// Operation holds type-specific data for operations and can be used to
// map blocks to handlers.
type Operation struct {
	*Dataset
	data      *Voxels
	versionID dvid.LocalID
	op        OpType
	wg        *sync.WaitGroup
}

type OpType uint8

const (
	GetOp OpType = iota
	PutOp
)

func (op OpType) String() string {
	switch op {
	case GetOp:
		return "GET"
	case PutOp:
		return "PUT"
	}
	return fmt.Sprintf("Illegal Op (%d)", op)
}

func (dset *Dataset) makeOp(data *Voxels, versionID dvid.LocalID, op OpType) *Operation {
	return &Operation{dset, data, versionID, op, new(sync.WaitGroup)}
}

func (pOp *Operation) IndexIterator() datastore.IndexIterator {
	return pOp.NewIndexIterator(pOp.data)
}

func (pOp *Operation) IsReadOnly() bool {
	if pOp.op == GetOp {
		return true
	}
	return false
}

func (pOp *Operation) DatastoreService() *datastore.Service {
	s := server.DataService()
	return s.Service
}

func (pOp *Operation) DatasetService() datastore.DatasetService {
	return pOp.Dataset
}

func (pOp *Operation) VersionLocalID() dvid.LocalID {
	return pOp.versionID
}

func (pOp *Operation) Map() error {
	return datastore.Map(pOp)
}

func (pOp *Operation) WaitAdd() {
	if pOp.wg != nil {
		pOp.wg.Add(1)
	}
}

func (pOp *Operation) Wait() {
	pOp.wg.Wait()
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
func (dset *Dataset) ServerAdd(request datastore.Request, reply *datastore.Response) error {

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
	offset, err := dvid.PointStr(offsetStr).VoxelCoord()
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
	dataShape, err := dvid.DataShapeString(shapeStr).DataShape()
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
			size := dvid.SizeFromRect(img.Bounds())
			slice, err := dvid.NewSlice(dataShape, offset, size)

			err = dset.PutImage(versionID, img, slice)

			dvid.ElapsedTime(dvid.Debug, sliceTime, "%s server-add %s", dset.DatasetName(), slice)
			if err == nil {
				numSuccessful++
			} else {
				lastErr = err
			}
		}
		offset = offset.Add(dvid.VoxelCoord{0, 0, 1})
	}
	if lastErr != nil {
		return fmt.Errorf("Error: %d of %d images successfully added [%s]\n",
			numSuccessful, len(filenames), lastErr.Error())
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (dset *Dataset) GetImage(versionID dvid.LocalID, slice dvid.Geometry) (img image.Image, err error) {
	bytesPerVoxel := dset.BytesPerVoxel()
	stride := slice.Width() * bytesPerVoxel

	numBytes := int64(bytesPerVoxel) * slice.NumVoxels()
	data := make([]uint8, numBytes, numBytes)
	operation := dset.makeOp(&Voxels{slice, data, stride}, versionID, GetOp)

	// Perform operation using mapping
	err = operation.Map()
	if err != nil {
		return
	}
	operation.Wait()

	img, err = dset.SliceImage(operation.data, 0)
	return
}

// PutImage adds a 2d image within given geometry to a version node.
func (dset *Dataset) PutImage(versionID dvid.LocalID, img image.Image, slice dvid.Geometry) error {
	bytesPerVoxel := dset.BytesPerVoxel()
	stride := slice.Width() * bytesPerVoxel

	var operation *Operation
	if data, actualStride, err := dvid.ImageData(img); err != nil {
		return err
	} else {
		if actualStride < stride {
			return fmt.Errorf("Too little data in input image (%d stride bytes)", stride)
		}
		operation = dset.makeOp(&Voxels{slice, data, stride}, versionID, PutOp)
	}

	if err := operation.Map(); err != nil {
		return err
	}
	operation.Wait() // TODO -- Move this out of loop so we don't wait at each image load

	return nil
}

func (dset *Dataset) GetVolume(versionID dvid.LocalID, vol dvid.Geometry) (data []byte, err error) {
	startTime := time.Now()

	bytesPerVoxel := dset.BytesPerVoxel()
	numBytes := int64(bytesPerVoxel) * vol.NumVoxels()
	voldata := make([]uint8, numBytes, numBytes)
	operation := dset.makeOp(&Voxels{vol, voldata, 0}, versionID, GetOp)

	// Perform operation using mapping
	err = operation.Map()
	if err != nil {
		return
	}
	operation.Wait()

	// server.Subvolume is a thrift-defined data structure
	encodedVol := &server.Subvolume{
		Dataset: proto.String(string(dset.DatasetName())),
		OffsetX: proto.Int32(operation.data.Geometry.Origin()[0]),
		OffsetY: proto.Int32(operation.data.Geometry.Origin()[1]),
		OffsetZ: proto.Int32(operation.data.Geometry.Origin()[2]),
		SizeX:   proto.Uint32(uint32(operation.data.Geometry.Size()[0])),
		SizeY:   proto.Uint32(uint32(operation.data.Geometry.Size()[1])),
		SizeZ:   proto.Uint32(uint32(operation.data.Geometry.Size()[2])),
		Data:    []byte(operation.data.data),
	}
	data, err = proto.Marshal(encodedVol)

	dvid.ElapsedTime(dvid.Normal, startTime, "%s %s (%s) %s", GetOp, operation.DatasetName(),
		operation.DatatypeName(), operation.data.Geometry)

	return
}

// ChunkHandler processes a chunk of data as part of a mapped operation.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc)
func (dset *Dataset) ChunkHandler(chunkOp *datastore.ChunkOp) {

	operation, ok := chunkOp.Operation.(*Operation)
	if !ok {
		log.Fatalf("Illegal operation passed to ChunkHandler() for dataset %s", operation.DatasetName())
	}
	voxels := operation.data
	index := datastore.IndexZYX(chunkOp.Key.Index)

	// Compute the bounding voxel coordinates for this block.
	blockSize := dset.BlockSize
	minBlockVoxel := index.OffsetToBlock(blockSize)
	maxBlockVoxel := minBlockVoxel.AddSize(blockSize)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := voxels.Origin()
	maxDataVoxel := voxels.EndVoxel()
	begVolCoord := minDataVoxel.Max(minBlockVoxel)
	endVolCoord := maxDataVoxel.Min(maxBlockVoxel)

	// Calculate the strides
	dtype := dset.TypeService().(*Datatype)
	bytesPerVoxel := int32(dtype.NumChannels * dtype.BytesPerVoxel)

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel)

	// Allocate the block buffer if PUT or use passed-in chunk if GET
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * bytesPerVoxel)
	var block []uint8
	if chunkOp.Chunk != nil {
		block = []uint8(chunkOp.Chunk)
		if len(block) != blockBytes {
			log.Fatalf("Retrieved block for dataset '%s' is %d bytes, not %d block size!\n",
				dset.DatasetName(), len(block), blockBytes)
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
	case dvid.XY:
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
	case dvid.XZ:
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
	case dvid.YZ:
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
	case dvid.Vol:
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
		db := operation.DatastoreService().KeyValueDB()
		serialization, err := dvid.SerializeData([]byte(block), dvid.Snappy, dvid.CRC32)
		if err != nil {
			fmt.Printf("Unable to serialize block: %s\n", err.Error())
		}
		db.Put(chunkOp.Key, serialization)
	}

	// Notify the requestor that this block is done.
	if operation.wg != nil {
		operation.wg.Done()
	}
}
