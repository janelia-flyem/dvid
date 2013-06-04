/*
	Package voxels implements DVID support for data using voxels as elements.
	A number of data types will embed this package and customize it using the
	"NumChannels" and "BytesPerVoxel" fields.
*/
package voxels

import (
	"fmt"
	"image"
	_ "log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"code.google.com/p/goprotobuf/proto"
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

// DefaultNumBlockHandlers specifies the number of block handlers for this data type
// for each image version. 
const DefaultNumBlockHandlers = 8

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

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
}

// Dataset embeds the datastore's Dataset so we can set voxel-specific properties.
type Dataset datastore.Dataset

// DatasetProps encapsulates what we need to clarify per voxel dataset.
type DatasetProps struct {
	// Block size for this dataset
	BlockSize dvid.BlockCoord

	// Relative resolution of voxels in volume
	VoxelRes dvid.VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.VoxelResolutionUnits
}

// --- TypeService interface ---

// NewDataset returns a pointer to a new voxels Dataset with default values.
// TODO -- Allow dataset-specific configuration
func (dtype *Datatype) NewDataset(name DatasetString, s *datastore.Service, config interface{},
	id []byte) *Dataset {

	return &Dataset{
		Datatype: dtype,
		name: name,
		props: &DatasetProps{
			BlockSize:     DefaultBlockMax,
			VoxelRes:      dvid.VoxelResolution{1.0, 1.0, 1.0},
			VoxelResUnits: "nanometers",
		},
		datasetKey: id,
		store: s,
	}
}

// BlockBytes returns the number of bytes within this data type's block data.
func (dtype *Datatype) BlockBytes() int {
	numVoxels := int(dtype.BlockMax[0] * dtype.BlockMax[1] * dtype.BlockMax[2])
	return numVoxels * dtype.NumChannels * dtype.BytesPerVoxel
}

// --- DatasetService interface ---

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
		return dtype.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this dataset. 
func (dset *Dataset) DoHTTP(w http.ResponseWriter, r *http.Request, apiURL string) error {
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
	url := r.URL.Path[len(apiURL):]
	parts := strings.Split(url, "/")

	// Setup the Operation using the correct version
	versionStr := parts[1]
	versionBytes, err := dset.store.VersionBytes(versionStr)
	if err != nil {
		return err
	}
	operation = dset.makeOperation(versionBytes, op)

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
			_, err = dtype.ProcessSlice(operation, slice, postedImg)
			if err != nil {
				return err
			}
		} else {
			img, err := dtype.ProcessSlice(operation, slice, nil)
			if err != nil {
				return err
			}
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
		dvid.ElapsedTime(dvid.Normal, startTime, "%s %s (%s) %s", op, dataSetName,
			dtype.DatatypeName(), slice)
	case dvid.Vol:
		subvol, err := dvid.NewSubvolumeFromStrings(parts[3], parts[4])
		if err != nil {
			return err
		}
		if op == GetOp {
			pb, err := dtype.ProcessSubvolume(operation, subvol, nil)
			if err != nil {
				return err
			}
			data, err := proto.Marshal(pb)
			if err != nil {
				return err
			}
			w.Header().Set("Content-type", "application/x-protobuf")
			_, err = w.Write(data)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("DVID does not yet support POST of subvolume data.")
		}
		dvid.ElapsedTime(dvid.Normal, startTime, "%s %s (%s) %s", op, dataSetName,
			dtype.DatatypeName(), subvol)
	case dvid.Arb:
		return fmt.Errorf("DVID does not yet support arbitrary planes.")
	}
	return nil
}

// NewIndexIterator returns an IndexIterator closure.  The closure for this voxels data
// type uses VoxelCoord, BlockCoord, and returns an IndexZYX.
func (dset *Dataset) NewIndexIterator(extents interface{}) IndexIterator {
	data, ok := extents.(dvid.Geometry)
	if !ok {
		log.Fatalf("Received bad data (%s) for NewIndexIterator for %s", abstractData, *dset)
	}
	blockSize := dset.Props.(*DatasetProps).BlockSize

	// Setup traversal
	startVoxel := data.Origin()
	endVoxel := data.EndVoxel()

	// Returns a closure that iterates in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	z := startBlockCoord[2]
	y := startBlockCoord[1]
	x := startBlockCoord[0]
	return func() Index {
		//dvid.Fmt(dvid.Debug, "IndexIterator: start at (%d,%d,%d)\n", x, y, z)
		if z > endBlockCoord[2] {
			return nil
		}
		// Convert an n-D block coordinate into a 1-D index
		index := datastore.BlockZYX{x, y, z}.MakeIndex()
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

// --- datastore.Operation interface ----

// Operation holds type-specific data for operations and can be used to 
// map blocks to handlers.
type Operation struct {
	*Dataset
	version []byte
	op      OpType
	wg      *sync.WaitGroup
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

func (dset *Dataset) makeOperation(version []byte, op OpType) *Operation {
	return &Operation{dset, version, op, new(sync.WaitGroup)}
}

func (pOp *Operation) VersionBytes() []byte {
	return pOp.version
}

func (pOp *Operation) Data() interface{} {
	return pOp.data
}

func (pOp *Operation) DatasetName() datastore.DatasetString {
	return pOp.dataset
}

func (pOp *Operation) WaitGroup() *sync.WaitGroup {
	return pOp.wg
}

// Process subvolume and slice function
//
// TODO -- Writing all blocks for a subvolume/slice is likely wasteful although it does
// provide assurance that modified blocks are written to disk.   Adjacent slices
// will usually intersect the same block so its more efficient to only write blocks
// that haven't been touched for some small amount of time.  On the other hand, block
// sizes might make seek times less important and leveldb also has a configurable
// write cache.

// ProcessSubvolume breaks a subvolume into constituent blocks and processes the blocks
// concurrently via MapBlocks(), then waiting until all blocks have
// completed before returning.  An subvolume is returned if the op is a GetOp.
// If the op is a PutOp, we write sequentially all modified blocks.
func (dset *Dataset) ProcessSubvolume(op *Operation, subvol *dvid.Subvolume) (
	encodedVol *server.Subvolume, err error) {

	startTime := time.Now()

	// Setup the data buffer
	bytesPerVoxel := dtype.BytesPerVoxel * dtype.NumChannels
	numBytes := bytesPerVoxel * subvol.NumVoxels()
	var data []uint8
	//var batch keyvalue.WriteBatch

	switch op.op {
	case PutOp:
		// Setup write batch to maximize sequential writes and exploit
		// leveldb write buffer.
		//batch = keyvalue.NewWriteBatch()
		//defer batch.Close()
	case GetOp:
		// supported op
	default:
		err = fmt.Errorf("Illegal operation (%d) in ProcessSubvolume()", op)
		return
	}

	// Do the mapping from subvol to blocks
	voxels := &Voxels{
		Geometry: subvol,
		data:     data,
	}
	var wg sync.WaitGroup
	err = vs.MapBlocks(op, voxels, &wg)
	if err != nil {
		return
	}
	// Wait for all map block processing.
	wg.Wait()
	if op == GetOp {
		dvid.PrintNonZero("ProcessSubvolume()", []byte(data))
		encodedVol = &server.Subvolume{
			Dataset: proto.String(string(dataSetName)),
			OffsetX: proto.Int32(subvol.Origin()[0]),
			OffsetY: proto.Int32(subvol.Origin()[1]),
			OffsetZ: proto.Int32(subvol.Origin()[2]),
			SizeX:   proto.Uint32(uint32(subvol.Size()[0])),
			SizeY:   proto.Uint32(uint32(subvol.Size()[1])),
			SizeZ:   proto.Uint32(uint32(subvol.Size()[2])),
			Data:    []byte(data),
		}
		dvid.ElapsedTime(dvid.Normal, startTime, "Retrieved %s.  Data has %d bytes",
			subvol, len(data))
	}
	return
}

// ProcessSlice breaks a slice into constituent blocks and processes the blocks
// concurrently via MapBlocks(), then waiting until all blocks have
// completed before returning.  An image is returned if the op is a GetOp.
// If the op is a PutOp, we write sequentially all modified blocks.
func (dset *Dataset) ProcessSlice(op *Operation, slice dvid.Geometry,
	inputImg image.Image) (outputImg image.Image, err error) {

	// Setup the data buffer
	bytesPerVoxel := dtype.BytesPerVoxel * dtype.NumChannels
	numBytes := bytesPerVoxel * slice.NumVoxels()
	var data []uint8
	var stride int32
	//var batch keyvalue.WriteBatch

	switch op {
	case PutOp:
		// Setup write batch to maximize sequential writes and exploit
		// leveldb write buffer.
		//batch = keyvalue.NewWriteBatch()
		//defer batch.Close()

		// Use input image bytes as data buffer.
		data, stride, err = dvid.ImageData(inputImg)
		if err != nil {
			return
		}
		expectedStride := int32(dtype.NumChannels*dtype.BytesPerVoxel) * slice.Width()
		if stride < expectedStride {
			typeInfo := fmt.Sprintf("(%s: %d channels, %d bytes/voxel, %s pixels)",
				dtype.DatatypeName(), dtype.NumChannels, dtype.BytesPerVoxel, slice.Size())
			err = fmt.Errorf("Input image has too little data (stride bytes = %d) for type %s",
				stride, typeInfo)
			return
		}
	case GetOp:
		data = make([]uint8, numBytes, numBytes)
		stride = slice.Width() * int32(bytesPerVoxel)
	default:
		err = fmt.Errorf("Illegal operation (%d) in ProcessSlice()", op)
		return
	}

	// Do the mapping from slice to blocks
	voxels := &Voxels{
		dataSetName: dataSetName,
		Geometry:    slice,
		TypeService: dtype,
		data:        data,
		stride:      stride,
	}
	var wg sync.WaitGroup
	err = vs.MapBlocks(op, voxels, &wg)
	if err != nil {
		return
	}
	// Wait for all map block processing.
	wg.Wait()
	if op == GetOp {
		// Write the image to requestor
		outputImg, err = voxels.SliceImage(0)
		if err != nil {
			return
		}
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
func (dset *Dataset) ServerAdd(request datastore.Request, reply *datastore.Response,
	service *datastore.Service) error {

	startTime := time.Now()

	// Parse the request
	var setName, cmdStr, uuidStr, offsetStr string
	filenames := request.CommandArgs(0, &setName, &cmdStr, &uuidStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
	}
	dataSetName := datastore.DatasetString(setName)

	vs, err := datastore.NewVersionService(service, uuidStr)
	if err != nil {
		return err
	}

	var addedFiles string
	if len(filenames) == 1 {
		addedFiles = filenames[0]
	} else {
		addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}
	dvid.Log(dvid.Debug, addedFiles+"\n")

	shapeStr, found := request.Parameter(dvid.KeyPlane)
	if !found {
		shapeStr = "xy"
	}
	dataShape, err := dvid.DataShapeString(shapeStr).DataShape()
	if err != nil {
		return err
	}

	offset, err := dvid.PointStr(offsetStr).VoxelCoord()
	if err != nil {
		return err
	}

	// Load each image and do map/reduce computation on blocks of the image.
	// We prefer to load the images sequentially instead of in parallel to
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
			_, err = dtype.ProcessSlice(dataSetName, vs, datastore.PutOp, slice, img)
			dvid.ElapsedTime(dvid.Debug, sliceTime, "%s %s %s",
				datastore.PutOp, dtype.DatatypeName(), slice)
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
	dvid.ElapsedTime(dvid.Normal, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// Voxels implements the DataStruct interface for a rectangular blocks of voxels.
// The Voxels data is eventually broken down into blocks for processing, and each
// block is handled by the BlockHandler() defined in this package.
type Voxels struct {
	// Which data set does this belong to?
	dataSetName datastore.DatasetString

	dvid.Geometry
	datastore.TypeService

	// The data itself
	data []uint8

	// The stride for 2d iteration.  For 3d subvolumes, we don't reuse standard Go
	// images but maintain fully packed data slices, so stride isn't necessary.
	stride int32
}

func (v *Voxels) Data() []uint8 {
	return v.data
}

func (v *Voxels) Stride() int32 {
	return v.stride
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s %s of size %d x %d x %d @ %s",
		v.DatatypeName(), v.DataShape(), size[0], size[1], size[2], v.Origin())
}

// SliceImage returns an image.Image for the z-th slice of the volume.
func (v *Voxels) SliceImage(z int) (img image.Image, err error) {
	dtype := v.TypeService.(*Datatype)
	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images with %d bytes/voxel and %d channels",
			dtype.BytesPerVoxel, dtype.NumChannels)
	}
	sliceBytes := int(v.Width()*v.Height()) * dtype.BytesPerVoxel * dtype.NumChannels
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

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the index.
func (i IndexZYX) OffsetToBlock() (coord dvid.VoxelCoord) {
	blockCoord := i.InvertIndex()
	blockSize := t.BlockSize()
	coord[0] = blockCoord[0] * blockSize[0]
	coord[1] = blockCoord[1] * blockSize[1]
	coord[2] = blockCoord[2] * blockSize[2]
	return
}

// BlockHandler processes a block of data as part of a request.  The BlockRequest
// holds data (the larger structure, e.g., plane or subvolume) that cuts across
// this particular block (a small rectangular volume of voxels).  The data may be
// thinner, wider, and longer than the block, depending on the data shape (XY, XZ, etc)
func (v *Voxels) BlockHandler(req *datastore.BlockRequest) {

	// Compute the bounding voxel coordinates for this block.
	blockSize := v.BlockSize()
	minBlockVoxel := req.IndexKey.OffsetToBlock(v)
	maxBlockVoxel := minBlockVoxel.AddSize(blockSize)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := v.Origin()
	maxDataVoxel := v.EndVoxel()
	begVolCoord := minDataVoxel.Max(minBlockVoxel)
	endVolCoord := maxDataVoxel.Min(maxBlockVoxel)

	// Calculate the strides
	dtype := v.TypeService.(*Datatype)
	bytesPerVoxel := int32(dtype.NumChannels * dtype.BytesPerVoxel)

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel)

	// Compute index into the block byte buffer, blockI
	var block []uint8
	if req.Block == nil {
		// Create a zero value block
		blockBytes := req.DataStruct.BlockBytes()
		block = make([]uint8, blockBytes, blockBytes)
	} else {
		block = []uint8(req.Block)
	}
	blockNumX := blockSize[0] * bytesPerVoxel
	blockNumXY := blockSize[1] * blockNumX

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	beg := begVolCoord.Sub(v.Origin())
	end := endVolCoord.Sub(v.Origin())

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.

	data := req.DataStruct.Data()

	//fmt.Printf("Data %s -> %s, Orig %s -> %s\n", beg, end, begVolCoord, endVolCoord)
	//dvid.Fmt(dvid.Debug, "Block start: %s\n", blockBeg)
	//dvid.Fmt(dvid.Debug, "Block buffer size: %d bytes\n", len(block))
	//dvid.Fmt(dvid.Debug, "Data buffer size: %d bytes\n", len(data))

	switch req.DataShape() {
	case dvid.XY:
		//fmt.Printf("XY Block: %s->%s, blockXY %d, blockX %d, blockBeg %s\n",
		//	begVolCoord, endVolCoord, blockXY, blockX, blockBeg)
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[1]*v.Stride() + beg[0]*bytesPerVoxel
		for y := beg[1]; y <= end[1]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch req.Op {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * bytesPerVoxel
			dataI += v.Stride()
		}
	case dvid.XZ:
		blockI := blockBeg[2]*blockNumXY + blockBeg[1]*blockNumX + blockBeg[0]*bytesPerVoxel
		dataI := beg[2]*v.Stride() + beg[0]*bytesPerVoxel
		for y := beg[2]; y <= end[2]; y++ {
			//fmt.Printf("XZ> y(%d) -> BlockI: %d, dataI: %d\n", y, blockI, dataI)
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch req.Op {
			case GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * blockSize[1] * bytesPerVoxel
			dataI += v.Stride()
		}
	case dvid.YZ:
		bx, bz := blockBeg[0], blockBeg[2]
		for y := beg[2]; y <= end[2]; y++ {
			dataI := y*v.Stride() + beg[1]*bytesPerVoxel
			blockI := bz*blockNumXY + blockBeg[1]*blockNumX + bx*bytesPerVoxel
			for x := beg[1]; x <= end[1]; x++ {
				switch req.Op {
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
		dataNumX := req.DataStruct.Width() * bytesPerVoxel
		dataNumXY := req.DataStruct.Height() * dataNumX
		blockZ := blockBeg[2]
		for dataZ := beg[2]; dataZ <= end[2]; dataZ++ {
			blockY := blockBeg[1]
			for dataY := beg[1]; dataY <= end[1]; dataY++ {
				blockI := blockZ*blockNumXY + blockY*blockNumX + blockBeg[0]*bytesPerVoxel
				dataI := dataZ*dataNumXY + dataY*dataNumX + beg[0]*bytesPerVoxel
				run := end[0] - beg[0] + 1
				bytes := run * bytesPerVoxel
				switch req.Op {
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
	if req.Op == PutOp {
		req.DB.Put(req.BlockKey, req.Block)
	}

	// Notify the requestor that this block is done.
	if req.Wait != nil {
		req.Wait.Done()
	}
}
