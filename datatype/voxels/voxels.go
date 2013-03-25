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
	"github.com/janelia-flyem/dvid/keyvalue"
)

const Version = "0.6"

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
const DefaultNumBlockHandlers = 1

// DefaultBlockMax specifies the default size for each block of this data type.
var DefaultBlockMax dvid.Point3d = dvid.Point3d{16, 16, 16}

// We don't init() this data type because it's like an abstract base class.
/*
func init() {
	datastore.RegisterDatatype(&Datatype{
		Datatype: datastore.Datatype{
			DatatypeID:  datastore.MakeDatatypeID("voxels", RepoUrl, Version),
			BlockMax:    DefaultBlockMax,
			Indexing:    datastore.SIndexZYX,
			IsolateData: true,
		},
		NumChannels:   1,
		BytesPerVoxel: 1,
	})
}
*/

// Datatype embeds the datastore's Datatype to create a unique type
// with voxel functions.  Refinements of general voxel types can be implemented 
// by embedding this type, choosing appropriate # of channels and bytes/voxel,
// overriding functions as needed, and calling datastore.RegisterDatatype().
type Datatype struct {
	datastore.Datatype
	NumChannels   int
	BytesPerVoxel int
}

// The following two functions must be defined to fulfill the TypeService interface.

// NumBlockHandlers returns the number of block handler goroutines that are launched
// for this data type per image version.
func (dtype *Datatype) NumBlockHandlers() int {
	return DefaultNumBlockHandlers
}

// BlockBytes returns the number of bytes within this data type's block data.
func (dtype *Datatype) BlockBytes() int {
	numVoxels := int(dtype.BlockMax[0] * dtype.BlockMax[1] * dtype.BlockMax[2])
	return numVoxels * dtype.NumChannels * dtype.BytesPerVoxel
}

// ProcessSlice breaks a slice into constituent blocks and processes the blocks
// concurrently via MapBlocks(), then waiting until all blocks have
// completed before returning.  An image is returned if the op is a GetOp.
// If the op is a PutOp, we write sequentially all modified blocks.
//
// TODO -- Writing all blocks for a slice is likely wasteful although it does provide
// assurance that modified blocks for a slice are written to disk.   Adjacent slices
// will usually intersect the same block so its more efficient to only write blocks
// that haven't been touched for some small amount of time.
func (dtype *Datatype) ProcessSlice(vs *datastore.VersionService, op datastore.OpType,
	slice dvid.Geometry, inputImg image.Image) (outputImg image.Image, err error) {

	// Setup the data buffer
	numBytes := dtype.BytesPerVoxel * dtype.NumChannels * slice.NumVoxels()
	var data []uint8
	//var batch keyvalue.WriteBatch

	switch op {
	case datastore.PutOp:
		// Setup write batch to maximize sequential writes and exploit
		// leveldb write buffer.
		//batch = keyvalue.NewWriteBatch()
		//defer batch.Close()

		fmt.Printf("Input image: %d x %d\n", inputImg.Bounds().Dx(), inputImg.Bounds().Dy())

		// Use input image bytes as data buffer.
		var stride int
		data, stride, err = dvid.ImageData(inputImg)
		if err != nil {
			return
		}
		expectedStride := dtype.NumChannels * dtype.BytesPerVoxel * int(slice.Width())
		if stride != expectedStride {
			typeInfo := fmt.Sprintf("(%s: %d channels, %d bytes/voxel, %s pixels)",
				dtype.TypeName(), dtype.NumChannels, dtype.BytesPerVoxel, slice.Size())
			err = fmt.Errorf("Input image does not match data type: stride bytes = %d for type %s",
				stride, typeInfo)
			return
		}
	case datastore.GetOp:
		data = make([]uint8, numBytes, numBytes)
	default:
		err = fmt.Errorf("Illegal operation (%d) in ProcessSlice()", op)
		return
	}

	// Do the mapping from slice to blocks
	voxels := &Voxels{
		Geometry:    slice,
		TypeService: dtype,
		data:        data,
	}
	var wg sync.WaitGroup
	err = vs.MapBlocks(op, voxels, &wg)
	if err != nil {
		return
	}

	// Wait for all block handling to finish, then handle result. 
	wg.Wait()
	switch op {
	case datastore.PutOp:
		/*
			// Store image using batch write.
			wo := keyvalue.NewWriteOptions()
			datastore.DiskAccess.Lock()
			err = vs.KeyValueDB().Write(batch, wo)
			datastore.DiskAccess.Unlock()
			if err != nil {
				return
			}
		*/
	case datastore.GetOp:
		// Write the image to requestor
		outputImg, err = voxels.SliceImage(0)
		if err != nil {
			return
		}
	}
	return
}

// Do acts as a switchboard for RPC commands. 
func (dtype *Datatype) DoRPC(request datastore.Request, reply *datastore.Response,
	service *datastore.Service) error {

	switch request.TypeCommand() {
	case "server-add":
		return dtype.ServerAdd(request, reply, service)
	//	case "get":
	//		return dtype.Get(request, reply)
	case "help":
		reply.Text = dtype.Help(HelpMessage)
	default:
		return dtype.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handls all incoming HTTP requests for this data type. 
func (dtype *Datatype) DoHTTP(w http.ResponseWriter, r *http.Request,
	service *datastore.Service, apiPrefixURL string) {

	startTime := time.Now()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	var op datastore.OpType
	switch action {
	case "get":
		op = datastore.GetOp
	case "post":
		op = datastore.PutOp
	default:
		badRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(apiPrefixURL):]
	parts := strings.Split(url, "/")

	// Get the datastore service corresponding to given version
	vs, err := datastore.NewVersionService(service, parts[1])
	if err != nil {
		badRequest(w, r, err.Error())
		return
	}

	// Get the data shape. 
	shapeStr := dvid.DataShapeString(parts[2])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		badRequest(w, r, fmt.Sprintf("Bad data shape given '%s'", shapeStr))
		return
	}

	switch dataShape {
	case dvid.XY, dvid.XZ, dvid.YZ:
		slice, err := dvid.NewSliceFromStrings(parts[2], parts[3], parts[4])
		if err != nil {
			badRequest(w, r, err.Error())
			return
		}
		if op == datastore.PutOp {
			// TODO -- Put in format checks for POSTed image.
			postedImg, _, err := dvid.ImageFromPost(r, "image")
			if err != nil {
				badRequest(w, r, err.Error())
				return
			}
			_, err = dtype.ProcessSlice(vs, datastore.PutOp, slice, postedImg)
		} else {
			img, err := dtype.ProcessSlice(vs, op, slice, nil)
			if err != nil {
				badRequest(w, r, err.Error())
				return
			}
			var formatStr string
			if len(parts) >= 6 {
				formatStr = parts[5]
			}
			err = dvid.WriteImageHttp(w, img, formatStr)
			if err != nil {
				badRequest(w, r, err.Error())
			}
		}
		dvid.ElapsedTime(dvid.Normal, startTime, "%s %s %s", op, dtype.TypeName(), slice)
	case dvid.Arb:
		badRequest(w, r, "DVID does not yet support arbitrary planes.")
	case dvid.Vol:
		badRequest(w, r, "DVID does not yet support volumes via HTTP API.  Use slices.")
	}
}

func badRequest(w http.ResponseWriter, r *http.Request, err string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", err, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
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
//  origin: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
//  image filename glob: filenames of images, e.g., foo-xy-*.png
//  plane: xy (default), xz, or yz.
//
// The image filename glob MUST BE absolute file paths that are visible to the server.
// This function is meant for mass ingestion of large data files, and it is inappropriate 
// to read gigabytes of data just to send it over the network to a local DVID.
func (dtype *Datatype) ServerAdd(request datastore.Request, reply *datastore.Response,
	service *datastore.Service) error {

	startTime := time.Now()

	// Parse the request
	var dataSetName, cmdStr, uuidStr, offsetStr string
	filenames := request.CommandArgs(0, &dataSetName, &cmdStr, &uuidStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
	}

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

	// Load each image and delegate to PUT function.
	//var wg sync.WaitGroup
	numSuccessful := 0
	var lastErr error
	for _, filename := range filenames {
		startTime := time.Now()
		img, _, err := dvid.ImageFromFile(filename)

		// DEBUG
		dataImg, _, _ := dvid.ImageData(img)
		dvid.PrintNonZero(filename, []byte(dataImg))

		if err != nil {
			lastErr = err
		} else {
			size := dvid.SizeFromRect(img.Bounds())
			slice, err := dvid.NewSlice(dataShape, offset, size)
			_, err = dtype.ProcessSlice(vs, datastore.PutOp, slice, img)
			dvid.ElapsedTime(dvid.Normal, startTime, "%s %s %s",
				datastore.PutOp, dtype.TypeName(), slice)
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
	//go dvid.WaitToComplete(&wg, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// Voxels implements the DataStruct interface for a rectangular blocks of voxels.
// The Voxels data is eventually broken down into blocks for processing, and each
// block is handled by the BlockHandler() defined in this package.
type Voxels struct {
	dvid.Geometry
	datastore.TypeService

	// The data itself
	data []uint8
}

func (v *Voxels) Data() []uint8 {
	return v.data
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s %s of size %d x %d x %d @ %s",
		v.TypeName(), v.DataShape(), size[0], size[1], size[2], v.Origin())
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
		default:
			err = unsupported()
		}
	case 4:
		switch dtype.BytesPerVoxel {
		case 1:
			img = &image.RGBA{v.data[beg:end], 4 * r.Dx(), r}
		default:
			err = unsupported()
		}
	default:
		err = unsupported()
	}
	return
}

// BlockHandler processes a block of data as part of a request.  The BlockRequest
// holds data (the larger structure, e.g., plane or subvolume) that cuts across
// this particular block (a small rectangular volume of voxels).  The data may be
// thinner, wider, and longer than the block, depending on the data shape (XY, XZ, etc)
func (v *Voxels) BlockHandler(req *datastore.BlockRequest) {

	// Compute the bounding voxel coordinates for this block.
	blockSize := v.BlockSize()
	minBlockVoxel := req.SpatialKey.OffsetToBlock(v)
	maxBlockVoxel := minBlockVoxel.AddSize(blockSize)

	// Compute the bound voxel coordinates for the data slice/subvolume and adjust
	// to our block bounds.
	minDataVoxel := v.Origin()
	maxDataVoxel := v.EndVoxel()
	begVolCoord := minDataVoxel.BoundMin(minBlockVoxel)
	endVolCoord := maxDataVoxel.BoundMax(maxBlockVoxel)

	// Calculate the strides
	dtype := v.TypeService.(*Datatype)
	bytesPerVoxel := int32(dtype.NumChannels * dtype.BytesPerVoxel)

	// Get useful data values
	data := req.DataStruct.Data()
	dataSize := v.Size()

	// Compute block coord matching beg's DVID volume space voxel coord
	blockBeg := begVolCoord.Sub(minBlockVoxel)

	// Compute index into the block byte buffer, blockI
	block := []uint8(req.Block)
	blockX := blockSize[0] * bytesPerVoxel
	blockXY := blockSize[1] * blockX

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	beg := begVolCoord.Sub(v.Origin())
	end := endVolCoord.Sub(v.Origin())

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.

	// TODO -- If data shape is Arbitrary plane, we need different looping.

	//dvid.Fmt(dvid.Debug, "Block start: %s\n", blockBeg)
	//dvid.Fmt(dvid.Debug, "Block buffer size: %d bytes\n", len(block))
	//dvid.Fmt(dvid.Debug, "Data buffer size: %d bytes\n", len(data))

	switch req.DataShape() {
	case dvid.XY:
		//fmt.Printf("XY Block handled: %s->%s for key %s\n", begVolCoord, endVolCoord, req.SpatialKey)
		blockI := blockBeg[2]*blockXY + blockBeg[1]*blockX + blockBeg[0]
		dataI := (beg[1]*dataSize[0] + beg[0]) * bytesPerVoxel
		for y := beg[1]; y <= end[1]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch req.Op {
			case datastore.GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case datastore.PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * bytesPerVoxel
			dataI += dataSize[0] * bytesPerVoxel
		}
	case dvid.XZ:
		blockI := blockBeg[2]*blockXY + blockBeg[1]*blockX + blockBeg[0]
		dataI := (beg[2]*v.Width() + beg[0]) * bytesPerVoxel
		for y := beg[2]; y <= end[2]; y++ {
			run := end[0] - beg[0] + 1
			bytes := run * bytesPerVoxel
			switch req.Op {
			case datastore.GetOp:
				copy(data[dataI:dataI+bytes], block[blockI:blockI+bytes])
			case datastore.PutOp:
				copy(block[blockI:blockI+bytes], data[dataI:dataI+bytes])
			}
			blockI += blockSize[0] * blockSize[1] * bytesPerVoxel
			dataI += v.Width() * bytesPerVoxel
		}
	case dvid.YZ:
		bx, bz := blockBeg[0], blockBeg[2]
		for y := beg[2]; y <= end[2]; y++ {
			dataI := (y*v.Width() + beg[1]) * bytesPerVoxel
			blockI := bz*blockXY + blockBeg[1]*blockX + bx
			for x := beg[1]; x <= end[1]; x++ {
				switch req.Op {
				case datastore.GetOp:
					copy(data[dataI:dataI+bytesPerVoxel], block[blockI:blockI+bytesPerVoxel])
				case datastore.PutOp:
					copy(block[blockI:blockI+bytesPerVoxel], data[dataI:dataI+bytesPerVoxel])
				}
				blockI += blockX
				dataI += bytesPerVoxel
			}
			bz++
		}
	default:
	}

	// If this is a PUT, place the modified block data into the database.
	if req.Op == datastore.PutOp {
		wo := keyvalue.NewWriteOptions()
		req.DB.Put(req.BlockKey, req.Block, wo)
	}

	// Notify the requestor that this block is done.
	if req.Wait != nil {
		req.Wait.Done()
	}
}
