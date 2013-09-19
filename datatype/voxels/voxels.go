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

	// "github.com/janelia-flyem/go/goprotobuf/proto"
)

const Version = "0.7"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/voxels"

const HelpMessage = `
API for datatypes derived from voxels (github.com/janelia-flyem/dvid/datatype/voxels)
=====================================================================================

Command-line:

$ dvid node <UUID> <data name> load local  <plane> <offset> <image glob>
$ dvid node <UUID> <data name> load remote <plane> <offset> <image glob>

    Adds image data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").

    Example: 

    $ dvid node 3f8c mygrayscale load local 0,0,100 xy data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    plane         One of "xy", "xz", or "yz".
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png
	
    ------------------

HTTP API (Level 2 REST):

GET  /api/node/<UUID>/<data name>/<plane>/<offset>/<size>[/<format>]
POST /api/node/<UUID>/<data name>/<plane>/<offset>/<size>[/<format>]

    Retrieves or puts orthogonal plane image data to named data within a version node.

    Example: 

    GET /api/node/3f8c/grayscale/xy/0,0,100/200,200/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    plane         One of "xy" (default), "xz", or "yz"
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    size          Size in pixels in the format "dx,dy".
	format        "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"

(TO DO)

GET  /api/node/<UUID>/<data name>/vol/<offset>/<size>[/<format>]
POST /api/node/<UUID>/<data name>/vol/<offset>/<size>[/<format>]

    Retrieves or puts 3d image volume to named data within a version node.

    Example: 

    GET /api/node/3f8c/grayscale/vol/0,0,100/200,200,200

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    size          Size in voxels in the format "dx,dy,dz"
	format        "sparse", "dense" (default: "dense")
                    Voxels returned are in thrift-encoded data structures.
                    See particular data type implementation for more detail.


GET  /api/node/<UUID>/<data name>/arb/<center>/<normal>/<size>[/<format>]

    Retrieves non-orthogonal (arbitrarily oriented planar) image data of named data 
    within a version node.

    Example: 

    GET /api/node/3f8c/grayscale/xy/200,200/2.0,1.3,1/100,100/jpg:80

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
	data := &Data{Data: basedata}
	data.BlockSize = DefaultBlockMax
	if obj, found := config["BlockSize"]; found {
		if blockSize, ok := obj.(Point3d); ok {
			data.BlockSize = blockSize
		} else {
			err = fmt.Errorf("BlockSize configuration is not a 3d point!")
			return
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
	service = data
	return
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
	dtype, ok := d.TypeService.(*Datatype)
	if !ok {
		log.Fatalf("Data %s does not have type of voxels.Datatype!", d.DataName())
	}
	return int32(dtype.BytesPerVoxel * dtype.NumChannels)
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
	source := request.Command[3]
	fmt.Printf("Request: %s\n", request.Command)
	switch source {
	case "local":
		d.LoadLocal(request, reply)
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

	// Get the data shape.
	shapeStr := DataShapeString(parts[2])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape {
	case XY, XZ, YZ:
		offsetStr, sizeStr := parts[3], parts[4]
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
		offsetStr, sizeStr := parts[3], parts[4]
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

// SliceImage returns an image.Image for the z-th slice of the volume.
func (d *Data) SliceImage(v *Voxels, z int) (img image.Image, err error) {
	dtype, ok := d.TypeService.(*Datatype)
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
//     $ dvid node 3f8c mygrayscale load local 0,0,100 data/*.png xy
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

	// Get plane
	plane, err := DataShapeString(planeStr).DataShape()
	if err != nil {
		return err
	}

	// Load and PUT each image.
	numSuccessful := 0
	var lastErr error
	for _, filename := range filenames {
		sliceTime := time.Now()
		img, _, err := dvid.ImageFromFile(filename)
		if err != nil {
			lastErr = err
		} else {
			size := SizeFromRect(img.Bounds())
			slice, err := NewSlice(plane, offset, size)

			err = d.PutImage(versionID, img, slice)

			dvid.ElapsedTime(dvid.Debug, sliceTime, "%s load local %s", d.DataName(), slice)
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
	dvid.ElapsedTime(dvid.Debug, startTime, "RPC load local (%s) completed", addedFiles)
	return nil
}

// Operation holds Voxel-specific data for processing chunks.
type Operation struct {
	data *Voxels
	vID  dvid.LocalID
	op   OpType
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
)

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetImage(versionID dvid.LocalID, slice Geometry) (img image.Image, err error) {
	db := server.KeyValueDB()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
	}
	bytesPerVoxel := d.BytesPerVoxel()
	stride := slice.Width() * bytesPerVoxel

	numBytes := int64(bytesPerVoxel) * slice.NumVoxels()
	data := make([]uint8, numBytes, numBytes)

	voxels := Voxels{slice, data, stride}

	op := Operation{
		data: &voxels,
		vID:  versionID,
		op:   GetOp,
	}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&op, wg}

	blockSize := d.BlockSize

	// Setup traversal
	startVoxel := slice.Origin()
	endVoxel := slice.EndVoxel()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	fmt.Printf("GetImage from startBlockCoord %s -> endBlockCoord %s\n", startBlockCoord, endBlockCoord)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := IndexZYX{startBlockCoord[0], y, z}
			i1 := IndexZYX{endBlockCoord[0], y, z}
			startKey := &storage.Key{d.DatasetID, d.ID, versionID, i0}
			endKey := &storage.Key{d.DatasetID, d.ID, versionID, i1}

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
	img, err = d.SliceImage(&voxels, 0)
	return
}

// PutImage adds a 2d image within given geometry to a version node.   Since chunk sizes
// are larger than a 2d slice, this also requires integrating this image into current
// chunks before writing result back out, so it's a PUT for nonexistant keys and GET/PUT
// for existing keys.
func (d *Data) PutImage(versionID dvid.LocalID, img image.Image, slice Geometry) error {
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

	voxels := Voxels{slice, data, stride}

	wg := new(sync.WaitGroup)
	op := Operation{
		data: &voxels,
		vID:  versionID,
		op:   PutOp,
	}
	chunkOp := &storage.ChunkOp{&op, wg}

	blockSize := d.BlockSize

	// Setup traversal
	startVoxel := slice.Origin()
	endVoxel := slice.EndVoxel()

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	versionMutex := datastore.VersionMutex(d, versionID)
	versionMutex.Lock()

	// Map: Iterate in x, then y, then z
	startBlockCoord := startVoxel.BlockCoord(blockSize)
	endBlockCoord := endVoxel.BlockCoord(blockSize)
	fmt.Printf("PutImage from startBlockCoord %s -> endBlockCoord %s\n", startBlockCoord, endBlockCoord)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			// We know for voxels indexing, x span is a contiguous range.
			i0 := IndexZYX{startBlockCoord[0], y, z}
			i1 := IndexZYX{endBlockCoord[0], y, z}
			startKey := &storage.Key{d.DatasetID, d.ID, versionID, i0}
			endKey := &storage.Key{d.DatasetID, d.ID, versionID, i1}

			// GET all the chunks for this range.
			keyvalues, err := db.GetRange(startKey, endKey)
			if err != nil {
				return fmt.Errorf("Error in reading data during PUT %s: %s",
					d.DataName(), err.Error())
			}

			// Send all data to chunk handlers for this range.
			var kv, oldkv storage.KeyValue
			numOldkv := len(keyvalues)
			v := 0
			if numOldkv > 0 {
				oldkv = keyvalues[v]
			}
			wg.Add(int(endBlockCoord[0]-startBlockCoord[0]) + 1)
			for x := startBlockCoord[0]; x <= endBlockCoord[0]; x++ {
				i := IndexZYX{x, y, z}
				key := &storage.Key{d.DatasetID, d.ID, versionID, i}
				// Check for this key among old key-value pairs and if so,
				// send the old value into chunk handler.
				if oldkv.K != nil {
					zyx := oldkv.K.Index.(IndexZYX)
					if zyx[0] == x {
						kv = oldkv
						v++
						if v < numOldkv {
							oldkv = keyvalues[v]
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
				fmt.Printf("PUT Process Chunk key %s\n", kv.K)
				go d.ProcessChunk(&storage.Chunk{chunkOp, kv})
			}
		}
	}
	wg.Wait()
	versionMutex.Unlock()

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

// ProcessChunk processes a chunk of data as part of a mapped operation.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc).
func (d *Data) ProcessChunk(chunk *storage.Chunk) {

	operation, ok := chunk.Op.(*Operation)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}
	voxels := operation.data
	index, ok := chunk.K.Index.(IndexZYX)
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
	dtype, ok := d.TypeService.(*Datatype)
	if !ok {
		log.Fatalf("Illegal data used for ProcessChunk [not voxels.Datatype]: %s\n", *d)
	}
	bytesPerVoxel := int32(dtype.NumChannels * dtype.BytesPerVoxel)
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
	beg := begVolCoord.Sub(voxels.Origin())
	end := endVolCoord.Sub(voxels.Origin())

	// For each geometry, traverse the data slice/subvolume and read/write from
	// the block data depending on the op.
	data := voxels.data

	//fmt.Printf("Data %s -> %s, Orig %s -> %s\n", beg, end, begVolCoord, endVolCoord)
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
		db.Put(chunk.K, serialization)
	}

	// Notify the requestor that this chunk is done.
	if chunk.Wg != nil {
		chunk.Wg.Done()
	}
}
