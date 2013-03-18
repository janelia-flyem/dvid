/*
	Package voxels implements DVID support for data using voxels as elements
*/
package voxels

import (
	"fmt"
	"image"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/cache"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
)

const Version = "0.5"

const RepoUrl = "github.com/janelia-flyem/dvid/datatype/voxels"

const HelpMessage = `
    Voxel Data Type Server-side Commands:

        voxel  server-post  <origin>  <image filename glob> [plane=<plane>]

    <origin>: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    <image filename glob>: filenames of images, e.g., foo-xy-*.png
    <plane>: xy (default), xz, or yz

    Note that the image filename glob MUST BE absolute file paths that are visible to
    the server.  

    The 'server-post' command is meant for mass ingestion of large data files, and
    it is inappropriate to read gigabytes of data just to send it over the network to
    a local DVID.

    If you want to send local data to a remote DVID, use POST via the HTTP API.
	
		voxel get <uuid> <data shape> <offset> <size> [<normal>] [<format>]

    ------------------

    Voxel Data Type HTTP API

    The voxel data type supports the following Level 2 REST HTTP API calls:

    1) Adding different voxel data sets

        POST /api/data/<voxel type>/<data set name>

    Parameters:
    <voxel type> = "voxel", "voxelN" where 'N' is the # bits per voxel,
        "voxelN-C" where 'C' is the # of channels.
    <data set name> = name of a data set of this type. 

    Examples:

        POST /api/data/voxels8-3/rgb8

    Adds a data set name "rgb8" that uses the voxels data type at 8 bits/voxel
    and with 3 channels.

    2) Storing and retrieving voxel data for a particular voxel data set

    If <data shape> is "xy", "xz", "yz", or "vol"

        POST /api/<data set name>/<uuid>/<data shape>/<offset>/<size>
        GET  /api/<data set name>/<uuid>/<data shape>/<offset>/<size>[/<format>]

    If <data shape> is "arb":

        GET  /api/<data set name>/<uuid>/<data shape>/<offset>/<top right>/<bottom left>[/<format>]

    Parameters:
    <data set name> = Data set name that is of type voxel, e.g., "rgb8" or "labels16"
    <uuid> = Hexadecimal number, as many digits as necessary to uniquely identify
       the UUID among nodes in this database.
    <data shape> = "xy", "xz", "yz", "arb", "vol"
    <offset> = x,y,z
    <size> = dx,dy,dz (if <volume> is "vol")
             dx,dy    (if <volume> is not "vol")
	<format> = "png", "jpg", "hdf5" (default: "png")
	<top right> = x,y,z of top right corner of arbitrary slice
	<bottom left> = x,y,z of bottom left corner of arbitrary slice

    Examples:

        GET /api/myvoxels/c66e0/xy/0,125,135/250,240

    Returns a PNG image with width 250 pixels, height 240 pixels that were taken
    from the XY plane with the upper left pixel sitting at (0,125,135) in 
    the image volume space.  The # bits/voxel and the # of channels are determined
    by the type of data associated with the data set name 'myvoxels'
`

const (
	// DefaultBlockMax specifies the default size for each block of this data type.
	DefaultBlockMax = dvid.VoxelCoord{16, 16, 16}

	// NumBlockHandlers specifies the number of block handlers for this data type
	// for each image version. 
	NumBlockHandlers = 8
)

// Datatype simply embeds the datastore's Datatype to create a unique type
// with voxel functions.  Refinements of general voxel types can be implemented 
// by simply embedding this type in another voxel-oriented type.
type Datatype struct {
	datastore.Datatype
	NumChannels   int
	BytesPerVoxel int
}

// Block satisfies the datastore.Block interface and is the unit of get/put for
// this type.  Handling of a Block is up to each data type.  Efficient storing
// and retrieval of a Block is delegated to the datastore.
type Block struct {
	dvid.DataShaper
	datastore.DataPacker

	op         datastore.OpType
	spatialKey datastore.SpatialIndex
	blockKey   keyvalue.Key

	modified time.Time

	// Let's us notify requestor when all blocks are done.
	wg *sync.WaitGroup
}

// BlockProcessor is a mutex to group all block requests for a particular
// subvolume.  This lock is necessary to prevent concurrent subvolume requests from
// interleaving block-level requests, possibly allowing returned subvolumes to show
// partial results.  For example, if we do concurrent PUT and GET of the same
// subvolume, some of the blocks processed during the GET would pick up partial PUT
// block processing.  See processBlocks() function in code.
var BlockProcessor sync.Mutex

func init() {
	// All the other parameters will be filled in during runtime 
	datastore.RegisterDatatype(&Datatype{
		datastore.Datatype{
			datastore.MakeDatatypeID("voxels", RepoUrl, Version),
			BlockMax:    DefaultBlockMax,
			Indexing:    datastore.SIndexZYX,
			IsolateDate: true,
		},
	})
}

// Do acts as a switchboard for RPC commands. 
func (dtype *Datatype) DoRPC(request *datastore.Request, reply dvid.Response) error {
	switch request.TypeCommand() {
	case "server-post":
		return dtype.ServerAdd(request, reply)
	//	case "get":
	//		return dtype.Get(request, reply)
	case "help":
		reply.SetText(dtype.Help(HelpMessage))
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
	const lenPath = len(apiPrefixURL)
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")

	// The data name may include bits/voxel and # channels, e.g., "voxel16-3"

	// Get the datastore service corresponding to given version
	uuidStr := parts[1]
	uuidNum, err := runningService.GetUuidFromString(uuidStr)
	if err != nil {
		badRequest(w, r, fmt.Sprintf("No version corresponds to uuid '%s'", uuidStr))
		return
	}
	vs := datastore.NewVersionService(service, uuidNum)

	// Get the data shape. 
	shapeStr := dvid.DataShapeStr(strings.ToLower(parts[2]))
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		badRequest(w, r, fmt.Sprintf("Bad data shape given '%s'", shapeStr))
		return
	}

	// Get the offset and size of the data
	offsetStr := dvid.PointStr(parts[3])
	offset, err := offsetStr.VoxelCoord()
	if err != nil {
		badRequest(w, r, fmt.Sprintf("Unable to parse Offset as 'x,y,z', got '%s'", offsetStr))
		return
	}

	sizeStr := dvid.PointStr(parts[4])

	var normalStr dvid.VectorStr
	if len(parts) > 5 {
		normalStr = dvid.VectorStr(parts[5])
	}

	// Get components of the block key
	uuidBytes := vs.UuidBytes()
	datatypeBytes := vs.DataIndexBytes(dtype.TypeName())

	// Further action depends on the shape of the data
	switch dataShape {
	case XY, XZ, YZ:
		size, err := sizeStr.Point2d()
		if err != nil {
			badRequest(w, r, fmt.Sprintf("Unable to parse Size as 'x,y', got '%s'", sizeStr))
			return
		}
		var slice dvid.Slice
		switch dataShape {
		case XY:
			slice = dvid.NewSliceXY(&offset, &size)
		case XZ:
			slice = dvid.NewSliceXZ(&offset, &size)
		case YZ:
			slice = dvid.NewSliceYZ(&offset, &size)
		case Arb:
			normal, err := normalStr.Vector3d()
			if err != nil {
				badRequest(w, r, fmt.Sprintf("Unable to parse normal as 'x,y,z', got '%s'",
					normalStr))
				return
			}
			slice = dvid.NewSliceArb(&offset, &size, &normal)
		default:
			badRequest(w, r, fmt.Sprintf("Illegal slice plane specification (%s)", parts[3]))
			return
		}

		// TODO -- if POST, transfer POSTed image into sliceVoxels.Data 

		// Make sure we have Block Handlers for this data type.  If this weren't
		// specified, the ProcessBlock() routine would create channels and block
		// handlers, but this way, we can specify the number of block handlers
		// specific for each data type.
		service.ReserveBlockHandlers(dtype, NumBlockHandlers)

		// Traverse blocks, get key/values if not in cache, and put block in queue for handler.
		var wg sync.WaitGroup
		ro = keyvalue.NewReadOptions()
		db_it, err := vs.kvdb.NewIterator(ro)
		defer db_it.Close()
		if err != nil {
			badRequest(w, r, err.Error())
			return
		}
		spatial_it := datastore.NewSpatialIterator(dtype, slice.Volume)
		start := true
		for {
			spatialBytes := spatial_it()
			if spatialBytes == nil {
				break
			}
			blockKey := datastore.BlockKey(uuidBytes, spatialBytes, datatypeBytes, dtype.IsolateData)

			// Is this block in the cache?
			data, found := datastore.GetCachedBlock(blockKey)

			// If not, pull from the datastore
			if !found {
				if start || db_it.Key() != blockKey {
					db_it.Seek(blockKey)
					start = false
				}
				if db_it.Valid() {
					data = db_it.Value()
					datastore.SetCachedBlock(blockKey, data)
				}

				// Advance the database iterator
				db_it.Next()
			}

			// Initialize the block request
			block := &Block{
				slice,
				DataPacker{
					dtype,
					Data: make([]uint8, dtype.BytesPerVoxel*dtype.NumChannels*slice.NumVoxels()),
				},
				op:         op,
				spatialKey: SpatialIndex(spatialBytes),
				blockKey:   blockKey,
				wg:         &wg,
			}

			// Put this block on a channel that's read by its handler.
			vs.ProcessBlock(block)
		}

		// Wait for all block handling to finish, then return result. 
		dvid.WaitToComplete(&wg, startTime, "GET vol (%s)", sliceVoxels.DataShaper)

		// Write the image to requestor
		w.Header().Set("Content-type", "image/png")
		png.Encode(w, typeService.GetSlice(&slice))
	case Arb:
		badRequest(w, r, "DVID does not yet support arbitrary planes.")
		return
	case Vol:
		badRequest(w, r, "DVID does not yet support volumes via HTTP API.  Use slices.")
		return
	}
}

func badRequest(w http.ResponseWriter, r *http.Request, err string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", err, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// Make sure we load correct image format.
func loadImage(filename string) (grayImage *image.Gray, err error) {
	img, _, err := datastore.LoadImage(filename)
	if err != nil {
		return
	}
	switch img.(type) {
	case *image.Gray:
		// pass
	default:
		err = fmt.Errorf(
			"Illegal image (%s) for grayscale8: Pixels aren't 8-bit grayscale = %s",
			filename, reflect.TypeOf(img))
		return
	}
	grayImage = img.(*image.Gray)
	return
}

// ServerAdd does a server-side PUT of a series of 2d grayscale8 images.
// The images are specified as a filename glob that must be visible to the server.  
// This is a mechanism for fast ingestion of large quantities of data, 
// so we don't want to pass all the data over the network using PUT slice.
func (v *Datatype) ServerAdd(request *datastore.Request, reply dvid.Response) error {
	var originStr string
	filenames := cmd.SetDatatypeArgs(&originStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", cmd)
	}
	coord, err := dvid.PointStr(originStr).VoxelCoord()
	if err != nil {
		return fmt.Errorf("Badly formatted origin (should be 'x,y,z'): %s", cmd)
	}
	planeStr, found := cmd.GetSetting(dvid.KeyPlane)
	if !found {
		planeStr = "xy"
	}

	startTime := time.Now()

	dvid.Log(dvid.Debug, "plane: %s\n", planeStr)
	dvid.Log(dvid.Debug, "origin: %s\n", coord)
	var addedFiles string
	if len(filenames) == 1 {
		addedFiles = filenames[0]
	} else {
		addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}
	dvid.Log(dvid.Debug, addedFiles+"\n")

	// Load each image and delegate to PUT function.
	var wg sync.WaitGroup
	numSuccessful := 0
	var lastErr error
	for _, filename := range filenames {
		grayImage, err := loadImage(filename)
		if err != nil {
			lastErr = err
		} else {
			subvol := &dvid.Subvolume{
				Text:   filename,
				Offset: coord,
				Size: dvid.VoxelCoord{
					int32(grayImage.Bounds().Max.X - grayImage.Bounds().Min.X),
					int32(grayImage.Bounds().Max.Y - grayImage.Bounds().Min.Y),
					1,
				},
				DataPacker: dvid.DataPacker{
					BytesPerVoxel: BytesPerVoxel,
					Data:          grayImage.Pix,
				},
			}
			err = v.processBlocks(cmd.VersionService(), datastore.PutOp, subvol, &wg)
			if err == nil {
				numSuccessful++
			} else {
				lastErr = err
			}
		}
		coord = coord.Add(&dvid.VoxelCoord{0, 0, 1})
	}
	if lastErr != nil {
		return fmt.Errorf("Error: %d of %d images successfully added [%s]\n",
			numSuccessful, len(filenames), lastErr.Error())
	}
	go dvid.WaitToComplete(&wg, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// Get determines what kind of data we need and delegates to GetSlice or GetVolume.
func (dtype *Datatype) Get(request *datastore.Request, reply dvid.Response) error {
	// TODO
	return nil
}

// GetSlice returns an image.Image
func (v *Datatype) GetSlice(subvol *dvid.Subvolume, planeStr string) image.Image {
	var r image.Rectangle
	switch planeStr {
	case "xy":
		r = image.Rect(0, 0, int(subvol.Size[0]), int(subvol.Size[1]))
	case "xz":
		r = image.Rect(0, 0, int(subvol.Size[0]), int(subvol.Size[2]))
	case "yz":
		r = image.Rect(0, 0, int(subvol.Size[1]), int(subvol.Size[2]))
	default:
		fmt.Println("Bad plane:", planeStr)
		return nil
	}
	sliceBytes := r.Dx() * r.Dy() * v.BytesPerVoxel
	data := make([]byte, sliceBytes, sliceBytes)
	copy(data, subvol.Data)
	return &image.Gray{data, 1 * r.Dx(), r}
}

// GetVolume breaks a subvolume GET into blocks, processes the blocks using
// multiple handler, and then returns when all blocks have been processed.
func (v *Datatype) GetVolume(vs *datastore.VersionService, subvol *dvid.Subvolume) error {
	var wg sync.WaitGroup
	datatype.processBlocks(vs, datastore.GetOp, subvol, &wg)
	dvid.WaitToComplete(&wg, time.Now(), "GET vol (%s)", subvol)
	return nil
}

// traverseXY iterates through blocks pertinent to a SliceXY, loads data into
// the cache, and sends off requests to BlockHandlers (kicks off the 'map' stage).
func (dtype *Datatype) traverseVolume(v Volume) {
	var wg sync.WaitGroup
	// TODO
	dvid.WaitToComplete(&wg, time.Now(), "SliceXY OP on %s", s)
}

// traverseSubvol iterates through blocks pertinent to a Subvol, loads data into
// the cache, and sends off requests to BlockHandlers (kicks off the 'map' stage).

func (v *Datatype) processBlocks(vs *datastore.VersionService,
	op datastore.OpType, subvol *dvid.Subvolume, wg *sync.WaitGroup) error {

	dvid.Log(dvid.Debug, "grayscale8.ProcessBlocks(OpType %d): %s\n", int(op), subvol)

	// Translate UUID index into bytes
	uuidBytes := vs.UuidBytes()

	// Determine the index of this datatype for this particular datastore.
	var datatypeIndex int8 = -1
	for i, d := range vs.Datatypes {
		if d.Url == v.Url {
			datatypeIndex = int8(i)
			break
		}
	}
	if datatypeIndex < 0 {
		return fmt.Errorf("Could not match datatype (%s) to supported data types!", v.Url)
	}

	// Iterate through all blocks traversed by this input data.
	// Do this under a package-wide block.
	startVoxel := subvol.Offset
	endVoxel := startVoxel.Add(&subvol.Size)

	startBlockCoord := vs.BlockCoord(startVoxel)
	endBlockCoord := vs.BlockCoord(endVoxel)

	BlockProcessor.Lock() // ----- Group all ProcessBlock() for a goroutine

	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			for x := startBlockCoord[0]; x <= endBlockCoord[0]; x++ {
				blockCoord := dvid.BlockCoord{x, y, z}
				spatialIndex := vs.SpatialIndex(blockCoord)
				blockKey := datastore.BlockKey(uuidBytes, []byte(spatialIndex),
					byte(datatypeIndex), v.IsolateData)
				if wg != nil {
					wg.Add(1)
				}
				blockRequest := datastore.NewBlockRequest(op, blockCoord,
					blockKey, subvol, wg)
				vs.ProcessBlock(blockRequest)
			}
		}
	}

	BlockProcessor.Unlock()

	return nil
}
