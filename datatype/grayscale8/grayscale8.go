package grayscale8

import (
	"fmt"
	"image"
	"reflect"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.5"

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

const helpMessage = `
    Grayscale8 Data Type Server-side Commands:

        grayscale8  server-add  <origin>  <image filename glob> [plane=<plane>]

    <origin>: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    <image filename glob>: filenames of images, e.g., foo-xy-*.png
    <plane>: xy (default), xz, or yz

    Note that the image filename glob MUST BE absolute file paths that are visible to
    the server.  

    The 'server-add' command is meant for mass ingestion of large data files, and
    it is inappropriate to read gigabytes of data just to send it over the network to
    a local DVID.

    If you want to send local data to a remote DVID, use POST via the HTTP API.

    ------------------

    Grayscale8 Data Type HTTP API

    POST /api/<uuid>/grayscale8/<volume>/<offset>/<size>[/<normal>]
    GET  /api/<uuid>/grayscale8/<volume>/<offset>/<size>[/<normal>]

    Parameters:
    <uuid> = Hexadecimal number, as many digits as necessary to uniquely identify
       the UUID among nodes in this database.
    <volume> = "xy", "xz", "yz", "arb", "vol"
    <offset> = x,y,z
    <size> = dx,dy,dz (if <volume> is "vol")
             dx,dy    (if <volume> is not "vol")
    <normal> = x,y,z if <volume> is "arb"

    Examples:

    GET /api/c66e0/grayscale8/xy/0,125,135/250,240

    Returns an image with width 250 pixels, height 240 pixels that were taken
    from the XY plane with the upper left pixel sitting at (0,125,135) in 
    the image volume space.
`

// Grayscale8 has one byte/voxel
const BytesPerVoxel = 1

// Grayscale8 Datatype simply embeds the datastore's Datatype to create a unique type
// (grayscale8.Datatype) with grayscale functions.
type Datatype struct {
	datastore.Datatype
}

// BlockProcessor is a grayscale8 mutex to group all block requests for a particular
// subvolume.  This lock is necessary to prevent concurrent subvolume requests from
// interleaving block-level requests, possibly allowing returned subvolumes to show
// partial results.  For example, if we do concurrent PUT and GET of the same
// subvolume, some of the blocks processed during the GET would pick up partial PUT
// block processing.  See processBlocks() function in code.
var BlockProcessor sync.Mutex

func init() {
	datastore.RegisterDatatype(&Datatype{datastore.Datatype{
		Name:        "grayscale8",
		Version:     Version,
		Url:         repoUrl,
		IsolateData: true,
	}})
}

// BaseDatatype returns the embedded Datatype if desired
func (datatype *Datatype) BaseDatatype() datastore.Datatype {
	return datatype.Datatype
}

// BytesPerVoxel returns the # of bytes per voxel for grayscale8
func (datatype *Datatype) BytesPerVoxel() int {
	return BytesPerVoxel
}

// Do acts as a switchboard for grayscale8 commands
func (datatype *Datatype) Do(versionService *datastore.VersionService,
	cmd dvid.Request, input, reply dvid.Response) error {

	switch cmd.TypeCommand() {
	case "server-add":
		return datatype.ServerAdd(versionService, cmd, input, reply)
	case "get":
		// verify vol type
		return datatype.Get(versionService, cmd, input, reply)
	case "help":
		reply.Text = datatype.Help(helpMessage)
	default:
		return datatype.UnknownCommand(cmd)
	}
	return nil
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
// so we don't want to pass all the data over the network using PutSlice().
//
// Aside from loading the images, most of the work is delegated to PutSlice(),
// which is used for the REST API.
func (datatype *Datatype) ServerAdd(versionService *datastore.VersionService,
	cmd *command.Command, input, reply *command.Packet) error {

	var originStr string
	filenames := cmd.SetDatatypeArgs(&originStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", cmd)
	}
	coord, err := dvid.CoordStr(originStr).VoxelCoord()
	if err != nil {
		return fmt.Errorf("Badly formatted origin (should be 'x,y,z'): %s", cmd)
	}
	planeStr, found := cmd.GetSetting(command.KeyPlane)
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
			imagePacket := command.Packet{dvid.Subvolume{
				Text:   filename,
				Offset: coord,
				Size: dvid.VoxelCoord{
					int32(grayImage.Bounds().Max.X - grayImage.Bounds().Min.X),
					int32(grayImage.Bounds().Max.Y - grayImage.Bounds().Min.Y),
					1,
				},
				BytesPerVoxel: BytesPerVoxel,
				Data:          grayImage.Pix,
			}}
			err = datatype.processBlocks(versionService, datastore.PutOp,
				&(imagePacket.Subvolume), &wg)
			if err == nil {
				numSuccessful++
			} else {
				lastErr = err
			}
		}
		coord = coord.Add(dvid.VoxelCoord{0, 0, 1})
	}
	if lastErr != nil {
		return fmt.Errorf("Error: %d of %d images successfully added [%s]\n",
			numSuccessful, len(filenames), lastErr.Error())
	}
	go dvid.WaitToComplete(&wg, startTime, "RPC server-add (%s) completed", addedFiles)
	return nil
}

// GetSlice returns an image.Image for grayscale8
func (datatype *Datatype) GetSlice(v *dvid.Subvolume, planeStr string) image.Image {
	var r image.Rectangle
	switch planeStr {
	case "xy":
		r = image.Rect(0, 0, int(v.Size[0]), int(v.Size[1]))
	case "xz":
		r = image.Rect(0, 0, int(v.Size[0]), int(v.Size[2]))
	case "yz":
		r = image.Rect(0, 0, int(v.Size[1]), int(v.Size[2]))
	default:
		fmt.Println("Bad plane:", planeStr)
		return nil
	}
	sliceBytes := r.Dx() * r.Dy() * BytesPerVoxel
	data := make([]byte, sliceBytes, sliceBytes)
	copy(data, v.Data)
	return &image.Gray{data, 1 * r.Dx(), r}
}

// GetVolume breaks a subvolume GET into blocks, processes the blocks using
// multiple handler, and then returns when all blocks have been processed.
func (datatype *Datatype) GetVolume(vs *datastore.VersionService,
	subvol *dvid.Subvolume) error {

	var wg sync.WaitGroup
	datatype.processBlocks(vs, datastore.GetOp, subvol, &wg)
	dvid.WaitToComplete(&wg, time.Now(), "GET subvolume (%s) completed", subvol)
	return nil
}

// processBlocks processes all the blocks pertinent to a single image subvolume
func (datatype *Datatype) processBlocks(vs *datastore.VersionService,
	op datastore.OpType, subvol *dvid.Subvolume, wg *sync.WaitGroup) error {

	dvid.Log(dvid.Debug, "grayscale8.ProcessBlocks(OpType %d): %s\n", int(op), subvol)

	// Translate UUID index into bytes
	uuidBytes := vs.UuidBytes()

	// Determine the index of this datatype for this particular datastore.
	var datatypeIndex int8 = -1
	for i, d := range vs.Datatypes {
		if d.Url == datatype.Url {
			datatypeIndex = int8(i)
			break
		}
	}
	if datatypeIndex < 0 {
		return fmt.Errorf("Could not match datatype (%s) to supported data types!",
			datatype.Url)
	}

	// Iterate through all blocks traversed by this input data.
	// Do this under a package-wide block.
	startVoxel := subvol.Offset
	endVoxel := startVoxel.Add(subvol.Size)

	startBlockCoord := vs.BlockCoord(startVoxel)
	endBlockCoord := vs.BlockCoord(endVoxel)

	BlockProcessor.Lock() // ----- Group all ProcessBlock() for a goroutine

	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			for x := startBlockCoord[0]; x <= endBlockCoord[0]; x++ {
				blockCoord := dvid.BlockCoord{x, y, z}
				spatialIndex := vs.SpatialIndex(blockCoord)
				blockKey := datastore.BlockKey(uuidBytes, []byte(spatialIndex),
					byte(datatypeIndex), datatype.IsolateData)
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
