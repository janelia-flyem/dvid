package grayscale8

import (
	"fmt"
	"image"
	"reflect"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

const Version = "0.5"

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

const helpMessage = `
	grayscale8 is simply a 'voxels' data type with 8-bits per voxel and 1 channel.
	See voxels help for usage.
`

// Grayscale8 has one byte/voxel
const BytesPerVoxel = 1

// Grayscale8 Datatype simply embeds voxels.Datatype to create a unique type
// (grayscale8.Datatype) with grayscale functions.
type Datatype struct {
	voxels.Datatype
}

func init() {
	datastore.RegisterDatatype(&Datatype{
		voxels.Datatype{
			datastore.Datatype{
				datastore.MakeDatatypeID("grayscale8", RepoUrl, Version),
				BlockMax:    DefaultBlockMax,
				Indexing:    datastore.SIndexZYX,
				IsolateDate: true,
			},
			NumChannels:   1,
			BytesPerVoxel: 1,
		},
	})
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
func (datatype *Datatype) ServerAdd(cmd *datastore.Command) error {

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
			err = datatype.processBlocks(cmd.VersionService(), datastore.PutOp, subvol, &wg)
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
func (datatype *Datatype) Get(cmd *datastore.Command) error {
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
	dvid.WaitToComplete(&wg, time.Now(), "GET vol (%s)", subvol)
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
