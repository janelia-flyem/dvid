package grayscale8

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image"
	"log"
	"reflect"
	_ "strconv"
	_ "strings"

	"github.com/janelia-flyem/dvid/command"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

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

    POST /grayscale8/<origin>/<size>
    GET /grayscale8/<origin>/<size>
`

// Grayscale8 has one byte/voxel
const BytesPerVoxel = 1

type Datatype struct {
	datastore.Datatype
}

func init() {
	datastore.RegisterDatatype(&Datatype{
		datastore.Datatype{
			Name:        "grayscale8",
			Url:         repoUrl,
			IsolateData: true,
		},
	})
}

// Do acts as a switchboard for grayscale8 commands
func (datatype *Datatype) Do(versionService *datastore.VersionService,
	cmd *command.Command, input, reply *command.Packet) error {

	switch cmd.TypeCommand() {
	case "server-add":
		return datatype.ServerAdd(versionService, cmd, input, reply)
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

// ServerAdd stores a series of 2d images specified as an image filename glob
// that is visible to the server.  This is a mechanism for fast ingestion
// of large quantities of data, so we don't want to pass all the data over
// the network just for client/server communication.  Aside from loading the images,
// most of the work is delegated to the Add function that will also be called
// via the REST API.
func (datatype *Datatype) ServerAdd(versionService *datastore.VersionService,
	cmd *command.Command, input, reply *command.Packet) error {

	var originStr command.CoordStr
	filenames := cmd.SetDatatypeArgs(&originStr.string)
	coord, err := originStr.GetCoord()
	if err != nil {
		return fmt.Errorf("Badly formatted origin (should be 'x,y,z'): %s", cmd)
	}
	planeStr, found := cmd.GetSetting(command.KeyPlane)
	if !found {
		planeStr = "xy"
	}

	log.Printf("")
	log.Println("plane:", planeStr)
	log.Println("origin:", coord)
	if len(filenames) >= 1 {
		fmt.Printf("filenames: %s [%d more]\n", filenames[0], len(filenames)-1)
	}

	// Load each image and use "Add" function that would normally be called when
	// adding []byte after unpacking from image file.
	// TODO -- test parallel version by packaging below as Goroutine.
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
			var addReply command.Packet
			err = datatype.Add(versionService, &imagePacket, &addReply)
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
	return nil
}

// Add stores a single image volume into the datastore.
func (datatype *Datatype) Add(vs *datastore.VersionService,
	input, reply *command.Packet) error {

	log.Println("grayscale8.Add(): Processing ", input)

	// Translate UUID index into bytes
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, vs.UuidNum())
	if err != nil {
		return fmt.Errorf("Error encoding binary uuid %d: %s", vs.UuidNum(), err.Error())
	}
	uuidIndex := buf.Bytes()

	// Determine the index of this datatype for this particular datastore.
	var datatypeIndex int8 = -1
	for i, d := range vs.Datatypes {
		if d.Url == datatype.GetUrl() {
			datatypeIndex = int8(i)
			break
		}
	}
	if datatypeIndex < 0 {
		return fmt.Errorf("Could not match datatype (%s) to supported data types!",
			datatype.GetUrl())
	}

	// Iterate through all blocks traversed by this input data.
	startVoxel := input.Offset
	endVoxel := startVoxel.Add(input.Size)

	startBlockCoord := vs.BlockCoord(startVoxel)
	endBlockCoord := vs.BlockCoord(endVoxel)
	for z := startBlockCoord[2]; z <= endBlockCoord[2]; z++ {
		for y := startBlockCoord[1]; y <= endBlockCoord[1]; y++ {
			for x := startBlockCoord[0]; x <= endBlockCoord[0]; x++ {
				blockCoord := dvid.BlockCoord{x, y, z}
				spatialIndex := vs.SpatialIndex(blockCoord)
				//
				// NEXT -- Figure out how to determine BlockKey
				blockKey := datastore.BlockKey(uuidIndex, []byte(spatialIndex),
					byte(datatypeIndex), datatype.IsolateData)
				vs.WriteBlock(&datastore.BlockRequest{
					blockCoord,
					blockKey,
					&input.Subvolume,
				})
			}
		}
	}

	return nil
}
