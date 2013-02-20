package grayscale8

import (
	"fmt"
	"image"
	"reflect"
	_ "strconv"
	_ "strings"

	"github.com/janelia-flyem/dvid/command"
	"github.com/janelia-flyem/dvid/datastore"
)

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

const helpMessage = `
    Grayscale8 Data Type Server-side Commands:

        grayscale8  server_load  <plane>  <origin>  <image filename glob>

    <plane>: xy, xz, or yz
    <origin>: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    <image filename glob>: filenames of images, e.g., foo-xy-*.png

    Note that the image filename glob MUST BE absolute file paths that are visible to
    the server.  

    The 'server_load' command is meant for mass ingestion of large data files, and
    it is inappropriate to read gigabytes of data just to send it over the network to
    a local DVID.

    If you want to send local data to a remote DVID, use PUT via the HTTP API.

    ------------------

    Grayscale8 Data Type HTTP API

    PUT /grayscale8/<plane>/<origin>
    GET /grayscale8/<plane>/<extent>
`

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
func (datatype *Datatype) Do(uuidNum int, cmd *command.Command,
	input *command.Packet, reply *command.Packet) error {

	switch cmd.TypeCommand() {
	case "server_load":
		return datatype.ServerLoad(uuidNum, cmd, input, reply)
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
		err = fmt.Errorf("Illegal image (%s) for grayscale8: Pixels aren't 8-bit grayscale = %s",
			filename, reflect.TypeOf(img))
		return
	}
	grayImage = img.(*image.Gray)
	return
}

// ServerLoad stores a series of 2d images specified as an image filename glob
// that is visible to the server.  This is a mechanism for fast ingestion
// of large quantities of data, so we don't want to pass all the data over
// the network just for client/server communication.  Aside from loading the images,
// most of the work is delegated to the "add" function that will also be called
// via the REST API.
func (datatype *Datatype) ServerLoad(uuidNum int, cmd *command.Command,
	input *command.Packet, reply *command.Packet) error {

	var planeStr string
	var originStr command.CoordStr
	filenames := cmd.SetDatatypeArgs(&planeStr, &originStr.string)
	x, y, z, err := originStr.GetCoord()
	if err != nil {
		return fmt.Errorf("Badly formatted origin (should be 'x,y,z'): %s", cmd)
	}

	fmt.Println("uuidNum:", uuidNum)
	fmt.Println("plane:", planeStr)
	fmt.Println("origin:", x, y, z)
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
			imagePacket := command.Packet{
				Text:   planeStr,
				Offset: [3]int{x, y, z},
				Size: [3]int{
					grayImage.Bounds().Max.X - grayImage.Bounds().Min.X,
					grayImage.Bounds().Max.Y - grayImage.Bounds().Min.Y,
					1,
				},
				Data: grayImage.Pix,
			}
			var addReply command.Packet
			err = datatype.Add(uuidNum, &imagePacket, &addReply)
			if err == nil {
				numSuccessful++
			} else {
				lastErr = err
			}
		}
		z += 1
	}
	if lastErr != nil {
		return fmt.Errorf("Error: %d of %d images successfully added [%s]\n",
			numSuccessful, len(filenames), lastErr.Error())
	}
	return nil
}

// Add stores a single image volume into the datastore.
func (datatype *Datatype) Add(uuidNum int, input, reply *command.Packet) error {

	fmt.Println("uuidNum:", uuidNum)
	fmt.Println("plane:", input.Text)
	fmt.Printf("origin: (%d, %d, %d)\n", input.Offset[0], input.Offset[1], input.Offset[2])
	fmt.Printf("  size: %d x %d x %d\n", input.Size[0], input.Size[1], input.Size[2])

	// Make sure we have buffer of blocks allocated to handle this set of images.
	//spindices := GetSpatialIndices()

	// When the buffer is filled, do a batch put.

	return nil
}
