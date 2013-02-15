package grayscale8

import (
	"fmt"
	_ "strconv"
	_ "strings"

	"github.com/janelia-flyem/dvid/command"
	"github.com/janelia-flyem/dvid/datastore"
)

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

const helpMessage = `
    Grayscale8 Data Type Commands:

        dvid grayscale8   add  <plane>  <origin>  <image filename glob>

    <plane>: xy, xz, or yz
    <origin>: 3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    <image filename glob>: filenames of images, e.g., foo-xy-*.png
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
func (datatype *Datatype) Do(uuid datastore.UUID, cmd *command.Command, 
	input *command.Packet, reply *command.Packet) error {

	switch cmd.TypeCommand() {
	case "add":
		return datatype.Add(uuid, cmd, input, reply)
	case "help":
		reply.Text = datatype.Help(helpMessage)
	default:
		return datatype.UnknownCommand(cmd)
	}
	return nil
}

// Add stores a series of 2d images either specified as an image filename glob or
// attached to the input packet.
func (datatype *Datatype) Add(uuid datastore.UUID, cmd *command.Command, 
	input *command.Packet, reply *command.Packet) error {

	var planeStr, originStr string
	filenames := cmd.SetDatatypeArgs(&planeStr, &originStr)

	fmt.Println("uuid:", uuid)
	fmt.Println("plane:", planeStr)
	fmt.Println("origin:", originStr)
	if len(filenames) >= 1 {
		fmt.Printf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}

	// Get the plane coordinates for images.

	// Load in the first image to determine the extent.

	// Make sure we have buffer of blocks allocated to handle this set of images.

	// Load each image, splitting the processing into block level goroutines.

	// When the buffer is filled, do a batch put.

	return nil
}
