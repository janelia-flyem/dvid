package grayscale8

import (
	_ "fmt"
	_ "strconv"
	_ "strings"

	"github.com/janelia-flyem/dvid/datastore"
)

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

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
func (datatype *Datatype) Do(uuid datastore.UUID, command string, args []string) error {
	switch command {
	case "add":
		return datatype.Add(args)
	default:
		return datatype.UnknownCommand(command)
	}
	return nil
}

// Add stores a series of 2d images
func (datatype *Datatype) Add(args []string) error {
	/*
		sliceType := args[0]
		origin := args[1]
		filenameGlob := args[2]

		// datastoreDir, filenameGlob, uuidString, params string
		badParams := func() error {
			return fmt.Errorf("Expected 'add' to have image coordinate like 'z=10'"+
				" or 'x=23', instead got '%s'", params)
		}
		// Get the plane coordinates for images.
		elems := strings.Split(params, "=")
		if len(elems) != 2 {
			return badParams()
		}
		axis := elems[0]
		var z int
		switch axis {
		case "z":
			var err error
			z, err = strconv.Atoi(elems[1])
			if err != nil {
				return fmt.Errorf("Could not parse z coordinate: %s", err.Error())
			}
		case "x", "y":
			return fmt.Errorf("Sorry, xz and yz slices haven't been added to 'add' yet!")
		default:
			return badParams()
		}

		// Load in the first image to determine the extent.

		// Make sure we have buffer of blocks allocated to handle this set of images.

		// Load each image, splitting the processing into block level goroutines.

		// When the buffer is filled, do a batch put.

		fmt.Println("Datastore Dir: ", datastoreDir)
		fmt.Println("filename Glob: ", filenameGlob)
		fmt.Println("uuid string: ", uuidString)
		fmt.Println("params: ", params)
	*/
	return nil
}
