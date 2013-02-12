package cli

import (
	"fmt"
	"log"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype"
)

// DoInit performs the "init" command, creating a new DVID datastore at the
// current directory.
func DoInit(datastoreDir string, args []string) {
	if len(args) < 2 {
		log.Fatalln("init command must be followed with filename of configuration")
	}
	configFile := args[1]
	config := datastore.ReadJsonConfig(configFile)
	fmt.Println("config:", config)

	log.Println("Initializing datastore at", datastoreDir)
	create := true
	uuid := datastore.InitDatastore(datastoreDir, config, create)
	fmt.Println("Root node UUID:", uuid)
}

// DoAdd performs the "add" command, delegating the exact mechanism of add to a
// datatype's implementation.
func DoAdd(datastoreDir string, args []string) {
	if len(args) < 3 {
		log.Fatalln("add command must be followed by <datatype name> and <filenames glob>")
	}
	datatypeName := args[1]
	filenameGlob := args[2]

	// Get UUID
	var uuidString string
	if len(args) >= 4 {
		param := strings.Split(args[3], "=")
		if param[0] != "uuid" {
			log.Fatalf("Error: expected 'uuid=...' as 3rd argument to 'add' command, instead got %s\n",
				args[3])
		}
		uuidString = param[1]
	}
	if len(args) > 5 {
		log.Fatalln("Too many arguments to add command:", len(args)+1, "arguments")
	}

	// Get datatype-specific add params
	var params string
	if len(args) == 5 {
		params = args[4]
	}

	// Call the given datatype to process the data.
	service, err := datatype.GetService(datatypeName)
	if err != nil {
		log.Fatalln(err.Error())
	} else {
		fmt.Printf("\nPerforming 'add' command using datatype %s [%s]...\n\n",
			service.GetName(), service.GetUrl())
	}
	service.Add(datastoreDir, filenameGlob, uuidString, params)
	return
}

func DoCommand(datastoreDir string, args []string) (err error) {
	command := args[0]
	switch command {
	case "init":
		DoInit(datastoreDir, args)
	case "add":
		DoAdd(datastoreDir, args)
	default:
		fmt.Printf("\nSorry, the '%s' command is not supported.\n", command)
	}
	return
}
