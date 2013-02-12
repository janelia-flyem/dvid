package cli

import (
	"fmt"
	"log"

	"github.com/janelia-flyem/dvid/datastore"
)

// DoInit performs the "init" command, creating a new DVID datastore at the
// current directory.
func DoInit(datastoreDir string, args []string) (err error) {
	if len(args) < 2 {
		log.Fatalln("init command must be followed with filename of configuration")
	}
	configFile := args[1]
	config := datastore.ReadJsonConfig(configFile)
	fmt.Println("config:", config)

	log.Println("Initializing datastore at", datastoreDir)
	create := true
	datastore.InitDatastore(datastoreDir, config, create)
	return
}

func DoCommand(datastoreDir string, args []string) (err error) {
	command := args[0]
	switch command {
	case "init":
		err = DoInit(datastoreDir, args)
	default:
		fmt.Printf("\nSorry, the '%s' command is not supported.\n", command)
	}
	return
}
