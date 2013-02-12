package cli

import (
	"fmt"
	"log"
	"os"

	"github.com/janelia-flyem/dvid/datastore"
)

// DoInit performs the "init" command, creating a new DVID datastore at the
// current directory.
func DoInit(args []string) (err error) {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	if len(args) < 2 {
		log.Fatalln("init command must be followed with filename of configuration")
	}
	configFile := args[1]
	config := datastore.ReadJsonConfig(configFile)
	fmt.Println("config:", config)

	log.Println("Initializing datastore at", currentDir)
	create := true
	datastore.InitDatastore(currentDir, config, create)
	return
}

func DoCommand(args []string) (err error) {
	command := args[0]
	switch command {
	case "init":
		err = DoInit(args)
	default:
		fmt.Printf("\nSorry, the '%s' command is not supported.\n", command)
	}
	return
}
