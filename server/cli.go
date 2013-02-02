package server

import (
	"dvid/datastore"
	_ "fmt"
	"log"
)

func DoCommand(command string, args []string, options Options) (err error) {
	switch command {
	case "init":
		log.Println("Initializing datastore at", options.Directory)
		config := datastore.NewConfig()
		config.Directory = options.Directory
		create := true
		err := datastore.Open(config, create)
		if err != nil {
			log.Fatalf("Error opening database (%s): %s\n", options.Directory, err)
		}
	}
	return
}
