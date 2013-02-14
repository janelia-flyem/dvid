package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
)

var showHelp bool
var showHelp2 bool
var showTypes bool

const helpMessage = `
dvid is a distributed, versioned image datastore

   usage: dvid [options] <command>

Commands that can be performed without a running server:

	init
	serve

Commands that require connection to a running server:

	branch
	lock
	types
	log
	pull
	push

` + datastore.SupportedTypeHelp() + `

For further information, launch the DVID server (enter "dvid serve"), then use
a web browser to visit the DVID web server ("localhost:4000" by default).
`

func init() {
	flag.BoolVar(&showHelp, "h", false, "Show help message")
	flag.BoolVar(&showHelp2, "help", false, "Show help message")
	flag.BoolVar(&showTypes, "types", false, "Show compiled DVID data types")
}

func main() {
	flag.Parse()

	if showTypes {
		fmt.Println(datastore.SupportedTypeChart())
	}

	if showHelp || showHelp2 || flag.NArg() == 0 {
		fmt.Println(helpMessage)
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
	} else {
		command := Command{flag.Args()}
		if err := command.Do(); err != nil {
			fmt.Println(err.Error())
		}
	}
}

type Command struct {
	server.Command
}

// DatastoreDir returns a directory specified in the arguments via "dir=..." or
// defaults to the current directory.
func (cmd Command) DatastoreDir() string {
	datastoreDir, found := cmd.GetParameter("dir")
	if !found {
		currentDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get current directory:", err)
		}
		return currentDir
	}
	return datastoreDir
}

// Do serves as a switchboard for commands and handles some common needs like
// converting a UUID string into a real UUID.
func (cmd Command) Do() error {
	if len(cmd.Args) == 0 {
		return fmt.Errorf("Blank command!")
	}

	switch cmd.Args[0] {

	// Handle commands that don't require server connection
	case "init":
		return command.Init()
	case "serve":
		return command.Serve()

	default:
		// Setup the server connection
		client, err := rpc.DialHTTP("tcp", rpcAddress)
		if err != nil {
			return fmt.Errorf("Could not establish rpc connection: %s", err)
		}

		// Send command to server
	}
	return nil
}

// Init performs the "init" command, creating a new DVID datastore.
func (cmd Command) Init() error {

	if len(cmd.Args) != 1 {
		return fmt.Errorf("Poorly structured 'init' command: %s", cmd)
	}
	configFile := cmd.Args[0]
	config := datastore.ReadJsonConfig(configFile)
	datastoreDir := cmd.DatastoreDir()

	log.Println("Initializing datastore at", datastoreDir)
	create := true
	uuid := datastore.Init(datastoreDir, config, create)
	fmt.Println("Root node UUID:", uuid)
	// TODO -- This should be stored in datastore
	return nil
}

// Serve opens a datastore then creates both web and rpc servers for the datastore
func (cmd Command) Serve(args []string) error {

	webAddress, _ := cmd.GetParameter("web")
	rpcAddress, _ := cmd.GetParameter("rpc")
	datastoreDir := cmd.DatastoreDir()

	if err := server.Serve(datastoreDir, webAddress, rpcAddress); err != nil {
		return err
	}
	return nil
}
