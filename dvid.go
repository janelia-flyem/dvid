package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/terminal"

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

	init <json config filename> [dir=/path/to/datastore/dir]
	serve [dir=/path/to/datastore/dir] [web=...] [rpc=...]

Commands that require connection to a running server:

	help
	branch 
	lock
	types
	log
	pull
	push

All of the commands above can include optional settings of the form:
	rpc=foo.com:1234  (Specifies the DVID server.)
	uuid=3efa87       (Specifies the image version within that datastore.)

There are also supported data type-specific commands that depend on 
the server configuration.  Use "dvid help" to get server-side help.

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

	if showHelp || showHelp2 {
		fmt.Println(helpMessage)
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
	} else if flag.NArg() == 0 {
		terminal.Shell()
	} else {
		command := &server.Command{Args: flag.Args()}
		if err := DoCommand(command); err != nil {
			fmt.Println(err.Error())
		}
	}
}

// DoCommand serves as a switchboard for commands, handling local ones and
// sending via rpc those commands that need a running server.
func DoCommand(cmd *server.Command) error {
	if len(cmd.Args) == 0 {
		return fmt.Errorf("Blank command!")
	}

	switch cmd.Args[0] {

	// Handle commands that don't require server connection
	case "init":
		return DoInit(cmd)
	case "serve":
		return DoServe(cmd)

	default:
		// Setup the server connection
		client, err := rpc.DialHTTP("tcp", cmd.GetRpcAddress())
		if err != nil {
			return fmt.Errorf("Could not establish rpc connection: %s", err.Error())
		}

		// Send command to server synchronously
		var reply datastore.CommandData
		err = client.Call("RpcConnection.Do", &cmd, &reply)
		if err != nil {
			return fmt.Errorf("RPC error for '%s': %s", cmd, err.Error())
		}
		fmt.Println(reply.Text)
	}
	return nil
}

// DoInit performs the "init" command, creating a new DVID datastore.
func DoInit(cmd *server.Command) error {

	if len(cmd.Args) != 1 {
		return fmt.Errorf("Poorly structured 'init' command: %s", cmd)
	}
	configFile := cmd.Args[0]
	config := datastore.ReadJsonConfig(configFile)
	datastoreDir := cmd.GetDatastoreDir()

	log.Println("Initializing datastore at", datastoreDir)
	create := true
	uuid := datastore.Init(datastoreDir, config, create)
	fmt.Println("Root node UUID:", uuid)
	// TODO -- This should be stored in datastore
	return nil
}

// DoServe opens a datastore then creates both web and rpc servers for the datastore
func DoServe(cmd *server.Command) error {

	webAddress := cmd.GetWebAddress()
	rpcAddress := cmd.GetRpcAddress()
	datastoreDir := cmd.GetDatastoreDir()

	if err := server.Serve(datastoreDir, webAddress, rpcAddress); err != nil {
		return err
	}
	return nil
}
