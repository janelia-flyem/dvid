package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
)

const DefaultWebAddress = "localhost:4000"
const DefaultRpcAddress = "localhost:6000"

// dataService holds the currently opened DVID datastore.  A DVID process can
// only handle one DVID datastore.
var dataService *datastore.Service

// DataService returns the currently opened DVID datastore service or nil if
// this DVID executable is not serving data.
func DataService() *datastore.Service {
	return dataService
}

// RpcConnection will export all of its functions for rpc access.
type RpcConnection struct{}

// Do acts as a switchboard for remote command execution
func (c *RpcConnection) Do(cmd *Command, reply *Param) error {
	if reply == nil {
		fmt.Println("reply is nil coming in!")
		return nil
	}
	if len(cmd.Args) == 0 {
		return fmt.Errorf("Server error: got empty command!")
	}
	if dataService == nil {
		return fmt.Errorf("Datastore not open!  Cannot execute command.")
	}

	switch cmd.Args[0] {
	// Handle builtin commands
	case "types":
		return cmd.types(reply)
	case "branch", "lock", "log", "pull", "push":
		reply.Text = fmt.Sprintf("Server would have processed '%s'", cmd)
	default:
		// Assume this is the command for a supported data type
		return cmd.datatypeDo(reply)
	}
	return nil
}

// Serve opens a datastore then creates both web and rpc servers for the datastore.
// This function must be called for DataService() to be non-nil.
func Serve(datastoreDir, webAddress, rpcAddress string) (err error) {

	// Make sure we don't already have an open datastore.
	if dataService != nil {
		return fmt.Errorf("Cannot create new server.  " +
			"A DVID process can serve only one datastore.")
	}

	// Get exclusive ownership of a DVID datastore
	log.Println("Getting exclusive ownership of datastore at:", datastoreDir)

	dataService, err = datastore.Open(datastoreDir)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Make sure we can support the datastore's types with our current DVID executable
	log.Println("Verifying datastore's supported types were compiled into DVID...")
	err = dataService.Config.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Launch the web server
	go ServeHttp(webAddress, dataService)

	// Launch the rpc server
	err = ServeRpc(rpcAddress, dataService)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return nil
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion: 
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func ServeHttp(address string, dataService *datastore.Service) {

	log.Printf("Web server listening at %s ...\n", address)

	if address == "" {
		address = DefaultWebAddress
	}
	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/api/", apiHandler)
	src.ListenAndServe()
}

// Listen and serve RPC requests using address.
func ServeRpc(address string, dataService *datastore.Service) error {

	log.Printf("Rpc server listening at %s ...\n", address)

	if address == "" {
		address = DefaultRpcAddress
	}
	c := new(RpcConnection)
	rpc.Register(c)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	http.Serve(listener, nil)
	return nil
}
