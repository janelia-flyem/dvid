package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

// The default URL of the DVID web server
const DefaultWebAddress = "localhost:4000"

// The default RPC address of the DVID RPC server
const DefaultRpcAddress = "localhost:6000"

// The name of the server error log, stored in the datastore directory.
const ErrorLogFilename = "dvid-errors.log"

// runningService is a global variable that holds the currently running
// datastore service.  One DVID process can handle only one open DVID
// datastore.  (Leveldb is an embedded library with one process access.)
var runningService = Service{nil, DefaultWebAddress, DefaultRpcAddress}

// Service holds information on the servers attached to a DVID datastore.
type Service struct {
	// The currently opened DVID datastore
	*datastore.Service

	// The address of the web server
	WebAddress string

	// The address of the rpc server
	RpcAddress string
}

// Serve opens a datastore then creates both web and rpc servers for the datastore.
// This function must be called for DataService() to be non-nil.
func Serve(datastoreDir, webAddress, rpcAddress string) (err error) {

	// Make sure we don't already have an open datastore.
	if runningService.Service != nil {
		return fmt.Errorf("Cannot create new server.  " +
			"A DVID process can serve only one datastore.")
	}

	// Get exclusive ownership of a DVID datastore
	log.Println("Getting exclusive ownership of datastore at:", datastoreDir)

	runningService.Service, err = datastore.Open(datastoreDir)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Register an error logger that appends to a file in this datastore directory.
	errorLog := filepath.Join(datastoreDir, ErrorLogFilename)
	file, err := os.OpenFile(errorLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to open error logging file (%s): %s\n", errorLog, err.Error())
	}
	dvid.SetErrorLoggingFile(file)

	// Make sure we can support the datastore's types with our current DVID executable
	log.Println("Verifying datastore's supported types were compiled into DVID...")
	err = runningService.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Launch the web server
	go runningService.ServeHttp(webAddress)

	// Launch the rpc server
	err = runningService.ServeRpc(rpcAddress)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return nil
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion: 
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func (service *Service) ServeHttp(address string) {

	if address == "" {
		address = DefaultWebAddress
	}
	service.WebAddress = address
	dvid.Log(dvid.Debug, "Web server listening at %s ...\n", address)

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/api/", apiHandler)
	src.ListenAndServe()
}

// Listen and serve RPC requests using address.
func (service *Service) ServeRpc(address string) error {

	if address == "" {
		address = DefaultRpcAddress
	}
	service.RpcAddress = address
	dvid.Log(dvid.Debug, "Rpc server listening at %s ...\n", address)

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
