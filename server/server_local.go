// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"net"
	"net/http"
	"net/rpc"
	"runtime"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage/local"
)

const (
	// The default URL of the DVID web server
	DefaultWebAddress = "localhost:8000"

	// The default RPC address of the DVID RPC server
	DefaultRPCAddress = "localhost:8001"

	// The name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"

	// Maximum number of throttled ops we can handle through API
	MaxThrottledOps = 1
)

// Local server configuration parameters.
type configT struct {
	dbPath, webAddress, webClientDir, rpcAddress string
}

var config configT

// Serve calls platform-specific initialization and sets server.Repos, a package variable
// that holds the currently running engines and context.
func Serve(dbPath, webAddress, webClientDir, rpcAddress string, c dvid.Config) error {
	var err error
	config = configT{dbPath, webAddress, webClientDir, rpcAddress}

	// Setup local storage engine
	if err = local.Initialize(dbPath, c); err != nil {
		return err
	}

	// Initialize datastore and set repo management as global var in package server
	Repos, err = datastore.Initialize()
	if err != nil {
		return err
	}

	dvid.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Launch the web server
	go serveHttp(webAddress, webClientDir)

	// Launch the rpc server
	if err := serveRpc(rpcAddress); err != nil {
		dvid.Criticalf("Could not start RPC server: %s\n", err.Error())
	}

	return nil
}

// Listen and serve RPC requests using address.
func serveRpc(address string) error {
	dvid.Infof("Rpc server listening at %s ...\n", address)

	c := new(RPCConnection)
	rpc.Register(c)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	http.Serve(listener, nil)
	return nil
}
