// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"runtime"

	"github.com/janelia-flyem/dvid/dvid"
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

type configT struct {
	httpAddress, rpcAddress, webClientDir string
}

func (c configT) HTTPAddress() string {
	return c.httpAddress
}

func (c configT) RPCAddress() string {
	return c.rpcAddress
}

func (c configT) WebClient() string {
	return c.webClientDir
}

// Serve starts HTTP and RPC servers.
func Serve(httpAddress, webClientDir, rpcAddress string) error {
	// Set the package-level config variable
	dvid.Infof("Serving HTTP on %s\n", httpAddress)
	dvid.Infof("Serving RPC  on %s\n", rpcAddress)
	dvid.Infof("Using web client files from %s\n", webClientDir)
	dvid.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())
	config = configT{httpAddress, rpcAddress, webClientDir}

	// Launch the web server
	go serveHttp(httpAddress, webClientDir)

	// Launch the rpc server
	if err := serveRpc(rpcAddress); err != nil {
		return fmt.Errorf("Could not start RPC server: %s\n", err.Error())
	}
	return nil
}

// Listen and serve RPC requests using address.
func serveRpc(address string) error {
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
