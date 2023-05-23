//go:build !clustered && !gcloud
// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"runtime"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
)

// ServeGrayscale starts a HTTP server for grayscale data.
func ServeGrayscale(console, ports, grayscaleRef string) {
	tc.Server.Host = DefaultHost
	SetConsole(console)
	SetPorts(ports)

	dvid.TimeInfof("DVID code version: %s\n", gitVersion)
	dvid.TimeInfof("Serving HTTP grayscale data on %s (host alias %q)\n", tc.Server.HTTPAddress, tc.Server.Host)
	dvid.TimeInfof("Serving command-line use via RPC %s\n", tc.Server.RPCAddress)
	dvid.TimeInfof("Using web client files from %s\n", tc.Server.WebClient)
	dvid.TimeInfof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Launch the web server
	go serveHTTP()

	// Launch the rpc server
	go func() {
		if err := rpc.StartServer(tc.Server.RPCAddress); err != nil {
			dvid.Criticalf("Could not start RPC server: %v\n", err)
		}
	}()

	<-shutdownCh
}
