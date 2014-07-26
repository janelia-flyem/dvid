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
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage/local"

	"github.com/zenazn/goji"
)

const (
	// The default RPC address of the DVID RPC server
	DefaultRPCAddress = "localhost:8001"

	// The name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"

	// Maximum number of throttled ops we can handle through API
	MaxThrottledOps = 1
)

type Context struct {
	dbPath, webAddress, webClientDir, rpcAddress string
	logger                                       Logger
}

var context *Context

// Serve calls platform-specific initialization and sets server.Repos, a package variable
// that holds the currently running engines and context.
func Serve(dbPath, webAddress, webClientDir, rpcAddress, logfile string, c dvid.Config) error {
	// Initialize logger
	logger := dvid.newLogger(logfile)

	// Set package-level context
	context = &Context{dbPath, webAddress, webClientDir, rpcAddress, logger}

	// Setup storage engine
	if err := SetupEngines(dbPath, c); err != nil {
		return err
	}

	// Setup storage tiers
	SetupTiers()

	// Initialize datastore and set repo management as global var in package server
	ctx := datastore.NewContext(logger, metadata, smalldata, bigdata)
	var err error
	Repos, err = datastore.Initialize(ctx)
	if err != nil {
		return err
	}

	logger.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Launch the web server
	go serveHttp(webAddress, webClientDir)

	// Launch the rpc server
	if err := serveRpc(rpcAddress); err != nil {
		logger.Criticalf("Could not start RPC server: %s\n", err.Error())
	}

	return nil
}

// ---- Handle Storage Setup

func SetupEngines(path string, config dvid.Config) error {
	var err error
	var ok bool

	create := true
	Engines.kvEngine, err = local.NewKeyValueStore(path, create, config)
	if err != nil {
		return err
	}
	Engines.kvDB, ok = Engines.kvEngine.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value database", path)
	}
	Engines.kvSetter, ok = Engines.kvEngine.(OrderedKeyValueSetter)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value setter", path)
	}
	Engines.kvGetter, ok = Engines.kvEngine.(OrderedKeyValueGetter)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value getter", path)
	}

	Engines.graphEngine, err = NewGraphStore(path, create, config, Engines.kvDB)
	if err != nil {
		return err
	}
	Engines.graphDB, ok = Engines.graphEngine.(GraphDB)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph database", path)
	}
	Engines.graphSetter, ok = Engines.graphEngine.(GraphSetter)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph setter", path)
	}
	Engines.graphGetter, ok = Engines.graphEngine.(GraphGetter)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph getter", path)
	}

	Engines.setup = true
	return nil
}

// --- Implement the three tiers of storage.
// --- In the case of a single local server with embedded storage engines, it's simpler
// --- because we don't worry about cross-process synchronization.

func SetupTiers() {
	MetaData = metaData{Engines.kvDB}
	SmallData = smallData{Engines.kvDB}
	BigData = bigData{Engines.kvDB}
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion:
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func serveHttp(address, clientDir string) {
	context.logger.Infof("Web server listening at %s ...\n", address)

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	initRoutes(clientDir)
	goji.Serve()
}

// Listen and serve RPC requests using address.
func serveRpc(address string) error {
	context.logger.Infof("Rpc server listening at %s ...\n", address)

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
