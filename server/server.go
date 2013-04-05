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

	"bitbucket.org/tebeka/nrsc"

	"code.google.com/p/go.net/websocket"
)

const (
	// The default URL of the DVID web server
	DefaultWebAddress = "localhost:4000"

	// The default RPC address of the DVID RPC server
	DefaultRpcAddress = "localhost:6000"

	// The relative URL path to our Level 2 REST API
	RestApiPath = "/api/"

	// The relative URL path to our WebSocket handler
	WebSocketPath = "/socket/"

	// The name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"
)

// runningService is a global variable that holds the currently running
// datastore service.  One DVID process can handle only one open DVID
// datastore.  (Leveldb is an embedded library with one process access.)
var runningService = Service{
	WebAddress: DefaultWebAddress,
	RpcAddress: DefaultRpcAddress,
}

// Timeout in seconds for waiting to open a datastore for exclusive access.
var TimeoutSecs int = 0

// Service holds information on the servers attached to a DVID datastore.
type Service struct {
	// The currently opened DVID datastore
	*datastore.Service

	// The address of the web server
	WebAddress string

	// The path to the DVID web client
	WebClientPath string

	// The address of the rpc server
	RpcAddress string
}

// Shutdown handles graceful cleanup of server functions before exiting DVID.
func Shutdown() {
	if runningService.Service != nil {
		datastore.Shutdown()
		runningService.Service.Close()
	}
}

// ServerlessDo runs a command locally, opening and closing a datastore
// as necessary.
func ServerlessDo(request datastore.Request, reply *datastore.Response) error {
	datastoreDir := request.DatastoreDir()

	// Make sure we don't already have an open datastore.
	if runningService.Service != nil {
		return fmt.Errorf("Cannot do concurrent requests on different datastores.")
	}

	// Get exclusive ownership of a DVID datastore.  Wait if allowed and necessary.
	dvid.Fmt(dvid.Debug, "Getting exclusive ownership of datastore at: %s\n", datastoreDir)
	startTime := time.Now()
	for {
		var err *datastore.OpenError
		runningService.Service, err = datastore.Open(datastoreDir)
		if err != nil {
			if TimeoutSecs == 0 || err.ErrorType != datastore.ErrorOpening {
				return err
			}
			dvid.Fmt(dvid.Debug, "Waiting a second for exclusive datastore access...\n")
			time.Sleep(1 * time.Second)
			elapsed := time.Since(startTime).Seconds()
			if elapsed > float64(TimeoutSecs) {
				return fmt.Errorf("Unable to obtain exclusive access of datastore (%s) in %d seconds",
					datastoreDir, TimeoutSecs)
			}
		} else {
			break
		}
	}
	defer Shutdown()

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

	// If it's not a builtin command, launch block handlers as goroutines for all data sets.
	switch request.Name() {
	case "help", "types", "version", "log", "dataset", "branch", "lock":
	default:
		for name, dtype := range runningService.DataSets() {
			datastore.ReserveBlockHandlers(name, dtype)
		}
	}

	// Issue local command
	var localConnect RpcConnection
	err = localConnect.Do(request, reply)
	if err != nil {
		return err
	}

	return nil
}

// Serve opens a datastore then creates both web and rpc servers for the datastore.
// This function must be called for DataService() to be non-nil.
func Serve(datastoreDir, webAddress, webClientDir, rpcAddress string) error {
	// Make sure we don't already have an open datastore.
	if runningService.Service != nil {
		return fmt.Errorf("Cannot create new server.  " +
			"A DVID process can serve only one datastore.")
	}

	// Get exclusive ownership of a DVID datastore
	log.Println("Getting exclusive ownership of datastore at:", datastoreDir)

	var openErr *datastore.OpenError
	runningService.Service, openErr = datastore.Open(datastoreDir)
	if openErr != nil {
		log.Fatalln(openErr.Error())
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

	// Launch block handlers as goroutines for all compiled types.
	for name, dtype := range runningService.DataSets() {
		datastore.ReserveBlockHandlers(name, dtype)
	}
	// Launch the web server
	go runningService.ServeHttp(webAddress, webClientDir)

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
func (service *Service) ServeHttp(address, clientDir string) {
	if address == "" {
		address = DefaultWebAddress
	}
	service.WebAddress = address
	service.WebClientPath = clientDir
	fmt.Printf("Web server listening at %s ...\n", address)
	fmt.Printf("WebSocket available at %s%s\n", address, WebSocketPath)

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	// Handle Level 2 REST API.
	http.HandleFunc(RestApiPath, apiHandler)

	// Handle Websocket API.
	http.Handle(WebSocketPath, websocket.Handler(socketHandler))

	// Handle everything else either through serving embedded files
	// via nrsc or loading files from a specified web client directory.
	if clientDir == "" {
		err := nrsc.Handle("/")
		if err != nil {
			fmt.Println("ERROR with nrsc trying to serve web pages:", err.Error())
			fmt.Println(webClientUnavailableMessage)
			fmt.Println("HTTP server will be started without webclient...\n")
			http.HandleFunc("/", mainHandler)
		} else {
			dvid.Log(dvid.Debug, "Serving built-in DVID web client...\n")
		}
	} else {
		http.HandleFunc("/", mainHandler)
		dvid.Log(dvid.Debug, "Serving web pages from %s\n", clientDir)
	}

	// Serve it up!
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
