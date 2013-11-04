package server

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/nrsc"
)

const (
	// The default URL of the DVID web server
	DefaultWebAddress = "localhost:8000"

	// The default RPC address of the DVID RPC server
	DefaultRPCAddress = "localhost:8001"

	// The relative URL path to our Level 2 REST API
	WebAPIPath = "/api/"

	// The relative URL path to a DVID web console.
	// The root URL will be redirected to /{ConsolePath}/index.html
	ConsolePath = "/console/"

	// The name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"
)

var (
	// runningService is a global variable that holds the currently running
	// datastore service.
	runningService = Service{
		WebAddress: DefaultWebAddress,
		RPCAddress: DefaultRPCAddress,
	}

	// ActiveHandlers is maximum number of active handlers over last second.
	ActiveHandlers int

	// Running tally of active handlers up to the last second
	curActiveHandlers int

	// MaxChunkHandlers sets the maximum number of chunk handlers (goroutines) that
	// can be multiplexed onto available cores.  (See -numcpu setting in dvid.go)
	MaxChunkHandlers = runtime.NumCPU()

	// HandlerToken is buffered channel to limit spawning of goroutines.
	// See ProcessChunk() in datatype/voxels for example.
	HandlerToken = make(chan int, MaxChunkHandlers)

	// Timeout in seconds for waiting to open a datastore for exclusive access.
	TimeoutSecs int

	// GzipAPI turns on gzip compression on REST API responses.
	// For high bandwidth networks or local use, it is better to leave gzip
	// off because delay due to compression is frequently higher than gains
	// from decreased response size.
	GzipAPI = false
)

// Service holds information on the servers attached to a DVID datastore.  If more than
// one storage engine is used by a DVID server, e.g., polyglot persistence where graphs
// are managed by a graph database and key-value by a key-value database, this would
// be the level at which the storage engines are integrated.
type Service struct {
	// The currently opened DVID datastore
	*datastore.Service

	// Error log directory
	ErrorLogDir string

	// The address of the web server
	WebAddress string

	// The path to the DVID web client
	WebClientPath string

	// The address of the rpc server
	RPCAddress string
}

func init() {
	// Initialize the number of handler tokens available.
	for i := 0; i < MaxChunkHandlers; i++ {
		HandlerToken <- 1
	}

	// Monitor the handler token load, resetting every second.
	loadCheckTimer := time.Tick(10 * time.Millisecond)
	ticks := 0
	go func() {
		for {
			<-loadCheckTimer
			ticks = (ticks + 1) % 100
			if ticks == 0 {
				ActiveHandlers = curActiveHandlers
				curActiveHandlers = 0
			}
			numHandlers := MaxChunkHandlers - len(HandlerToken)
			if numHandlers > curActiveHandlers {
				curActiveHandlers = numHandlers
			}
		}
	}()
}

// DatastoreService returns the current datastore service.  One DVID process
// is assigned to one datastore service, although it may be possible to have
// multiple (polyglot) persistence backends attached to that one service.
func DatastoreService() *datastore.Service {
	return runningService.Service
}

// ServerAddress returns the server location and port.
func ServerAddress() string {
	return runningService.WebAddress
}

// MatchingUUID returns a UUID on this server that uniquely matches a uuid string.
func MatchingUUID(uuidStr string) (uuid datastore.UUID, err error) {
	if runningService.Service == nil {
		err = fmt.Errorf("Datastore service has not been started on this server.")
		return
	}
	uuid, _, _, err = runningService.Service.NodeIDFromString(uuidStr)
	return
}

// VersionLocalID returns a server-specific local ID for the node with the given UUID.
func VersionLocalID(uuid datastore.UUID) (datastore.VersionLocalID, error) {
	if runningService.Service == nil {
		return 0, fmt.Errorf("Datastore service has not been started on this server.")
	}
	_, versionID, err := runningService.Service.LocalIDFromUUID(uuid)
	if err != nil {
		return 0, err
	}
	return versionID, nil
}

// StorageEngine returns the default storage engine or nil if it's not available.
func StorageEngine() storage.Engine {
	if runningService.Service == nil {
		return nil
	}
	return runningService.StorageEngine()
}

// Shutdown handles graceful cleanup of server functions before exiting DVID.
// This may not be so graceful if the chunk handler uses cgo since the interrupt
// may be caught during cgo execution.
func Shutdown() {
	if runningService.Service != nil {
		runningService.Service.Shutdown()
	}
	for {
		active := MaxChunkHandlers - len(HandlerToken)
		if active > 0 {
			log.Printf("Waiting for %d chunk handlers to finish...\n", active)
		} else {
			log.Println("No chunk handlers active...")
			break
		}
		time.Sleep(1 * time.Second)
	}
	dvid.BlockOnActiveCgo()
}

// ServerlessDo runs a command locally, opening and closing a datastore
// as necessary.
func ServerlessDo(datastoreDir string, request datastore.Request, reply *datastore.Response) error {
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

	// Issue local command
	var localConnect RPCConnection
	err = localConnect.Do(request, reply)
	if err != nil {
		return err
	}

	return nil
}

// OpenDatastore returns a Server service.  Only one datastore can be opened
// for any server.
func OpenDatastore(datastoreDir string) (service *Service, err error) {
	// Make sure we don't already have an open datastore.
	if runningService.Service != nil {
		err = fmt.Errorf("Cannot create new server. A DVID process can serve only one datastore.")
		return
	}

	// Get exclusive ownership of a DVID datastore
	log.Println("Getting exclusive ownership of datastore at:", datastoreDir)

	var openErr *datastore.OpenError
	runningService.Service, openErr = datastore.Open(datastoreDir)
	if openErr != nil {
		err = openErr
		return
	}
	runningService.ErrorLogDir = datastoreDir

	service = &runningService
	return
}

// Serve opens a datastore then creates both web and rpc servers for the datastore.
// This function must be called for DatastoreService() to be non-nil.
func (service *Service) Serve(webAddress, webClientDir, rpcAddress string) error {
	log.Printf("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Register an error logger that appends to a file in this datastore directory.
	errorLog := filepath.Join(service.ErrorLogDir, ErrorLogFilename)
	file, err := os.OpenFile(errorLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to open error logging file (%s): %s\n", errorLog, err.Error())
	}
	dvid.SetErrorLoggingFile(file)

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

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	// Handle Level 2 REST API.
	if GzipAPI {
		fmt.Println("HTTP server will return gzip values if permitted by browser.")
		http.HandleFunc(WebAPIPath, makeGzipHandler(apiHandler))
	} else {
		http.HandleFunc(WebAPIPath, apiHandler)
	}

	// Handle static files through serving embedded files
	// via nrsc or loading files from a specified web client directory.
	if clientDir == "" {
		err := nrsc.Handle(ConsolePath)
		if err != nil {
			fmt.Println("ERROR with nrsc trying to serve web pages:", err.Error())
			fmt.Println(webClientUnavailableMessage)
			fmt.Println("HTTP server will be started without webclient...\n")
			http.HandleFunc(ConsolePath, mainHandler)
		} else {
			fmt.Println("Serving web client from embedded files...")
		}
	} else {
		http.HandleFunc(ConsolePath, mainHandler)
		dvid.Log(dvid.Debug, "Serving web pages from %s\n", clientDir)
	}

	// Manage redirection from / to the ConsolePath.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var urlStr string
		if r.URL.Path == "/" {
			urlStr = ConsolePath + "index.html"
		} else {
			urlStr = ConsolePath + strings.TrimLeft(r.URL.Path, "/")
		}
		dvid.Fmt(dvid.Debug, "Redirect %s -> %s\n", r.URL.Path, urlStr)
		http.Redirect(w, r, urlStr, http.StatusMovedPermanently)
	})

	// Serve it up!
	src.ListenAndServe()
}

// Listen and serve RPC requests using address.
func (service *Service) ServeRpc(address string) error {
	if address == "" {
		address = DefaultRPCAddress
	}
	service.RPCAddress = address
	dvid.Log(dvid.Debug, "Rpc server listening at %s ...\n", address)

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

// Nod to Andrew Gerrand for simple gzip solution:
// See https://groups.google.com/forum/m/?fromgroups#!topic/golang-nuts/eVnTcMwNVjM
type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func makeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		dvid.Log(dvid.Debug, "Responding to request with gzip\n")
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fn(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	}
}
