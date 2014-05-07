package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
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

	// WebAPIVersion is the string version of the API.  Once DVID is somewhat stable,
	// this will be "v1/", "v2/", etc.
	WebAPIVersion = ""

	// The relative URL path to our Level 2 REST API
	WebAPIPath = "/api/" + WebAPIVersion

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

	// SpawnGoroutineMutex is a global lock for compute-intense processes that want to
	// spawn goroutines that consume handler tokens.  This lets processes capture most
	// if not all available handler tokens in a FIFO basis rather than have multiple
	// concurrent requests launch a few goroutines each.
	SpawnGoroutineMutex sync.Mutex

	// Timeout in seconds for waiting to open a datastore for exclusive access.
	TimeoutSecs int

	// Keep track of the startup time for uptime.
	startupTime time.Time = time.Now()
)

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

// MatchingUUID returns a UUID on this server that uniquely matches a uuid string.
func MatchingUUID(uuidStr string) (uuid dvid.UUID, err error) {
	if runningService.Service == nil {
		err = fmt.Errorf("Datastore service has not been started on this server.")
		return
	}
	uuid, _, _, err = runningService.Service.NodeIDFromString(uuidStr)
	return
}

// VersionLocalID returns a server-specific local ID for the node with the given UUID.
func VersionLocalID(uuid dvid.UUID) (dvid.VersionLocalID, error) {
	if runningService.Service == nil {
		return 0, fmt.Errorf("Datastore service has not been started on this server.")
	}
	_, versionID, err := runningService.Service.LocalIDFromUUID(uuid)
	if err != nil {
		return 0, err
	}
	return versionID, nil
}

// --- Return datastore.Service and various database interfaces to support polyglot persistence --

// DatastoreService returns the current datastore service.  One DVID process
// is assigned to one datastore service, although it may be possible to have
// multiple (polyglot) persistence backends attached to that one service.
func DatastoreService() *datastore.Service {
	return runningService.Service
}

// OrderedKeyValueDB returns the default key-value database
func OrderedKeyValueDB() (storage.OrderedKeyValueDB, error) {
	if runningService.Service == nil {
		return nil, fmt.Errorf("No running datastore service is available.")
	}
	return runningService.OrderedKeyValueDB()
}

// OrderedKeyValueGetter returns the default service for retrieving key-value pairs.
func OrderedKeyValueGetter() (storage.OrderedKeyValueGetter, error) {
	if runningService.Service == nil {
		return nil, fmt.Errorf("No running datastore service is available.")
	}
	return runningService.OrderedKeyValueGetter()
}

// GraphDB returns the default service for handling grah operations.
func GraphDB() (storage.GraphDB, error) {
	if runningService.Service == nil {
		return nil, fmt.Errorf("No running datastore service is available.")
	}
	return runningService.GraphDB()
}

// OrderedKeyValueSetter returns the default service for storing key-value pairs.
func OrderedKeyValueSetter() (storage.OrderedKeyValueSetter, error) {
	if runningService.Service == nil {
		return nil, fmt.Errorf("No running datastore service is available.")
	}
	return runningService.OrderedKeyValueSetter()
}

// StorageEngine returns the default storage engine or nil if it's not available.
func StorageEngine() (storage.Engine, error) {
	if runningService.Service == nil {
		return nil, fmt.Errorf("No running datastore service is available.")
	}
	return runningService.StorageEngine(), nil
}

// Shutdown handles graceful cleanup of server functions before exiting DVID.
// This may not be so graceful if the chunk handler uses cgo since the interrupt
// may be caught during cgo execution.
func Shutdown() {
	if runningService.Service != nil {
		runningService.Service.Shutdown()
	}
	waits := 0
	for {
		active := MaxChunkHandlers - len(HandlerToken)
		if waits >= 20 {
			log.Printf("Already waited for 20 seconds.  Continuing with shutdown...")
			break
		} else if active > 0 {
			log.Printf("Waiting for %d chunk handlers to finish...\n", active)
			waits++
		} else {
			log.Println("No chunk handlers active...")
			break
		}
		time.Sleep(1 * time.Second)
	}
	storage.Shutdown()
	dvid.BlockOnActiveCgo()
}

// OpenDatastore returns a Server service.  Only one datastore can be opened
// for any server.
func OpenDatastore(datastorePath string) (service *Service, err error) {
	// Make sure we don't already have an open datastore.
	if runningService.Service != nil {
		err = fmt.Errorf("Cannot create new server. A DVID process can serve only one datastore.")
		return
	}

	// Get exclusive ownership of a DVID datastore
	log.Println("Getting exclusive ownership of datastore at:", datastorePath)

	var openErr *datastore.OpenError
	runningService.Service, openErr = datastore.Open(datastorePath)
	if openErr != nil {
		err = openErr
		return
	}
	runningService.ErrorLogDir = filepath.Dir(datastorePath)

	service = &runningService
	return
}

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

func (service *Service) sendContent(path string, w http.ResponseWriter, r *http.Request) {
	if len(path) > 0 && path[len(path)-1:] == "/" {
		path = filepath.Join(path, "index.html")
	}
	if service.WebClientPath == "" {
		if len(path) > 0 && path[0:1] == "/" {
			path = path[1:]
		}
		dvid.Log(dvid.Debug, "[%s] Serving from embedded files: %s\n", r.Method, path)

		resource := nrsc.Get(path)
		if resource == nil {
			http.NotFound(w, r)
			return
		}
		rsrc, err := resource.Open()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		data, err := ioutil.ReadAll(rsrc)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		dvid.SendHTTP(w, r, path, data)
	} else {
		// Use a non-embedded directory of files.
		filename := filepath.Join(runningService.WebClientPath, path)
		dvid.Log(dvid.Debug, "[%s] Serving from webclient directory: %s\n", r.Method, filename)
		http.ServeFile(w, r, filename)
	}
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

// Wrapper function so that http handlers recover from panics gracefully
// without crashing the entire program.  The error message is written to
// the log.
func logHttpPanics(handler func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Caught panic on HTTP request: %s", err)
				log.Printf("IP: %v, URL: %s", request.RemoteAddr, request.URL.Path)
				log.Printf("Stack Dump:\n%s", debug.Stack())
			}
		}()
		handler(writer, request)
	}
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

	// Handle RAML interface
	http.HandleFunc("/interface/raw", logHttpPanics(service.interfaceHandler))
	http.HandleFunc("/interface/version", logHttpPanics(versionHandler))
	http.HandleFunc("/interface", logHttpPanics(service.apiHelpHandler))

	// Handle Level 2 REST API.
	http.HandleFunc(WebAPIPath, logHttpPanics(apiHandler))

	// Handle static files through serving embedded files
	// via nrsc or loading files from a specified web client directory.
	if clientDir == "" {
		dvid.Log(dvid.Normal, "Serving web client from embedded files...")
		if err := nrsc.Initialize(); err != nil {
			dvid.Log(dvid.Normal, "Error initializing embedded data access: %s\n", err.Error())
		}
	} else {
		dvid.Log(dvid.Normal, "Serving web pages from %s\n", clientDir)
	}
	http.HandleFunc("/", logHttpPanics(service.mainHandler))

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
