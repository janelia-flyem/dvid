/*
	This file contains code useful for arbitrary data types supported in DVID.
*/

package datastore

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// This message is used for all data types to explain options.
const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 
`

type UrlString string

// TypeID provides methods for determining the identity of a data type.
type TypeID interface {
	// TypeName describes a data type and may not be unique.
	DatatypeName() string

	// TypeUrl returns the unique package name that fulfills the DVID Data interface
	DatatypeUrl() UrlString

	// TypeVersion describes the version identifier of this data type code
	DatatypeVersion() string
}

// TypeService is an interface for operations using arbitrary data types.
type TypeService interface {
	TypeID

	// Help returns a string explaining how to use a data type's service
	Help() string

	// Create Data that is an instance of this data type
	NewData(id DatasetID, config dvid.Config) DatasetService
}

// CompiledTypes is the set of registered data types compiled into DVID and
// held as a global variable initialized at runtime.
var CompiledTypes = map[UrlString]TypeService{}

// CompiledTypeNames returns a list of data type names compiled into this DVID.
func CompiledTypeNames() string {
	var names []string
	for _, datatype := range CompiledTypes {
		names = append(names, datatype.DatatypeName())
	}
	return strings.Join(names, ", ")
}

// CompiledTypeUrls returns a list of data type urls supported by this DVID.
func CompiledTypeUrls() string {
	var urls []string
	for url, _ := range CompiledTypes {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// CompiledTypeChart returns a chart (names/urls) of data types compiled into this DVID.
func CompiledTypeChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range CompiledTypes {
		writeLine(datatype.DatatypeName(), datatype.DatatypeUrl())
	}
	return text + "\n"
}

// RegisterDatatype registers a data type for DVID use.
func RegisterDatatype(t TypeService) {
	if CompiledTypes == nil {
		CompiledTypes = make(map[UrlString]TypeService)
	}
	CompiledTypes[t.DatatypeUrl()] = t
}

// DatatypeID uniquely identifies a DVID-supported data type and provides a
// shorthand name.
type DatatypeID struct {
	// Data type name and may not be unique.
	Name string

	// The unique package name that fulfills the DVID Data interface
	Url UrlString

	// The version identifier of this data type code
	Version string
}

func MakeDatatypeID(name string, url UrlString, version string) DatatypeID {
	return DatatypeID{name, url, version}
}

func (id *DatatypeID) DatatypeName() string { return id.Name }

func (id *DatatypeID) DatatypeUrl() UrlString { return id.Url }

func (id *DatatypeID) DatatypeVersion() string { return id.Version }

// Datatype is the base struct that satisfies a TypeService and can be embedded
// in other data types.
type Datatype struct {
	DatatypeID

	// A list of interface requirements for the backend datastore
	Requirements storage.Requirements
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

// Types must add a NewData() function...

func (datatype *Datatype) Help() string {
	return fmt.Sprintf(helpMessage, datatype.Name, datatype.Url)
}

// Request supports requests to the DVID server.  Since input and reply payloads
// are different depending on the command and the data type, we use an ArbitraryInput
// (empty interface) for the payload.
type Request struct {
	dvid.Command
	Input ArbitraryInput
}

type ArbitraryInput interface{}

// Response supports responses from DVID.
type Response struct {
	dvid.Response
	Output ArbitraryOutput
}

type ArbitraryOutput interface{}

// DataString is a string that is the name of a DVID data set.
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data name.
type DataString string

// DataService is an interface for operations on arbitrary datas that
// use a supported TypeService.  Block handlers can be allocated at this level,
// so an implementation can own a number of goroutines.
//
// DataService operations are completely type-specific, and each datatype
// handles operations through RPC (DoRPC) and HTTP (DoHTTP).
// TODO -- Add SPDY as wrapper to HTTP.
type DataService interface {
	TypeService

	// DataName returns the name of the data (e.g., grayscale data that is grayscale8 data type).
	DataName() DataString

	// DataLocalID returns a DVID instance-specific id for this data,
	// which can be held in a relatively small number of bytes and is useful
	// as a key component.
	DataLocalID() dvid.LocalID

	// ChunkHandler processes a chunk of data in a mapped operation.
	ChunkHandler(op *ChunkOp)

	// Handle iteration through a data in abstract way.
	NewIndexIterator(extents interface{}) IndexIterator

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request) error

	// Returns standard error response for unknown commands
	UnknownCommand(r Request) error
}

// DataID identifies data within a DVID server.
type DataID struct {
	Name    DataString
	LocalID dvid.LocalID
}

func (id DataID) DataName() DataString { return id.Name }

func (id DataID) DataLocalID() dvid.LocalID { return id.LocalID }

// Data is an instance of a data type and has an associated datastore.
type Data struct {
	DataID

	// Underlying implementation of this data is a Datatype
	Datatype TypeService
}

func NewData(id DataID, t TypeService) *Data {
	return &Data{id, t}
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.Name, d.Datatype.DatatypeName(), request.TypeCommand())
}

// Forward the TypeService interface calls to the embedded Datatype.  Datatype
// is not simply embedded because of potential conflicts in Name, etc, between
// Data and Datatype.  Prefer finer control.

func (d *Data) DatatypeName() string { return d.Datatype.DatatypeName() }

func (d *Data) DatatypeUrl() UrlString { return d.Datatype.DatatypeUrl() }

func (d *Data) DatatypeVersion() string { return d.Datatype.DatatypeVersion() }

func (d *Data) Help() string { return d.Datatype.Help() }

func (d *Data) NewData(id DataID, config dvid.Config) DataService {
	return d.Datatype.NewData(id, config)
}

// Constants that allow tuning of DVID for a particular target computer.
const (
	// Default number of block handlers to use per data.
	DefaultNumChunkHandlers = 8

	// Number of chunk write requests that can be buffered on each block handler
	// before sender is blocked.
	ChunkHandlerBufferSize = 100000
)

// Each data has a pool of channels to communicate with gouroutine handlers.
type ChunkChannels map[DataString]([]chan *ChunkOp)

// Track requested/completed block ops
type loadStruct struct {
	Requests  int
	Completed int
}
type loadMap map[DataString]loadStruct

var (
	chunkChannels ChunkChannels

	// Monitor the requested and completed block ops
	loadLastSec    loadMap
	loadAccess     sync.RWMutex
	doneChannel    chan DataString
	requestChannel chan DataString
)

func init() {
	chunkChannels = make(ChunkChannels)
	loadLastSec = make(loadMap)
	doneChannel = make(chan DataString)
	requestChannel = make(chan DataString)
	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	requests := make(map[DataString]int)
	completed := make(map[DataString]int)
	for {
		select {
		case name := <-doneChannel:
			completed[name]++
		case name := <-requestChannel:
			requests[name]++
		case <-secondTick:
			loadAccess.RLock()
			for name, _ := range loadLastSec {
				loadLastSec[name] = loadStruct{
					Requests:  requests[name],
					Completed: completed[name],
				}
				requests[name] = 0
				completed[name] = 0
			}
			loadAccess.RUnlock()
		}
	}
}

// StartChunkHandlers starts goroutines that handle chunks for every data in
// the service.
func (s *Service) StartChunkHandlers() {
	for _, data := range s.Datas {
		StartDataChunkHandlers(data)
	}
}

// StartDataChunkHandlers makes sure we have chunk handler goroutines for each
// data set.  Chunks are routed to the same handler each time, so concurrent
// access to a chunk by multiple requests are funneled sequentially into a
// channel reserved for that chunk's handler.
func StartDataChunkHandlers(data DataService) {
	dataName := data.DataName()

	loadAccess.Lock()
	loadLastSec[dataName] = loadStruct{}
	loadAccess.Unlock()

	var channelMapAccess sync.Mutex
	channelMapAccess.Lock()
	// Do we have channels and handlers for this type and image version?
	_, found := chunkChannels[dataName]
	if found {
		log.Printf("Already have chunk handlers for data set '%s' (%s)\n",
			dataName, data.DatatypeName())
	} else {
		log.Printf("Starting %d block handlers for data set '%s' (%s)...\n",
			data.NumChunkHandlers(), dataName, data.DatatypeName())
		channels := make([]chan *ChunkOp, 0, data.NumChunkHandlers())
		for i := 0; i < data.NumChunkHandlers(); i++ {
			channel := make(chan *ChunkOp, ChunkHandlerBufferSize)
			channels = append(channels, channel)
			go func(i int, c chan *ChunkOp) {
				dvid.Log(dvid.Debug, "Starting chunk handler %d for %s...",
					i+1, dataName)
				for {
					chunkOp := <-c
					if chunkOp == nil {
						log.Fatalln("Received nil chunk in chunk handler!")
					}
					if chunkOp.shutdown {
						dvid.Log(dvid.Debug, "Shutting down chunk handler %d for %s...",
							i+1, dataName)
						break
					}
					//dvid.Fmt(dvid.Debug, "Running handler on block %x...\n", block.IndexKey)
					data.ChunkHandler(chunkOp)
					doneChannel <- dataName
				}
			}(i, channel)
			// TODO -- keep stats on # of handlers
		}
		chunkChannels[dataName] = channels
	}
	channelMapAccess.Unlock()
}

func stopChunkHandlers(data DataService) {
	// Remove load stat cache
	loadAccess.Lock()
	delete(loadLastSec, data.DataName())
	loadAccess.Unlock()

	// Send shutdown op to all handlers for this data
	var haltOp ChunkOp
	haltOp.shutdown = true
	for _, c := range chunkChannels[data.DataName()] {
		c <- &haltOp
	}
}

// ChunkLoadJSON returns a JSON description of the chunk op requests for each data.
func ChunkLoadJSON() (jsonStr string, err error) {
	loadAccess.RLock()
	m, err := json.Marshal(loadLastSec)
	loadAccess.RUnlock()
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}
