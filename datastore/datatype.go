/*
	This file contains code useful for arbitrary data types supported in DVID.
*/

package datastore

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"

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
	NewData(id DataID, config dvid.Config) DataService
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

// DataString is a string that is the name of DVID data.
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data name.
type DataString string

// DataService is an interface for operations on arbitrary data that
// use a supported TypeService.  Chunk handlers are allocated at this level,
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

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request) error

	// Returns standard error response for unknown commands
	UnknownCommand(r Request) error
}

// ChunkHandlers have a set of channels on which the storage engine can send them
// chunks of data for processing.  They are goroutines assigned to each data, and
// each chunk location is always sent to the same handler via a hash of its Index.
type ChunkHandler interface {
	ChannelSpecs() (number, bufferSize int)

	StartChunkHandlers()

	StopChunkHandlers()

	ChunkChannels() []storage.ChunkChannel

	ProcessChunk(*storage.Chunk)
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

	// Data can be chunked and mapped over channels for processing.
	Channels []storage.ChunkChannel
}

func NewData(id DataID, t TypeService) *Data {
	return &Data{DataID: id, Datatype: t}
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

// ---- ChunkHandler interface ----

const (
	// Number of chunks that can be buffered on each channel
	// before sender is blocked.
	ChannelBufferSize = 1000
)

/*
// Handler encapsulates the pool of chunk channels for data-specific handlers.
type Handler struct {
	channels []storage.ChunkChannel
	haltchan chan bool
}

// Each data has a pool of channels to communicate with gouroutine handlers.
var dataHandlers map[DataString]Handler

func init() {
	dataHandlers = make(map[DataString]Handler)
}
*/

// HaltOp is an empty struct to signal chunk handlers to quit.
type HaltOp struct{}

// HaltChunk returns a chunk with a HaltOp
func HaltChunk() *storage.Chunk {
	return &storage.Chunk{
		ChunkOp: storage.ChunkOp{Op: HaltOp{}},
	}
}

// ChannelSpecs returns the default number of chunk channels/handlers and
// the buffer size of a channel.
func (d *Data) ChannelSpecs() (number, bufferSize int) {
	return runtime.NumCPU(), ChannelBufferSize
}

// StartChunkHandlers starts goroutines that handle chunks for this data.
func (d *Data) StartChunkHandlers() {
	dataName := d.DataName()
	StartMonitor(dataName)

	var mapAccess sync.Mutex
	mapAccess.Lock()
	if d.Channels != nil {
		log.Printf("Already have chunk handlers for data set '%s' (%s)\n",
			dataName, d.DatatypeName())
	} else {
		numHandlers, bufferSize := d.ChannelSpecs()
		log.Printf("Starting %d chunk handlers for data '%s' (%s)...\n",
			numHandlers, dataName, d.DatatypeName())
		channels := make([]storage.ChunkChannel, 0, numHandlers)
		for i := 0; i < numHandlers; i++ {
			channel := make(storage.ChunkChannel, bufferSize)
			channels = append(channels, channel)
			go func(i int, c storage.ChunkChannel) {
				dvid.Log(dvid.Debug, "Starting chunk handler %d for %s...",
					i+1, dataName)
				for {
					chunk := <-c
					switch chunk.Op.(type) {
					case *storage.Chunk:
						d.ProcessChunk(chunk)
						DoneChannel <- dataName
					case HaltOp:
						dvid.Log(dvid.Debug, "Shutting down chunk handler %d for %s...",
							i+1, dataName)
						break
					}
				}
			}(i, channel)
		}
		d.Channels = channels
	}
	mapAccess.Unlock()
}

// StopChunkHandlers halts the goroutines handling chunks for this data.
func (d *Data) StopChunkHandlers() {
	for _, channel := range d.Channels {
		channel <- HaltChunk()
	}
	StopMonitor(d.DataName())
}

func (d *Data) ProcessChunk(chunk *storage.Chunk) {
	// Should be implemented by type.
}
