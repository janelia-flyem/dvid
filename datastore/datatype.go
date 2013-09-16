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
	// TypeName is an abbreviated type name.
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
	NewData(id *DataID, config dvid.Config) (service DataService, err error)
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

// TypeServiceByName returns a type-specific service given a type name.
func TypeServiceByName(typeName string) (typeService TypeService, err error) {
	for _, dtype := range CompiledTypes {
		if typeName == dtype.DatatypeName() {
			typeService = dtype
			return
		}
	}
	err = fmt.Errorf("Data type '%s' is not supported in current DVID executable", typeName)
	return
}

// ---- TypeService Implementation ----

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

func MakeDatatypeID(name string, url UrlString, version string) *DatatypeID {
	return &DatatypeID{name, url, version}
}

func (id *DatatypeID) DatatypeName() string { return id.Name }

func (id *DatatypeID) DatatypeUrl() UrlString { return id.Url }

func (id *DatatypeID) DatatypeVersion() string { return id.Version }

// Datatype is the base struct that satisfies a TypeService and can be embedded
// in other data types.
type Datatype struct {
	*DatatypeID

	// A list of interface requirements for the backend datastore
	Requirements *storage.Requirements
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

	// DatasetLocalID returns a DVID instance-specific id for this dataset, which
	// can be held in a relatively small number of bytes and is a key component.
	DatasetLocalID() dvid.LocalID32

	// DataLocalID returns a DVID instance-specific id for this data.
	DataLocalID() dvid.LocalID

	// IsVersioned returns true if this data can be mutated across versions.
	IsVersioned() bool

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(uuid UUID, w http.ResponseWriter, r *http.Request) error

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

// ---- DataService and ChunkHandler implementation ----

// DataID identifies data within a DVID server.
type DataID struct {
	Name      DataString
	ID        dvid.LocalID
	DatasetID dvid.LocalID32
}

func (id DataID) DataName() DataString { return id.Name }

func (id DataID) DataLocalID() dvid.LocalID { return id.ID }

func (id DataID) DatasetLocalID() dvid.LocalID32 { return id.DatasetID }

// Data is an instance of a data type with some identifiers and it satisfies
// a ChunkHandler interface.  Each Data is dataset-specific.
type Data struct {
	*DataID
	TypeService

	// If false (default), we allow changes along nodes.
	unversioned bool

	// Data can be chunked and mapped over channels for processing.
	channels []storage.ChunkChannel
}

// NewData returns a base data struct and sets the versioning depending on config.
func NewData(id *DataID, t TypeService, config dvid.Config) (data *Data, err error) {
	data = &Data{DataID: id, TypeService: t}
	var versioned bool
	versioned, err = config.IsVersioned()
	if err != nil {
		return
	}
	data.unversioned = !versioned
	return
}

func (d *Data) IsVersioned() bool {
	return !d.unversioned
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.Name, d.DatatypeName(), request.TypeCommand())
}

// ---- ChunkHandler interface ----

// Number of chunks that can be buffered on each channel
// before sender is blocked.
const ChannelBufferSize = 1000

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

// ChunkChannels returns a slice of channels to communicate with chunk handlers.
func (d *Data) ChunkChannels() []storage.ChunkChannel {
	return d.channels
}

// StartChunkHandlers starts goroutines that handle chunks for this data.
func (d *Data) StartChunkHandlers() {
	// Lock this function to prevent interleaved StartChunkHandlers()
	var mapAccess sync.Mutex
	mapAccess.Lock()
	defer mapAccess.Unlock()

	dataName := d.DataName()
	StartMonitor(dataName)
	if d.channels == nil {
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
		d.channels = channels
	}
}

// StopChunkHandlers halts the goroutines handling chunks for this data.
func (d *Data) StopChunkHandlers() {
	for _, channel := range d.channels {
		channel <- HaltChunk()
	}
	StopMonitor(d.DataName())
}

// ProcessChunk should be implemented by each type if that type supports map/reduce
// operations on chunks.  See datatype/voxels package.
func (d *Data) ProcessChunk(chunk *storage.Chunk) {
}
