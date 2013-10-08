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

	// Create Data that is an instance of this data type in the given Dataset
	NewDataService(dset *Dataset, id *DataID, config dvid.Config) (service DataService, err error)
}

// Subsetter is a type that can tell us its range of Index and how much it has
// actually available in this server.  It's used to implement limited cloning,
// e.g., only cloning a quarter of an image volume.
type Subsetter interface {
	// MaximumExtents returns a range of indices for which data is available at
	// some DVID server.
	MaximumExtents() dvid.IndexRange

	// AvailableExtents returns a range of indices for which data is available
	// at this DVID server.  It is the currently available extents.
	AvailableExtents() dvid.IndexRange
}

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
	DatasetID() DatasetLocalID

	// LocalID returns a DVID instance-specific id for this data.
	LocalID() DataLocalID

	// IsVersioned returns true if this data can be mutated across versions.
	IsVersioned() bool

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(uuid UUID, w http.ResponseWriter, r *http.Request) error

	// Returns standard error response for unknown commands
	UnknownCommand(r Request) error
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

// --- Base implementation and some functions using above types and interfaces -----

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

// ---- DataService implementation ----

// DataID identifies data within a DVID server.
type DataID struct {
	Name   DataString
	ID     DataLocalID
	DsetID DatasetLocalID
}

func (id DataID) DataName() DataString { return id.Name }

func (id DataID) LocalID() DataLocalID { return id.ID }

func (id DataID) DatasetID() DatasetLocalID { return id.DsetID }

// Data is an instance of a data type with some identifiers and it satisfies
// a DataService interface.  Each Data is dataset-specific.
type Data struct {
	*DataID
	TypeService

	// If false (default), we allow changes along nodes.
	Unversioned bool
}

// NewDataService returns a base data struct and sets the versioning depending on config.
func NewDataService(id *DataID, t TypeService, config dvid.Config) (data *Data, err error) {
	data = &Data{DataID: id, TypeService: t}
	var versioned bool
	versioned, err = config.IsVersioned()
	if err != nil {
		return
	}
	data.Unversioned = !versioned
	return
}

func (d *Data) IsVersioned() bool {
	return !d.Unversioned
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.Name, d.DatatypeName(), request.TypeCommand())
}
