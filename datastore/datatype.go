/*
	This file contains code useful for arbitrary data types supported in DVID.
	It includes the base Datatype struct which are embedded in user-supplied
	data types as well as useful functions like image loading likely to be used
	by many data types.
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

	// Create a Dataset of this data type
	NewDataset(s *Service, id DatasetID, config dvid.Config) DatasetService

	// Returns standard error response for unknown commands
	UnknownCommand(request Request) error
}

// DatasetString is a string that is the name of a DVID data set.
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data set name.
type DatasetString string

// DatasetService is an interface for operations on arbitrary datasets that
// use a supported TypeService.  Block handlers can be allocated at this level,
// so an implementation can own a number of goroutines.
//
// DatasetService operations are completely type-specific, and each datatype
// handles operations through RPC (DoRPC) and HTTP (DoHTTP).
// TODO -- Add SPDY as wrapper to HTTP.
type DatasetService interface {
	TypeService

	// DatasetName returns the name of the dataset.
	DatasetName() DatasetString

	// DatasetLocalID returns a DVID instance-specific id for this dataset,
	// which can be held in a relatively small number of bytes and is useful
	// as a key component.
	DatasetLocalID() dvid.LocalID

	// Handle iteration through a dataset in abstract way
	NewIndexIterator(extents interface{}) IndexIterator

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request, apiPrefixURL string) error

	// Shutdown closes any cache and halts any block handlers for this data set.
	Shutdown()
}

// CompiledTypes is the set of registered data types compiled into DVID and
// held as a global variable initialized at runtime.
var CompiledTypes map[UrlString]TypeService

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
	// Name describes a data type and may not be unique.
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url UrlString

	// Version describes the version identifier of this data type code
	Version string
}

func MakeDatatypeID(name string, url UrlString, version string) DatatypeID {
	return DatatypeID{name, url, version}
}

func (id DatatypeID) DatatypeName() string { return id.Name }

func (id DatatypeID) DatatypeUrl() UrlString { return id.Url }

func (id DatatypeID) DatatypeVersion() string { return id.Version }

// Datatype is the base struct that satisfies a TypeService and can be embedded
// in other data types.
type Datatype struct {
	DatatypeID

	// A list of interface requirements for the backend datastore
	Requirements storage.Requirements

	// IsolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

// Types must add a NewDataset() function...

func (datatype *Datatype) IsolatedKeys() bool {
	return datatype.IsolateData
}

func (datatype *Datatype) Help() string {
	return fmt.Sprintf(helpMessage, datatype.Name, datatype.Url)
}

func (datatype *Datatype) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		datatype.Name, datatype.Url, request.TypeCommand())
}

// DatasetID identifies a dataset within a DVID instance.
type DatasetID struct {
	Name    DatasetString
	LocalID dvid.LocalID
}

func (id DatasetID) DatasetName() DatasetString { return id.Name }

func (id DatasetID) DatasetLocalID() dvid.LocalID { return id.LocalID }

// Dataset is an instance of a data type and has an associated datastore.
type Dataset struct {
	TypeService
	DatasetID

	// Each dataset has a pointer to the storage service it uses.
	store *Service
}

func BaseDataset(t TypeService, id DatasetID, s *Service) *Dataset {
	return &Dataset{t, id, s}
}
