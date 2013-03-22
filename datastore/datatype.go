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
)

// This message is used for all data types to explain options.
const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 

    The following set options are available on the command line:

        uuid=<UUID>    Example: uuid=3f7a088

        rpc=<address>  Example: rpc=my.server.com:1234
`

type UrlString string

type DataFormat string

const (
	PNG DataFormat = "png"
	JPG            = "jpg"
	HDF            = "hdf5"
)

// DataStruct is an interface to structs that know their shape and data and is
// the unit of transfer between DVID and clients. Slices of various orientation 
// and subvolumes should satisfy this interface.
type DataStruct interface {
	dvid.Geometry
	TypeService

	// BlockHandler processes each block of this DataStruct in a hopefully
	// efficient manner.
	BlockHandler(r *BlockRequest)

	// The data itself.  Go image data is usually held in []uint8.
	Data() []uint8
}

// Request supports requests to the DVID server.  Since input and reply payloads 
// are different depending on the command, we use DataStruct interfaces for the
// payloads. 
type Request struct {
	dvid.Command
	DataStruct
}

// Response supports responses from DVID.
type Response struct {
	dvid.Response
	DataStruct
}

// TypeID provides methods for determining the identity of a data type.
type TypeID interface {
	// TypeName describes a data type and may not be unique.
	TypeName() string

	// TypeUrl returns the unique package name that fulfills the DVID Data interface
	TypeUrl() UrlString

	// TypeVersion describes the version identifier of this data type code
	TypeVersion() string
}

// TypeService is an interface for operations using arbitrary data types.
type TypeService interface {
	TypeID

	// BlockSize returns the block size for this data type
	BlockSize() dvid.Point3d

	// SpatialIndexing returns the spatial indexing scheme employed by this data type
	SpatialIndexing() SpatialIndexScheme

	// IsolatedKeys returns true if this type's data keys should be grouped by itself.
	IsolatedKeys() bool

	// The number of block handlers (goroutines) spawned for this data type for each
	// image version. 
	NumBlockHandlers() int

	// Help returns a string explaining how to use a data type's service
	Help(textHelp string) string

	// Do handles RPC commands specific to a data type
	DoRPC(request Request, reply *Response, s *Service) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request, s *Service, apiPrefixURL string)

	// Returns standard error response for unknown commands
	UnknownCommand(request Request) error
}

// RegisterDatatype registers a data type for DVID use.
func RegisterDatatype(t TypeService) {
	if CompiledTypes == nil {
		CompiledTypes = make(map[UrlString]TypeService)
	}
	CompiledTypes[t.TypeUrl()] = t
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

func (id DatatypeID) TypeName() string { return id.Name }

func (id DatatypeID) TypeUrl() UrlString { return id.Url }

func (id DatatypeID) TypeVersion() string { return id.Version }

// Datatype adds instance-specific information on how a data type is being used
// and for what named data sets it's being used.
type Datatype struct {
	DatatypeID

	// Block size
	BlockMax dvid.Point3d

	// Spatial indexing scheme
	Indexing SpatialIndexScheme

	// isolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

func (datatype *Datatype) BlockSize() dvid.Point3d {
	return datatype.BlockMax
}

func (datatype *Datatype) SpatialIndexing() SpatialIndexScheme {
	return datatype.Indexing
}

func (datatype *Datatype) IsolatedKeys() bool {
	return datatype.IsolateData
}

func (datatype *Datatype) Help(typeHelp string) string {
	return fmt.Sprintf(helpMessage+typeHelp, datatype.Name, datatype.Url)
}

func (datatype *Datatype) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		datatype.Name, datatype.Url, request.TypeCommand())
}

// CompiledTypes is the set of registered data types compiled into DVID and
// held as a global variable initialized at runtime.
var CompiledTypes map[UrlString]TypeService

// CompiledTypeNames returns a list of data type names compiled into this DVID. 
func CompiledTypeNames() string {
	var names []string
	for _, datatype := range CompiledTypes {
		names = append(names, datatype.TypeName())
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
		writeLine(datatype.TypeName(), datatype.TypeUrl())
	}
	return text + "\n"
}
