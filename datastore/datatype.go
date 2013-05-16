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
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
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

// DataStruct is an interface to structs that know their shape and data and is
// the unit of transfer between DVID and clients. Slices of various orientation 
// and subvolumes should satisfy this interface.
type DataStruct interface {
	dvid.Geometry
	TypeService

	// BlockHandler processes a block of data.
	BlockHandler(r *BlockRequest)

	// The data set this data belongs to.
	DataSetName() DataSetString

	// The data itself.  Go image data is usually held in []uint8.
	Data() []uint8

	// Stride gives the increment in bytes between vertically adjacent pixels if
	// the Data() is a 2d image.  This is necessary because the Data might not
	// be packed only with image data but might including padding or metadata.
	Stride() int32
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

	// BlockBytes returns the number of bytes in block data buffer
	BlockBytes() int

	// IndexScheme returns the index scheme employed by this data type
	IndexScheme() IndexScheme

	// IsolatedKeys returns true if this type's data keys should be grouped by itself.
	IsolatedKeys() bool

	// The number of block handlers (goroutines) spawned for this data type for each
	// image version. 
	NumBlockHandlers() int

	// Help returns a string explaining how to use a data type's service
	Help() string

	// Do handles RPC commands specific to a data type
	DoRPC(request Request, reply *Response, s *Service) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request, s *Service, apiPrefixURL string) error

	// Returns standard error response for unknown commands
	UnknownCommand(request Request) error
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

// Datatype adds instance-specific information on how a data type is being used.
type Datatype struct {
	DatatypeID

	// Block size
	BlockMax dvid.Point3d

	// Spatial indexing scheme
	Indexing IndexScheme

	// A list of interface requirements for the backend datastore
	Requirements keyvalue.Requirements

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

func (datatype *Datatype) BlockSize() dvid.Point3d {
	return datatype.BlockMax
}

func (datatype *Datatype) IndexScheme() IndexScheme {
	return datatype.Indexing
}

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

// MapBlocks breaks down a DataStruct into a sequence of blocks that can be
// handled concurrently by datatype-specific block handers.  Each block handler
// is assigned the same blocks based on its index.
//
// Phase 1: Time leveldb built-in LRU cache and write buffer. (current)
// Phase 2: Minimize leveldb built-in LRU cache and use DVID LRU cache with
//   periodic and on-demand writes. 
// TODO -- Examine possible interleaving of block-level requests across MapBlocks()
//   calls and its impact on GET requests fulfilled while some blocks are still being
//   modified.
func (vs *VersionService) MapBlocks(op OpType, data DataStruct, wg *sync.WaitGroup) error {

	// Get components of the block key
	uuidBytes := vs.UuidBytes()
	datatypeBytes, err := vs.DataIndexBytes(data.DataSetName())
	if err != nil {
		return err
	}

	// Make sure we have Block Handlers for this data type.
	channels, found := HandlerChannels[data.DataSetName()]
	if !found {
		return fmt.Errorf("Error in reserving block handlers in MapBlocks() for %s!",
			data.DataSetName())
	}

	// Traverse blocks, get key/values if not in cache, and put block in queue for handler.
	ro := keyvalue.NewReadOptions()
	db_it, err := vs.kvdb.NewIterator(ro)
	defer db_it.Close()
	if err != nil {
		return err
	}
	index_it := NewIndexIterator(data)
	seek := true

	//dvid.Fmt(dvid.Debug, "Mapping blocks for %s\n", data)
	DiskAccess.Lock()
	switch op {
	case PutOp, GetOp:
		for {
			indexBytes := index_it()
			if indexBytes == nil {
				break
			}
			blockKey := BlockKey(uuidBytes, indexBytes, datatypeBytes, data.IsolatedKeys())

			// Pull from the datastore
			if seek || (db_it.Valid() && string(db_it.Key()) != string(blockKey)) {
				db_it.Seek(blockKey)
				seek = false
			}
			var value keyvalue.Value
			if db_it.Valid() && string(db_it.Key()) == string(blockKey) {
				//fmt.Printf("Got value\n")
				value = db_it.Value()
				db_it.Next()
			} else {
				seek = true
				if op == PutOp {
					value = make(keyvalue.Value, data.BlockBytes(), data.BlockBytes())
				} else {
					continue // If have no value, simply use zero value of slice/subvolume.
				}
			}

			// Initialize the block request
			req := &BlockRequest{
				DataStruct: data,
				Block:      value,
				Op:         op,
				IndexKey:   Index(indexBytes),
				BlockKey:   blockKey,
				Wait:       wg,
				DB:         vs.kvdb,
				//WriteBatch: writeBatch,
			}

			// Try to spread sequential block keys among different block handlers to get 
			// most out of our concurrent processing.
			if wg != nil {
				wg.Add(1)
			}
			channelNum := req.IndexKey.Hash(data, len(channels))
			//dvid.Fmt(dvid.Debug, "Sending %s block %s request %s down channel %d\n",
			//	op, Index(indexBytes).BlockCoord(data), data, channelNum)
			channels[channelNum] <- req
			requestChannel <- data.DataSetName()
		}
	default:
		return fmt.Errorf("Illegal operation (%d) asked for in MapBlocks()", op)
	}
	DiskAccess.Unlock()
	return nil
}
