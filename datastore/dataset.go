/*
	This file supports Datasets, particular instances of user-defined Datatype.
*/

package datastore

import (
	"fmt"
	"net/http"

	"github.com/janelia-flyem/dvid/dvid"
)

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

	// Returns standard error response for unknown commands
	UnknownCommand(request Request) error

	// Shutdown closes any cache and halts any block handlers for this data set.
	Shutdown()
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
	DatasetID

	// Underlying implementation of this dataset is a Datatype
	Datatype TypeService
}

func BaseDataset(id DatasetID, t TypeService) *Dataset {
	return &Dataset{id, t}
}

func (d *Dataset) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.Name, d.Datatype.DatatypeName(), request.TypeCommand())
}

// Forward the TypeService interface calls to the embedded Datatype.  Datatype
// is not simply embedded because of potential conflicts in Name, etc, between
// datasets and datatypes.  Prefer finer control.

func (d *Dataset) DatatypeName() string { return d.Datatype.DatatypeName() }

func (d *Dataset) DatatypeUrl() UrlString { return d.Datatype.DatatypeUrl() }

func (d *Dataset) DatatypeVersion() string { return d.Datatype.DatatypeVersion() }

func (d *Dataset) Help() string { return d.Datatype.Help() }

func (d *Dataset) NewDataset(id DatasetID, config dvid.Config) DatasetService {
	return d.Datatype.NewDataset(id, config)
}
