package datastore

import (
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func init() {
	gob.Register(&Type{})
}

// Type identifies the datatype underlying a DataService.
type Type struct {
	// Data type name and may not be unique.
	Name dvid.TypeString

	// The unique package name that fulfills the DVID Data interface
	URL dvid.URLString

	// The version identifier of this datatype code
	Version string

	// A list of interface requirements for the backend datastore
	Requirements *storage.Requirements
}

func (t *Type) GetTypeName() dvid.TypeString {
	return t.Name
}

func (t *Type) GetTypeURL() dvid.URLString {
	return t.URL
}

func (t *Type) GetTypeVersion() string {
	return t.Version
}

func (t *Type) GetStorageRequirements() *storage.Requirements {
	return t.Requirements
}

// Do error message.  Do should be overridden
func (t *Type) Do(cmd Request, reply *Response) error {
	return fmt.Errorf("data type %q has not been setup to handle the given command", t.Name)
}

// TypeService is an interface all datatype implementations must fulfill.
// New types can be made by embedding Type and will automatically fulfill the Get* functions.
type TypeService interface {
	GetTypeName() dvid.TypeString

	GetTypeURL() dvid.URLString

	GetTypeVersion() string

	GetStorageRequirements() *storage.Requirements

	// Create an instance of this datatype in the given repo (identified by its root UUID)
	// with local instance ID and name, passing configuration parameters via dvid.Config.
	NewDataService(dvid.UUID, dvid.InstanceID, dvid.InstanceName, dvid.Config) (DataService, error)

	// Help returns a string explaining how to use a datatype's service
	Help() string

	// Do executes type-specific commands from command line.
	Do(Request, *Response) error
}

var (
	// Compiled is the set of registered datatypes compiled into DVID and
	// held as a global variable initialized at runtime.
	Compiled map[dvid.URLString]TypeService
)

// Register registers a datatype for DVID use.
func Register(t TypeService) {
	if Compiled == nil {
		Compiled = make(map[dvid.URLString]TypeService)
	}
	Compiled[t.GetTypeURL()] = t
}

// CompiledTypes returns a map of datatypes compiled into this DVID.
func CompiledTypes() map[dvid.TypeString]TypeService {
	types := make(map[dvid.TypeString]TypeService, len(Compiled))
	for _, typeservice := range Compiled {
		name := typeservice.GetTypeName()
		types[name] = typeservice
	}
	return types
}

// CompiledURLs returns a list of datatype urls supported by this DVID.
func CompiledURLs() string {
	var urls []string
	for url, _ := range Compiled {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// CompiledChart returns a chart (names/urls) of datatypes compiled into this DVID.
func CompiledChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name dvid.TypeString, url dvid.URLString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "URL")
	for _, t := range Compiled {
		writeLine(t.GetTypeName(), t.GetTypeURL())
	}
	return text + "\n"
}

// TypeServiceByName returns a TypeService given a type name.  Note that the
// type name is possibly ambiguous, particularly if using type names across
// different DVID servers.
func TypeServiceByName(name dvid.TypeString) (TypeService, error) {
	for _, typeservice := range Compiled {
		if name == typeservice.GetTypeName() {
			return typeservice, nil
		}
	}
	return nil, fmt.Errorf("Data type %q is not supported by current DVID server", name)
}

// TypeServiceByURL returns a TypeService given its URL.  This is the preferred
// method for accessing datatype implementations since they should work across different
// DVID servers.
func TypeServiceByURL(url dvid.URLString) (TypeService, error) {
	t, found := Compiled[url]
	if !found {
		return nil, fmt.Errorf("Data type %q is not supported by current DVID server", url)
	}
	return t, nil
}
