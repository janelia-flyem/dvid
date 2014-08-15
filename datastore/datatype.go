package datastore

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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

func (t *Type) GetType() *Type {
	return t
}

// TypeService is an interface all datatype implementations must fulfill.
type TypeService interface {
	GetType() *Type

	// Create an instance of this datatype in the given repo (identified by its root UUID)
	// with local instance ID and name, passing configuration parameters via dvid.Config.
	NewDataService(dvid.UUID, dvid.InstanceID, dvid.DataString, dvid.Config) (DataService, error)

	// Help returns a string explaining how to use a datatype's service
	Help() string
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
	Compiled[t.GetType().URL] = t
}

// CompiledNames returns a list of datatype names compiled into this DVID.
func CompiledNames() string {
	var names []string
	for _, typeservice := range Compiled {
		names = append(names, string(typeservice.GetType().Name))
	}
	return strings.Join(names, ", ")
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
	for _, typeservice := range Compiled {
		t := typeservice.GetType()
		writeLine(t.Name, t.URL)
	}
	return text + "\n"
}

// TypeServiceByName returns a TypeService given a type name.  Note that the
// type name is possibly ambiguous, particularly if using type names across
// different DVID servers.
func TypeServiceByName(name dvid.TypeString) (TypeService, error) {
	for _, typeservice := range Compiled {
		if name == typeservice.GetType().Name {
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
