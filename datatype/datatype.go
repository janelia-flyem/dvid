package datatype

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type UrlString string

// ID provides methods for determining the identity of a datatype.  Note that
// we are verbose for the functions of this interface because datatype.ID is
// likely to be embedded in other structs that will also have names, etc.
type ID interface {
	// TypeName is an abbreviated datatype name.
	TypeName() dvid.TypeString

	// TypeUrl returns the unique package url of the datatype implementation.
	TypeUrl() UrlString

	// TypeVersion describes the version identifier of this datatype code
	TypeVersion() string
}

// Service is an interface for operations using arbitrary datatypes.
type Service interface {
	ID

	// Help returns a string explaining how to use a datatype's service
	Help() string

	// Create Data that is an instance of this datatype in the given Repo
	NewDataService(id *DataInstance, config dvid.Config) (service DataService, err error)
}

var (
	// Compiled is the set of registered datatypes compiled into DVID and
	// held as a global variable initialized at runtime.
	Compiled map[UrlString]Service
)

// Register registers a datatype for DVID use.
func Register(t Service) {
	if Compiled == nil {
		Compiled = make(map[UrlString]Service)
	}
	Compiled[t.TypeUrl()] = t
}

// CompiledNames returns a list of datatype names compiled into this DVID.
func CompiledNames() string {
	var names []string
	for _, datatype := range Compiled {
		names = append(names, string(datatype.DatatypeName()))
	}
	return strings.Join(names, ", ")
}

// CompiledUrls returns a list of datatype urls supported by this DVID.
func CompiledUrls() string {
	var urls []string
	for url, _ := range Compiled {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// CompiledChart returns a chart (names/urls) of datatypes compiled into this DVID.
func CompiledChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name dvid.TypeString, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range Compiled {
		writeLine(datatype.DatatypeName(), datatype.DatatypeUrl())
	}
	return text + "\n"
}

// ServiceByName returns a type-specific service given a type name.
func ServiceByName(typeName dvid.TypeString) (typeService Service, err error) {
	for _, dtype := range Compiled {
		if typeName == dtype.DatatypeName() {
			typeService = dtype
			return
		}
	}
	err = fmt.Errorf("Data type '%s' is not supported in current DVID executable", typeName)
	return
}

// ---- Service Implementation ----

// DatatypeID uniquely identifies a DVID-supported datatype and provides a
// shorthand name.
type DatatypeID struct {
	// Data type name and may not be unique.
	Name dvid.TypeString

	// The unique package name that fulfills the DVID Data interface
	Url UrlString

	// The version identifier of this datatype code
	Version string
}

func MakeDatatypeID(name dvid.TypeString, url UrlString, version string) *DatatypeID {
	return &DatatypeID{name, url, version}
}

func (id *DatatypeID) TypeName() dvid.TypeString { return id.Name }

func (id *DatatypeID) TypeUrl() UrlString { return id.Url }

func (id *DatatypeID) TypeVersion() string { return id.Version }

const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 
`

// T is the base struct that satisfies a Service and can be embedded in other datatypes.
type T struct {
	DatatypeID

	// A list of interface requirements for the backend datastore
	Requirements *storage.Requirements
}

func (t *T) Help() string {
	return fmt.Sprintf(helpMessage, t.Name, t.Url)
}
