package datastore

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type URLString string

// TypeID provides methods for determining the identity of a datatype.  Note that
// we are verbose for the functions of this interface because TypeID is
// likely to be embedded in other structs like DataInstance that will also have names, etc.
type TypeID interface {
	// TypeName is an abbreviated datatype name.
	TypeName() dvid.TypeString

	// TypeURL returns the unique package url of the datatype implementation.
	TypeURL() URLString

	// TypeVersion describes the version identifier of this datatype code
	TypeVersion() string
}

// TypeService is an interface all datatype implementations must fulfill.
type TypeService interface {
	TypeID

	// Help returns a string explaining how to use a datatype's service
	Help() string

	// Create an instance of this datatype in the given repo with local instance ID
	// and name, passing configuration parameters via dvid.Config.
	NewDataService(Repo, dvid.InstanceID, dvid.DataString, dvid.Config) (DataService, error)
}

var (
	// Compiled is the set of registered datatypes compiled into DVID and
	// held as a global variable initialized at runtime.
	Compiled map[URLString]TypeService
)

// Register registers a datatype for DVID use.
func Register(t TypeService) {
	if Compiled == nil {
		Compiled = make(map[URLString]TypeService)
	}
	Compiled[t.TypeURL()] = t
}

// CompiledNames returns a list of datatype names compiled into this DVID.
func CompiledNames() string {
	var names []string
	for _, datatype := range Compiled {
		names = append(names, string(datatype.TypeName()))
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
	writeLine := func(name dvid.TypeString, url URLString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range Compiled {
		writeLine(datatype.TypeName(), datatype.TypeURL())
	}
	return text + "\n"
}

// TypeServiceByName returns a TypeService given a type name.
func TypeServiceByName(name dvid.TypeString) (TypeService, error) {
	for _, dtype := range Compiled {
		if name == dtype.TypeName() {
			return dtype, nil
		}
	}
	return nil, fmt.Errorf("Data type '%s' is not supported in current DVID executable", name)
}

// ---- Service Implementation ----

// DatatypeID uniquely identifies a DVID-supported datatype and provides a
// shorthand name.
type DatatypeID struct {
	// Data type name and may not be unique.
	Name dvid.TypeString

	// The unique package name that fulfills the DVID Data interface
	Url URLString

	// The version identifier of this datatype code
	Version string
}

func MakeDatatypeID(name dvid.TypeString, url URLString, version string) *DatatypeID {
	return &DatatypeID{name, url, version}
}

func (id *DatatypeID) TypeName() dvid.TypeString { return id.Name }

func (id *DatatypeID) TypeURL() URLString { return id.Url }

func (id *DatatypeID) TypeVersion() string { return id.Version }

const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 
`

// Datatype is the base struct that satisfies a Service and can be embedded in other datatypes.
type Datatype struct {
	*DatatypeID

	// A list of interface requirements for the backend datastore
	Requirements *storage.Requirements
}

func (t *Datatype) Help() string {
	return fmt.Sprintf(helpMessage, t.Name, t.Url)
}
