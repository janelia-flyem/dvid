package datastore

import (
	"fmt"
	"strings"
)

type UrlString string

// Datatype uniquely identifies a DVID-supported data type and how it is arranged
// within a DVID datastore instance.
type Datatype struct {
	// Name describes a data type & may not be as unique (e.g., "grayscale")
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url UrlString

	// IsolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// TypeService is an interface for operations using arbitrary data types.
type TypeService interface {
	GetName() string
	GetUrl() UrlString
	GetIsolateData() bool

	// Help returns a string explaining how to use a data type's service
	Help() string

	// Do implements commands specific to a data type
	Do(uuid UUID, command string, args []string) error

	// Returns standard error response for unknown commands
	UnknownCommand(command string) error
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

func (datatype *Datatype) GetName() string {
	return datatype.Name
}

func (datatype *Datatype) GetUrl() UrlString {
	return datatype.Url
}

func (datatype *Datatype) GetIsolateData() bool {
	return datatype.IsolateData
}

func (datatype *Datatype) Help() string {
	helpMessage := `
	DVID data type information

	name: %s 
	 url: %s 

	This data type currently does not have a help message explaining
	its supported operations.
	`
	return fmt.Sprintf(helpMessage, datatype.GetName(), datatype.GetUrl())
}

func (datatype *Datatype) UnknownCommand(command string) error {
	return fmt.Errorf("Unknown command: %s [%s] does not support '%s' command",
		datatype.GetName(), datatype.GetUrl(), command)
}

// SupportedTypes is the set of registered data types held as a global variable 
// initialized at runtime.
var SupportedTypes map[UrlString]TypeService

// VerifySupportedTypeName returns no error if the given data type name has been
// compiled into DVID and also is supported by the current DVID datastore.
func VerifySupportedTypeName(name string) error {
	if SupportedTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}

	// Verify this data type was compiled into DVID
	var found bool
	for _, datatype := range SupportedTypes {
		if name == datatype.GetName() {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("DVID was not compiled with support for data type '%s'", name)
	}

	// Verify this data type is supported in current DVID datastore
	return nil
}

// SupportedTypeHelp returns all the help messages for each supported data type
// that is both compiled into DVID and available with the current open datastore.
func SupportedTypeHelp() string {
	return "No help here!\n"
}

// SupportedTypeNames returns a list of data type names supported by this DVID
func SupportedTypeNames() string {
	var names []string
	for _, datatype := range SupportedTypes {
		names = append(names, datatype.GetName())
	}
	return strings.Join(names, ", ")
}

// SupportedTypeUrls returns a list of data type urls supported by this DVID
func SupportedTypeUrls() string {
	var urls []string
	for url, _ := range SupportedTypes {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// SupportedTypeChart returns a chart (names/urls) of data types supported by this DVID
func SupportedTypeChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range SupportedTypes {
		writeLine(datatype.GetName(), datatype.GetUrl())
	}
	return text + "\n"
}

// RegisterDatatype registers a data type for DVID use.
func RegisterDatatype(datatype TypeService) {
	fmt.Printf("Registering data type datatype: %s (%s)\n",
		datatype.GetName(), datatype.GetUrl())
	if SupportedTypes == nil {
		SupportedTypes = make(map[UrlString]TypeService)
	}
	SupportedTypes[datatype.GetUrl()] = datatype
}

// GetService returns a supported data type's service or an error if name is not supported 
// or ambiguous.
func GetService(name string) (service TypeService, err error) {
	for _, datatype := range SupportedTypes {
		if name == datatype.GetName() {
			if service != nil {
				err = fmt.Errorf("Given data type (%s) is ambiguous.  Supporting '%s' and '%s'",
					name, service.GetName(), datatype.GetName())
				service = nil
				return
			} else {
				service = datatype
			}
		}
	}
	if service == nil {
		err = fmt.Errorf(
			"Data type (%s) is unsupported.  DVID has been compiled with these data types: %s",
			name, SupportedTypeNames())
	}
	return
}
