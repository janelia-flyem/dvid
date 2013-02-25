/*
	This file contains code useful for arbitrary data types supported in DVID.
	It includes the base Datatype struct which are embedded in user-supplied
	data types as well as useful functions like image loading likely to be used
	by many data types.
*/

package datastore

import (
	"fmt"
	"os"
	"strings"

	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"github.com/janelia-flyem/dvid/command"
	_ "github.com/janelia-flyem/dvid/dvid"
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

// Datatype uniquely identifies a DVID-supported data type and how it is arranged
// within a DVID datastore instance.
type Datatype struct {
	// Name describes a data type & may not be as unique (e.g., "grayscale")
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url UrlString

	// Version describes the version identifier of this data type code
	Version string

	// IsolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// TypeService is an interface for operations using arbitrary data types.
type TypeService interface {
	BaseDatatype() Datatype

	// Help returns a string explaining how to use a data type's service
	Help(textHelp string) string

	// Do implements commands specific to a data type
	Do(vs *VersionService, cmd *command.Command, input, reply *command.Packet) error

	// Returns standard error response for unknown commands
	UnknownCommand(cmd *command.Command) error
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

func (datatype *Datatype) BaseDatatype() Datatype {
	return *datatype
}

func (datatype *Datatype) Help(typeHelp string) string {
	return fmt.Sprintf(helpMessage+typeHelp, datatype.Name, datatype.Url)
}

func (datatype *Datatype) UnknownCommand(cmd *command.Command) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		datatype.Name, datatype.Url, cmd.TypeCommand())
}

// CompiledTypes is the set of registered data types compiled into DVID and
// held as a global variable initialized at runtime.
var CompiledTypes map[UrlString]TypeService

// VerifyCompiledTypeName returns no error if the given data type name has been
// compiled into DVID.
func VerifyCompiledTypeName(name string) error {
	if CompiledTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}

	// Verify this data type was compiled into DVID
	var found bool
	for _, datatype := range CompiledTypes {
		if name == datatype.BaseDatatype().Name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("DVID was not compiled with support for data type '%s'", name)
	}
	return nil
}

// CompiledTypesList returns a list of data types compiled into this DVID
func CompiledTypesList() (datatypes []Datatype) {
	for _, datatype := range CompiledTypes {
		datatypes = append(datatypes, datatype.BaseDatatype())
	}
	return
}

// CompiledTypeNames returns a list of data type names compiled into this DVID
func CompiledTypeNames() string {
	var names []string
	for _, datatype := range CompiledTypes {
		names = append(names, datatype.BaseDatatype().Name)
	}
	return strings.Join(names, ", ")
}

// CompiledTypeUrls returns a list of data type urls supported by this DVID
func CompiledTypeUrls() string {
	var urls []string
	for url, _ := range CompiledTypes {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// CompiledTypeChart returns a chart (names/urls) of data types compiled into this DVID
func CompiledTypeChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range CompiledTypes {
		writeLine(datatype.BaseDatatype().Name, datatype.BaseDatatype().Url)
	}
	return text + "\n"
}

// RegisterDatatype registers a data type for DVID use.
func RegisterDatatype(datatype TypeService) {
	if CompiledTypes == nil {
		CompiledTypes = make(map[UrlString]TypeService)
	}
	CompiledTypes[datatype.BaseDatatype().Url] = datatype
}

// GetTypeService returns a supported data type's service or an error if name is not supported 
// or ambiguous.
func GetTypeService(name string) (service TypeService, err error) {
	for _, datatype := range CompiledTypes {
		if name == datatype.BaseDatatype().Name {
			if service != nil {
				err = fmt.Errorf("Given data type (%s) is ambiguous.  Supporting '%s' and '%s'",
					name, service.BaseDatatype().Name, datatype.BaseDatatype().Name)
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
			name, CompiledTypeNames())
	}
	return
}

/***** Image Utilities ******/

func LoadImage(filename string) (img image.Image, format string, err error) {
	var file *os.File
	file, err = os.Open(filename)
	if err != nil {
		err = fmt.Errorf("Unable to open image (%s).  Is this visible to server process?",
			filename)
		return
	}
	img, format, err = image.Decode(file)
	return
}
