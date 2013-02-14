package server

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
)

const helpMessage = `
dvid is a distributed, versioned image datastore

   usage: dvid [options] <command>

Commands executed locally:

	init <json config filename> [dir=/path/to/datastore/dir]
	serve [dir=/path/to/datastore/dir] [web=...] [rpc=...]

Commands executed on the server (%s):

	help  [command you are running]
	branch
	lock
	types
	log
	pull
	push

All of the commands above can include optional settings of the form:
	rpc=foo.com:1234  (Specifies the DVID server.)
	uuid=3efa87       (Specifies the image version within that datastore.)

%s
For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

// Command supports command-based interaction with DVID
type Command struct {
	// Args lists the elements of the command where Args[0] is the command string
	Args []string

	// Param allows commands to attach input data
	datastore.CommandData
}

func (cmd Command) String() string {
	return strings.Join(cmd.Args, " ")
}

// GetParameter scans a command for any "key=value" argument and returns
// the value of the passed 'key'.
func (cmd Command) GetParameter(key string) (value string, found bool) {
	if len(cmd.Args) > 1 {
		for _, param := range cmd.Args[1:] {
			elems := strings.Split(param, "=")
			if len(elems) == 2 && elems[0] == key {
				value = elems[1]
				found = true
				return
			}
		}
	}
	return
}

// GetRpcAddress returns the address set by a "rpc=..." argument or if that is 
// missing, the default rpc address.
func (cmd Command) GetRpcAddress() string {
	address, found := cmd.GetParameter("rpc")
	if !found {
		return DefaultRpcAddress
	}
	return address
}

// GetWebAddress returns the address set by a "web=..." argument or if that is 
// missing, the default web address.
func (cmd Command) GetWebAddress() string {
	address, found := cmd.GetParameter("web")
	if !found {
		return DefaultWebAddress
	}
	return address
}

// GetDatastoreDir returns a directory specified in the arguments via "dir=..." or
// defaults to the current directory.
func (cmd Command) GetDatastoreDir() string {
	datastoreDir, found := cmd.GetParameter("dir")
	if !found {
		currentDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get current directory:", err)
		}
		return currentDir
	}
	return datastoreDir
}

// GetUuid returns the UUID corresponding to the string supplied by a "uuid=..."
// argument or if that's missing, the UUID for the head node.  
func (cmd Command) GetUuid(dataService *datastore.Service) (uuid datastore.UUID,
	err error) {

	uuidString, found := cmd.GetParameter("uuid")
	if found {
		uuid, err = dataService.GetUuidFromString(uuidString)
	} else {
		uuid, err = dataService.GetHeadUuid()
	}
	return
}

// The following command implementations assume dataService is non-nil, hence their
// unexported nature.

func (cmd Command) datatypeDo(reply *datastore.CommandData) error {
	// Get the TypeService for this data type.  Let user know if it's not supported.
	typeUrl, err := runningService.data.GetSupportedTypeUrl(cmd.Args[0])
	if err != nil {
		return err
	}
	typeService, found := datastore.SupportedTypes[typeUrl]
	if !found {
		return fmt.Errorf("Support for data type not compiled in: %s [%s]",
			cmd.Args[0], typeUrl)
	}

	// Make sure we have at least a command in addition to the data type name
	if len(cmd.Args[1:]) < 1 {
		return fmt.Errorf("Must have at least a command in addition to data type!")
	}

	uuid, err := cmd.GetUuid(runningService.data)
	if err != nil {
		return err
	}
	return typeService.Do(uuid, cmd.Args[1], cmd.Args[2:], reply)
}

func (cmd Command) help(reply *datastore.CommandData) error {
	reply.Text = fmt.Sprintf(helpMessage, runningService.RpcAddress,
		runningService.data.SupportedTypeChart(), runningService.WebAddress)
	return nil
}

func (cmd Command) types(reply *datastore.CommandData) error {
	reply.Text = runningService.data.SupportedTypeChart()
	return nil
}
