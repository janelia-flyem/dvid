package server

import (
	"fmt"

	"github.com/janelia-flyem/dvid/command"
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

// RpcConnection will export all of its functions for rpc access.
type RpcConnection struct{}

// Do acts as a switchboard for remote command execution
func (c *RpcConnection) Do(cmd *Command, reply *command.Packet) error {
	if reply == nil {
		fmt.Println("reply is nil coming in!")
		return nil
	}
	if len(cmd.Args) == 0 {
		return fmt.Errorf("Server error: got empty command!")
	}
	if runningService.data == nil {
		return fmt.Errorf("Datastore not open!  Cannot execute command.")
	}

	switch cmd.Name() {
	// Handle builtin commands
	case "types":
		return cmd.types(reply)
	case "help":
		return cmd.help(reply)
	case "branch", "lock", "log", "pull", "push":
		reply.Text = fmt.Sprintf("Server would have processed '%s'", cmd)
	default:
		// Assume this is the command for a supported data type
		return cmd.datatypeDo(reply)
	}
	return nil
}

// Command supports command-based interaction with DVID.  It extends the standard
// command package Command by bundling a packet used for input data since Go's
// rpc convention is to pass a single struct for input.  Once we pass the data
// through the rpc connection, we unpack the command and input packet to pass
// it to data types-specific handling.
type Command struct {
	command.Command

	command.Packet
}


// GetUuid returns the UUID corresponding to the string supplied by a "uuid=..."
// argument or if that's missing, the UUID for the head node.  
func (cmd *Command) GetUuid(dataService *datastore.Service) (uuid datastore.UUID,
	err error) {

	uuidString, found := cmd.GetSetting(command.KeyUuid)
	if found {
		uuid, err = dataService.GetUuidFromString(uuidString)
	} else {
		uuid, err = dataService.GetHeadUuid()
	}
	return
}

// The following command implementations assume dataService is non-nil, hence their
// unexported nature.

func (cmd *Command) datatypeDo(reply *command.Packet) error {
	// Get the TypeService for this data type.  Let user know if it's not supported.
	typeUrl, err := runningService.data.GetSupportedTypeUrl(cmd.Name())
	if err != nil {
		return fmt.Errorf("Command '%s' invalid and %s", cmd.Name(), err.Error())
	}
	typeService, found := datastore.SupportedTypes[typeUrl]
	if !found {
		return fmt.Errorf("Support for data type not compiled in: %s [%s]",
			cmd.Name(), typeUrl)
	}

	// Make sure we have at least a command in addition to the data type name
	if len(cmd.Args) < 2 {
		return fmt.Errorf("Must give a command in addition to data type!  Try '%s help'.",
			cmd.Name())
	}

	uuid, err := cmd.GetUuid(runningService.data)
	if err != nil {
		return err
	}
	return typeService.Do(uuid, &cmd.Command, &cmd.Packet, reply)
}

func (cmd *Command) help(reply *command.Packet) error {
	reply.Text = fmt.Sprintf(helpMessage, runningService.RpcAddress,
		runningService.data.SupportedTypeChart(), runningService.WebAddress)
	return nil
}

func (cmd *Command) types(reply *command.Packet) error {
	reply.Text = runningService.data.SupportedTypeChart()
	return nil
}
