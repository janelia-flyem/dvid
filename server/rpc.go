/*
	This file handles RPC connections, usually from DVID clients.
*/

package server

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

const helpMessage = `
Commands executed on the server (%s):

	help  [command you are running]
	log
	version
	types
	dataset <dataset name> <data type name>   (example: "dataset mygrayscale grayscale8")

	(Commands on roadmap)
	comment <uuid> "..."
	add <uuid> <dataset name> <local filename glob>
	branch <uuid>
	lock <uuid>
	pull <remote dvid address> <uuid> [<extents>]
	push <remote dvid address> <uuid> [<extents>]
	archive <uuid>

All of the commands above can include optional settings of the form:
	rpc=foo.com:1234  (Specifies the DVID server.)

%s

Use "<data type name> help" to get data type-specific help.

For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

// RpcConnection will export all of its functions for rpc access.
type RpcConnection struct{}

// Do acts as a switchboard for remote command execution
func (c *RpcConnection) Do(cmd datastore.Request, reply *datastore.Response) error {
	if reply == nil {
		dvid.Log(dvid.Debug, "reply is nil coming in!\n")
		return nil
	}
	if cmd.Name() == "" {
		return fmt.Errorf("Server error: got empty command!")
	}
	if runningService.Service == nil {
		return fmt.Errorf("Datastore not open!  Cannot execute command.")
	}

	switch cmd.Name() {
	// Handle builtin commands
	case "help":
		reply.Text = fmt.Sprintf(helpMessage,
			runningService.RpcAddress, runningService.SupportedDataChart(),
			runningService.WebAddress)
	case "types":
		reply.Text = runningService.SupportedDataChart()
	case "version":
		reply.Text = fmt.Sprintf("%s\n%s", runningService.Versions())
	case "log":
		reply.Text = runningService.LogInfo()
	case "dataset":
		var dataSetName, typeName string
		cmd.CommandArgs(1, &dataSetName, &typeName)
		err := runningService.NewDataSet(dataSetName, typeName)
		if err != nil {
			return err
		}
		reply.Text = fmt.Sprintf("Added data set '%s' of type '%s'", dataSetName, typeName)
	case "branch", "lock", "pull", "push", "archive":
		reply.Text = fmt.Sprintf("Server would have processed '%s'", cmd)
	default:
		// Assume this is the command for a supported data type
		return datatypeDo(cmd, reply)
	}
	return nil
}

// Branch creates a new version, returning a UUID for the new version.
// By default, we copy all key/values for the parent UUID since we optimize
// for speed and not for size of datastore.  (Size can be decreased byte
// running 'archive' command on a UUID.)
func branch(cmd datastore.Request, reply *datastore.Response) error {
	return nil
}

// The following command implementations assume dataService is non-nil, hence their
// unexported nature.

func datatypeDo(cmd datastore.Request, reply *datastore.Response) error {
	// Get the TypeService for this data set name.  Let user know if it's not supported.
	typeService, err := runningService.DataSetService(cmd.Name())
	if err != nil {
		// See if the user is addressing a data type name, not a data set name.
		typeService, err = runningService.TypeService(cmd.Name())
		if err != nil {
			return fmt.Errorf("Command '%s' is neither a data set or data type name!", cmd.Name())
		}
	}

	// Make sure we have at least a command in addition to the data type name
	if cmd.TypeCommand() == "" {
		return fmt.Errorf("Must give a command in addition to data type!  Try '%s help'.",
			cmd.Name())
	}

	// Send the command to the data type
	return typeService.DoRPC(cmd, reply, runningService.Service)
}
