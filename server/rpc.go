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
	branch
	lock
	types
	log
	pull
	push
	archive

All of the commands above can include optional settings of the form:
	rpc=foo.com:1234  (Specifies the DVID server.)
	uuid=3efa87       (Specifies the image version within that datastore.)

%s

Use "<data type name> help" to get data type-specific help.

For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

// RpcConnection will export all of its functions for rpc access.
type RpcConnection struct{}

// Do acts as a switchboard for remote command execution
func (c *RpcConnection) Do(cmd dvid.Request, reply dvid.Response) error {
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
	case "types":
		reply.(*dvid.SimpleResponse).Text = runningService.SupportedDataChart()
	case "help":
		reply.(*dvid.SimpleResponse).Text = fmt.Sprintf(helpMessage,
			runningService.RpcAddress, runningService.SupportedDataChart(),
			runningService.WebAddress)
	case "version":
		reply.(*dvid.SimpleResponse).Text = fmt.Sprintf(
			"%s\n%s", runningService.Versions())
	case "branch":
		return branch(cmd.(*dvid.Command), reply.(*dvid.SimpleResponse))
	case "lock", "log", "pull", "push":
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
func branch(cmd *dvid.Command, reply *dvid.SimpleResponse) error {

	return nil
}

// The following command implementations assume dataService is non-nil, hence their
// unexported nature.

func datatypeDo(cmd dvid.Request, reply dvid.Response) error {
	// Get the TypeService for this data type.  Let user know if it's not supported.
	typeService, err := runningService.TypeService(cmd.Name())
	if err != nil {
		return fmt.Errorf("Command '%s' invalid: %s", cmd.Name(), err.Error())
	}

	// Make sure we have at least a command in addition to the data type name
	if cmd.TypeCommand == "" {
		return fmt.Errorf("Must give a command in addition to data type!  Try '%s help'.",
			cmd.Name())
	}

	// Get the UUID from command arguments
	var uuidNum int
	dvid.Log(dvid.Debug, "GetUuid() cmd = %s\n", cmd)
	uuidString, found := cmd.GetSetting(command.KeyUuid)
	dvid.Log(dvid.Debug, "  uuidstring = %s; found = %s\n", uuidString, found)
	if found {
		uuidNum, err = runningService.Service.GetUUIDFromString(uuidString)
		dvid.Log(dvid.Debug, "  after found, uuidNum = %s; err = %s\n", uuidNum, err)
	}
	if err != nil {
		reply.Text = fmt.Sprintf("Could not get uuid from command: %s", cmd)
		return err
	}

	// Send the command to the data type
	return typeService.DoRPC(
		&datastore.Request{
			svc: datastore.NewVersionService(runningService.Service, uuidNum),
			cmd,
		},
		reply,
	)
}
