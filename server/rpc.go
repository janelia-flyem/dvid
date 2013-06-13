/*
	This file handles RPC connections, usually from DVID clients.
*/

package server

import (
	"fmt"
	"os"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

const helpMessage = `
Commands executed on the server (rpc address = %s):

	help  [command you are running]
	about
	log
	types
	dataset <dataset name> <data type name>   (example: "dataset mygrayscale grayscale8")
	shutdown

	(Commands on roadmap)
	comment <uuid> "..."
	add <uuid> <dataset name> <local filename glob>
	branch <uuid>
	lock <uuid>
	pull <remote dvid address> <uuid> [<extents>]
	push <remote dvid address> <uuid> [<extents>]
	archive <uuid>

%s

Use "<data type name> help" to get data type-specific help.

For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

// RPCConnection will export all of its functions for rpc access.
type RPCConnection struct{}

// Do acts as a switchboard for remote command execution
func (c *RPCConnection) Do(cmd datastore.Request, reply *datastore.Response) error {
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
			runningService.RPCAddress, runningService.SupportedDataChart(),
			runningService.WebAddress)
	case "types":
		reply.Text = runningService.SupportedDataChart()
	case "about":
		reply.Text = fmt.Sprintf("%s\n", runningService.About())
	case "log":
		reply.Text = runningService.LogInfo()
	case "dataset":
		var dataName, typeName string
		cmd.CommandArgs(1, &dataName, &typeName)
		dataSetName := datastore.DatasetString(dataName)
		err := runningService.NewDataset(dataSetName, typeName, dvid.Config{})
		if err != nil {
			return err
		}
		reply.Text = fmt.Sprintf("Added data set '%s' of type '%s'", dataSetName, typeName)
	case "dataset-delete":
		// Temporary command while we debug system
		var dataName string
		cmd.CommandArgs(1, &dataName)
		dataSetName := datastore.DatasetString(dataName)
		err := runningService.DeleteDataset(dataSetName)
		if err != nil {
			return err
		}
		reply.Text = fmt.Sprintf("Deleted data set '%s' ", dataSetName)
	case "shutdown":
		Shutdown()
		// Make this process shutdown in a second to allow time for RPC to finish.
		// TODO -- Better way to do this?
		go func() {
			time.Sleep(1 * time.Second)
			os.Exit(0)
		}()
	case "branch", "lock", "pull", "push", "archive":
		reply.Text = fmt.Sprintf("Server would have processed '%s'", cmd)
	default:
		// Assume this is the command for a supported data type
		return datasetDo(cmd, reply)
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

func datasetDo(cmd datastore.Request, reply *datastore.Response) error {
	// Get the DatasetService for this data set name.  Let user know if it's not supported.
	datasetName := datastore.DatasetString(cmd.Name())
	dataset, err := runningService.DatasetService(datasetName)
	if err != nil {
		if cmd.TypeCommand() == "help" {
			datatype, err := runningService.TypeService(cmd.Name())
			if err != nil {
				return err
			}
			fmt.Println(datatype.Help())
			return nil
		} else {
			return fmt.Errorf("Command '%s' is not a registered data set ", cmd.Name())
		}
	}

	// Make sure we have at least a command in addition to the data type name
	if cmd.TypeCommand() == "" {
		return fmt.Errorf("Must give a command in addition to data type!  Try '%s help'.",
			cmd.Name())
	}

	// Send the command to the data type
	return dataset.DoRPC(cmd, reply)
}
