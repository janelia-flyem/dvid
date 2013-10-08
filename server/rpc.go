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

const helpMessage = `Commands executed on the server (rpc address = %s):

	help
	about
	shutdown

	types
	datasets info
	datasets new         (returns UUID of dataset's root node)

	dataset <UUID> new <datatype name> <data name> <datatype-specific config>...
	dataset <UUID> new <datatype name> <data name> <datatype-specific config>...

	dataset <UUID> <data name> help

	node <UUID> lock
	node <UUID> branch   (returns UUID of new child node)
	node <UUID> <data name> <type-specific commands>

%s

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

	case "help":
		reply.Text = fmt.Sprintf(helpMessage,
			runningService.RPCAddress, runningService.SupportedDataChart(),
			runningService.WebAddress)

	case "about":
		reply.Text = fmt.Sprintf("%s\n", runningService.About())

	case "shutdown":
		Shutdown()
		// Make this process shutdown in a second to allow time for RPC to finish.
		// TODO -- Better way to do this?
		go func() {
			time.Sleep(1 * time.Second)
			os.Exit(0)
		}()
		reply.Text = fmt.Sprintf("DVID server at %s has been halted.",
			runningService.RPCAddress)

	case "types":
		reply.Text = runningService.SupportedDataChart()

	case "datasets":
		var subcommand string
		cmd.CommandArgs(1, &subcommand)
		switch subcommand {
		case "info":
			jsonStr, err := runningService.DatasetsListJSON()
			if err != nil {
				return err
			}
			reply.Text = jsonStr
		case "new":
			uuid, _, err := runningService.NewDataset()
			if err != nil {
				return err
			}
			reply.Text = string(uuid)
		default:
			return fmt.Errorf("Unknown datasets command: %q", subcommand)
		}

	case "dataset":
		var uuidStr, subcommand, typename, dataname string
		cmd.CommandArgs(1, &uuidStr, &subcommand)
		uuid, _, _, err := runningService.NodeIDFromString(uuidStr)
		if err != nil {
			return err
		}
		switch subcommand {
		case "new":
			cmd.CommandArgs(3, &typename, &dataname)
			config := cmd.Settings()
			err = runningService.NewData(uuid, typename, dataname, config)
			if err != nil {
				return err
			}
			reply.Text = fmt.Sprintf("Data %q [%s] added to node %s", dataname, typename, uuidStr)
		default:
			dataname := datastore.DataString(subcommand)
			dataservice, err := runningService.DataService(uuid, dataname)
			if err != nil {
				return err
			}
			var subcommand2 string
			cmd.CommandArgs(3, &subcommand2)
			if subcommand2 == "help" {
				reply.Text = dataservice.Help()
			} else {
				return fmt.Errorf("Unknown command: %q", cmd)
			}
		}

	case "node":
		var uuidStr, descriptor string
		cmd.CommandArgs(1, &uuidStr, &descriptor)
		uuid, _, _, err := runningService.NodeIDFromString(uuidStr)
		if err != nil {
			return err
		}
		switch descriptor {
		case "lock":
			err := runningService.Lock(uuid)
			if err != nil {
				return err
			}
		case "branch":
			newuuid, err := runningService.NewVersion(uuid)
			if err != nil {
				return err
			}
			reply.Text = string(newuuid)

		default:
			dataname := datastore.DataString(descriptor)
			var subcommand string
			cmd.CommandArgs(3, &subcommand)
			dataservice, err := runningService.DataService(uuid, dataname)
			if err != nil {
				return err
			}
			if subcommand == "help" {
				reply.Text = dataservice.Help()
				return nil
			}
			return dataservice.DoRPC(cmd, reply)
		}

	default:
		return fmt.Errorf("Unknown command: '%s'", cmd)
	}
	return nil
}
