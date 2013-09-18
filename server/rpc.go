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

	help
	about
	shutdown

	types
	datasets info
	datasets new         (returns UUID of dataset's root node)

	dataset <UUID> versioned <datatype name> <data name>
	dataset <UUID> unversioned <datatype name> <data name>

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
			jsonStr, err := runningService.Datasets.JSON()
			if err != nil {
				return err
			}
			reply.Text = jsonStr
		case "new":
			dataset, err := runningService.NewDataset()
			if err != nil {
				return err
			}
			reply.Text = string(dataset.RootUUID())
		default:
			return fmt.Errorf("Unknown datasets command: %q", subcommand)
		}

	case "dataset":
		var uuidStr, descriptor, typename, dataname string
		cmd.CommandArgs(1, &uuidStr, &descriptor)
		switch descriptor {
		case "versioned", "unversioned":
			cmd.CommandArgs(3, &typename, &dataname)
			err := newData(uuidStr, typename, dataname, descriptor == "versioned")
			if err != nil {
				return err
			}
			reply.Text = fmt.Sprintf("Data %q [%s] added to node %s", dataname, typename, uuidStr)
		default:
			// Must be data name.
			var subcommand string
			cmd.CommandArgs(3, &subcommand)
			if subcommand != "help" {
				return fmt.Errorf("Unknown command: %q", cmd)
			}
			reply.Text = fmt.Sprintf("TODO -- Implement help for data %s in node %s", descriptor, uuidStr)
		}

	case "node":
		var uuidStr, descriptor string
		cmd.CommandArgs(1, &uuidStr, &descriptor)
		dataset, uuid, err := runningService.DatasetFromString(uuidStr)
		if err != nil {
			return err
		}
		switch descriptor {
		case "lock":
			err := dataset.Lock(uuid)
			if err != nil {
				return err
			}
		case "branch":
			newuuid, err := dataset.NewChild(uuid)
			if err != nil {
				return err
			}
			reply.Text = string(newuuid)

		default:
			dataname := datastore.DataString(descriptor)
			var subcommand string
			cmd.CommandArgs(3, &subcommand)
			if subcommand == "help" {
				typeservice, err := dataset.TypeServiceForData(dataname)
				if err != nil {
					return err
				}
				reply.Text = typeservice.Help()
				return nil
			}
			data, err := dataset.Data(dataname)
			if err != nil {
				return err
			}
			return data.DoRPC(cmd, reply)
		}

	default:
		return fmt.Errorf("Unknown command: '%s'", cmd)
	}
	return nil
}

// Adds new data to a dataset specified by a UUID string.
func newData(uuidStr, typename, dataname string, versioned bool) error {
	dataset, uuid, err := runningService.DatasetFromString(uuidStr)
	if err != nil {
		return err
	}
	config := dvid.Config{"versioned": versioned}
	return dataset.NewData(uuid, datastore.DataString(dataname), typename, config)
}
