/*
	This file handles RPC connections, usually from DVID clients.
	TODO: Remove all command-line commands aside from the most basic ones, and
	   force use of the HTTP API.  Curl can be used from command line.
*/

package server

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
)

const RPCHelpMessage = `Commands executed on the server (rpc address = %s):

	help
	shutdown

	repos new  <alias> <description>

	repo <UUID> new <datatype name> <data name> <datatype-specific config>...

	node <UUID> <data name> <type-specific commands>

For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

// Client provides RPC access to a DVID server.
type Client struct {
	rpcAddress string
	client     *rpc.Client
}

// NewClient returns an RPC client to the given address.
func NewClient(rpcAddress string) (*Client, error) {
	client, err := rpc.DialHTTP("tcp", rpcAddress)
	if err != nil {
		return nil, fmt.Errorf("Did not find DVID server for RPC at %s [%s]\n", rpcAddress, err.Error())
	}
	return &Client{rpcAddress, client}, nil
}

// Send transmits an RPC command if a server is available.
func (c *Client) Send(request datastore.Request) error {
	var reply datastore.Response
	if c.client != nil {
		err := c.client.Call("RPCConnection.Do", request, &reply)
		if err != nil {
			return fmt.Errorf("RPC error for '%s': %s", request.Command, err.Error())
		}
	} else {
		reply.Output = []byte(fmt.Sprintf("No DVID server is available: %s\n", request.Command))
	}
	return reply.Write(os.Stdout)
}

// RPCConnection will export all of its functions for rpc access.
type RPCConnection struct {
	message.RPCConnection
}

// Do acts as a switchboard for remote command execution
func (c *RPCConnection) Do(cmd datastore.Request, reply *datastore.Response) error {
	if reply == nil {
		dvid.Debugf("reply is nil coming in!\n")
		return nil
	}
	if cmd.Name() == "" {
		return fmt.Errorf("Server error: got empty command!")
	}

	switch cmd.Name() {

	case "help":
		reply.Text = fmt.Sprintf(RPCHelpMessage, config.RPCAddress(), config.HTTPAddress())

	case "shutdown":
		Shutdown()
		// Make this process shutdown in a second to allow time for RPC to finish.
		// TODO -- Better way to do this?
		log.Printf("DVID server halted due to 'shutdown' command.")
		reply.Text = fmt.Sprintf("DVID server at %s has been halted.\n", config.RPCAddress())
		go func() {
			time.Sleep(1 * time.Second)
			os.Exit(0)
		}()

	case "types":
		if len(cmd.Command) == 1 {
			text := "\nData Types within this DVID Server\n"
			text += "----------------------------------\n"
			mapTypes, err := datastore.Types()
			if err != nil {
				return fmt.Errorf("Error trying to retrieve data types within this DVID server!")
			}
			for url, typeservice := range mapTypes {
				text += fmt.Sprintf("%-20s %s\n", typeservice.GetTypeName(), url)
			}
			reply.Text = text
		} else {
			if len(cmd.Command) != 3 || cmd.Command[2] != "help" {
				return fmt.Errorf("Unknown types command: %q", cmd.Command)
			}
			var typename string
			cmd.CommandArgs(1, &typename)
			typeservice, err := datastore.TypeServiceByName(dvid.TypeString(typename))
			if err != nil {
				return err
			}
			reply.Text = typeservice.Help()
		}

	case "repos":
		var subcommand, alias, description string
		cmd.CommandArgs(1, &subcommand, &alias, &description)
		switch subcommand {
		case "new":
			repo, err := datastore.NewRepo(alias, description)
			if err != nil {
				return err
			}
			if err := repo.SetAlias(alias); err != nil {
				return err
			}
			if err := repo.SetDescription(description); err != nil {
				return err
			}
			reply.Text = fmt.Sprintf("New repo %q created with head node %s\n", alias, repo.RootUUID())
		default:
			return fmt.Errorf("Unknown repos command: %q", subcommand)
		}

	case "repo":
		var uuidStr, subcommand string
		cmd.CommandArgs(1, &uuidStr, &subcommand)
		uuid, _, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}
		// Get Repo
		repo, err := datastore.RepoFromUUID(uuid)
		if err != nil {
			return err
		}

		switch subcommand {
		case "new":
			var typename, dataname string
			cmd.CommandArgs(3, &typename, &dataname)

			// Get TypeService
			typeservice, err := datastore.TypeServiceByName(dvid.TypeString(typename))
			if err != nil {
				return err
			}

			// Create new data
			config := cmd.Settings()
			_, err = repo.NewData(typeservice, dvid.InstanceName(dataname), config)
			if err != nil {
				return err
			}
			reply.Text = fmt.Sprintf("Data %q [%s] added to node %s\n", dataname, typename, uuid)
			repo.AddToRepoLog(cmd.String())
		case "push":
			/*
				var target string
				cmd.CommandArgs(3, &target)
				config := cmd.Settings()
				if err = datastore.Push(repo, target, config); err != nil {
					return err
				}
				reply.Text = fmt.Sprintf("Repo %q pushed to %q\n", repo.RootUUID(), target)
			*/
			return fmt.Errorf("push command has been temporarily suspended")
		default:
			return fmt.Errorf("Unknown command: %q", cmd)
		}

	case "node":
		var uuidStr, descriptor string
		cmd.CommandArgs(1, &uuidStr, &descriptor)
		uuid, _, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}

		// Get Repo
		repo, err := datastore.RepoFromUUID(uuid)
		if err != nil {
			return err
		}

		// Get the DataService
		dataname := dvid.InstanceName(descriptor)
		var subcommand string
		cmd.CommandArgs(3, &subcommand)
		dataservice, err := repo.GetDataByName(dataname)
		if err != nil {
			return err
		}
		if subcommand == "help" {
			reply.Text = dataservice.Help()
			return nil
		}
		return dataservice.DoRPC(cmd, reply)

	default:
		return fmt.Errorf("Unknown command: '%s'", cmd)
	}
	return nil
}
