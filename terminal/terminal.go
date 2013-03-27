package terminal

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

const helpMessage = `
DVID Terminal Help

	Use 'q' or 'quit' to exit.

	address [DVID rpc address]  - sets or returns DVID server address
	version [UUID] - sets or returns the UUID for the image version
`

// Global variables to store DVID states and connection info
var version string
var rpcAddress string
var client *rpc.Client

func init() {
	rpcAddress = server.DefaultRpcAddress
}

func prompt(message string) dvid.Command {
	fmt.Print(message)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return dvid.Command(strings.Split(line, " "))
}

// Shell takes commands and processes them in an endless loop.
func Shell() {
	fmt.Printf("\nDVID %s Terminal\n\n", datastore.Version)

	// Command-line loop
	takeCommands := true
	for takeCommands {
		cmd := prompt("DVID> ")
		switch cmd.Name() {
		case "":
			fmt.Println("Enter 'help' to see commands")
		case "help", "h":
			fmt.Printf(helpMessage)
			Send(dvid.Command([]string{"help"}))
		case "quit", "q":
			takeCommands = false
		case "address":
			if len(cmd) > 1 {
				cmd.CommandArgs(1, &rpcAddress)
				fmt.Printf("Set rpc address to %s\n", rpcAddress)
			} else {
				fmt.Printf("Current rpc address: %s\n", rpcAddress)
			}
		case "version":
			if len(cmd) > 1 {
				cmd.CommandArgs(1, &version)
				fmt.Printf("Set version to %s\n", version)
			} else {
				fmt.Printf("Current version: %s\n", version)
			}
		default:
			err := Send(cmd)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
}

func Send(cmd dvid.Command) (err error) {
	// Change rpc if encoded in command
	address, rpcOverriden := cmd.Parameter(dvid.KeyRpc)
	if rpcOverriden {
		rpcAddress = address
	}

	// If we haven't already established a connection, make one.
	if client == nil {
		client, err = rpc.DialHTTP("tcp", rpcAddress)
		if err != nil {
			client = nil  // Close connection if any error and try serverless mode.
		}
	}

	// Fulfill command either by remote or local call.
	var reply datastore.Response
	request := datastore.Request{Command: cmd}
	if client != nil {
		fmt.Printf("Issuing RPC command %s...\n", cmd)
		err = client.Call("RpcConnection.Do", request, &reply)
		if err != nil {
			err = fmt.Errorf("RPC error for '%s': %s", cmd, err.Error())
			return
		}
	} else {
		fmt.Printf("Issuing local command %s...\n", cmd)
		err = server.ServerlessDo(request, &reply)
		if err != nil {
			err = fmt.Errorf("Error for '%s': %s", cmd, err.Error())
			return
		}
	}
	fmt.Println(reply.Text)
	return
}
