package terminal

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"time"

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
			err = fmt.Errorf("Could not establish rpc connection: %s", err.Error())
			return
		}
	}

	// Send command to server synchronously
	startTime := time.Now()
	var reply dvid.Response
	err = client.Call("RpcConnection.Do", &cmd, &reply)
	if err != nil {
		err = fmt.Errorf("RPC error for '%s': %s", cmd, err.Error())
		return
	}
	fmt.Printf("%s [OK Response]: %s\n", cmd.Name(), time.Since(startTime))
	fmt.Println(reply.Text())
	return
}
