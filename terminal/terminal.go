package terminal

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/command"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/server"
)

const helpMessage = `
DVID Terminal Help

	Use 'q' or 'quit' to exit.

	set address <DVID rpc address>
	set node <UUID>
`

// Global variables to store DVID states and connection info
var head datastore.UUID
var rpcAddress string
var client *rpc.Client

func init() {
	rpcAddress = server.DefaultRpcAddress
}

func prompt(message string) *command.Command {
	fmt.Print(message)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return &command.Command{strings.Split(line, " ")}
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
			Send(&command.Command{Args: []string{"help"}})
		case "quit", "q":
			takeCommands = false
		default:
			err := Send(cmd)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
}

func Send(cmd *command.Command) (err error) {
	// Change rpc if encoded in command
	address, rpcOverriden := cmd.GetSetting(command.KeyRpc)
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
	var input, reply command.Packet
	serverCmd := server.Command{*cmd, input}
	err = client.Call("RpcConnection.Do", &serverCmd, &reply)
	if err != nil {
		err = fmt.Errorf("RPC error for '%s': %s", cmd, err.Error())
		return
	}
	fmt.Println(reply.Text)
	return
}
