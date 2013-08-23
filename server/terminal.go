package server

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

const shellHelp = `
DVID Terminal Help

	Use 'q' or 'quit' to exit.

	version [UUID] - sets or returns the UUID for the image version
`

func prompt(message string) dvid.Command {
	fmt.Print(message)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return dvid.Command(strings.Split(line, " "))
}

// Terminal provides a stateful client for DVID interaction.  Unlike using
// DVID commands from the shell, terminal use keeps several DVID values
// (e.g., rpc address, image version UUID) in memory and provides them
// automatically to the DVID server.
type Terminal struct {
	datastoreDir string
	rpcAddress   string
	version      string
	client       *rpc.Client
}

// NewTerminal returns a terminal with an RPC connection to the given
// rpcAddress.
func NewTerminal(datastoreDir, rpcAddress string) *Terminal {
	client, err := rpc.DialHTTP("tcp", rpcAddress)
	if err != nil {
		fmt.Printf("Did not find DVID server for RPC at %s: %s\n", rpcAddress, err.Error())
		client = nil // Close connection if any error and try serverless mode.
	} else {
		dvid.Fmt(dvid.Debug, "Found DVID server for RPC at %s\n", rpcAddress)
	}
	return &Terminal{
		datastoreDir: datastoreDir,
		rpcAddress:   rpcAddress,
		client:       client,
	}
}

// Shell takes commands and processes them in an endless loop.
func (terminal *Terminal) Shell() {
	fmt.Printf("\nDVID %s Terminal\n\n", datastore.Version)

	// Command-line loop
	takeCommands := true
	for takeCommands {
		cmd := prompt("DVID> ")
		switch cmd.Name() {
		case "":
			fmt.Println("Enter 'help' to see commands")
		case "help", "h":
			fmt.Printf(shellHelp)
			terminal.Send(dvid.Command([]string{"help"}))
		case "quit", "q":
			takeCommands = false
		case "version":
			if len(cmd) > 1 {
				cmd.CommandArgs(1, &(terminal.version))
				fmt.Printf("Set version to %s\n", terminal.version)
			} else {
				fmt.Printf("Current version: %s\n", terminal.version)
			}
		default:
			err := terminal.Send(cmd)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
}

// Send transmits an RPC command if a server is available or else it
// runs the command in serverless mode.
func (terminal *Terminal) Send(cmd dvid.Command) (err error) {
	var reply datastore.Response
	request := datastore.Request{Command: cmd}
	if terminal.client != nil {
		dvid.Fmt(dvid.Debug, "Running remote command '%s'...\n", cmd)
		err = terminal.client.Call("RPCConnection.Do", request, &reply)
		if err != nil {
			err = fmt.Errorf("RPC error for '%s': %s", cmd, err.Error())
			return
		}
	} else {
		dvid.Fmt(dvid.Debug, "Running local command '%s'...\n", cmd)
		err = ServerlessDo(terminal.datastoreDir, request, &reply)
		if err != nil {
			err = fmt.Errorf("Error for '%s': %s", cmd, err.Error())
			return
		}
	}
	if reply.Text != "" {
		fmt.Println(reply.Text)
	}
	return
}
