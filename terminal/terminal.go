package terminal

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/server"
)

const helpMessage = `
DVID Terminal Help

  set address <DVID rpc address>
  set node <UUID>
`

// Global variables to store DVID states and connection info
var head datastore.UUID
var rpcAddress string = server.DefaultRpcAddress

func prompt(message string) (in string) {
	fmt.Print(message)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	in = strings.TrimSpace(line)
	return
}

// Shell takes commands and processes them in an endless loop.
func Shell() {
	fmt.Printf("\nDVID %s Terminal\n\n", datastore.Version)

	// Command-line loop
	takeCommands := true
	for takeCommands {
		command := prompt("DVID> ")
		switch command {
		case "help", "h":
			fmt.Printf(helpMessage)
		case "quit", "q":
			takeCommands = false
		default:
			fmt.Println("\nUnable to execute command!\n")
			fmt.Printf(helpMessage)
		}
	}
}
