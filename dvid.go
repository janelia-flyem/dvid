package main

import (
	"flag"
	"fmt"
	_ "log"
	"os"
	_ "path/filepath"

	"github.com/janelia-flyem/dvid/cli"
	"github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
)

var showHelp bool
var showHelp2 bool

var httpPort int

const helpMessage = `
dvid is a distributed, versioned image datastore

usage: dvid [options] <command>

Use "serve" command to launch DVID server.  Then launch a web browser and
visit the main page ("localhost:4000" by default) and surf documentation.
`

func init() {
	flag.BoolVar(&showHelp, "h", false, "Show help message")
	flag.BoolVar(&showHelp2, "help", false, "Show help message")

	flag.IntVar(&httpPort, "port", 4000, "Default port number for server")
}

func main() {
	//  programName := filepath.Base(os.Args[0])
	flag.Parse()

	webAddr := fmt.Sprintf("localhost:%d", httpPort)

	if flag.NArg() >= 1 {
		command := flag.Arg(0)
		switch command {
		case "serve":
			server.ServeHttp(webAddr)
		case "init", "child", "add", "lock", "log", "clone":
			err := cli.DoCommand(flag.Args())
			if err != nil {
				showHelp = true
			}
		default:
			showHelp = true
		}
	} else {
		showHelp = true
	}

	if showHelp || showHelp2 {
		fmt.Println(helpMessage)
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
		os.Exit(1)
	}

}
