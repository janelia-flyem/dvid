package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	_ "path/filepath"

	"dvid/cli"
	"dvid/server"
)

var options cli.Options
var foo string

var showHelp bool
var showHelp2 bool

var httpPort int

const helpMessage = `
dvid is a distributed, versioned image datastore

usage: dvid [options] <command>
` + cli.HelpMessage + `
Utility commands:

  serve                     Launch HTTP server for datastore.
`

func init() {
	flag.BoolVar(&showHelp, "h", false, "Show help message")
	flag.BoolVar(&showHelp2, "help", false, "Show help message")

	flag.IntVar(&httpPort, "port", 4000, "Default port number for server")

	flag.IntVar(&options.OriginX, "x", 0, "X-coordinate in datastore space")
	flag.IntVar(&options.OriginY, "y", 0, "Y-coordinate in datastore space")
	flag.IntVar(&options.OriginZ, "z", 0, "Z-coordinate in datastore space")

	currentDir, err := os.Getwd()
	options.Directory = currentDir
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
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
		case "init", "add", "commit", "log":
			cli.DoCommand(flag.Args(), options)
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
