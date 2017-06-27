// Periodically ping dvid

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")
)

const helpMessage = `

ping periodically calls DVID as a heartbeat.

Usage: dvidping [options] <delay in seconds> <dvid url to ping>

  Example get URL: http://emdata2.int.janelia.org:7000/api/node/e2f02/labelarray

  -h, -help       (flag)    Show help message
`

var usage = func() {
	// Print local DVID help
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(0)
	}

	args := flag.Args()

	if *showHelp || flag.NArg() != 2 {
		flag.Usage()
		os.Exit(0)
	}

	pause, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Printf("error parsing pause time %q: %v\n", args[0], err)
		os.Exit(1)
	}
	dvidURL := args[1]

	for t := range time.Tick(time.Duration(pause) * time.Second) {
		resp, err := http.Get(dvidURL)
		if err != nil {
			fmt.Printf("%s: error on GET of %q: %v\n", t, dvidURL, err)
			os.Exit(1)
		}
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Bad response %s: %v\n", time.Now(), resp)
		}
	}
}
