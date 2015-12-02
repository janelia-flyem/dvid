package main

import (
	"flag"
	"fmt"
	"math"
	"os"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Run in verbose mode if true.
	runVerbose = flag.Bool("verbose", false, "")

	dryrun = flag.Bool("dryrun", false, "")

	outdir = flag.String("outdir", "", "")
	url    = flag.String("url", "", "")

	minz = flag.Int("minz", 0, "")
	maxz = flag.Int("maxz", math.MaxInt32, "")

	compression = flag.String("compress", "gzip", "")

	roi = flag.String("roi", "", "")
)

const helpMessage = `
dvid-transfer copies label data from one DVID server to either a set of optionally compressed
files or to another DVID server using HTTP API calls.

Usage: dvid-transfer [options] host uuid name

  where host = URL for the DVID server, e.g., http://host:7000
        uuid = the UUID with sufficient characters to distinguish version
        name = name of the label data instance 

  Either the -outdir or -url option must be present.

	-outdir         =string   Output directory for file output
	-url            =string   POST URL for DVID, e.g., "http://dvidserver.com/api/653/dataname"

	-compress       =string   Compression for output files.  default "gzip" but allows "none".

	-roi            =string   Instance name of ROI to delimit labels saved

	-minz           =number   Starting Z slice to process.
	-maxz           =number   Ending Z slice to process.

	-dryrun         (flag)    Don't write files or send POST requests to DVID
	-verbose    (flag)    Run in verbose mode.
	-h, -help   (flag)    Show help message
`

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = func() {
		fmt.Printf(helpMessage)
	}
	flag.Parse()

	if *showHelp || flag.NArg() != 3 {
		flag.Usage()
		os.Exit(0)
	}

	if *url == "" && *outdir == "" {
		fmt.Printf("Must either use -url and/or -outdir for output!\n")
		os.Exit(1)
	}

	host := flag.Args()[0]
	uuid := flag.Args()[1]
	name := flag.Args()[2]

	transferData(host, uuid, name)
}
