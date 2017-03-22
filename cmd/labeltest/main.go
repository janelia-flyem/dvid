// Runs sparse volume download tests.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")
)

const helpMessage = `

labeltest runs a benchmark for sparse volume GETs.

Usage: labeltest [options] <get URL before label> <label 1> <label 2> ... <label N>

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
	help := strings.ToLower(args[0]) == "help"

	if help && flag.NArg() == 1 {
		*showHelp = true
	}
	if *showHelp || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	postURL := args[0]
	numLabels := len(args) - 1
	labels := make([]uint64, numLabels)
	var err error
	for i := 0; i < numLabels; i++ {
		labels[i], err = strconv.ParseUint(args[i+1], 10, 64)
		if err != nil {
			fmt.Printf("error parsing label %q\n", args[i+1])
			os.Exit(1)
		}
	}

	// Fetch each label's sparse volume and time it.
	for _, label := range labels {
		start := time.Now()
		url := fmt.Sprintf("%s/sparsevol/%d", postURL, label)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("error on GET of %q: %v\n", url, err)
			os.Exit(1)
		}
		fmt.Printf("Received status %d, sparsevol for label %d: %s\n", resp.StatusCode, label, time.Since(start))
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("error on trying to read sparsevol %d response: %v\n", label, err)
			os.Exit(1)
		}
		fmt.Printf("   --> Read all %d bytes: %s (from request)\n", len(data), time.Since(start))
	}
}
