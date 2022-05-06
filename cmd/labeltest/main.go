// Runs sparse volume download tests.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Wrote sorted RLEs to output
	writeRLEs = flag.Bool("rle", false, "")
)

const helpMessage = `

labeltest runs a benchmark for sparse volume GETs.

Usage: labeltest [options] <get URL before label> <label 1> <label 2> ... <label N>

  Example get URL: http://emdata2.int.janelia.org:7000/api/node/e2f02/labelarray

  -h, -help       (flag)    Show help message
  -r, -rle        (flag)    Output RLEs in sorted format
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
	flag.BoolVar(writeRLEs, "r", false, "Output RLEs in sorted format")
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
		if resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		data, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("error on trying to read sparsevol %d response: %v\n", label, err)
			os.Exit(1)
		}
		fmt.Printf("   --> Read all %d bytes: %s (from request)\n", len(data), time.Since(start))

		if *writeRLEs {
			if len(data) < 12 {
				fmt.Printf("Could not parse return data.  Exiting\n")
				os.Exit(1)
			}
			numRuns := binary.LittleEndian.Uint32(data[8:12])
			fmt.Printf("Number of runs: %d\n", numRuns)
			rles := make(dvid.RLEs, numRuns)
			var numVoxels int64
			for i := uint32(0); i < numRuns; i++ {
				var rle dvid.RLE
				if err := rle.UnmarshalBinary(data[12+i*16 : 28+i*16]); err != nil {
					fmt.Printf("error trying to unmarshal span %d: %v\n", i, err)
					os.Exit(1)
				}
				rles[i] = rle
				numVoxels += int64(rle.Length())
			}
			fmt.Printf("Number of voxels: %d\n", numVoxels)
			sort.Sort(rles)
			for i, rle := range rles {
				fmt.Printf("Span %5d: %s\n", i, rle)
			}
		}
	}
}
