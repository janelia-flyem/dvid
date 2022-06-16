package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
)

const helpMessage = `

displays information on a label index using the DVID API

Usage: analyze-index [options] <server> <uuid> <instance name> <label>

      -blocks      (string)  Blocks in x0,y0,z0,x1,y1,z1,... format
      -verbose     (flag)    Run in verbose mode.
  -h, -help        (flag)    Show help message

`

var (
	showHelp    = flag.Bool("help", false, "Show help message")
	runVerbose  = flag.Bool("verbose", false, "Run in verbose mode")
	blockcoords = flag.String("blocks", "", "List of block coordinates in x0,y0,z0,x1,y1,z1,... format")
)

var usage = func() {
	fmt.Printf(helpMessage)
}

func atoi(s string) int32 {
	i, err := strconv.Atoi(s)
	if err != nil {
		fmt.Printf("can't parse coordinate %q: %v\n", s, err)
		os.Exit(1)
	}
	return int32(i)
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 4 || *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	args := flag.Args()
	server := args[0]
	uuid := args[1]
	name := args[2]
	label, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		fmt.Printf("can't parse label %q: %v\n", args[3], err)
		os.Exit(1)
	}

	blocks := make(map[uint64]struct{})
	if *blockcoords != "" {
		coordarray := strings.Split(*blockcoords, ",")
		if len(coordarray)%3 != 0 {
			fmt.Printf("block coordinates should be three coordinates per block, got %v\n", blockcoords)
			os.Exit(1)
		}
		numBlocks := len(coordarray) / 3
		n := 0
		for i := 0; i < numBlocks; i++ {
			zyx := labels.EncodeBlockIndex(atoi(coordarray[n]), atoi(coordarray[n+1]), atoi(coordarray[n+2]))
			blocks[zyx] = struct{}{}
			n += 3
		}
	}

	url := fmt.Sprintf("http://%s/api/node/%s/%s/index/%d", server, uuid, name, label)
	resp, err := http.Get(url)
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("couldn't read index data: %v\n", err)
		os.Exit(1)
	}
	if len(data) == 0 {
		fmt.Printf("Read label index %d returned no bytes\n", label)
		os.Exit(1)
	}
	idx := new(labels.Index)
	if err := pb.Unmarshal(data, idx); err != nil {
		fmt.Printf("couldn't unmarshal index: %v\n", err)
	}
	fmt.Printf("\nLabel: %d\n", idx.Label)
	fmt.Printf("Last Mutation ID: %d\n", idx.LastMutId)
	fmt.Printf("Last Modification Time: %s\n", idx.LastModTime)
	fmt.Printf("Last Modification User: %s\n", idx.LastModUser)
	fmt.Printf("Last Modification App:  %s\n\n", idx.LastModApp)

	fmt.Printf("Total blocks: %d\n", len(idx.Blocks))
	for zyx, svc := range idx.Blocks {
		if len(blocks) > 0 {
			_, found := blocks[zyx]
			if !found {
				continue
			}
		}
		izyxStr := labels.BlockIndexToIZYXString(zyx)
		fmt.Printf("Block %s:\n", izyxStr)
		for sv, count := range svc.Counts {
			fmt.Printf("  Supervoxel %10d: %d voxels\n", sv, count)
		}
		fmt.Printf("\n")
	}
}
