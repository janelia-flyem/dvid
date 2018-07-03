package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const helpMessage = `

Store compressed label blocks that cover a given body into a file

Usage: body-blocks [options] <server> <uuid> <instance name> <label>

Example: body-blocks emdata3:8900 662ed segmentation 1539193374

      -filename    (string)  File to store retrieved block stream.  Default is body-<label>.dat
      -supervoxels (flag)    Store raw blocks without mapping to body labels.
      -verbose     (flag)    Run in verbose mode.
  -h, -help        (flag)    Show help message

`

var (
	showHelp    = flag.Bool("help", false, "Show help message")
	runVerbose  = flag.Bool("verbose", false, "Run in verbose mode")
	supervoxels = flag.Bool("supervoxels", false, "Access raw blocks without mapping to body labels")
	storefile   = flag.String("filename", "", "File to store retrieved block stream.  Default is body-<label>.dat")

	server, uuid, name string
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
	server = args[0]
	uuid = args[1]
	name = args[2]
	label, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		fmt.Printf("can't parse label %q: %v\n", args[3], err)
		os.Exit(1)
	}

	var filename string
	if *storefile != "" {
		filename = *storefile
	} else {
		filename = fmt.Sprintf("label-%d.dat", label)
	}
	f, err := os.Create(filename)
	if err != nil {
		fmt.Printf("error trying to create file %q: %v\n", filename, err)
		os.Exit(1)
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
	if err := idx.Unmarshal(data); err != nil {
		fmt.Printf("couldn't unmarshal index: %v\n", err)
	}
	fmt.Printf("\nLabel: %d\n", idx.Label)
	fmt.Printf("Last Mutation ID: %d\n", idx.LastMutId)
	fmt.Printf("Last Modification Time: %s\n", idx.LastModTime)
	fmt.Printf("Last Modification User: %s\n", idx.LastModUser)
	fmt.Printf("Last Modification App:  %s\n\n", idx.LastModApp)

	fmt.Printf("Total blocks: %d\n", len(idx.Blocks))
	var bcoords []dvid.ChunkPoint3d
	var blocksSkipped int
	for zyx, svc := range idx.Blocks {
		izyxStr := labels.BlockIndexToIZYXString(zyx)
		var numVoxels uint32
		for _, count := range svc.Counts {
			numVoxels += count
		}
		if numVoxels == 0 {
			fmt.Printf("\n-- ignoring block %s, 0 count: %v\n", izyxStr, svc.Counts)
			blocksSkipped++
		} else {
			bcoord, err := izyxStr.ToChunkPoint3d()
			if err != nil {
				fmt.Printf("bad coord: %v\n", err)
				os.Exit(1)
			}
			bcoords = append(bcoords, bcoord)
			if len(bcoords) == 50 {
				writeBlocks(f, bcoords)
				bcoords = nil
			}
		}
	}
	if len(bcoords) > 0 {
		writeBlocks(f, bcoords)
	}
	f.Close()
	fmt.Printf("Wrote %d of %d blocks to %s.\n", len(idx.Blocks)-blocksSkipped, len(idx.Blocks), filename)
}

func writeBlocks(f *os.File, bcoords []dvid.ChunkPoint3d) {
	strs := make([]string, len(bcoords))
	for i, bcoord := range bcoords {
		strs[i] = fmt.Sprintf("%d,%d,%d", bcoord[0], bcoord[1], bcoord[2])
	}
	bcoordStr := strings.Join(strs, ",")
	url := fmt.Sprintf("http://%s/api/node/%s/%s/specificblocks?supervoxels=%t&blocks=%s", server, uuid, name, *supervoxels, bcoordStr)
	resp, err := http.Get(url)
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("couldn't read block data: %v\n", err)
		os.Exit(1)
	}
	if len(data) == 0 {
		fmt.Printf("Read blocks returned no bytes: %s\n", bcoords)
		return
	}
	n, err := f.Write(data)
	if err != nil {
		fmt.Printf("error writing blocks: %v\n", err)
		os.Exit(1)
	}
	if n != len(data) {
		fmt.Printf("couldn't write %d bytes of data, only wrote %d!\n", len(data), n)
		os.Exit(1)
	}
}
