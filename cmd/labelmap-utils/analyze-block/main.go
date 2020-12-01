package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const helpMessage = `

displays information on a compressed label block and one coordinate using the DVID API

Usage: analyze-block [options] <server> <uuid> <instance name> <voxel x> <voxel y> <voxel z>

	  -blockwidth  (int)     Size of blocks in each dimension, default 64
	  -scale       (int) 	 A number from 0 up to MaxDownresLevel where each level has 1/2
	      					   resolution of previous level. Level 0 (default) is highest res.
      -supervoxels (flag)    Access raw blocks without mapping to body labels.
      -verbose     (flag)    Run in verbose mode, e.g., shows sub-block data.
  -h, -help        (flag)    Show help message

`

var (
	showHelp    = flag.Bool("help", false, "Show help message")
	runVerbose  = flag.Bool("verbose", false, "Run in verbose mode")
	supervoxels = flag.Bool("supervoxels", false, "Access raw blocks without mapping to body labels")
	blockwidth  = flag.Int("blockwidth", 64, "Size of blocks in each dimension, default 64")
	scale       = flag.Int("scale", 0, "Scale from 0 (highest res) to larger scales")
)

var usage = func() {
	fmt.Printf(helpMessage)
}

func getCoord(s string) int32 {
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

	if flag.NArg() != 6 || *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	args := flag.Args()
	server := args[0]
	uuid := args[1]
	name := args[2]

	var coord dvid.Point3d
	coord[0] = getCoord(args[3])
	coord[1] = getCoord(args[4])
	coord[2] = getCoord(args[5])

	width := int32(*blockwidth)
	blockSize := dvid.Point3d{width, width, width}
	bcoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	// offset := dvid.Point3d{bcoord[0] * width, bcoord[1] * width, bcoord[2] * width}
	// url := fmt.Sprintf("http://emdata1:8900/api/node/fa6b/segmentation/blocks/64_64_64/%d_%d_%d?compression=blocks", offset[0], offset[1], offset[2])

	url := fmt.Sprintf("http://%s/api/node/%s/%s/specificblocks?supervoxels=%t&blocks=%d,%d,%d", server, uuid, name, *supervoxels, bcoord[0], bcoord[1], bcoord[2])
	if *scale != 0 {
		url += fmt.Sprintf("&scale=%d", *scale)
	}
	resp, err := http.Get(url)
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("couldn't read block data: %v\n", err)
		os.Exit(1)
	}
	if len(data) == 0 {
		fmt.Printf("Read block %s returned no bytes\n", bcoord)
		os.Exit(1)
	}

	var b int
	x := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	y := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	z := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	n := int(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	fmt.Printf("Received block (%d, %d, %d), %d bytes\n", x, y, z, n)

	var uncompressed []byte
	gzipIn := bytes.NewBuffer(data[b : b+n])
	zr, err := gzip.NewReader(gzipIn)
	if err != nil {
		fmt.Printf("can't uncompress gzip block: %v\n", err)
		os.Exit(1)
	}
	uncompressed, err = ioutil.ReadAll(zr)
	if err != nil {
		fmt.Printf("can't uncompress gzip block: %v\n", err)
		os.Exit(1)
	}
	zr.Close()

	var block labels.Block
	if err = block.UnmarshalBinary(uncompressed); err != nil {
		fmt.Printf("unable to deserialize label block (%d, %d, %d): %v\n", x, y, z, err)
		os.Exit(1)
	}
	fmt.Printf("%s\n", block.StringDump(*runVerbose))
}
