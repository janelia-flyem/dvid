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
      -supervoxels (flag)    Access raw blocks without mapping to body labels.
      -verbose     (flag)    Run in verbose mode, e.g., shows sub-block data.
  -h, -help        (flag)    Show help message

`

var (
	showHelp    = flag.Bool("help", false, "Show help message")
	runVerbose  = flag.Bool("verbose", false, "Run in verbose mode")
	supervoxels = flag.Bool("supervoxels", false, "Access raw blocks without mapping to body labels")
	blockwidth  = flag.Int("blockwidth", 64, "Size of blocks in each dimension, default 64")
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
	uint64array, size := block.MakeLabelVolume()
	if !size.Equals(blockSize) {
		fmt.Printf("got bad block size from deserialized block (%d, %d, %d): %s\n", x, y, z, size)
		os.Exit(1)
	}
	ptInBlock := coord.Point3dInChunk(blockSize)
	i := (ptInBlock[2]*width*width + ptInBlock[1]*width + ptInBlock[0]) * 8
	label := binary.LittleEndian.Uint64(uint64array[i : i+8])
	fmt.Printf("Coord %s, pt in block %s = label %d by direct lookup after inflation\n", coord, ptInBlock, label)
	fmt.Printf("Coord %s, pt in block %s = label %d by using block.Value()\n", coord, ptInBlock, block.Value(ptInBlock))

	counts := make(map[uint64]int, len(block.Labels))
	for i := 0; i < len(uint64array); i += 8 {
		label := binary.LittleEndian.Uint64(uint64array[i : i+8])
		counts[label]++
	}
	for _, label := range block.Labels {
		fmt.Printf("Label %10d: %d voxels\n", label, counts[label])
	}
	fmt.Printf("Block Size: %s\n", block.Size)

	subBlocksPerDim := int(width) / labels.SubBlockSize
	numSubBlocks := subBlocksPerDim * subBlocksPerDim * subBlocksPerDim
	if len(block.NumSBLabels) != numSubBlocks {
		fmt.Printf("Expected %d entries for number of sub-block indices but got %d instead!\n", numSubBlocks, len(block.NumSBLabels))
		os.Exit(1)
	}
	if !*runVerbose {
		os.Exit(0)
	}
	i = 0
	sb := 0
	for z := 0; z < subBlocksPerDim; z++ {
		z0 := z * labels.SubBlockSize
		z1 := z0 + labels.SubBlockSize - 1
		for y := 0; y < subBlocksPerDim; y++ {
			y0 := y * labels.SubBlockSize
			y1 := y0 + labels.SubBlockSize - 1
			for x := 0; x < subBlocksPerDim; x++ {
				x0 := x * labels.SubBlockSize
				x1 := x0 + labels.SubBlockSize - 1
				fmt.Printf("(%2d-%2d, %2d-%2d, %2d-%2d) ", x0, x1, y0, y1, z0, z1)
				for n := 0; n < int(block.NumSBLabels[sb]); n++ {
					index := block.SBIndices[i]
					label := block.Labels[index]
					fmt.Printf("%d [%d]  ", block.SBIndices[i], label)
					i++
				}
				fmt.Printf("\n")
				sb++
			}
		}
	}
}
