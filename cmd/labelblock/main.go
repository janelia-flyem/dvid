package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

var (
	compression = flag.String("compress", "", "")

	// Display usage if true.
	showHelp = flag.Bool("help", false, "")
)

const helpMessage = `
labelblock compresses and decompresses label array data.   If -compress=NX,NY,NZ is used,
the program accepts a packed array of little-endian uint64 in Z, Y, and then X order, where
dimensions of input array is NX x NY x NZ.  The output of compression is the serialization
of a DVID compressed label block.  If no -compress flag is given, program expects stdin to 
be serialized DVID compressed label block.

Usage: labelblock [options]

	If -compress is not specified giving block (and input) dimensions, we assume decompression
	of the stdin stream is requested.

	-compress       =string   Dimensions ("NX,NY,NZ") of little-endian packed array of uint64.
	-h, -help       (flag)    Show help message
`

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = func() {
		fmt.Printf(helpMessage)
	}
	flag.Parse()

	if *showHelp || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(0)
	}

	b, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		os.Exit(1)
	}
	if len(*compression) != 0 {
		var nx, ny, nz int32
		if n, err := fmt.Sscanf(*compression, "%d,%d,%d", &nx, &ny, &nz); n != 3 || err != nil {
			fmt.Fprintf(os.Stderr, "Could not interpret block size, should be -compress=nx,ny,nz: %v\n", err)
			os.Exit(1)
		}
		compress(b, dvid.Point3d{nx, ny, nz})
	} else {
		uncompress(b)
	}
}

func compress(b []byte, bsize dvid.Point3d) {
	if len(b) != int(bsize.Prod()*8) {
		fmt.Fprintf(os.Stderr, "Bad input.  Expected %d bytes, got %d bytes\n", bsize.Prod()*8, len(b))
		os.Exit(1)
	}
	block, err := labels.MakeBlock(b, bsize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create Block from uint64 array: %v\n", err)
		os.Exit(1)
	}
	serialization, err := block.MarshalBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to serialize Block: %v\n", err)
		os.Exit(1)
	}
	writeOut(serialization)
}

func uncompress(b []byte) {
	var block labels.Block

	if err := block.UnmarshalBinary(b); err != nil {
		fmt.Fprintf(os.Stderr, "Error trying to deserialize supplied bytes: %v\n", err)
		os.Exit(1)
	}
	uint64array, _ := block.MakeLabelVolume()
	writeOut(uint64array)
}

func writeOut(b []byte) {
	bytesIn := len(b)
	var n, bytesOut int
	var err error
	for {
		if n, err = os.Stdout.Write(b); err != nil {
			fmt.Fprintf(os.Stderr, "Error trying to write bytes to stdout: %v\n", err)
			os.Exit(1)
		}
		bytesOut += n
		if bytesOut == bytesIn {
			break
		}
	}
}
