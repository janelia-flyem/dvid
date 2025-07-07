package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
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

	if len(*compression) != 0 {
		compress()
	} else {
		uncompress()
	}
}

func compress() {
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		os.Exit(1)
	}
	var nx, ny, nz int32
	if n, err := fmt.Sscanf(*compression, "%d,%d,%d", &nx, &ny, &nz); n != 3 || err != nil {
		fmt.Fprintf(os.Stderr, "Could not interpret block size, should be -compress=nx,ny,nz: %v\n", err)
		os.Exit(1)
	}
	bsize := dvid.Point3d{nx, ny, nz}
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
	zw := gzip.NewWriter(os.Stdout)
	if _, err = zw.Write(serialization); err != nil {
		fmt.Fprintf(os.Stderr, "unable to gzip serialization: %v\n", err)
		os.Exit(1)
	}
	zw.Flush()
	zw.Close()
}

func uncompress() {
	fz, err := gzip.NewReader(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to use gzip for reading: %v\n", err)
		os.Exit(1)
	}

	serialization, err := io.ReadAll(fz)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to uncompress gzip input: %v\n", err)
		os.Exit(1)
	}
	if err := fz.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error on close of gzip uncompression of input: %v\n", err)
		os.Exit(1)
	}

	var block labels.Block
	if err := block.UnmarshalBinary(serialization); err != nil {
		fmt.Fprintf(os.Stderr, "Error trying to deserialize supplied bytes: %v\n", err)
		os.Exit(1)
	}
	uint64array, _ := block.MakeLabelVolume()
	bytesIn := len(uint64array)
	var n, bytesOut int
	for {
		if n, err = os.Stdout.Write(uint64array); err != nil {
			fmt.Fprintf(os.Stderr, "Error trying to write bytes to stdout: %v\n", err)
			os.Exit(1)
		}
		bytesOut += n
		if bytesOut == bytesIn {
			break
		}
	}
}
