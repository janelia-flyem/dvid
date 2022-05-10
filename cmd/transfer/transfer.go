package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
)

type BaseMetadata struct {
	TypeName    string
	TypeURL     string
	TypeVersion string
	Name        string
	RepoUUID    string
	Compression string
	Checksum    string
	Persistence string
	Versioned   bool
}

type Metadata struct {
	Base     BaseMetadata
	Extended interface{}
}

func getLabelMetadata(dstURL string) *LabelMetadata {
	infoUrl := dstURL + "/info"
	resp, err := http.Get(infoUrl)
	if err != nil {
		fmt.Printf("Error on getting metadata (%s): %v\n", infoUrl, err.Error())
		os.Exit(1)
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Bad status on getting metadata (%s): %d\n", infoUrl, resp.StatusCode)
		os.Exit(1)
	}
	metadata, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		fmt.Printf("Could not read metadata from labels (%s): %v\n", infoUrl, err.Error())
		os.Exit(1)
	}
	m := new(LabelMetadata)
	if err := json.Unmarshal(metadata, m); err != nil {
		fmt.Printf("Error parsing metadata from labels: %s\n", err.Error())
		os.Exit(1)
	}
	return m
}

type LabelMetadata struct {
	Base     BaseMetadata
	Extended struct {
		BlockSize [3]int
		MinPoint  [3]int
		MaxPoint  [3]int
		MinIndex  [3]int
		MaxIndex  [3]int
	}
}

func transferData(host, uuid, name string) {
	// If we are transferring to another DVID host, verify the destination.
	var dstMetadata *LabelMetadata
	if *url != "" {
		dstMetadata = getLabelMetadata(*url)
		dsttype := dstMetadata.Base.TypeName

		switch dsttype {
		case "labelblk":
			fmt.Printf("-- verified that POST URL (%v) is labelblk.\n", *url)
		case "labelarray":
			fmt.Printf("-- verified that POST URL (%v) is labelarray.\n", *url)
		default:
			fmt.Printf("POST URL (%v) must be labelblk but is %s data type\n", *url, dsttype)
			os.Exit(1)
		}
	}

	// Get the source metadata
	srcURL := fmt.Sprintf("%s/api/node/%s/%s", host, uuid, name)
	srcMetadata := getLabelMetadata(srcURL)
	if srcMetadata.Base.TypeName != "labelblk" && srcMetadata.Base.TypeName != "labelarray" {
		fmt.Printf("Source data instance %q is not labelblk or labelarray type.  Aborting.\n", name)
		os.Exit(1)
	}

	// If we are writing files to output directory, make sure it's created.
	// Make sure output directory exists if it's specified.
	if *outdir != "" {
		if fileinfo, err := os.Stat(*outdir); os.IsNotExist(err) {
			fmt.Printf("Creating output directory: %s\n", *outdir)
			err := os.MkdirAll(*outdir, 0744)
			if err != nil {
				fmt.Printf("Can't make output directory: %s\n", err.Error())
				os.Exit(1)
			}
		} else if !fileinfo.IsDir() {
			fmt.Printf("Supplied output path (%s) is not a directory.", *outdir)
			os.Exit(1)
		}
	}

	sendLabels(srcMetadata, dstMetadata, name, srcURL)
}

func sendLabels(src, dst *LabelMetadata, name, srcURL string) {
	var fastForward bool
	var ffx, ffy, ffz int
	if *start != "" {
		if _, err := fmt.Sscanf(*start, "%d,%d,%d", &ffx, &ffy, &ffz); err != nil {
			fmt.Printf("Can't parse -start coordinate %q: %v\n", *start, err)
			os.Exit(1)
		}
		fastForward = true
	}
	minIndex := src.Extended.MinIndex
	maxIndex := src.Extended.MaxIndex
	srcBlock := src.Extended.BlockSize
	if srcBlock[0] != srcBlock[1] {
		fmt.Printf("Can't handle non-cubic block sizes as in %q data instance: %v\n", name, srcBlock)
		os.Exit(1)
	}
	if minIndex[0] == 0 && maxIndex[0] == 0 {
		fmt.Printf("min/max indices seem broken.  computing using min/max point...\n")
		if src.Extended.MaxPoint[0] == 0 && src.Extended.MaxPoint[1] == 0 && src.Extended.MaxPoint[2] == 0 {
			fmt.Printf("min point, max point %v unable to be converted to min/max indices\n", src.Extended.MaxPoint)
			os.Exit(1)
		}
		minIndex[0] = src.Extended.MinPoint[0] / srcBlock[0]
		minIndex[1] = src.Extended.MinPoint[1] / srcBlock[1]
		minIndex[2] = src.Extended.MinPoint[2] / srcBlock[2]
		maxIndex[0] = src.Extended.MaxPoint[0] / srcBlock[0]
		maxIndex[1] = src.Extended.MaxPoint[1] / srcBlock[1]
		maxIndex[2] = src.Extended.MaxPoint[2] / srcBlock[2]
	}
	fmt.Printf("MinIndex: %v\n", minIndex)
	fmt.Printf("MaxIndex: %v\n", maxIndex)
	dstBlock := dst.Extended.BlockSize

	nz := dstBlock[2]
	ny := dstBlock[1]

	startz := minIndex[2] * srcBlock[2]
	if startz%dstBlock[2] != 0 {
		startz -= startz % dstBlock[2]
	}
	starty := minIndex[1] * srcBlock[1]
	if starty%dstBlock[1] != 0 {
		starty -= starty % dstBlock[1]
	}
	startx := minIndex[0] * srcBlock[0]
	if startx%dstBlock[0] != 0 {
		startx -= startx % dstBlock[0]
	}

	xsize := 2048
	for oz := startz; oz <= maxIndex[2]*srcBlock[2]; oz += nz {
		for oy := starty; oy <= maxIndex[1]*srcBlock[1]; oy += ny {
			for ox := startx; ox <= maxIndex[0]*srcBlock[0]; ox += xsize {
				nx := xsize
				if ox+nx >= (maxIndex[0]+1)*srcBlock[0] {
					nx = (maxIndex[0]+1)*srcBlock[0] - ox
				}
				if nx%64 != 0 {
					nx += (nx % 64)
				}
				if nx != 0 {
					if fastForward {
						if oz < ffz {
							continue
						} else if oz == ffz {
							if oy < ffy {
								continue
							} else if oy == ffy {
								if ox+nx < ffx {
									continue
								}
							}
						}
					}
					fmt.Printf("Sending strip of %d x %d x %d voxels @ offset (%d, %d, %d)\n", nx, ny, nz, ox, oy, oz)
					sendStrip(name, srcURL, nx, ny, nz, ox, oy, oz)
				}
			}
		}
	}
	fmt.Printf("Completed transfer.")
}

func sendStrip(name, srcURL string, vx, vy, vz, ox, oy, oz int) {
	var err error
	getURL := fmt.Sprintf("%s/raw/0_1_2/%d_%d_%d/%d_%d_%d?compression=lz4", srcURL, vx, vy, vz, ox, oy, oz)
	if *roi != "" {
		getURL += fmt.Sprintf("&roi=%s", *roi)
	}
	var resp *http.Response
	if *dryrun {
		fmt.Printf("-- would GET %s\n", getURL)
	} else {
		resp, err = http.Get(getURL)
		if err != nil {
			fmt.Printf("Receive error: %s\n", err.Error())
			os.Exit(1)
		}
		if resp.StatusCode != http.StatusOK {
			var respnote []byte
			data, _ := ioutil.ReadAll(resp.Body)
			if len(data) < 2000 {
				respnote = data
			}
			fmt.Printf("Bad status on receiving data: (%d) %s\n", resp.StatusCode, string(respnote))
			os.Exit(1)
		}
	}
	if *url != "" {
		postURL := fmt.Sprintf("%s/raw/0_1_2/%d_%d_%d/%d_%d_%d?compression=lz4", *url, vx, vy, vz, ox, oy, oz)
		if *dryrun {
			fmt.Printf("-- would POST %s\n", postURL)
		} else {
			resp2, err := http.Post(postURL, "application/octet-stream", resp.Body)
			if err != nil {
				fmt.Printf("Transmit error: %s\n", err.Error())
				os.Exit(1)
			}
			if resp2.StatusCode != http.StatusOK {
				fmt.Printf("Bad status on sending data: %d\n", resp2.StatusCode)
				os.Exit(1)
			}
		}
	}
	if *outdir != "" {
		// Compute the output file name
		var ext string
		switch *compression {
		case "none":
			ext = "dat"
		case "lz4":
			ext = "lz4"
		case "gzip":
			ext = "gz"
		default:
			fmt.Printf("unknown compression type %q", *compression)
			os.Exit(1)
		}

		base := fmt.Sprintf("%s-%dx%dx%d+%d+%d+%d.%s", name, vx, vy, vz, ox, oy, oz, ext)
		filename := filepath.Join(*outdir, base)
		if err := writeFile(resp.Body, filename); err != nil {
			fmt.Printf("Error writing file: %s\n", filename)
			os.Exit(1)
		}
	}
}

func writeFile(data io.ReadCloser, filename string) error {
	if *dryrun {
		return nil
	}
	defer data.Close()

	// Setup file for write
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var out io.WriteCloser
	switch *compression {
	case "none":
		out = f
	case "gzip":
		out = gzip.NewWriter(f)
		defer out.Close()
	default:
		return fmt.Errorf("Can't compress to type %q, only %q\n", *compression, "gzip")
	}

	written, err := io.Copy(out, data)
	if err != nil {
		fmt.Printf("Error on copy of data from DVID to file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote %d MB to %s\n", written/1000000, filename)
	return nil
}
