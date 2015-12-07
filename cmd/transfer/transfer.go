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
		MinIndex  [3]int
		MaxIndex  [3]int
	}
}

func sendLabels(src *LabelMetadata, name, srcURL string) {
	minIndex := src.Extended.MinIndex
	maxIndex := src.Extended.MaxIndex
	blockSize := src.Extended.BlockSize
	if blockSize[0] != blockSize[1] {
		fmt.Printf("Can't handle non-cubic block sizes as in %q data instance: %v\n", name, blockSize)
		os.Exit(1)
	}
	fmt.Printf("MinIndex: %v\n", minIndex)
	fmt.Printf("MaxIndex: %v\n", maxIndex)

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

	// Iterate through each XY plane of one block deep data.
	// Send it to destination, synchronously.
	nx := maxIndex[0] - minIndex[0] + 1
	ny := maxIndex[1] - minIndex[1] + 1

	// # voxels in x, and xy
	vx := nx * blockSize[0]
	vy := ny * blockSize[1]
	vz := blockSize[2]

	strips := 1
	if vx*vy*vz*8 > 2000000000 {
		strips = (vx*vy*vz*8)/2000000000 + 1
	}
	byPerStrip := ny / strips
	fmt.Printf("Number of strips per layer: %d\n", strips)
	fmt.Printf("Total uncompressed bytes per file: %d MB\n", vx*vy*vz*8/(strips*1000000))

	var err error
	ox := minIndex[0] * blockSize[0]
	for z := minIndex[2]; z <= maxIndex[2]; z++ {
		oz := z * blockSize[2]
		if oz+blockSize[2] < *minz || oz > *maxz {
			continue
		}
		by0 := minIndex[1]
		for n := 0; n < strips; n++ {
			if by0 > maxIndex[1] {
				break
			}
			by1 := by0 + byPerStrip - 1
			if by1 > maxIndex[1] {
				by1 = maxIndex[1]
			}

			oy := by0 * blockSize[1]
			vy := (by1 - by0 + 1) * blockSize[1]
			getURL := fmt.Sprintf("%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", srcURL, vx, vy, vz, ox, oy, oz)
			if *roi != "" {
				getURL += fmt.Sprintf("?roi=%s", *roi)
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
					fmt.Printf("Bad status on receiving data: %d\n", resp.StatusCode)
					os.Exit(1)
				}
			}
			if *url != "" {
				postURL := fmt.Sprintf("%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", *url, vx, vy, vz, ox, oy, oz)
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
				base := fmt.Sprintf("%s-%dx%dx%d+%d+%d+%d.%s", name, vx, vy, vz, ox, oy, oz, ext)
				filename := filepath.Join(*outdir, base)
				if err := writeFile(resp.Body, filename); err != nil {
					fmt.Printf("Error writing file: %s\n", filename)
					os.Exit(1)
				}
			}

			by0 += byPerStrip
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

func transferData(host, uuid, name string) {
	// If we are transferring to another DVID host, verify the destination.
	var dstMetadata *LabelMetadata
	if *url != "" {
		dstMetadata = getLabelMetadata(*url)
		dsttype := dstMetadata.Base.TypeName

		switch dsttype {
		case "labelblk":
			fmt.Printf("-- verified that POST URL (%v) is labelblk.\n", *url)
		default:
			fmt.Printf("POST URL (%v) must be labelblk but is %s data type\n", *url, dsttype)
			os.Exit(1)
		}
	}

	// Get the source metadata
	srcURL := fmt.Sprintf("%s/api/node/%s/%s", host, uuid, name)
	srcMetadata := getLabelMetadata(srcURL)
	if srcMetadata.Base.TypeName != "labelblk" {
		fmt.Printf("Source data instance %q is not labelblk type.  Aborting.\n", name)
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

	sendLabels(srcMetadata, name, srcURL)
}
