package labelmap

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/janelia-flyem/dvid/dvid"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

// uncompressReaderData returns label data from a potentially compressed ("lz4", "gzip") reader.
func uncompressReaderData(compression string, in io.ReadCloser, estsize int64) ([]byte, error) {
	var err error
	var data []byte
	switch compression {
	case "":
		tlog := dvid.NewTimeLog()
		data, err = io.ReadAll(in)
		if err != nil {
			return nil, err
		}
		tlog.Debugf("read 3d uncompressed POST")
	case "lz4":
		tlog := dvid.NewTimeLog()
		data, err = io.ReadAll(in)
		if err != nil {
			return nil, err
		}
		tlog.Debugf("read 3d lz4 POST: %d bytes", len(data))
		if len(data) == 0 {
			return nil, fmt.Errorf("received 0 LZ4 compressed bytes")
		}
		tlog = dvid.NewTimeLog()
		uncompressed := make([]byte, estsize)
		if err = lz4.Uncompress(data, uncompressed); err != nil {
			return nil, err
		}
		data = uncompressed
		tlog.Debugf("uncompressed 3d lz4 POST: %d bytes", len(data))
	case "gzip":
		tlog := dvid.NewTimeLog()
		gr, err := gzip.NewReader(in)
		if err != nil {
			return nil, err
		}
		data, err = io.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		if err = gr.Close(); err != nil {
			return nil, err
		}
		tlog.Debugf("read and uncompress 3d gzip POST: %d bytes", len(data))
	default:
		return nil, fmt.Errorf("unknown compression type %q", compression)
	}
	return data, nil
}

// compressGoogle uses the neuroglancer compression format
func compressGoogle(data []byte, subvol *dvid.Subvolume) ([]byte, error) {
	// TODO: share table between blocks
	subvolsizes := subvol.Size()

	// must <= 32
	BLKSIZE := int32(8)

	xsize := subvolsizes.Value(0)
	ysize := subvolsizes.Value(1)
	zsize := subvolsizes.Value(2)
	gx := subvolsizes.Value(0) / BLKSIZE
	gy := subvolsizes.Value(1) / BLKSIZE
	gz := subvolsizes.Value(2) / BLKSIZE
	if xsize%BLKSIZE > 0 || ysize%BLKSIZE > 0 || zsize%BLKSIZE > 0 {
		return nil, fmt.Errorf("volume must be a multiple of the block size")
	}

	// add initial 4 byte to designate as a header for the compressed data
	// 64 bit headers for each 8x8x8 block and pre-allocate some data based on expected data size
	dword := 4
	globaloffset := dword

	datagoogle := make([]byte, gx*gy*gz*8+int32(globaloffset), xsize*ysize*zsize*8/10)
	datagoogle[0] = byte(globaloffset / dword) // compressed data starts after first 4 bytes

	// everything is written out little-endian
	for gziter := int32(0); gziter < gz; gziter++ {
		for gyiter := int32(0); gyiter < gy; gyiter++ {
			for gxiter := int32(0); gxiter < gx; gxiter++ {
				unique_vals := make(map[uint64]uint32)
				unique_list := make([]uint64, 0)

				currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE) * 8

				// extract unique values in the 8x8x8 block
				for z := int32(0); z < BLKSIZE; z++ {
					for y := int32(0); y < BLKSIZE; y++ {
						for x := int32(0); x < BLKSIZE; x++ {
							if _, ok := unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])]; !ok {
								unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])] = 0
								unique_list = append(unique_list, binary.LittleEndian.Uint64(data[currpos:currpos+8]))
							}
							currpos += 8
						}
						currpos += ((xsize - BLKSIZE) * 8)
					}
					currpos += (xsize*ysize - (xsize * (BLKSIZE))) * 8
				}
				// write out mapping
				for pos, val := range unique_list {
					unique_vals[val] = uint32(pos)
				}

				// write-out compressed data
				encodedBits := uint32(math.Ceil(math.Log2(float64(len(unique_vals)))))
				switch {
				case encodedBits == 0, encodedBits == 1, encodedBits == 2:
				case encodedBits <= 4:
					encodedBits = 4
				case encodedBits <= 8:
					encodedBits = 8
				case encodedBits <= 16:
					encodedBits = 16
				}

				// starting location for writing out data
				currpos2 := len(datagoogle)
				compressstart := len(datagoogle) / dword // in 4-byte units
				// number of bytes to add (encode bytes + table size of 8 byte numbers)
				addedBytes := uint32(encodedBits*uint32(BLKSIZE*BLKSIZE*BLKSIZE)/8) + uint32(len(unique_vals)*8) // will always be a multiple of 4 bytes
				datagoogle = append(datagoogle, make([]byte, addedBytes)...)

				// do not need to write-out anything if there is only one entry
				if encodedBits > 0 {
					currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE) * 8

					for z := uint32(0); z < uint32(BLKSIZE); z++ {
						for y := uint32(0); y < uint32(BLKSIZE); y++ {
							for x := uint32(0); x < uint32(BLKSIZE); x++ {
								mappedval := unique_vals[binary.LittleEndian.Uint64(data[currpos:currpos+8])]
								currpos += 8

								// write out encoding
								startbit := uint32((encodedBits * x) % uint32(8))
								if encodedBits == 16 {
									// write two bytes worth of data
									datagoogle[currpos2] = byte(255 & mappedval)
									currpos2++
									datagoogle[currpos2] = byte(255 & (mappedval >> 8))
									currpos2++
								} else {
									// write bit-shifted data
									datagoogle[currpos2] |= byte(255 & (mappedval << startbit))
								}
								if int(startbit) == (8 - int(encodedBits)) {
									currpos2++
								}

							}
							currpos += ((xsize - BLKSIZE) * 8)
						}
						currpos += (xsize*ysize - (xsize * (BLKSIZE))) * 8
					}
				}
				tablestart := currpos2 / dword // in 4-byte units
				// write-out lookup table
				for _, val := range unique_list {
					for bytespot := uint32(0); bytespot < uint32(8); bytespot++ {
						datagoogle[currpos2] = byte(255 & (val >> (bytespot * 8)))
						currpos2++
					}
				}

				// write-out block header
				// 8 bytes per header entry
				headerpos := (gziter*(gy*gx)+gyiter*gx+gxiter)*8 + int32(globaloffset) // shift start by global offset

				// write out lookup table start
				tablestart -= (globaloffset / dword) // relative to the start of the compressed data
				datagoogle[headerpos] = byte(255 & tablestart)
				headerpos++
				datagoogle[headerpos] = byte(255 & (tablestart >> 8))
				headerpos++
				datagoogle[headerpos] = byte(255 & (tablestart >> 16))
				headerpos++

				// write out number of encoded bits
				datagoogle[headerpos] = byte(255 & encodedBits)
				headerpos++

				// write out block compress start
				compressstart -= (globaloffset / dword) // relative to the start of the compressed data
				datagoogle[headerpos] = byte(255 & compressstart)
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 8))
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 16))
				headerpos++
				datagoogle[headerpos] = byte(255 & (compressstart >> 24))
			}
		}
	}

	return datagoogle, nil
}

// Given an array of label data (little-endian uint64) of dimensions related by subvol, write
// via HTTP with the appropriate compression schemes.
func writeCompressedToHTTP(compression string, data []byte, subvol *dvid.Subvolume, w http.ResponseWriter) error {
	var err error
	w.Header().Set("Content-type", "application/octet-stream")
	switch compression {
	case "":
		_, err = w.Write(data)
		if err != nil {
			return err
		}
	case "lz4":
		compressed := make([]byte, lz4.CompressBound(data))
		var n, outSize int
		if outSize, err = lz4.Compress(data, compressed); err != nil {
			return err
		}
		compressed = compressed[:outSize]
		if n, err = w.Write(compressed); err != nil {
			return err
		}
		if n != outSize {
			errmsg := fmt.Sprintf("Only able to write %d of %d lz4 compressed bytes\n", n, outSize)
			dvid.Errorf("%s", errmsg)
			return err
		}
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err = gw.Write(data); err != nil {
			return err
		}
		if err = gw.Close(); err != nil {
			return err
		}
	case "google", "googlegzip": // see neuroglancer for details of compressed segmentation format
		datagoogle, err := compressGoogle(data, subvol)
		if err != nil {
			return err
		}
		if compression == "googlegzip" {
			w.Header().Set("Content-encoding", "gzip")
			gw := gzip.NewWriter(w)
			if _, err = gw.Write(datagoogle); err != nil {
				return err
			}
			if err = gw.Close(); err != nil {
				return err
			}

		} else {
			_, err = w.Write(datagoogle)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown compression type %q", compression)
	}
	return nil
}
