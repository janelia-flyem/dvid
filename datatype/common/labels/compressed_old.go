// This file implements the initial version of Google Block compression by Steve Plaza.
// It is retained for benchmarks as a proxy for standard Neuroglancer-based Block compression.

package labels

import (
	"fmt"
	"math"
	"runtime"

	"github.com/janelia-flyem/dvid/dvid"
)

// oldCompress takes an input 3D label array of a given volume dimensions and compresses
// it using the segmentation format defined in Neuroglancer.  VolSize must be tileable by
// 8x8x8 block size.
func oldCompress(b []byte, volsize dvid.Point) ([]byte, error) {
	uintSlice, err := dvid.AliasByteToUint64(b)
	if err != nil {
		return nil, err
	}
	seg, err := oldCompressUint64(uintSlice, volsize)
	if err != nil {
		return nil, err
	}
	runtime.KeepAlive(&b)
	return dvid.AliasUint32ToByte(seg), nil
}

// oldCompressUint64 takes an input 3D label array of a given volume dimensions and compresses
// it using the segmentation format defined in Neuroglancer.  VolSize must be tileable by
// 8x8x8 block size.  The output data is an array of 32 bit numbers. TODO: reuse table
// encoding to improve compression.
func oldCompressUint64(input []uint64, volsize dvid.Point) ([]uint32, error) {
	BLKSIZE := uint(8)

	xsize := uint(volsize.Value(0))
	ysize := uint(volsize.Value(1))
	zsize := uint(volsize.Value(2))
	gx := xsize / BLKSIZE
	gy := ysize / BLKSIZE
	gz := zsize / BLKSIZE
	if xsize%BLKSIZE > 0 || ysize%BLKSIZE > 0 || zsize%BLKSIZE > 0 {
		return nil, fmt.Errorf("volume must be a multiple of the block size")
	}

	// must add initial 4 byte to designate as a header
	// for the compressed data for neuroglancer

	// 64 bit headers for each 8x8x8 block and pre-allocate some data based on expected data size
	globaloffset := 0 // !! set to 1 if header used by neuroglancer which needs to know first byte of multi-channel compression

	datagoogle := make([]uint32, gx*gy*gz*2+uint(globaloffset), xsize*ysize*zsize*2/10)
	//datagoogle[0] = byte(1) // !! compressed data starts after first 4 bytes for neuroglancer

	// everything is written out little-endian
	for gziter := uint(0); gziter < gz; gziter++ {
		for gyiter := uint(0); gyiter < gy; gyiter++ {
			for gxiter := uint(0); gxiter < gx; gxiter++ {
				unique_vals := make(map[uint64]uint32)
				unique_list := make([]uint64, 0)

				currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE)

				// extract unique values in the 8x8x8 block
				for z := uint(0); z < BLKSIZE; z++ {
					for y := uint(0); y < BLKSIZE; y++ {
						for x := uint(0); x < BLKSIZE; x++ {
							if _, ok := unique_vals[input[currpos]]; !ok {
								unique_vals[input[currpos]] = 0
								unique_list = append(unique_list, input[currpos])
							}
							currpos += 1
						}
						currpos += (xsize - BLKSIZE)
					}
					currpos += (xsize*ysize - (xsize * (BLKSIZE)))
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
				compressstart := len(datagoogle) // in 4-byte units
				// number of bytes to add (encode bytes + table size of 8 byte numbers)
				addedInts := uint32(encodedBits*uint32(BLKSIZE*BLKSIZE*BLKSIZE)/8)/4 + uint32(len(unique_vals)*2) // will always be a multiple of 4 bytes
				bitspot := uint(len(datagoogle) * 32)
				datagoogle = append(datagoogle, make([]uint32, addedInts)...)

				// do not need to write-out anything if there is only one entry
				if encodedBits > 0 {
					currpos := (gziter*BLKSIZE*(xsize*ysize) + gyiter*BLKSIZE*xsize + gxiter*BLKSIZE)
					for z := uint32(0); z < uint32(BLKSIZE); z++ {
						for y := uint32(0); y < uint32(BLKSIZE); y++ {
							for x := uint32(0); x < uint32(BLKSIZE); x++ {
								mappedval := unique_vals[input[currpos]]
								bitshift := bitspot % 32
								bytespot := bitspot / 32
								datagoogle[bytespot] |= (mappedval << bitshift)

								bitspot += uint(encodedBits)
								currpos += 1
							}
							currpos += (xsize - BLKSIZE)
						}
						currpos += (xsize*ysize - (xsize * (BLKSIZE)))
					}
				}

				// write-out lookup table
				tablestart := bitspot / 32 // in 4-byte units
				for _, val := range unique_list {
					datagoogle[bitspot/32] = uint32(val)
					bitspot += 32
					datagoogle[bitspot/32] = uint32(val >> 32)
					bitspot += 32
				}

				// write-out block header
				// 8 bytes per header entry
				headerpos := (gziter*(gy*gx)+gyiter*gx+gxiter)*2 + uint(globaloffset) // shift start by global offset

				// write out lookup table start
				tablestart -= uint(globaloffset) // relative to the start of the compressed data
				datagoogle[headerpos] = uint32(tablestart)

				// write out number of encoded bits
				datagoogle[headerpos] |= (encodedBits << 24)
				headerpos++

				// write out block compress start
				compressstart -= (globaloffset) // relative to the start of the compressed data
				datagoogle[headerpos] = uint32(compressstart)
				headerpos++
			}
		}
	}

	return datagoogle, nil

}

// oldDecompress takes an input compressed array of 64-bit labels of a given volume dimensions
// and decompresses it. VolSize must be tileable by 8x8x8 block size.  The output
// data is a slice of bytes that represents packed uint64.
func oldDecompress(b []byte, volsize dvid.Point) ([]byte, error) {
	seg, err := dvid.AliasByteToUint32(b)
	if err != nil {
		return nil, err
	}
	labels, err := oldDecompressUint64(seg, volsize)
	if err != nil {
		return nil, err
	}
	return dvid.AliasUint64ToByte(labels), nil
}

// oldDecompressUint64 takes an input compressed array of 64-bit labels of a given volume dimensions
// and decompresses it. VolSize must be tileable by 8x8x8 block size.  The output
// data is an array of 64 bit numbers.
func oldDecompressUint64(input []uint32, volsize dvid.Point) ([]uint64, error) {
	BLKSIZE := uint(8)

	xsize := uint(volsize.Value(0))
	ysize := uint(volsize.Value(1))
	zsize := uint(volsize.Value(2))
	if xsize%BLKSIZE > 0 || ysize%BLKSIZE > 0 || zsize%BLKSIZE > 0 {
		return nil, fmt.Errorf("volume must be a multiple of the block size")
	}

	// create output buffer
	output := make([]uint64, xsize*ysize*zsize)

	grid_size := [3]uint{xsize / BLKSIZE, ysize / BLKSIZE, zsize / BLKSIZE}
	block := [3]uint{0, 0, 0}

	for block[2] = 0; block[2] < grid_size[2]; block[2]++ {
		for block[1] = 0; block[1] < grid_size[1]; block[1]++ {
			for block[0] = 0; block[0] < grid_size[0]; block[0]++ {

				block_offset := block[0] + grid_size[0]*(block[1]+grid_size[1]*block[2])

				tableoffset := input[block_offset*2] & 0xffffff
				encoded_bits := (input[block_offset*2] >> 24) & 0xff
				encoded_value_start := input[block_offset*2+1]

				// find absolute positions in output array
				xmin := block[0] * BLKSIZE
				xmax := xmin + BLKSIZE

				ymin := block[1] * BLKSIZE
				ymax := ymin + BLKSIZE

				zmin := block[2] * BLKSIZE
				zmax := zmin + BLKSIZE

				bitmask := (1 << encoded_bits) - 1

				for z := zmin; z < zmax; z++ {
					for y := ymin; y < ymax; y++ {

						outindex := (z*ysize+y)*xsize + xmin
						bitpos := BLKSIZE * ((z-zmin)*(BLKSIZE) + (y - ymin)) * uint(encoded_bits)

						for x := xmin; x < xmax; x++ {
							bitshift := bitpos % 32

							arraypos := bitpos / (32)
							bitval := uint(0)
							if encoded_bits > 0 {
								bitval = (uint(input[uint(encoded_value_start)+arraypos]) >> bitshift) & uint(bitmask)
							}
							val := uint64(input[uint(tableoffset)+bitval*2])
							val |= uint64(input[uint(tableoffset)+bitval*2+1]) << 32
							output[outindex] = val

							bitpos += uint(encoded_bits)
							outindex++
						}

					}
				}
			}

		}

	}
	return output, nil
}
