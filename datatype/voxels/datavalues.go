/*
   File handles interleaved data of different types and operations on those volumes.
   Generics would be nice here :)
*/

package voxels

import (
	_ "encoding/binary"
)

// DataValue describes the data type and label for each value within a voxel.
type DataValue struct {
	DataType string
	Label    string
}

// ValueBytes returns the number of bytes for this value
func (dv DataValue) ValueBytes() int32 {
	return typeBytes[dv.DataType]
}

// DataValues describes the interleaved values within a voxel.
type DataValues []DataValue

var typeBytes = map[string]int32{
	"uint8":   1,
	"int8":    1,
	"uint16":  2,
	"int16":   2,
	"uint32":  4,
	"int32":   4,
	"uint64":  8,
	"int64":   8,
	"float32": 4,
	"float64": 8,
}

func (values DataValues) BytesPerVoxel() int32 {
	var bytes int32
	for _, dataValue := range values {
		bytes += typeBytes[dataValue.DataType]
	}
	return bytes
}

func (values DataValues) ValueBytes(dim int) int32 {
	if dim < 0 || dim >= len(values) {
		return 0
	}
	return typeBytes[values[dim].DataType]
}

func (values DataValues) averageData(src, dst []byte, srcW, dstW, dstH, reduceW, reduceH int32) {
	var offset int32
	for _, dv := range values {
		switch dv.DataType {
		case "uint8":
			uint8average(src[offset:], dst[offset:], values.BytesPerVoxel(), srcW, dstW, dstH, reduceW, reduceH)
		}
		offset += dv.ValueBytes()
	}

}

func uint8average(src, dst []byte, bytesPerVoxel, srcW, dstW, dstH, reduceW, reduceH int32) {
	var reduceSize uint64 = uint64(reduceW) * uint64(reduceH)
	var srcStride int32 = srcW * bytesPerVoxel
	var dstI, srcY, srcX int32
	var x, y, rx, ry int32
	for y = 0; y < dstH; y++ {
		srcX = 0
		for x = 0; x < dstW; x++ {
			var sum uint64
			for ry = 0; ry < reduceH; ry++ {
				srcI := (srcY+ry)*srcStride + srcX*bytesPerVoxel
				for rx = 0; rx < reduceW; rx++ {
					sum += uint64(src[srcI])
					srcI += bytesPerVoxel
				}
			}
			sum /= reduceSize
			dst[dstI] = uint8(sum)
			dstI++
			srcX += reduceW
		}
		srcY += reduceH
	}
}
