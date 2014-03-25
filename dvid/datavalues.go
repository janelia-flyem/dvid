/*
   This file handles layout of data for a element, e.g., a voxel, and routines that
   extract data from a slice of bytes.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// DataType is a unique ID for each type of data within DVID, e.g., a uint8 or a float32.
type DataType uint8

const (
	T_uint8 DataType = iota
	T_int8
	T_uint16
	T_int16
	T_uint32
	T_int32
	T_uint64
	T_int64
	T_float32
	T_float64
)

var typeBytes = map[DataType]int32{
	T_uint8:   1,
	T_int8:    1,
	T_uint16:  2,
	T_int16:   2,
	T_uint32:  4,
	T_int32:   4,
	T_uint64:  8,
	T_int64:   8,
	T_float32: 4,
	T_float64: 8,
}

// DataTypeBytes returns the # of bytes for a given type string.
// For example, "uint16" is 2 bytes.  No error checking is performed
// to make sure string is valid.
func DataTypeBytes(t DataType) int32 {
	return typeBytes[t]
}

// DataValue describes the data type and label for each value within an element.
// Terminology: An "element" is some grouping, e.g., data associated with a voxel.
// A "value" is one component of the data for an element, or said another way,
// an element can have many values that may be of differing type and size.
type DataValue struct {
	T     DataType
	Label string
}

// ValueBytes returns the number of bytes for this value
func (dv DataValue) ValueBytes() int32 {
	return typeBytes[dv.T]
}

// MarshalJSON implements the json.Marshaler interface.
func (dv DataValue) MarshalJSON() ([]byte, error) {
	var dataType string
	switch dv.T {
	case T_uint8:
		dataType = "uint8"
	case T_int8:
		dataType = "int8"
	case T_uint16:
		dataType = "uint16"
	case T_int16:
		dataType = "int16"
	case T_uint32:
		dataType = "uint32"
	case T_int32:
		dataType = "int32"
	case T_uint64:
		dataType = "uint64"
	case T_int64:
		dataType = "int64"
	case T_float32:
		dataType = "float32"
	case T_float64:
		dataType = "float64"
	}
	return []byte(fmt.Sprintf(`{"DataType":%q,"Label":%q}`, dataType, dv.Label)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (dv *DataValue) UnmarshalJSON(b []byte) error {
	var m struct {
		DataType string
		Label    string
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	var dataType DataType
	switch m.DataType {
	case "uint8":
		dataType = T_uint8
	case "int8":
		dataType = T_int8
	case "uint16":
		dataType = T_uint16
	case "int16":
		dataType = T_int16
	case "uint32":
		dataType = T_uint32
	case "int32":
		dataType = T_int32
	case "uint64":
		dataType = T_uint64
	case "int64":
		dataType = T_int64
	case "float32":
		dataType = T_float32
	case "float64":
		dataType = T_float64
	}
	dv.T = dataType
	dv.Label = m.Label
	return nil
}

// DataValues describes the interleaved values within an element.
type DataValues []DataValue

func (values DataValues) ValuesPerElement() int32 {
	return int32(len(values))
}

func (values DataValues) BytesPerElement() int32 {
	var bytes int32
	for _, dataValue := range values {
		bytes += typeBytes[dataValue.T]
	}
	return bytes
}

// ValueDataType returns the dvid.DataType used for all values in
// an element.  If the data types vary, e.g., an int32 then a float32,
// this function will return an error.
func (values DataValues) ValueDataType() (DataType, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("ValueDataType() called on empty DataValues!")
	}
	dataType := values[0].T
	for i := 1; i < len(values); i++ {
		if values[i].T != dataType {
			return 0, fmt.Errorf("Data format '%s' has varying value data types within an element.", values)
		}
	}
	return dataType, nil
}

// BytesPerValue returns the # bytes for each value of an element if
// all values have equal size.  An error is returned if values are varying
// sizes for an element.  Use ValueBytes(dim) to get bytes/value for
// elements with varying # of bytes per value.
func (values DataValues) BytesPerValue() (int32, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("BytesPerValue() called on empty DataValues!")
	}
	bytesPerValue := typeBytes[values[0].T]
	for i := 1; i < len(values); i++ {
		if bytesPerValue != typeBytes[values[i].T] {
			return 0, fmt.Errorf("BytesPerValue() called on DataValues with varying bytes/value.")
		}
	}
	return bytesPerValue, nil
}

// ValueBytes returnes the size of the nth value in an element.
func (values DataValues) ValueBytes(n int) int32 {
	if n < 0 || n >= len(values) {
		return 0
	}
	return typeBytes[values[n].T]
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (values DataValues) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	numValues := int16(len(values))
	if err := binary.Write(&buffer, binary.LittleEndian, numValues); err != nil {
		return nil, err
	}
	for i := int16(0); i < numValues; i++ {
		if err := binary.Write(&buffer, binary.LittleEndian, values[i].T); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, uint16(len(values[i].Label))); err != nil {
			return nil, err
		}
		if _, err := buffer.Write([]byte(values[i].Label)); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (values *DataValues) UnmarshalBinary(b []byte) error {
	buffer := bytes.NewBuffer(b)
	var numValues uint16
	if err := binary.Read(buffer, binary.LittleEndian, &numValues); err != nil {
		return err
	}
	*values = make(DataValues, numValues, numValues)
	for i := uint16(0); i < numValues; i++ {
		var dataType DataType
		if err := binary.Read(buffer, binary.LittleEndian, &dataType); err != nil {
			return err
		}
		(*values)[i].T = dataType

		var length uint16
		if err := binary.Read(buffer, binary.LittleEndian, &length); err != nil {
			return err
		}
		label := make([]byte, length)
		if _, err := buffer.Read(label); err != nil {
			return err
		}
		(*values)[i].Label = string(label)
	}
	return nil
}

// AverageData averages the source slice of data and stores the result in the dst slice.
func (values DataValues) AverageData(src, dst []byte, srcW, dstW, dstH, reduceW, reduceH int32) {
	var offset int32
	for _, dv := range values {
		switch dv.T {
		case T_uint8:
			uint8average(src[offset:], dst[offset:], values.BytesPerElement(), srcW, dstW, dstH, reduceW, reduceH)
		default:
			panic(fmt.Sprintf("AverageData() in datavalues.go is being called with unexpected value type: %d [%s]\n", dv.T, dv.Label))
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
