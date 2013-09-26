// Implements reading and writing of V3D Raw File formats.

package multichan16

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
)

type V3DRawMarshaler struct{}

func (V3DRawMarshaler) UnmarshalV3DRaw(reader io.Reader) ([]*Channel, error) {
	magicString := make([]byte, 24)
	if n, err := reader.Read(magicString); n != 24 || err != nil {
		return nil, fmt.Errorf("Error reading magic string in V3D Raw file: %s", err.Error())
	}
	if string(magicString) != "raw_image_stack_by_hpeng" {
		return nil, fmt.Errorf("Bad magic string in V3D Raw File: %s", string(magicString))
	}
	endianType := make([]byte, 1, 1)
	if n, err := reader.Read(endianType); n != 1 || err != nil {
		return nil, fmt.Errorf("Could not read endianness of V3D Raw file: %s", err.Error())
	}
	var byteOrder binary.ByteOrder
	switch string(endianType) {
	case "L":
		byteOrder = binary.LittleEndian
	case "B":
		byteOrder = binary.BigEndian
	default:
		return nil, fmt.Errorf("Illegal byte order '%s' in V3D Raw File", endianType)
	}
	var dataType uint16
	if err := binary.Read(reader, byteOrder, &dataType); err != nil {
		return nil, err
	}
	var bytesPerVoxel uint32
	switch dataType {
	case 1:
		bytesPerVoxel = 1
	case 2:
		bytesPerVoxel = 2
	default:
		return nil, fmt.Errorf("Cannot handle V3D Raw File with data type %d", dataType)
	}
	var width, height, depth, numChannels uint32
	if err := binary.Read(reader, byteOrder, &width); err != nil {
		return nil, fmt.Errorf("Error reading width in V3D Raw File: %s", err.Error())
	}
	if err := binary.Read(reader, byteOrder, &height); err != nil {
		return nil, fmt.Errorf("Error reading height in V3D Raw File: %s", err.Error())
	}
	if err := binary.Read(reader, byteOrder, &depth); err != nil {
		return nil, fmt.Errorf("Error reading depth in V3D Raw File: %s", err.Error())
	}
	if err := binary.Read(reader, byteOrder, &numChannels); err != nil {
		return nil, fmt.Errorf("Error reading # channels in V3D Raw File: %s", err.Error())
	}

	// Allocate the V3DRaw struct for the # channels
	bytesPerChannel := int(bytesPerVoxel * width * height * depth)
	size := voxels.Point3d{int32(width), int32(height), int32(depth)}
	volume := voxels.NewSubvolume(voxels.Coord{0, 0, 0}, size)
	v3draw := make([]*Channel, numChannels, numChannels)
	var c int32
	for c = 0; c < int32(numChannels); c++ {
		v3draw[c] = &Channel{
			Geometry:      volume,
			channelNum:    c,
			bytesPerVoxel: int32(bytesPerVoxel),
			data:          make([]uint8, bytesPerChannel, bytesPerChannel),
			stride:        int32(width * bytesPerVoxel),
		}
	}

	// Read in the data for each channel
	for c = 0; c < int32(numChannels); c++ {
		if err := binary.Read(reader, byteOrder, v3draw[c].data); err != nil {
			return nil, fmt.Errorf("Error reading data for channel %d: %s", c, err.Error())
		}
		if dvid.Mode == dvid.Debug {
			chanStr := fmt.Sprintf("Channel %d", c)
			dvid.PrintNonZero(chanStr, v3draw[c].data)
		}
	}
	return v3draw, nil
}
