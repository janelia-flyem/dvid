/*
	This file contains data types and functions that support a number of layers in DVID.
*/

package dvid

import (
	"encoding/binary"
	"fmt"
)

// LocalID is a unique id for some data in a DVID instance.  This unique id is presumably
// a much smaller representation than the actual data (e.g., a version UUID or dataset
// description) and can be represented with fewer bytes in keys.
type LocalID uint16

// LocalID32 is a 32-bit unique id within this DVID instance.
type LocalID32 uint32

const (
	sizeOfLocalID   = 2
	sizeOfLocalID32 = 4
)

// Bytes returns a sequence of bytes encoding this LocalID.
func (id LocalID) Bytes() []byte {
	buf := make([]byte, sizeOfLocalID, sizeOfLocalID)
	binary.LittleEndian.PutUint16(buf, uint16(id))
	return buf
}

// LocalIDFromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalIDFromBytes(b []byte) (id LocalID, length int) {
	return LocalID(binary.LittleEndian.Uint16(b)), sizeOfLocalID
}

// Bytes returns a sequence of bytes encoding this LocalID32.
func (id LocalID32) Bytes() []byte {
	buf := make([]byte, sizeOfLocalID32, sizeOfLocalID32)
	binary.LittleEndian.PutUint32(buf, uint32(id))
	return buf
}

// LocalID32FromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalID32FromBytes(b []byte) (id LocalID32, length int) {
	return LocalID32(binary.LittleEndian.Uint32(b)), sizeOfLocalID32
}

// Config is a map of keyword to arbitrary data to specify configurations via keyword.
type Config map[string]interface{}

// IsVersioned returns true if we want this data versioned.
func (c Config) IsVersioned() (versioned bool, err error) {
	param, ok := c["versioned"]
	if !ok {
		err = fmt.Errorf("No 'versioned' key in DVID configuration")
		return
	}
	versioned, ok = param.(bool)
	if !ok {
		err = fmt.Errorf("Illegal 'versioned' value in DVID configuration")
	}
	return
}
