/*
	This file contains data types and functions that support a number of layers in DVID.
*/

package dvid

import (
	"encoding/binary"
)

// LocalID is a unique id for some data in a DVID instance.  This unique id is presumably
// a much smaller representation than the actual data (e.g., a version UUID or dataset
// description) and can be represented with fewer bytes in keys.
type LocalID uint16

const sizeOfLocalID = 2

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

// Config is a map of keyword to arbitrary data to specify configurations via keyword.
type Config map[string]interface{}
