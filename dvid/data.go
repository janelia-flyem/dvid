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

// Bytes returns a sequence of bytes encoding this LocalID.
func (id LocalID) Bytes() []byte {
	buf := make([]byte, 2, 2)
	binary.LittleEndian.PutUint16(buf, uint16(id))
	return buf
}

// GetLocalID parses a slice of bytes into a LocalID.
func GetLocalID(b []byte) (id LocalID, err error) {
	if len(b) != 2 {
		err = fmt.Errorf("Illegal slice size (%d) supplied to GetLocalID()", len(b))
		return
	}
	id = LocalID(binary.LittleEndian.Uint16(b))
	return
}

// Config is a map of keyword to arbitrary data to specify configurations via keyword.
type Config map[string]interface{}
