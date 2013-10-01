/*
	This file defines the base identification and indexing schemes used within DVID.
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

	MaxLocalID   = 0xFFFF
	MaxLocalID32 = 0xFFFFFFFF
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

// Index is a one-dimensional index, typically constructed using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to a 1-D index.
// It is assumed that implementations for this interface are castable to []byte.
type Index interface {
	// Bytes returns a byte representation of the Index.
	Bytes() []byte

	// BytesToIndex returns an Index from a byte representation
	IndexFromBytes(b []byte) (Index, error)

	// Hash provides a consistent mapping from an Index to an integer (0,n]
	Hash(n int) int

	// Scheme returns a string describing the indexing scheme.
	Scheme() string

	// String returns a hexadecimal string representation
	String() string
}

// IndexIterator is a function that returns a sequence of indices and ends with nil.
type IndexIterator func() Index

// IndexIteratorMakers can make new IndexIterators.
type IndexIteratorMaker interface {
	NewIndexIterator() IndexIterator
}

// IndexUint8 satisfies an Index interface with an 8-bit unsigned integer index.
type IndexUint8 uint8

func (i IndexUint8) String() string {
	return fmt.Sprintf("%x", i)
}

// Bytes returns a byte representation of the Index.
func (i IndexUint8) Bytes() []byte {
	return []byte{byte(i)}
}

// Hash returns an integer [0, n)
func (i IndexUint8) Hash(n int) int {
	return int(i) % n
}

func (i IndexUint8) Scheme() string {
	return "Unsigned 8-bit Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexUint8) IndexFromBytes(b []byte) (Index, error) {
	return IndexUint8(b[0]), nil
}

// IndexRange defines a range of indices.  Since an Index is one-dimensional, a minimum
// and maximum Index defines a range.
type IndexRange struct {
	Minimum, Maximum Index
}
