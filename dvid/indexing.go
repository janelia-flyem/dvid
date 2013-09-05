/*
	This file defines the interface to indexing schemes.
*/

package dvid

import "fmt"

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
