/*
	This file defines the interface to indexing schemes.
*/

package dvid

// ChunkIndex represents an index to a datatype-specific partition of data.
// For example, a voxels data type would have a 3d block coordinate.  Chunk
// indices do *not* have to be 3d coordinates but can be optimized for
// a data type's internal data structure.
type ChunkIndex interface {
	// MakeIndex returns a one-dimensional index.
	MakeIndex() (Index, error)

	// String returns a human-readable representation of the block index.
	String() string
}

// Index is a one-dimensional index, typically constructed using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to a 1-D index.
// It is assumed that implementations for this interface are castable to []byte.
type Index interface {
	// Bytes gives a slice of bytes.
	Bytes() []byte

	// InvertIndex returns a chunk index given a one-dimensional Index.
	// It is the inverse of MakeIndex().
	InvertIndex() ChunkIndex

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
