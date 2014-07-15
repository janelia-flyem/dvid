/*
	This file defines datatype-specific components of keys that provide an index into
	the key/value pairs associated with a data instance.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
)

func init() {
	// Register Index implementations that may fulfill interface for Gob
	gob.Register(IndexUint8(0))
	gob.Register(IndexZYX{})
	gob.Register(IndexCZYX{})
}

// Index is the datatype-specific (usually spatiotemporal) key that allows
// partitioning of the data.  In the case of voxels, this could be an IndexZYX
// implementation that uses a 3d coordinate packed into a slice of bytes.  For
// the keyvalue datatype, the index is simply a string.
type Index interface {
	// Duplicate returns a duplicate Index
	Duplicate() Index

	// Bytes returns a byte representation of the Index.  Integer components of
	// the Index should probably be serialized in big endian for improved
	// lexicographic ordering.
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

// ChunkIndexer adds chunk point access to an index.
type ChunkIndexer interface {
	Index

	ChunkPoint

	// Min returns a ChunkIndexer that is the minimum of its value and the passed one.
	Min(ChunkIndexer) (min ChunkIndexer, changed bool)

	// Max returns a ChunkIndexer that is the maximum of its value and the passed one.
	Max(ChunkIndexer) (max ChunkIndexer, changed bool)
}

// KeyToChunkIndexer takes a Key and returns an implementation of a ChunkIndexer if possible.
func KeyToChunkIndexer(key *DataKey) (ChunkIndexer, error) {
	ptIndex, ok := key.index.(ChunkIndexer)
	if !ok {
		return nil, fmt.Errorf("Can't convert DataKey.Index (%s) to ChunkIndexer",
			reflect.TypeOf(key.index))
	}
	return ptIndex, nil
}

// IndexIterator is a function that returns a sequence of indices and ends with nil.
type IndexIterator interface {
	Valid() bool
	IndexSpan() (beg, end Index, err error)
	NextSpan()
}

// IndexRange defines the extent of data via minimum and maximum indices.
type IndexRange struct {
	Minimum, Maximum Index
}

// ---- Index Implementations --------

// IndexBytes satisfies an Index interface with a slice of bytes.
type IndexBytes []byte

func (i IndexBytes) Duplicate() Index {
	dup := make(IndexBytes, len(i))
	copy(dup, i)
	return dup
}

func (i IndexBytes) String() string {
	return string(i)
}

func (i IndexBytes) Bytes() []byte {
	return []byte(i)
}

func (i IndexBytes) Hash(n int) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(i))
	if err != nil {
		Errorf("Could not write to fnv hash in IndexBytes.Hash()")
		return 0
	}
	return int(hash.Sum32()) % n
}

func (i IndexBytes) Scheme() string {
	return "Bytes Indexing"
}

func (i IndexBytes) IndexFromBytes(b []byte) (Index, error) {
	return IndexBytes(b), nil
}

// IndexString satisfies an Index interface with a string.
type IndexString string

func (i IndexString) Duplicate() Index {
	return i
}

func (i IndexString) String() string {
	return string(i)
}

func (i IndexString) Bytes() []byte {
	return []byte(i)
}

func (i IndexString) Hash(n int) int {
	hash := fnv.New32()
	_, err := hash.Write(i.Bytes())
	if err != nil {
		Errorf("Could not write to fnv hash in IndexString.Hash()")
		return 0
	}
	return int(hash.Sum32()) % n
}

func (i IndexString) Scheme() string {
	return "String Indexing"
}

func (i IndexString) IndexFromBytes(b []byte) (Index, error) {
	return IndexString(b), nil
}

// IndexUint8 satisfies an Index interface with an 8-bit unsigned integer index.
type IndexUint8 uint8

func (i IndexUint8) Duplicate() Index {
	return i
}

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

// IndexZYX implements the Index interface and provides simple indexing on Z,
// then Y, then X.  Note that index elements are unsigned to better handle
// sequential access of negative/positive coordinates.
// The binary representation of an index must behave reasonably for both negative and
// positive coordinates, e.g., when moving from -1 to 0 the binary representation isn't
// discontinous so the lexicographical ordering switches.  The simplest way to achieve
// this is to convert to an unsigned (positive) integer space where all coordinates are
// greater or equal to (0,0,...).
type IndexZYX ChunkPoint3d

var (
	MaxIndexZYX = IndexZYX(MaxChunkPoint3d)
	MinIndexZYX = IndexZYX(MinChunkPoint3d)
)

const IndexZYXSize = ChunkPoint3dSize

func (i IndexZYX) Duplicate() Index {
	dup := i
	return dup
}

func (i IndexZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.  This should layout
// integer space as consecutive in binary representation so we use
// bigendian and convert signed integer space to unsigned integer space.
func (i IndexZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(int64(i[2])-math.MinInt32))
	binary.Write(buf, binary.BigEndian, uint32(int64(i[1])-math.MinInt32))
	binary.Write(buf, binary.BigEndian, uint32(int64(i[0])-math.MinInt32))
	return buf.Bytes()
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IndexZYX) Hash(n int) int {
	return int(i[0]+i[1]+i[2]) % n
}

func (i IndexZYX) Scheme() string {
	return "ZYX Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexZYX) IndexFromBytes(b []byte) (Index, error) {
	z := int32(int64(binary.BigEndian.Uint32(b[0:4])) + math.MinInt32)
	y := int32(int64(binary.BigEndian.Uint32(b[4:8])) + math.MinInt32)
	x := int32(int64(binary.BigEndian.Uint32(b[8:12])) + math.MinInt32)
	return &IndexZYX{x, y, z}, nil
}

// ------- ChunkIndexer interface ----------

func (i IndexZYX) NumDims() uint8 {
	return 3
}

// Value returns the value at the specified dimension for this index.
func (i IndexZYX) Value(dim uint8) int32 {
	return i[dim]
}

// MinPoint returns the minimum voxel coordinate for a chunk.
func (i IndexZYX) MinPoint(size Point) Point {
	return ChunkPoint3d(i).MinPoint(size)
}

// MaxPoint returns the maximum voxel coordinate for a chunk.
func (i IndexZYX) MaxPoint(size Point) Point {
	return ChunkPoint3d(i).MaxPoint(size)
}

// Min returns a ChunkIndexer that is the minimum of its value and the passed one.
func (i IndexZYX) Min(idx ChunkIndexer) (ChunkIndexer, bool) {
	var changed bool
	min := i
	if min[0] > idx.Value(0) {
		min[0] = idx.Value(0)
		changed = true
	}
	if min[1] > idx.Value(1) {
		min[1] = idx.Value(1)
		changed = true
	}
	if min[2] > idx.Value(2) {
		min[2] = idx.Value(2)
		changed = true
	}
	return min, changed
}

// Max returns a ChunkIndexer that is the maximum of its value and the passed one.
func (i IndexZYX) Max(idx ChunkIndexer) (ChunkIndexer, bool) {
	var changed bool
	max := i
	if max[0] < idx.Value(0) {
		max[0] = idx.Value(0)
		changed = true
	}
	if max[1] < idx.Value(1) {
		max[1] = idx.Value(1)
		changed = true
	}
	if max[2] < idx.Value(2) {
		max[2] = idx.Value(2)
		changed = true
	}
	return max, changed
}

// ----- IndexIterator implementation ------------
type IndexZYXIterator struct {
	x, y, z  int32
	begBlock ChunkPoint3d
	endBlock ChunkPoint3d
	endBytes []byte
}

// NewIndexZYXIterator returns an IndexIterator that iterates over XYZ space.
func NewIndexZYXIterator(start, end ChunkPoint3d) *IndexZYXIterator {
	return &IndexZYXIterator{
		x:        start[0],
		y:        start[1],
		z:        start[2],
		begBlock: start,
		endBlock: end,
		endBytes: IndexZYX(end).Bytes(),
	}
}

func (it *IndexZYXIterator) Valid() bool {
	cursorBytes := IndexZYX{it.x, it.y, it.z}.Bytes()
	if bytes.Compare(cursorBytes, it.endBytes) > 0 {
		return false
	}
	return true
}

func (it *IndexZYXIterator) IndexSpan() (beg, end Index, err error) {
	beg = IndexZYX{it.begBlock[0], it.y, it.z}
	end = IndexZYX{it.endBlock[0], it.y, it.z}
	return
}

func (it *IndexZYXIterator) NextSpan() {
	it.x = it.begBlock[0]
	it.y += 1
	if it.y > it.endBlock[1] {
		it.y = it.begBlock[1]
		it.z += 1
	}
}

// IndexCZYX implements the Index interface and provides simple indexing on "channel" C,
// then Z, then Y, then X.  Since IndexZYX is embedded, we get ChunkIndexer interface.
type IndexCZYX struct {
	Channel int32
	IndexZYX
}

func (i IndexCZYX) Duplicate() Index {
	dup := i
	return dup
}

func (i IndexCZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i IndexCZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i.Channel)
	buf.Write(i.IndexZYX.Bytes())
	return buf.Bytes()
}

func (i IndexCZYX) Scheme() string {
	return "CZYX Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i IndexCZYX) IndexFromBytes(b []byte) (Index, error) {
	c := int32(binary.BigEndian.Uint16(b[0:4]))
	index, err := i.IndexFromBytes(b[4:])
	if err != nil {
		return nil, err
	}
	return &IndexCZYX{c, index.(IndexZYX)}, nil
}

// ----- IndexIterator implementation ------------
type IndexCZYXIterator struct {
	channel  int32
	x, y, z  int32
	begBlock ChunkPoint3d
	endBlock ChunkPoint3d
	endBytes []byte
}

// NewIndexCZYXIterator returns an IndexIterator that iterates over XYZ space for a C.
func NewIndexCZYXIterator(channel int32, start, end ChunkPoint3d) *IndexCZYXIterator {
	endIndex := IndexCZYX{channel, IndexZYX{end[0], end[1], end[2]}}
	return &IndexCZYXIterator{
		channel:  channel,
		x:        start[0],
		y:        start[1],
		z:        start[2],
		begBlock: start,
		endBlock: end,
		endBytes: endIndex.Bytes(),
	}
}

func (it *IndexCZYXIterator) Valid() bool {
	cursorBytes := IndexCZYX{it.channel, IndexZYX{it.x, it.y, it.z}}.Bytes()
	if bytes.Compare(cursorBytes, it.endBytes) > 0 {
		return false
	}
	return true
}

func (it *IndexCZYXIterator) IndexSpan() (beg, end Index, err error) {
	beg = IndexCZYX{it.channel, IndexZYX{it.begBlock[0], it.y, it.z}}
	end = IndexCZYX{it.channel, IndexZYX{it.endBlock[0], it.y, it.z}}
	return
}

func (it *IndexCZYXIterator) NextSpan() {
	it.x = it.begBlock[0]
	it.y += 1
	if it.y > it.endBlock[1] {
		it.y = it.begBlock[1]
		it.z += 1
	}
}

// TODO -- Morton (Z-order) curve
type IndexMorton []byte

func (i IndexMorton) Scheme() string {
	return "Morton/Z-order Indexing"
}

// TODO -- Hilbert curve
type IndexHilbert []byte

func (i IndexHilbert) Scheme() string {
	return "Hilbert Indexing"
}
