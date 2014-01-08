/*
	This file defines the base identification and indexing schemes used within DVID.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
)

func init() {
	// Register types that may fulfill interface for Gob
	gob.Register(IndexUint8(0))
	gob.Register(IndexZYX{})
	gob.Register(IndexCZYX{})
}

// LocalID is a unique id for some data in a DVID instance.  This unique id is presumably
// a much smaller representation than the actual data (e.g., a version UUID or dataset
// description) and can be represented with fewer bytes in keys.
type LocalID uint16

// LocalID32 is a 32-bit unique id within this DVID instance.
type LocalID32 uint32

const (
	LocalIDSize   = 2
	LocalID32Size = 4

	MaxLocalID   = 0xFFFF
	MaxLocalID32 = 0xFFFFFFFF
)

// Bytes returns a sequence of bytes encoding this LocalID.
func (id LocalID) Bytes() []byte {
	buf := make([]byte, LocalIDSize, LocalIDSize)
	binary.BigEndian.PutUint16(buf, uint16(id))
	return buf
}

// LocalIDFromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalIDFromBytes(b []byte) (id LocalID, length int) {
	return LocalID(binary.BigEndian.Uint16(b)), LocalIDSize
}

// Bytes returns a sequence of bytes encoding this LocalID32.
func (id LocalID32) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// LocalID32FromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalID32FromBytes(b []byte) (id LocalID32, length int) {
	return LocalID32(binary.BigEndian.Uint32(b)), LocalID32Size
}

// Index is a one-dimensional index, typically constructed using some sort of
// spatiotemporal indexing scheme.  For example, Z-curves map n-D space to a 1-D index.
// It is assumed that implementations for this interface are castable to []byte.
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

// PointIndexer adds Point access to an index.
type PointIndexer interface {
	Index

	// Value returns the index point's value for the specified dimension without checking dim bounds.
	Value(dim uint8) uint32

	// FirstPoint returns the first point within a chunk given an index's iteration.
	FirstPoint(size Point) Point

	// LastPoint returns the last point within a chunk given an index's iteration.
	LastPoint(size Point) Point

	// Min returns a PointIndexer that is the minimum of its value and the passed one.
	Min(PointIndexer) (min PointIndexer, changed bool)

	// Max returns a PointIndexer that is the maximum of its value and the passed one.
	Max(PointIndexer) (max PointIndexer, changed bool)
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
		Error("Could not write to fnv hash in IndexBytes.Hash()")
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
		Error("Could not write to fnv hash in IndexString.Hash()")
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
// sequential access of negative/positive coordinates.  Conversion to and
// from signed point space is handled automatically through the Chunkable
// interface.
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
// integer space as consecutive in binary representation, i.e., BigEndian.
func (i IndexZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i[2])
	binary.Write(buf, binary.BigEndian, i[1])
	binary.Write(buf, binary.BigEndian, i[0])
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
	z := binary.BigEndian.Uint32(b[0:4])
	y := binary.BigEndian.Uint32(b[4:8])
	x := binary.BigEndian.Uint32(b[8:12])
	return &IndexZYX{x, y, z}, nil
}

// ------- PointIndexer interface ----------

// Value returns the value at the specified dimension for this index.
func (i IndexZYX) Value(dim uint8) uint32 {
	return i[dim]
}

// FirstPoint returns the first voxel coordinate for a chunk based on the index's
// standard iteration.
func (i IndexZYX) FirstPoint(size Point) Point {
	return Point3d{
		int32(int64(i[0])*int64(size.Value(0)) - middleValue),
		int32(int64(i[1])*int64(size.Value(1)) - middleValue),
		int32(int64(i[2])*int64(size.Value(2)) - middleValue),
	}
}

// LastPoint returns the last voxel coordinate for a chunk based on the index's
// standard iteration.
func (i IndexZYX) LastPoint(size Point) Point {
	return Point3d{
		int32(int64(i[0]+1)*int64(size.Value(0)) - middleValue - 1),
		int32(int64(i[1]+1)*int64(size.Value(1)) - middleValue - 1),
		int32(int64(i[2]+1)*int64(size.Value(2)) - middleValue - 1),
	}
}

// Min returns a PointIndexer that is the minimum of its value and the passed one.
func (i IndexZYX) Min(idx PointIndexer) (PointIndexer, bool) {
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

// Max returns a PointIndexer that is the maximum of its value and the passed one.
func (i IndexZYX) Max(idx PointIndexer) (PointIndexer, bool) {
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
	geom     Geometry
	x, y, z  uint32
	begBlock ChunkPoint3d
	endBlock ChunkPoint3d
	endBytes []byte
}

// NewIndexZYXIterator returns an IndexIterator that iterates over XYZ space.
func NewIndexZYXIterator(geom Geometry, start, end ChunkPoint3d) *IndexZYXIterator {
	return &IndexZYXIterator{
		geom:     geom,
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
// then Z, then Y, then X.  Since IndexZYX is embedded, we get PointIndexer interface.
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
	geom     Geometry
	x, y, z  uint32
	begBlock ChunkPoint3d
	endBlock ChunkPoint3d
	endBytes []byte
}

// NewIndexCZYXIterator returns an IndexIterator that iterates over XYZ space for a C.
func NewIndexCZYXIterator(channel int32, geom Geometry, start, end ChunkPoint3d) *IndexCZYXIterator {
	endIndex := IndexCZYX{channel, IndexZYX{end[0], end[1], end[2]}}
	return &IndexCZYXIterator{
		channel:  channel,
		geom:     geom,
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
