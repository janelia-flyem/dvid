/*
	This file defines index types that can be used as datatype-specific components of keys (TKey).
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
	"sync"
)

func init() {
	// Register Index implementations that may fulfill interface for Gob
	//var indexBytes = IndexBytes{}
	gob.Register(&IndexBytes{})
	var indexString IndexString
	gob.Register(&indexString)
	var indexUint8 = IndexUint8(0)
	gob.Register(&indexUint8)
	gob.Register(&IndexZYX{})
	gob.Register(&IndexCZYX{})
}

// Index provides partioning of the data, typically in spatiotemporal ways.
// In the case of voxels, this could be an IndexZYX implementation that uses a
// 3d coordinate packed into a slice of bytes.  It can be used to fill a TKey.
type Index interface {
	// Duplicate returns a duplicate Index
	Duplicate() Index

	// Bytes returns a byte representation of the Index.  Integer components of
	// the Index should probably be serialized in big endian for improved
	// lexicographic ordering.
	Bytes() []byte

	// IndexFromBytes sets the receiver from the given bytes.
	IndexFromBytes([]byte) error

	// Scheme returns a string describing the indexing scheme.
	Scheme() string

	// String returns a human readable description of the Index.
	String() string
}

// Hashable is a type that provies a function to hash itself to an integer range.
type Hashable interface {
	// Hash provides a consistent mapping from an Index to an integer (0,n]
	Hash(n int) int
}

// ChunkIndexer adds chunk point access to an index.
type ChunkIndexer interface {
	Index

	ChunkPoint

	// Min returns a ChunkIndexer that is the minimum of its value and the passed one.
	Min(ChunkIndexer) (min ChunkIndexer, changed bool)

	// Max returns a ChunkIndexer that is the maximum of its value and the passed one.
	Max(ChunkIndexer) (max ChunkIndexer, changed bool)

	// DuplicateChunkIndexer returns a duplicate that can act as a ChunkIndexer.
	DuplicateChunkIndexer() ChunkIndexer
}

// IndexIterator is an interface that can return a sequence of indices.
type IndexIterator interface {
	Valid() bool
	IndexSpan() (beg, end Index, err error)
	NextSpan()
}

// Subsetter can tell us its range of Index and how much it has actually
// available in this server.  It's used to implement limited cloning,
// e.g., only cloning a quarter of an image volume.
// TODO: Fulfill implementation for voxels data type.
type Subsetter interface {
	// MaximumExtents returns a range of indices for which data is available at
	// some DVID server.
	MaximumExtents() IndexRange

	// AvailableExtents returns a range of indices for which data is available
	// at this DVID server.  It is the currently available extents.
	AvailableExtents() IndexRange
}

// IndexRange defines the extent of data via minimum and maximum indices.
type IndexRange struct {
	Minimum, Maximum Index
}

// IndexBytes satisfies an Index interface with a slice of bytes.
type IndexBytes []byte

func (i *IndexBytes) Hash(n int) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(*i))
	if err != nil {
		Errorf("Could not write to fnv hash in IndexBytes.Hash()")
		return 0
	}
	return int(hash.Sum32()) % n
}

// ---- Index interface implementation --------

func (i *IndexBytes) Duplicate() Index {
	dup := make(IndexBytes, len(*i))
	copy(dup, *i)
	return &dup
}

func (i *IndexBytes) String() string {
	return string(*i)
}

func (i *IndexBytes) Bytes() []byte {
	return []byte(*i)
}

func (i *IndexBytes) Scheme() string {
	return "Bytes Indexing"
}

func (i *IndexBytes) IndexFromBytes(b []byte) error {
	*i = IndexBytes(b)
	return nil
}

// IndexString satisfies an Index interface with a string.
type IndexString string

func (i *IndexString) Hash(n int) int {
	hash := fnv.New32()
	_, err := hash.Write(i.Bytes())
	if err != nil {
		Errorf("Could not write to fnv hash in IndexString.Hash()")
		return 0
	}
	return int(hash.Sum32()) % n
}

// ---- Index interface implementation --------

func (i *IndexString) Duplicate() Index {
	return i
}

func (i *IndexString) String() string {
	return string(*i)
}

func (i *IndexString) Bytes() []byte {
	return []byte(*i)
}

func (i *IndexString) Scheme() string {
	return "String Indexing"
}

func (i *IndexString) IndexFromBytes(b []byte) error {
	*i = IndexString(b)
	return nil
}

// IndexUint8 satisfies an Index interface with an 8-bit unsigned integer index.
type IndexUint8 uint8

// Hash returns an integer [0, n)
func (i *IndexUint8) Hash(n int) int {
	return int(*i) % n
}

// ---- Index interface implementation --------

func (i *IndexUint8) Duplicate() Index {
	return i
}

func (i *IndexUint8) String() string {
	return fmt.Sprintf("%x", *i)
}

// Bytes returns a byte representation of the Index.
func (i *IndexUint8) Bytes() []byte {
	return []byte{byte(*i)}
}

func (i *IndexUint8) Scheme() string {
	return "Unsigned 8-bit Indexing"
}

func (i *IndexUint8) IndexFromBytes(b []byte) error {
	if len(b) != 1 {
		return fmt.Errorf("IndexUint8 should only be one byte not %d", len(b))
	}
	*i = IndexUint8(b[0])
	return nil
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

func (i *IndexZYX) Unpack() (x, y, z int32) {
	return i[0], i[1], i[2]
}

func (i *IndexZYX) ToIZYXString() IZYXString {
	return IZYXString(i.Bytes())
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
// The underlying algorithm is identical to use of IZYXString so the has for either
// an IZYXString or IndexZYX representation will return the identical hash.
func (i *IndexZYX) Hash(n int) int {
	return IZYXString(i.Bytes()).Hash(n)
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface and stores
// index ZYX as X, Y, Z in little endian int32 format.  Note that this should NOT
// be used when creating a DVID key; use Bytes(), which stores big endian for
// better lexicographic ordering.
func (i *IndexZYX) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	for dim := 0; dim < 3; dim++ {
		if err := binary.Write(&buf, binary.LittleEndian, (*i)[dim]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface and sets
// index ZYX using little endian int32 format.  Note that this should NOT
// be used when retrieving a DVID key; use IndexFromBytes(), which decodes big
// endian due to key lexicographic ordering.
func (i *IndexZYX) UnmarshalBinary(b []byte) error {
	buf := bytes.NewBuffer(b)
	if len(b) != 12 {
		return fmt.Errorf("Bad IndexZYX serialization.  Has length %d bytes != 12", len(b))
	}
	for dim := 0; dim < 3; dim++ {
		var value int32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return fmt.Errorf("Bad IndexZYX elem %d read: %v", dim, err)
		}
		(*i)[dim] = value
	}
	return nil
}

// ---- Index interface implementation

func (i *IndexZYX) Duplicate() Index {
	dup := *i
	return &dup
}

// String produces a pretty printable string of the index in hex format.
func (i *IndexZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

func (i *IndexZYX) Scheme() string {
	return "ZYX Indexing"
}

// Bytes returns a byte representation of the Index.  This should layout
// integer space as consecutive in binary representation so we use
// bigendian and convert signed integer space to unsigned integer space.
func (i *IndexZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(int64((*i)[2])-math.MinInt32))
	binary.Write(buf, binary.BigEndian, uint32(int64((*i)[1])-math.MinInt32))
	binary.Write(buf, binary.BigEndian, uint32(int64((*i)[0])-math.MinInt32))
	return buf.Bytes()
}

// IndexFromBytes returns an index from key bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i *IndexZYX) IndexFromBytes(b []byte) error {
	if len(b) != 12 {
		return fmt.Errorf("Illegal byte length (%d) for IndexZYX", len(b))
	}
	z := int32(int64(binary.BigEndian.Uint32(b[0:4])) + math.MinInt32)
	y := int32(int64(binary.BigEndian.Uint32(b[4:8])) + math.MinInt32)
	x := int32(int64(binary.BigEndian.Uint32(b[8:12])) + math.MinInt32)
	*i = IndexZYX{x, y, z}
	return nil
}

// ----- ChunkIndexer interface implementation

func (i *IndexZYX) NumDims() uint8 {
	return 3
}

// Value returns the value at the specified dimension for this index.
func (i *IndexZYX) Value(dim uint8) int32 {
	return (*i)[dim]
}

// MinPoint returns the minimum voxel coordinate for a chunk.
func (i *IndexZYX) MinPoint(size Point) Point {
	return ChunkPoint3d(*i).MinPoint(size)
}

// MaxPoint returns the maximum voxel coordinate for a chunk.
func (i *IndexZYX) MaxPoint(size Point) Point {
	return ChunkPoint3d(*i).MaxPoint(size)
}

// DuplicateChunkIndexer returns a duplicate that can act as a ChunkIndexer.
func (i *IndexZYX) DuplicateChunkIndexer() ChunkIndexer {
	dup := *i
	return &dup
}

// Min returns a ChunkIndexer that is the minimum of its value and the passed one.
func (i *IndexZYX) Min(idx ChunkIndexer) (ChunkIndexer, bool) {
	var changed bool
	min := *i
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
	return &min, changed
}

// Max returns a ChunkIndexer that is the maximum of its value and the passed one.
func (i *IndexZYX) Max(idx ChunkIndexer) (ChunkIndexer, bool) {
	var changed bool
	max := *i
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
	return &max, changed
}

// IZYXString is the stringified version of IndexZYX.Bytes(), optimized for lexicographic
// ordering.
type IZYXString string

// IndexZYX returns an index from a string representation of the coordinate.
func (i IZYXString) IndexZYX() (IndexZYX, error) {
	var idx IndexZYX
	if err := idx.IndexFromBytes([]byte(i)); err != nil {
		return idx, err
	}
	return idx, nil
}

// Unpack returns the constituent x, y, and z coordinates of the corresponding index.
func (i IZYXString) Unpack() (x, y, z int32, err error) {
	var idx IndexZYX
	if err = idx.IndexFromBytes([]byte(i)); err != nil {
		return
	}
	x, y, z = idx.Unpack()
	return
}

func (i IZYXString) Z() (int32, error) {
	idx, err := i.IndexZYX()
	if err != nil {
		return 0, err
	}
	return idx[2], nil
}

func (i IZYXString) ToChunkPoint3d() (ChunkPoint3d, error) {
	var idx IndexZYX
	err := idx.IndexFromBytes([]byte(i))
	return ChunkPoint3d(idx), err
}

// String returns a nicely formatted string of the 3d block coordinate or "corrupted coordinate".
func (i IZYXString) String() string {
	idx, err := i.IndexZYX()
	if err != nil {
		return "corrupted coordinate"
	}
	return fmt.Sprintf("(%d, %d, %d)", idx[0], idx[1], idx[2])
}

// Hash returns an integer [0, n) where the returned values should be reasonably
// spread among the range of returned values.  This implementation makes sure
// that any range query along x, y, or z direction will map to different handlers.
func (i IZYXString) Hash(n int) int {
	h := fnv.New32a()
	h.Write([]byte(i))
	return int(h.Sum32()) % n
}

// Downres returns an IZYXString representing a chunk point divided in two.
func (i IZYXString) Downres() (IZYXString, error) {
	chunkPt, err := i.ToChunkPoint3d()
	if err != nil {
		return "", err
	}
	dx := chunkPt[0] >> 1
	dy := chunkPt[1] >> 1
	dz := chunkPt[2] >> 1
	downresPt := ChunkPoint3d{dx, dy, dz}
	return downresPt.ToIZYXString(), nil
}

// BlockCounts is a thread-safe type for counting block references.
type BlockCounts struct {
	sync.RWMutex
	m map[IZYXString]int
}

// Incr increments the count for a block.
func (c *BlockCounts) Incr(block IZYXString) {
	if c.m == nil {
		c.m = make(map[IZYXString]int)
	}
	c.Lock()
	defer c.Unlock()
	c.m[block] = c.m[block] + 1
}

// Decr decrements the count for a block.
func (c *BlockCounts) Decr(block IZYXString) {
	if c.m == nil {
		c.m = make(map[IZYXString]int)
	}
	c.Lock()
	defer c.Unlock()
	c.m[block] = c.m[block] - 1
	if c.m[block] == 0 {
		delete(c.m, block)
	}
}

// Value returns the count for a block.
func (c *BlockCounts) Value(block IZYXString) int {
	if c.m == nil {
		return 0
	}
	c.RLock()
	defer c.RUnlock()
	return c.m[block]
}

// Empty returns true if there are no counts.
func (c *BlockCounts) Empty() bool {
	if len(c.m) == 0 {
		return true
	}
	return false
}

// DirtyBlocks tracks dirty labels across versions
type DirtyBlocks struct {
	sync.RWMutex
	dirty map[InstanceVersion]*BlockCounts
}

func (d *DirtyBlocks) Incr(iv InstanceVersion, block IZYXString) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[InstanceVersion]*BlockCounts)
	}
	d.incr(iv, block)
}

func (d *DirtyBlocks) Decr(iv InstanceVersion, block IZYXString) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[InstanceVersion]*BlockCounts)
	}
	d.decr(iv, block)
}

func (d *DirtyBlocks) IsDirty(iv InstanceVersion, block IZYXString) bool {
	d.RLock()
	defer d.RUnlock()

	if d.dirty == nil {
		return false
	}

	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		return false
	}
	if cnts.Value(block) == 0 {
		return false
	}
	return true
}

func (d *DirtyBlocks) Empty(iv InstanceVersion) bool {
	d.RLock()
	defer d.RUnlock()

	if len(d.dirty) == 0 {
		return true
	}
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		return true
	}
	return cnts.Empty()
}

func (d *DirtyBlocks) incr(iv InstanceVersion, block IZYXString) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		cnts = new(BlockCounts)
		d.dirty[iv] = cnts
	}
	cnts.Incr(block)
}

func (d *DirtyBlocks) decr(iv InstanceVersion, block IZYXString) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		Errorf("decremented non-existant count for block %s, version %v\n", block, iv)
		return
	}
	cnts.Decr(block)
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
	index := IndexZYX(end)
	return &IndexZYXIterator{
		x:        start[0],
		y:        start[1],
		z:        start[2],
		begBlock: start,
		endBlock: end,
		endBytes: (&index).Bytes(),
	}
}

func (it *IndexZYXIterator) Valid() bool {
	index := &IndexZYX{it.x, it.y, it.z}
	cursorBytes := index.Bytes()
	if bytes.Compare(cursorBytes, it.endBytes) > 0 {
		return false
	}
	return true
}

func (it *IndexZYXIterator) IndexSpan() (beg, end Index, err error) {
	beg = &IndexZYX{it.begBlock[0], it.y, it.z}
	end = &IndexZYX{it.endBlock[0], it.y, it.z}
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

func (i *IndexCZYX) Duplicate() Index {
	dup := i
	return dup
}

func (i *IndexCZYX) String() string {
	return hex.EncodeToString(i.Bytes())
}

// Bytes returns a byte representation of the Index.
func (i *IndexCZYX) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i.Channel)
	buf.Write(i.IndexZYX.Bytes())
	return buf.Bytes()
}

func (i *IndexCZYX) Scheme() string {
	return "CZYX Indexing"
}

// IndexFromBytes returns an index from bytes.  The passed Index is used just
// to choose the appropriate byte decoding scheme.
func (i *IndexCZYX) IndexFromBytes(b []byte) error {
	i.Channel = int32(binary.BigEndian.Uint16(b[0:4]))
	if err := i.IndexZYX.IndexFromBytes(b[4:]); err != nil {
		return err
	}
	return nil
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
	index := &IndexCZYX{it.channel, IndexZYX{it.x, it.y, it.z}}
	cursorBytes := index.Bytes()
	if bytes.Compare(cursorBytes, it.endBytes) > 0 {
		return false
	}
	return true
}

func (it *IndexCZYXIterator) IndexSpan() (beg, end Index, err error) {
	beg = &IndexCZYX{it.channel, IndexZYX{it.begBlock[0], it.y, it.z}}
	end = &IndexCZYX{it.channel, IndexZYX{it.endBlock[0], it.y, it.z}}
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
