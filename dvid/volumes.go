/*
	This file contains code that supports sparse and binary volumes.
*/

package dvid

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
)

func init() {
	sqrt3div3 := math.Sqrt(3.0) / 3.0
	sqrt2div2 := math.Sqrt(2.0) / 2.0

	// Initialize Zucker-Hummel 3x3x3 filter

	// Fill in z = 0 for Z gradient kernel
	zhZ[0][0][0] = -sqrt3div3
	zhZ[1][0][0] = -sqrt2div2
	zhZ[2][0][0] = -sqrt3div3

	zhZ[0][1][0] = -sqrt2div2
	zhZ[1][1][0] = -1.0
	zhZ[2][1][0] = -sqrt2div2

	zhZ[0][2][0] = -sqrt3div3
	zhZ[1][2][0] = -sqrt2div2
	zhZ[2][2][0] = -sqrt3div3

	// Fill in z=1,2 for Z gradient kernel
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			zhZ[x][y][1] = 0.0
			zhZ[x][y][2] = -zhZ[x][y][0]
		}
	}

	// Copy Z gradient kernel to X and Y gradient kernels
	for z := 0; z < 3; z++ {
		for y := 0; y < 3; y++ {
			for x := 0; x < 3; x++ {
				zhX[z][x][y] = zhZ[x][y][z]
				zhY[x][z][y] = zhZ[x][y][z]
			}
		}
	}
}

// Sparse Volume binary encoding payload descriptors.
const (
	// EncodingBinary denotes no payload bytes since binary sparse volume is
	// defined by just start and run length.
	EncodingBinary byte = 0x00

	// EncodingGrayscale8 denotes an 8-bit grayscale payload.
	EncodingGrayscale8 = 0x01

	// EncodingGrayscale16 denotes a 16-bit grayscale payload.
	EncodingGrayscale16 = 0x02

	// EncodingNormal16 denotes 16-bit encoded normals.
	EncodingNormal16 = 0x04
)

var (
	zhX, zhY, zhZ [3][3][3]float64
)

// Bounding provides two levels of optional bounds (voxel and block) as well as
// whether bounds must be exact to voxel level.
type Bounds struct {
	Voxel *OptionalBounds
	Block *OptionalBounds
	Exact bool // If false, just screen on blocks.
}

// OptionalBounds holds optional bounds in X, Y, and Z.
// This differs from Extents in allowing optional min
// and max bounds along each dimension.
type OptionalBounds struct {
	minx, maxx, miny, maxy, minz, maxz *int32
}

// OptionalBoundsFromQueryString returns OptionalBounds from a set of query strings.
func OptionalBoundsFromQueryString(r *http.Request) (*OptionalBounds, error) {
	bounds := new(OptionalBounds)
	queryStrings := r.URL.Query()

	var minx, maxx, miny, maxy, minz, maxz int32
	minxStr := queryStrings.Get("minx")
	if minxStr != "" {
		val, err := strconv.ParseInt(minxStr, 10, 32)
		if err != nil {
			return nil, err
		}
		minx = int32(val)
		bounds.minx = &minx
	}
	maxxStr := queryStrings.Get("maxx")
	if maxxStr != "" {
		val, err := strconv.ParseInt(maxxStr, 10, 32)
		if err != nil {
			return nil, err
		}
		maxx = int32(val)
		bounds.maxx = &maxx
	}
	minyStr := queryStrings.Get("miny")
	if minyStr != "" {
		val, err := strconv.ParseInt(minyStr, 10, 32)
		if err != nil {
			return nil, err
		}
		miny = int32(val)
		bounds.miny = &miny
	}
	maxyStr := queryStrings.Get("maxy")
	if maxyStr != "" {
		val, err := strconv.ParseInt(maxyStr, 10, 32)
		if err != nil {
			return nil, err
		}
		maxy = int32(val)
		bounds.maxy = &maxy
	}
	minzStr := queryStrings.Get("minz")
	if minzStr != "" {
		val, err := strconv.ParseInt(minzStr, 10, 32)
		if err != nil {
			return nil, err
		}
		minz = int32(val)
		bounds.minz = &minz
	}
	maxzStr := queryStrings.Get("maxz")
	if maxzStr != "" {
		val, err := strconv.ParseInt(maxzStr, 10, 32)
		if err != nil {
			return nil, err
		}
		maxz = int32(val)
		bounds.maxz = &maxz
	}
	return bounds, nil
}

func nilOrInt32(p *int32) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *p)
}

func (b *OptionalBounds) String() string {
	text := "OptionalBounds{\n"
	text += "  minx: " + nilOrInt32(b.minx) + "\n"
	text += "  maxx: " + nilOrInt32(b.maxx) + "\n"
	text += "  miny: " + nilOrInt32(b.miny) + "\n"
	text += "  maxy: " + nilOrInt32(b.maxy) + "\n"
	text += "  minz: " + nilOrInt32(b.minz) + "\n"
	text += "  maxz: " + nilOrInt32(b.maxz) + "\n"
	text += "}\n"
	return text
}

func (b *OptionalBounds) SetMinX(x int32) {
	b.minx = &x
}

func (b *OptionalBounds) SetMaxX(x int32) {
	b.maxx = &x
}

func (b *OptionalBounds) SetMinY(y int32) {
	b.miny = &y
}

func (b *OptionalBounds) SetMaxY(y int32) {
	b.maxy = &y
}

func (b *OptionalBounds) SetMinZ(z int32) {
	b.minz = &z
}

func (b *OptionalBounds) SetMaxZ(z int32) {
	b.maxz = &z
}

// Adjust modifies the given minimum and maximum point to reflect additional
// optional bounds.
func (b *OptionalBounds) Adjust(minPt, maxPt *Point3d) {
	if b == nil {
		return
	}
	if b.minx != nil {
		if *(b.minx) > (*minPt)[0] {
			(*minPt)[0] = *(b.minx)
		}
	}
	if b.maxx != nil {
		if *(b.maxx) < (*maxPt)[0] {
			(*maxPt)[0] = *(b.maxx)
		}
	}
	if b.miny != nil {
		if *(b.miny) > (*minPt)[1] {
			(*minPt)[1] = *(b.miny)
		}
	}
	if b.maxy != nil {
		if *(b.maxy) < (*maxPt)[1] {
			(*maxPt)[1] = *(b.maxy)
		}
	}
	if b.minz != nil {
		if *(b.minz) > (*minPt)[2] {
			(*minPt)[2] = *(b.minz)
		}
	}
	if b.maxz != nil {
		if *(b.maxz) < (*maxPt)[2] {
			(*maxPt)[2] = *(b.maxz)
		}
	}
}

func (b *OptionalBounds) MinX() (x int32, ok bool) {
	if b == nil || b.minx == nil {
		return
	}
	return *(b.minx), true
}

func (b *OptionalBounds) MaxX() (x int32, ok bool) {
	if b == nil || b.maxx == nil {
		return
	}
	return *(b.maxx), true
}

func (b *OptionalBounds) MinY() (y int32, ok bool) {
	if b == nil || b.miny == nil {
		return
	}
	return *(b.miny), true
}

func (b *OptionalBounds) MaxY() (y int32, ok bool) {
	if b == nil || b.maxy == nil {
		return
	}
	return *(b.maxy), true
}

func (b *OptionalBounds) MinZ() (z int32, ok bool) {
	if b == nil || b.minz == nil {
		return
	}
	return *(b.minz), true
}

func (b *OptionalBounds) MaxZ() (z int32, ok bool) {
	if b == nil || b.maxz == nil {
		return
	}
	return *(b.maxz), true
}

// Divide returns a new bounds that has all optionally set
// bounds divided by the given point.
func (b *OptionalBounds) Divide(pt Point3d) *OptionalBounds {
	if b == nil {
		return nil
	}
	newB := new(OptionalBounds)
	if b.minx != nil {
		newB.minx = new(int32)
		*(newB.minx) = *(b.minx) / pt[0]
	}
	if b.maxx != nil {
		newB.maxx = new(int32)
		*(newB.maxx) = *(b.maxx) / pt[0]
	}
	if b.miny != nil {
		newB.miny = new(int32)
		*(newB.miny) = *(b.miny) / pt[1]
	}
	if b.maxy != nil {
		newB.maxy = new(int32)
		*(newB.maxy) = *(b.maxy) / pt[1]
	}
	if b.minz != nil {
		newB.minz = new(int32)
		*(newB.minz) = *(b.minz) / pt[2]
	}
	if b.maxz != nil {
		newB.maxz = new(int32)
		*(newB.maxz) = *(b.maxz) / pt[2]
	}
	return newB
}

// BoundedX returns true if there is some bound set in the X dimension
func (b *OptionalBounds) BoundedX() bool {
	return b != nil && (b.minx != nil || b.maxx != nil)
}

// BoundedY returns true if there is some bound set in the Y dimension
func (b *OptionalBounds) BoundedY() bool {
	return b != nil && (b.miny != nil || b.maxy != nil)
}

// BoundedZ returns true if there is some bound set in the Z dimension
func (b *OptionalBounds) BoundedZ() bool {
	return b != nil && (b.minz != nil || b.maxz != nil)
}

// IsSet returns true if at least one bound in some dimension has been set.
func (b *OptionalBounds) IsSet() bool {
	if b == nil {
		return false
	}
	if b.minx != nil || b.maxx != nil || b.miny != nil || b.maxy != nil || b.minz != nil || b.maxz != nil {
		return true
	}
	return false
}

// OutsideX returns true if the given X is outside a set X bound.
func (b *OptionalBounds) OutsideX(x int32) bool {
	if b == nil {
		return false
	}
	if b.minx != nil && x < *(b.minx) {
		return true
	}
	if b.maxx != nil && x > *(b.maxx) {
		return true
	}
	return false
}

// OutsideY returns true if the given Y is outside a set Y bound.
func (b *OptionalBounds) OutsideY(y int32) bool {
	if b == nil {
		return false
	}
	if b.miny != nil && y < *(b.miny) {
		return true
	}
	if b.maxy != nil && y > *(b.maxy) {
		return true
	}
	return false
}

// OutsideZ returns true if the given Z is outside a set Z bound.
func (b *OptionalBounds) OutsideZ(z int32) bool {
	if b == nil {
		return false
	}
	if b.minz != nil && z < *(b.minz) {
		return true
	}
	if b.maxz != nil && z > *(b.maxz) {
		return true
	}
	return false
}

// Outside returns true if the given chunk point is outside any set bound.
func (b *OptionalBounds) Outside(pt ChunkPoint3d) bool {
	if b == nil {
		return false
	}
	if b.minx != nil && pt[0] < *(b.minx) {
		return true
	}
	if b.maxx != nil && pt[0] > *(b.maxx) {
		return true
	}
	if b.miny != nil && pt[1] < *(b.miny) {
		return true
	}
	if b.maxy != nil && pt[1] > *(b.maxy) {
		return true
	}
	if b.minz != nil && pt[2] < *(b.minz) {
		return true
	}
	if b.maxz != nil && pt[2] > *(b.maxz) {
		return true
	}
	return false
}

// BeyondZ returns true if the given Z is beyond a set max Z bound.
func (b *OptionalBounds) BeyondZ(pt ChunkPoint3d) bool {
	if b == nil {
		return false
	}
	if b.maxz != nil && pt[2] > *(b.maxz) {
		return true
	}
	return false
}

// RLE is a single run-length encoded span with a start coordinate and length along
// a coordinate (typically X).
type RLE struct {
	start  Point3d
	length int32
}

func NewRLE(start Point3d, length int32) RLE {
	return RLE{start, length}
}

// WriteTo fulfills the io.WriterTo interface.
func (rle RLE) WriteTo(w io.Writer) (n int64, err error) {
	var buf [16]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rle.start[0]))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(rle.start[1]))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(rle.start[2]))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(rle.length))
	var numBytes int
	numBytes, err = w.Write(buf[:])
	n = int64(numBytes)
	return
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (rle RLE) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rle.start[0]))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(rle.start[1]))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(rle.start[2]))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(rle.length))
	return buf, nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (rle *RLE) UnmarshalBinary(b []byte) error {
	lenEncoding := len(b)
	if lenEncoding != 16 {
		return fmt.Errorf("RLE encoding # bytes is not 16: %d", len(b))
	}
	x := int32(binary.LittleEndian.Uint32(b[0:4]))
	y := int32(binary.LittleEndian.Uint32(b[4:8]))
	z := int32(binary.LittleEndian.Uint32(b[8:12]))
	length := int32(binary.LittleEndian.Uint32(b[12:16]))
	rle.start = Point3d{x, y, z}
	rle.length = length
	return nil
}

func (rle RLE) StartPt() Point3d {
	return rle.start
}

func (rle RLE) Length() int32 {
	return rle.length
}

func (rle *RLE) Extend(n int32) {
	rle.length += n
}

func (rle RLE) String() string {
	return fmt.Sprintf("dvid.NewRLE{%s, %d}", rle.start, rle.length)
}

func (rle RLE) GetRangeIZYXString() (beg, end IZYXString) {
	var izyx IndexZYX
	izyx[0] = rle.start[0]
	izyx[1] = rle.start[1]
	izyx[2] = rle.start[2]
	beg = izyx.ToIZYXString()
	izyx[0] += rle.length - 1
	end = izyx.ToIZYXString()
	return
}

func (rle RLE) Within(pt Point3d) bool {
	if pt[2] != rle.start[2] || pt[1] != rle.start[1] {
		return false
	}
	if pt[0] < rle.start[0] || pt[0] >= rle.start[0]+rle.length {
		return false
	}
	return true
}

func (rle RLE) Intersects(rle2 RLE) bool {
	if rle.start[2] != rle2.start[2] || rle.start[1] != rle2.start[1] {
		return false
	}
	x0 := rle.start[0]
	x1 := x0 + rle.length - 1
	sx0 := rle2.start[0]
	sx1 := sx0 + rle2.length - 1
	if x0 > sx1 || x1 < sx0 {
		return false
	}
	return true
}

// Excise returns the portion of the receiver that is not in the passed RLE.
// If the RLEs do not intersect, nil is returned.  Up to two fragments can
// be generated and they will be in sorted in start X.
func (rle RLE) Excise(rle2 RLE) RLEs {
	if rle.start[2] != rle2.start[2] || rle.start[1] != rle2.start[1] {
		return nil
	}
	z := rle.start[2]
	y := rle.start[1]
	x0 := rle.start[0]
	x1 := x0 + rle.length - 1
	sx0 := rle2.start[0]
	sx1 := sx0 + rle2.length - 1
	if x0 > sx1 || x1 < sx0 {
		return nil
	}
	frags := RLEs{}
	if sx0 > x0 {
		frags = append(frags, RLE{Point3d{x0, y, z}, sx0 - x0})
	}
	if sx1 < x1 {
		frags = append(frags, RLE{Point3d{sx1 + 1, y, z}, x1 - sx1})
	}
	return frags
}

func (rle RLE) Less(rle2 RLE) bool {
	if rle.start[2] < rle2.start[2] {
		return true
	}
	if rle.start[2] > rle2.start[2] {
		return false
	}
	if rle.start[1] < rle2.start[1] {
		return true
	}
	if rle.start[1] > rle2.start[1] {
		return false
	}
	return rle.start[0] < rle2.start[0]
}

// RLEs are simply a slice of RLE.  Sorting only takes into account
// the start point and not the length.
type RLEs []RLE

func ReadRLEs(r io.Reader) (RLEs, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if header[0] != EncodingBinary {
		return nil, fmt.Errorf("sparse vol is not binary: %v", header[0])
	}
	var numSpans uint32
	if err := binary.Read(r, binary.LittleEndian, &numSpans); err != nil {
		return nil, err
	}
	var rles RLEs
	if err := rles.UnmarshalBinaryReader(r, numSpans); err != nil {
		return nil, err
	}
	return rles, nil
}

// Within returns a slice of indices to the given pts that are within
// the RLEs.
func (rles RLEs) Within(pts []Point3d) (in []int) {
	inset := make(map[int]struct{})
	for _, rle := range rles {
		for i, pt := range pts {
			if rle.Within(pt) {
				inset[i] = struct{}{}
			}
		}
	}
	in = make([]int, len(inset))
	i := 0
	for k := range inset {
		in[i] = k
		i++
	}
	return
}

// Offset returns RLEs that have been offset by negative of given point.
func (rles RLEs) Offset(offset Point3d) (out RLEs) {
	out = make(RLEs, len(rles))
	for i, rle := range rles {
		x := rle.start[0] - offset[0]
		y := rle.start[1] - offset[1]
		z := rle.start[2] - offset[2]
		out[i] = RLE{start: Point3d{x, y, z}, length: rle.length}
	}
	return
}

// --- sort interface

func (rles RLEs) Len() int {
	return len(rles)
}

func (rles RLEs) Swap(i, j int) {
	rles[i], rles[j] = rles[j], rles[i]
}

func (rles RLEs) Less(i, j int) bool {
	return rles[i].Less(rles[j])
}

// Normalize returns a sorted slice of RLEs where there are no directly adjacent RLEs along X.
func (rles RLEs) Normalize() RLEs {
	if rles == nil || len(rles) == 0 {
		return RLEs{}
	}

	// Sort the RLE
	norm := make(RLEs, len(rles)) // We know # normalized RLEs <= current # RLEs
	copy(norm, rles)
	sort.Sort(norm)
	nOrig := len(norm)

	// Iterate through each (y,z) and combine adjacent spans.
	var old *RLE
	n := 0 // current position of normalized slice end
	for o := 0; o < nOrig; o++ {
		pt := norm[o].start
		if old == nil {
			old = &RLE{pt, norm[o].length}
		} else {
			// Handle new normalized RLE by saving old one.
			if pt[1] != old.start[1] || pt[2] != old.start[2] || pt[0] != old.start[0]+old.length {
				norm[n] = *old
				n++
				old.start = pt
				old.length = norm[o].length
			} else {
				old.length += norm[o].length
			}
		}
	}
	norm[n] = *old
	return norm[:n+1]
}

// Split removes RLEs, which must be a subset of current RLEs, from the receiver and returns the remainder.
func (rles RLEs) Split(splits RLEs) (RLEs, error) {
	if splits == nil || len(splits) == 0 {
		return rles, nil
	}

	// Normalize the two RLEs so they are sorted and all contiguous RLE are joined.
	orles := rles.Normalize() // original RLEs
	srles := splits.Normalize()

	// Make a list of current RLEs
	out := list.New()
	for _, rle := range orles {
		out.PushBack(rle)
	}
	orles = nil

	// Perform all the splits.
	e := out.Front()
	for _, split := range srles {
		// Fast forward until we have the intersecting RLE
		for {
			if e == nil {
				return nil, fmt.Errorf("Split RLE %s is not contained in original RLEs", split)
			}
			orle := e.Value.(RLE)

			frags := orle.Excise(split)
			if frags == nil { // no intersection, move forward
				e = e.Next()
				continue
			}
			switch len(frags) {
			case 0:
				// Split fully covers.  Cannot be larger than underlying RLE due to subset requirement
				// and normalization.
				next := e.Next()
				out.Remove(e)
				e = next

			case 1:
				// Replace the current RLE
				next := out.InsertAfter(frags[0], e)
				out.Remove(e)
				e = next

			case 2:
				// There's a left and right portion.  All future splits can only intersect right one
				// since splits are also sorted in X.
				out.InsertBefore(frags[0], e)
				next := out.InsertAfter(frags[1], e)
				out.Remove(e)
				e = next

			default:
				return nil, fmt.Errorf("bad RLE excision - %d fragments for split %s by %s\n", len(frags), orle, split)
			}
			break
		}
	}

	numRLEs := out.Len()
	if numRLEs == 0 {
		return RLEs{}, nil
	}
	remain := make(RLEs, numRLEs)
	i := 0
	for e := out.Front(); e != nil; e = e.Next() {
		remain[i] = e.Value.(RLE)
		i++
	}
	return remain, nil
}

// Partition splits RLEs up into block-sized RLEs using the given block size.
// The return is a map of RLEs with stringified ZYX block coordinate keys.
func (rles RLEs) Partition(blockSize Point3d) (BlockRLEs, error) {
	brles := make(BlockRLEs, 100)
	for _, rle := range rles {
		// Get the block coord for starting point.
		bcoord := rle.start.Chunk(blockSize).(ChunkPoint3d)

		// Iterate block coord in X until block end is past span, storing fragments in block map.
		bBegX := bcoord[0] * blockSize[0]
		rx := rle.start[0]
		remain := rle.length
		for remain >= 1 {
			// Store block-clipped rle
			dx := bBegX + blockSize[0] - rx
			if remain < dx {
				brles.appendBlockRLE(bcoord, rx, rle.start[1], rle.start[2], remain)
			} else {
				brles.appendBlockRLE(bcoord, rx, rle.start[1], rle.start[2], dx)
			}

			// Go to next block
			bcoord[0]++
			bBegX += blockSize[0]
			rx += dx
			remain -= dx
		}
	}
	return brles, nil
}

// FitToBounds returns a copy that has been adjusted to fit
// within the given optional bounds.
func (rles RLEs) FitToBounds(bounds *OptionalBounds) RLEs {
	newRLEs := make(RLEs, 0, len(rles))
	if bounds == nil {
		copy(newRLEs, rles)
		return newRLEs
	}
	for _, rle := range rles {
		if bounds.minz != nil && rle.start[2] < *(bounds.minz) {
			continue
		}
		if bounds.maxz != nil && rle.start[2] > *(bounds.maxz) {
			continue
		}
		if bounds.miny != nil && rle.start[1] < *(bounds.miny) {
			continue
		}
		if bounds.maxy != nil && rle.start[1] > *(bounds.maxy) {
			continue
		}
		if bounds.minx != nil {
			if rle.start[0]+rle.length-1 < *(bounds.minx) {
				continue
			}
			if rle.start[0] < *(bounds.minx) {
				rle.length -= *(bounds.minx) - rle.start[0]
				rle.start[0] = *(bounds.minx)
			}
		}
		if bounds.maxx != nil {
			if rle.start[0] > *(bounds.maxx) {
				continue
			}
			if rle.start[0]+rle.length-1 > *(bounds.maxx) {
				rle.length = *(bounds.maxx) - rle.start[0] + 1
			}
		}
		newRLEs = append(newRLEs, rle)
	}
	return newRLEs
}

//

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (rles RLEs) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if rles != nil {
		for _, rle := range rles {
			if err := binary.Write(buf, binary.LittleEndian, rle.start[0]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.start[1]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.start[2]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.length); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (rles *RLEs) UnmarshalBinary(b []byte) error {
	lenEncoding := len(b)
	if lenEncoding%16 != 0 {
		return fmt.Errorf("RLE encoding # bytes is not divisible by 16: %d", len(b))
	}
	buf := bytes.NewBuffer(b)
	numRLEs := lenEncoding / 16
	*rles = make(RLEs, numRLEs, numRLEs)
	for i := 0; i < numRLEs; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[0])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[1])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[2])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].length)); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalBinaryReader reads from a reader instead of a static slice of bytes.
// This will likely be more efficient for very large RLEs that are being streamed
// into the server.
func (rles *RLEs) UnmarshalBinaryReader(r io.Reader, numRLEs uint32) error {
	*rles = make(RLEs, numRLEs, numRLEs)
	for i := uint32(0); i < numRLEs; i++ {
		if err := binary.Read(r, binary.LittleEndian, &((*rles)[i].start[0])); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &((*rles)[i].start[1])); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &((*rles)[i].start[2])); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &((*rles)[i].length)); err != nil {
			return err
		}
	}
	return nil
}

// Add adds the given RLEs to the receiver when there's a possibility of overlapping RLEs.
// If you are guaranteed the RLEs are disjoint, e.g., the passed and receiver RLEs are in
// different subvolumes, then just concatenate the RLEs instead of calling this function.
// The returned "voxelsAdded" gives the # of non-overlapping voxels added.
// TODO: If this is a bottleneck, employ better than this brute force insertion method.
func (rles *RLEs) Add(rles2 RLEs) (voxelsAdded int64) {
	for _, rle2 := range rles2 {
		var found bool
		for i, rle := range *rles {
			// If this rle has same z and y, modify the RLE, else just add rle.
			if rle.start[1] == rle2.start[1] && rle.start[2] == rle2.start[2] {
				x0 := rle.start[0]
				x1 := x0 + rle.length - 1
				cur_x0 := rle2.start[0]
				cur_x1 := cur_x0 + rle2.length - 1
				if x1 < cur_x0 {
					continue
				}
				if x0 > cur_x1 {
					continue
				}
				if x0 > cur_x0 {
					voxelsAdded += int64(x0 - cur_x0)
					x0 = cur_x0
				}
				if x1 < cur_x1 {
					voxelsAdded += int64(cur_x1 - x1)
					x1 = cur_x1
				}
				rle.start[0] = x0
				rle.length = x1 - x0 + 1
				(*rles)[i] = rle
				found = true
				break
			}
		}
		if !found {
			*rles = append(*rles, rle2)
			voxelsAdded += int64(rle2.length)
		}
	}
	return
}

// Stats returns the total number of voxels and runs.
func (rles RLEs) Stats() (numVoxels uint64, numRuns int32) {
	if rles == nil || len(rles) == 0 {
		return 0, 0
	}
	for _, rle := range rles {
		numVoxels += uint64(rle.length)
	}
	return numVoxels, int32(len(rles))
}

// BlockRLEs is a single label's map of block coordinates to RLEs for that label.
// The key is a string of the serialized block coordinate.
type BlockRLEs map[IZYXString]RLEs

func (brles BlockRLEs) appendBlockRLE(bcoord ChunkPoint3d, x, y, z, n int32) error {
	rle := RLE{Point3d{x, y, z}, n}
	idx := IndexZYX(bcoord)
	s := idx.ToIZYXString()

	rles, found := brles[s]
	if !found || rles == nil {
		brles[s] = RLEs{rle}
	} else {
		brles[s] = append(brles[s], rle)
	}
	return nil
}

// NumVoxels is the number of voxels contained within a label's block RLEs.
func (brles BlockRLEs) NumVoxels() uint64 {
	var size uint64
	for _, rles := range brles {
		numVoxels, _ := rles.Stats()
		size += uint64(numVoxels)
	}
	return size
}

// IZYXSlice is a typically sorted slice of IZYXString
type IZYXSlice []IZYXString

func (i IZYXSlice) Len() int           { return len(i) }
func (i IZYXSlice) Swap(a, b int)      { i[a], i[b] = i[b], i[a] }
func (i IZYXSlice) Less(a, b int) bool { return i[a] < i[b] }

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (i IZYXSlice) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(i)*12)
	off := 0
	for _, izyxStr := range i {
		copy(buf[off:off+12], string(izyxStr))
		off += 12
	}
	return buf, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (i *IZYXSlice) UnmarshalBinary(b []byte) error {
	// TODO -- the serialization is the raw data minus the slice wrapper
	// yet the deserialization requires a lot of copy.  Is there a way to
	// simply supply the slice wrapper (ptr, len, cap) where ptr goes to
	// the serialization?
	sz := len(b)
	if sz == 0 {
		*i = IZYXSlice{}
		return nil
	}
	if sz%12 != 0 {
		return fmt.Errorf("cannot unmarshal IZYXSlice of %d bytes", sz)
	}
	n := sz / 12
	s := make(IZYXSlice, n)
	off := 0
	for j := 0; j < n; j++ {
		s[j] = IZYXString(b[off : off+12])
		off += 12
	}
	*i = s
	return nil
}

// Delete removes the given sorted IZYXSlice from the receiver.
func (i *IZYXSlice) Delete(i2 IZYXSlice) {
	len1 := len(*i)
	len2 := len(i2)
	if len1 == 0 || len2 == 0 || (*i)[0] > i2[len2-1] || (*i)[len1-1] < i2[0] {
		return
	}
	pos1 := 0
	pos2 := 0
	for {
		if pos1 >= len1 || pos2 >= len2 {
			break
		}
		switch {
		case (*i)[pos1] == i2[pos2]:
			*i = append((*i)[:pos1], (*i)[pos1+1:]...)
			pos2++
			len1--
		case (*i)[pos1] > i2[pos2]:
			pos2++
		case (*i)[pos1] < i2[pos2]:
			pos1++
		}
	}
	*i = (*i)[:len1]
}

// Merge merges the given sorted IZYXSlice into the receiver.
func (i *IZYXSlice) Merge(i2 IZYXSlice) {
	len1 := len(*i)
	len2 := len(i2)
	if len2 == 0 {
		return
	}
	if len1 == 0 {
		*i = make(IZYXSlice, len2)
		copy(*i, i2)
		return
	}
	if i2[0] > (*i)[len1-1] {
		*i = append(*i, i2...)
		return
	}
	if (*i)[0] > i2[len2-1] {
		buf := make(IZYXSlice, len1+len2)
		copy(buf[:len2], i2)
		copy(buf[len2:], *i)
		*i = buf
		return
	}
	*i = (*i).MergeCopy(i2)
}

// MergeCopy returns a merge of two sorted IZYXSlice.  Both IZYXSlices should be sorted
// before use of this function and the result is a copy not a reference.
func (i IZYXSlice) MergeCopy(i2 IZYXSlice) IZYXSlice {
	len1 := len(i)
	len2 := len(i2)
	if len1 == 0 {
		dup := make(IZYXSlice, len2)
		copy(dup, i2)
		return dup
	}
	if len2 == 0 {
		dup := make(IZYXSlice, len1)
		copy(dup, i)
		return dup
	}
	out := make(IZYXSlice, len1+len2)

	n := 0
	p1 := 0
	p2 := 0
	for {
		if i[p1] < i2[p2] {
			out[n] = i[p1]
			n++
			p1++
			if p1 == len1 {
				copy(out[n:], i2[p2:])
				n += len2 - p2
				break
			}
		} else if i2[p2] < i[p1] {
			out[n] = i2[p2]
			n++
			p2++
			if p2 == len2 {
				copy(out[n:], i[p1:])
				n += len1 - p1
				break
			}
		} else if i[p1] == i2[p2] {
			out[n] = i[p1]
			n++
			p1++
			p2++
			if p1 == len1 && p2 == len2 {
				break
			}
			stop := false
			if p1 == len1 {
				copy(out[n:], i2[p2:])
				n += len2 - p2
				stop = true
			}
			if p2 == len2 {
				copy(out[n:], i[p1:])
				n += len1 - p1
				stop = true
			}
			if stop {
				break
			}
		}
	}
	return out[:n]
}

// Split removes the IZYXs from the receiver and returns the remainder as a copy.
// This assumes both deleted blocks and receiver are sorted.
func (i IZYXSlice) Split(rm IZYXSlice) (IZYXSlice, error) {
	if len(i) == 0 {
		return IZYXSlice{}, nil
	}
	if len(rm) == 0 {
		dup := make(IZYXSlice, len(i))
		copy(dup, i)
		return dup, nil
	}
	out := make(IZYXSlice, len(i))
	var ipos, outpos, rmpos int
	rzyx := rm[rmpos]
	for {
		if ipos >= len(i) {
			break
		}
		izyx := i[ipos]
		for rzyx < izyx {
			rmpos++
			if rmpos < len(rm) {
				rzyx = rm[rmpos]
			} else {
				break
			}
		}
		if izyx != rzyx {
			out[outpos] = izyx
			outpos++
		}
		ipos++
	}
	return out[:outpos], nil
}

// WriteSerializedRLEs writes serialized RLEs and returns the number of spans.
// Note this function assumes the receiver is already sorted.
func (i IZYXSlice) WriteSerializedRLEs(w io.Writer) (spans uint32, err error) {
	var length int32
	var startPt, chunkPt ChunkPoint3d
	for _, izyx := range i {
		chunkPt, err = izyx.ToChunkPoint3d()
		if err != nil {
			return
		}
		if startPt[2] != chunkPt[2] || startPt[1] != chunkPt[1] || startPt[0]+length != chunkPt[0] {
			if length > 0 {
				binary.Write(w, binary.LittleEndian, startPt[0])
				binary.Write(w, binary.LittleEndian, startPt[1])
				binary.Write(w, binary.LittleEndian, startPt[2])
				binary.Write(w, binary.LittleEndian, length)
				spans++
			}
			length = 1
			startPt = chunkPt
		} else {
			length++
		}
	}
	if length > 0 {
		binary.Write(w, binary.LittleEndian, startPt[0])
		binary.Write(w, binary.LittleEndian, startPt[1])
		binary.Write(w, binary.LittleEndian, startPt[2])
		binary.Write(w, binary.LittleEndian, length)
		spans++
	}
	return
}

// GetBounds returns the minimum and maximum coordinate in the IZYXSlice.
func (i IZYXSlice) GetBounds() (minPt, maxPt Point3d, err error) {
	if len(i) == 0 {
		return
	}
	minPt[0] = math.MaxInt32
	minPt[1] = math.MaxInt32
	minPt[2] = math.MaxInt32
	maxPt[0] = -math.MaxInt32 + 1
	maxPt[1] = -math.MaxInt32 + 1
	maxPt[2] = -math.MaxInt32 + 1
	for _, izyxStr := range i {
		var blockPt ChunkPoint3d
		blockPt, err = izyxStr.ToChunkPoint3d()
		if err != nil {
			err = fmt.Errorf("unable to convert IZYXString to chunk point: %v", err)
			return
		}
		if minPt[0] > blockPt[0] {
			minPt[0] = blockPt[0]
		}
		if minPt[1] > blockPt[1] {
			minPt[1] = blockPt[1]
		}
		if minPt[2] > blockPt[2] {
			minPt[2] = blockPt[2]
		}
		if maxPt[0] < blockPt[0] {
			maxPt[0] = blockPt[0]
		}
		if maxPt[1] < blockPt[1] {
			maxPt[1] = blockPt[1]
		}
		if maxPt[2] < blockPt[2] {
			maxPt[2] = blockPt[2]
		}
	}
	return
}

// FitToBounds returns a copy IZYXSlice that has been adjusted to fit
// within the given optional block bounds.  The receiver IZYXSlice is
// assumed to be sorted.
func (i IZYXSlice) FitToBounds(bounds *OptionalBounds) (IZYXSlice, error) {
	cropped := make(IZYXSlice, len(i))
	if bounds == nil {
		copy(cropped, i)
		return cropped, nil
	}
	num := 0
	for _, izyxStr := range i {
		blockPt, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return nil, fmt.Errorf("unable to convert IZYXString to chunk point: %v", err)
		}
		if bounds.minz != nil && blockPt[2] < *(bounds.minz) {
			continue
		}
		if bounds.maxz != nil && blockPt[2] > *(bounds.maxz) {
			break
		}
		if bounds.miny != nil && blockPt[1] < *(bounds.miny) {
			continue
		}
		if bounds.maxy != nil && blockPt[1] > *(bounds.maxy) {
			continue
		}
		if bounds.minx != nil && blockPt[0] < *(bounds.minx) {
			continue
		}
		if bounds.maxx != nil && blockPt[0] > *(bounds.maxx) {
			continue
		}
		cropped[num] = izyxStr
		num++
	}
	cropped = cropped[0:num]
	return cropped, nil
}

// Downres returns a down-resolution version of the IZYXSlice where the factor is 2^scale.
// The receiver IZYXSlice does not have to be sorted.
func (i IZYXSlice) Downres(scale uint8) (IZYXSlice, error) {
	if scale == 0 {
		downres := make(IZYXSlice, len(i))
		copy(downres, i)
		return downres, nil
	}

	downresMap := make(map[IZYXString]struct{}, len(i))
	for _, izyxStr := range i {
		blockPt, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return nil, fmt.Errorf("unable to convert IZYXString to chunk point: %v", err)
		}
		blockPt[0] >>= scale
		blockPt[1] >>= scale
		blockPt[2] >>= scale
		izyxStr = blockPt.ToIZYXString()
		downresMap[izyxStr] = struct{}{}
	}
	downres := make(IZYXSlice, len(downresMap))
	pos := 0
	for izyxStr := range downresMap {
		downres[pos] = izyxStr
		pos++
	}
	sort.Sort(downres)
	return downres, nil
}

// SortedKeys returns a slice of IZYXString sorted in ascending order.
func (brles BlockRLEs) SortedKeys() IZYXSlice {
	sk := make(IZYXSlice, len(brles))
	var i int
	for k := range brles {
		sk[i] = k
		i++
	}
	sort.Sort(sk)
	return sk
}

// SparseVol represents a collection of voxels that may be in an arbitrary shape and have a label.
// It is particularly good for storing sparse voxels that may traverse large amounts of space.
type SparseVol struct {
	initialized bool
	numVoxels   uint64
	minPt       Point3d
	maxPt       Point3d
	label       uint64
	rles        RLEs
	pos         int // Current index into rle.
}

func (vol *SparseVol) MinimumPoint3d() Point3d {
	return vol.minPt
}

func (vol *SparseVol) MaximumPoint3d() Point3d {
	return vol.maxPt
}

func (vol *SparseVol) Size() Point3d {
	return Point3d{vol.maxPt[0] - vol.minPt[0] + 1, vol.maxPt[1] - vol.minPt[1] + 1, vol.maxPt[2] - vol.minPt[2] + 1}
}

func (vol *SparseVol) RLEs() RLEs {
	return vol.rles
}

func (vol *SparseVol) NumVoxels() uint64 {
	return vol.numVoxels
}

func (vol *SparseVol) Label() uint64 {
	return vol.label
}

func (vol *SparseVol) SetLabel(label uint64) {
	vol.label = label
}

func (vol *SparseVol) Clear() {
	vol.initialized = false
	vol.pos = 0
	vol.numVoxels = 0
}

// AddSerializedRLEs adds binary encoding of RLEs to SparseVol.
func (vol *SparseVol) AddSerializedRLEs(encoding []byte) error {
	lenEncoding := len(encoding)
	if lenEncoding%16 != 0 {
		return fmt.Errorf("RLE encoding # bytes is not divisible by 16: %d", len(encoding))
	}
	numRLEs := lenEncoding / 16
	if vol.pos+numRLEs >= cap(vol.rles) {
		newsize := vol.pos + numRLEs
		tmp := make(RLEs, newsize, newsize)
		copy(tmp[0:len(vol.rles)], vol.rles)
		vol.rles = tmp
	}

	buf := bytes.NewBuffer(encoding)
	var x, y, z, length int32
	for i := 0; i < numRLEs; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &x); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &y); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &z); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return err
		}
		pt := Point3d{x, y, z}
		vol.rles[vol.pos] = RLE{pt, length}
		vol.numVoxels += uint64(length)
		vol.pos++
		if vol.initialized {
			vol.minPt.SetMinimum(pt)
			vol.maxPt.SetMaximum(Point3d{x + length - 1, y, z})
		} else {
			vol.minPt = pt
			vol.maxPt = Point3d{x + length - 1, y, z}
			vol.initialized = true
		}
	}
	return nil
}

// AddRLE adds an RLE to a SparseVol.
func (vol *SparseVol) AddRLE(rles RLEs) {
	if vol.pos+len(rles) >= cap(vol.rles) {
		newsize := vol.pos + len(rles)
		tmp := make(RLEs, newsize, newsize)
		copy(tmp[0:len(vol.rles)], vol.rles)
		vol.rles = tmp
	}
	for _, rle := range rles {
		vol.rles[vol.pos] = rle
		vol.numVoxels += uint64(rle.length)
		vol.pos++
		endPt := rle.start
		endPt[0] += rle.length - 1
		if vol.initialized {
			vol.minPt.SetMinimum(rle.start)
			vol.maxPt.SetMaximum(endPt)
		} else {
			vol.minPt = rle.start
			vol.maxPt = endPt
			vol.initialized = true
		}
	}
}

// SurfaceSerialization returns binary-encoded surface data with the following format:
//    First 4 bytes (little-endian) # voxels (N)
//    Array of N vertices, each with 3 little-endian float32 (x,y,z)
//    Array of N normals, each with 3 little-endian float32 (nx,ny,nz)
//
// The blockNz parameter is necessary since underlying RLEs in the SparseVol are ordered
// by blocks in Z but not within a block, so RLEs can have different Z within a block.
func (vol *SparseVol) SurfaceSerialization(blockNz int32, res NdFloat32) ([]byte, error) {
	// TODO -- can be more efficient in buffer space by only needing 8 blocks worth
	// of data (4 for current XY processing and 4 for next Z), but for simplicity this
	// function uses total XY extents + some # of slices (blockNz).
	dx := vol.maxPt[0] - vol.minPt[0] + 3
	dy := vol.maxPt[1] - vol.minPt[1] + 3
	dz := vol.maxPt[2] - vol.minPt[2] + 3

	// Note that SparseVol RLEs can jump in Z within a block of data, so buffer must be at least
	// as big as a block in Z.
	if dz > blockNz*2+1 {
		dz = blockNz*2 + 1
	}

	// Allocate buffer for processing
	offset := vol.minPt.AddScalar(-1).(Point3d)
	binvol := NewBinaryVolume(offset, Point3d{dx, dy, dz}, res)

	var vertexBuf, normalBuf bytes.Buffer
	var surfaceSize uint32
	rleI := 0
	Debugf("Label %d, # voxels %d, buffer size %s, minPt %s, maxPt %s",
		vol.label, vol.numVoxels, binvol.size, vol.minPt, vol.maxPt)

	for {
		var minX int32 = 1      //dx
		var maxX int32 = dx - 1 //0
		var minY int32 = 1      //dy
		var maxY int32 = dy - 1 //0
		// Populate the buffer.
		for {
			if rleI >= vol.pos {
				// We've added entire volume.
				break
			}
			r := vol.rles[rleI]
			bz := int64(r.start[2] - binvol.offset[2])
			if bz >= int64(dz) {
				// rles have filled this buffer.
				break
			}
			by := int64(r.start[1] - binvol.offset[1])
			bx := int64(r.start[0] - binvol.offset[0])
			p := bz*int64(dx*dy) + by*int64(dx) + bx
			for i := int64(0); i < int64(r.length); i++ {
				binvol.data[p+i] = 255
			}

			// For this buffer, set bounds.  For large sparse volumes that snake
			// through a lot of space, the XY footprint might be relatively small.
			// This is currently not working but is a TODO.
			/*
				if minX > bx {
					minX = bx
				}
				if maxX < bx+r.length-1 {
					maxX = bx + r.length - 1
				}
				if minY > by {
					minY = by
				}
				if maxY < by {
					maxY = by
				}
			*/
			rleI++
		}

		// Iterate through XY layers to compute surface and normal
		for z := int32(1); z <= blockNz; z++ {
			if binvol.offset[2]+z > vol.maxPt[2] {
				// We've passed through all of this sparse volume's voxels
				break
			}
			// TODO -- Keep track of bounding box per Z and limit checks to it.
			for y := minY; y <= maxY; y++ {
				for x := minX; x <= maxX; x++ {
					nx, ny, nz, isSurface := binvol.CheckSurface(x, y, z)
					if isSurface {
						surfaceSize++
						fx := float32(x + binvol.offset[0])
						fy := float32(y + binvol.offset[1])
						fz := float32(z + binvol.offset[2])
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fx); err != nil {
							return nil, err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fy); err != nil {
							return nil, err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fz); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nx); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, ny); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nz); err != nil {
							return nil, err
						}
					}
				}
			}
		}

		// Shift buffer
		if binvol.offset[2]+blockNz < vol.maxPt[2] {
			binvol.ShiftUp(blockNz)
		} else {
			break
		}
	}

	// Store computation
	// TODO -- Make this more efficient in terms of memory
	numBytes := 4 + vertexBuf.Len() + normalBuf.Len()
	data := make([]byte, numBytes, numBytes)
	i := 0
	j := 4
	binary.LittleEndian.PutUint32(data[i:j], surfaceSize)
	i = j
	j += vertexBuf.Len()
	copy(data[i:j], vertexBuf.Bytes())
	i = j
	j += normalBuf.Len()
	copy(data[i:j], normalBuf.Bytes())
	return data, nil
}

// BinaryVolume holds 3d binary data including a 3d offset.
type BinaryVolume struct {
	offset      Point3d
	size        Point3d
	xanisotropy float64
	yanisotropy float64
	zanisotropy float64
	data        []byte
}

// NewBinaryVolume returns a BinaryVolume with an allocated 3d volume for data.
func NewBinaryVolume(offset, size Point3d, res NdFloat32) *BinaryVolume {
	minRes := res.GetMin()
	numBytes := size[0] * size[1] * size[2]
	return &BinaryVolume{
		offset:      offset,
		size:        size,
		xanisotropy: float64(res[0] / minRes),
		yanisotropy: float64(res[1] / minRes),
		zanisotropy: float64(res[2] / minRes),
		data:        make([]byte, numBytes, numBytes),
	}
}

// Shift the buffer up by dz voxels.
func (binvol *BinaryVolume) ShiftUp(dz int32) {
	binvol.offset[2] += dz
	sliceBytes := binvol.size[0] * binvol.size[1]
	var i0, j0, z int32
	j0 = dz * sliceBytes
	maxStartI := int32(len(binvol.data)) - sliceBytes
	for z = 0; z < binvol.size[2]; z++ {
		if j0 <= maxStartI {
			copy(binvol.data[i0:i0+sliceBytes], binvol.data[j0:j0+sliceBytes])
			j0 += sliceBytes
		} else if i0 <= maxStartI {
			for i := i0; i < i0+sliceBytes; i++ {
				binvol.data[i] = 0
			}
		}
		i0 += sliceBytes
	}
}

// CheckSurface checks to see if the given voxel within the BinaryVolume is set, and if so,
// calculates a normal based on a Zucker-Hummel 3x3x3 convolution.
func (binvol *BinaryVolume) CheckSurface(x, y, z int32) (normx, normy, normz float32, isSurface bool) {
	nx := binvol.size[0]
	nxy := binvol.size[1] * nx
	if binvol.data[z*nxy+y*nx+x] == 0 {
		return
	}
	// If any neighbor is 0, this is a surface voxel.
	var ix, iy, iz, pz, py, p int32
	for iz = z - 1; iz <= z+1; iz++ {
		pz = iz * nxy
		for iy = y - 1; iy <= y+1; iy++ {
			p = pz + iy*nx + x - 1
			for ix = 0; ix < 3; ix++ {
				if binvol.data[p] == 0 {
					isSurface = true
					goto ComputeNormal
				}
				p++
			}
		}
	}

ComputeNormal:
	//fmt.Printf("CheckSurface (%d, %d, %d) with binvol size %d x %d x %d\n", x, y, z, binvol.size[0], binvol.size[1], binvol.size[2])
	var xgrad, ygrad, zgrad float64
	pz = (z - 1) * nxy
	for iz = 0; iz < 3; iz++ {
		py = (y - 1) * nx
		for iy = 0; iy < 3; iy++ {
			p = pz + py + x - 1
			for ix = 0; ix < 3; ix++ {
				value := float64(binvol.data[p])
				xgrad += value * zhX[ix][iy][iz]
				ygrad += value * zhY[ix][iy][iz]
				zgrad += value * zhZ[ix][iy][iz]
				p++
			}
			py += nx
		}
		pz += nxy
	}

	// Cheap hack to try to compensate for anisotropy.
	// TODO -- Implement distance transform followed by gradient to better smooth
	// and handle anisotropy.
	xgrad /= binvol.xanisotropy
	ygrad /= binvol.yanisotropy
	zgrad /= binvol.zanisotropy

	mag := math.Sqrt(xgrad*xgrad + ygrad*ygrad + zgrad*zgrad)
	normx = float32(xgrad / mag)
	normy = float32(ygrad / mag)
	normz = float32(zgrad / mag)
	return
}
