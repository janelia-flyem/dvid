package labels

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/janelia-flyem/dvid/dvid"
)

var maxSliceSize int

const maxSliceSize32 = 1 << 27 // max slice for a 8-byte value (uint64) on 32-bit server
const maxSliceSize64 = 1 << 34 // max slice, given DVID request bounds, for a 8-byte value (uint64) on 64-bit server

const intSize = 32 << (^uint(0) >> 63) // size in bits of an int

func init() {
	// If this is not a little-endian machine, exit because this package is only optimized
	// for these types of machines.
	var check uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&check)) != 0x04 {
		fmt.Printf("This machine is not little-endian.  Currently, DVID label compression does not support this machine.\n")
		os.Exit(1)
	}

	switch intSize {
	case 32:
		maxSliceSize = maxSliceSize32
	case 64:
		maxSliceSize = maxSliceSize64
	default:
		fmt.Printf("Unknown architecture with int size of %d bits.  DVID works with 32 or 64 bit architectures.\n")
		os.Exit(1)
	}
}

// NOTE: The following slice aliasing functions should be used with caution, particularly
//       when reusing preallocated slices.  The intended use is for reuse of preallocated
//       slices.

func byteToUint64(b []byte) (out []uint64, err error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("bad length in byteToUint64: len %d", len(b))
	}
	if uintptr(unsafe.Pointer(&b[0]))%8 != 0 {
		return nil, fmt.Errorf("bad alignment in byteToUint64: uintptr = %d", uintptr(unsafe.Pointer(&b[0])))
	}
	if intSize == 32 {
		return (*[maxSliceSize32]uint64)(unsafe.Pointer(&b[0]))[:len(b)/8 : cap(b)/8], nil
	}
	return (*[maxSliceSize64]uint64)(unsafe.Pointer(&b[0]))[:len(b)/8 : cap(b)/8], nil
}

func byteToUint32(b []byte) (out []uint32, err error) {
	if len(b)%4 != 0 || uintptr(unsafe.Pointer(&b[0]))%4 != 0 {
		return nil, fmt.Errorf("bad len, cap, or alignment of byteToUint32 len %d", len(b))
	}
	if intSize == 32 {
		return (*[maxSliceSize32 << 1]uint32)(unsafe.Pointer(&b[0]))[:len(b)/4 : cap(b)/4], nil
	}
	return (*[maxSliceSize64 << 1]uint32)(unsafe.Pointer(&b[0]))[:len(b)/4 : cap(b)/4], nil
}

func byteToUint16(b []byte) (out []uint16, err error) {
	if len(b)%2 != 0 || uintptr(unsafe.Pointer(&b[0]))%4 != 0 {
		return nil, fmt.Errorf("bad len, cap, or alignment of byteToUint16 len %d", len(b))
	}
	if intSize == 32 {
		return (*[maxSliceSize32 << 2]uint16)(unsafe.Pointer(&b[0]))[:len(b)/2 : cap(b)/2], nil
	}
	return (*[maxSliceSize64 << 2]uint16)(unsafe.Pointer(&b[0]))[:len(b)/2 : cap(b)/2], nil
}

func uint16ToByte(in []uint16) []byte {
	if intSize == 32 {
		return (*[maxSliceSize32 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*2]
	}
	return (*[maxSliceSize64 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*2]
}

func uint32ToByte(in []uint32) []byte {
	if intSize == 32 {
		return (*[maxSliceSize32 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*4]
	}
	return (*[maxSliceSize64 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*4]
}

func uint64ToByte(in []uint64) []byte {
	if intSize == 32 {
		return (*[maxSliceSize32 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*8]
	}
	return (*[maxSliceSize64 << 3]byte)(unsafe.Pointer(&in[0]))[:len(in)*8]
}

// returns the # of bits necessary to hold an index for n values.
// 0 and 1 should return 0.
func bitsFor(n uint16) (bits uint32) {
	if n < 2 {
		return 0
	}
	n--
	for {
		if n > 0 {
			bits++
		} else {
			return
		}
		n >>= 1
	}
}

const SubBlockSize = 8
const DefaultSubBlocksPerBlock = 8
const DefaultBlockSize = DefaultSubBlocksPerBlock * SubBlockSize

// left mask for bithead at each bit position in a byte
var leftBitMask [8]byte = [8]byte{
	0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01,
}

// SubBlock is the basic unit of compressed DVID labels, set to 8x8x8 to simplify Go
// coding and lock in the number of bits required for maximum # of labels, etc.
// The slices share memory with the containing Block data buffer and are a referencing
// convenience.
type SubBlock struct {
	Index  []uint32
	Values []byte
}

// Block is the unit of storage for compressed DVID labels.  It is inspired by the
// Neuroglancer compression scheme and makes the following changes: (1) a global
// label list with block-level indices into the list (23 bits vs 64 bits in
// original Neuroglancer scheme) and a 9 bit count of the # of voxels in the block
// with that label, (2) the number of bits for encoding values is not required to be
// a power of two.  A global LUT allows easy sharing of labels between blocks, and
// block-level storage can be more efficient due to the smaller LUT (at the cost of
// an indirection) and better encoded value packing (at the cost of byte alignment).
// In both cases memory is gained for increased computation.
type Block struct {
	Labels    []uint64 // shares memory with data
	Numbers   []uint16 // # of labels per sub-block
	SubBlocks []SubBlock

	data []byte // actual data in serialized format, referenced by above slices, and aligned to 64-bit words.

	size dvid.Point3d // # voxels in each dimension for this block
}

// MakeSolidBlock returns a Block that represents a single label of the given block size.
func MakeSolidBlock(label uint64, blockSize dvid.Point3d) *Block {
	gx := uint32(blockSize[0] / SubBlockSize)
	gy := uint32(blockSize[1] / SubBlockSize)
	gz := uint32(blockSize[2] / SubBlockSize)
	numLabels := uint32(1)

	b := new(Block)
	b.size = blockSize

	data := make([]byte, 24)

	binary.LittleEndian.PutUint32(data[0:4], gx)
	binary.LittleEndian.PutUint32(data[4:8], gy)
	binary.LittleEndian.PutUint32(data[8:12], gz)
	binary.LittleEndian.PutUint32(data[12:16], numLabels)
	binary.LittleEndian.PutUint64(data[16:24], label)

	b.Labels = []uint64{label}
	b.SubBlocks = solidSubBlocks(gx * gy * gz)
	b.data = data

	// setup the Go side to mirror the data.
	return b
}

// MarshalBinary implements the encoding.BinaryMarshaler interface. Note that for
// efficiency, the returned byte slice will share memory with the receiver Block.
// Blocks cover nx * ny * nz voxels.  This implementation allows any choice of nx, ny, and nz
// with two restrictions: (1) nx, ny, and nz must be a multiple of 8 greater than 16, and
// (2) the total number of labels cannot exceed the capacity of a uint32.
//
// Internally, labels are stored in 8x8x8 sub-blocks.  There are gx * gy * gz sub-blocks where
// gx = nx / 8; gy = ny / 8; gz = nz / 8.
//
// The byte layout will be the following if there are N labels in the Block:
//
//      3 * uint32            values of gx, gy, and gz
//      uint32                # of labels (N)
//      N * uint64            packed labels in little-endian format
//
//      ----- Data below is only included if N > 1, otherwise it is a solid block.
//
//      gx*gy*gz * uint16     # of labels for sub-blocks.  Ns[i] = # labels for a sub-block
//
//      Labels within gz * gy * gx sub-blocks with each sub-block data in the following format:
//
//			Ns[i] * uint32    Ns packed indices into the N labels, padded to a 64-bit words.
//          values            512 * ceil(log2(Ns[i])) bits, padded to 32-bit words.
//                            At most we use 9 bits per voxel for up to the 512 labels in sub-block.
//                            A value gives the sub-block index which points to the index into
//                            the N labels.  If Ns[i] <= 1, there are no values.  If Ns[i] = 0,
//                            the 8x8x8 voxels are set to label 0.  If Ns[i] = 1, all voxels
//                            are the given label index.
func (b Block) MarshalBinary() ([]byte, error) {
	return b.data, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.  Note that
// for efficiency, the receive Block will share memory with the given byte slice.
func (b *Block) UnmarshalBinary(data []byte) (err error) {
	if len(data)%4 != 0 {
		return fmt.Errorf("can't unmarshal binary into labels.Block with non-aligned data of length %d", len(data))
	}
	// Get the sub-blocks along each dimension
	gx := binary.LittleEndian.Uint32(data[0:4])
	gy := binary.LittleEndian.Uint32(data[4:8])
	gz := binary.LittleEndian.Uint32(data[8:12])

	b.size[0] = int32(gx * SubBlockSize)
	b.size[1] = int32(gy * SubBlockSize)
	b.size[2] = int32(gz * SubBlockSize)
	b.data = data

	// Get the label slice
	numLabels := binary.LittleEndian.Uint32(data[12:16])
	if numLabels == 0 {
		return fmt.Errorf("received labels.Block serialization with 0 labels")
	}
	nbytes := numLabels * 8
	b.Labels, err = byteToUint64(data[16 : 16+nbytes])
	if err != nil {
		return
	}
	numSubBlocks := gx * gy * gz
	if numLabels == 1 {
		b.SubBlocks = solidSubBlocks(numSubBlocks)
		return
	}

	// Get the # of labels in the Nb sub-blocks
	numbersPos := 16 + nbytes
	subBlockPos := numbersPos + numSubBlocks*2
	b.Numbers, err = byteToUint16(data[numbersPos:subBlockPos])
	if err != nil {
		return
	}

	b.setSubBlocks(numSubBlocks, data[subBlockPos:])
	return
}

func solidSubBlocks(numSubBlocks uint32) []SubBlock {
	s := make([]SubBlock, numSubBlocks)
	for sb := uint32(0); sb < numSubBlocks; sb++ {
		s[sb].Index = []uint32{0}
	}
	return s
}

// link the SubBlock structs to the underlying serialization.
// Note: the passed data slice is ONLY the sub-blocks data, not preceding header,
// and the Numbers slice must have been previously initialized.
func (b *Block) setSubBlocks(numSubBlocks uint32, data []byte) error {
	var pos uint32
	b.SubBlocks = make([]SubBlock, numSubBlocks)
	for sb := uint32(0); sb < numSubBlocks; sb++ {
		numIndices := b.Numbers[sb]
		indexBytes := uint32(numIndices * 4) // size of sub-block index table
		indices, err := byteToUint32(data[pos : pos+indexBytes])
		if err != nil {
			return err
		}
		pos += indexBytes

		var values []byte
		if numIndices > 1 {
			valueBytes := (512 * bitsFor(numIndices)) >> 3
			values = data[pos : pos+valueBytes]
			pos += valueBytes
			pos += valueBytes % 4 // We want sub-blocks to be aligned on 64-bit words.
		}
		b.SubBlocks[sb] = SubBlock{
			Index:  indices,
			Values: values,
		}
	}
	return nil
}

// label array and portion of data that is being processed
type subvolumeData struct {
	data      []uint64
	volsize   [3]uint32 // full size of volume
	blockOff  [3]uint32 // offset from corner of subvolume to block being processed
	blockSize [3]uint32 // size of block extending from blockOff
}

// get # sub-blocks in each dimension
func (s subvolumeData) getSubBlockDims() (gx, gy, gz uint32) {
	return s.blockSize[0] / SubBlockSize, s.blockSize[1] / SubBlockSize, s.blockSize[2] / SubBlockSize
}

// run checks and do conversions
func setSubvolume(uint64array []byte, volsize, blockOff dvid.Point, blockSize dvid.Point3d) (*subvolumeData, error) {
	if volsize.Prod() >= int64(maxSliceSize) {
		return nil, fmt.Errorf("Volume %s is too large.  Please decrease array dimensions to have at most %d voxels", volsize, maxSliceSize)
	}
	if blockSize[0]%SubBlockSize != 0 || blockSize[1]%SubBlockSize != 0 || blockSize[2]%SubBlockSize != 0 {
		return nil, fmt.Errorf("uint64 array of size %s not supported, must be multiple of %d", blockSize, SubBlockSize)
	}
	if blockSize[0] < 16 || blockSize[1] < 16 || blockSize[2] < 16 {
		return nil, fmt.Errorf("Blocks must be at least 16x16x16, so this size is illegal: %s", blockSize)
	}
	boundsCheck := volsize.Sub(blockOff.Add(blockSize))
	if boundsCheck.Value(0) < 0 || boundsCheck.Value(1) < 0 || boundsCheck.Value(2) < 0 {
		return nil, fmt.Errorf("Bad block offset %s + block size %s > volume size %s", blockOff, blockSize, volsize)
	}
	s := new(subvolumeData)
	var err error
	s.data, err = byteToUint64(uint64array)
	if err != nil {
		return nil, err
	}
	s.volsize[0] = uint32(volsize.Value(0))
	s.volsize[1] = uint32(volsize.Value(1))
	s.volsize[2] = uint32(volsize.Value(2))

	s.blockOff[0] = uint32(blockOff.Value(0))
	s.blockOff[1] = uint32(blockOff.Value(1))
	s.blockOff[2] = uint32(blockOff.Value(2))

	s.blockSize[0] = uint32(blockSize[0])
	s.blockSize[1] = uint32(blockSize[1])
	s.blockSize[2] = uint32(blockSize[2])
	return s, nil
}

// iterate through the subvolume corresponding to the Block and do encoding
func (s *subvolumeData) encodeBlock() (labels map[uint64]uint32, labelNumbers []uint16, subBlockBytes []byte, err error) {
	gx, gy, gz := s.getSubBlockDims()
	numSubBlocks := gx * gy * gz

	var numLabels uint32
	labels = make(map[uint64]uint32)            // map of label -> index position in Block
	labelNumbers = make([]uint16, numSubBlocks) // # of labels in each sub-block

	slabelList := make([]uint32, 512) // sub-block labels that are index into block-level label list
	svalues := make([]byte, 512*2)    // max size of sub-block values

	dy := s.volsize[0]
	dz := s.volsize[0] * s.volsize[1]

	var subBlockNum int
	var subBlockBuf bytes.Buffer // will hold the serialization of the sub-blocks data
	for sz := uint32(0); sz < gz; sz++ {
		uz := sz*SubBlockSize + s.blockOff[2]
		// uzpos := (sz*SubBlockSize)*s.blockSize[0]*s.blockSize[1]
		for sy := uint32(0); sy < gy; sy++ {
			uy := sy*SubBlockSize + s.blockOff[1]
			// upos := uzpos + (sy*SubBlockSize)*s.blockSize[0]
			for sx := uint32(0); sx < gx; sx++ {
				ux := sx*SubBlockSize + s.blockOff[0]

				// iterate through sub-block and get labels, indices
				var numSBLabels uint16
				slabels := make(map[uint64]uint16) // map of label -> index position in sub-block

				upos := uz*dz + uy*dy + ux
				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
							label := s.data[upos]
							if _, found := slabels[label]; !found {
								slabels[label] = numSBLabels
								numSBLabels++
							}
							upos++
						}
						upos += dy - SubBlockSize
					}
					upos += dz - s.volsize[0]*SubBlockSize
				}

				// 2nd pass through sub-block, write indices now that we know required bits per voxel.
				bits := int(bitsFor(numSBLabels))

				var bitpos int
				if bits > 0 {
					var curbyte byte // byte in values slice under the write head
					upos = uz*dz + uy*dy + ux
					for z = 0; z < SubBlockSize; z++ {
						for y = 0; y < SubBlockSize; y++ {
							for x = 0; x < SubBlockSize; x++ {
								index := slabels[s.data[upos]]
								bithead := bitpos % 8
								bytepos := bitpos >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									leftshift := uint(8 - bits - bithead)
									curbyte |= byte(index << leftshift)
									svalues[bytepos] = curbyte
									if leftshift == 0 {
										curbyte = 0x00
									}
								} else {
									// this straddles a byte boundary
									leftshift := uint(16 - bits - bithead)
									index <<= leftshift
									curbyte |= byte((index & 0xFF00) >> 8)
									svalues[bytepos] = curbyte
									curbyte = byte(index & 0x00FF)
									svalues[bytepos+1] = curbyte
								}
								bitpos += bits
								upos++
							}
							upos += dy - SubBlockSize
						}
						upos += dz - s.volsize[0]*SubBlockSize
					}
				}

				// Revise the global labels and convert the sub-block index into the block-level label index
				labelNumbers[subBlockNum] = numSBLabels
				slabelList = slabelList[:numSBLabels]
				for label, sbindex := range slabels {
					blockIndex, found := labels[label]
					if found {
						slabelList[sbindex] = blockIndex
					} else {
						labels[label] = numLabels
						slabelList[sbindex] = numLabels
						numLabels++
					}
				}
				if _, err = subBlockBuf.Write(uint32ToByte(slabelList)); err != nil {
					return
				}
				if bits > 0 {
					valueBytes := bitpos >> 3
					valueBytes += valueBytes % 4 // round to 4 byte word for slice.
					if _, err = subBlockBuf.Write(svalues[:valueBytes]); err != nil {
						return
					}
				}
				subBlockNum++
			}
		}
	}
	subBlockBytes = subBlockBuf.Bytes()
	return
}

// SubvolumeToBlock converts a portion of the given label array into a compressed Block.
// It accepts a packed little-endian uint64 label array and a description of its subvolume,
// i.e., its extents in dvid space, and returns a compressed Block for the given chunk when
// tiling dvid space with the given chunk size.
func SubvolumeToBlock(subvol *dvid.Subvolume, uint64array []byte, index dvid.IndexZYX, blockSize dvid.Point3d) (*Block, error) {
	// iterate through the subvolume corresponding to the Block and do encoding
	dvidOff := index.ToVoxelOffset(blockSize) // offset to block in dvid space
	blockOff := dvidOff.Sub(subvol.StartPoint())
	s, err := setSubvolume(uint64array, subvol.Size(), blockOff, blockSize)
	if err != nil {
		return nil, err
	}
	labels, labelNumbers, subBlockBytes, err := s.encodeBlock()
	if err != nil {
		return nil, err
	}
	numLabels := uint32(len(labels))
	numSubBlocks := uint32(len(labelNumbers))

	// Write the entire Block serialization and link to Block and SubBlock structs
	b := new(Block)
	b.size = blockSize

	// -- write the first part of header
	var subBlockDataSize uint32
	if numLabels > 1 {
		subBlockDataSize = numSubBlocks*2 + uint32(len(subBlockBytes))
	}
	data := make([]byte, 16+numLabels*8+subBlockDataSize)
	b.data = data
	gx, gy, gz := s.getSubBlockDims()
	binary.LittleEndian.PutUint32(data[0:4], gx)
	binary.LittleEndian.PutUint32(data[4:8], gy)
	binary.LittleEndian.PutUint32(data[8:12], gz)
	binary.LittleEndian.PutUint32(data[12:16], numLabels)

	// -- write block labels
	numbersPos := 16 + numLabels*8
	if b.Labels, err = byteToUint64(data[16:numbersPos]); err != nil {
		return nil, err
	}
	for label, index := range labels {
		b.Labels[index] = label
	}
	if len(labels) == 1 {
		b.SubBlocks = solidSubBlocks(numSubBlocks)
		return b, nil
	}

	// -- write # of labels for each sub-block
	subBlockPos := numbersPos + numSubBlocks*2
	if b.Numbers, err = byteToUint16(data[numbersPos:subBlockPos]); err != nil {
		return nil, err
	}
	copy(b.Numbers, labelNumbers)

	// -- write all the sub-block data
	copy(data[subBlockPos:], subBlockBytes)

	// -- link the sub-block data to the SubBlock structs, which we couldn't do until
	//    we had the final serialization.
	b.setSubBlocks(numSubBlocks, data[subBlockPos:])
	return b, nil
}

// MakeBlock returns a compressed label Block given a packed little-endian uint64
// label array.  It is the inverse of MakeLabelVolume().  There is no sharing of
// underlying memory between the returned Block and the given byte slice.
func MakeBlock(uint64array []byte, blockSize dvid.Point3d) (*Block, error) {
	// iterate through the subvolume corresponding to the Block and do encoding
	s, err := setSubvolume(uint64array, blockSize, dvid.Point3d{0, 0, 0}, blockSize)
	if err != nil {
		return nil, err
	}
	labels, labelNumbers, subBlockBytes, err := s.encodeBlock()
	if err != nil {
		return nil, err
	}
	numLabels := uint32(len(labels))
	numSubBlocks := uint32(len(labelNumbers))

	// Write the entire Block serialization and link to Block and SubBlock structs
	b := new(Block)
	b.size = blockSize

	// -- write the first part of header
	var subBlockDataSize uint32
	if numLabels > 1 {
		subBlockDataSize = numSubBlocks*2 + uint32(len(subBlockBytes))
	}
	data := make([]byte, 16+numLabels*8+subBlockDataSize)
	b.data = data
	gx, gy, gz := s.getSubBlockDims()
	binary.LittleEndian.PutUint32(data[0:4], gx)
	binary.LittleEndian.PutUint32(data[4:8], gy)
	binary.LittleEndian.PutUint32(data[8:12], gz)
	binary.LittleEndian.PutUint32(data[12:16], numLabels)

	// -- write block labels
	numbersPos := 16 + numLabels*8
	if b.Labels, err = byteToUint64(data[16:numbersPos]); err != nil {
		return nil, err
	}
	for label, index := range labels {
		b.Labels[index] = label
	}
	if len(labels) == 1 {
		b.SubBlocks = solidSubBlocks(numSubBlocks)
		return b, nil
	}

	// -- write # of labels for each sub-block
	subBlockPos := numbersPos + numSubBlocks*2
	if b.Numbers, err = byteToUint16(data[numbersPos:subBlockPos]); err != nil {
		return nil, err
	}
	copy(b.Numbers, labelNumbers)

	// -- write all the sub-block data
	copy(data[subBlockPos:], subBlockBytes)

	// -- link the sub-block data to the SubBlock structs, which we couldn't do until
	//    we had the final serialization.
	b.setSubBlocks(numSubBlocks, data[subBlockPos:])
	return b, nil
}

// MakeLabelVolume returns a byte slice with packed little-endian uint64 labels in ZYX order,
// i.e., a uint64 for each voxel where consecutive values are in the (x,y,z) order:
// (0,0,0), (1,0,0), (2,0,0) ... (0,1,0)
// There is no sharing of memory between the returned byte slice and the Block data.
func (b Block) MakeLabelVolume() (uint64array []byte, size dvid.Point3d, err error) {
	numVoxels := b.size.Prod()
	labeldata := make([]uint64, numVoxels)
	gx, gy, gz := b.size[0]/SubBlockSize, b.size[1]/SubBlockSize, b.size[2]/SubBlockSize

	var subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx++ {
				subBlock := b.SubBlocks[subBlockNum]
				numSBLabels := uint16(len(subBlock.Index))
				bits := int(bitsFor(numSBLabels))

				pos := sz*SubBlockSize*b.size[0]*b.size[1] + sy*SubBlockSize*b.size[0] + sx*SubBlockSize

				var bitpos int
				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
							switch {
							case numSBLabels == 0:
								labeldata[pos] = 0
							case numSBLabels == 1:
								labeldata[pos] = b.Labels[subBlock.Index[0]]
							default:
								var index uint16
								bithead := bitpos % 8
								bytepos := bitpos >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									rightshift := uint(8 - bithead - bits)
									index = uint16((subBlock.Values[bytepos] & leftBitMask[bithead]) >> rightshift)
								} else {
									// index spans byte boundaries
									index = uint16(subBlock.Values[bytepos]&leftBitMask[bithead]) << 8
									index |= uint16(subBlock.Values[bytepos+1])
									index >>= uint(16 - bithead - bits)
								}
								labeldata[pos] = b.Labels[subBlock.Index[index]]
								bitpos += bits
							}
							pos++
						}
						pos += b.size[0] - SubBlockSize
					}
					pos += b.size[0]*b.size[1] - b.size[0]*SubBlockSize
				}
				subBlockNum++
			}
		}
	}

	return uint64ToByte(labeldata), b.size, nil
}

// BlockDelta keeps track of labels, their coarse layout, and their size change in #
// voxels within a 3d subvolume encompassing a Chunk.
type BlockDelta struct {
	Position dvid.ChunkPoint3d
	Present  map[uint64]uint8 //
	Delta    map[uint64]int32 // Change in # voxels for a label
}

// IngestBlock creates a Block from uint64 labels in a packed byte array with
// known size in voxels.
func IngestBlock(cur []byte, size dvid.Point3d) (b Block, bd BlockDelta, err error) {
	return
}

// MutateBlock creates a Block from uint64 labels in a packed byte array with
// known size in voxels, returning a delta that gives the changes in the underlying block.
func MutateBlock(cur []byte, prev Block, size dvid.Point3d) (b Block, bd BlockDelta, err error) {
	return
}

// GoogleCompression writes label compression compliant with the Google Neuroglancer
// specification:   https://goo.gl/IyQbzL
func (b Block) WriteGoogleCompression(w io.Writer) error {
	return fmt.Errorf("labels.Block -> Google Compression not implemented yet")
}
