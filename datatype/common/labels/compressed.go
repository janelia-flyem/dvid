package labels

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/janelia-flyem/dvid/dvid"
)

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

// returns the uint byte size to hold an index.  Can be 1, 2, or 4 bytes.
func indexBytes(n uint32) (bytes uint32) {
	if n <= 256 {
		return 1
	}
	if n <= 65536 {
		return 2
	}
	return 4
}

const SubBlockSize = 8
const DefaultSubBlocksPerBlock = 8
const DefaultBlockSize = DefaultSubBlocksPerBlock * SubBlockSize

// PositionedBlock is a Block that also knows its position in DVID space via a chunk coordinate.
type PositionedBlock struct {
	Block
	BCoord dvid.IZYXString
}

// OffsetDVID returns the DVID voxel coordinate corresponding to the first voxel of the Block,
// i.e., the lowest (x,y,z).
func (pb PositionedBlock) OffsetDVID() (dvid.Point3d, error) {
	return pb.BCoord.VoxelOffset(pb.Size)
}

// Split a label defined by RLEs into a block.  The target label is not necessary since any
// voxel defined by the RLEs that fall within the block are set to the given split label.
func (pb PositionedBlock) Split(op SplitOp) (split *Block, keptSize, splitSize uint64, err error) {
	var offset dvid.Point3d
	if offset, err = pb.OffsetDVID(); err != nil {
		return
	}

	gx, gy, gz := pb.Size[0]/SubBlockSize, pb.Size[1]/SubBlockSize, pb.Size[2]/SubBlockSize
	numSubBlocks := uint32(gx * gy * gz)

	// Create a bitmask for all split voxels of the Block.
	rles := op.RLEs.Offset(offset)
	splitVoxels := make([]bool, pb.Size[0]*pb.Size[1]*pb.Size[2])
	for _, rle := range rles {
		pt := rle.StartPt()
		i := pt[2]*pb.Size[1]*pb.Size[0] + pt[1]*pb.Size[0] + pt[0]
		for x := int32(0); x < rle.Length(); x++ {
			splitVoxels[i] = true
			i++
		}
	}

	// Check if the split label is present.
	var labelIndex uint32
	labelPresent := false
	for i, label := range pb.Labels {
		if label == op.NewLabel {
			labelPresent = true
			labelIndex = uint32(i)
		}
	}
	numLabels := uint32(len(pb.Labels))
	numNewLabels := numLabels
	if !labelPresent {
		labelIndex = numLabels
		numNewLabels++
	}

	// Iterate through all the sub-blocks, determining if the split label adds to that sub-block's indices
	// and therefore changes the # of encoding bits necessary for the values.
	indexAdded := make([]bool, numSubBlocks)    // true if we added index to split label for the sub-block
	svalues := make([]byte, numSubBlocks*512*2) // max size allocation for sub-blocks' encoded values
	var sbNum, indexPos uint32
	var bitpos, bitposNew uint32
	var numNewSubBlockIndices uint32
	for sz := int32(0); sz < gz; sz++ {
		for sy := int32(0); sy < gy; sy++ {
			for sx := int32(0); sx < gx; sx++ {
				numSBLabels := pb.NumSBLabels[sbNum]
				bits := bitsFor(numSBLabels)
				numSBLabelsNew := numSBLabels

				// is the split label already in index?
				found := false
				var splitIndex uint16
				for i := uint16(0); i < numSBLabels; i++ {
					index := pb.SBIndices[indexPos]
					if index == labelIndex {
						found = true
						splitIndex = i
					}
					indexPos++
				}
				if !found {
					indexAdded[sbNum] = true
					splitIndex = numSBLabels
					numSBLabelsNew++
				}
				bitsNew := bitsFor(numSBLabelsNew)
				numNewSubBlockIndices += uint32(numSBLabelsNew)

				if bitsNew > 0 {
					// Transfer the data from old to new with possible added index size.
					for z := int32(0); z < SubBlockSize; z++ {
						blockZ := sz*SubBlockSize + z
						for y := int32(0); y < SubBlockSize; y++ {
							blockY := sy*SubBlockSize + y
							for x := int32(0); x < SubBlockSize; x++ {
								var oldIndex, newIndex uint16
								bithead := bitpos % 8
								bytepos := bitpos >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									rightshift := uint(8 - bithead - bits)
									oldIndex = uint16((pb.SBValues[bytepos] & leftBitMask[bithead]) >> rightshift)
								} else {
									// index spans byte boundaries
									oldIndex = uint16(pb.SBValues[bytepos]&leftBitMask[bithead]) << 8
									oldIndex |= uint16(pb.SBValues[bytepos+1])
									oldIndex >>= uint(16 - bithead - bits)
								}

								blockPos := blockZ*pb.Size[1]*pb.Size[0] + blockY*pb.Size[0] + sx*SubBlockSize + x
								if splitVoxels[blockPos] {
									newIndex = splitIndex
								} else {
									newIndex = oldIndex
								}

								bitheadNew := bitposNew % 8
								byteposNew := bitposNew >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									leftshift := uint(8 - bits - bitheadNew)
									svalues[byteposNew] |= byte(newIndex << leftshift)
								} else {
									// this straddles a byte boundary
									leftshift := uint(16 - bits - bitheadNew)
									newIndex <<= leftshift
									svalues[byteposNew] |= byte((newIndex & 0xFF00) >> 8)
									svalues[byteposNew+1] = byte(newIndex & 0x00FF)
								}

								bitpos += bits
								bitposNew += bitsNew
							}
						}
					}
					// make sure a byte doesn't have two sub-blocks' encoded values
					if bitpos%8 != 0 {
						bitpos += 8 - (bitpos % 8)
					}
					if bitposNew%8 != 0 {
						bitposNew += 8 - (bitposNew % 8)
					}
				}
				sbNum++
			}
		}
	}

	// Write all the labels, num sb labels, sb indices, and values to final buffer.
	subBlockIndexBytes := numNewSubBlockIndices * 4
	subBlockValueBytes := uint32(bitposNew >> 3)
	blockBytes := 16 + numNewLabels*8 + numSubBlocks*2 + subBlockIndexBytes + subBlockValueBytes

	split = new(Block)
	split.Size = pb.Size
	split.data = make([]byte, blockBytes)
	pos := 16 + numLabels*8
	copy(split.data[:pos], pb.data[:pos])
	if !labelPresent {
		binary.LittleEndian.PutUint32(split.data[12:16], numNewLabels)
		binary.LittleEndian.PutUint64(split.data[pos:pos+8], op.NewLabel)
		pos += 8
	}

	nbytes := numSubBlocks * 2
	if split.NumSBLabels, err = dvid.ByteToUint16(split.data[pos : pos+nbytes]); err != nil {
		return
	}
	for i, num := range pb.NumSBLabels {
		if indexAdded[i] {
			split.NumSBLabels[i] = num + 1
		} else {
			split.NumSBLabels[i] = num
		}
	}

	pos += nbytes
	if split.SBIndices, err = dvid.ByteToUint32(split.data[pos : pos+subBlockIndexBytes]); err != nil {
		return
	}
	indexPos = 0
	var newIndexPos uint32
	for sbNum := uint32(0); sbNum < numSubBlocks; sbNum++ {
		for i := uint16(0); i < pb.NumSBLabels[sbNum]; i++ {
			split.SBIndices[newIndexPos] = pb.SBIndices[indexPos]
			newIndexPos++
			indexPos++
		}
		if indexAdded[sbNum] {
			split.SBIndices[newIndexPos] = labelIndex
			newIndexPos++
		}
	}

	pos += subBlockIndexBytes
	split.SBValues = split.data[pos:]
	copy(split.SBValues, svalues[:subBlockValueBytes])

	return
}

// Split a label defined by RLEs into a block.  The target label is not necessary since any
// voxel defined by the RLEs that fall within the block are set to the given split label.
// TODO: If we continue to use RLEs for splits (which we shouldn't), optimize this function
// since it is currently simple but slower than optimal.
/*
func (pb PositionedBlock) SplitBAD(op SplitOp) (split *Block, keptSize, splitSize uint64, err error) {
	split = new(Block)
	var offset dvid.Point3d
	if offset, err = pb.OffsetDVID(); err != nil {
		return
	}

	gx, gy, gz := pb.Size[0]/SubBlockSize, pb.Size[1]/SubBlockSize, pb.Size[2]/SubBlockSize
	numSubBlocks := uint32(gx * gy * gz)

	// Check if the split label is present.
	labelPresent := false
	for i, label := range pb.Labels {
		if label == op.NewLabel {
			labelPresent = true
			labelIndex = i
		}
	}
	if !labelPresent {
		labelIndex = len(pb.Labels)
	}

	// get # of sub-blocks in each dimension within this block.
	nz := pb.Size[2] / SubBlockSize
	ny := pb.Size[1] / SubBlockSize
	nx := pb / Size[0] / SubBlockSize
	numSubBlockVoxels := SubBlockSize * SubBlockSize * SubBlockSize

	sbValuePos := make([]uint32, numSubBlocks)
	var k uint32
	for i, n := range pb.NumSBLabels {
		sbValuePos[i] = k
		bits := uint32(bitsFor(n))
		sbBits := uint32(subBlockNumVoxels) * bits
		valueBytes := sbBits / 8
		if sbBits%8 != 0 {
			valueBytes++
		}
		k += valueBytes
	}

	// If not, create a new label and adjust block data slice to be 8 bytes larger.
	for _, rle := range op.RLEs {
		pos := rle.StartPt()
		bz := pos[2] - offset[2]
		by := pos[1] - offset[1]
		if by < 0 || by >= pb.Size[1] || bz < 0 || bz >= pb.Size[2] {
			continue
		}
		sz := bz / SubBlockSize
		sy := by / SubBlockSize
		goff := sz*ny*nx + sy*nx
		boff := bz*pb.Size[0]*pb.Size[1] + by*pb.Size[0]
		for x := pos[0]; x < pos[0]+rle.Length(); x++ {
			bx := x - offset[0]
			if bx >= 0 && x < pb.Size[0] {
				sbNum := goff + bx/SubBlockSize
				numSBLabels := pb.NumSBLabels[sbNum]
				bits := int(bitsFor(numSBLabels))
				if bits < 2 {
					// Need to modify sub-block and save.
				}
				bithead := bitpos % 8
				bytepos := uint32(bitpos>>3) + sbValuePos[sbNum]
				if bithead+bits <= 8 {
					// index totally within this byte
					leftshift := uint(8 - bits - bithead)
					pb.SBValues[bytepos] |= byte(labelIndex << leftshift)
				} else {
					// this straddles a byte boundary
					leftshift := uint(16 - bits - bithead)
					labelIndex <<= leftshift
					pb.SBValues[bytepos] |= byte((labelIndex & 0xFF00) >> 8)
					pb.SBValues[bytepos+1] = byte(labelIndex & 0x00FF)
				}
			}
		}
	}
	return
}
*/

// MakeSolidBlock returns a Block that represents a single label of the given block size.
func MakeSolidBlock(label uint64, blockSize dvid.Point3d) *Block {
	b := new(Block)
	b.data = make([]byte, 24)

	b.Labels = []uint64{label}
	b.Size = blockSize

	gx := uint32(blockSize[0] / SubBlockSize)
	gy := uint32(blockSize[1] / SubBlockSize)
	gz := uint32(blockSize[2] / SubBlockSize)

	binary.LittleEndian.PutUint32(b.data[0:4], gx)
	binary.LittleEndian.PutUint32(b.data[4:8], gy)
	binary.LittleEndian.PutUint32(b.data[8:12], gz)
	binary.LittleEndian.PutUint32(b.data[12:16], 1)
	binary.LittleEndian.PutUint64(b.data[16:24], label)

	return b
}

// SubvolumeToBlock converts a portion of the given label array into a compressed Block.
// It accepts a packed little-endian uint64 label array and a description of its subvolume,
// i.e., its extents in dvid space, and returns a compressed Block for the given chunk when
// tiling dvid space with the given chunk size.
func SubvolumeToBlock(sv *dvid.Subvolume, lbls []byte, idx dvid.IndexZYX, bsize dvid.Point3d) (*Block, error) {
	dvidOff := idx.ToVoxelOffset(bsize) // offset to block in dvid space
	blockOff := dvidOff.Sub(sv.StartPoint())
	s, err := setSubvolume(lbls, sv.Size(), blockOff, bsize)
	if err != nil {
		return nil, err
	}
	return s.encodeBlock()
}

// MakeBlock returns a compressed label Block given a packed little-endian uint64
// label array.  It is the inverse of MakeLabelVolume().  There is no sharing of
// underlying memory between the returned Block and the given byte slice.
func MakeBlock(uint64array []byte, bsize dvid.Point3d) (*Block, error) {
	// iterate through the subvolume corresponding to the Block and do encoding
	s, err := setSubvolume(uint64array, bsize, dvid.Point3d{0, 0, 0}, bsize)
	if err != nil {
		return nil, err
	}
	return s.encodeBlock()
}

// Block is the unit of storage for compressed DVID labels.  It is inspired by the
// Neuroglancer compression scheme and makes the following changes: (1) a block-level
// label list with sub-block indices into the list (minimal required bits vs 64 bits in
// original Neuroglancer scheme), (2) the number of bits for encoding values is not
// required to be a power of two.  A block-level label list allows easy sharing of labels
// between sub-blocks, and sub-block storage can be more efficient due to the smaller index
// (at the cost of an indirection) and better encoded value packing (at the cost of byte
// alignment).  In both cases memory is gained for increased computation.
//
// Blocks cover nx * ny * nz voxels.  This implementation allows any choice of nx, ny, and nz
// with two restrictions: (1) nx, ny, and nz must be a multiple of 8 greater than 16, and
// (2) the total number of labels cannot exceed the capacity of a uint32.
//
// Internally, labels are stored in 8x8x8 sub-blocks.  There are gx * gy * gz sub-blocks where
// gx = nx / 8; gy = ny / 8; gz = nz / 8.
//
// The byte layout will be the following if there are N labels in the Block:
//
//      3 * uint32      values of gx, gy, and gz
//      uint32          # of labels (N), cannot exceed uint32.
//      N * uint64      packed labels in little-endian format.  Label 0 can be used to represent
//                          deleted labels, e.g., after a merge operation to avoid changing all
//                          sub-block indices.
//
//      ----- Data below is only included if N > 1, otherwise it is a solid block.
//            Nsb = # sub-blocks = gx * gy * gz
//
//      Nsb * uint16        # of labels for sub-blocks.  Each uint16 Ns[i] = # labels for sub-block i.
//                              If Ns[i] == 0, the sub-block has no data (uninitialized), which
//                              is useful for constructing Blocks with sparse data.
//
//      Nsb * Ns * uint32   label indices for sub-blocks where Ns = sum of Ns[i] over all sub-blocks.
//                              For each sub-block i, we have Ns[i] label indices of lBits.
//
//		Nsb * values        sub-block indices for each voxel.
//                              Data encompasses 512 * ceil(log2(Ns[i])) bits, padded so no two
//                              sub-blocks have indices in the same byte.
//                              At most we use 9 bits per voxel for up to the 512 labels in sub-block.
//                              A value gives the sub-block index which points to the index into
//                              the N labels.  If Ns[i] <= 1, there are no values.  If Ns[i] = 0,
//                              the 8x8x8 voxels are set to label 0.  If Ns[i] = 1, all voxels
//                              are the given label index.
type Block struct {
	Labels []uint64
	Size   dvid.Point3d // # voxels in each dimension for this block

	// The folloing exported properties are only non-nil if len(Labels) > 1

	NumSBLabels []uint16 // # of labels for each sub-block
	SBIndices   []uint32 // indices into Labels array
	SBValues    []byte   // compressed voxel values giving index into SBIndices.

	data []byte // serialized format as described above
}

// MergeLabels returns a new block that has computed the given MergeOp.
func (b Block) MergeLabels(op MergeOp) (merged *Block, err error) {
	merged = new(Block)
	merged.data = make([]byte, len(b.data)) // should be at most the length of the unmerged Block
	copy(merged.data, b.data)
	if err = merged.setExportedVars(); err != nil {
		merged = nil
		return
	}

	var targetFound bool
	var targetIndex uint32
	mergedIndices := make(map[uint32]struct{}, len(op.Merged))
	var numMerged uint32
	for i, label := range b.Labels {
		_, found := op.Merged[label]
		if found {
			numMerged++
			mergedIndices[uint32(i)] = struct{}{}
			merged.Labels[i] = 0
		}
		if label == op.Target {
			targetFound = true
			targetIndex = uint32(i)
		}
	}

	if numMerged == 0 {
		return
	}

	if !targetFound {
		var mergedIndex uint32
		for mergedIndex = range mergedIndices {
			break
		}
		targetIndex = mergedIndex
		merged.Labels[targetIndex] = op.Target
	}

	for i, index := range merged.SBIndices {
		if _, found := mergedIndices[index]; found {
			merged.SBIndices[i] = targetIndex
		}
	}
	return
}

// ReplaceLabel replaces references to the target label with newLabel.
func (b Block) ReplaceLabel(target, newLabel uint64) (split *Block, splitSize uint64, err error) {
	split = new(Block)
	split.data = make([]byte, len(b.data))
	copy(split.data, b.data)
	if err = split.setExportedVars(); err != nil {
		split = nil
		return
	}

	var targetFound bool
	var targetIndex uint32
	for i, label := range split.Labels {
		if label == target {
			targetFound = true
			targetIndex = uint32(i)
			break
		}
	}

	if !targetFound {
		splitSize = 0
		return
	}

	split.Labels[targetIndex] = newLabel
	return
}

// ModifyHighres modifies the portion of a Block corresponding to a Block at 2x higher
// resolution.  The passed PositionedBlock should have Block coordinates 2x as dense
// as the Block coordinate of the receiver.
func (b *Block) ModifyHighres(hiresBCoord dvid.IZYXString, hiresBlock *Block) error {
	return nil
}

// FillUninitialized fills in the receiver Block uninitialized sub-blocks with any
// initialized sub-blocks in the src Block.
func (b *Block) FillUninitialized(src *Block) error {
	return nil
}

// MakeLabelVolume returns a byte slice with packed little-endian uint64 labels in ZYX order,
// i.e., a uint64 for each voxel where consecutive values are in the (x,y,z) order:
// (0,0,0), (1,0,0), (2,0,0) ... (0,1,0)
// There is no sharing of memory between the returned byte slice and the Block data.
func (b Block) MakeLabelVolume() (uint64array []byte, size dvid.Point3d) {
	size = b.Size

	numVoxels := b.Size.Prod()
	outarray := make([]uint64, numVoxels)

	gx, gy, gz := b.Size[0]/SubBlockSize, b.Size[1]/SubBlockSize, b.Size[2]/SubBlockSize

	if len(b.Labels) < 2 {
		var label uint64
		if len(b.Labels) == 1 {
			label = b.Labels[0]
		}
		for i := int64(0); i < numVoxels; i++ {
			outarray[i] = label
		}
		uint64array = dvid.Uint64ToByte(outarray)
		return
	}

	subBlockNumVoxels := SubBlockSize * SubBlockSize * SubBlockSize
	sbLabels := make([]uint64, subBlockNumVoxels) // preallocate max # of labels for sub-block

	var indexPos uint32
	var bitpos, subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx++ {

				numSBLabels := b.NumSBLabels[subBlockNum]
				bits := int(bitsFor(numSBLabels))

				for i := uint16(0); i < numSBLabels; i++ {
					sbLabels[i] = b.Labels[b.SBIndices[indexPos]]
					indexPos++
				}

				lblpos := sz*SubBlockSize*b.Size[0]*b.Size[1] + sy*SubBlockSize*b.Size[0] + sx*SubBlockSize

				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
							switch numSBLabels {
							case 0:
								outarray[lblpos] = 0
							case 1:
								outarray[lblpos] = sbLabels[0]
							default:
								var index uint16
								bithead := bitpos % 8
								bytepos := bitpos >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									rightshift := uint(8 - bithead - bits)
									index = uint16((b.SBValues[bytepos] & leftBitMask[bithead]) >> rightshift)
								} else {
									// index spans byte boundaries
									index = uint16(b.SBValues[bytepos]&leftBitMask[bithead]) << 8
									index |= uint16(b.SBValues[bytepos+1])
									index >>= uint(16 - bithead - bits)
								}
								outarray[lblpos] = sbLabels[index]
								bitpos += bits
							}
							lblpos++
						}
						lblpos += b.Size[0] - SubBlockSize
					}
					lblpos += b.Size[0]*b.Size[1] - b.Size[0]*SubBlockSize
				}
				if bitpos%8 != 0 {
					bitpos += 8 - (bitpos % 8)
				}
				subBlockNum++
			}
		}
	}
	uint64array = dvid.Uint64ToByte(outarray)
	return
}

// MarshalBinary implements the encoding.BinaryMarshaler interface. Note that for
// efficiency, the returned byte slice will share memory with the receiver Block.
func (b Block) MarshalBinary() ([]byte, error) {
	return b.data, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.  Note that
// for efficiency, the receive Block will share memory with the given byte slice.
func (b *Block) UnmarshalBinary(data []byte) error {
	b.data = data
	return b.setExportedVars()
}

// assumes b.data is set and we need to compute all other properties of a Block
func (b *Block) setExportedVars() (err error) {
	// Get the sub-blocks along each dimension
	gx := binary.LittleEndian.Uint32(b.data[0:4])
	gy := binary.LittleEndian.Uint32(b.data[4:8])
	gz := binary.LittleEndian.Uint32(b.data[8:12])
	numSubBlocks := uint32(gx * gy * gz)

	b.Size[0] = int32(gx * SubBlockSize)
	b.Size[1] = int32(gy * SubBlockSize)
	b.Size[2] = int32(gz * SubBlockSize)

	numLabels := binary.LittleEndian.Uint32(b.data[12:16])

	pos := uint32(16)
	b.Labels, err = dvid.ByteToUint64(b.data[16 : 16+numLabels*8])
	if err != nil {
		return
	}

	pos += numLabels * 8
	nbytes := numSubBlocks * 2
	b.NumSBLabels, err = dvid.ByteToUint16(b.data[pos : pos+nbytes])
	if err != nil {
		return
	}
	var numSubBlockIndices uint32
	for _, num := range b.NumSBLabels {
		numSubBlockIndices += uint32(num)
	}

	pos += nbytes
	subBlockIndexBytes := numSubBlockIndices * 4
	b.SBIndices, err = dvid.ByteToUint32(b.data[pos : pos+subBlockIndexBytes])
	if err != nil {
		return
	}

	pos += subBlockIndexBytes
	b.SBValues = b.data[pos:]
	return
}

// immutable representation of (y,z) coordinate, suitable for maps.
type yzString string

func getImmutableYZ(y, z int32) yzString {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(y))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(z))
	return yzString(buf)
}

// tracks all RLEs that halted at X edge of a past Block.
type rleBuffer struct {
	rles  map[yzString]dvid.RLE
	coord dvid.ChunkPoint3d // block coord
}

// WriteTo fulfills the io.WriterTo interface.
func (r rleBuffer) WriteTo(w io.Writer) (n int64, err error) {
	for _, rle := range r.rles {
		var curN int64
		curN, err = rle.WriteTo(w)
		if err != nil {
			return
		}
		n += curN
	}
	return
}

// sends all RLEs in buffer without clearing.
func (r rleBuffer) flush(w io.Writer) error {
	if len(r.rles) != 0 {
		if _, err := r.WriteTo(w); err != nil {
			return err
		}
	}
	return nil
}

func (r rleBuffer) clear() {
	for yz := range r.rles {
		delete(r.rles, yz)
	}
}

func (r rleBuffer) extend(yz yzString, pt dvid.Point3d) {
	rle, found := r.rles[yz]
	if found {
		rle.Extend(1)
		r.rles[yz] = rle
	} else {
		r.rles[yz] = dvid.NewRLE(pt, 1)
	}
}

// WriteRLEs, like WriteBinaryBlocks, writes a compact serialization of a binarized Block to
// the supplied Writer.  In this case, the serialization uses little-endian encoded integers
// and RLEs with the repeating units of the following format:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//        int32   Length of run in X direction
//
// The offset is the DVID space offset to the first voxel in the Block.
func WriteRLEs(lbls Set, w io.Writer, pbCh chan *PositionedBlock, bounds dvid.Bounds, errCh chan error) {
	var rleBuf rleBuffer
	for pb := range pbCh {
		bcoord, err := pb.BCoord.ToChunkPoint3d()
		if err != nil {
			errCh <- err
			return
		}
		labelIndices := make(map[uint32]struct{})
		var inBlock bool
		for i, label := range pb.Labels {
			_, found := lbls[label]
			if found {
				labelIndices[uint32(i)] = struct{}{}
				inBlock = true
				if len(labelIndices) == len(lbls) {
					break
				}
			}
		}
		if !inBlock {
			continue
		}

		if rleBuf.rles == nil { // first target-containing block
			yzCap := pb.Size[1] * pb.Size[2]
			rleBuf.rles = make(map[yzString]dvid.RLE, yzCap)
		} else {
			expected := rleBuf.coord
			expected[0]++
			if !expected.Equals(bcoord) {
				rleBuf.flush(w)
				rleBuf.clear()
			}
		}
		if err := pb.writeRLEs(labelIndices, w, &rleBuf, bounds); err != nil {
			errCh <- err
			return
		}
		rleBuf.coord = bcoord
	}

	errCh <- rleBuf.flush(w)
}

func (pb *PositionedBlock) writeRLEs(indices map[uint32]struct{}, w io.Writer, rleBuf *rleBuffer, bounds dvid.Bounds) error {
	offset, err := pb.OffsetDVID()
	if err != nil {
		return err
	}

	var multiForeground bool
	var labelIndex uint32
	if len(indices) > 1 {
		multiForeground = true
	} else {
		for labelIndex = range indices {
			break
		}
	}

	gx, gy, gz := pb.Size[0]/SubBlockSize, pb.Size[1]/SubBlockSize, pb.Size[2]/SubBlockSize
	numSubBlocks := uint32(gx * gy * gz)
	subBlockNumVoxels := SubBlockSize * SubBlockSize * SubBlockSize

	sbIndexPos := make([]uint32, numSubBlocks)
	sbValuePos := make([]uint32, numSubBlocks)
	var j, k uint32
	for i, n := range pb.NumSBLabels {
		sbIndexPos[i] = j
		j += uint32(n)
		sbValuePos[i] = k
		bits := uint32(bitsFor(n))
		sbBits := uint32(subBlockNumVoxels) * bits
		valueBytes := sbBits / 8
		if sbBits%8 != 0 {
			valueBytes++
		}
		k += valueBytes
	}

	curIndices := make([]uint32, subBlockNumVoxels) // preallocate max # of indices for sub-block

	// Keep track of the bit position in each sub-blocks values byte slice so we can easily
	// traverse the sub-blocks in block coordinates.
	minPt := offset
	maxPt := dvid.Point3d{
		offset[0] + pb.Size[0] - 1,
		offset[1] + pb.Size[1] - 1,
		offset[2] + pb.Size[2] - 1,
	}
	if bounds.Exact && bounds.Voxel.IsSet() {
		bounds.Voxel.Adjust(&minPt, &maxPt)
	}

	for vz := minPt[2]; vz <= maxPt[2]; vz++ {
		z := vz - offset[2]
		sbz := vz % SubBlockSize
		dsz := (z / SubBlockSize) * gy * gx

		for vy := minPt[1]; vy <= maxPt[1]; vy++ {
			y := vy - offset[1]
			sby := vy % SubBlockSize
			sbNumStart := dsz + (y/SubBlockSize)*gx
			yz := getImmutableYZ(vy, vz)
			rle, inRun := rleBuf.rles[yz]

			var sbNum int32 = -1
			var numSBLabels uint16
			var foreground, stepByVoxel bool
			var bitpos, bits int32
			var dx int32
			vx := minPt[0]
			for {
				x := vx - offset[0]
				sbNumCur := sbNumStart + x/SubBlockSize
				if sbNum != sbNumCur {
					sbNum = sbNumCur
					numSBLabels = pb.NumSBLabels[sbNum]
					bits = int32(bitsFor(numSBLabels))

					indexPos := sbIndexPos[sbNum]
					for i := uint16(0); i < numSBLabels; i++ {
						curIndices[i] = pb.SBIndices[indexPos]
						indexPos++
					}

					switch numSBLabels {
					case 0:
						return fmt.Errorf("Sub-block with 0 labels detected: %s\n", pb.BCoord)
					case 1:
						dx = SubBlockSize - x%SubBlockSize
						if multiForeground {
							_, foreground = indices[curIndices[0]]
						} else {
							foreground = (curIndices[0] == labelIndex)
						}
						stepByVoxel = false
					default:
						sbx := vx % SubBlockSize
						bitpos = (sbz*SubBlockSize*SubBlockSize + sby*SubBlockSize + sbx) * bits
						dx = 1
						stepByVoxel = true
					}
				}
				if stepByVoxel {
					var index uint16
					bithead := bitpos % 8
					bytepos := uint32(bitpos>>3) + sbValuePos[sbNum]
					if bithead+bits <= 8 {
						// index totally within this byte
						rightshift := uint(8 - bithead - bits)
						index = uint16((pb.SBValues[bytepos] & leftBitMask[bithead]) >> rightshift)
					} else {
						// index spans byte boundaries
						index = uint16(pb.SBValues[bytepos]&leftBitMask[bithead]) << 8
						index |= uint16(pb.SBValues[bytepos+1])
						index >>= uint(16 - bithead - bits)
					}
					bitpos += bits
					if multiForeground {
						_, foreground = indices[curIndices[index]]
					} else {
						foreground = (curIndices[index] == labelIndex)
					}
				}
				if foreground {
					if inRun {
						rle.Extend(dx)
					} else {
						rle = dvid.NewRLE(dvid.Point3d{vx, vy, vz}, dx)
						inRun = true
					}
				} else if inRun {
					if _, err := rle.WriteTo(w); err != nil {
						return err
					}
					inRun = false
				}
				vx += dx
				if vx > maxPt[0] {
					break
				}
			}

			if inRun {
				rleBuf.rles[yz] = rle
			} else {
				delete(rleBuf.rles, yz)
			}
		}
	}
	return nil
}

// WriteBinaryBlocks writes a compact serialization of a binarized Block to the
// supplied Writer.  The serialization is a stream of blocks of the folloing format:
//
//   3 * uint32      values of gx, gy, and gz
//   uint64          foreground label
//
//   Stream of binary blocks with format specified by WriteBinaryBlock() function.
//
func WriteBinaryBlocks(lbls Set, w io.Writer, pbCh chan *PositionedBlock, bounds dvid.Bounds, errCh chan error) {
	for pb := range pbCh {
		labelIndices := make(map[uint32]struct{})
		var inBlock bool
		for i, label := range pb.Labels {
			_, found := lbls[label]
			if found {
				labelIndices[uint32(i)] = struct{}{}
				inBlock = true
				if len(labelIndices) == len(lbls) {
					break
				}
			}
		}
		if inBlock {
			if err := pb.WriteBinaryBlock(labelIndices, w, bounds); err != nil {
				errCh <- err
				return
			}
		}
	}
	errCh <- nil
}

// WriteBinaryBlock writes the binary version of a Block to the supplied Writer, where
// the serialized data represents just the label voxels.  By definition, a binary block
// has at most two labels (0 = background, 1 = given label) and encoding is a bit per voxel.
// The binary format is related to the Google and internal DVID label block compression
// but is simplified, the DVID space offset of the block is included, and the sub-block
// data are arranged to allow streaming.
//
// Internally, the mask is stored in 8x8x8 sub-blocks.  There are gx * gy * gz sub-blocks where
// gx = nx / 8; gy = ny / 8; gz = nz / 8, and (gx, gy, gz) is relayed in a header outside of
// the data returned by this function.  For example, for a full sparse volume response, there
// would be a header followed by some number of these binary blocks.
//
// The byte layout will be the following:
//
//      3 * int32       offset of first voxel of Block in DVID space (x, y, z)
//      byte            content flag:
//                      0 = background ONLY  (no more data for this block)
//                      1 = foreground ONLY  (no more data for this block)
//                      2 = both background and foreground so stream of sub-blocks required.
//
//      Stream of gx * gy * gz sub-blocks with the following data:
//
//      byte            content flag:
//                      0 = background ONLY  (no more data for this sub-block)
//                      1 = foreground ONLY  (no more data for this sub-block)
//                      2 = both background and foreground so mask data required.
//      mask            64 byte bitmask where each voxel is 0 (background) or 1 (foreground)
func (pb *PositionedBlock) WriteBinaryBlock(indices map[uint32]struct{}, w io.Writer, bounds dvid.Bounds) error {
	var multiForeground bool
	var labelIndex uint32
	if len(indices) > 1 {
		multiForeground = true
	} else {
		for labelIndex = range indices {
			break
		}
	}

	offset, err := pb.OffsetDVID()
	if err != nil {
		return err
	}

	buf := make([]byte, 13)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(offset[0]))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(offset[1]))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(offset[2]))

	numLabels := binary.LittleEndian.Uint32(pb.data[12:16])
	switch numLabels {
	case 0:
		buf[12] = 0
	case 1:
		buf[12] = 1
	default:
		buf[12] = 2
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	if numLabels < 2 {
		return nil
	}

	gx, gy, gz := pb.Size[0]/SubBlockSize, pb.Size[1]/SubBlockSize, pb.Size[2]/SubBlockSize

	subBlockNumVoxels := SubBlockSize * SubBlockSize * SubBlockSize
	curIndices := make([]uint32, subBlockNumVoxels) // preallocate max # of indices for sub-block

	data := make([]byte, 65) // sub-block data will at most be status byte + 64 bytes (8x8x8 bits).

	var indexPos uint32
	var bitpos, subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx++ {

				numSBLabels := pb.NumSBLabels[subBlockNum]
				bits := int(bitsFor(numSBLabels))

				for i := uint16(0); i < numSBLabels; i++ {
					curIndices[i] = pb.SBIndices[indexPos]
					indexPos++
				}

				switch numSBLabels {
				case 0:
					data[0] = 0
					if _, err := w.Write(data[:1]); err != nil {
						return err
					}
					continue
				case 1:
					var foreground bool
					if multiForeground {
						_, foreground = indices[curIndices[0]]
					} else {
						foreground = (curIndices[0] == labelIndex)
					}
					if foreground {
						data[0] = 1
					} else {
						data[0] = 0
					}
					if _, err := w.Write(data[:1]); err != nil {
						return err
					}
					continue
				default:
				}

				outbitpos := int(8) // Start at 2nd byte for output

				var background bool // true if a non-index voxel is in block
				var foreground bool // true if index is in block
				var curbyte byte    // byte in values slice under the write head

				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
							// read full label sub-block data
							var index uint16
							bithead := bitpos % 8
							bytepos := bitpos >> 3
							if bithead+bits <= 8 {
								// index totally within this byte
								rightshift := uint(8 - bithead - bits)
								index = uint16((pb.SBValues[bytepos] & leftBitMask[bithead]) >> rightshift)
							} else {
								// index spans byte boundaries
								index = uint16(pb.SBValues[bytepos]&leftBitMask[bithead]) << 8
								index |= uint16(pb.SBValues[bytepos+1])
								index >>= uint(16 - bithead - bits)
							}

							// write binary sub-block data
							bithead = outbitpos % 8
							bytepos = outbitpos >> 3
							var value byte
							if multiForeground {
								_, foreground = indices[curIndices[index]]
							} else {
								foreground = (curIndices[index] == labelIndex)
							}
							if foreground {
								value = 1
							}
							leftshift := uint(7 - bithead)
							curbyte |= value << leftshift
							data[bytepos] = curbyte
							if bithead == 7 {
								curbyte = 0x00
							}

							bitpos += bits
							outbitpos++
						}
					}
				}

				if background && foreground {
					data[0] = 2
					if _, err := w.Write(data); err != nil {
						return err
					}
				} else if foreground {
					data[0] = 1
					if _, err := w.Write(data[:1]); err != nil {
						return err
					}
				} else {
					data[0] = 0
					if _, err := w.Write(data[:1]); err != nil {
						return err
					}
				}

				rem := bitpos % 8
				if rem != 0 {
					bitpos += 8 - rem
				}
				subBlockNum++
			}
		}
	}

	return nil
}

// GoogleCompression writes label compression compliant with the Google Neuroglancer
// specification:   https://goo.gl/IyQbzL
func (b Block) WriteGoogleCompression(w io.Writer) error {
	return fmt.Errorf("labels.Block -> Google Compression not implemented yet")
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

func (s subvolumeData) getSize() dvid.Point3d {
	return dvid.Point3d{int32(s.volsize[0]), int32(s.volsize[1]), int32(s.volsize[2])}
}

// run checks and do conversions
func setSubvolume(uint64array []byte, volsize, blockOff dvid.Point, blockSize dvid.Point3d) (*subvolumeData, error) {
	if volsize.Prod() >= math.MaxUint32 {
		return nil, fmt.Errorf("Volume %s is too large.  Please decrease array dimensions to have at most %d voxels", volsize, math.MaxUint32)
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
	s.data, err = dvid.ByteToUint64(uint64array)
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

// left mask for bithead at each bit position in a byte
var leftBitMask [8]byte = [8]byte{
	0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01,
}

// map of label -> index position in sub-block
type subBlockIndex map[uint64]uint16

// iterate through the subvolume corresponding to the Block and do encoding
func (s *subvolumeData) encodeBlock() (*Block, error) {
	gx, gy, gz := s.getSubBlockDims()
	numSubBlocks := gx * gy * gz

	numSubBlockLabels := make([]uint16, numSubBlocks)      // # of labels in each sub-block
	subBlockIndices := make([]subBlockIndex, numSubBlocks) // sub-block indexing

	// Full Pass: Compute everything but the label indices for sub-blocks since we don't have
	// the entire block-level label list until the end of the first pass.
	dy := s.volsize[0]
	dz := s.volsize[0] * s.volsize[1]

	svalues := make([]byte, numSubBlocks*512*2) // max size allocation for sub-blocks' encoded values
	var bitpos int

	var subBlockNum int
	for sz := uint32(0); sz < gz; sz++ {
		uz := sz*SubBlockSize + s.blockOff[2]
		for sy := uint32(0); sy < gy; sy++ {
			uy := sy*SubBlockSize + s.blockOff[1]
			for sx := uint32(0); sx < gx; sx++ {
				ux := sx*SubBlockSize + s.blockOff[0]

				// 1st pass: get # labels for this sub-block
				var numSBLabels uint16
				slabels := make(subBlockIndex) // map of label -> index position in sub-block

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
				subBlockIndices[subBlockNum] = slabels
				numSubBlockLabels[subBlockNum] = numSBLabels

				// 2nd pass through sub-block, write indices now that we know required bits per voxel.
				bits := int(bitsFor(numSBLabels))
				if bits > 0 {
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
									svalues[bytepos] |= byte(index << leftshift)
								} else {
									// this straddles a byte boundary
									leftshift := uint(16 - bits - bithead)
									index <<= leftshift
									svalues[bytepos] |= byte((index & 0xFF00) >> 8)
									svalues[bytepos+1] = byte(index & 0x00FF)
								}
								bitpos += bits
								upos++
							}
							upos += dy - SubBlockSize
						}
						upos += dz - s.volsize[0]*SubBlockSize
					}

					// make sure a byte doesn't have two sub-blocks' encoded values
					if bitpos%8 != 0 {
						bitpos += 8 - (bitpos % 8)
					}
				}
				subBlockNum++
			}
		}
	}

	// Compute block-level label table

	var numLabels uint32
	var numSubBlockIndices uint32
	labels := make(map[uint64]uint32)
	for _, slabels := range subBlockIndices {
		numSubBlockIndices += uint32(len(slabels))
		for label := range slabels {
			_, found := labels[label]
			if !found {
				labels[label] = numLabels
				numLabels++
			}
		}
	}

	// Write all the data to the Block buffer.
	b := new(Block)
	subBlockIndexBytes := numSubBlockIndices * 4
	subBlockValueBytes := uint32(bitpos >> 3)
	blockBytes := 16 + numLabels*8 + numSubBlocks*2 + subBlockIndexBytes + subBlockValueBytes
	b.data = make([]byte, blockBytes)

	b.Size = s.getSize()

	binary.LittleEndian.PutUint32(b.data[0:4], gx)
	binary.LittleEndian.PutUint32(b.data[4:8], gy)
	binary.LittleEndian.PutUint32(b.data[8:12], gz)
	binary.LittleEndian.PutUint32(b.data[12:16], numLabels)

	pos := uint32(16)
	var err error
	b.Labels, err = dvid.ByteToUint64(b.data[pos : pos+numLabels*8])
	if err != nil {
		return nil, err
	}
	for label, index := range labels {
		b.Labels[index] = label
	}

	pos += numLabels * 8
	nbytes := numSubBlocks * 2
	b.NumSBLabels, err = dvid.ByteToUint16(b.data[pos : pos+nbytes])
	if err != nil {
		return nil, err
	}
	copy(b.NumSBLabels, numSubBlockLabels)

	pos += nbytes
	b.SBIndices, err = dvid.ByteToUint32(b.data[pos : pos+subBlockIndexBytes])
	if err != nil {
		return nil, err
	}
	var i uint32
	for sbNum, sbmap := range subBlockIndices {
		for label, sbindex := range sbmap {
			labelIndex, found := labels[label]
			if !found {
				return nil, fmt.Errorf("Found label %d not in block-level map!", label)
			}
			b.SBIndices[i+uint32(sbindex)] = labelIndex
		}
		i += uint32(b.NumSBLabels[sbNum])
	}

	pos += subBlockIndexBytes
	b.SBValues = b.data[pos:]
	copy(b.SBValues, svalues[:subBlockValueBytes])

	return b, nil
}
