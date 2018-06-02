package labels

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

const SubBlockSize = 8
const DefaultSubBlocksPerBlock = 8
const DefaultBlockSize = DefaultSubBlocksPerBlock * SubBlockSize

const MaxBlockSize = 1024 // N^3 < max uint32, so N <= 2^10
const MaxSubBlockSize = MaxBlockSize / SubBlockSize

// PositionedBlock is a Block that also knows its position in DVID space via a chunk coordinate.
type PositionedBlock struct {
	Block
	BCoord dvid.IZYXString
}

func (pb PositionedBlock) String() string {
	return pb.BCoord.String()
}

// OffsetDVID returns the DVID voxel coordinate corresponding to the first voxel of the Block,
// i.e., the lowest (x,y,z).
func (pb PositionedBlock) OffsetDVID() (dvid.Point3d, error) {
	return pb.BCoord.VoxelOffset(pb.Size)
}

// SplitWithStats writes a new label into the RLEs defined by the split and returns how each supervoxel
// (counts key) was split.  This is done by doing full expansion of block into uint64 array.
func (pb PositionedBlock) SplitWithStats(op SplitOp, m *SVSplitMap, newLabelFunc func() (uint64, error)) (split *Block, counts map[uint64]SVSplitCount, err error) {
	var offset dvid.Point3d
	if offset, err = pb.OffsetDVID(); err != nil {
		return
	}
	lblarrayBytes, _ := pb.MakeLabelVolume()
	lblarray, err := dvid.ByteToUint64(lblarrayBytes)
	if err != nil {
		return
	}
	counts = make(map[uint64]SVSplitCount)

	replacements := make(map[uint64]uint64)
	rles := op.RLEs.Offset(offset)
	for _, rle := range rles {
		pt := rle.StartPt()
		i := pt[2]*pb.Size[1]*pb.Size[0] + pt[1]*pb.Size[0] + pt[0]
		for x := int32(0); x < rle.Length(); x++ {
			lbl := lblarray[i]
			if lbl != 0 {
				svc := counts[lbl]

				var relabel SVSplit
				relabel, _, err = m.getMapping(lbl, newLabelFunc)
				if err != nil {
					return
				}
				lblarray[i] = relabel.Split
				replacements[lbl] = relabel.Remain
				svc.SVSplit = relabel
				svc.Voxels++
				counts[lbl] = svc
			}
			i++
		}
	}

	if split, err = MakeBlock(lblarrayBytes, pb.Size); err != nil {
		return
	}
	if len(replacements) > 0 {
		split, _, err = split.ReplaceLabels(replacements)
	}
	return
}

// Split a target label using RLEs within a block.  Only the target label is split.
// A nil split block is returned if target label is not within block.
// TODO: If RLEs continue to be used for splits, refactor / split up to make this more readable.
func (pb PositionedBlock) Split(op SplitOp) (split *Block, keptSize, splitSize uint64, err error) {
	return pb.splitSlow(op)
	// return pb.splitFast(op)
}

// splitSlow splits a target label using RLEs within a block by doing full expansion of block into
// uint64 array.  Only the target label is split.  A nil split block is returned if target label
// is not within block.
func (pb PositionedBlock) splitSlow(op SplitOp) (split *Block, keptSize, splitSize uint64, err error) {
	var offset dvid.Point3d
	if offset, err = pb.OffsetDVID(); err != nil {
		return
	}
	lblarrayBytes, _ := pb.MakeLabelVolume()
	lblarray, err := dvid.ByteToUint64(lblarrayBytes)
	if err != nil {
		return
	}
	for i := 0; i < len(lblarray); i++ {
		if lblarray[i] == op.Target {
			keptSize++
		}
	}
	if keptSize == 0 {
		return
	}

	rles := op.RLEs.Offset(offset)
	for _, rle := range rles {
		pt := rle.StartPt()
		i := pt[2]*pb.Size[1]*pb.Size[0] + pt[1]*pb.Size[0] + pt[0]
		for x := int32(0); x < rle.Length(); x++ {
			lbl := lblarray[i]
			if lbl == op.Target {
				lblarray[i] = op.NewLabel
				splitSize++
				keptSize--
			}
			i++
		}
	}

	split, err = MakeBlock(lblarrayBytes, pb.Size)
	return
}

// not working at this time.
func (pb PositionedBlock) splitFast(op SplitOp) (split *Block, keptSize, splitSize uint64, err error) {
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
			if i >= int32(len(splitVoxels)) {
				err = fmt.Errorf("bad RLE / block size: rle %s, block size %s, offset %s", rle, pb.Size, offset)
				return
			}
			// fmt.Printf("Added split voxel @ (%d, %d, %d)\n", pt[0]+x+offset[0], pt[1]+offset[1], pt[2]+offset[2])
			splitVoxels[i] = true
			i++
		}
	}

	// Check if the target and split label is present.
	var splitIndex, targetIndex uint32
	var splitPresent, targetPresent bool
	for i, label := range pb.Labels {
		if label == op.NewLabel {
			splitPresent = true
			splitIndex = uint32(i)
		}
		if label == op.Target {
			targetPresent = true
			targetIndex = uint32(i)
		}
	}
	if !targetPresent {
		return
	}
	numLabels := uint32(len(pb.Labels))
	numNewLabels := numLabels
	if !splitPresent {
		splitIndex = numLabels
		numNewLabels++
	}

	// Iterate through all the sub-blocks, determining if the split label adds to that sub-block's indices
	// and therefore changes the # of encoding bits necessary for the values.
	subBlockNumVoxels := uint32(SubBlockSize * SubBlockSize * SubBlockSize)
	indexAdded := make([]bool, numSubBlocks)    // true if we added index to split label for the sub-block
	svalues := make([]byte, numSubBlocks*512*2) // max size allocation for sub-blocks' encoded values
	var sbNum, indexPos uint32
	var bitpos, bitposNew uint32
	var numNewSubBlockIndices uint32
	for sz := int32(0); sz < gz; sz++ {
		for sy := int32(0); sy < gy; sy++ {
			for sx := int32(0); sx < gx; sx, sbNum = sx+1, sbNum+1 {
				numSBLabels := pb.NumSBLabels[sbNum]
				bits := bitsFor(numSBLabels)
				numSBLabelsNew := numSBLabels
				numNewSubBlockIndices += uint32(numSBLabels)

				// is the target or split label in index?
				var sbSplitFound, sbTargetFound bool
				var sbSplitIndex, sbTargetIndex uint16
				for i := uint16(0); i < numSBLabels; i++ {
					index := pb.SBIndices[indexPos]
					if index == splitIndex {
						sbSplitFound = true
						sbSplitIndex = i
					}
					if index == targetIndex {
						sbTargetFound = true
						sbTargetIndex = i
					}
					indexPos++
				}
				if !sbTargetFound {
					// We can skip this sub-block.
					bytepos := bitpos >> 3
					byteposNew := bitposNew >> 3
					sbBits := bits * subBlockNumVoxels
					if sbBits%8 != 0 {
						sbBits += 8 - (sbBits % 8)
					}
					sbBytes := sbBits >> 3
					copy(svalues[byteposNew:byteposNew+sbBytes], pb.SBValues[bytepos:bytepos+sbBytes])
					bitpos += sbBits
					bitposNew += sbBits
					continue
				}
				if !sbSplitFound {
					indexAdded[sbNum] = true
					sbSplitIndex = numSBLabels
					numSBLabelsNew++
					numNewSubBlockIndices++
				}
				bitsNew := bitsFor(numSBLabelsNew)

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

								newIndex = oldIndex
								if oldIndex == sbTargetIndex {
									blockPos := blockZ*pb.Size[1]*pb.Size[0] + blockY*pb.Size[0] + sx*SubBlockSize + x
									if splitVoxels[blockPos] {
										newIndex = sbSplitIndex
										splitSize++
									} else {
										keptSize++
									}
								}

								bitheadNew := bitposNew % 8
								byteposNew := bitposNew >> 3
								if bithead+bits <= 8 {
									// index totally within this byte
									leftshift := uint(8 - bitsNew - bitheadNew)
									svalues[byteposNew] |= byte(newIndex << leftshift)
								} else {
									// this straddles a byte boundary
									leftshift := uint(16 - bitsNew - bitheadNew)
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
			}
		}
	}

	// Write all the labels, num sb labels, sb indices, and values to final buffer.
	subBlockIndexBytes := numNewSubBlockIndices * 4
	subBlockValueBytes := uint32(bitposNew >> 3)
	blockBytes := 16 + numNewLabels*8 + numSubBlocks*2 + subBlockIndexBytes + subBlockValueBytes

	split = new(Block)
	split.Size = pb.Size
	split.data = dvid.New8ByteAlignBytes(blockBytes)
	pos := uint32(16)
	nbytes := numLabels * 8
	copy(split.data[:pos+nbytes], pb.data[:pos+nbytes])
	if !splitPresent {
		binary.LittleEndian.PutUint32(split.data[12:16], numNewLabels)
		binary.LittleEndian.PutUint64(split.data[pos+nbytes:pos+nbytes+8], op.NewLabel)
		nbytes += 8
	}
	if split.Labels, err = dvid.ByteToUint64(split.data[pos : pos+nbytes]); err != nil {
		return
	}

	pos += nbytes
	nbytes = numSubBlocks * 2
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
			split.SBIndices[newIndexPos] = splitIndex
			newIndexPos++
		}
	}

	pos += subBlockIndexBytes
	split.SBValues = split.data[pos:]
	copy(split.SBValues, svalues[:subBlockValueBytes])
	return
}

// SplitSupervoxel splits a target supervoxel using RLEs within a block.
func (pb PositionedBlock) SplitSupervoxel(op SplitSupervoxelOp) (split *Block, keptSize, splitSize uint64, err error) {
	var offset dvid.Point3d
	if offset, err = pb.OffsetDVID(); err != nil {
		return
	}
	lblarrayBytes, _ := pb.MakeLabelVolume()
	lblarray, err := dvid.ByteToUint64(lblarrayBytes)
	if err != nil {
		return
	}

	brles, found := op.Split[pb.BCoord]
	if found {
		rles := brles.Offset(offset)
		for _, rle := range rles {
			pt := rle.StartPt()
			i := pt[2]*pb.Size[1]*pb.Size[0] + pt[1]*pb.Size[0] + pt[0]
			for x := int32(0); x < rle.Length(); x++ {
				if lblarray[i] == op.Supervoxel {
					lblarray[i] = op.SplitSupervoxel
					splitSize++
				}
				i++
			}
		}
	}

	for i := 0; i < len(lblarray); i++ {
		if lblarray[i] == op.Supervoxel {
			lblarray[i] = op.RemainSupervoxel
			keptSize++
		}
	}

	split, err = MakeBlock(lblarrayBytes, pb.Size)
	return
}

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
//      Nsb * uint16        # of labels for sub-blocks (Ns[i]).
//                              Each uint16 Ns[i] = # labels for sub-block i.
//                              If Ns[i] == 0, the sub-block has no data (uninitialized), which
//                              is useful for constructing Blocks with sparse data.
//
//      Nsb * Ns * uint32   label indices for sub-blocks where Ns = sum of Ns[i] over all sub-blocks.
//                              For each sub-block i, we have Ns[i] label indices of lBits.
//
//      Nsb * values        sub-block indices for each voxel.
//                              Data encompasses 512 * ceil(log2(Ns[i])) bits, padded so no two
//                              sub-blocks have indices in the same byte.
//                              At most we use 9 bits per voxel for up to the 512 labels in sub-block.
//                              A value gives the sub-block index which points to the index into
//                              the N labels.  If Ns[i] <= 1, there are no values.  If Ns[i] = 0,
//                              the 8x8x8 voxels are set to label 0.  If Ns[i] = 1, all voxels
//                              are the given label index.
type Block struct {
	Labels []uint64     // labels in Block.
	Size   dvid.Point3d // # voxels in each dimension for this block

	// The folloing exported properties are only non-nil if len(Labels) > 1

	NumSBLabels []uint16 // # of labels for each sub-block
	SBIndices   []uint32 // indices into Labels array
	SBValues    []byte   // compressed voxel values giving index into SBIndices.

	data []byte // serialized format as described above
}

// CompressGZIP returns a gzip compressed encoding of the serialized block data.
func (b Block) CompressGZIP() ([]byte, error) {
	var gzipOut bytes.Buffer
	zw := gzip.NewWriter(&gzipOut)
	if _, err := zw.Write(b.data); err != nil {
		return nil, err
	}
	zw.Flush()
	zw.Close()
	return gzipOut.Bytes(), nil
}

// CalcNumLabels calculates the change in the number of voxels under each non-zero label.
// If a previous Block is given, the change is calculated from the previous numbers.
func (b Block) CalcNumLabels(prev *Block) map[uint64]int32 {
	delta := make(map[uint64]int32)

	// if previous block given, init those counts as negative
	if prev != nil {
		prev.calcNumLabels(delta, false)
	}
	b.calcNumLabels(delta, true)

	return delta
}

func (b Block) calcNumLabels(delta map[uint64]int32, add bool) {
	numVoxels := int32(b.Size.Prod())

	switch len(b.Labels) {
	case 0:
		dvid.Infof("Block has 0 labels!\n")
		return
	case 1:
		if b.Labels[0] == 0 {
			return
		}
		if add {
			delta[b.Labels[0]] += numVoxels
		} else {
			delta[b.Labels[0]] -= numVoxels
		}
		return
	default:
	}

	gx, gy, gz := b.Size[0]/SubBlockSize, b.Size[1]/SubBlockSize, b.Size[2]/SubBlockSize
	subBlockNumVoxels := int32(SubBlockSize * SubBlockSize * SubBlockSize)
	sbLabels := make([]uint64, subBlockNumVoxels) // preallocate max # of labels for sub-block

	var indexPos uint32
	var bitpos, subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx, subBlockNum = sx+1, subBlockNum+1 {

				numSBLabels := b.NumSBLabels[subBlockNum]

				switch numSBLabels {
				case 0:
					continue
				case 1:
					label := b.Labels[b.SBIndices[indexPos]]
					indexPos++
					if label == 0 {
						continue
					}
					if add {
						delta[label] += subBlockNumVoxels
					} else {
						delta[label] -= subBlockNumVoxels
					}
					continue
				default:
				}
				bits := int(bitsFor(numSBLabels))
				for i := uint16(0); i < numSBLabels; i++ {
					sbLabels[i] = b.Labels[b.SBIndices[indexPos]]
					indexPos++
				}

				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
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
							label := sbLabels[index]
							if label != 0 {
								if add {
									delta[label]++
								} else {
									delta[label]--
								}
							}
							bitpos += bits
						}
					}
				}
				if bitpos%8 != 0 {
					bitpos += 8 - (bitpos % 8)
				}
			}
		}
	}
}

// get the # voxels corresponding to a particular label index.
func (b *Block) getNumVoxels(labelIndex uint32) (labelVoxels uint64) {
	gx, gy, gz := b.Size[0]/SubBlockSize, b.Size[1]/SubBlockSize, b.Size[2]/SubBlockSize
	subBlockNumVoxels := uint64(SubBlockSize * SubBlockSize * SubBlockSize)

	if len(b.Labels) == 0 {
		return 0
	} else if len(b.Labels) == 1 {
		if labelIndex == 0 {
			return uint64(b.Size.Prod())
		}
		return 0
	}

	var indexPos uint32
	var bitpos, subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx, subBlockNum = sx+1, subBlockNum+1 {

				numSBLabels := b.NumSBLabels[subBlockNum]

				switch numSBLabels {
				case 0:
					continue
				case 1:
					if b.SBIndices[indexPos] == labelIndex {
						labelVoxels += subBlockNumVoxels
					}
					indexPos++
					continue
				default:
				}

				var found bool
				var targetIndex uint16
				for i := uint16(0); i < numSBLabels; i++ {
					if b.SBIndices[indexPos] == labelIndex {
						found = true
						targetIndex = i
					}
					indexPos++
				}
				if !found {
					continue
				}
				bits := int(bitsFor(numSBLabels))

				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
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
							if index == targetIndex {
								labelVoxels++
							}
							bitpos += bits
						}
					}
				}
				if bitpos%8 != 0 {
					bitpos += 8 - (bitpos % 8)
				}
			}
		}
	}
	return
}

// MergeLabels returns a new block that has computed the given MergeOp.
func (b *Block) MergeLabels(op MergeOp) (merged *Block, err error) {
	merged = new(Block)
	merged.data = dvid.New8ByteAlignBytes(uint32(len(b.data))) // at most the length of the unmerged Block
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
		targetIndex = mergedIndex // use the first of the merged labels as target label
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
func (b *Block) ReplaceLabel(target, newLabel uint64) (replace *Block, replaceSize uint64, err error) {
	replace = new(Block)
	replace.data = dvid.New8ByteAlignBytes(uint32(len(b.data)))
	copy(replace.data, b.data)
	if err = replace.setExportedVars(); err != nil {
		replace = nil
		return
	}

	var targetFound bool
	var targetIndex uint32
	for i, label := range replace.Labels {
		if label == target {
			targetFound = true
			targetIndex = uint32(i)
			break
		}
	}

	if !targetFound {
		replaceSize = 0
		return
	}

	replace.Labels[targetIndex] = newLabel
	replaceSize = replace.getNumVoxels(targetIndex)
	return
}

// ReplaceLabels replaces labels according to mapping and doesn't compute sizes.
func (b *Block) ReplaceLabels(mapping map[uint64]uint64) (replace *Block, replaced bool, err error) {
	replace = new(Block)
	replace.data = dvid.New8ByteAlignBytes(uint32(len(b.data)))
	copy(replace.data, b.data)
	if err = replace.setExportedVars(); err != nil {
		replace = nil
		return
	}

	for i, label := range replace.Labels {
		replacement, found := mapping[label]
		if found {
			replaced = true
			replace.Labels[i] = replacement
		}
	}
	return
}

// sub-block data after downres.
type sbData struct {
	indices []uint32
	values  []byte
}

// returns true if we set Block to blank 0 or some solid label
func (b *Block) setBlank(octants [8]*Block) bool {
	var ok bool
	var lbl uint64
	if octants[0] == nil {
		ok = true // nil octants are solid label 0 block
	} else if len(octants[0].Labels) == 1 {
		lbl = octants[0].Labels[0]
		ok = true
	}
	if ok {
		for i := 1; i < 8; i++ {
			if octants[i] == nil {
				if lbl != 0 {
					ok = false
					break
				}
			} else if len(octants[i].Labels) != 1 || lbl != octants[i].Labels[0] {
				ok = false
				break
			}
		}
		if ok {
			*b = *MakeSolidBlock(lbl, b.Size)
			return true
		}
	}
	return false
}

// DownresSlow is same as Downres() but uses simpler and more memory/compute-intensive
// approach to computing down-res block.
func (b *Block) DownresSlow(octants [8]*Block) error {
	filled := true
	for i := 0; i < 8; i++ {
		if octants[i] == nil {
			filled = false
			break
		}
	}
	var result []byte
	if filled {
		numArrayBytes := b.Size.Prod() * 8
		result = make([]byte, numArrayBytes)
	} else {
		result, _ = b.MakeLabelVolume()
	}

	for i := 0; i < 8; i++ {
		if octants[i] == nil {
			continue
		}
		uint64array, size := octants[i].MakeLabelVolume()
		if !size.Equals(b.Size) {
			return fmt.Errorf("octant block size %s not equal to original size %s", size, b.Size)
		}

		oz := int32(i) >> 2
		oy := (int32(i) - oz*4) >> 1
		ox := int32(i) % 2
		vz := oz * b.Size[2] >> 1 // voxel offset into result array
		vy := oy * b.Size[1] >> 1
		vx := ox * b.Size[0] >> 1

		downresArray(uint64array, result, vx, vy, vz, size)
	}

	resultBlock, err := MakeBlock(result, b.Size)
	if err != nil {
		return err
	}
	*b = *resultBlock
	return nil
}

// downres hires array by 2x and store in portion of lores array with offset voxel (vx, vy, vz)
func downresArray(hires, lores []byte, vx, vy, vz int32, blockSize dvid.Point3d) {
	votemap := make(map[uint64]int, 8)
	nyx := blockSize[0] * blockSize[1]
	var lx, ly, lz, x, y, z int32
	for z = 0; z < blockSize[2]; z += 2 {
		lz = z >> 1
		for y = 0; y < blockSize[1]; y += 2 {
			ly = y >> 1
			for x = 0; x < blockSize[0]; x += 2 {
				lx = x >> 1

				var ix, iy, iz int32
				for iz = 0; iz < 2; iz++ {
					for iy = 0; iy < 2; iy++ {
						for ix = 0; ix < 2; ix++ {
							i := (z+iz)*nyx + (y+iy)*blockSize[0] + x + ix
							lbl := binary.LittleEndian.Uint64(hires[i*8 : i*8+8])
							if lbl != 0 {
								votemap[lbl]++
							}
						}
					}
				}

				var winner uint64
				var winnerVotes int
				for lbl, votes := range votemap {
					if winnerVotes < votes {
						winnerVotes = votes
						winner = lbl
					}
					delete(votemap, lbl)
				}
				li := (lz+vz)*nyx + (ly+vy)*blockSize[0] + lx + vx
				binary.LittleEndian.PutUint64(lores[li*8:li*8+8], winner)
			}
		}
	}
}

// Downres takes eight Blocks that represent higher-resolution octants (by 2x) of
// the receiving block, and modifies the receiving Block to be a half-resolution
// representation.  If a given octant is a nil Block, the receiving Block is not modified
// for that portion of the higher-resolution octant.
func (b *Block) Downres(octants [8]*Block) error {
	if b == nil {
		return fmt.Errorf("block cannot be nil but can be an empty block")
	}

	// if all octants are solid and of same label, produce a solid block as well.
	if b.setBlank(octants) {
		return nil
	}

	err := b.DownresSlow(octants)
	// fmt.Printf("After downres, block: %v\n", *b)
	return err
	/*

		// check consistency of octants
		var gx, gy, gz int32 // # sub-blocks in each dimension
		var filled, maxLabels, maxSBIndices int
		for i, oct := range octants {
			if oct != nil {
				filled++
				maxLabels += len(oct.Labels)
				x, y, z := oct.Size[0]/SubBlockSize, oct.Size[1]/SubBlockSize, oct.Size[2]/SubBlockSize
				if filled > 1 && (x != gx || y != gy || z != gz) {
					return fmt.Errorf("can't downres with octants of different size: (%d,%d,%d) vs (%d,%d,%d)", x, y, z, gx, gy, gz)
				}
				gx, gy, gz = x, y, z
				maxSBIndices += len(oct.SBIndices)
				fmt.Printf("Octant %d: %d labels, %d indices in %d x %d x %d sub-blocks\n", i, len(oct.Labels), len(oct.SBIndices), gx, gy, gz)
				// for n, lbl := range oct.Labels {
				// 	fmt.Printf("Label %d: %d\n", n, lbl)
				// }
				// for n, idx := range oct.SBIndices {
				// 	fmt.Printf("Index %d: %d\n", n, idx)
				// }
			} else {
				fmt.Printf("Octant %d is nil\n", i)
			}
		}
		// fmt.Printf("Complete\n")
		if filled == 0 {
			return nil
		}
		if gx%2 != 0 || gy%2 != 0 || gz%2 != 0 {
			return fmt.Errorf("downres of octants only works for block-sizes with even # of sub-blocks: not %d x %d x %d", gx, gy, gz)
		}

		// Allocate temp buffers for lores block, which we need since we'll be visiting them out of order.
		numSubBlocks := gx * gy * gz
		var loNumLabels uint32
		loLabels := make(map[uint64]uint32, maxLabels)
		loSBIndices := make([][]uint32, numSubBlocks)
		loSBValues := make([][]byte, numSubBlocks)

		// Iterate through all octants, doing downres and filling in corresponding sub-blocks.
		octIndices := make([][]uint32, numSubBlocks)
		octValues := make([][]byte, numSubBlocks)
		var hires [8]sbData
		for i, oct := range octants {
			oz := int32(i) >> 2
			oy := (int32(i) - oz*4) >> 1
			ox := int32(i) % 2
			osbz := oz * gz >> 1 // octant sub-block offset
			osby := oy * gy >> 1
			osbx := ox * gx >> 1
			fmt.Printf("octant (%d,%d,%d)\n", ox, oy, oz)
			if oct == nil || len(oct.Labels) <= 1 {
				// blank out all sub-blocks for this octant.
				var indices []uint32
				var lbl uint64
				if oct != nil && len(oct.Labels) == 1 {
					lbl = oct.Labels[0]
				}
				_, found := loLabels[lbl]
				if !found {
					loLabels[lbl] = loNumLabels
					indices = []uint32{loNumLabels}
					loNumLabels++
				}
				var sbx, sby, sbz int32
				for sbz = 0; sbz < gz; sbz += 2 {
					for sby = 0; sby < gy; sby += 2 {
						for sbx = 0; sbx < gx; sbx += 2 {
							loSBNum := (osbz+(sbz>>1))*gx*gy + (osby+(sby>>1))*gx + osbx + (sbx >> 1)
							loSBIndices[loSBNum] = indices
							loSBValues[loSBNum] = nil
						}
					}
				}
			} else {
				// get the index and value slices for each sub-block.
				var indexPos, valuePos int
				for sb := int32(0); sb < numSubBlocks; sb++ {
					numLabels := int(oct.NumSBLabels[sb])
					valueBits := SubBlockSize * SubBlockSize * SubBlockSize * int(bitsFor(uint16(numLabels)))
					valueBytes := valueBits >> 3
					if valueBits%8 != 0 {
						valueBytes++
					}
					octIndices[sb] = oct.SBIndices[indexPos : indexPos+numLabels]
					octValues[sb] = oct.SBValues[valuePos : valuePos+valueBytes]
					indexPos += numLabels
					valuePos += valueBytes
					// fmt.Printf(" sub-block %d: %d labels, %d value bits, %d value bytes / indexpos %d, valuepos %d\n", sb, numLabels, valueBits, valueBytes, indexPos, valuePos)
				}

				// iterate through sub-blocks for each octant, down-res them, and integrate them into the low-res block.
				var x, y, z, sbx, sby, sbz int32
				for sbz = 0; sbz < gz; sbz += 2 {
					for sby = 0; sby < gy; sby += 2 {
						for sbx = 0; sbx < gx; sbx += 2 {
							var i int
							for z = 0; z < 2; z++ {
								for y = 0; y < 2; y++ {
									for x = 0; x < 2; x++ {
										sbNum := (sbz+z)*gx*gy + (sby+y)*gx + sbx + x
										hires[i].indices = octIndices[sbNum]
										hires[i].values = octValues[sbNum]
										i++
									}
								}
							}

							// fmt.Printf("sub-block(%d,%d,%d) hires: %v\n", sbx, sby, sbz, hires)
							lores, err := downresSubBlock(hires)
							if err != nil {
								return err
							}

							// set this low-res sub-block where we use index into lo-res block
							loSBNum := (osbz+(sbz>>1))*gx*gy + (osby+(sby>>1))*gx + osbx + (sbx >> 1)
							loSBIndices[loSBNum] = make([]uint32, len(lores.indices))
							loSBValues[loSBNum] = lores.values
							for i, idx := range lores.indices {
								lbl := oct.Labels[idx]
								bindex, found := loLabels[lbl]
								if !found {
									bindex = loNumLabels
									loLabels[lbl] = loNumLabels
									loNumLabels++
								}
								loSBIndices[loSBNum][i] = bindex
							}
						}
					}
				}
			}
		}

		// Reconstitute the Block using the temporary buffers for the lores block.
		return b.setData(uint32(gx), uint32(gy), uint32(gz), loLabels, loSBIndices, loSBValues)
	*/
}

func (b *Block) setData(gx, gy, gz uint32, sbLabels map[uint64]uint32, sbIndices [][]uint32, sbValues [][]byte) error {
	if len(sbIndices) != len(sbValues) {
		return fmt.Errorf("indices and values slices must have same # of sub-blocks: %d != %d", len(sbIndices), len(sbValues))
	}
	numLabels := uint32(len(sbLabels))
	numSubBlocks := gx * gy * gz

	var numSBIndices, sbIndexBytes, numSBValueBytes uint32
	if numLabels <= 1 {
		b.data = dvid.New8ByteAlignBytes(16 + numLabels*8)
	} else {
		for _, indices := range sbIndices {
			numSBIndices += uint32(len(indices))
		}
		sbIndexBytes = numSBIndices * 4
		for _, valueBytes := range sbValues {
			numSBValueBytes += uint32(len(valueBytes))
		}
		blockBytes := 16 + numLabels*8 + numSubBlocks*2 + sbIndexBytes + numSBValueBytes
		b.data = dvid.New8ByteAlignBytes(blockBytes)
	}

	binary.LittleEndian.PutUint32(b.data[0:4], gx)
	binary.LittleEndian.PutUint32(b.data[4:8], gy)
	binary.LittleEndian.PutUint32(b.data[8:12], gz)
	binary.LittleEndian.PutUint32(b.data[12:16], numLabels)

	pos := uint32(16)
	var err error
	b.Labels, err = dvid.ByteToUint64(b.data[pos : pos+numLabels*8])
	if err != nil {
		return err
	}
	for label, index := range sbLabels {
		b.Labels[index] = label
	}

	if numLabels <= 1 {
		return nil
	}

	if gx*gy*gz != uint32(len(sbIndices)) {
		return fmt.Errorf("number of sub-blocks from gx * gy * gz (%d) != sub-block indices (%d)", gx*gy*gz, len(sbIndices))
	}

	pos += numLabels * 8
	nbytes := numSubBlocks * 2
	b.NumSBLabels, err = dvid.ByteToUint16(b.data[pos : pos+nbytes])
	if err != nil {
		return err
	}
	pos += nbytes
	b.SBIndices, err = dvid.ByteToUint32(b.data[pos : pos+sbIndexBytes])
	if err != nil {
		return err
	}
	pos += sbIndexBytes
	b.SBValues = b.data[pos:]

	idxpos := 0
	valpos := 0
	for sbNum, indices := range sbIndices {
		numIndices := len(indices)
		b.NumSBLabels[sbNum] = uint16(numIndices)
		copy(b.SBIndices[idxpos:idxpos+numIndices], indices)
		idxpos += numIndices

		valbytes := len(sbValues[sbNum])
		copy(b.SBValues[valpos:valpos+valbytes], sbValues[sbNum])
		valpos += valbytes
	}
	return nil
}

// take eight 3d neighboring sub-blocks and make one 2x downres sub-block.
// allow missing sub-blocks.
func downresSubBlock(hires [8]sbData) (lores sbData, err error) {
	numSBVoxels := uint32(SubBlockSize * SubBlockSize * SubBlockSize)
	sbValues := make([]uint32, numSBVoxels*8)

	// first pass: write 8 indices for each down-res voxel
	for i := uint32(0); i < 8; i++ {
		if len(hires[i].indices) == 0 {
			continue
		}
		oz := i >> 2
		oy := (i - oz*4) >> 1
		ox := i % 2

		oz *= SubBlockSize >> 1
		oy *= SubBlockSize >> 1
		ox *= SubBlockSize >> 1

		var x, y, z, idx, bitpos uint32
		bits := bitsFor(uint16(len(hires[i].indices)))
		if bits == 0 {
			idx = hires[i].indices[0]
		}
		for z = 0; z < SubBlockSize; z++ {
			dz := (z>>1)*SubBlockSize*SubBlockSize + oz
			offz := (z % 2) >> 2
			for y = 0; y < SubBlockSize; y++ {
				dyz := dz + (y>>1)*SubBlockSize + oy
				offyz := offz + (y%2)>>1
				for x = 0; x < SubBlockSize; x++ {
					if bits > 0 {
						val := getPackedValue(hires[i].values, bitpos, bits)
						idx = hires[i].indices[val]
						bitpos += bits
					}

					dx := (x >> 1) + ox
					off := offyz + x%2
					v := (dyz+dx)*8 + off
					sbValues[v] = idx
				}
			}
		}
	}

	// second pass: find best index for each down-res voxel and store.
	var numIndices uint32
	sbIndices := make(map[uint32]uint32) // cache sub-block index and eventual down-res block index
	loresValues := make([]uint32, numSBVoxels)

	votemap := make(map[uint32]int, 8)
	for v := uint32(0); v < numSBVoxels; v++ {
		for i := uint32(0); i < 8; i++ {
			idx := sbValues[v*8+i]
			if idx != 0 {
				votemap[idx]++
			}
		}
		var winner uint32
		var winnerVotes int
		for idx, votes := range votemap {
			if winnerVotes < votes {
				winnerVotes = votes
				winner = idx
			}
			delete(votemap, idx)
		}

		index, found := sbIndices[winner]
		if !found {
			index = numIndices
			sbIndices[winner] = index
			numIndices++
		}
		loresValues[v] = index
	}

	// third pass: pack indices using minimal bits encoding.
	bits := bitsFor(uint16(numIndices))
	valueBits := numSBVoxels * bits
	valueBytes := valueBits >> 3
	if valueBits%8 != 0 {
		valueBytes++
	}
	// fmt.Printf("numIndices %d, bits %d, valueBits %d, valueBytes %d\n", numIndices, bits, valueBits, valueBytes)

	lores.indices = make([]uint32, numIndices)
	for idx, i := range sbIndices {
		lores.indices[i] = idx
	}
	if valueBytes > 0 {
		lores.values = make([]byte, valueBytes)
		var bitpos uint32
		for v := uint32(0); v < numSBVoxels; v++ {
			index := loresValues[v]
			bithead := bitpos % 8
			bytepos := bitpos >> 3
			if bithead+bits <= 8 {
				// index totally within this byte
				leftshift := uint(8 - bits - bithead)
				lores.values[bytepos] |= byte(index << leftshift)
			} else {
				// this straddles a byte boundary
				leftshift := uint(16 - bits - bithead)
				index <<= leftshift
				lores.values[bytepos] |= byte((index & 0xFF00) >> 8)
				lores.values[bytepos+1] = byte(index & 0x00FF)
			}
			bitpos += bits
		}
	}
	return
}

// Value returns the label for a voxel using its 3d location within block.  If the given
// location is outside the block extent, label 0 is returned.  Note that this function
// is inefficient for multi-voxel value retrieval.
func (b *Block) Value(pos dvid.Point3d) uint64 {
	if pos[0] < 0 || pos[0] >= b.Size[0] || pos[1] < 0 || pos[1] >= b.Size[1] || pos[2] < 0 || pos[2] >= b.Size[2] {
		return 0
	}
	if len(b.Labels) == 0 {
		return 0
	}
	if len(b.Labels) == 1 {
		return b.Labels[0]
	}
	sbz := pos[2] / SubBlockSize
	sby := pos[1] / SubBlockSize
	sbx := pos[0] / SubBlockSize
	gx, gy := b.Size[0]/SubBlockSize, b.Size[1]/SubBlockSize
	sbNum := sbz*gx*gy + sby*gx + sbx
	var bitPos uint32
	var idxPos int
	for sb := int32(0); sb < sbNum; sb++ {
		n := b.NumSBLabels[sb]
		idxPos += int(n)
		if n > 1 {
			bits := bitsFor(n)
			bitPos += SubBlockSize * SubBlockSize * SubBlockSize * bits
			if bitPos%8 != 0 {
				bitPos += 8 - (bitPos % 8)
			}
		}
	}
	n := b.NumSBLabels[sbNum]
	bits := bitsFor(n)
	if bits == 0 {
		idx := b.SBIndices[idxPos]
		return b.Labels[idx]
	}
	x, y, z := pos[0]%SubBlockSize, pos[1]%SubBlockSize, pos[2]%SubBlockSize
	bitPos += uint32(z*SubBlockSize*SubBlockSize+y*SubBlockSize+x) * bits
	val := getPackedValue(b.SBValues, bitPos, bits)
	index := b.SBIndices[idxPos+int(val)]
	return b.Labels[index]
}

// MakeLabelVolume returns a byte slice with packed little-endian uint64 labels in ZYX order,
// i.e., a uint64 for each voxel where consecutive values are in the (x,y,z) order:
// (0,0,0), (1,0,0), (2,0,0) ... (0,1,0)
// There is no sharing of memory between the returned byte slice and the Block data.
func (b Block) MakeLabelVolume() (uint64array []byte, size dvid.Point3d) {
	size = b.Size

	numVoxels := b.Size.Prod()
	uint64array = make([]byte, numVoxels*8)
	outarray, _ := dvid.ByteToUint64(uint64array)

	gx, gy, gz := b.Size[0]/SubBlockSize, b.Size[1]/SubBlockSize, b.Size[2]/SubBlockSize

	if len(b.Labels) < 2 {
		var label uint64
		if len(b.Labels) == 1 {
			label = b.Labels[0]
		}
		for i := int64(0); i < numVoxels; i++ {
			outarray[i] = label
		}
		return
	}

	subBlockNumVoxels := SubBlockSize * SubBlockSize * SubBlockSize
	sbLabels := make([]uint64, subBlockNumVoxels) // preallocate max # of labels for sub-block

	var indexPos, bitpos uint32
	var subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx++ {

				numSBLabels := b.NumSBLabels[subBlockNum]
				bits := bitsFor(numSBLabels)

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
								index := getPackedValue(b.SBValues, bitpos, bits)
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
	return
}

// MarshalBinary implements the encoding.BinaryMarshaler interface. Note that for
// efficiency, the returned byte slice will share memory with the receiver Block.
func (b Block) MarshalBinary() ([]byte, error) {
	return b.data, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.  The source
// byte slice is copied into a new 8-byte aligned slice so the receiver block does
// not depend on the passed slice.
func (b *Block) UnmarshalBinary(data []byte) error {
	if len(data) < 24 {
		return fmt.Errorf("can't unmarshal block binary of length %d", len(data))
	}
	numBytes := uint32(len(data))
	b.data = dvid.New8ByteAlignBytes(numBytes)
	copy(b.data, data)
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
	if numLabels == 0 {
		return fmt.Errorf("block has 0 labels, which is not allowed")
	}

	if gx > MaxSubBlockSize || gy > MaxSubBlockSize || gz > MaxSubBlockSize {
		return fmt.Errorf("%d x %d x %d sub-blocks exceed max dimension of %d voxels (%d sub-blocks)", gx, gy, gz, MaxBlockSize, MaxSubBlockSize)
	}
	if numLabels > MaxBlockSize*MaxBlockSize*MaxBlockSize {
		return fmt.Errorf("number of labels (%d) exceeds what can be contained in max block size %d", numLabels, MaxBlockSize)
	}

	b.Labels, err = dvid.ByteToUint64(b.data[16 : 16+numLabels*8])
	if err != nil {
		return
	}

	if len(b.Labels) <= 1 {
		b.NumSBLabels = nil
		b.SBIndices = nil
		b.SBValues = nil
		return
	}

	pos := uint32(16)
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

// OutputOp provides a way to communicate with writing goroutines,
// TODO: concurrency support on the given io.Writer.
type OutputOp struct {
	w          io.Writer
	pbCh       chan *PositionedBlock
	errCh      chan error
	sync.Mutex // lock on writing
}

func NewOutputOp(w io.Writer) *OutputOp {
	op := new(OutputOp)
	op.w = w
	op.pbCh = make(chan *PositionedBlock, 1000)
	op.errCh = make(chan error)
	return op
}

func (op OutputOp) Process(pb *PositionedBlock) {
	op.pbCh <- pb
}

// Finish signals all input to an OutputOp is done and waits for completion.
// Any error from the OutputOp is returned.
func (op OutputOp) Finish() error {
	close(op.pbCh)
	err := <-op.errCh
	return err
}

// WriteRLEs, like WriteBinaryBlocks, writes a compact serialization of a binarized Block to
// the supplied Writer.  In this case, the serialization uses little-endian encoded integers
// and RLEs with the repeating units of the following format:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//        int32   Length of run in X direction
//
// The offset is the DVID space offset to the first voxel in the Block.  After the RLEs have
// been written to the io.Writer, an error message is sent down the given errCh.
func WriteRLEs(lbls Set, op *OutputOp, bounds dvid.Bounds) {
	var rleBuf rleBuffer
	for pb := range op.pbCh {
		bcoord, err := pb.BCoord.ToChunkPoint3d()
		if err != nil {
			op.errCh <- err
			return
		}

		labelIndices := make(map[uint32]struct{})
		var inBlock bool
		for i, label := range pb.Labels {
			_, found := lbls[label]
			if found {
				labelIndices[uint32(i)] = struct{}{}
				inBlock = true
				// can't break here because there could be multiple entries for a label in a block
				// e.g., due to fast merge where a variety of previous labels gets set to merge label.
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
				rleBuf.flush(op.w)
				rleBuf.clear()
			}
		}
		if err := pb.writeRLEs(labelIndices, op, &rleBuf, bounds); err != nil {
			op.errCh <- err
			return
		}
		rleBuf.coord = bcoord
	}

	op.errCh <- rleBuf.flush(op.w)
}

func (pb *PositionedBlock) writeRLEs(indices map[uint32]struct{}, op *OutputOp, rleBuf *rleBuffer, bounds dvid.Bounds) error {
	offset, err := pb.OffsetDVID()
	if err != nil {
		return err
	}
	minPt := offset
	maxPt := dvid.Point3d{
		offset[0] + pb.Size[0] - 1,
		offset[1] + pb.Size[1] - 1,
		offset[2] + pb.Size[2] - 1,
	}
	if bounds.Exact && bounds.Voxel.IsSet() {
		bounds.Voxel.Adjust(&minPt, &maxPt)
	}

	if len(pb.Labels) == 1 {
		writeSolidBlockRLEs(minPt, maxPt, offset, op, rleBuf, bounds)
		return nil
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
		if sbBits%8 != 0 {
			sbBits += 8 - (sbBits % 8)
		}
		k += sbBits
	}

	curIndices := make([]uint32, subBlockNumVoxels) // preallocate max # of indices for sub-block

	// Keep track of the bit position in each sub-blocks values byte slice so we can easily
	// traverse the sub-blocks in block coordinates.
	for vz := minPt[2]; vz <= maxPt[2]; vz++ {
		z := vz - offset[2]
		blockz := vz % SubBlockSize
		dsz := (z / SubBlockSize) * gy * gx

		for vy := minPt[1]; vy <= maxPt[1]; vy++ {
			y := vy - offset[1]
			blocky := vy % SubBlockSize
			sbNumStart := dsz + (y/SubBlockSize)*gx
			yz := getImmutableYZ(vy, vz)
			rle, inRun := rleBuf.rles[yz]

			var sbNum int32 = -1
			var numSBLabels uint16
			var foreground, stepByVoxel bool
			var bitpos, bits uint32
			var dx int32
			vx := minPt[0]

			for {
				x := vx - offset[0]
				sbNumCur := sbNumStart + x/SubBlockSize
				if sbNum != sbNumCur {
					sbNum = sbNumCur
					numSBLabels = pb.NumSBLabels[sbNum]
					bits = bitsFor(numSBLabels)
					bitpos = sbValuePos[sbNum] + uint32(blockz*SubBlockSize*SubBlockSize+blocky*SubBlockSize+vx%SubBlockSize)*bits
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
						dx = 1
						stepByVoxel = true
					}
				}
				if stepByVoxel {
					index := getPackedValue(pb.SBValues, bitpos, bits)
					if multiForeground {
						_, foreground = indices[curIndices[index]]
					} else {
						foreground = (curIndices[index] == labelIndex)
					}
					bitpos += bits
				}
				if foreground {
					if inRun {
						rle.Extend(dx)
					} else {
						rle = dvid.NewRLE(dvid.Point3d{vx, vy, vz}, dx)
						inRun = true
					}
				} else if inRun {
					if _, err := rle.WriteTo(op.w); err != nil {
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

// writes RLEs for solid block that is known to be within selected labels.
func writeSolidBlockRLEs(minPt, maxPt, offset dvid.Point3d, op *OutputOp, rleBuf *rleBuffer, bounds dvid.Bounds) {
	for vz := minPt[2]; vz <= maxPt[2]; vz++ {
		for vy := minPt[1]; vy <= maxPt[1]; vy++ {
			yz := getImmutableYZ(vy, vz)
			rle, inRun := rleBuf.rles[yz]

			vx := minPt[0]
			dx := maxPt[0] - vx + 1
			if inRun {
				rle.Extend(dx)
			} else {
				rle = dvid.NewRLE(dvid.Point3d{vx, vy, vz}, dx)
			}
			rleBuf.rles[yz] = rle
		}
	}
}

type BinaryBlock struct {
	Offset dvid.Point3d // voxel offset for the first voxels in this block
	Size   dvid.Point3d
	Label  uint64
	Voxels []bool
}

func (b BinaryBlock) String() string {
	return fmt.Sprintf("[offset %s, size %s, label %d, voxel array of %d bytes", b.Offset, b.Size, b.Label, len(b.Voxels))
}

func (b *BinaryBlock) Read(r io.Reader, gx, gy, gz int32, label uint64) error {
	header := make([]byte, 13)
	if n, err := io.ReadFull(r, header); err != nil {
		return err
	} else if n != 13 {
		return fmt.Errorf("Unable to read header (only %d bytes) of block", n)
	}
	nx := gx * SubBlockSize
	ny := gy * SubBlockSize
	nz := gz * SubBlockSize
	ox := int32(binary.LittleEndian.Uint32(header[:4]))
	oy := int32(binary.LittleEndian.Uint32(header[4:8]))
	oz := int32(binary.LittleEndian.Uint32(header[8:12]))

	b.Offset = dvid.Point3d{ox, oy, oz}
	b.Size = dvid.Point3d{nx, ny, nz}
	b.Label = label
	b.Voxels = make([]bool, nx*ny*nz)
	switch header[12] {
	case 0:
		// do nothing because all background.
	case 1:
		// all foreground
		for i := 0; i < len(b.Voxels); i++ {
			b.Voxels[i] = true
		}
	case 2:
		// iterate through all sub-blocks and fill in volume.
		buf := make([]byte, 65)
		var vz, x, y, z int32
		var sx, sy, sz int32 // sub-block pos
		for sz = 0; sz < gz; sz++ {
			var vy int32
			for sy = 0; sy < gy; sy++ {
				vy = sy * SubBlockSize
				var vx int32
				for sx = 0; sx < gx; sx++ {
					if n, err := r.Read(buf[:1]); err != nil {
						return err
					} else if n != 1 {
						return fmt.Errorf("couldn't get content flag for sub-block (%d,%d,%d)", sx, sy, sz)
					}
					switch buf[0] {
					case 0:
						// do nothing, all background
					case 1:
						for z = 0; z < SubBlockSize; z++ {
							for y = 0; y < SubBlockSize; y++ {
								for x = 0; x < SubBlockSize; x++ {
									i := (vz+z)*nx*ny + (vy+y)*nx + vx + x
									b.Voxels[i] = true
								}
							}
						}
					case 2:
						if n, err := io.ReadFull(r, buf[1:]); err != nil {
							return err
						} else if n != 64 {
							return fmt.Errorf("couldn't get mask for sub-block (%d,%d,%d), %d bytes received", sx, sy, sz, n)
						}
						var bithead int32
						for z = 0; z < SubBlockSize; z++ {
							for y = 0; y < SubBlockSize; y++ {
								for x = 0; x < SubBlockSize; x++ {
									bytepos := bithead >> 3
									bitpos := bithead % 8
									val := buf[1+bytepos] & bitMask[bitpos]
									i := (vz+z)*nx*ny + (vy+y)*nx + vx + x
									b.Voxels[i] = (val > 0)
									bithead++
								}
							}
						}
					default:
						return fmt.Errorf("bad content flag (%d) for sub-block (%d,%d,%d)", buf[0], sx, sy, sz)
					}
					vx += SubBlockSize
				}
				vy += SubBlockSize
			}
			vz += SubBlockSize
		}

	default:
		return fmt.Errorf("got bad content flag (%d) in block", header[12])
	}
	return nil
}

// ReceiveBinaryBlocks returns a slice of BinaryBlock, easily parseable but not necessarily
// optimally compressed format.
func ReceiveBinaryBlocks(r io.Reader) ([]BinaryBlock, error) {
	header := make([]byte, 20)
	if n, err := io.ReadFull(r, header); err != nil {
		return nil, err
	} else if n != 20 {
		return nil, fmt.Errorf("Unable to read header (only %d bytes) of binary blocks serialization", n)
	}
	gx := int32(binary.LittleEndian.Uint32(header[:4]))
	gy := int32(binary.LittleEndian.Uint32(header[4:8]))
	gz := int32(binary.LittleEndian.Uint32(header[8:12]))
	label := binary.LittleEndian.Uint64(header[12:20])
	var blocks []BinaryBlock
	for {
		var block BinaryBlock
		err := block.Read(r, gx, gy, gz, label)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// WriteBinaryBlocks writes a compact serialization of a binarized Block to the
// supplied Writer.  The serialization is a header + stream of blocks.  The header
// is the following:
//
//   3 * uint32      values of gx, gy, and gz
//   uint64          foreground label
//
//  The format of each binary block in the stream is detailed by the WriteBinaryBlock() function.
//
func WriteBinaryBlocks(mainLabel uint64, lbls Set, op *OutputOp, bounds dvid.Bounds) {
	var blockWritten bool
	for pb := range op.pbCh {
		labelIndices := make(map[uint32]struct{})
		var inBlock, hasBackground bool
		for i, label := range pb.Labels {
			_, found := lbls[label]
			if found {
				labelIndices[uint32(i)] = struct{}{}
				inBlock = true
			} else {
				hasBackground = true // true if any non-targeted label exists
				if len(labelIndices) == len(lbls) {
					break
				}
			}
		}
		if inBlock {
			if !blockWritten {
				blockWritten = true
				gx := uint32(pb.Block.Size[0] / SubBlockSize)
				gy := uint32(pb.Block.Size[1] / SubBlockSize)
				gz := uint32(pb.Block.Size[2] / SubBlockSize)
				buf := make([]byte, 20)
				binary.LittleEndian.PutUint32(buf[0:4], gx)
				binary.LittleEndian.PutUint32(buf[4:8], gy)
				binary.LittleEndian.PutUint32(buf[8:12], gz)
				binary.LittleEndian.PutUint64(buf[12:20], mainLabel)
				if _, err := op.w.Write(buf); err != nil {
					op.errCh <- fmt.Errorf("Unable to write header for block %s: %v\n", pb.BCoord, err)
					return
				}
			}
			if err := pb.WriteBinaryBlock(labelIndices, hasBackground, op, bounds); err != nil {
				op.errCh <- err
				return
			}
		}
	}
	op.errCh <- nil
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
func (pb *PositionedBlock) WriteBinaryBlock(indices map[uint32]struct{}, hasBackground bool, op *OutputOp, bounds dvid.Bounds) error {
	offset, err := pb.OffsetDVID()
	if err != nil {
		return err
	}
	buf := make([]byte, 13)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(offset[0]))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(offset[1]))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(offset[2]))

	var mixedData bool
	if len(indices) == 0 {
		buf[12] = 0 // background only
	} else if hasBackground {
		buf[12] = 2 // background + foreground
		mixedData = true
	} else {
		buf[12] = 1 // foreground only
	}
	if n, err := op.w.Write(buf); err != nil {
		return err
	} else if n != 13 {
		return fmt.Errorf("couldn't write header for block %s, only %d bytes written", pb.BCoord, n)
	}
	if !mixedData {
		return nil
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

	subBlockNumVoxels := SubBlockSize * SubBlockSize * SubBlockSize
	curIndices := make([]uint32, subBlockNumVoxels) // preallocate max # of indices for sub-block

	data := make([]byte, 65) // sub-block data will at most be status byte + 64 bytes (8x8x8 bits).

	var indexPos, bitpos uint32
	var subBlockNum int
	var sx, sy, sz int32
	for sz = 0; sz < gz; sz++ {
		for sy = 0; sy < gy; sy++ {
			for sx = 0; sx < gx; sx, subBlockNum = sx+1, subBlockNum+1 {

				numSBLabels := pb.NumSBLabels[subBlockNum]
				bits := bitsFor(numSBLabels)

				for i := uint16(0); i < numSBLabels; i++ {
					curIndices[i] = pb.SBIndices[indexPos]
					indexPos++
				}

				switch numSBLabels {
				case 0:
					data[0] = 0
					if _, err := op.w.Write(data[:1]); err != nil {
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
					if _, err := op.w.Write(data[:1]); err != nil {
						return err
					}
					continue
				default:
				}

				outbytepos := int(1)
				outbitpos := int(8) // Start at 2nd byte for output

				var background bool // true if a non-index voxel is in block
				var foreground bool // true if index is in block

				var x, y, z int32
				for z = 0; z < SubBlockSize; z++ {
					for y = 0; y < SubBlockSize; y++ {
						for x = 0; x < SubBlockSize; x++ {
							index := getPackedValue(pb.SBValues, bitpos, bits)

							// write binary sub-block data
							var curForeground bool
							if multiForeground {
								_, curForeground = indices[curIndices[index]]
							} else if curIndices[index] == labelIndex {
								curForeground = true
							}
							if curForeground {
								data[outbytepos] |= bitMask[outbitpos%8]
								foreground = true
							} else {
								data[outbytepos] &^= bitMask[outbitpos%8]
								background = true
							}

							bitpos += bits
							outbitpos++
							if outbitpos%8 == 0 {
								outbytepos++
							}
						}
					}
				}

				if background && foreground {
					data[0] = 2
					if _, err := op.w.Write(data); err != nil {
						return err
					}
				} else if foreground {
					data[0] = 1
					if _, err := op.w.Write(data[:1]); err != nil {
						return err
					}
				} else {
					data[0] = 0
					if _, err := op.w.Write(data[:1]); err != nil {
						return err
					}
				}

				rem := bitpos % 8
				if rem != 0 {
					bitpos += 8 - rem
				}
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

// get block size of the subvolume
func (s subvolumeData) getBlockSize() dvid.Point3d {
	return dvid.Point3d{int32(s.blockSize[0]), int32(s.blockSize[1]), int32(s.blockSize[2])}
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

var bitMask [8]byte = [8]byte{
	0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80,
}

// map of label -> index position in sub-block
type subBlockIndex map[uint64]uint16

// iterate through the subvolume corresponding to the Block and do encoding
func (s *subvolumeData) encodeBlock() (*Block, error) {
	gx, gy, gz := s.getSubBlockDims()
	if gx > MaxSubBlockSize || gy > MaxSubBlockSize || gz > MaxSubBlockSize {
		return nil, fmt.Errorf("%d x %d x %d sub-blocks exceed max dimension of %d voxels (%d sub-blocks)", gx, gy, gz, MaxBlockSize, MaxSubBlockSize)
	}
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
	if numLabels == 1 {
		var label uint64
		for label = range labels {
			break
		}
		blockSize := dvid.Point3d{int32(s.blockSize[0]), int32(s.blockSize[1]), int32(s.blockSize[2])}
		return MakeSolidBlock(label, blockSize), nil
	}

	// Write all the data to the Block buffer.
	b := new(Block)
	subBlockIndexBytes := numSubBlockIndices * 4
	subBlockValueBytes := uint32(bitpos >> 3)
	blockBytes := 16 + numLabels*8 + numSubBlocks*2 + subBlockIndexBytes + subBlockValueBytes
	b.data = dvid.New8ByteAlignBytes(blockBytes)

	b.Size = s.getBlockSize()

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

// getPackedValue returns a 9 bit value from a packed array of values of "bits" bits
// starting from "bithead" bits into the given byte slice.  Values cannot straddle
// more than 2 bytes.
func getPackedValue(b []byte, bitHead, bits uint32) (index uint16) {
	bytePos := bitHead >> 3
	bitPos := bitHead % 8
	if bitPos+bits <= 8 {
		// index totally within this byte
		rightshift := uint(8 - bitPos - bits)
		index = uint16((b[bytePos] & leftBitMask[bitPos]) >> rightshift)
	} else {
		// index spans byte boundaries
		index = uint16(b[bytePos]&leftBitMask[bitPos]) << 8
		index |= uint16(b[bytePos+1])
		index >>= uint(16 - bitPos - bits)
	}
	return
}
