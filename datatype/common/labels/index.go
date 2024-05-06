package labels

import (
	fmt "fmt"
	"sort"

	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
)

type Index struct {
	proto.LabelIndex
}

// EncodeBlockIndex converts signed (x,y,z) block coordinate into
// a single uint64, which is packed in ZYX order with MSB empty,
// the most-significant 21 bits is Z (21st bit is sign flag), next
// 21 bits is Y, then least-significant 21 bits is X.
func EncodeBlockIndex(x, y, z int32) (zyx uint64) {
	if z < 0 {
		zyx |= 0x00100000
		z = -z
	}
	zyx |= uint64(z & 0x000FFFFF)
	zyx <<= 21
	if y < 0 {
		zyx |= 0x00100000
		y = -y
	}
	zyx |= uint64(y & 0x000FFFFF)
	zyx <<= 21
	if x < 0 {
		zyx |= 0x00100000
		x = -x
	}
	zyx |= uint64(x & 0x000FFFFF)
	return
}

// IZYXStringToBlockIndex returns an encoded Block Index for a given IZYXString,
// returning an error if the IZYXString is formatted incorrectly.
func IZYXStringToBlockIndex(s dvid.IZYXString) (zyx uint64, err error) {
	var blockPt dvid.ChunkPoint3d
	blockPt, err = s.ToChunkPoint3d()
	if err != nil {
		return
	}
	return EncodeBlockIndex(blockPt[0], blockPt[1], blockPt[2]), nil
}

// DecodeBlockIndex decodes a packed block index into int32 coordinates.
// At most, each block int32 coordinate can be 20 bits.
func DecodeBlockIndex(zyx uint64) (x, y, z int32) {
	x = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		x = -x
	}
	zyx >>= 21
	y = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		y = -y
	}
	zyx >>= 21
	z = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		z = -z
	}
	return
}

// BlockIndexToIZYXString decodes a packed block index into an IZYXString.
// At most, each block int32 coordinate can be 20 bits.
func BlockIndexToIZYXString(zyx uint64) dvid.IZYXString {
	var x, y, z int32
	x = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		x = -x
	}
	zyx >>= 21
	y = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		y = -y
	}
	zyx >>= 21
	z = int32(zyx & 0x00000000000FFFFF)
	if zyx&0x0000000000100000 != 0 {
		z = -z
	}
	return dvid.ChunkPoint3d{x, y, z}.ToIZYXString()
}

// Equal returns true if the receiver and passed Index are equivalent.
func (idx Index) Equal(idx2 Index) bool {
	if idx.Label != idx2.Label {
		return false
	}
	if idx.LastMutId != idx2.LastMutId {
		return false
	}
	if idx.LastModTime != idx2.LastModTime {
		return false
	}
	if idx.LastModUser != idx2.LastModUser {
		return false
	}
	if idx.LastModApp != idx2.LastModApp {
		return false
	}
	if len(idx.Blocks) != len(idx2.Blocks) {
		return false
	}
	if len(idx.Blocks) > 0 {
		for block, svc := range idx.Blocks {
			svc2, found := idx2.Blocks[block]
			if !found {
				return false
			}
			if (svc == nil && svc2 != nil) || (svc != nil && svc2 == nil) {
				return false
			}
			if svc != nil {
				if len(svc.Counts) != len(svc2.Counts) {
					return false
				}
				for label, count := range svc.Counts {
					count2, found := svc2.Counts[label]
					if !found {
						return false
					}
					if count != count2 {
						return false
					}
				}
			}
		}
	}
	return true
}

// StringDump returns a description of the data within the Index.
// If showMutationInfo is true, the mutation ID and information about
// modification is also printed.
func (idx Index) StringDump(showMutationInfo bool) string {
	s := fmt.Sprintf("\nLabel: %d\n", idx.Label)
	if showMutationInfo {
		s += fmt.Sprintf("Last Mutation ID: %d\n", idx.LastMutId)
		s += fmt.Sprintf("Last Modification Time: %s\n", idx.LastModTime)
		s += fmt.Sprintf("Last Modification User: %s\n", idx.LastModUser)
		s += fmt.Sprintf("Last Modification App:  %s\n\n", idx.LastModApp)
	}

	s += fmt.Sprintf("Total blocks: %d\n", len(idx.Blocks))
	for zyx, svc := range idx.Blocks {
		izyxStr := BlockIndexToIZYXString(zyx)
		s += fmt.Sprintf("Block %s:\n", izyxStr)
		for sv, count := range svc.Counts {
			s += fmt.Sprintf("  Supervoxel %10d: %d voxels\n", sv, count)
		}
		s += fmt.Sprintf("\n")
	}
	return s
}

// NumVoxels returns the number of voxels for the Index.
func (idx Index) NumVoxels() uint64 {
	if len(idx.Blocks) == 0 {
		return 0
	}
	var numVoxels uint64
	for _, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			for _, sz := range svc.Counts {
				numVoxels += uint64(sz)
			}
		}
	}
	return numVoxels
}

// SupervoxelsPresent checks whether each label from a Set are within the index.
func (idx *Index) SupervoxelsPresent(supervoxels Set) (present map[uint64]bool) {
	present = make(map[uint64]bool, len(supervoxels))
	toCheck := make(Set, len(supervoxels))
	for supervoxel := range supervoxels {
		present[supervoxel] = false
		toCheck[supervoxel] = struct{}{}
	}
	if idx == nil || len(idx.Blocks) == 0 || len(toCheck) == 0 {
		return
	}
	for _, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			for sv := range svc.Counts {
				if _, found := toCheck[sv]; found {
					present[sv] = true
					delete(toCheck, sv)
					if len(toCheck) == 0 {
						return
					}
				}
			}
		}
	}
	return
}

// GetSupervoxels returns a set of supervoxels within the receiver Index.
func (idx *Index) GetSupervoxels() Set {
	if idx == nil || len(idx.Blocks) == 0 {
		return Set{}
	}
	lbls := make(Set, 2*len(idx.Blocks)) // guess 2 supervoxel per block
	for _, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			for sv := range svc.Counts {
				lbls[sv] = struct{}{}
			}
		}
	}
	return lbls
}

// GetBlockIndices returns the block coordinates within the Index.
func (idx *Index) GetBlockIndices() dvid.IZYXSlice {
	if idx == nil || len(idx.Blocks) == 0 {
		return nil
	}
	blocks := make(dvid.IZYXSlice, len(idx.Blocks))
	i := 0
	for zyx := range idx.Blocks {
		blocks[i] = BlockIndexToIZYXString(zyx)
		i++
	}
	return blocks
}

// GetSupervoxelCount returns the # of voxels for a supervoxel in an Index.
// Note that the counts are uint64 because although each block might only hold
// a # of voxels < max uint32, a massive supervoxel could hold many more.
func (idx *Index) GetSupervoxelCount(supervoxel uint64) (count uint64) {
	if idx == nil || len(idx.Blocks) == 0 {
		return
	}
	for _, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			count += uint64(svc.Counts[supervoxel])
		}
	}
	return
}

// GetSupervoxelCounts returns the # of voxels for each supervoxel in an Index.
// Note that the counts are uint64 because although each block might only hold
// a # of voxels < max uint32, a massive supervoxel could hold many more.
func (idx *Index) GetSupervoxelCounts() (counts map[uint64]uint64) {
	counts = make(map[uint64]uint64)
	if idx == nil || len(idx.Blocks) == 0 {
		return
	}
	for _, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			for supervoxel, sz := range svc.Counts {
				counts[supervoxel] += uint64(sz)
			}
		}
	}
	return
}

// LimitToSupervoxel returns a copy of the index but with only the given supervoxel
func (idx *Index) LimitToSupervoxel(supervoxel uint64) (*Index, error) {
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, nil
	}
	sidx := new(Index)
	sidx.Label = idx.Label
	sidx.LastMutId = idx.LastMutId
	sidx.LastModTime = idx.LastModTime
	sidx.LastModUser = idx.LastModUser
	sidx.Blocks = make(map[uint64]*proto.SVCount)
	for zyx, svc := range idx.Blocks {
		if svc != nil && len(svc.Counts) != 0 {
			count, found := svc.Counts[supervoxel]
			if found {
				if count == 0 {
					dvid.Debugf("ignoring block %s for supervoxel %d because zero count\n", BlockIndexToIZYXString(zyx), supervoxel)
					continue
				}
				sidx.Blocks[zyx] = &proto.SVCount{Counts: map[uint64]uint32{supervoxel: count}}
			}
		}
	}
	if len(sidx.Blocks) == 0 {
		return nil, nil
	}
	return sidx, nil
}

// GetProcessedBlockIndices returns the blocks for an index, possibly bounded in and with
// down-res applied by the given scale.  If supervoxel is 0, assumes that all blocks
// should be returned after the other restrictions, otherwise it screens for only
// blocks that contain the given supervoxel id.
func (idx *Index) GetProcessedBlockIndices(scale uint8, bounds dvid.Bounds, supervoxel uint64) (dvid.IZYXSlice, error) {
	if idx == nil {
		return nil, nil
	}

	// Get all blocks in index, skipping any that are empty.
	indices := make(dvid.IZYXSlice, len(idx.Blocks))
	totBlocks := 0
	for zyx := range idx.Blocks {
		izyx := BlockIndexToIZYXString(zyx)
		svc := idx.Blocks[zyx]
		if svc == nil || svc.Counts == nil {
			dvid.Debugf("ignoring block %s for label %d because of nil Counts\n", izyx, idx.Label)
			continue
		}
		if supervoxel != 0 {
			count, found := svc.Counts[supervoxel]
			if !found || count == 0 {
				continue // filter any blocks not containing given supervoxel id
			}
		} else {
			var ok bool
			for _, count := range svc.Counts {
				if count > 0 {
					ok = true
					break
				}
			}
			if !ok {
				dvid.Debugf("ignoring block %s for label %d because all counts are zero: %v\n", izyx, idx.Label, svc.Counts)
				continue
			}
		}
		indices[totBlocks] = izyx
		totBlocks++
	}
	if totBlocks == 0 {
		return nil, nil
	}
	indices = indices[:totBlocks]

	// Downres if requested.
	if scale > 0 {
		var err error
		if indices, err = indices.Downres(scale); err != nil {
			return nil, err
		}
	}

	// Apply bounds if given.
	if bounds.Block.IsSet() {
		if scale == 0 {
			sort.Sort(indices)
		}
		return indices.FitToBounds(bounds.Block)
	}
	return indices, nil
}

// GetSupervoxelsBlocks returns the blocks for a given list of supervoxels.
func (idx *Index) GetSupervoxelsBlocks(supervoxels Set) map[dvid.IZYXString]struct{} {
	if idx == nil {
		return nil
	}
	blockMap := make(map[dvid.IZYXString]struct{})
	for zyx := range idx.Blocks {
		izyx := BlockIndexToIZYXString(zyx)
		svc := idx.Blocks[zyx]
		if svc == nil || svc.Counts == nil {
			continue
		}
		for supervoxel, count := range svc.Counts {
			if _, found := supervoxels[supervoxel]; found && count > 0 {
				blockMap[izyx] = struct{}{}
				break
			}
		}
	}
	return blockMap
}

// FitToBounds modifies the receiver to fit the given optional block bounds.
func (idx *Index) FitToBounds(bounds *dvid.OptionalBounds) error {
	if bounds == nil {
		return nil
	}
	for zyx := range idx.Blocks {
		x, y, z := DecodeBlockIndex(zyx)
		blockPt := dvid.ChunkPoint3d{x, y, z}
		if bounds.BeyondZ(blockPt) {
			break
		}
		if bounds.Outside(blockPt) {
			continue
		}
		delete(idx.Blocks, zyx)
	}
	return nil
}

// Add adds the given Index to the receiver. Any blocks modified will result in
// the surface_mutid being set since the surface in the block would have changed.
func (idx *Index) Add(idx2 *Index, mutInfo dvid.MutInfo) error {
	if idx == nil {
		return fmt.Errorf("can't use Index.Add with nil receiver Index")
	}
	if idx2 == nil || len(idx2.Blocks) == 0 {
		return nil
	}
	if idx.Blocks == nil {
		idx.Blocks = idx2.Blocks
		return nil
	}
	for zyx, svc2 := range idx2.Blocks {
		svc, found := idx.Blocks[zyx]
		if !found || svc == nil || svc.Counts == nil {
			idx.Blocks[zyx] = svc2
		} else {
			// supervoxels cannot be in more than one set index, so if it's in idx2,
			// that supervoxel can't be in idx.
			for sv2, c2 := range svc2.Counts {
				svc.Counts[sv2] = c2
			}
			svc.SurfaceMutid = mutInfo.MutID
		}
	}
	idx.LastMutId = mutInfo.MutID
	idx.LastModTime = mutInfo.Time
	idx.LastModUser = mutInfo.User
	idx.LastModApp = mutInfo.App
	return nil
}

// Cleave the given supervoxels from an index and returns a new index, modifying both receiver
// and creating new cleaved index.
func (idx *Index) Cleave(cleaveLabel uint64, toCleave []uint64, mutInfo dvid.MutInfo) (cleavedSize, remainSize uint64, cidx *Index) {
	idx.LastMutId = mutInfo.MutID
	idx.LastModUser = mutInfo.User
	idx.LastModTime = mutInfo.Time
	idx.LastModApp = mutInfo.App

	cleaveSet := NewSet(toCleave...)
	cidx = new(Index)
	cidx.Label = cleaveLabel
	cidx.Blocks = make(map[uint64]*proto.SVCount)
	cidx.LastMutId = mutInfo.MutID
	cidx.LastModUser = mutInfo.User
	cidx.LastModTime = mutInfo.Time
	cidx.LastModApp = mutInfo.App

	for zyx, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			cleavedCounts := make(map[uint64]uint32)
			for supervoxel, sz := range svc.Counts {
				_, inCleave := cleaveSet[supervoxel]
				if inCleave {
					cleavedSize += uint64(sz)
					cleavedCounts[supervoxel] = sz
					delete(svc.Counts, supervoxel)
					svc.SurfaceMutid = mutInfo.MutID
				} else {
					remainSize += uint64(sz)
				}
			}
			if len(cleavedCounts) > 0 {
				cidx.Blocks[zyx] = &proto.SVCount{Counts: cleavedCounts, SurfaceMutid: mutInfo.MutID}
			}
		}
	}
	for zyx, svc := range idx.Blocks {
		if svc == nil || len(svc.Counts) == 0 {
			delete(idx.Blocks, zyx)
		}
	}
	return
}

// SupervoxelChanges tabulates changes in voxels among supervoxels across blocks.
type SupervoxelChanges map[uint64]map[dvid.IZYXString]int32

// ModifyBlocks modifies the receiver Index to incorporate supervoxel changes among the given blocks.
func (idx *Index) ModifyBlocks(label uint64, sc SupervoxelChanges) error {
	if idx == nil {
		return fmt.Errorf("cannot pass nil index into ModifyBlocks()")
	}
	if idx.Blocks == nil {
		idx.Blocks = make(map[uint64]*proto.SVCount)
	}
	labelSupervoxels := idx.GetSupervoxels()
	if len(labelSupervoxels) == 0 {
		labelSupervoxels[label] = struct{}{} // A new index has at least its original label
	}
	for supervoxel, blockChanges := range sc {
		_, inSet := labelSupervoxels[supervoxel]
		if inSet {
			for izyxStr, delta := range blockChanges {
				zyx, err := IZYXStringToBlockIndex(izyxStr)
				if err != nil {
					return err
				}
				svc, found := idx.Blocks[zyx]
				if found && svc != nil {
					oldsz := svc.Counts[supervoxel]
					newsz := oldsz
					if delta < 0 && uint32(-delta) > oldsz {
						return fmt.Errorf("bad attempt to subtract %d from %d voxels for supervoxel %d in block %s", -delta, oldsz, supervoxel, izyxStr)
					}
					newsz = uint32(int64(oldsz) + int64(delta))
					if newsz == 0 {
						delete(svc.Counts, supervoxel)
					} else {
						svc.Counts[supervoxel] = newsz
					}
				} else {
					svc = new(proto.SVCount)
					svc.Counts = make(map[uint64]uint32)
					if delta < 0 {
						return fmt.Errorf("bad attempt to subtract %d voxels from supervoxel %d in block %s when it wasn't previously in that block", -delta, supervoxel, izyxStr)
					}
					svc.Counts[supervoxel] = uint32(delta)
					idx.Blocks[zyx] = svc
				}
			}
		}
	}
	// if blocks no longer have any supervoxels, delete them.
	for zyx, svc := range idx.Blocks {
		if svc == nil || len(svc.Counts) == 0 {
			delete(idx.Blocks, zyx)
		}
	}
	return nil
}
