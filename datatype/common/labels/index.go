package labels

import (
	fmt "fmt"

	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
)

type Index struct {
	proto.LabelIndex
}

// NumVoxels returns the number of voxels in the given label set index.
func (idx Index) NumVoxels() uint64 {
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

func (idx Index) GetConstituentLabels() Set {
	if len(idx.Blocks) == 0 {
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

func (idx *Index) GetBlockIndices() dvid.IZYXSlice {
	if idx == nil || len(idx.Blocks) == 0 {
		return nil
	}
	blocks := make(dvid.IZYXSlice, len(idx.Blocks))
	i := 0
	for izyx := range idx.Blocks {
		blocks[i] = dvid.IZYXString(izyx)
		i++
	}
	return blocks
}

// GetProcessedBlockIndices returns the blocks for an index, possibly with bounds and down-res.
func (idx *Index) GetProcessedBlockIndices(scale uint8, bounds dvid.Bounds) (dvid.IZYXSlice, error) {
	indices := make(dvid.IZYXSlice, len(idx.Blocks))
	totBlocks := 0
	for s := range idx.Blocks {
		izyx := dvid.IZYXString(s)
		if bounds.Block.IsSet() {
			blockPt, err := izyx.ToChunkPoint3d()
			if err != nil {
				return nil, fmt.Errorf("error decoding block %v: %v", izyx, err)
			}
			if bounds.Block.Outside(blockPt) {
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
	if scale > 0 {
		return indices.Downres(scale)
	}
	return indices, nil
}

// FitToBounds modifies the receiver to fit the given optional block bounds.
func (idx *Index) FitToBounds(bounds *dvid.OptionalBounds) error {
	if bounds == nil {
		return nil
	}
	for s := range idx.Blocks {
		blockPt, err := dvid.IZYXString(s).ToChunkPoint3d()
		if err != nil {
			return fmt.Errorf("unable to convert IZYXString to chunk point: %v", err)
		}
		if bounds.BeyondZ(blockPt) {
			break
		}
		if bounds.Outside(blockPt) {
			continue
		}
		delete(idx.Blocks, s)
	}
	return nil
}

// Add adds the given Index to the receiver.
func (idx *Index) Add(idx2 *Index) error {
	if idx == nil || idx2 == nil {
		return fmt.Errorf("can't use Index.Add with nil Index")
	}
	if idx.Blocks == nil {
		idx.Blocks = idx2.Blocks
		return nil
	}
	for izyx, svc2 := range idx2.Blocks {
		svc, found := idx.Blocks[izyx]
		if !found || svc == nil || svc.Counts == nil {
			idx.Blocks[izyx] = svc2
		} else {
			// supervoxels cannot be in more than one set index, so if it's in idx2,
			// that supervoxel can't be in idx.
			for sv2, c2 := range svc2.Counts {
				svc.Counts[sv2] = c2
			}
		}
	}
	return nil
}

// Cleave the given supervoxels from an index and returns a new index, modifying both receiver
// and creating new cleaved index.
func (idx *Index) Cleave(toCleave []uint64) *Index {
	cleaveSet := NewSet(toCleave...)
	cidx := new(Index)
	cidx.Blocks = make(map[string]*proto.SVCount)

	for s, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			cleavedCounts := make(map[uint64]uint32)
			for supervoxel, sz := range svc.Counts {
				_, inCleave := cleaveSet[supervoxel]
				if inCleave {
					cleavedCounts[supervoxel] = sz
					delete(svc.Counts, supervoxel)
				}
			}
			cidx.Blocks[s] = &proto.SVCount{Counts: cleavedCounts}
		}
	}
	return cidx
}

// SupervoxelChanges tabulates changes in voxels among supervoxels across blocks.
type SupervoxelChanges map[uint64]map[dvid.IZYXString]int32

// ModifyBlocks modifies the receiver Index to incorporate supervoxel changes among the given blocks.
func (idx *Index) ModifyBlocks(sc SupervoxelChanges) error {
	labelSupervoxels := idx.GetConstituentLabels()
	for supervoxel, blockChanges := range sc {
		_, inSet := labelSupervoxels[supervoxel]
		if inSet {
			for izyxStr, delta := range blockChanges {
				svc, found := idx.Blocks[string(izyxStr)]
				if found && svc != nil {
					oldsz := svc.Counts[supervoxel]
					newsz := oldsz
					if delta < 0 && uint32(-delta) > oldsz {
						return fmt.Errorf("bad attempt to subtract %d from %d voxels for supervoxel %d in block %s", -delta, oldsz, supervoxel, izyxStr)
					}
					newsz = uint32(int64(oldsz) + int64(delta))
					svc.Counts[supervoxel] = newsz
				} else {
					svc = new(proto.SVCount)
					svc.Counts = make(map[uint64]uint32)
					if delta < 0 {
						return fmt.Errorf("bad attempt to subtract %d voxels from supervoxel %d in block %s when it wasn't previously in that block", -delta, supervoxel, izyxStr)
					}
					svc.Counts[supervoxel] = uint32(delta)
					idx.Blocks[string(izyxStr)] = svc
				}
			}
		}
	}
	return nil
}
