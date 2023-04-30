// Functions that support point operations across DVID datatypes.

package labelmap

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

// GetLabelAtScaledPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtScaledPoint(v dvid.VersionID, pt dvid.Point, scale uint8, supervoxels bool) (uint64, error) {
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return 0, fmt.Errorf("Can't determine block of point %s", pt)
	}
	blockSize := d.BlockSize()
	bcoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d)

	labelData, err := d.getBlockLabels(v, bcoord, scale, supervoxels)
	if err != nil {
		return 0, err
	}
	if len(labelData) == 0 {
		return 0, nil
	}

	// Retrieve the particular label within the block.
	ptInBlock := coord.PointInChunk(blockSize)
	nx := int64(blockSize.Value(0))
	nxy := nx * int64(blockSize.Value(1))
	i := (int64(ptInBlock.Value(0)) + int64(ptInBlock.Value(1))*nx + int64(ptInBlock.Value(2))*nxy) * 8

	return binary.LittleEndian.Uint64(labelData[i : i+8]), nil
}

// The following functions implement an interface to synced data types like annotation.

// GetLabelBytes returns a block of hi-res (body) labels (scale 0) in packed little-endian uint64 format
func (d *Data) GetLabelBytes(v dvid.VersionID, bcoord dvid.ChunkPoint3d) ([]byte, error) {
	return d.getBlockLabels(v, bcoord, 0, false)
}

// GetLabelAtPoint returns the 64-bit unsigned int label for a given point.
func (d *Data) GetLabelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	return d.GetLabelAtScaledPoint(v, pt, 0, false)
}

// GetSupervoxelAtPoint returns the 64-bit unsigned int supervoxel id for a given point.
func (d *Data) GetSupervoxelAtPoint(v dvid.VersionID, pt dvid.Point) (uint64, error) {
	return d.GetLabelAtScaledPoint(v, pt, 0, true)
}

type ptsIndex struct {
	pts     []dvid.Point3d
	indices []int
}

type blockPtsI struct {
	*labels.Block
	ptsIndex
}

func (d *Data) partitionPoints(pts []dvid.Point3d) map[dvid.IZYXString]ptsIndex {
	blockSize := d.BlockSize().(dvid.Point3d)
	blockPts := make(map[dvid.IZYXString]ptsIndex)
	for i, pt := range pts {
		x := pt[0] / blockSize[0]
		y := pt[1] / blockSize[1]
		z := pt[2] / blockSize[2]
		bx := pt[0] % blockSize[0]
		by := pt[1] % blockSize[1]
		bz := pt[2] % blockSize[2]
		bpt := dvid.Point3d{bx, by, bz}
		bcoord := dvid.ChunkPoint3d{x, y, z}.ToIZYXString()
		ptsi, found := blockPts[bcoord]
		if found {
			ptsi.pts = append(ptsi.pts, bpt)
			ptsi.indices = append(ptsi.indices, i)
		} else {
			ptsi.pts = []dvid.Point3d{bpt}
			ptsi.indices = []int{i}
		}
		blockPts[bcoord] = ptsi
	}
	return blockPts
}

// GetLabelPoints returns labels or supervoxels corresponding to given 3d points.
func (d *Data) GetLabelPoints(v dvid.VersionID, pts []dvid.Point3d, scale uint8, useSupervoxels bool) (mapped []uint64, err error) {
	if len(pts) == 0 {
		return
	}
	timedLog := dvid.NewTimeLog()

	mapped = make([]uint64, len(pts))
	blockPts := d.partitionPoints(pts)

	// Get mapping.
	var mapping *VCache
	var mappedVersions distFromRoot
	if !useSupervoxels {
		if mapping, err = getMapping(d, v); err != nil {
			return nil, err
		}
		if mapping != nil {
			mappedVersions = mapping.getMappedVersionsDist(v)
			if err != nil {
				err = fmt.Errorf("unable to get ancestry for version %d: %v", v, err)
				return
			}
		}
	}

	// Iterate through blocks and get labels without inflating blocks and concurrently process data.
	var wg sync.WaitGroup
	var labelsMu sync.Mutex
	concurrency := len(blockPts) / 10
	if concurrency < 1 {
		concurrency = 1
	}
	ch := make(chan blockPtsI, len(blockPts))
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			for bptsI := range ch {
				if len(mappedVersions) > 0 {
					mapping.ApplyMappingToBlock(mappedVersions, bptsI.Block)
				}
				blockLabels := bptsI.Block.GetPointLabels(bptsI.pts)
				labelsMu.Lock()
				for i, index := range bptsI.indices {
					mapped[index] = blockLabels[i]
				}
				labelsMu.Unlock()
			}
			wg.Done()
		}()
	}
	for bcoord, ptsi := range blockPts {
		chunkPt3d, err := bcoord.ToChunkPoint3d()
		if err != nil {
			close(ch)
			return nil, err
		}
		block, err := d.getSupervoxelBlock(v, chunkPt3d, scale)
		if err != nil {
			close(ch)
			return nil, err
		}
		ch <- blockPtsI{block, ptsi}
	}
	close(ch)
	wg.Wait()

	if len(blockPts) > 10 {
		timedLog.Infof("Larger label query for annotation %q at %d points -> %d blocks with %d goroutines", d.DataName(), len(pts), len(blockPts), concurrency)
	}
	return
}

// GetPointsInSupervoxels returns the 3d points that fall within given supervoxels that are
// assumed to be mapped to one label.  If supervoxels are assigned to more than one label,
// or a mapping is not available, an error is returned.  The label index is not used so
// this function will use immutable data underneath if there are no supervoxel splits.
func (d *Data) GetPointsInSupervoxels(v dvid.VersionID, pts []dvid.Point3d, supervoxels []uint64) (inSupervoxels []bool, err error) {
	inSupervoxels = make([]bool, len(pts))
	if len(pts) == 0 || len(supervoxels) == 0 {
		return
	}
	timedLog := dvid.NewTimeLog()

	supervoxelSet := make(labels.Set)
	for _, supervoxel := range supervoxels {
		supervoxelSet[supervoxel] = struct{}{}
	}
	ptsByBlock := d.partitionPoints(pts)

	// Launch the block processing goroutines
	var wg sync.WaitGroup
	concurrency := len(ptsByBlock) / 10
	if concurrency < 1 {
		concurrency = 1
	}
	ch := make(chan blockPtsI, len(ptsByBlock))
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			for bptsI := range ch {
				blockLabels := bptsI.Block.GetPointLabels(bptsI.pts)
				for i, index := range bptsI.indices {
					supervoxel := blockLabels[i]
					if _, found := supervoxelSet[supervoxel]; found {
						inSupervoxels[index] = true
					}
				}
			}
			wg.Done()
		}()
	}

	// Send the blocks spanning the given supervoxels.
	var block *labels.Block
	for bcoord, bptsI := range ptsByBlock {
		var chunkPt3d dvid.ChunkPoint3d
		if chunkPt3d, err = bcoord.ToChunkPoint3d(); err != nil {
			close(ch)
			return
		}
		if block, err = d.getSupervoxelBlock(v, chunkPt3d, 0); err != nil {
			close(ch)
			return nil, err
		}
		ch <- blockPtsI{block, bptsI}
	}
	close(ch)
	wg.Wait()

	timedLog.Infof("Annotation %q point check for %d supervoxels, %d points -> %d blocks (%d goroutines)", d.DataName(), len(supervoxels), len(pts), len(ptsByBlock), concurrency)
	return
}
