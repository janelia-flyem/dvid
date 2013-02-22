/*
	This file supports spatial indexing of blocks using a variety of schemes.
*/

package datastore

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/janelia-flyem/dvid"
)

// SpatialIndex represents a n-dimensional coordinate according to some
// spatial indexing scheme.
type SpatialIndex string

// SpatialIndexScheme represents a particular spatial indexing scheme that
// hopefully maximizes sequential reads/writes when used as part of a key.
type SpatialIndexScheme byte

// Types of spatial indexing
const (
	// Simple indexing on Z, then X, then Y
	SIndexZXY SpatialIndexScheme = iota

	// Hilbert curve
	SIndexHilbert

	// Diagonal indexing within 2d plane, z separate
	SIndexXYDiagonal
)

// BlockCoord returns the coordinate of the block containing a given voxel coordinate.
// Since block sizes can vary among datastores, this function requires a datastore.Service.
func (s *Service) BlockCoord(vc dvid.VoxelCoord) (bc dvid.BlockCoord) {
	bc[0] = vc[0] / s.BlockMax[0]
	bc[1] = vc[1] / s.BlockMax[1]
	bc[2] = vc[2] / s.BlockMax[2]
	return
}

// BlockVoxel returns the voxel coordinate within a containing block for the given voxel 
// coordinate.
func (s *Service) BlockVoxel(vc dvid.VoxelCoord) (bc dvid.VoxelCoord) {
	bc[0] = vc[0] % s.BlockMax[0]
	bc[1] = vc[1] % s.BlockMax[1]
	bc[2] = vc[2] % s.BlockMax[2]
	return
}

// VoxelCoordToBlockIndex returns an index into a block's data that corresponds to
// a given voxel coordinate.  Note that the index is NOT multiplied by BytesPerVoxel
// but gives the element number, since we also want to set dirty flags.
func (s *Service) VoxelCoordToBlockIndex(vc dvid.VoxelCoord) (index int) {
	bv := s.BlockVoxel(vc)
	index = int(bv[2]*s.BlockMax[0]*s.BlockMax[1] + bv[1]*s.BlockMax[0] + bv[0])
	return
}

// SpatialIndex encodes a block coord into a spatial index
func (s *Service) SpatialIndex(coord dvid.BlockCoord) (si SpatialIndex) {
	buf := new(bytes.Buffer)
	switch s.Indexing {
	case SIndexZXY:
		for i := 2; i >= 0; i-- {
			err := binary.Write(buf, binary.LittleEndian, coord[i])
			if err != nil {
				log.Fatalf("SpatialIndex(%s): error in SIndexZXY encoding: %s\n", coord, err.Error())
			}
		}
		si = SpatialIndex(buf.String())
	default:
		log.Fatalf("SpatialIndex(%s): Unsupported spatial indexing scheme!\n", coord)
	}
	return
}

// OffsetToBlock returns the voxel coordinate at the top left corner of the block
// corresponding to the spatial index.
func (s *Service) OffsetToBlock(si SpatialIndex) (coord dvid.VoxelCoord) {
	blockCoord := si.BlockCoord(s)
	coord[0] = blockCoord[0] * s.BlockMax[0]
	coord[1] = blockCoord[1] * s.BlockMax[1]
	coord[2] = blockCoord[2] * s.BlockMax[2]
	return
}

// BlockCoord decodes a spatial index into a block coordinate
func (si SpatialIndex) BlockCoord(s *Service) (coord dvid.BlockCoord) {
	buf := bytes.NewBufferString(string(si))
	switch s.Indexing {
	case SIndexZXY:
		for i := 2; i >= 0; i-- {
			err := binary.Read(buf, binary.LittleEndian, &coord[i])
			if err != nil {
				log.Fatalln("BlockCoord(): error in SIndexZXY decoding: ", err.Error())
			}
		}
	default:
		log.Fatalf("BlockCoord(%d): Unsupported spatial indexing scheme!\n", s.Indexing)
	}
	return
}
