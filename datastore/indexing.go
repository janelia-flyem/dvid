/*
	This file supports spatial indexing of blocks using a variety of schemes.
*/

package datastore

import (
	_ "github.com/janelia-flyem/dvid/keyvalue"
)

const (
	// A block coord can be up to 2 million
	BlockCoordMaxBits  = 21
	BlockCoordMaxValue = (1 << BlockCoordMaxBits) - 1
	BlockCoordMask     = 0x001FFFFF

	// Number of bytes needed for 3 block coordinates
	BlockIndexBytes = 8
)

// blockIndex is a slice of bytes sufficient to encode up to BlockCoordMaxValue.
type blockIndex [BlockIndexBytes]byte

// BlockCoord is the (X,Y,Z) of a Block
type BlockCoord [3]int
