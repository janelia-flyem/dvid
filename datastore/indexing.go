package datastore

import (
	_ "github.com/janelia-flyem/dvid/keyvalue"
)

const BlockCoordMaxBits = 21
const BlockCoordMaxValue = (1 << BlockCoordMaxBits) - 1
const BlockCoordMask = 0x001FFFFF

// Number of bytes needed for 3 block coordinates
const BlockIndexBytes = 8

// BlockIndex is a slice of bytes sufficient to encode up to BlockCoordMaxValue.
type blockIndex [BlockIndexBytes]byte

// BlockCoord is the (X,Y,Z) of a Block
type BlockCoord [3]int
