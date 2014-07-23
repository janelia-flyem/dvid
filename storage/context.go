/*
	This file contains types that manage valid key space within a DVID key-value database
	and support versioning.
*/

package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

const (
	metadataKeyPrefix byte = iota
	dataKeyPrefix
)

// ---- Context implementations -----

// MetadataContext is an implementation of Context for MetadataContext persistence.
type MetadataContext struct{}

func NewMetadataContext() MetadataContext {
	return MetadataContext{}
}

func (ctx MetadataContext) implementsOpaque() {}

func (ctx MetadataContext) ConstructKey(index dvid.Index) []byte {
	return append([]byte{metadataKeyPrefix}, index.Bytes()...)
}

func (ctx MetadataContext) String() string {
	return "Metadata Context"
}

func (ctx MetadataContext) Versioned() bool {
	return false
}

// DataContext supports both unversioned and versioned data persistence.
type DataContext struct {
	data    dvid.Data
	version dvid.VersionID
}

// NewDataContext provides a way for datatypes to create a Context that adheres to DVID
// key space partitioning.  Since Context and VersionedContext interfaces are opaque, i.e., can
// only be implemented within package storage, we force compatible implementations to embed
// DataContext and initialize it via this function.
func NewDataContext(data dvid.Data, version dvid.VersionID) *DataContext {
	return &DataContext{data, version}
}

// DataContextIndex returns the byte representation of a datatype-specific key (index)
// from a key constructed using a DataContext.
func DataContextIndex(b []byte) ([]byte, error) {
	if b[0] != dataKeyPrefix {
		return nil, fmt.Errorf("Cannot extract Index from key not constructed using DataContext")
	}
	start := 1 + dvid.InstanceIDSize
	end := len(b) - dvid.VersionIDSize
	return b[start:end], nil
}

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) ConstructKey(index dvid.Index) []byte {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, index.Bytes()...)
	return append(key, ctx.version.Bytes()...)
}

func (ctx *DataContext) String() string {
	return fmt.Sprintf("Data Context for %q (local id %d, version id %d)", ctx.data.DataName(),
		ctx.data.InstanceID(), ctx.version)
}

func (ctx *DataContext) Versioned() bool {
	return ctx.data.Versioned()
}

// Returns lower bound key for versions of given byte slice key representation.
func (ctx *DataContext) MinVersionKey(b []byte) ([]byte, error) {
	pos := len(b) - dvid.VersionIDSize
	minKey := make([]byte, pos)
	copy(minKey, b[0:pos])
	return append(minKey, dvid.VersionID(0).Bytes()...), nil
}

// Returns upper bound key for versions of given byte slice key representation.
func (ctx *DataContext) MaxVersionKey(b []byte) ([]byte, error) {
	pos := len(b) - dvid.VersionIDSize
	maxKey := make([]byte, pos)
	copy(maxKey, b[0:pos])
	return append(maxKey, dvid.VersionID(dvid.MaxVersionID).Bytes()...), nil
}

func (ctx *DataContext) VersionedKeyValue(values []KeyValue) (*KeyValue, error) {
	// This data needs to be Versioned or return an error.
	if !ctx.data.Versioned() {
		return nil, fmt.Errorf("Data instance %v is not versioned so can't do VersionedKeyValue()",
			ctx.data.DataName())
	}
	vdata, ok := ctx.data.(dvid.VersionedData)
	if !ok {
		return nil, fmt.Errorf("Data instance %v should have implemented GetIterator()",
			ctx.data.DataName())
	}

	// Set up a map[VersionID]KeyValue
	versionMap := make(map[dvid.VersionID]*KeyValue, len(values))
	for i, kv := range values {
		pos := len(kv.K) - dvid.VersionIDSize
		vid := dvid.VersionIDFromBytes(kv.K[pos:])
		versionMap[vid] = &(values[i])
	}

	// Iterate from the current node up the ancestors in the version DAG, checking if
	// current best is present.
	for it, err := vdata.GetIterator(ctx.version); err == nil && it.Valid(); it.Next() {
		if kv, found := versionMap[it.VersionID()]; found {
			return kv, nil
		}
	}
	return nil, nil
}
