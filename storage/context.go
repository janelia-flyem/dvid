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

func (ctx MetadataContext) ConstructKey(b []byte) []byte {
	return append([]byte{metadataKeyPrefix}, b...)
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
	version VersionID
}

// NewDataContext provides a way for datatypes to create a Context that adheres to DVID
// key space partitioning.  Since Context and VersionedContext interfaces are opaque, i.e., can
// only be implemented within package storage, we force compatible implementations to embed
// DataContext and initialize it via this function.
func NewDataContext(data Data, version VersionID) *DataContext {
	return &DataKey{data, version}
}

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) ConstructKey(b []byte) []byte {
	key := append([]byte{dataKeyPrefix}, data.InstanceID().Bytes()...)
	key = append(key, b...)
	return append(key, data.VersionID().Bytes()...)
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
	k, err := key.BytesToKey(b)
	pos := len(b) - dvid.VersionIDSize
	maxKey := make([]byte, pos)
	copy(maxKey, b[0:pos])
	return append(maxKey, dvid.VersionID(dvid.MaxVersionID).Bytes()...), nil
}

func (ctx *DataContext) VersionedKeyValue(values []KeyValue) (*KeyValue, error) {
	// This data needs to be Versioned or return an error.
	if !ctx.data.Versioned() {
		return KeyValue{}, fmt.Errorf("Data instance %v is not versioned so can't do VersionedKeyValue()",
			ctx.data.DataName())
	}
	vdata, ok := ctx.data.(VersionedData)
	if !ok {
		return KeyValue{}, fmt.Errorf("Data instance %v should have implemented GetIterator()",
			ctx.data.DataName())
	}

	// Set up a map[VersionID]KeyValue
	versionMap := make(map[dvid.VersiondID]*KeyValue, len(values))
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
