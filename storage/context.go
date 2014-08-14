/*
	This file contains types that manage valid key space within a DVID key-value database
	and support versioning.
*/

package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// Context allows encapsulation of data that defines the partitioning of the DVID
// key space.  To prevent conflicting implementations, Context is an opaque interface type
// that requires use of an implementation from the storage package, either directly or
// through embedding.  The storage engines should accept a nil Context, which allows
// direct saving of a raw key without use of a ConstructKey() transformation.
//
// For a description of Go language opaque types, see the following:
//   http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html
type Context interface {
	// VersionID returns the local version ID of the DAG node being operated on.
	// If not versioned, the version is the root ID.
	VersionID() dvid.VersionID

	// ConstructKey takes an index, a type-specific slice of bytes, and generates a
	// namespaced key that fits with the DVID-wide key space partitioning.
	ConstructKey(index []byte) []byte

	// String prints a description of the Context
	String() string

	// Versioned is true if this Context is also a VersionedContext.
	Versioned() bool

	// Enforces opaque data type.
	implementsOpaque()
}

// VersionedContext extends a Context with the minimal functions necessary to handle
// versioning in storage engines.
type VersionedContext interface {
	Context

	// Returns lower bound key for versions of given byte slice index.
	MinVersionKey(index []byte) (key []byte, err error)

	// Returns upper bound key for versions of given byte slice index.
	MaxVersionKey(index []byte) (key []byte, err error)

	// VersionedKeyValue returns the key-value pair corresponding to this key's version
	// given a list of key-value pairs across many versions.  If no suitable key-value
	// pair is found, nil is returned.
	VersionedKeyValue([]*KeyValue) (*KeyValue, error)
}

// ---- Context implementations -----

const (
	metadataKeyPrefix byte = iota
	dataKeyPrefix
)

// MetadataContext is an implementation of Context for MetadataContext persistence.
type MetadataContext struct{}

func NewMetadataContext() MetadataContext {
	return MetadataContext{}
}

func (ctx MetadataContext) implementsOpaque() {}

func (ctx MetadataContext) VersionID() dvid.VersionID {
	return 0 // Only one version of Metadata
}

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

// KeyToIndexZYX parses a DataContext key and returns the index as a dvid.IndexZYX
func KeyToIndexZYX(k []byte) (dvid.IndexZYX, error) {
	var zyx dvid.IndexZYX
	indexBytes, err := DataContextIndex(k)
	if err != nil {
		return zyx, fmt.Errorf("Cannot convert key %v to IndexZYX: %s\n", k, err.Error())
	}
	if err := zyx.IndexFromBytes(indexBytes); err != nil {
		return zyx, fmt.Errorf("Cannot recover ZYX index from key %v: %s\n", k, err.Error())
	}
	return zyx, nil
}

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) VersionID() dvid.VersionID {
	return ctx.version
}

func (ctx *DataContext) ConstructKey(index []byte) []byte {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, index...)
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
func (ctx *DataContext) MinVersionKey(index []byte) ([]byte, error) {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, index...)
	return append(key, dvid.VersionID(0).Bytes()...), nil
}

// Returns upper bound key for versions of given byte slice key representation.
func (ctx *DataContext) MaxVersionKey(index []byte) ([]byte, error) {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, index...)
	return append(key, dvid.VersionID(dvid.MaxVersionID).Bytes()...), nil
}

// VersionedKeyValue returns the key-value pair corresponding to this key's version
// given a list of key-value pairs across many versions.  If no suitable key-value
// pair is found, nil is returned.
func (ctx *DataContext) VersionedKeyValue(values []*KeyValue) (*KeyValue, error) {
	// This data needs to be Versioned or return an error.
	if !ctx.data.Versioned() {
		return nil, fmt.Errorf("Data instance %q is not versioned so can't do VersionedKeyValue()",
			ctx.data.DataName())
	}
	vdata, ok := ctx.data.(dvid.VersionedData)
	if !ok {
		return nil, fmt.Errorf("Expected versioned data instance %q: must implement GetIterator()",
			ctx.data.DataName())
	}

	// Set up a map[VersionID]KeyValue
	versionMap := make(map[dvid.VersionID]*KeyValue, len(values))
	for _, kv := range values {
		pos := len(kv.K) - dvid.VersionIDSize
		vid := dvid.VersionIDFromBytes(kv.K[pos:])
		versionMap[vid] = kv
	}

	// Iterate from the current node up the ancestors in the version DAG, checking if
	// current best is present.
	it, err := vdata.GetIterator(ctx.version)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get iterator from %s: %s\n", vdata.DataName(), err.Error())
	}
	for {
		if it.Valid() {
			if kv, found := versionMap[it.VersionID()]; found {
				return kv, nil
			}
		} else {
			break
		}
		it.Next()
	}
	return nil, nil
}
