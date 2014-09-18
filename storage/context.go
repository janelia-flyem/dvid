/*
	This file contains types that manage valid key space within a DVID key-value database
	and support versioning.
*/

package storage

import (
	"fmt"
	"sync"

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

	// IndexFromKey returns an index, the type-specific component of the key, from
	// an entire storage key.
	IndexFromKey(key []byte) (index []byte, err error)

	// String prints a description of the Context
	String() string

	// Returns a sync.Mutex specific to this context.
	Mutex() *sync.Mutex

	// Versioned is true if this Context is also a VersionedContext.
	Versioned() bool

	// Enforces opaque data type.
	implementsOpaque()
}

// VersionedContext extends a Context with the minimal functions necessary to handle
// versioning in storage engines.  For DataContext, only GetIterator() needs to be
// implemented at higher levels where the version DAG is available.
type VersionedContext interface {
	Context

	// GetIterator returns an iterator up a version DAG.
	GetIterator() (VersionIterator, error)

	// Returns lower bound key for versions of given byte slice index.
	MinVersionKey(index []byte) (key []byte, err error)

	// Returns upper bound key for versions of given byte slice index.
	MaxVersionKey(index []byte) (key []byte, err error)

	// VersionedKeyValue returns the key-value pair corresponding to this key's version
	// given a list of key-value pairs across many versions.  If no suitable key-value
	// pair is found, nil is returned.
	VersionedKeyValue([]*KeyValue) (*KeyValue, error)
}

// VersionIterator allows iteration through ancestors of version DAG.  It is assumed
// only one parent is needed based on how merge operations are handled.
type VersionIterator interface {
	Valid() bool
	VersionID() dvid.VersionID
	Next()
}

var contextMutexes map[mutexID]*sync.Mutex

func init() {
	contextMutexes = make(map[mutexID]*sync.Mutex)
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
	return 1 // Only one version of Metadata
}

func (ctx MetadataContext) ConstructKey(index []byte) []byte {
	return append([]byte{metadataKeyPrefix}, index...)
}

func (ctx MetadataContext) IndexFromKey(key []byte) ([]byte, error) {
	if key[0] != metadataKeyPrefix {
		return nil, fmt.Errorf("Cannot extract MetadataContext index from different key")
	}
	return key[1:], nil
}

var metadataMutex sync.Mutex

func (ctx MetadataContext) Mutex() *sync.Mutex {
	return &metadataMutex
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

// MinDataContextKeyRange returns the minimum and maximum key for data with a given local
// instance id.  Note that the returned keys are not type-specific indices but full DVID
// keys to be used with nil storage.Context.
func DataContextKeyRange(instanceID dvid.InstanceID) (minKey, maxKey []byte) {
	minKey = make([]byte, 1+dvid.InstanceIDSize)
	maxKey = make([]byte, 1+dvid.InstanceIDSize)
	minKey[0] = dataKeyPrefix
	maxKey[0] = dataKeyPrefix
	copy(minKey[1:], instanceID.Bytes())
	copy(maxKey[1:], (instanceID + 1).Bytes())
	return minKey, maxKey
}

// NewDataContext provides a way for datatypes to create a Context that adheres to DVID
// key space partitioning.  Since Context and VersionedContext interfaces are opaque, i.e., can
// only be implemented within package storage, we force compatible implementations to embed
// DataContext and initialize it via this function.
func NewDataContext(data dvid.Data, versionID dvid.VersionID) *DataContext {
	return &DataContext{data, versionID}
}

// KeyToLocalIDs parses a key under a DataContext and returns instance and version ids.
func KeyToLocalIDs(k []byte) (dvid.InstanceID, dvid.VersionID, error) {
	if k[0] != dataKeyPrefix {
		return 0, 0, fmt.Errorf("Cannot extract local IDs from a non-DataContext key")
	}
	instanceID := dvid.InstanceIDFromBytes(k[1 : 1+dvid.InstanceIDSize])
	end := len(k) - dvid.VersionIDSize
	versionID := dvid.VersionIDFromBytes(k[end:])
	return instanceID, versionID, nil
}

func UpdateDataContextKey(k []byte, instance dvid.InstanceID, version dvid.VersionID) error {
	if k[0] != dataKeyPrefix {
		return fmt.Errorf("Cannot update non-DataContext key")
	}
	copy(k[1:1+dvid.InstanceIDSize], instance.Bytes())
	end := len(k) - dvid.VersionIDSize
	copy(k[end:], version.Bytes())
	return nil
}

// ---- storage.Context implementation

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) VersionID() dvid.VersionID {
	return ctx.version
}

func (ctx *DataContext) ConstructKey(index []byte) []byte {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, index...)
	return append(key, ctx.version.Bytes()...)
}

func (ctx *DataContext) IndexFromKey(key []byte) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("Cannot extract DataContext index from nil key")
	}
	if key[0] != dataKeyPrefix {
		return nil, fmt.Errorf("Cannot extract DataContext index from different key")
	}
	start := 1 + dvid.InstanceIDSize
	end := len(key) - dvid.VersionIDSize
	return key[start:end], nil
}

type mutexID struct {
	instance dvid.InstanceID
	version  dvid.VersionID
}

var dataMutex sync.Mutex

func (ctx *DataContext) Mutex() *sync.Mutex {
	dataMutex.Lock()
	defer dataMutex.Unlock()

	id := mutexID{ctx.data.InstanceID(), ctx.version}
	mu, found := contextMutexes[id]
	if !found {
		mu = new(sync.Mutex)
		contextMutexes[id] = mu
	}
	return mu
}

func (ctx *DataContext) String() string {
	return fmt.Sprintf("Data Context for %q (local id %d, version id %d)", ctx.data.DataName(),
		ctx.data.InstanceID(), ctx.version)
}

func (ctx *DataContext) Versioned() bool {
	return ctx.data.Versioned()
}

// ----- partial storage.VersionedContext implementation

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
