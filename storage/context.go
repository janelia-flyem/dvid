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
// through embedding.
//
// For a description of Go language opaque types, see the following:
//   http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html
type Context interface {
	// VersionID returns the local version ID of the DAG node being operated on.
	// If not versioned, the version is the root ID.
	VersionID() dvid.VersionID

	// ConstructKey takes a type-specific key component, and generates a
	// namespaced key that fits with the DVID-wide key space partitioning.
	ConstructKey(TKey) Key

	// TKeyFromKey returns the type-specific component of the key.
	TKeyFromKey(Key) (TKey, error)

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

	// TombstoneKey takes a type-specific key component and returns a key that
	// signals a deletion of any ancestor values.
	TombstoneKey(TKey) Key

	// IsTombstoneKey is true if the given storage key is a tombstone key.
	IsTombstoneKey(Key) bool

	// GetIterator returns an iterator up a version DAG.
	GetIterator() (VersionIterator, error)

	// Returns lower bound key for versions.
	MinVersionKey(TKey) (Key, error)

	// Returns upper bound key for versions.
	MaxVersionKey(TKey) (Key, error)

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

func (ctx MetadataContext) ConstructKey(tk TKey) Key {
	return Key(append([]byte{metadataKeyPrefix}, tk...))
}

func (ctx MetadataContext) TKeyFromKey(key Key) (TKey, error) {
	if key[0] != metadataKeyPrefix {
		return nil, fmt.Errorf("Cannot extract MetadataContext index from different key")
	}
	return TKey(key[1:]), nil
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

// KeyToLocalIDs parses a key under a DataContext and returns instance, version and client ids.
func DataKeyToLocalIDs(k Key) (dvid.InstanceID, dvid.VersionID, dvid.ClientID, error) {
	if k[0] != dataKeyPrefix {
		return 0, 0, 0, fmt.Errorf("Cannot extract local IDs from a non-DataContext key")
	}
	instanceID := dvid.InstanceIDFromBytes(k[1 : 1+dvid.InstanceIDSize])
	start := len(k) - dvid.VersionIDSize - dvid.ClientIDSize - 1
	versionID := dvid.VersionIDFromBytes(k[start : start+dvid.VersionIDSize])
	start += dvid.VersionIDSize
	clientID := dvid.ClientIDFromBytes(k[start : start+dvid.ClientIDSize])
	return instanceID, versionID, clientID, nil
}

func UpdateDataKey(k Key, instance dvid.InstanceID, version dvid.VersionID, client dvid.ClientID) error {
	if k[0] != dataKeyPrefix {
		return fmt.Errorf("Cannot update non-DataContext key: %v", k)
	}
	copy(k[1:1+dvid.InstanceIDSize], instance.Bytes())
	start := len(k) - dvid.VersionIDSize - dvid.ClientIDSize - 1
	copy(k[start:start+dvid.VersionIDSize], version.Bytes())
	start += dvid.VersionIDSize
	copy(k[start:start+dvid.ClientIDSize], client.Bytes())
	return nil
}

// DataContext supports both unversioned and versioned data persistence.
type DataContext struct {
	data    dvid.Data
	version dvid.VersionID
	client  dvid.ClientID
}

// NewDataContext provides a way for datatypes to create a Context that adheres to DVID
// key space partitioning.  Since Context and VersionedContext interfaces are opaque, i.e., can
// only be implemented within package storage, we force compatible implementations to embed
// DataContext and initialize it via this function.
func NewDataContext(data dvid.Data, versionID dvid.VersionID) *DataContext {
	return &DataContext{data, versionID, 0}
}

func (ctx *DataContext) InstanceVersion() dvid.InstanceVersion {
	return dvid.InstanceVersion{ctx.data.DataName(), ctx.version}
}

func (ctx *DataContext) DataName() dvid.InstanceName {
	return ctx.data.DataName()
}

// ---- storage.Context implementation

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) VersionID() dvid.VersionID {
	return ctx.version
}

const (
	byteData      = 0x03
	byteTombstone = 0x4F
)

func (ctx *DataContext) ConstructKey(tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, ctx.version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, byteData))
}

func (ctx *DataContext) TombstoneKey(tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, ctx.version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, byteTombstone))
}

func (ctx *DataContext) IsTombstoneKey(key Key) bool {
	sz := len(key)
	if sz == 0 {
		return false
	}
	switch key[sz-1] {
	case byteData:
		return false
	case byteTombstone:
		return true
	default:
		dvid.Criticalf("Illegal key checked for tombstone marker: %v\n", key)
	}
	return false
}

func (ctx *DataContext) TKeyFromKey(key Key) (TKey, error) {
	if key == nil {
		return nil, fmt.Errorf("Cannot extract DataContext type-specific key component from nil key")
	}
	if key[0] != dataKeyPrefix {
		return nil, fmt.Errorf("Cannot extract DataContext type-specific key component from key type %v", key[0])
	}
	start := 1 + dvid.InstanceIDSize
	end := len(key) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // substract version, client, and tombstone
	return TKey(key[start:end]), nil
}

func (ctx *DataContext) VersionFromKey(key Key) (dvid.VersionID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract DataContext version from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract DataContext version from different key type")
	}
	start := len(key) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // substract version, client, and tombstone
	return dvid.VersionIDFromBytes(key[start : start+dvid.VersionIDSize]), nil
}

func (ctx *DataContext) ClientFromKey(key Key) (dvid.ClientID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract DataContext client from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract DataContext client from different key type")
	}
	start := len(key) - dvid.ClientIDSize - 1 // substract client, and tombstone
	return dvid.ClientIDFromBytes(key[start : start+dvid.ClientIDSize]), nil
}

// Versioned returns false.  This can be overriden by embedding DataContext in structures
// that will support the VersionedContext interface.
func (ctx *DataContext) Versioned() bool {
	return false
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

// ----- partial storage.VersionedContext implementation

// Returns lower bound key for versions of given byte slice key representation.
func (ctx *DataContext) MinVersionKey(tk TKey) (Key, error) {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, dvid.VersionID(0).Bytes()...)
	key = append(key, dvid.ClientID(0).Bytes()...)
	return append(key, 0), nil
}

// Returns upper bound key for versions of given byte slice key representation.
func (ctx *DataContext) MaxVersionKey(tk TKey) (Key, error) {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, dvid.VersionID(dvid.MaxVersionID).Bytes()...)
	key = append(key, dvid.ClientID(dvid.MaxClientID).Bytes()...)
	return append(key, 0xFF), nil
}
