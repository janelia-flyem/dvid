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

	// KeyRange returns the minimum and maximum keys for this context.
	KeyRange() (min, max Key)

	// String prints a description of the Context
	String() string

	// Returns a sync.Mutex specific to this context.
	Mutex() *sync.Mutex

	// Versioned is true if this Context is also a VersionedCtx.
	Versioned() bool

	// Enforces opaque data type.
	implementsOpaque()
}

// VersionedCtx extends a Context with the minimal functions necessary to handle
// versioning in storage engines.
type VersionedCtx interface {
	Context

	// UnversionedKey returns a unversioned Key and the version id
	// as separate components.  This can be useful for storage systems
	// like column stores where the row key is the unversioned Key and
	// the column qualifier is the version id.
	UnversionedKey(TKey) (Key, dvid.VersionID, error)

	// TombstoneKey takes a type-specific key component and returns a key that
	// signals a deletion of any ancestor values.  The returned key must have
	// as its last byte storage.MarkTombstone.
	TombstoneKey(TKey) Key

	// Returns lower bound key for versions.
	MinVersionKey(TKey) (Key, error)

	// Returns upper bound key for versions.
	MaxVersionKey(TKey) (Key, error)

	// VersionedKeyValue returns the key-value pair corresponding to this key's version
	// given a list of key-value pairs across many versions.  If no suitable key-value
	// pair is found, nil is returned.
	VersionedKeyValue([]*KeyValue) (*KeyValue, error)
}

const (
	// MarkData is a byte indicating real data stored and should be the last byte of any
	// versioned key.
	MarkData = 0x03

	// MarkTombstone is a byte indicating a tombstone -- a marker for deleted data.
	MarkTombstone = 0x4F
)

// IsTombstone returns true if the given key is a tombstone key.
func (k Key) IsTombstone() bool {
	sz := len(k)
	if sz == 0 {
		return false
	}
	switch k[sz-1] {
	case MarkData:
		return false
	case MarkTombstone:
		return true
	default:
		dvid.Criticalf("Illegal key checked for tombstone marker: %v\n", k)
	}
	return false
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

func (ctx MetadataContext) KeyRange() (min, max Key) {
	// since all keys starting with dataKeyPrefix have additional bytes, the
	// shorter array with just dataKeyPrefix should precede all data keys.
	return []byte{metadataKeyPrefix}, []byte{dataKeyPrefix}
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
// key space partitioning.  Since Context and VersionedCtx interfaces are opaque, i.e., can
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

func (ctx *DataContext) InstanceID() dvid.InstanceID {
	return ctx.data.InstanceID()
}

// ---- storage.Context implementation

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) VersionID() dvid.VersionID {
	return ctx.version
}

func (ctx *DataContext) ConstructKey(tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, ctx.version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, MarkData))
}

func (ctx *DataContext) TombstoneKey(tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, ctx.version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, MarkTombstone))
}

// TKeyFromKey returns a type-specific key from a full key.  Any DataContext is sufficient as receiver.
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

// KeyRange returns the min and max full keys.  The DataContext can have any version since min/max keys for a data instance
// is independent of the current context's version.
func (ctx *DataContext) KeyRange() (min, max Key) {
	id := ctx.data.InstanceID()
	min = append([]byte{dataKeyPrefix}, id.Bytes()...)
	id++
	max = append([]byte{dataKeyPrefix}, id.Bytes()...)
	return min, max
}

// VersionFromKey returns a version ID from a full key.  Any DataContext is sufficient as receiver.
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
// that will support the VersionedCtx interface.
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
	return fmt.Sprintf("unversioned data ctx %q (local id %d, version id %d)", ctx.data.DataName(),
		ctx.data.InstanceID(), ctx.version)
}

// ----- partial storage.VersionedCtx implementation

// UnversionedKey returns a unversioned Key and the version id
// as separate components.  This can be useful for storage systems
// like column stores where the row key is the unversioned Key and
// the column qualifier is the version id.
func (ctx *DataContext) UnversionedKey(tk TKey) (Key, dvid.VersionID, error) {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	return Key(key), ctx.version, nil
}

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
