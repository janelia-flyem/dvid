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

var (
	minTKey, maxTKey TKey
)

func init() {
	minTKey = []byte{0}
	sz := 128
	maxTKey = make([]byte, sz)
	for i := 0; i < sz; i++ {
		maxTKey[i] = 0xFF
	}
}

// Context allows encapsulation of data that defines the partitioning of the DVID
// key space.
type Context interface {
	// VersionID returns the local version ID of the DAG node being operated on.
	// If not versioned, the version is the root ID.
	VersionID() dvid.VersionID

	// RepoRoot returns the root uuid.
	RepoRoot() (dvid.UUID, error)

	// ConstructKey takes a type-specific key component, and generates a
	// namespaced key that fits with the DVID-wide key space partitioning.
	ConstructKey(TKey) Key

	// ConstructKeyVersion constructs a key like ConstructKey
	// but using specified version
	ConstructKeyVersion(TKey, dvid.VersionID) Key

	// KeyRange returns the minimum and maximum keys for this context.
	KeyRange() (min, max Key)

	// String prints a description of the Context
	String() string

	// Returns a sync.Mutex specific to this context.
	Mutex() *sync.Mutex

	// Versioned is true if this Context is also a VersionedCtx.
	Versioned() bool

	// SplitKey returns key components useful to store all versiones in a familyColumn if the storage engine supports it
	SplitKey(tk TKey) (Key, []byte, error)
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

	// VersionFromKey returns a version ID from a full key.  Any VersionedContext is sufficient as receiver
	VersionFromKey(Key) (dvid.VersionID, error)

	// TombstoneKey takes a type-specific key component and returns a key that
	// signals a deletion of any ancestor values.  The returned key must have
	// as its last byte storage.MarkTombstone.
	TombstoneKey(TKey) Key

	// TombstoneKeyVersion implement TombstoneKey but for the specified version
	TombstoneKeyVersion(TKey, dvid.VersionID) Key

	// Head checks whether this the open head of the master branch
	Head() bool

	// MasterVersion checks whether current version is on master branch
	MasterVersion(dvid.VersionID) bool

	// NumVersions returns the number of version in the current DAG
	NumVersions() int32
	// Returns lower bound key for versions.
	MinVersionKey(TKey) (Key, error)

	// Returns upper bound key for versions.
	MaxVersionKey(TKey) (Key, error)

	// VersionedKeyValue returns the key-value pair corresponding to this key's version
	// given a list of key-value pairs across many versions.  If no suitable key-value
	// pair is found, nil is returned.
	VersionedKeyValue([]*KeyValue) (*KeyValue, error)

	// Data returns the data associated with this context
	Data() dvid.Data
}

// RequestCtx is associated with a particular request (typically a web request) and can
// set and retrieve information about it.
type RequestCtx interface {
	// GetRequestID returns a string identifier or the empty string if none have been set.
	GetRequestID() string

	// SetRequestID sets a string identifier.
	SetRequestID(id string)
}

// DataKeyRange returns the min and max Key across all data keys.
func DataKeyRange() (minKey, maxKey Key) {
	var minID, maxID dvid.InstanceID
	minID, maxID = 0, dvid.MaxInstanceID
	minKey = append([]byte{dataKeyPrefix}, minID.Bytes()...)
	maxKey = append([]byte{dataKeyPrefix}, maxID.Bytes()...)
	return minKey, maxKey
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

// IsMetadataKey returns true if the given key is in the metadata keyspace (instead of data or blob).
func (k Key) IsMetadataKey() bool {
	if k[0] != metadataKeyPrefix {
		return false
	}
	return true
}

// IsDataKey returns true if the given key is in the data keyspace (instead of metadata or blob).
func (k Key) IsDataKey() bool {
	if len(k) < 14 {
		return false
	}
	if k[0] != dataKeyPrefix {
		return false
	}
	return true
}

// IsBlobKey returns true if the given key is in the metadata keyspace (instead of data or blob).
func (k Key) IsBlobKey() bool {
	if k[0] != blobKeyPrefix {
		return false
	}
	return true
}

// TKeyFromKey returns a type-specific key from a full key.  An error is returned if the key is
// not one with a type-specific key component.
func TKeyFromKey(key Key) (TKey, error) {
	if key == nil {
		return nil, fmt.Errorf("Cannot extract DataContext type-specific key component from nil key")
	}
	switch key[0] {
	case metadataKeyPrefix:
		return TKey(key[1:]), nil
	case dataKeyPrefix:
		start := 1 + dvid.InstanceIDSize
		end := len(key) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // subtract version, client, and tombstone
		return TKey(key[start:end]), nil
	default:
		return nil, fmt.Errorf("Cannot extract type-specific key component from key type %v", key[0])
	}
}

// SplitKey returns key components depending on whether the passed Key
// is a metadata or data key.  If metadata, it returns the key and a 0 version id.
// If it is a data key, it returns the unversioned portion of the Key and the
// version id.
func SplitKey(k Key) (unversioned Key, versioned Key, err error) {
	switch k[0] {
	case metadataKeyPrefix:
		unversioned = k
		versioned = make([]byte, 0)
	case dataKeyPrefix:
		start := len(k) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // subtract version, client, and tombstone
		unversioned = k[:start]
		versioned = k[start:len(k)]
	default:
		err = fmt.Errorf("Bad Key given to UnversionedKey().  Key prefix = %d", k[0])
	}
	return
}

func (ctx MetadataContext) SplitKey(tk TKey) (Key, []byte, error) {
	unvKey := append([]byte{metadataKeyPrefix}, tk...)
	verKey := make([]byte, 0)
	return Key(unvKey), verKey, nil
}

//Split the key in two parts: the first one call unversioned key,
//and the second one called versioned key
func (ctx *DataContext) SplitKey(tk TKey) (Key, []byte, error) {
	unvKey := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	unvKey = append(unvKey, tk...)
	verKey := append(ctx.version.Bytes(), ctx.client.Bytes()...)
	verKey = append(verKey, MarkData)
	return Key(unvKey), verKey, nil
}

var contextMutexes map[mutexID]*sync.Mutex

func init() {
	contextMutexes = make(map[mutexID]*sync.Mutex)
}

// ---- Context implementations -----

const (
	metadataKeyPrefix byte = iota
	dataKeyPrefix
	blobKeyPrefix
)

// ConstructBlobKey returns a blob Key, partitioned from other key spaces, for a given key.
func ConstructBlobKey(k []byte) Key {
	return Key(append([]byte{blobKeyPrefix}, k...))
}

// MetadataContext is an implementation of Context for MetadataContext persistence.
type MetadataContext struct{}

func NewMetadataContext() MetadataContext {
	return MetadataContext{}
}

func (ctx MetadataContext) implementsOpaque() {}

func (ctx MetadataContext) VersionID() dvid.VersionID {
	return 0 // Only one version of Metadata
}

func (ctx MetadataContext) RepoRoot() (dvid.UUID, error) {
	return dvid.UUID(""), nil // no repo
}

func (ctx MetadataContext) ConstructKey(tk TKey) Key {
	return Key(append([]byte{metadataKeyPrefix}, tk...))
}

// Note: needed to satisfy interface but should not be called
func (ctx MetadataContext) ConstructKeyVersion(tk TKey, version dvid.VersionID) Key {
	return Key(append([]byte{metadataKeyPrefix}, tk...))
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

func (ctx MetadataContext) RequestID() string {
	return ""
}

// DataKeyToLocalIDs parses a key and returns instance, version and client ids.
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
	reqID   string
}

// NewDataContext provides a way for datatypes to create a Context that adheres to DVID
// key space partitioning.  Since Context and VersionedCtx interfaces are opaque, i.e., can
// only be implemented within package storage, we force compatible implementations to embed
// DataContext and initialize it via this function.
func NewDataContext(data dvid.Data, versionID dvid.VersionID) *DataContext {
	return &DataContext{data, versionID, 0, ""}
}

func (ctx *DataContext) UpdateInstance(k Key) error {
	if k[0] != dataKeyPrefix {
		return fmt.Errorf("Cannot update non-DataContext key: %v", k)
	}
	copy(k[1:1+dvid.InstanceIDSize], ctx.data.InstanceID().Bytes())
	return nil
}

func (ctx *DataContext) InstanceVersion() dvid.InstanceVersion {
	return dvid.InstanceVersion{ctx.data.DataUUID(), ctx.version}
}

func (ctx *DataContext) Data() dvid.Data {
	return ctx.data
}

func (ctx *DataContext) DataName() dvid.InstanceName {
	return ctx.data.DataName()
}

func (ctx *DataContext) InstanceID() dvid.InstanceID {
	return ctx.data.InstanceID()
}

func (ctx *DataContext) ClientID() dvid.ClientID {
	return ctx.client
}

// ---- storage.RequestCtx implementation

// GetRequestID returns a string identifier or the empty string if none have been set.
func (ctx *DataContext) GetRequestID() string {
	return ctx.reqID
}

// SetRequestID sets a string identifier.
func (ctx *DataContext) SetRequestID(id string) {
	ctx.reqID = id
}

// ---- storage.Context implementation

func (ctx *DataContext) implementsOpaque() {}

func (ctx *DataContext) VersionID() dvid.VersionID {
	return ctx.version
}

func (ctx *DataContext) RepoRoot() (dvid.UUID, error) {
	return ctx.data.DAGRootUUID()
}

func (ctx *DataContext) ConstructKey(tk TKey) Key {
	return constructDataKey(ctx.data.InstanceID(), ctx.version, ctx.client, tk)
}

func (ctx *DataContext) ConstructKeyVersion(tk TKey, version dvid.VersionID) Key {
	return constructDataKey(ctx.data.InstanceID(), version, ctx.client, tk)
}

func (ctx *DataContext) TombstoneKey(tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, ctx.version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, MarkTombstone))
}

func (ctx *DataContext) TombstoneKeyVersion(tk TKey, version dvid.VersionID) Key {
	key := append([]byte{dataKeyPrefix}, ctx.data.InstanceID().Bytes()...)
	key = append(key, tk...)
	key = append(key, version.Bytes()...)
	key = append(key, ctx.client.Bytes()...)
	return Key(append(key, MarkTombstone))
}

func constructDataKey(i dvid.InstanceID, v dvid.VersionID, c dvid.ClientID, tk TKey) Key {
	key := append([]byte{dataKeyPrefix}, i.Bytes()...)
	key = append(key, tk...)
	key = append(key, v.Bytes()...)
	key = append(key, c.Bytes()...)
	return Key(append(key, MarkData))
}

// TKeyClassRange returns the min and max full keys for this class of TKeys.
func (ctx *DataContext) TKeyClassRange(c TKeyClass) (min, max Key) {
	id := ctx.data.InstanceID()
	min = append([]byte{dataKeyPrefix}, id.Bytes()...)
	min = append(min, byte(c), 0)
	max = append([]byte{dataKeyPrefix}, id.Bytes()...)
	max = append(max, byte(c), 255, 255, 255, 255, 255, 255, 255, 255, 255)
	return min, max
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

func MinDataKey() Key {
	var minInstanceID dvid.InstanceID
	return append([]byte{dataKeyPrefix}, minInstanceID.Bytes()...)
}

func MaxDataKey() Key {
	var maxInstanceID dvid.InstanceID = dvid.MaxInstanceID
	return append([]byte{dataKeyPrefix}, maxInstanceID.Bytes()...)
}

// TKeyRange returns min and max type-specific keys.  The max key is not guaranteed to be the theoretical maximum TKey but
// should be so for any TKey of 128 bytes or less.  The DataContext can be nil.
func (ctx *DataContext) TKeyRange() (min, max TKey) {
	return minTKey, maxTKey
}

// InstanceFromKey returns an InstanceID from a full key.  Any DataContext is sufficient as receiver.
func (ctx *DataContext) InstanceFromKey(key Key) (dvid.InstanceID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract DataContext instance from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract DataContext version from different key type")
	}
	if len(key) < 5 {
		return 0, fmt.Errorf("Cannot get instance from Key %v less than 5 bytes", key)
	}
	return dvid.InstanceIDFromBytes(key[1 : 1+dvid.InstanceIDSize]), nil
}

// VersionFromKey returns a version ID from a full key.  Any DataContext is sufficient as receiver.
func (ctx *DataContext) VersionFromKey(key Key) (dvid.VersionID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract DataContext version from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract DataContext version from different key type")
	}
	if len(key) < dvid.InstanceIDSize+dvid.VersionIDSize+dvid.ClientIDSize+2 { // TKey must be 0 or larger.
		return 0, fmt.Errorf("Cannot extract version from DataKey that is only %d bytes", len(key))
	}
	start := len(key) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // subtract version, client, and tombstone
	return dvid.VersionIDFromBytes(key[start : start+dvid.VersionIDSize]), nil
}

// VersionFromDataKey returns a version ID from a data key.
func VersionFromDataKey(key Key) (dvid.VersionID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract version from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract version from non-data key type")
	}
	if len(key) < dvid.InstanceIDSize+dvid.VersionIDSize+dvid.ClientIDSize+2 { // TKey must be 0 or larger.
		return 0, fmt.Errorf("Cannot extract version from DataKey that is only %d bytes", len(key))
	}
	start := len(key) - dvid.VersionIDSize - dvid.ClientIDSize - 1 // subtract version, client, and tombstone
	return dvid.VersionIDFromBytes(key[start : start+dvid.VersionIDSize]), nil
}

// ClientFromKey returns a clientID from a full key.  Any DataContext is sufficient as receiver.
func (ctx *DataContext) ClientFromKey(key Key) (dvid.ClientID, error) {
	if key == nil {
		return 0, fmt.Errorf("Cannot extract DataContext client from nil key")
	}
	if key[0] != dataKeyPrefix {
		return 0, fmt.Errorf("Cannot extract DataContext client from different key type")
	}
	if len(key) < dvid.InstanceIDSize+dvid.VersionIDSize+dvid.ClientIDSize+2 { // TKey must be 0 or larger.
		return 0, fmt.Errorf("Cannot extract client from DataKey that is only %d bytes", len(key))
	}
	start := len(key) - dvid.ClientIDSize - 1 // subtract client, and tombstone
	return dvid.ClientIDFromBytes(key[start : start+dvid.ClientIDSize]), nil
}

// ValidKV returns true if a key-value pair is in an allowed set of versions.
// A nil kv always returns true.  An uninterpretable key returns false.
func (ctx *DataContext) ValidKV(kv *KeyValue, versions map[dvid.VersionID]struct{}) bool {
	if kv == nil {
		return true
	}
	v, err := ctx.VersionFromKey(kv.K)
	if err != nil {
		return false
	}
	_, found := versions[v]
	return found
}

// Versioned returns false.  This can be overriden by embedding DataContext in structures
// that will support the VersionedCtx interface.
func (ctx *DataContext) Versioned() bool {
	return false
}

func (ctx *DataContext) RequestID() string {
	return ""
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

func MergeKey(unvKey Key, verKey []byte) Key {
	return Key(append([]byte(unvKey), verKey...))
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
