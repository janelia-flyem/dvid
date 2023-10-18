/*
	This file contains the core DVID types that track data within repositories.
*/

package dvid

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/twinj/uuid"
)

// LocalID is a unique id for some data in a DVID instance.  This unique id is a much
// smaller representation than the actual data (e.g., a version UUID or data type url)
// and can be represented with fewer bytes in keys.
type LocalID uint16

// LocalID32 is a 32-bit unique id within this DVID instance.
type LocalID32 uint32

const (
	LocalIDSize   = 2
	LocalID32Size = 4

	MaxLocalID   = 0xFFFF
	MaxLocalID32 = 0xFFFFFFFF
)

// Bytes returns a sequence of bytes encoding this LocalID.  Binary representation
// will be big-endian to make integers lexicographically ordered.
func (id LocalID) Bytes() []byte {
	buf := make([]byte, LocalIDSize, LocalIDSize)
	binary.BigEndian.PutUint16(buf, uint16(id))
	return buf
}

// LocalIDFromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalIDFromBytes(b []byte) (id LocalID, length int) {
	return LocalID(binary.BigEndian.Uint16(b)), LocalIDSize
}

// Bytes returns a sequence of bytes encoding this LocalID32.
func (id LocalID32) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// LocalID32FromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for LocalID.
func LocalID32FromBytes(b []byte) (id LocalID32, length int) {
	return LocalID32(binary.BigEndian.Uint32(b)), LocalID32Size
}

// ---- Base identifiers of data within DVID -----

// UUID is a 32 character hexadecimal string (a RFC4122 version 4 UUID) that uniquely identifies
// nodes in a datastore's DAG.  We need universally unique identifiers to prevent collisions
// during creation of child nodes by distributed DVIDs:
// http://en.wikipedia.org/wiki/Universally_unique_identifier
// An empty string is an invalid (NilUUID).
type UUID string

// NewUUID returns a UUID
func NewUUID() UUID {
	u := uuid.NewV4()
	return UUID(fmt.Sprintf("%032x", u.Bytes()))
}

const NilUUID = UUID("")

// StringToUUID converts a string to a UUID, checking to make sure it is a 32 character hex string.
func StringToUUID(s string) (UUID, error) {
	var err error
	if len(s) != 32 {
		err = fmt.Errorf("UUID must be 32 character hexadecimal string")
	} else {
		_, err = hex.DecodeString(s)
	}
	if err != nil {
		return NilUUID, err
	}
	return UUID(s), nil
}

// UUIDSet is a set of UUIDs.
type UUIDSet map[UUID]struct{}

// Equals returns true if the two UUIDSets have the same elements.
func (uset UUIDSet) Equals(uset2 UUIDSet) bool {
	if len(uset) != len(uset2) {
		return false
	}
	for uuid := range uset {
		if uset[uuid] != uset2[uuid] {
			return false
		}
	}
	return true
}

// Add adds a given UUIDSet to the receiver.
func (uset UUIDSet) Add(uset2 UUIDSet) {
	if len(uset2) == 0 {
		return
	}
	for uuid := range uset2 {
		uset[uuid] = struct{}{}
	}
}

// Note: TypeString and InstanceName are types to add static checks and prevent conflation
// of the two types of identifiers.

// TypeString is a string that is the name of a DVID data type.
type TypeString string

// URLString is a string representing a URL.
type URLString string

// InstanceName is a string that is the name of DVID data.
type InstanceName string

// InstanceNames is a slice of DVID data instance names.
type InstanceNames []InstanceName

func (i InstanceNames) String() string {
	s := make([]string, len(i))
	for j, name := range i {
		s[j] = string(name)
	}
	return strings.Join(s, ", ")
}

// InstanceID is a DVID server-specific identifier for data instances.  Each InstanceID
// is only used within one repo, so all key/values for a repo can be obtained by
// doing range queries on instances associated with a repo.  Valid InstanceIDs should
// be greater than 0.
type InstanceID LocalID32

// Bytes returns a sequence of bytes encoding this InstanceID.
func (id InstanceID) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// InstanceIDFromBytes returns a LocalID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for InstanceID.
func InstanceIDFromBytes(b []byte) InstanceID {
	return InstanceID(binary.BigEndian.Uint32(b))
}

// InstanceVersion identifies a particular version of a data instance.
type InstanceVersion struct {
	Data    UUID
	Version VersionID
}

func (iv InstanceVersion) String() string {
	return fmt.Sprintf("[%s version %d]", iv.Data, iv.Version)
}

// RepoID is a DVID server-specific identifier for a particular Repo.  Valid RepoIDs
// should be greater than 0.
type RepoID LocalID32

// Bytes returns a sequence of bytes encoding this RepoID.  Binary representation is big-endian
// to preserve lexicographic order.
func (id RepoID) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// RepoIDFromBytes returns a RepoID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for RepoID.
func RepoIDFromBytes(b []byte) RepoID {
	return RepoID(binary.BigEndian.Uint32(b))
}

// VersionID is a DVID server-specific identifier for a particular version or
// node of a repo's DAG.  Valid VersionIDs should be greater than 0.
type VersionID LocalID32

// Bytes returns a sequence of bytes encoding this VersionID.  Binary representation is big-endian
// to preserve lexicographic order.
func (id VersionID) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// VersionIDFromBytes returns a VersionID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for VersionID.
func VersionIDFromBytes(b []byte) VersionID {
	return VersionID(binary.BigEndian.Uint32(b))
}

type InstanceMap map[InstanceID]InstanceID
type VersionMap map[VersionID]VersionID

const (
	MaxInstanceID = MaxLocalID32
	MaxRepoID     = MaxLocalID32
	MaxVersionID  = MaxLocalID32
	MaxClientID   = MaxLocalID32

	InstanceIDSize = 4
	RepoIDSize     = 4
	VersionIDSize  = 4
	ClientIDSize   = 4
)

// ClientID is a DVID server-specific identifier of an authorized client.  It is used with data keys
// to track key-values on a per-client basis.
type ClientID LocalID32

// Bytes returns a sequence of bytes encoding this ClientID.  Binary representation is big-endian
// to preserve lexicographic order.
func (id ClientID) Bytes() []byte {
	buf := make([]byte, LocalID32Size, LocalID32Size)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// ClientIDFromBytes returns a VersionID from the start of the slice and the number of bytes used.
// Note: No error checking is done to ensure byte slice has sufficient bytes for ClientID.
func ClientIDFromBytes(b []byte) ClientID {
	return ClientID(binary.BigEndian.Uint32(b))
}

// DataID returns unique identifiers for a data instance.  A data instance can be identified
// by a globally-unique, invariant UUID or a changing two-tuple (name, data root UUID).
type DataID struct {
	// DataUUID is a globally unique identifier for this particular data instance,
	// whether it gets renamed or a portion of its DAG gets distributed to another
	// server.
	DataUUID UUID

	// Name, which can be changed over time.
	Name InstanceName

	// RootUUID is the root of the subgraph in which this data instance is defined.
	// Because a portion of data can be distributed to other servers, the current
	// DAG might be truncated, so the RootUUID can also be a child of the data
	// instance's original RootUUID.
	RootUUID UUID
}

// Data is the minimal interface for datatype-specific data that is implemented
// in datatype packages.  This interface is defined in the low-level dvid package so it
// is accessible at all levels of dvid, although the implementation of this interface
// occurs at the datastore and datatype-level packages.
type Data interface {
	InstanceID() InstanceID

	DataUUID() UUID
	DataName() InstanceName
	RootUUID() UUID
	RootVersionID() (VersionID, error)
	DAGRootUUID() (UUID, error)

	TypeName() TypeString
	TypeURL() URLString
	TypeVersion() string

	Tags() map[string]string

	// Versioned returns false if this data has only one version for an entire repo.
	Versioned() bool

	// KVStore returns the key-value store used for this data.
	KVStore() (Store, error)

	// Returns a concurrency-friendly unique operations ID for this Data.
	// The ID is monotonically increasing although it is not necessarily sequential.
	NewMutationID() uint64

	DataSetter

	IsDeleted() bool
	SetDeleted(bool) // use true if this data is in process of being deleted
}

// DataSetter provides interface for setting main properties of Data during
// initialization and remote transmission.
type DataSetter interface {
	SetKVStore(Store)
	SetLogStore(Store)

	SetInstanceID(InstanceID)
	SetDataUUID(UUID)
	SetName(InstanceName)
	SetRootUUID(UUID)

	// SetSync defines a set of data UUIDs for syncing with this data instance.
	// This could be used by higher software layers to implement pub/sub-style syncing.
	SetSync(UUIDSet)

	SetTags(map[string]string)

	// PersistMetadata writes changes for this data instance out to the metadata store.
	PersistMetadata() error
}

// Axis enumerates different types of axis (x, y, z, time, etc)
type Axis uint8

const (
	XAxis Axis = iota
	YAxis
	ZAxis
	TAxis
)

func (a Axis) String() string {
	switch a {
	case XAxis:
		return "X axis"
	case YAxis:
		return "Y axis"
	case ZAxis:
		return "Z axis"
	case TAxis:
		return "Time"
	default:
		return "Unknown"
	}
}
