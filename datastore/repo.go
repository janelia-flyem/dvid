/*
	This file contains platform-independent code for handling a Repo, the basic unit of
	versioning in DVID, and Manager, a collection of Repo.  A Repo consists of a DAG
	where nodes can be optionally locked.
*/

package datastore

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	ErrModifyLockedNode = errors.New("can't modify locked node")
)

// IDManager allows atomic ID incrementing across a DVID installation.  In the case
// of a cluster of DVID servers using a common clustered DB, this requires
// consensus between the DVID servers.
type IDManager interface {
	NewInstanceID() (dvid.InstanceID, error)
	NewRepoID() (dvid.RepoID, error)

	// Creates a new local VersionID for the given UUID.  Will return
	// an error if the given UUID already exists locally, so mainly used
	// in p2p transmission of data that keeps the remote UUID.
	NewVersionID(dvid.UUID) (dvid.VersionID, error)

	// Creates a UUID and its associated local version ID.
	NewUUID() (dvid.UUID, dvid.VersionID, error)

	UUIDFromVersion(dvid.VersionID) (dvid.UUID, error)
	VersionFromUUID(dvid.UUID) (dvid.VersionID, error)
}

type RepoManager interface {
	IDManager

	// MatchingUUID returns version identifiers that uniquely matches a uuid string.
	MatchingUUID(uuidStr string) (dvid.UUID, dvid.VersionID, error)

	// RepoFromUUID returns a Repo given a UUID.  Returns nil Repo if not found.
	RepoFromUUID(dvid.UUID) (Repo, error)

	// RepoFromID returns a Repo from a RepoID.  Returns error if not found.
	RepoFromID(dvid.RepoID) (Repo, error)

	// NewRepo creates and returns a new Repo.
	NewRepo(alias, description string) (Repo, error)

	// AddRepo adds a preallocated Repo.
	AddRepo(Repo) error

	// SaveRepo persists a Repo to the MetaDataStore.
	SaveRepo(dvid.UUID) error
	SaveRepoByVersionID(dvid.VersionID) error

	Types() (map[dvid.URLString]TypeService, error)

	gob.GobDecoder
	gob.GobEncoder
	json.Marshaler
}

type Describer interface {
	GetAlias() string
	SetAlias(string) error

	GetDescription() string
	SetDescription(string) error

	// GetProperty returns a value or nil if there is no named property.
	GetProperty(name string) (interface{}, error)

	// GetProperties returns a map of properties.
	GetProperties() (map[string]interface{}, error)

	SetProperty(name string, value interface{}) error
	SetProperties(map[string]interface{}) error

	GetLog() ([]string, error)
	AddToLog(hx string) error
}

type Repo interface {
	Describer

	RepoID() dvid.RepoID

	RootUUID() dvid.UUID

	Types() (map[dvid.URLString]TypeService, error)

	// GetAllData returns all data instances for this repo.
	GetAllData() (map[dvid.DataString]DataService, error)

	// GetDataByName returns a DataService if the name is present or nil otherwise.
	// Names can  be UTF8 except for the hyphen, which is a way of passing additional
	// information to a data instance.  For example, "foo-R" will be parsed as name "foo"
	// with additional information "R" passed to the DataService.
	GetDataByName(dvid.DataString) (DataService, error)

	// GetIterator returns a VersionIterator capable of ascending ancestor path from
	// a particular version in the DAG.
	GetIterator(dvid.VersionID) (storage.VersionIterator, error)

	// NewData adds a new, named instance of a datatype to repo.  Settings can be passed
	// via the 'config' argument.  For example, config["versioned"] with a bool value
	// will specify whether the data is versioned.
	NewData(TypeService, dvid.DataString, dvid.Config) (DataService, error)

	// ModifyData modifies a preexisting data instance with new configuration settings.
	ModifyData(dvid.DataString, dvid.Config) error

	// DeleteDataByName deletes all data associated with the data instance and removes
	// it from the Repo.
	DeleteDataByName(dvid.DataString) error

	// NewVersion creates a new child node off a LOCKED parent node.  Will return
	// an error if the parent node has not been locked.
	NewVersion(dvid.UUID) (dvid.UUID, error)

	// Save persists the repo to the MetaDataStore.
	Save() error

	// Lock "locks" the given node of the DAG to be read-only.
	Lock(dvid.UUID) error

	gob.GobDecoder
	gob.GobEncoder
	json.Marshaler
}

// ---- Key space handling for metadata

type keyType byte

const (
	repoToUUIDKey keyType = iota
	versionToUUIDKey
	newIDsKey
	repoKey
	formatKey  // Stores MetadataVersion
)

// NetadataVersion is the version of the metadata so we can add new metadata 
// without breaking db.
const MetadataVersion uint64 = 1

func (t keyType) String() string {
	switch t {
	case repoToUUIDKey:
		return "repo to UUID map"
	case versionToUUIDKey:
		return "version to UUID map"
	case newIDsKey:
		return "next new local ids"
	case repoKey:
		return "repository metadata"
	default:
		return fmt.Sprintf("unknown metadata key: %v", t)
	}
}

type metadataIndex struct {
	t      keyType
	repoID dvid.RepoID // Only used for repoKey
}

func (i *metadataIndex) Duplicate() dvid.Index {
	dup := *i
	return &dup
}

func (i *metadataIndex) String() string {
	return fmt.Sprintf("Metadata key type %d, repo ID %d", i.t, i.repoID)
}

func (i *metadataIndex) Bytes() []byte {
	return append([]byte{byte(i.t)}, i.repoID.Bytes()...)
}

func (i *metadataIndex) Scheme() string {
	return "Metadata Index"
}

func (i *metadataIndex) IndexFromBytes(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("Cannot parse index of zero-length slice of bytes")
	}
	i.t = keyType(b[0])
	if i.t == repoKey {
		if len(b) != 1+dvid.RepoIDSize {
			return fmt.Errorf("Bad index for repo: length %d", len(b))
		}
		i.repoID = dvid.RepoIDFromBytes(b[1 : 1+dvid.RepoIDSize])
	}
	return nil
}
