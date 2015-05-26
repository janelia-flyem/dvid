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

	// Creates a UUID or uses a given UUID and returns a local version ID to track it with.
	NewUUID(assign *dvid.UUID) (dvid.UUID, dvid.VersionID, error)

	UUIDFromVersion(dvid.VersionID) (dvid.UUID, error)
	VersionFromUUID(dvid.UUID) (dvid.VersionID, error)
}

// RepoManager is the highest-level DVID interface, managing all repos within the datastore.
type RepoManager interface {
	IDManager

	// MatchingUUID returns version identifiers that uniquely matches a uuid string.
	MatchingUUID(uuidStr string) (dvid.UUID, dvid.VersionID, error)

	// RepoFromUUID returns a Repo given a UUID.  Returns nil Repo if not found.
	RepoFromUUID(dvid.UUID) (Repo, error)

	// RepoFromID returns a Repo from a RepoID.  Returns error if not found.
	RepoFromID(dvid.RepoID) (Repo, error)

	// NewRepo creates and returns a new Repo if none is provided.
	NewRepo(alias, description string, assign *dvid.UUID) (Repo, error)

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

	GetRepoLog() ([]string, error)
	AddToRepoLog(string) error
}

type DAGManager interface {
	RootUUID() dvid.UUID
	RootVersion() dvid.VersionID

	GetNodeLog(dvid.UUID) ([]string, error)
	AddToNodeLog(dvid.UUID, []string) error

	// Commit "locks" the given node of the DAG to be read-only, attaches a human-readable
	// note, and appends a slice of strings to the log.
	Commit(uuid dvid.UUID, note string, log []string) error

	// Locked returns true if a node is locked from a commit.
	Locked(dvid.UUID) (bool, error)

	// NewVersion creates a new child node off a LOCKED parent node, either using an
	// assigned UUID if provided or generating a new UUID.  Will return an error if
	// the parent node has not been locked.
	NewVersion(parent dvid.UUID, assign *dvid.UUID) (dvid.UUID, error)

	// GetIterator returns a VersionIterator capable of ascending ancestor path from
	// a particular version in the DAG.
	GetIterator(dvid.VersionID) (storage.VersionIterator, error)

	// GetChildren returns the children of a given version.  This allows you to
	// move through the entire DAG.
	GetChildren(dvid.VersionID) ([]dvid.VersionID, error)
}

type Shutdowner interface {
	Shutdown()
}

type Repo interface {
	Describer
	DAGManager

	RepoID() dvid.RepoID

	Types() (map[dvid.URLString]TypeService, error)

	// GetAllData returns all data instances for this repo.
	GetAllData() (map[dvid.InstanceName]DataService, error)

	// GetDataByName returns a DataService if the name is present or nil otherwise.
	// Names can  be UTF8 except for the hyphen, which is a way of passing additional
	// information to a data instance.  For example, "foo-R" will be parsed as name "foo"
	// with additional information "R" passed to the DataService.
	GetDataByName(dvid.InstanceName) (DataService, error)

	// NewData adds a new, named instance of a datatype to repo.  Settings can be passed
	// via the 'config' argument.  For example, config["versioned"] with a bool value
	// will specify whether the data is versioned.
	NewData(TypeService, dvid.InstanceName, dvid.Config) (DataService, error)

	// ModifyData modifies a preexisting data instance with new configuration settings.
	ModifyData(dvid.InstanceName, dvid.Config) error

	// DeleteDataByName deletes all data associated with the data instance and removes
	// it from the Repo.
	DeleteDataByName(dvid.InstanceName) error

	// Save persists the repo to the MetaDataStore.
	Save() error

	// NotifySubscribers notifies any subscribed instances of an event and sends an
	// event-specific message down internal channels.
	NotifySubscribers(SyncEvent, SyncMessage) error

	gob.GobDecoder
	gob.GobEncoder
	json.Marshaler
}

// ---- Key space handling for metadata

const (
	keyUnknown storage.TKeyClass = iota
	repoToUUIDKey
	versionToUUIDKey
	newIDsKey
	repoKey
	formatKey
)
