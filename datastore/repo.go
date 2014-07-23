/*
	This file contains platform-independent code for handling a Repo, the basic unit of
	versioning in DVID, and Manager, a collection of Repo.  A Repo consists of a DAG
	where nodes can be optionally locked.
*/

package datastore

import (
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	ErrModifyLockedNode = errors.New("can't modify locked node")
)

// Context provides repo management, logging and storage of key-value pairs using tiers of
// storage.  Contexts are typically set by the server package and are tailored for local,
// clustered, and cloud-based (service-oriented) DVID servers.
type Context interface {
	dvid.Logger
	storage.MetaData
	storage.SmallData
	storage.BigData
}

// IDManager allows atomic ID incrementing across a DVID installation.  In the case
// of a cluster of DVID servers using a common clustered DB, this requires
// consensus between the DVID servers.
type IDManager interface {
	NewInstanceID() (dvid.InstanceID, error)
	NewRepoID() (dvid.RepoID, error)
	NewVersionID() (dvid.UUID, dvid.VersionID, error)

	UUIDFromVersion(dvid.VersionID) (dvid.UUID, error)
	VersionFromUUID(dvid.UUID) (dvid.VersionID, error)
}

type RepoManager interface {
	IDManager
	gob.GobDecoder
	gob.GobEncoder

	// MatchingUUID returns a Repo that uniquely matches a uuid string.
	MatchingUUID(uuidStr string) (dvid.UUID, Repo, error)

	// RepoFromUUID returns a Repo given a UUID.
	RepoFromUUID(dvid.UUID) (Repo, error)

	// RepoFromID returns a Repo from a RepoID.
	RepoFromID(dvid.RepoID) (Repo, error)

	NewRepo() (Repo, error)

	Datatypes() (map[UrlString]TypeService, error)
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
}

type Repo interface {
	gob.GobDecoder
	gob.GobEncoder
	Describer

	RepoID() dvid.RepoID

	RootUUID() dvid.UUID

	// GetDataByName returns a DataService if the name is present or nil otherwise.
	// Names can  be UTF8 except for the hyphen, which is a way of passing additional
	// information to a data instance.  For example, "foo-R" will be parsed as name "foo"
	// with additional information "R" passed to the DataService.
	GetDataByName(dvid.DataString) (DataService, error)

	// GetIterator returns a VersionIterator capable of ascending ancestor path from
	// a particular version in the DAG.
	GetIterator(dvid.VersionID) (dvid.VersionIterator, error)

	// NewData adds a new, named instance of a datatype to repo.  Settings can be passed
	// via the 'config' argument.  For example, config["versioned"] with a bool value
	// will specify whether the data is versioned.
	NewData(TypeService, dvid.DataString, dvid.Config) (DataService, error)

	// ModifyData modifies a preexisting data instance with new configuration settings.
	ModifyData(dvid.DataString, dvid.Config) error

	// NewChild creates a new child node off a LOCKED parent node.  Will return
	// an error if the parent node has not been locked.
	NewChild(dvid.UUID) (dvid.UUID, error)

	Lock(dvid.UUID) error

	Datatypes() (map[UrlString]TypeService, error)
}

// ---- Key space handling for metadata

type keyType byte

const (
	repoToUUIDKey keyType = iota
	versionToUUIDKey
	newIDsKey
	repoKey
)

type metadataIndex struct {
	t      keyType
	repoID dvid.RepoID // Only used for repoKey
}

func (i *metadataIndex) Duplicate() Index {
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
