/*
	This file provides the exported view of the datastore metadata handling functions.
	All platform-specific code is isolated to the *_local, *_cluster, and similarly named files.

	The repo management functions are package-level functions to avoid lower-level exported
	types like Repo, which invariably depend on global version ids and coordination with the
	singleton repo manager.
*/

package datastore

import (
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.9.2"
)

var (
	// manager provides high-level repository management for DVID and is initialized
	// on start.  Package functions provide a quick alias to this platform-specific repo manager.
	manager *repoManager

	migrator_mu sync.RWMutex
	migrators   map[string]MigrationFunc
)

type MigrationFunc func()

func RegisterAsyncMigration(f MigrationFunc, desc string) {
	migrator_mu.Lock()
	defer migrator_mu.Unlock()

	if migrators == nil {
		migrators = make(map[string]MigrationFunc)
	}
	migrators[desc] = f
}

func Shutdown() {
	// TODO: make sure any kind of shutdown is graceful.
}

func Types() (map[dvid.URLString]TypeService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.types()
}

// MarshalJSON returns JSON of object where each repo is a property with root UUID name
// and value corresponding to repo info.
func MarshalJSON() ([]byte, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.MarshalJSON()
}

// ---- Datastore ID functions ----------

func NewInstanceID() (dvid.InstanceID, error) {
	if manager == nil {
		return 0, ErrManagerNotInitialized
	}
	return manager.newInstanceID()
}

func NewRepoID() (dvid.RepoID, error) {
	if manager == nil {
		return 0, ErrManagerNotInitialized
	}
	return manager.newRepoID()
}

func NewUUID(assign *dvid.UUID) (dvid.UUID, dvid.VersionID, error) {
	if manager == nil {
		return dvid.NilUUID, 0, ErrManagerNotInitialized
	}
	return manager.newUUID(assign)
}

func UUIDFromVersion(v dvid.VersionID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.uuidFromVersion(v)
}

func VersionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	if manager == nil {
		return 0, ErrManagerNotInitialized
	}
	return manager.versionFromUUID(uuid)
}

// MatchingUUID returns version identifiers that uniquely matches a uuid string.
func MatchingUUID(uuidStr string) (dvid.UUID, dvid.VersionID, error) {
	if manager == nil {
		return dvid.NilUUID, 0, ErrManagerNotInitialized
	}
	return manager.matchingUUID(uuidStr)
}

// ----- Repo functions -----------

// NewRepo creates a new Repo and returns its UUID, either an assigned UUID if
// provided or creating a new UUID.
func NewRepo(alias, description string, assign *dvid.UUID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	r, err := manager.newRepo(alias, description, assign)
	return r.uuid, err
}

func GetRepoRoot(uuid dvid.UUID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.getRepoRoot(uuid)
}

func GetRepoJSON(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoJSON(uuid)
}

func GetRepoAlias(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoAlias(uuid)
}

func SetRepoAlias(uuid dvid.UUID, alias string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setRepoAlias(uuid, alias)
}

func GetRepoDescription(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoDescription(uuid)
}

func SetRepoDescription(uuid dvid.UUID, desc string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setRepoDescription(uuid, desc)
}

func GetRepoLog(uuid dvid.UUID) ([]string, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getRepoLog(uuid)
}

func AddToRepoLog(uuid dvid.UUID, msgs []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.addToRepoLog(uuid, msgs)
}

func GetNodeLog(uuid dvid.UUID) ([]string, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getNodeLog(uuid)
}

func AddToNodeLog(uuid dvid.UUID, msgs []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.addToNodeLog(uuid, msgs)
}

// ----- Repo-level DAG functions ----------

// NewVersion creates a new version as a child of the given parent.  If the
// assign parameter is not nil, the new node is given the UUID.
func NewVersion(parent dvid.UUID, note string, assign *dvid.UUID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.newVersion(parent, note, assign)
}

// GetParents returns the parent nodes of the given version id.
func GetParentsByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getParentsByVersion(v)
}

// GetChildren returns the child nodes of the given version id.
func GetChildrenByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getChildrenByVersion(v)
}

// LockedVersion returns true if a given UUID is locked.
func LockedUUID(uuid dvid.UUID) (bool, error) {
	if manager == nil {
		return false, ErrManagerNotInitialized
	}
	return manager.lockedUUID(uuid)
}

// LockedVersion returns true if a given version is locked.
func LockedVersion(v dvid.VersionID) (bool, error) {
	if manager == nil {
		return false, ErrManagerNotInitialized
	}
	return manager.lockedVersion(v)
}

func Commit(uuid dvid.UUID, note string, log []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.commit(uuid, note, log)
}

func Merge(parents []dvid.UUID, note string, mt MergeType) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.merge(parents, note, mt)
}

// ----- Data Instance functions -----------

// NewData adds a new, named instance of a datatype to repo.  Settings can be passed
// via the 'config' argument.  For example, config["versioned"] with a bool value
// will specify whether the data is versioned.
func NewData(uuid dvid.UUID, t TypeService, name dvid.InstanceName, c dvid.Config) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.newData(uuid, t, name, c)
}

// SaveDataByUUID persists metadata for a data instance with given uuid.
// TODO -- Make this more efficient by storing data metadata separately from repo.
//   Currently we save entire repo.
func SaveDataByUUID(uuid dvid.UUID, data DataService) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.saveRepoByUUID(uuid)
}

// SaveDataByVersion persists metadata for a data instance with given version.
// TODO -- Make this more efficient by storing data metadata separately from repo.
//   Currently we save entire repo.
func SaveDataByVersion(v dvid.VersionID, data DataService) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.saveRepoByVersion(v)
}

// GetDataByUUID returns a data service given an instance name and UUID.
func GetDataByUUID(uuid dvid.UUID, name dvid.InstanceName) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByUUID(uuid, name)
}

// GetDataByVersion returns a data service given an instance name and version.
func GetDataByVersion(v dvid.VersionID, name dvid.InstanceName) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByVersion(v, name)
}

// DeleteDataByUUID returns a data service given an instance name and UUID.
func DeleteDataByUUID(uuid dvid.UUID, name dvid.InstanceName) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.deleteDataByUUID(uuid, name)
}

// DeleteDataByVersion returns a data service given an instance name and UUID.
func DeleteDataByVersion(v dvid.VersionID, name dvid.InstanceName) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.deleteDataByVersion(v, name)
}

func ModifyDataConfigByUUID(uuid dvid.UUID, name dvid.InstanceName, c dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.modifyDataByUUID(uuid, name, c)
}

// ------ Cross-platform k/v pair matching for given version, necessary for versioned get.

type kvVersions map[dvid.VersionID]*storage.KeyValue

// FindMatch returns the correct key-value pair for a given version and which version
// that key-value pair came from.
func (kvv kvVersions) FindMatch(v dvid.VersionID) (*storage.KeyValue, dvid.VersionID, error) {
	// Get the ancestor graph for this version.
	if manager == nil {
		return nil, 0, ErrManagerNotInitialized
	}

	// Start from current version and traverse the ancestor graph.  Whenever there's a branch, make
	// sure we only have one matching key.
	return manager.findMatch(kvv, v)
}
