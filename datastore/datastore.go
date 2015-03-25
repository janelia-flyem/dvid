/*
	This file provides the highest-level view of the datastore via a Service.
*/

package datastore

import (
	"fmt"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.9"
)

var (
	// Manager provides high-level repository management for DVID and is initialized
	// on start.  Package functions provide a quick alias to this default RepoManager.
	Manager RepoManager
)

// ---- Aliased package functions for Repo management.

func NewInstanceID() (dvid.InstanceID, error) {
	if Manager == nil {
		return 0, fmt.Errorf("datastore not initialized")
	}
	return Manager.NewInstanceID()
}

func NewRepoID() (dvid.RepoID, error) {
	if Manager == nil {
		return 0, fmt.Errorf("datastore not initialized")
	}
	return Manager.NewRepoID()
}

func NewUUID() (dvid.UUID, dvid.VersionID, error) {
	if Manager == nil {
		return dvid.NilUUID, 0, fmt.Errorf("datastore not initialized")
	}
	return Manager.NewUUID()
}

func UUIDFromVersion(v dvid.VersionID) (dvid.UUID, error) {
	if Manager == nil {
		return dvid.NilUUID, fmt.Errorf("datastore not initialized")
	}
	return Manager.UUIDFromVersion(v)
}

func VersionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	if Manager == nil {
		return 0, fmt.Errorf("datastore not initialized")
	}
	return Manager.VersionFromUUID(uuid)
}

// MatchingUUID returns version identifiers that uniquely matches a uuid string.
func MatchingUUID(uuidStr string) (dvid.UUID, dvid.VersionID, error) {
	if Manager == nil {
		return dvid.NilUUID, 0, fmt.Errorf("datastore not initialized")
	}
	return Manager.MatchingUUID(uuidStr)
}

// RepoFromUUID returns a Repo given a UUID.  Returns nil Repo if not found.
func RepoFromUUID(uuid dvid.UUID) (Repo, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	return Manager.RepoFromUUID(uuid)
}

// RepoFromVersionID returns a Repo given a version id.
func RepoFromVersionID(v dvid.VersionID) (Repo, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	uuid, err := Manager.UUIDFromVersion(v)
	if err != nil {
		return nil, err
	}
	return Manager.RepoFromUUID(uuid)
}

// RepoFromID returns a Repo from a RepoID.  Returns error if not found.
func RepoFromID(repoID dvid.RepoID) (Repo, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	return Manager.RepoFromID(repoID)
}

// NewRepo creates and returns a new Repo.
func NewRepo(alias, description string) (Repo, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	return Manager.NewRepo(alias, description)
}

// SaveRepo persists a Repo to the MetaDataStore.
func SaveRepo(uuid dvid.UUID) error {
	if Manager == nil {
		return fmt.Errorf("datastore not initialized")
	}
	return Manager.SaveRepo(uuid)
}

func SaveRepoByVersionID(v dvid.VersionID) error {
	if Manager == nil {
		return fmt.Errorf("datastore not initialized")
	}
	return Manager.SaveRepoByVersionID(v)
}

func Types() (map[dvid.URLString]TypeService, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	return Manager.Types()
}

func GetDataByVersion(v dvid.VersionID, name dvid.InstanceName) (DataService, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	uuid, err := Manager.UUIDFromVersion(v)
	if err != nil {
		return nil, err
	}
	repo, err := Manager.RepoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	return repo.GetDataByName(name)
}

func GetDataByUUID(uuid dvid.UUID, name dvid.InstanceName) (DataService, error) {
	if Manager == nil {
		return nil, fmt.Errorf("datastore not initialized")
	}
	repo, err := Manager.RepoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	return repo.GetDataByName(name)
}

// GetParent returns the parent node of the given node.
func GetParentByVersion(v dvid.VersionID) (parent dvid.VersionID, found bool, err error) {
	if Manager == nil {
		err = fmt.Errorf("datastore not initialized")
		return
	}
	var uuid dvid.UUID
	uuid, err = Manager.UUIDFromVersion(v)
	if err != nil {
		return
	}
	var repo Repo
	repo, err = Manager.RepoFromUUID(uuid)
	if err != nil {
		return
	}
	// Get version iterator and check first ancestor.
	var it storage.VersionIterator
	it, err = repo.GetIterator(v)
	if err != nil {
		return
	}
	it.Next()
	return it.VersionID(), it.Valid(), nil
}

// ---- Server Context code, not to be confused with storage.Context.

// The ctxkey type is unexported to prevent collisions with context keys defined in
// other packages.  See Context article at http://blog.golang.org/context
type ctxkey int

const repoCtxKey ctxkey = 0

type repoContext struct {
	repo     Repo
	versions []dvid.VersionID
}

// NewServerContext returns a server Context extended with the Repo and optionally one or more
// versions within that Repo for this request.
func NewServerContext(ctx context.Context, repo Repo, versions ...dvid.VersionID) context.Context {
	return context.WithValue(ctx, repoCtxKey, repoContext{repo, versions})
}

// FromContext returns Repo and optional versions within that Repo from a server Context.
func FromContext(ctx context.Context) (Repo, []dvid.VersionID, error) {
	repoCtxValue := ctx.Value(repoCtxKey)
	value, ok := repoCtxValue.(repoContext)
	if !ok {
		return value.repo, value.versions, fmt.Errorf("Server context has bad value: %v", repoCtxValue)
	}
	return value.repo, value.versions, nil
}
