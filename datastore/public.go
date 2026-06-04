package datastore

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/janelia-flyem/dvid/dvid"
)

type publicSet struct {
	uuids    map[dvid.UUID]struct{}
	versions map[dvid.VersionID]struct{}
}

var publicVersions atomic.Pointer[publicSet]

// SetPublicVersions validates configured release UUIDs, expands them over all
// transitive ancestors, and publishes an immutable public-access set.
func SetPublicVersions(configured []string) error {
	if len(configured) == 0 {
		publicVersions.Store(&publicSet{
			uuids:    make(map[dvid.UUID]struct{}),
			versions: make(map[dvid.VersionID]struct{}),
		})
		return nil
	}
	m := getManager()
	if m == nil {
		return ErrManagerNotInitialized
	}
	return m.setPublicVersions(configured)
}

func (m *repoManager) setPublicVersions(configured []string) error {
	ps := &publicSet{
		uuids:    make(map[dvid.UUID]struct{}),
		versions: make(map[dvid.VersionID]struct{}),
	}
	for _, raw := range configured {
		releaseUUID, releaseV, err := m.validatePublicReleaseUUID(raw)
		if err != nil {
			return err
		}
		locked, err := m.lockedVersion(releaseV)
		if err != nil {
			return fmt.Errorf("public_versions UUID %q lock check failed: %v", raw, err)
		}
		if !locked {
			return fmt.Errorf("public_versions UUID %q must be committed/locked", raw)
		}
		closure, err := m.ancestorsClosure(releaseV)
		if err != nil {
			return fmt.Errorf("public_versions UUID %q ancestry check failed: %v", raw, err)
		}
		for v := range closure {
			uuid, err := m.uuidFromVersion(v)
			if err != nil {
				return fmt.Errorf("public_versions UUID %q has invalid ancestor version %d: %v", raw, v, err)
			}
			ps.versions[v] = struct{}{}
			ps.uuids[uuid] = struct{}{}
		}
		ps.versions[releaseV] = struct{}{}
		ps.uuids[releaseUUID] = struct{}{}
	}
	publicVersions.Store(ps)
	return nil
}

func (m *repoManager) validatePublicReleaseUUID(raw string) (dvid.UUID, dvid.VersionID, error) {
	if strings.Contains(raw, ":") {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q must be a committed UUID, not a branch selector", raw)
	}
	if raw != strings.ToLower(raw) {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q must be a lowercase canonical UUID", raw)
	}
	uuid, err := dvid.StringToUUID(raw)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q must be a full canonical UUID: %v", raw, err)
	}
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q is unknown: %v", raw, err)
	}
	resolved, err := m.uuidFromVersion(v)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q resolved to invalid version %d: %v", raw, v, err)
	}
	if resolved != uuid || string(resolved) != raw {
		return dvid.NilUUID, 0, fmt.Errorf("public_versions entry %q resolved to %q; full exact UUIDs are required", raw, resolved)
	}
	return uuid, v, nil
}

func (m *repoManager) ancestorsClosure(v dvid.VersionID) (map[dvid.VersionID]struct{}, error) {
	seen := make(map[dvid.VersionID]struct{})
	var walk func(dvid.VersionID) error
	walk = func(cur dvid.VersionID) error {
		if _, found := seen[cur]; found {
			return nil
		}
		seen[cur] = struct{}{}
		parents, err := m.getParentsByVersion(cur)
		if err != nil {
			return err
		}
		for _, parent := range parents {
			if err := walk(parent); err != nil {
				return err
			}
		}
		return nil
	}
	return seen, walk(v)
}

// IsPublic reports whether a resolved UUID is public. It fails closed when no
// public set has been configured.
func IsPublic(uuid dvid.UUID) bool {
	ps := publicVersions.Load()
	if ps == nil {
		return false
	}
	_, found := ps.uuids[uuid]
	return found
}

// IsPublicVersion reports whether a version id is in the cached public set.
func IsPublicVersion(v dvid.VersionID) bool {
	ps := publicVersions.Load()
	if ps == nil {
		return false
	}
	_, found := ps.versions[v]
	return found
}
