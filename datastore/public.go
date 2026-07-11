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
	seeds := make([]dvid.VersionID, 0, len(configured))
	for _, raw := range configured {
		_, releaseV, err := m.validatePublicReleaseUUID(raw)
		if err != nil {
			return err
		}
		locked, err := m.lockedVersion(releaseV)
		if err != nil {
			return fmt.Errorf("public release UUID %q lock check failed: %v", raw, err)
		}
		if !locked {
			return fmt.Errorf("public release UUID %q must be committed/locked", raw)
		}
		seeds = append(seeds, releaseV)
	}
	return m.publishPublicVersions(seeds)
}

// SetPublicVersionsFromSeeds expands committed release versions over all
// transitive ancestors and atomically publishes the resulting public set.
// Uncommitted seeds are ignored so incomplete releases cannot become public.
func SetPublicVersionsFromSeeds(seeds []dvid.VersionID) error {
	if len(seeds) == 0 {
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
	committed := make([]dvid.VersionID, 0, len(seeds))
	for _, seed := range seeds {
		locked, err := m.lockedVersion(seed)
		if err != nil {
			return fmt.Errorf("public seed version %d lock check failed: %v", seed, err)
		}
		if !locked {
			dvid.Warningf("Skipping uncommitted public seed version %d\n", seed)
			continue
		}
		committed = append(committed, seed)
	}
	return m.publishPublicVersions(committed)
}

func (m *repoManager) publishPublicVersions(seeds []dvid.VersionID) error {
	ps := &publicSet{
		uuids:    make(map[dvid.UUID]struct{}),
		versions: make(map[dvid.VersionID]struct{}),
	}
	for _, seed := range seeds {
		closure, err := m.ancestorsClosure(seed)
		if err != nil {
			return fmt.Errorf("public seed version %d ancestry check failed: %v", seed, err)
		}
		for v := range closure {
			uuid, err := m.uuidFromVersion(v)
			if err != nil {
				return fmt.Errorf("public seed version %d has invalid ancestor version %d: %v", seed, v, err)
			}
			ps.versions[v] = struct{}{}
			ps.uuids[uuid] = struct{}{}
		}
	}
	publicVersions.Store(ps)
	return nil
}

func (m *repoManager) validatePublicReleaseUUID(raw string) (dvid.UUID, dvid.VersionID, error) {
	if strings.Contains(raw, ":") {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q must be a committed UUID, not a branch selector", raw)
	}
	if raw != strings.ToLower(raw) {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q must be a lowercase canonical UUID", raw)
	}
	uuid, err := dvid.StringToUUID(raw)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q must be a full canonical UUID: %v", raw, err)
	}
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q is unknown: %v", raw, err)
	}
	resolved, err := m.uuidFromVersion(v)
	if err != nil {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q resolved to invalid version %d: %v", raw, v, err)
	}
	if resolved != uuid || string(resolved) != raw {
		return dvid.NilUUID, 0, fmt.Errorf("public release entry %q resolved to %q; full exact UUIDs are required", raw, resolved)
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

// VersionPrecedesOrEquals reports whether requested is the same version as,
// or an ancestor of, anchor.
func VersionPrecedesOrEquals(requested, anchor dvid.VersionID) (bool, error) {
	m := getManager()
	if m == nil {
		return false, ErrManagerNotInitialized
	}
	closure, err := m.ancestorsClosure(anchor)
	if err != nil {
		return false, err
	}
	_, found := closure[requested]
	return found, nil
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
