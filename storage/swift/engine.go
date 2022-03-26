// +build swift

package swift

import (
	"fmt"

	"github.com/blang/semver"

	"github.com/janelia-flyem/dvid/dvid"
)

// Engine implements storage.Engine for the Openstack Swift backend.
type Engine struct{}

func (e Engine) String() string {
	return fmt.Sprintf(`Swift engine "%s" version %s`, engineName, engineVersion)
}

// GetName returns the DVID storage driver identifier.
func (e Engine) GetName() string {
	return engineName
}

// IsDistributed returns true because Swift is a distributed database.
func (e Engine) IsDistributed() bool {
	return true
}

// GetSemVer returns the engine's current version.
func (e Engine) GetSemVer() semver.Version {
	return semver.MustParse(engineVersion)
}

// NewStore returns a new Swift storage engine given the passed configuration.
func (e Engine) NewStore(config dvid.StoreConfig) (db dvid.Store, initMetadata bool, err error) {
	return NewStore(config)
}
