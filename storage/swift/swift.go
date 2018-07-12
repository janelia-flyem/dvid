// +build swift

package swift

import "github.com/janelia-flyem/dvid/storage"

const (
	// The DVID storage driver identifier for the Openstack Swift engine.
	engineName = "swift"

	// The Swift engine's current version.
	engineVersion = "0.0.1"
)

func init() {
	// Register this engine.
	storage.RegisterEngine(Engine{})
}
