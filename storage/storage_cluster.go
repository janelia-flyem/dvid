// +build clustered

// TODO: Implement clustered storage support

package storage

func init() {
	// AddEngineDescription("Local Clustered Datastore")
}

var manager managerT

// managerT should be implemented for each type of storage implementation (local, clustered, gcloud)
// and it should fulfill a storage.Manager interface.
type managerT struct {
}

func MetaDataStore() (MetaDataStorer, error) {
	return nil, nil
}

func MutableStore() (MutableStorer, error) {
	return nil, nil
}

func ImmutableStore() (ImmutableStorer, error) {
	return nil, nil
}

// Shutdown handles any storage-specific shutdown procedures.
func Shutdown() {
}
