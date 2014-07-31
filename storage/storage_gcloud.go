// +build gcloud

// TODO: Implement google cloud storage support

package storage

func init() {
	// AddEngineDescription("Google Cloud Datastore/Blobstore")
	SetupEngines()
	SetupTiers()
}

var manager managerT

// managerT should be implemented for each type of storage implementation (local, clustered, gcloud)
// and it should fulfill a storage.Manager interface.
type managerT struct {
}

func MetaDataStore() (MetaDataStorer, error) {
	return nil, nil
}

func SmallDataStore() (SmallDataStorer, error) {
	return nil, nil
}

func BigDataStore() (BigDataStorer, error) {
	return nil, nil
}

func SetupEngines() {
}

// --- Implement the three tiers of storage using Google services.

func SetupTiers() {
}

// Shutdown handles any storage-specific shutdown procedures.
func Shutdown() {
}
