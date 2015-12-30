package dvid

import "google.golang.org/cloud/bigtable/bttest"

type StoreConfig struct {
	MetaData  EngineConfig
	Mutable   EngineConfig
	Immutable EngineConfig
}

type EngineConfig struct {
	Config

	// Engine is a simple name describing the engine, e.g., "basholeveldb"
	Engine string

	// Path can be a file path or URL stem
	Path string

	// Testing is true if this store is to be used for testing.
	Testing bool

	// --- Used for big table
	//Id of the google cloud's Project where dvid and bigTable's cluster are running
	Project string

	//Zone where the cluster is allocated
	Zone string

	//Name of the cluster running bigTable servers
	Cluster string

	//Name of the table to be used by the engine
	Table string

	//Test server used for testing big table
	TestSrv *bttest.Server

	// --- Used for big table

}
