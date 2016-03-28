package dvid

import (
	"fmt"

	"google.golang.org/cloud/bigtable/bttest"
)

// StoreCloser stores can be closed.
type StoreCloser interface {
	Close()
}

// StoreIdentifiable stores can say whether they are identified by a given store configuration.
type StoreIdentifiable interface {
	// Equal returns true if this store matches the given store configuration.
	Equal(StoreConfig) bool
}

// Store allows polyglot persistence of data.  The Store implementation
// could be an ordered key-value database, graph store, etc.
type Store interface {
	fmt.Stringer
	StoreCloser
	StoreIdentifiable
}

type StoreConfig struct {
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

	// Bucket is name of Google Bucket
	Bucket string
}
