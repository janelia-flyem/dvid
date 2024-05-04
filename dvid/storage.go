package dvid

import (
	"fmt"
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

	GetStoreConfig() StoreConfig
}

// AutoInstanceStore is a store that can request automatic temporary instances
// to be created.
type AutoInstanceStore interface {
	AutoInstances() (name, repoUUID string, n int) // name is empty string if none requested
}

// StoreConfig is a store-specific configuration where each store implementation
// defines the types of parameters it accepts.
type StoreConfig struct {
	Config

	// Engine is a simple name describing the engine, e.g., "badger"
	Engine string
}

// DataSpecifier is a scope for a configuration setting.  It can be one of the
// following:
//
//	a plain string -> TypeString (the name of a datatype)
//	a quoted string of form "InstanceName, VersionUUID" -> data instance
//	a quoted string of form "DataUUID" -> data instance
//
// The DataSpecifier can be used as the key of a map, and is useful when configuring
// which data is stored using different storage engines.
type DataSpecifier string

// GetDataSpecifier returns an DataSpecifier given an instance name and a version UUID.
func GetDataSpecifier(name InstanceName, uuid UUID) DataSpecifier {
	return DataSpecifier(name) + DataSpecifier(uuid)
}

// GetDataSpecifierByTag returns a DataSpecifier given a tag and its value.
func GetDataSpecifierByTag(tag, value string) DataSpecifier {
	return DataSpecifier(tag + "-" + value)
}
