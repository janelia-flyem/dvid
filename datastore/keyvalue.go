package datastore

import (
	"github.com/jmhodges/levigo"
	_ "log"
)

// Default size of a side of a cuboid, the "atoms" of storage in datastore.
const DefaultCuboidSize = 16

// Default size of leveldb cache
const Giga = 1024 * 1024 * 1024
const DefaultCacheSize = 1 * Giga

// Config contains the essential properties of an image volume.
type Config struct {
	// Directory of datastore
	Directory string

	// Maximum size of space stored by datastore
	MaxX, MaxY, MaxZ int

	// CuboidSize is the length of a side of a cuboid, the "atoms" of storage in datastore.
	CuboidSize int

	// Leveldb options
	options *levigo.Options

	// Leveldb connection
	db *levigo.DB
}

// NewConfig returns a pointer to a default Config
func NewConfig() *Config {
	options := levigo.NewOptions()
	options.SetCache(levigo.NewLRUCache(DefaultCacheSize))

	return &Config{
		MaxX:       0,
		MaxY:       0,
		MaxZ:       0,
		CuboidSize: DefaultCuboidSize,
		options:    options,
	}
}

// Keys used for image volume and version properties
const (
	// Keys that define essential properties of image volume
	KeyMaxX       = "DVID_MAX_X"
	KeyMaxY       = "DVID_MAX_Y"
	KeyMaxZ       = "DVID_MAX_Z"
	KeyCuboidSize = "DVID_CUBOID_SIZE"

	// Keys associated with the DAG for image versioning

	// Properties of a particular image version, i.e., node of the DAG
)

// Open will open and possibly create a datastore at the given directory.
func Open(config *Config, create bool) (err error) {
	config.options.SetCreateIfMissing(create)
	config.db, err = levigo.Open(config.Directory, config.options)
	return
}
