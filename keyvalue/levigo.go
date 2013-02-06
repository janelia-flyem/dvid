// +build !purego

package keyvalue

import (
	"github.com/jmhodges/levigo"
	_ "log"
)

type LevigoLDB struct {
	// Directory of datastore
	directory string

	// Leveldb options
	options *levigo.Options

	// Leveldb connection
	ldb *levigo.DB
}

// Open will open and possibly create a datastore at the given directory.
func OpenLeveldb(path string, create bool) (leveldb *LevigoLDB, err error) {
	leveldb = &LevigoLDB{
		directory: path,
		options:   levigo.NewOptions(),
	}
	leveldb.options.SetCreateIfMissing(create)
	leveldb.ldb, err = levigo.Open(path, leveldb.options)
	return
}

// SetCache sets the size of the LRU cache that caches frequently used 
// uncompressed blocks.
func (leveldb *LevigoLDB) SetCache(nBytes int) {
	leveldb.options.SetCache(levigo.NewLRUCache(nBytes))
}
