/*
	This file contains constants and functions needed to compose keys and handle
	the interface with the underlying key/value datastore.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	_ "log"
	"reflect"

	"github.com/janelia-flyem/dvid/keyvalue"
)

/*
	Key holds DVID-specific keys that follow a convention of how to organize
	data within the key space.  The first byte of the key is a prefix that
	separates general categories of data.  It also allows some headroom to
	create different versions of datastore layout.

		Global data blobs (one item per datastore, e.g., Config):

		  0   Data Type Prefix 
		|---|------------------|

		Global lists (many items per datastore):

		  0   Data Type Prefix   Type-specific key
		|---|------------------|-------------------|

		Versioned block data that are "isolated" so that sequential reads and writes
		don't have any other data types interleaved within the read/write blocks:

		  1     UUID     0   Data Type Prefix     Spatial Key      Type-specific key
		|---|----------|---|------------------|------------------|-------------------|

		Versioned block data that are not "isolated", e.g., different data types 
		within a block:

		  1     UUID     1     Spatial Key      Data Type Prefix   Type-specific key
		|---|----------|---|------------------|------------------|-------------------|

*/
type Key keyvalue.Key

// Key prefixes used for partitioning families of data
const (
	keyFamilyGlobal byte = iota
	keyFamilyBlock
)

// Key prefixes for each global DVID type
const (
	keyPrefixConfig byte = iota
	keyPrefixCache
	keyPrefixDAG
)

func (db kvdb) put(key Key, object interface{}) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(object)
	if err != nil {
		return fmt.Errorf("Error serializing %s: %s", reflect.TypeOf(object), err.Error())
	}
	wo := keyvalue.GetWriteOptions()
	return db.KeyValueDB.Put(keyvalue.Key(key), buffer.Bytes(), wo)
}

func (db kvdb) get(key Key, object interface{}) error {
	ro := keyvalue.GetReadOptions()
	value, err := db.KeyValueDB.Get(keyvalue.Key(key), ro)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(object)
	if err != nil {
		return fmt.Errorf("Error deserializing %s: %s", reflect.TypeOf(object), err.Error())
	}
	return nil
}

func (cache *cachedData) put(db kvdb) error {
	return db.put(Key{keyFamilyGlobal, keyPrefixCache}, *cache)
}

func (cache *cachedData) get(db kvdb) error {
	return db.get(Key{keyFamilyGlobal, keyPrefixCache}, cache)
}

func (config *configData) put(db kvdb) error {
	return db.put(Key{keyFamilyGlobal, keyPrefixConfig}, *config)
}

func (config *configData) get(db kvdb) error {
	return db.get(Key{keyFamilyGlobal, keyPrefixConfig}, config)
}
