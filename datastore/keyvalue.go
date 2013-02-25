/*
	This file contains constants and functions needed to compose keys and handle
	the interface with the underlying key/value datastore.  If a datastore package
	type needs to be stored in the datastore using a DVID-specific key, its put and 
	get functions should be in this file.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	_ "log"
	"reflect"

	_ "github.com/janelia-flyem/dvid/dvid"
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

// BlockKey returns a DVID-specific Key given datastore-specific indices for
// the UUID and data type, and the spatial index and whether the block is "isolated"
// in the key space.
func BlockKey(uuidIndex, spatialIndex []byte, keyDatatype byte, isolated bool) (key Key) {
	key = append(key, keyFamilyBlock)
	key = append(key, uuidIndex...)
	if isolated {
		key = append(key, byte(0))
		key = append(key, keyDatatype)
		key = append(key, spatialIndex...)
	} else {
		key = append(key, byte(1))
		key = append(key, spatialIndex...)
		key = append(key, keyDatatype)
	}
	return
}

// putValue handles serialization of Go value and storage into the key/value datastore.
func (db kvdb) putValue(key Key, object interface{}) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(object)
	if err != nil {
		return fmt.Errorf("Error serializing %s: %s", reflect.TypeOf(object), err.Error())
	}
	return db.putBytes(key, buffer.Bytes())
}

// putBytes handles storage of bytes into the key/value datastore.
func (db kvdb) putBytes(key Key, data []byte) error {
	wo := keyvalue.GetWriteOptions()
	return db.KeyValueDB.Put(keyvalue.Key(key), data, wo)
}

// getValue handles deserialization of Go value and retrieval from the key/value datastore.
func (db kvdb) getValue(key Key, object interface{}) error {
	data, err := db.getBytes(key)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(object)
	if err != nil {
		return fmt.Errorf("Error deserializing %s: %s", reflect.TypeOf(object), err.Error())
	}
	return nil
}

// getBytes handles retrieval from the key/value datastore.
func (db kvdb) getBytes(key Key) (data []byte, err error) {
	ro := keyvalue.GetReadOptions()
	value, err := db.KeyValueDB.Get(keyvalue.Key(key), ro)
	data = value
	return
}

// DVID types are assigned keys and delegated to the type-agnostic get/put functions above

func (cache *cachedData) put(db kvdb) error {
	return db.putValue(Key{keyFamilyGlobal, keyPrefixCache}, *cache)
}

func (cache *cachedData) get(db kvdb) error {
	return db.getValue(Key{keyFamilyGlobal, keyPrefixCache}, cache)
}

func (config *configData) put(db kvdb) error {
	return db.putValue(Key{keyFamilyGlobal, keyPrefixConfig}, *config)
}

func (config *configData) get(db kvdb) error {
	return db.getValue(Key{keyFamilyGlobal, keyPrefixConfig}, config)
}
