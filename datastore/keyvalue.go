/*
	This file supports key and value handling for the storage layer.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	_ "log"
	"reflect"

	"github.com/janelia-flyem/dvid/dvid"
	_ "github.com/janelia-flyem/dvid/storage"
)

/*
	Key holds DVID-specific keys that follow a convention of how to organize
	data within the key space.  The first byte of the key is a prefix that
	separates general categories of data.  It also allows some headroom to
	create different versions of datastore layout.

	The "Data Index" component describes the type of data and is specific
	to a particular dvid instance.  For example, in one datastore, the data
	index for data set name "grayscale" of data type "voxels" might be 0x02 
	while in another datastore it would be 0x04.  The "Data Index" is a concise
	way to describe both the data set and the data type of this key/value. 

	The "Type-specific key" component depends on the data type.  

    The "Spatial Key" component corresponds to the spatial indexing producing
    by whatever scheme is used for this particular data type. 

		Global data blobs (one item per datastore, e.g., Config):

		  0   Data Index 
		|---|------------|

		Global lists (many items per datastore):

		  0   Data Index   Type-specific key
		|---|------------|-------------------|

		Versioned block data that are "isolated" so that sequential reads and writes
		don't have any other data types interleaved within the read/write blocks:

		  1     UUID     0   Data Index     Spatial Key      Type-specific key
		|---|----------|---|------------|------------------|-------------------|

		Versioned block data that are not "isolated", e.g., different data types 
		within a block:

		  1     UUID     1     Spatial Key      Data Index   Type-specific key
		|---|----------|---|------------------|------------|-------------------|

*/

// Key prefixes used for partitioning families of data
const (
	keyFamilyGlobal byte = iota
	keyFamilyBlock
)

// Key prefixes for each global DVID type
const (
	keyInitConfig byte = iota
	keyRuntimeConfig
	keyDAG
)

// HexString returns a hexadecimal representation of the value bytes.
func HexString(value Value) string {
	b, err := value.Serialize()
	if err != nil {
		log.Fatalf("Serialization of %s failed: %s\n", value, err)
	}
	return fmt.Sprintf("%x", b)
}

// Size returns the # of bytes in this Value.
func Size(value Value) int {
	b, err := value.Serialize()
	if err != nil {
		log.Fatalf("Serialization of %s failed: %s\n", value, err)
	}
	return len(b)
}

// BlockKey returns a DVID-specific Key given datastore-specific indices for
// the UUID and data type, and the spatial index and whether the block is "isolated"
// in the key space.
func BlockKey(uuidIndex, spatialIndex, dataIndex []byte, isolated bool) (key keyvalue.Key) {
	key = append(key, keyFamilyBlock)
	key = append(key, uuidIndex...)
	if isolated {
		key = append(key, byte(0))
		key = append(key, dataIndex...)
		key = append(key, spatialIndex...)
	} else {
		key = append(key, byte(1))
		key = append(key, spatialIndex...)
		key = append(key, dataIndex...)
	}
	return
}

// putValue handles serialization of Go value and storage into the key/value datastore.
func (db kvdb) putValue(key keyvalue.Key, object interface{}) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(object)
	if err != nil {
		errorText := fmt.Sprintf("Error serializing %s: %s",
			reflect.TypeOf(object), err.Error())
		dvid.Error(errorText)
		return fmt.Errorf(errorText)
	}
	return db.putBytes(key, buffer.Bytes())
}

// putBytes handles storage of bytes into the key/value datastore.
func (db kvdb) putBytes(key keyvalue.Key, data []byte) error {
	wo := keyvalue.NewWriteOptions()
	return db.KeyValueDB.Put(key, data, wo)
}

// getValue handles deserialization of Go value and retrieval from the key/value datastore.
func (db kvdb) getValue(key keyvalue.Key, object interface{}) error {
	data, err := db.getBytes(key)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(object)
	if err != nil {
		errorText := fmt.Sprintf("Error deserializing %s: %s",
			reflect.TypeOf(object), err.Error())
		dvid.Error(errorText)
		return fmt.Errorf(errorText)
	}
	return nil
}

// getBytes handles retrieval from the key/value datastore.
// If a key does not exist, we assume whatever implements the KeyValueDB
// will return a nil []byte.
func (db kvdb) getBytes(key keyvalue.Key) (data []byte, err error) {
	ro := keyvalue.NewReadOptions()
	data, err = db.KeyValueDB.Get(key, ro)
	return
}

// DVID types are assigned keys and delegated to the type-agnostic get/put functions above

func (dag *VersionDAG) put(db kvdb) error {
	return db.putValue(keyvalue.Key{keyFamilyGlobal, keyDAG}, *dag)
}

func (dag *VersionDAG) get(db kvdb) error {
	return db.getValue(keyvalue.Key{keyFamilyGlobal, keyDAG}, dag)
}

func (config *initConfig) put(db kvdb) error {
	return db.putValue(keyvalue.Key{keyFamilyGlobal, keyInitConfig}, *config)
}

func (config *initConfig) get(db kvdb) error {
	return db.getValue(keyvalue.Key{keyFamilyGlobal, keyInitConfig}, config)
}

type rconfig struct {
	TypeUrl        UrlString
	DataIndexBytes []byte
}

func (config *runtimeConfig) put(db kvdb) error {
	c := make(map[DatasetString]rconfig)
	for key, value := range config.datasets {
		c[key] = rconfig{value.TypeService.DatatypeUrl(), value.dataIndexBytes}
	}
	return db.putValue(keyvalue.Key{keyFamilyGlobal, keyRuntimeConfig}, c)
}

func (config *runtimeConfig) get(db kvdb) error {
	config.datasets = map[DatasetString]datastoreType{}
	c := make(map[DatasetString]rconfig)
	err := db.getValue(keyvalue.Key{keyFamilyGlobal, keyRuntimeConfig}, &c)
	if err != nil {
		return err
	}
	for key, value := range c {
		dtype, found := CompiledTypes[value.TypeUrl]
		if !found {
			return fmt.Errorf("Data set in datastore no longer present in DVID executable: %s",
				value.TypeUrl)
		}
		config.datasets[key] = datastoreType{dtype, value.DataIndexBytes}
	}
	return nil
}
