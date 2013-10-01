/*
	This file contains the Key interface and the KeyValue struct returned by range queries.

	While Key data has a number of components so key space can be managed
	more efficiently by the storage engine, values are simply []byte at
	this level.  We assume value serialization/deserialization occur above the
	storage level.
*/

package storage

import (
	"bytes"

	"github.com/janelia-flyem/dvid/dvid"
)

// KeyType is a portion of a serialized Key that partitions key space into
// distinct areas and makes sure keys don't conflict.
type KeyType byte

type Key interface {
	KeyType() KeyType
	BytesToKey([]byte) (Key, error)
	Bytes() []byte
	BytesString() string
	String() string
}

// KeyValue stores a key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// Deserialize returns a key-value pair where the value has been deserialized.
func (kv KeyValue) Deserialize(uncompress bool) (KeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return KeyValue{kv.K, value}, err
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K.Bytes(), kv[j].K.Bytes()) <= 0
}
