package datastore

import (
	"github.com/janelia-flyem/dvid/keyvalue"
)

// Keys used for partitioning families of data
type keyFamily byte

const (
	keyPrefixConfig keyFamily = iota
	keyPrefixDAG
	keyPrefixBlock
)

// Properties of image volume
type keyVolumeProperties byte

const (
	keyMaxX keyVolumeProperties = iota
	keyMaxY
	keyMaxZ
	keyMaxT
	keyResX
	keyResY
	keyResZ
	keyBlockMaxX
	keyBlockMaxY
	keyBlockMaxZ
)

// Keys used for versioning data
const (
// Keys associated with the DAG for image versioning

// Properties of a particular image version, i.e., node of the DAG
)

func getKeyVolumeProperties(propType keyVolumeProperties) keyvalue.Key {
	return keyvalue.Key{byte(keyPrefixConfig), byte(propType)}
}
