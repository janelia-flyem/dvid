/*
	This file supports the keyspace for the keyvalue data type.
*/
package neuronjson

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// key = bodyid. value = serialized map[string]interface{} annotation
	keyAnnotation = 179

	// single key-value for neutu/neu3 schema JSON
	keySchema = 180

	// single key-value for neutu/neu3 schema-batch JSON
	keySchemaBatch = 181

	// single key-value for JSON validation schema
	keyJSONSchema = 182
)

var MinAnnotationTKey = storage.MinTKey(keyAnnotation)
var MaxAnnotationTKey = storage.MaxTKey(keyAnnotation)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyAnnotation:
		return "neuron annotation"
	case keySchema:
		return "neutu/neu3 schema"
	case keySchemaBatch:
		return "neutu/neu3 schema for batches"
	case keyJSONSchema:
		return "neuron annotation JSON schema for validation"
	default:
	}
	return "unknown neuronjson key"
}

// NewTKey returns a TKey for the annotation kv pairs.
func NewTKey(key string) (storage.TKey, error) {
	return storage.NewTKey(keyAnnotation, append([]byte(key), 0)), nil
}

// DecodeTKey returns the string of the bodyid used for this annotation.
func DecodeTKey(tk storage.TKey) (string, error) {
	ibytes, err := tk.ClassBytes(keyAnnotation)
	if err != nil {
		return "", err
	}
	sz := len(ibytes) - 1
	if sz <= 0 {
		return "", fmt.Errorf("empty key")
	}
	if ibytes[sz] != 0 {
		return "", fmt.Errorf("expected 0 byte ending key of neuronjson key, got %d", ibytes[sz])
	}
	return string(ibytes[:sz]), nil
}

// NewJSONSchemaTKey returns a TKey for JSON validation schema storage.
func NewJSONSchemaTKey() (storage.TKey, error) {
	return storage.NewTKey(keyJSONSchema, nil), nil
}

// NewSchemaTKey returns a TKey for schema storage.
func NewSchemaTKey() (storage.TKey, error) {
	return storage.NewTKey(keySchema, nil), nil
}

// NewSchemaBatchTKey returns a TKey for batch schema storage.
func NewSchemaBatchTKey() (storage.TKey, error) {
	return storage.NewTKey(keySchemaBatch, nil), nil
}

func getMetadataKey(meta Schema) (tkey storage.TKey, err error) {
	switch meta {
	case JSONSchema:
		if tkey, err = NewJSONSchemaTKey(); err != nil {
			return
		}
	case NeuSchema:
		if tkey, err = NewSchemaTKey(); err != nil {
			return
		}
	case NeuSchemaBatch:
		if tkey, err = NewSchemaBatchTKey(); err != nil {
			return
		}
	}
	return tkey, nil
}
