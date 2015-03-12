/*
	This file supports the keyspace for the keyvalue data type.
*/

package keyvalue

import (
	"fmt"
	"regexp"

	"github.com/janelia-flyem/dvid/storage"
)

// keyType is the first byte of a type-specific index, allowing partitioning of the
// type-specific key space.
type keyType byte

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown keyType = iota

	// the byte id for a standard key of a keyvalue
	keyStandard = 177
)

var (
	validRE = regexp.MustCompile(`^[a-zA-Z0-9_/\-]*$`)
)

func (t keyType) String() string {
	switch t {
	case keyUnknown:
		return "Unknown key Type"
	case keyStandard:
		return "Standard key"
	default:
		return "Unknown key Type"
	}
}

// no checking of key
func newIndex(key string) ([]byte, error) {
	sz := len(key)
	ibytes := make([]byte, 1+sz+1)
	ibytes[0] = byte(keyStandard)
	copy(ibytes[1:1+sz], []byte(key))
	ibytes[1+sz] = 0
	return ibytes, nil
}

// NewIndex returns an identifier for storing a key
func NewIndex(key string) ([]byte, error) {
	// Make sure the key is suitable
	if !validRE.MatchString(key) {
		return nil, fmt.Errorf("key %q is not composed of just alphanumeric, underscore, forward slash and hyphen characters", key)
	}
	return newIndex(key)
}

// DecodeKey returns the string key used for this keyvalue.
func DecodeKey(key []byte) (string, error) {
	var ctx storage.DataContext
	ibytes, err := ctx.IndexFromKey(key)
	if err != nil {
		return "", err
	}
	if ibytes[0] != byte(keyStandard) {
		return "", fmt.Errorf("Expected keyStandard index, got %d byte instead", ibytes[0])
	}
	sz := len(ibytes) - 2
	if sz <= 0 {
		return "", fmt.Errorf("empty key")
	}
	if ibytes[1+sz] != 0 {
		return "", fmt.Errorf("expected 0 byte ending key of keyvalue instance, got %d", ibytes[1+sz])
	}
	return string(ibytes[1 : 1+sz]), nil
}
