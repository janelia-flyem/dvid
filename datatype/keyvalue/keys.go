/*
	This file supports the keyspace for the keyvalue data type.
*/

package keyvalue

import (
	"fmt"
	"regexp"

	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// the byte id for a standard key of a keyvalue
	keyStandard = 177
)

var (
	validRE = regexp.MustCompile(`^[a-zA-Z0-9_/\-]*$`)
)

// no checking of key
func newTKey(key string) storage.TKey {
	return storage.NewTKey(keyStandard, append([]byte(key), 0))
}

// NewTKey returns the "key" key component.
func NewTKey(key string) (storage.TKey, error) {
	// Make sure the key is suitable
	if !validRE.MatchString(key) {
		return nil, fmt.Errorf("key %q is not composed of just alphanumeric, underscore, forward slash and hyphen characters", key)
	}
	return newTKey(key), nil
}

// DecodeTKey returns the string key used for this keyvalue.
func DecodeTKey(tk storage.TKey) (string, error) {
	ibytes, err := tk.ClassBytes(keyStandard)
	if err != nil {
		return "", err
	}
	sz := len(ibytes) - 1
	if sz <= 0 {
		return "", fmt.Errorf("empty key")
	}
	if ibytes[sz] != 0 {
		return "", fmt.Errorf("expected 0 byte ending key of keyvalue key, got %d", ibytes[sz])
	}
	return string(ibytes[:sz]), nil
}
