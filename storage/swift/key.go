// +build swift

package swift

import (
	"encoding/hex"

	"github.com/janelia-flyem/dvid/storage"
)

// encodeKey translates a key into a string used to address Swift objects. The
// first string byte is the encoding version, followed by the encoded key.
//
// Currently, we encode keys in lower-case hexadecimal strings. As the upper
// limit for Swift object names is 1024 bytes, a DVID key can be at most 511
// bytes long. This is not tested in this function.
func encodeKey(key storage.Key) string {
	// Version "0": lower-case hexadecimal encoding.
	return "0" + hex.EncodeToString(key)
}

// decodeKey translates a Swift object name into a DVID storage key. See
// encodeKey() for more information.
//
// A nil value is returned if the object name could not be decoded.
func decodeKey(objectName string) storage.Key {
	if len(objectName) < 3 || objectName[0] != '0' {
		return nil
	}
	key, err := hex.DecodeString(objectName[1:])
	if err != nil {
		return nil
	}
	return key
}
