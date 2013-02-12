package datastore

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/hex"
	"fmt"
)

// UUID values identify unique nodes in a datastore's DAG.
// We need universally unique identifiers to prevent collisions
// during creation of child nodes by distributed DVIDs:
// http://en.wikipedia.org/wiki/Universally_unique_identifier
type UUID uuid.UUID

// NewUUID returns a UUID
func NewUUID() UUID {
	return UUID(uuid.NewUUID())
}

// UUIDfromString returns a UUID from its hexadecimal string representation
func UUIDfromString(s string) (u UUID, err error) {
	bytes, err := hex.DecodeString(s)
	if err != nil {
		u = UUID(bytes)
	}
	return
}

// String returns the UUID as a 32 character
// hexidecimal string or "" if the uuid is invalid.
func (u UUID) String() string {
	if u == nil || len(u) != 16 {
		return ""
	}
	return fmt.Sprintf("%032x", []byte(u))
}
