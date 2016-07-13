/*
	This file contains platform-independent code for handling DVID metadata describing
	the collection of repos, each of which is a versioned dataset that holds a DAG and
	a list of data instances.  DVID access to metadata should be through package-level
	functions.
*/

package datastore

import "errors"

// MergeType describes the expectation of processing for the merge, e.g., is it
// expected to be free of conflicts at the key-value level, require automated
// type-specific key-value merging, or require external guidance.
type MergeType uint8

const (
	// MergeConflictFree assumes that changes in nodes-to-be-merged are disjoint
	// at the key-value level.
	MergeConflictFree MergeType = iota

	// MergeTypeSpecificAuto requires datatype-specific code for merging at each
	// key-value pair.
	MergeTypeSpecificAuto

	// MergeExternalData requires external data to reconcile merging of nodes.
	MergeExternalData
)

var (
	ErrManagerNotInitialized = errors.New("datastore repo manager not initialized")
	ErrBadMergeType          = errors.New("bad merge type")

	ErrInvalidUUID         = errors.New("UUID is not present in datastore")
	ErrInvalidVersion      = errors.New("server-specific version id is invalid")
	ErrInvalidRepoID       = errors.New("server-specific repo id is invalid")
	ErrExistingUUID        = errors.New("UUID already exists in datastore")
	ErrInvalidDataName     = errors.New("invalid data instance name")
	ErrInvalidDataInstance = errors.New("invalid data instance id")
	ErrInvalidDataUUID     = errors.New("invalid data instance UUID")

	ErrInvalidStore = errors.New("invalid data store")

	ErrModifyLockedNode   = errors.New("can't modify locked node")
	ErrBranchUnlockedNode = errors.New("can't branch an unlocked node")
)
