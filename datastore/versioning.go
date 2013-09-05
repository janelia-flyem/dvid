/*
	This file handles UUIDs and the version DAG.
*/

package datastore

import (
	"fmt"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/go-uuid/uuid"
)

// UUID is a 32 character hexidecimal string ("" if invalid) that uniquely identifies
// nodes in a datastore's DAG.  We need universally unique identifiers to prevent collisions
// during creation of child nodes by distributed DVIDs:
// http://en.wikipedia.org/wiki/Universally_unique_identifier
type UUID string

// NewUUID returns a UUID
func NewUUID() UUID {
	u := uuid.NewUUID()
	if u == nil || len(u) != 16 {
		return UUID("")
	}
	return UUID(fmt.Sprintf("%032x", []byte(u)))
}

// VersionNode contains all information for a node in the version DAG like its parents,
// children, and provenance.
type VersionNode struct {
	// Id is a globally unique id.
	GlobalID UUID

	// VersionID is a DVID instance-specific id per datastore.
	VersionID dvid.LocalID

	// Locked nodes are read-only and can be branched.
	Locked bool

	// Archived nodes only have delta blocks from its parents, and earlier parents
	// in the parent list have priority in determining the delta.
	Archived bool

	// Parents is an ordered list of parent nodes.
	Parents []UUID

	// Children is a list of child nodes.
	Children []UUID

	// Note holds general information on this node.
	Note string

	// Provenance describes the operations performed between the locking of
	// this node's parents and its current state.
	Provenance string

	Created time.Time
	Updated time.Time
}

// VersionDAG is the directed acyclic graph of VersionNode and an index by UUID into
// the graph.
type VersionDAG struct {
	// NewID keeps track of the VersionID for any new version.
	NewID dvid.LocalID

	Head       UUID
	Nodes      []VersionNode
	VersionMap map[UUID]dvid.LocalID
}

// NewVersionDAG creates a version DAG and initializes the first unlocked node,
// assigning its UUID.
func NewVersionDAG() *VersionDAG {
	dag := VersionDAG{
		NewID: 0,
		Head:  NewUUID(),
		Nodes: []VersionNode{},
	}
	t := time.Now()
	node := VersionNode{
		GlobalID:  dag.Head,
		VersionID: 0,
		Created:   t,
		Updated:   t,
	}
	dag.Nodes = append(dag.Nodes, node)
	dag.VersionMap = map[UUID]dvid.LocalID{dag.Head: 0}
	return &dag
}

// LogInfo returns provenance information for all the version nodes.
func (dag *VersionDAG) LogInfo() string {
	text := "Versions:\n"
	for _, node := range dag.Nodes {
		text += fmt.Sprintf("%s  (%d)\n", node.GlobalID, node.VersionID)
	}
	return text
}

// NewVersion creates a new version node with the given parents.
func (dag *VersionDAG) NewVersion(parents []dvid.LocalID) (node *VersionNode, err error) {
	// TODO -- Implement
	return
}

// Get retrieves a configuration from a KeyValueDB.
func (dag *VersionDAG) Get(db storage.KeyValueDB) (err error) {
	// Get data
	var data []byte
	data, err = db.Get(&KeyVersionDAG)
	if err != nil {
		return
	}

	// Deserialize into object
	err = dag.Deserialize(dvid.Serialization(data))
	return
}

// Put stores a configuration into a KeyValueDB.
func (dag *VersionDAG) Put(db storage.KeyValueDB) (err error) {
	// Get serialization
	var serialization dvid.Serialization
	serialization, err = dag.Serialize()

	// Put data
	return db.Put(&KeyVersionDAG, []byte(serialization))
}

// Serialize returns a serialization of VersionDAG with Snappy compression and
// CRC32 checksum.
func (dag *VersionDAG) Serialize() (s dvid.Serialization, err error) {
	return dvid.Serialize(dag, dvid.Snappy, dvid.CRC32)
}

// Deserialize converts a serialization to a VersionDAG
func (dag *VersionDAG) Deserialize(s dvid.Serialization) (err error) {
	err = dvid.Deserialize(s, dag)
	return
}

// VersionIDFromUUID returns a local version ID given a UUID.  This method does
// not do partial matching, unlike VersionIDFromString().
func (dag *VersionDAG) VersionIDFromUUID(uuid UUID) (id dvid.LocalID, err error) {
	id, found := dag.VersionMap[uuid]
	if !found {
		err = fmt.Errorf("VersionIDFromUUID(): Illegal UUID (%s) not found in Version DAG", uuid)
	}
	return
}

// VersionIDFromString returns a UUID index given its string representation.
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE...,
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (dag *VersionDAG) VersionIDFromString(str string) (id dvid.LocalID, err error) {
	var lastMatch dvid.LocalID
	numMatches := 0
	for uuid, id := range dag.VersionMap {
		if strings.HasPrefix(string(uuid), str) {
			numMatches++
			lastMatch = id
		}
	}
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", str)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", str)
	} else {
		id = lastMatch
	}
	return
}
