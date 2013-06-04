/*
	This file handles UUIDs and the version DAG.
*/

package datastore

import (
	"fmt"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"code.google.com/p/go-uuid/uuid"
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

// VersionID is a unique id that corresponds to one UUID in a datastore.  We limit the
// number of versions that can be present in a datastore and use that more compact 
// index compared to the full 16 byte UUID for construction of keys.
type VersionID dvid.LocalID

// VersionNode contains all information for a node in the version DAG like its parents,
// children, and provenance.
type VersionNode struct {
	// Id is a globally unique id.
	Id UUID

	// Index is a unique id per datastore.
	Index VersionID

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
	Head       UUID
	Nodes      []VersionNode
	VersionMap map[UUID]VersionID
}

// NewVersionDAG creates a version DAG and initializes the first unlocked node,
// assigning its UUID.
func NewVersionDAG() *VersionDAG {
	dag := VersionDAG{
		Head:  NewUUID(),
		Nodes: []VersionNode{},
	}
	t := time.Now()
	node := VersionNode{
		Id:      dag.Head,
		Index:   0,
		Created: t,
		Updated: t,
	}
	dag.Nodes = append(dag.Nodes, node)
	dag.VersionMap = map[UUID]VersionID{dag.Head: 0}
	return &dag
}

// LogInfo returns provenance information for all the version nodes.
func (dag *VersionDAG) LogInfo() string {
	text := "Versions:\n"
	for _, node := range dag.Nodes {
		text += fmt.Sprintf("%s  (%d)\n", node.Id, node.Index)
	}
	return text
}

// VersionIDFromString returns a UUID index given its string representation.  
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (dag *VersionDAG) VersionIDFromString(str string) (id VersionID, err error) {
	var lastMatch VersionID
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
