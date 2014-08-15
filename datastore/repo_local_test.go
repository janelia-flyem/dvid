// +build !clustered,!gcloud

package datastore

import (
	"reflect"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestRepoGobEncoding(t *testing.T) {
	now := time.Now()
	repo := &repoT{
		repoID: 3,
		rootID: dvid.UUID("23f8"),
		log: []string{
			"Did this",
			"Then that",
			"And the other thing",
		},
		properties: map[string]interface{}{
			"foo": 42,
			"bar": "some string",
			"baz": []int{3, 9, 7},
		},
		dag:     &dagT{},
		data:    make(map[dvid.DataString]DataService),
		created: now,
		updated: now,
	}
	encoding, err := repo.GobEncode()
	if err != nil {
		t.Fatalf("Could not encode repo: %s\n", err.Error())
	}
	received := repoT{}
	if err = received.GobDecode(encoding); err != nil {
		t.Fatalf("Could not decode repo: %s\n", err.Error())
	}
	// Test DAG elsewhere
	repo.dag = nil
	received.dag = nil
	if len(received.properties) != 3 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	foo, ok := received.properties["foo"]
	if !ok || foo != 42 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	bar, ok := received.properties["bar"]
	if !ok || bar != "some string" {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	baz, ok := received.properties["baz"]
	if !ok || !reflect.DeepEqual(baz, []int{3, 9, 7}) {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	repo.properties = nil
	received.properties = nil
	if !reflect.DeepEqual(*repo, received) {
		t.Fatalf("Repo Gob messed up:\nOriginal: %v\nReceived: %v\n", *repo, received)
	}
}

/*
func TestNewDAG(t *testing.T) {
	dag := NewVersionDAG()
	c.Assert(dag.NewVersionID, Equals, dvid.VersionLocalID(1))
	c.Assert(dag.Nodes, HasLen, 1)
	c.Assert(dag.VersionMap, HasLen, 1)
}

func TestRepoPersistence(t *testing.T) {
	dir := c.MkDir()

	// Create a new datastore.
	err := Init(dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Open the datastore
	service, err := Open(dir)
	c.Assert(err, IsNil)

	root, _, err := service.NewRepo()
	c.Assert(err, IsNil)

	c.Assert(service.Lock(root), IsNil)

	child1, err := service.NewVersion(root)
	c.Assert(err, IsNil)

	_, err = service.NewVersion(root)
	c.Assert(err, IsNil)

	c.Assert(service.Lock(child1), IsNil)

	_, err = service.NewVersion(child1)
	c.Assert(err, IsNil)

	oldJSON, err := service.ReposAllJSON()
	c.Assert(err, IsNil)

	service.Shutdown()

	// Open using different service
	service2, err := Open(dir)
	c.Assert(err, IsNil)

	newJSON, err := service2.ReposAllJSON()
	c.Assert(err, IsNil)

	c.Assert(newJSON, DeepEquals, oldJSON)
}

// Make sure each new repo has a different local ID.
func TestNewRepoDifferent(t *testing.T) {
	root1, repoID1, err := s.service.NewRepo()
	c.Assert(err, IsNil)

	root2, repoID2, err := s.service.NewRepo()
	c.Assert(err, IsNil)

	c.Assert(repoID1, Not(Equals), repoID2)
	c.Assert(root1, Not(Equals), root2)
}
*/
