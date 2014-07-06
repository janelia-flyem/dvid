// +build !clustered,!gcloud

package datastore

import (
	_ "testing"
	. "github.com/janelia-flyem/go/gocheck"

	"github.com/janelia-flyem/dvid/dvid"
)

func (s *DataSuite) TestNewDAG(c *C) {
	dag := NewVersionDAG()
	c.Assert(dag.NewVersionID, Equals, dvid.VersionLocalID(1))
	c.Assert(dag.Nodes, HasLen, 1)
	c.Assert(dag.VersionMap, HasLen, 1)
}

func (s *DataSuite) TestRepoPersistence(c *C) {
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
func (s *DataSuite) TestNewRepoDifferent(c *C) {
	root1, repoID1, err := s.service.NewRepo()
	c.Assert(err, IsNil)

	root2, repoID2, err := s.service.NewRepo()
	c.Assert(err, IsNil)

	c.Assert(repoID1, Not(Equals), repoID2)
	c.Assert(root1, Not(Equals), root2)
}
