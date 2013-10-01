package datastore

import (
	. "github.com/janelia-flyem/go/gocheck"
	_ "testing"
)

func (suite *DataSuite) TestNewDAG(c *C) {
	dag := NewVersionDAG()
	c.Assert(dag.NewVersionID, Equals, VersionLocalID(1))
	c.Assert(dag.Nodes, HasLen, 1)
	c.Assert(dag.VersionMap, HasLen, 1)
}
