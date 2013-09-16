package datastore

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"
)

func (suite *DataSuite) TestNewDAG(c *C) {
	dag := NewVersionDAG()
	c.Assert(dag.newID, Equals, 1)
	c.Assert(dag.nodes, HasLen, 1)
	c.Assert(dag.localMap, HasLen, 1)
}
