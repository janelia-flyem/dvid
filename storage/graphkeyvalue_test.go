// +build graphkeyvalue

package storage

import (
	. "github.com/janelia-flyem/go/gocheck"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage/local"
)

type GDataSuite struct {
	dir string
	db  Engine
	gdb Engine
}

var _ = Suite(&GDataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (s *GDataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	s.dir = c.MkDir()

	// Create a new storage engine.
	db, err := local.NewKeyValueStore(s.dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Initialize the graph backend database
	gengine, err := NewGraphStore(s.dir, true, dvid.Config{}, db.(OrderedKeyValueDB))
	c.Assert(err, IsNil)

	s.db = db
	s.gdb = gengine
}

func (s *GDataSuite) TearDownSuite(c *C) {
	s.gdb.Close()
	s.db.Close()
}

func (s *GDataSuite) TestBasicGraph(c *C) {
	graphDB, ok := s.gdb.(GraphDB)
	if !ok {
		c.Fail()
	}

	graphKey := NewKey("graph")
	err := graphDB.CreateGraph(graphKey)
	c.Assert(err, IsNil)

	err = graphDB.AddVertex(graphKey, 1, 5)
	c.Assert(err, IsNil)

	err = graphDB.AddVertex(graphKey, 2, 11)
	c.Assert(err, IsNil)

	err = graphDB.AddEdge(graphKey, 1, 2, 0.3)
	c.Assert(err, IsNil)

	vert1, err := graphDB.GetVertex(graphKey, 1)
	c.Assert(err, IsNil)
	c.Assert(vert1.Weight, Equals, float64(5))

	vert2, err := graphDB.GetVertex(graphKey, 2)
	c.Assert(err, IsNil)
	c.Assert(vert2.Weight, Equals, float64(11))

	edge, err := graphDB.GetEdge(graphKey, 1, 2)
	c.Assert(err, IsNil)
	c.Assert(edge.Weight, Equals, float64(0.3))

	edge, err = graphDB.GetEdge(graphKey, 2, 1)
	c.Assert(err, IsNil)
	c.Assert(edge.Weight, Equals, float64(0.3))

	err = graphDB.RemoveGraph(graphKey)
	c.Assert(err, IsNil)
}
