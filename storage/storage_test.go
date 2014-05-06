package storage

import (
	"fmt"
	"testing"

	. "github.com/janelia-flyem/go/gocheck"

	"github.com/janelia-flyem/dvid/dvid"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct {
	dir string
	db  Engine
	gdb Engine
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (s *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	s.dir = c.MkDir()

	// Create a new storage engine.
	db, err := NewStore(s.dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Initialize the graph backend database
	gengine, err := NewGraphStore(s.dir, true, dvid.Config{}, db.(OrderedKeyValueDB))
	c.Assert(err, IsNil)

	s.db = db
	s.gdb = gengine
}

func (s *DataSuite) TearDownSuite(c *C) {
	s.gdb.Close()
	s.db.Close()
}

// Implement a test Key
type TestKey []byte

const TestKeyType = 1

func NewKey(s string) TestKey {
	return []byte(s)
}

func (k TestKey) KeyType() KeyType {
	return TestKeyType
}

func (k TestKey) BytesToKey(b []byte) (Key, error) {
	return TestKey(b), nil
}

func (k TestKey) Bytes() []byte {
	return []byte(k)
}

func (k TestKey) BytesString() string {
	return string(k)
}

func (k TestKey) String() string {
	return fmt.Sprintf("%x", string(k))
}

func (s *DataSuite) TestSingleItem(c *C) {
	kvDB, ok := s.db.(OrderedKeyValueDB)
	if !ok {
		c.Fail()
	}

	value, err := kvDB.Get(NewKey("some key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	err = kvDB.Put(NewKey("some key"), []byte("some value"))
	c.Assert(err, IsNil)

	value, err = kvDB.Get(NewKey("not my key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	value, err = kvDB.Get(NewKey("some key"))
	c.Assert(err, IsNil)
	c.Assert(string(value), Equals, "some value")
}

func (s *DataSuite) TestDeleteItem(c *C) {
	kvDB, ok := s.db.(OrderedKeyValueDB)
	if !ok {
		c.Fail()
	}

	value, err := kvDB.Get(NewKey("some key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	err = kvDB.Put(NewKey("some key"), []byte("some value"))
	c.Assert(err, IsNil)

	value, err = kvDB.Get(NewKey("some key"))
	c.Assert(err, IsNil)
	c.Assert(string(value), Equals, "some value")

	err = kvDB.Delete(NewKey("some key"))
	c.Assert(err, IsNil)

	value, err = kvDB.Get(NewKey("some key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

}

func (s *DataSuite) TestMultipleItems(c *C) {
	kvDB, ok := s.db.(OrderedKeyValueDB)
	if !ok {
		c.Fail()
	}

	items := []KeyValue{
		{K: NewKey("key a"), V: []byte("some value A")},
		{K: NewKey("key b"), V: []byte("some value B")},
		{K: NewKey("yet another key C"), V: []byte("some larger value for key C")},
	}

	value, err := kvDB.Get(NewKey("key a"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	err = kvDB.PutRange(items)
	c.Assert(err, IsNil)

	value, err = kvDB.Get(NewKey("not my key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	value, err = kvDB.Get(NewKey("key a"))
	c.Assert(err, IsNil)
	c.Assert(string(value), Equals, "some value A")

	values, err := kvDB.GetRange(NewKey("key a"), NewKey("yet another key F"))
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)
	for i, kv := range values {
		c.Assert(string(kv.V), Equals, string(items[i].V))
	}
}

func (s *DataSuite) TestBasicGraph(c *C) {
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
