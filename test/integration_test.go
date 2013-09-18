/*
	The test package allows testing of all supplied data types and the datastore.
	Because data types are initialized via module inits, we cannot test them within
	the datastore package without dependency cycles.
*/
package test

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
	_ "github.com/janelia-flyem/dvid/datatype/labels32"
	_ "github.com/janelia-flyem/dvid/datatype/labels64"
	_ "github.com/janelia-flyem/dvid/datatype/rgba8"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct {
	dir     string
	service *server.Service
	head    datastore.UUID
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (suite *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	suite.dir = c.MkDir()

	// Create a new datastore.
	err := datastore.Init(suite.dir, true)
	c.Assert(err, IsNil)

	// Open the datastore
	suite.service, err = server.OpenDatastore(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TearDownSuite(c *C) {
	suite.service.Shutdown()
}

func (suite *DataSuite) TestVersionedDataOps(c *C) {
	dataset1, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	config := dvid.Config{}
	config["versioned"] = true

	err = dataset1.NewData(dataset1.Root, "grayscale", "grayscale8", config)
	c.Assert(err, IsNil)

	err = dataset1.NewData(dataset1.Root, "labels", "labels64", config)
	c.Assert(err, IsNil)

	child1, err := dataset1.NewChild(dataset1.Root)
	// Should be an error because we have not locked previous node before making a child.
	c.Assert(err, NotNil)

	err = dataset1.Lock(dataset1.Root)
	c.Assert(err, IsNil)

	child1, err = dataset1.NewChild(dataset1.Root)
	c.Assert(err, IsNil)

	// Add a second Dataset
	dataset2, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	root2 := dataset2.RootUUID()
	c.Assert(dataset1.Root, Not(Equals), root2)

	err = dataset2.NewData(root2, "labels2", "labels64", config)
	c.Assert(err, IsNil)

	err = dataset2.NewData(root2, "grayscale2", "grayscale8", config)
	c.Assert(err, IsNil)

	err = dataset2.Lock(root2)
	c.Assert(err, IsNil)

	child2, err := dataset2.NewChild(root2)
	c.Assert(err, IsNil)

	c.Assert(child1, Not(Equals), child2)
}
