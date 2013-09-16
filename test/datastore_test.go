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
	suite.head = datastore.Init(suite.dir, true)
	if len(suite.head) == 0 {
		c.Errorf("Initialization of test datastore resulted in zero length UUID")
	} else {
		c.Logf("Test datastore initialized with head UUID = %s\n", suite.head)
	}

	// Open the datastore
	var err error
	suite.service, err = server.OpenDatastore(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TestDataOps(c *C) {
	dataset1, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	root1 := dataset1.Root()

	err = dataset1.NewData(root1, "grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)

	err = dataset1.NewData(root1, "labels", "labels64", dvid.Config{})
	c.Assert(err, IsNil)

	child1, err := dataset1.NewChild(root1)
	// Should be an error because we have not locked previous node before making a child.
	c.Assert(err, NotNil)

	err = dataset1.Lock(root1)
	c.Assert(err, IsNil)

	child1, err = dataset1.NewChild(root1)
	c.Assert(err, IsNil)

	// Add a second Dataset
	dataset2, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	root2 := dataset2.Root()
	c.Assert(root1, Not(Equals), root2)

	err = dataset2.NewData(root2, "labels2", "labels64", dvid.Config{})
	c.Assert(err, IsNil)

	err = dataset2.NewData(root2, "grayscale2", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)

	err = dataset2.Lock(root2)
	c.Assert(err, IsNil)

	child2, err = dataset2.NewChild(root2)
	c.Assert(err, IsNil)

}
