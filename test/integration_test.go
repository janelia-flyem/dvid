/*
	The test package tests a variety of data types in an integrated fashion.
*/
package test

import (
	"testing"
	. "github.com/janelia-flyem/go/gocheck"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/labels64"
	_ "github.com/janelia-flyem/dvid/datatype/multichan16"
	_ "github.com/janelia-flyem/dvid/datatype/multiscale2d"
	_ "github.com/janelia-flyem/dvid/datatype/voxels"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct {
	dir     string
	service *server.Service
	head    dvid.UUID
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (suite *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	suite.dir = c.MkDir()

	// Create a new datastore.
	err := datastore.Init(suite.dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Open the datastore
	suite.service, err = server.OpenDatastore(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TearDownSuite(c *C) {
	suite.service.Shutdown()
}

func (suite *DataSuite) TestVersionedDataOps(c *C) {
	root1, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root1, "grayscale8", "grayscale", config)
	c.Assert(err, IsNil)

	//err = suite.service.NewData(root1, "labels64", "labels", config)
	//c.Assert(err, IsNil)

	child1, err := suite.service.NewVersion(root1)
	// Should be an error because we have not locked previous node before making a child.
	c.Assert(err, NotNil)

	err = suite.service.Lock(root1)
	c.Assert(err, IsNil)

	child1, err = suite.service.NewVersion(root1)
	c.Assert(err, IsNil)

	// Add a second Dataset
	root2, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	c.Assert(root1, Not(Equals), root2)

	//err = suite.service.NewData(root2, "labels64", "labels2", config)
	//c.Assert(err, IsNil)

	err = suite.service.NewData(root2, "grayscale8", "grayscale2", config)
	c.Assert(err, IsNil)

	err = suite.service.Lock(root2)
	c.Assert(err, IsNil)

	child2, err := suite.service.NewVersion(root2)
	c.Assert(err, IsNil)

	c.Assert(child1, Not(Equals), child2)
}

// Make sure Datasets configuration persists even after shutdown.
func (suite *DataSuite) TestDatasetPersistence(c *C) {
	dir := c.MkDir()

	// Create a new datastore.
	err := datastore.Init(dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Open the datastore
	service, err := datastore.Open(dir)
	c.Assert(err, IsNil)

	root, _, err := service.NewDataset()
	c.Assert(err, IsNil)

	config := dvid.NewConfig()
	config.SetVersioned(false)

	err = service.NewData(root, "grayscale8", "node1image", config)
	c.Assert(err, IsNil)

	root, _, err = service.NewDataset()
	c.Assert(err, IsNil)

	err = service.NewData(root, "grayscale8", "node2image", config)
	c.Assert(err, IsNil)

	err = service.NewData(root, "multichan16", "node2multichan", config)
	c.Assert(err, IsNil)

	err = service.NewData(root, "labels64", "node2labels64", config)
	c.Assert(err, IsNil)

	err = service.NewData(root, "rgba8", "node2rgba8", config)
	c.Assert(err, IsNil)

	oldJSON, err := service.DatasetsAllJSON()
	c.Assert(err, IsNil)

	service.Shutdown()

	// Open using different service
	service2, err := datastore.Open(dir)
	c.Assert(err, IsNil)

	newJSON, err := service2.DatasetsAllJSON()
	c.Assert(err, IsNil)

	c.Assert(newJSON, DeepEquals, oldJSON)
}
