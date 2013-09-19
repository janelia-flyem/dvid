package datastore

import (
	. "github.com/janelia-flyem/go/gocheck"
	_ "testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func (suite *DataSuite) TestNewDAG(c *C) {
	dag := NewVersionDAG()
	c.Assert(dag.NewVersionID, Equals, dvid.LocalID(1))
	c.Assert(dag.Nodes, HasLen, 1)
	c.Assert(dag.VersionMap, HasLen, 1)
}

// Make sure Datasets configuration persists even after shutdown.
func (suite *DataSuite) TestDatasetPersistence(c *C) {
	dir := c.MkDir()

	// Create a new datastore.
	err := Init(dir, true)
	c.Assert(err, IsNil)

	// Open the datastore
	service, err := Open(dir)
	c.Assert(err, IsNil)

	_, _, err = service.NewDataset()
	c.Assert(err, IsNil)

	_, _, err = service.NewDataset()
	c.Assert(err, IsNil)

	oldJSON, err := service.DatasetsJSON()
	c.Assert(err, IsNil)

	service.Shutdown()

	// Open using different service
	service2, err := Open(dir)
	c.Assert(err, IsNil)

	newJSON, err := service2.DatasetsJSON()
	c.Assert(err, IsNil)

	c.Assert(service2.datasets.Datasets, HasLen, 2)

	c.Assert(newJSON, DeepEquals, oldJSON)
}
