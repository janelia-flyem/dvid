package labelgraph

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
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

// Make sure new labelgraph data have different IDs.
func (suite *DataSuite) TestNewDataDifferent(c *C) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "labelgraph", "lg1", config)
	c.Assert(err, IsNil)

	dataservice1, err := suite.service.DataServiceByUUID(root, "lg1")
	c.Assert(err, IsNil)

	err = suite.service.NewData(root, "labelgraph", "lg2", config)
	c.Assert(err, IsNil)

	dataservice2, err := suite.service.DataServiceByUUID(root, "lg2")
	c.Assert(err, IsNil)

	data1, ok := dataservice1.(*Data)
	c.Assert(ok, Equals, true)

	data2, ok := dataservice2.(*Data)
	c.Assert(ok, Equals, true)

	c.Assert(data1.DsetID, Equals, data2.DsetID)
	c.Assert(data1.ID, Not(Equals), data2.ID)
}
