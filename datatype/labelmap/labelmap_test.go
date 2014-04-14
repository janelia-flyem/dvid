package labelmap

import (
	"testing"
	. "github.com/janelia-flyem/go/gocheck"

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

	mylabels datastore.DataService
	lmap     datastore.DataService
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

	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "labels64", "mylabels", config)
	c.Assert(err, IsNil)

	suite.mylabels, err = suite.service.DataServiceByUUID(root, "mylabels")
	c.Assert(err, IsNil)

	config.Set("Labels", "mylabels")
	err = suite.service.NewData(root, "labelmap", "lmap", config)
	c.Assert(err, IsNil)

	suite.lmap, err = suite.service.DataServiceByUUID(root, "lmap")
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TearDownSuite(c *C) {
	suite.service.Shutdown()
}

// Make sure the binary serializations are OK.
func (suite *DataSuite) TestSerialization(c *C) {
	data, ok := suite.lmap.(*Data)
	c.Assert(ok, Equals, true)

	// Test Labels reference
	b, err := data.Labels.MarshalBinary()
	c.Assert(err, IsNil)
	if len(b) == 0 {
		c.Fail()
	}

	var ref LabelsRef
	err = ref.UnmarshalBinary(b)
	c.Assert(err, IsNil)
	c.Assert(ref.name, Equals, dvid.DataString("mylabels"))
}
