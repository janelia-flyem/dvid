package keyvalue

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

// Make sure new keyvalue data have different IDs.
func (suite *DataSuite) TestNewDataDifferent(c *C) {
	// Create a new repo
	root, _, err := suite.service.NewRepo()
	c.Assert(err, IsNil)

	// Add data
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "keyvalue", "kv1", config)
	c.Assert(err, IsNil)

	dataservice1, err := suite.service.DataServiceByUUID(root, "kv1")
	c.Assert(err, IsNil)

	err = suite.service.NewData(root, "keyvalue", "kv2", config)
	c.Assert(err, IsNil)

	dataservice2, err := suite.service.DataServiceByUUID(root, "kv2")
	c.Assert(err, IsNil)

	data1, ok := dataservice1.(*Data)
	c.Assert(ok, Equals, true)

	data2, ok := dataservice2.(*Data)
	c.Assert(ok, Equals, true)

	c.Assert(data1.DsetID, Equals, data2.DsetID)
	c.Assert(data1.ID, Not(Equals), data2.ID)
}

func (suite *DataSuite) TestRoundTrip(c *C) {
	root, _, err := suite.service.NewRepo()
	c.Assert(err, IsNil)

	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "keyvalue", "kv", config)
	c.Assert(err, IsNil)

	kvservice, err := suite.service.DataServiceByUUID(root, "kv")
	c.Assert(err, IsNil)

	kvdata, ok := kvservice.(*Data)
	if !ok {
		c.Errorf("Can't cast keyservice data service into Data\n")
	}

	keyStr := "testkey"
	value := []byte("I like Japan and this is some unicode: \u65e5\u672c\u8a9e")

	err = kvdata.PutData(root, keyStr, value)
	c.Assert(err, IsNil)

	retrieved, found, err := kvdata.GetData(root, keyStr)
	c.Assert(err, IsNil)
	c.Assert(found, Equals, true)

	c.Assert(retrieved, DeepEquals, value)
}
