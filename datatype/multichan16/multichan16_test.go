package multichan16

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
	head    datastore.UUID
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (s *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	s.dir = c.MkDir()

	// Create a new datastore.
	err := datastore.Init(s.dir, true)
	c.Assert(err, IsNil)

	// Open the datastore
	s.service, err = server.OpenDatastore(s.dir)
	c.Assert(err, IsNil)
}

func (s *DataSuite) TearDownSuite(c *C) {
	s.service.Shutdown()
}

func (s *DataSuite) TestDatasetPersistence(c *C) {
	dir := c.MkDir()

	// Create a new datastore.
	err := datastore.Init(dir, true)
	c.Assert(err, IsNil)

	// Open the datastore
	service, err := datastore.Open(dir)
	c.Assert(err, IsNil)

	root, _, err := service.NewDataset()
	c.Assert(err, IsNil)

	c.Assert(service.Lock(root), IsNil)

	child1, err := service.NewVersion(root)
	c.Assert(err, IsNil)

	_, err = service.NewVersion(root)
	c.Assert(err, IsNil)

	c.Assert(service.Lock(child1), IsNil)

	child1_1, err := service.NewVersion(child1)
	c.Assert(err, IsNil)

	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = service.NewData(child1_1, "multichan16", "test", config)
	c.Assert(err, IsNil)

	// Go into the dataset and modify the above data so we can make sure it persist.
	test := datastore.DataString("test")
	dataservice, err := service.DataService(child1_1, test)
	c.Assert(err, IsNil)

	mchan := dataservice.(*Data)
	mchan.NumChannels = 18

	oldJSON, err := service.DatasetsAllJSON()
	c.Assert(err, IsNil)

	err = service.SaveDataset(child1_1)
	c.Assert(err, IsNil)

	service.Shutdown()

	// Open using different service
	service2, err := datastore.Open(dir)
	c.Assert(err, IsNil)

	newJSON, err := service2.DatasetsAllJSON()
	c.Assert(err, IsNil)

	c.Assert(newJSON, DeepEquals, oldJSON)
}
