/*
	The test package allows testing of all supplied data types and the datastore.
	Because data types are initialized via module inits, we cannot test them within
	the datastore package without dependency cycles.
*/
package test

import (
	. "launchpad.net/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	_ "github.com/janelia-flyem/dvid/server"

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
	service *datastore.Service
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
	suite.service, err = datastore.Open(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TestNewDataset(c *C) {
	err := suite.service.NewDataset("grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)
}
