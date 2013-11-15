package datastore

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct {
	dir     string
	service *Service
	head    UUID
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (suite *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	suite.dir = c.MkDir()

	// Create a new datastore.
	err := Init(suite.dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Open the datastore
	suite.service, err = Open(suite.dir)
	c.Assert(err, IsNil)
}
