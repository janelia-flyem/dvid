package datastore

import (
	. "github.com/DocSavage/gocheck"
	"testing"
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
	suite.head = Init(suite.dir, true)
	if len(suite.head) == 0 {
		c.Errorf("Initialization of test datastore resulted in zero length UUID")
	} else {
		c.Logf("Test datastore initialized with head UUID = %s\n", suite.head)
	}

	// Open the datastore
	var err error
	suite.service, err = Open(suite.dir)
	c.Assert(err, IsNil)
}
