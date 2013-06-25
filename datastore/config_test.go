package datastore

import (
	. "github.com/janelia-flyem/go/gocheck"
	_ "testing"
)

func (suite *DataSuite) TestGet(c *C) {
	var config runtimeConfig
	err := config.Get(suite.service.db)
	c.Assert(err, IsNil)
}
