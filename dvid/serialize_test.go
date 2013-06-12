package dvid

import (
	. "launchpad.net/gocheck"
	_ "testing"
)

func (suite *MySuite) TestSerialize(c *C) {
	stringObj := "Hi there!"
	var returnObj string

	// Check uncompressed and no checksum
	s, err := Serialize(stringObj, Uncompressed, NoChecksum)
	c.Assert(err, IsNil)
	if len(s) == 0 {
		c.Errorf("Bad Serialize() - output length 0")
	}

	err = Deserialize(s, &returnObj)
	c.Assert(err, IsNil)
	c.Assert(returnObj, Equals, stringObj)

	// Check Snappy + CRC32
	s, err = Serialize(stringObj, Snappy, CRC32)
	c.Assert(err, IsNil)

	err = Deserialize(s, &returnObj)
	c.Assert(err, IsNil)
	c.Assert(returnObj, Equals, stringObj)
}
