package dvid

import (
	. "github.com/DocSavage/gocheck"
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

	// Check more complex object
	type ComplexObj struct {
		Title string
		MyMap map[interface{}]interface{}
	}
	complexObj := ComplexObj{
		Title: "my complex object",
		MyMap: map[interface{}]interface{}{
			42:           []byte("here's another string"),
			"some index": []byte{'\x33', '\x18', '\xD0', '\x92', '\x01'},
			32.1:         []string{"It's ", "amazing", " what ", "we", " put ", "here"},
		},
	}
	s, err = Serialize(complexObj, Snappy, CRC32)
	c.Assert(err, IsNil)

	var returnComplexObj ComplexObj
	err = Deserialize(s, &returnComplexObj)
	c.Assert(err, IsNil)
	c.Assert(returnComplexObj, DeepEquals, complexObj)

	// Check Checksum on complex object
	s[5] = s[5] ^ 0x04 // Flip a bit
	err = Deserialize(s, &returnComplexObj)
	c.Assert(err, NotNil)
}
