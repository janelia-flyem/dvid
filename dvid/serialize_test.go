package dvid

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"
)

func (s *DataSuite) TestLocalID(c *C) {
	id := LocalID(41)
	b := id.Bytes()
	id2, length := LocalIDFromBytes(b)

	c.Assert(id, Equals, id2)
	c.Assert(length, Equals, LocalIDSize)
}

func (suite *DataSuite) TestSerialization(c *C) {
	stringObj := "Hi there!"
	var returnObj string

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

	for _, compression := range []Compression{Uncompressed, Snappy, LZ4} {
		for _, checksum := range []Checksum{NoChecksum, CRC32} {
			// Check simple object
			s, err := Serialize(stringObj, compression, checksum)
			c.Assert(err, IsNil)
			if len(s) == 0 {
				c.Errorf("Bad Serialize() - output length 0")
			}

			err = Deserialize(s, &returnObj)
			c.Assert(err, IsNil)
			c.Assert(returnObj, Equals, stringObj)

			// Check more complex object
			s, err = Serialize(complexObj, compression, checksum)
			c.Assert(err, IsNil)

			var returnComplexObj ComplexObj
			err = Deserialize(s, &returnComplexObj)
			c.Assert(err, IsNil)
			c.Assert(returnComplexObj, DeepEquals, complexObj)

			if checksum != NoChecksum {
				// Check Checksum on complex object
				s[5] = s[5] ^ 0x04 // Flip a bit
				err = Deserialize(s, &returnComplexObj)
				c.Assert(err, NotNil)
			}
		}
	}
}

func (suite *DataSuite) testUncompressed(checksum Checksum) {
	stringObj := "Hi there!"
	var returnObj string

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

	s, _ := Serialize(stringObj, Uncompressed, checksum)
	_ = Deserialize(s, &returnObj)

	s, _ = Serialize(complexObj, Uncompressed, checksum)
	var returnComplexObj ComplexObj
	_ = Deserialize(s, &returnComplexObj)
}

func (suite *DataSuite) BenchmarkUncompressedNoChecksum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		suite.testUncompressed(NoChecksum)
	}
}
