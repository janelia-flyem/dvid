package dvid

import (
	"testing"
	. "github.com/janelia-flyem/go/gocheck"
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

	for _, format := range []CompressionFormat{Uncompressed, Snappy, LZ4, Gzip} {
		for _, checksum := range []Checksum{NoChecksum, CRC32} {
			compression, err := NewCompression(format, DefaultCompression)
			c.Assert(err, IsNil)

			// Check simple object
			var csum Checksum
			if format == Gzip {
				csum = NoChecksum
			} else {
				csum = checksum
			}
			s, err := Serialize(stringObj, compression, csum)
			c.Assert(err, IsNil)
			if len(s) == 0 {
				c.Errorf("Bad Serialize() - output length 0")
			}

			err = Deserialize(s, &returnObj)
			c.Assert(err, IsNil)
			c.Assert(returnObj, Equals, stringObj)

			// Check more complex object
			s, err = Serialize(complexObj, compression, csum)
			c.Assert(err, IsNil)

			var returnComplexObj ComplexObj
			err = Deserialize(s, &returnComplexObj)
			c.Assert(err, IsNil)
			c.Assert(returnComplexObj, DeepEquals, complexObj)

			if csum != NoChecksum || format == Gzip {
				// Check Checksum on complex object with many bit flips.  If only one or two,
				// the gzip header might be impervious.
				for i := 0; i < len(s); i++ {
					s[i] = s[i] ^ 0x04
				}
				err = Deserialize(s, &returnComplexObj)
				c.Assert(err, NotNil, Commentf("format %s did not catch checksum error", format))
			}
		}
	}
}

func (suite *DataSuite) testUncompressed(b *testing.B, checksum Checksum) {
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
	compression, err := NewCompression(Uncompressed, DefaultCompression)
	b.Error(err)

	s, _ := Serialize(stringObj, compression, checksum)
	_ = Deserialize(s, &returnObj)

	s, _ = Serialize(complexObj, compression, checksum)
	var returnComplexObj ComplexObj
	_ = Deserialize(s, &returnComplexObj)
}

func (suite *DataSuite) BenchmarkUncompressedNoChecksum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		suite.testUncompressed(b, NoChecksum)
	}
}
