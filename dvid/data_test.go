package dvid

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestLocalID(c *C) {
	id := LocalID{41}
	b := id.Bytes()
	id2, length := LocalIDFromBytes(b)

	c.Assert(id, Equals, id2)
	c.Assert(length, Equals, sizeOfLocalID)
}
