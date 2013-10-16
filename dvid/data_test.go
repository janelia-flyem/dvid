package dvid

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct{}

var _ = Suite(&DataSuite{})

func (s *DataSuite) TestPoint3d(c *C) {
	a := Point3d{10, 21, 837821}
	b := Point3d{78312, -200, 40123}
	result := a.Add(b)
	c.Assert(result.Value(0), Equals, a[0]+b[0])
	c.Assert(result.Value(1), Equals, a[1]+b[1])
	c.Assert(result.Value(2), Equals, a[2]+b[2])

	result = a.Sub(b)
	c.Assert(result.Value(0), Equals, a[0]-b[0])
	c.Assert(result.Value(1), Equals, a[1]-b[1])
	c.Assert(result.Value(2), Equals, a[2]-b[2])

	result = a.Mod(b)
	c.Assert(result.Value(0), Equals, a[0]%b[0])
	c.Assert(result.Value(1), Equals, a[1]%b[1])
	c.Assert(result.Value(2), Equals, a[2]%b[2])

	result = a.Div(b)
	c.Assert(result.Value(0), Equals, a[0]/b[0])
	c.Assert(result.Value(1), Equals, a[1]/b[1])
	c.Assert(result.Value(2), Equals, a[2]/b[2])

	d := Point3d{1, 1, 1}
	e := Point3d{4, 4, 4}
	dist := d.Distance(e)
	c.Assert(dist, Equals, int32(5))

	c.Assert(a.String(), Equals, "(10,21,837821)")

	result = a.Max(b)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})
	result = b.Max(a)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})

	result = a.Min(b)
	c.Assert(result, Equals, Point3d{10, -200, 40123})
	result = b.Min(a)
	c.Assert(result, Equals, Point3d{10, -200, 40123})

	d = Point3d{111, 213, 678}
	blockSize := Point3d{20, 30, 40}
	g := d.Chunk(blockSize)
	c.Assert(g, Equals, Point3d{5, 7, 16})

	d = Point3d{111, 213, 680}
	g = d.Chunk(blockSize)
	c.Assert(g, Equals, Point3d{5, 7, 17})

	d = Point3d{111, 213, 678}
	blockSize = Point3d{20, 30, 1}
	g = d.Chunk(blockSize)
	c.Assert(g, Equals, Point3d{5, 7, 678})

	result = d.PointInChunk(blockSize)
	c.Assert(result, Equals, Point3d{11, 3, 0})
}

func (s *DataSuite) TestChunk(c *C) {
	a := Point3d{123, 8191, 32001}
	b := Point3d{2980, 617, 99}
	result := a.Add(b)
	c.Assert(result.Value(0), Equals, a[0]+b[0])
	c.Assert(result.Value(1), Equals, a[1]+b[1])
	c.Assert(result.Value(2), Equals, a[2]+b[2])

	result = a.Sub(b)
	c.Assert(result.Value(0), Equals, a[0]-b[0])
	c.Assert(result.Value(1), Equals, a[1]-b[1])
	c.Assert(result.Value(2), Equals, a[2]-b[2])

	c.Assert(a.String(), Equals, "(123,8191,32001)")

	result = a.Max(b)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})
	result = b.Max(a)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})

	result = a.Min(b)
	c.Assert(result, Equals, Point3d{123, 617, 99})
	result = b.Min(a)
	c.Assert(result, Equals, Point3d{123, 617, 99})

}
