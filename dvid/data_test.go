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

	result, _ = a.Max(b)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})
	result, _ = b.Max(a)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})

	result, _ = a.Min(b)
	c.Assert(result, Equals, Point3d{10, -200, 40123})
	result, _ = b.Min(a)
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

func (s *DataSuite) TestPointNd(c *C) {
	a := PointNd{10, 21, 837821, 100}
	b := PointNd{78312, -200, 40123, -100}
	result := a.Add(b)
	c.Assert(result.Value(0), Equals, a[0]+b[0])
	c.Assert(result.Value(1), Equals, a[1]+b[1])
	c.Assert(result.Value(2), Equals, a[2]+b[2])
	c.Assert(result.Value(3), Equals, a[3]+b[3])

	result = a.Sub(b)
	c.Assert(result.Value(0), Equals, a[0]-b[0])
	c.Assert(result.Value(1), Equals, a[1]-b[1])
	c.Assert(result.Value(2), Equals, a[2]-b[2])
	c.Assert(result.Value(3), Equals, a[3]-b[3])

	result = a.Mod(b)
	c.Assert(result.Value(0), Equals, a[0]%b[0])
	c.Assert(result.Value(1), Equals, a[1]%b[1])
	c.Assert(result.Value(2), Equals, a[2]%b[2])
	c.Assert(result.Value(3), Equals, a[3]%b[3])

	result = a.Div(b)
	c.Assert(result.Value(0), Equals, a[0]/b[0])
	c.Assert(result.Value(1), Equals, a[1]/b[1])
	c.Assert(result.Value(2), Equals, a[2]/b[2])
	c.Assert(result.Value(3), Equals, a[3]/b[3])

	d := PointNd{1, 1, 1, 8}
	e := PointNd{4, 4, 4, -2}
	dist := d.Distance(e)
	c.Assert(dist, Equals, int32(11))

	c.Assert(a.String(), Equals, "(10,21,837821,100)")

	result, _ = a.Max(b)
	c.Assert(result, DeepEquals, PointNd{78312, 21, 837821, 100})
	result, _ = b.Max(a)
	c.Assert(result, DeepEquals, PointNd{78312, 21, 837821, 100})

	result, _ = a.Min(b)
	c.Assert(result, DeepEquals, PointNd{10, -200, 40123, -100})
	result, _ = b.Min(a)
	c.Assert(result, DeepEquals, PointNd{10, -200, 40123, -100})

	d = PointNd{111, 213, 678}
	blockSize := PointNd{20, 30, 40}
	g := d.Chunk(blockSize)
	c.Assert(g, DeepEquals, PointNd{5, 7, 16})

	d = PointNd{111, 213, 680}
	g = d.Chunk(blockSize)
	c.Assert(g, DeepEquals, PointNd{5, 7, 17})

	d = PointNd{111, 213, 678}
	blockSize = PointNd{20, 30, 1}
	g = d.Chunk(blockSize)
	c.Assert(g, DeepEquals, PointNd{5, 7, 678})

	result = d.PointInChunk(blockSize)
	c.Assert(result, DeepEquals, PointNd{11, 3, 0})
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

	result, _ = a.Max(b)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})
	result, _ = b.Max(a)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})

	result, _ = a.Min(b)
	c.Assert(result, Equals, Point3d{123, 617, 99})
	result, _ = b.Min(a)
	c.Assert(result, Equals, Point3d{123, 617, 99})

}
