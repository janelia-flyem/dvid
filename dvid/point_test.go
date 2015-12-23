package dvid

import (
	"reflect"
	"testing"

	. "github.com/janelia-flyem/go/gocheck"
)

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

	c.Assert(a.String(), Equals, "dvid.Point3d{10,21,837821}")

	result = a.AddScalar(10)
	c.Assert(result, Equals, Point3d{20, 31, 837831})

	result, _ = a.Max(b)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})
	result, _ = b.Max(a)
	c.Assert(result, Equals, Point3d{78312, 21, 837821})

	result, _ = a.Min(b)
	c.Assert(result, Equals, Point3d{10, -200, 40123})
	result, _ = b.Min(a)
	c.Assert(result, Equals, Point3d{10, -200, 40123})

	a = Point3d{123, 8191, 32001}
	b = Point3d{2980, 617, 99}
	result = a.Add(b)
	c.Assert(result.Value(0), Equals, a[0]+b[0])
	c.Assert(result.Value(1), Equals, a[1]+b[1])
	c.Assert(result.Value(2), Equals, a[2]+b[2])

	result = a.Sub(b)
	c.Assert(result.Value(0), Equals, a[0]-b[0])
	c.Assert(result.Value(1), Equals, a[1]-b[1])
	c.Assert(result.Value(2), Equals, a[2]-b[2])

	c.Assert(a.String(), Equals, "dvid.Point3d{123,8191,32001}")

	result, _ = a.Max(b)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})
	result, _ = b.Max(a)
	c.Assert(result, Equals, Point3d{2980, 8191, 32001})

	result, _ = a.Min(b)
	c.Assert(result, Equals, Point3d{123, 617, 99})
	result, _ = b.Min(a)
	c.Assert(result, Equals, Point3d{123, 617, 99})
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

	d = PointNd{111, -213, 678}
	blockSize := PointNd{20, 30, 40}
	g := d.Chunk(blockSize)
	c.Assert(g, DeepEquals, ChunkPointNd{5, -8, 16})

	d = PointNd{111, 213, 680}
	g = d.Chunk(blockSize)
	c.Assert(g, DeepEquals, ChunkPointNd{5, 7, 17})

	d = PointNd{111, 213, 678}
	blockSize = PointNd{20, 30, 1}
	g = d.Chunk(blockSize)
	c.Assert(g, DeepEquals, ChunkPointNd{5, 7, 678})

	result = d.PointInChunk(blockSize)
	c.Assert(result, DeepEquals, PointNd{11, 3, 0})
}

func (s *DataSuite) TestChunk(c *C) {
	d := Point3d{111, -213, 671}
	blockSize := Point3d{20, 30, 40}
	g := d.Chunk(blockSize)
	c.Assert(g, Equals, ChunkPoint3d{5, -8, 16})

	chunkPt := d.Chunk(blockSize)
	minVoxelPt := chunkPt.(ChunkPoint3d).MinPoint(blockSize)
	maxVoxelPt := chunkPt.(ChunkPoint3d).MaxPoint(blockSize)
	for dim := uint8(0); dim < uint8(3); dim++ {
		if d.Value(dim) < minVoxelPt.Value(dim) || d.Value(dim) > maxVoxelPt.Value(dim) {
			c.Errorf("Original voxel pt dim %d (%d) is not in chunk range %d to %d\n",
				dim, d.Value(dim), minVoxelPt.Value(dim), maxVoxelPt.Value(dim))
		}
	}

	d = Point3d{111, 213, 680}
	g = d.Chunk(blockSize)
	c.Assert(g, Equals, ChunkPoint3d{5, 7, 17})

	d = Point3d{111, 213, 678}
	blockSize = Point3d{20, 30, 1}
	g = d.Chunk(blockSize)
	c.Assert(g, Equals, ChunkPoint3d{5, 7, 678})

	result := d.PointInChunk(blockSize)
	c.Assert(result, Equals, Point3d{11, 3, 0})
}

var (
	testSpans = Spans{
		{1, 1, 3, 8}, {1, 1, 6, 21}, {2, 4, 10, 23}, {1, 1, 21, 39}, {2, 4, 22, 28},
	}
	testSpansNorm = Spans{
		{1, 1, 3, 39},
		{2, 4, 10, 28},
	}
)

func TestSpans(t *testing.T) {
	normalized := testSpans.Normalize()
	if !reflect.DeepEqual(normalized, testSpansNorm) {
		t.Errorf("Expected normalized spans of:\n%v\nGot this:\n%v\n", testSpansNorm, normalized)
	}
}
