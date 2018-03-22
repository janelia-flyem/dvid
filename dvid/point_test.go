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

	c.Assert(a.String(), Equals, "(10,21,837821)")

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
	s := Spans{
		{20, 20, 15, 28}, // 14 voxels
		{20, 21, 14, 30}, // 17 voxels
		{21, 23, 16, 74}, // 16 voxels + 32 voxels + 11 voxels
		{22, 23, 30, 60}, // 2 voxels (0, 0, 0) + 29 voxels (1, 0, 0)
		{48, 30, 40, 65}, // 24 voxels (1, 0, 1) + 2 voxels (2, 0, 1)
	}
	counts := s.VoxelCounts(Point3d{32, 32, 32})
	if len(counts) != 5 {
		t.Errorf("expected VoxelCounts for 5 blocks got: %v\n", counts)
	}
	bcoord := ChunkPoint3d{0, 0, 0}.ToIZYXString()
	if counts[bcoord] != 49 {
		t.Errorf("expected # voxels for %s to be 50, got %d voxels\n", bcoord, counts[bcoord])
	}
	bcoord = ChunkPoint3d{1, 0, 0}.ToIZYXString()
	if counts[bcoord] != 61 {
		t.Errorf("expected # voxels for %s to be 61, got %d voxels\n", bcoord, counts[bcoord])
	}
	bcoord = ChunkPoint3d{2, 0, 0}.ToIZYXString()
	if counts[bcoord] != 11 {
		t.Errorf("expected # voxels for %s to be 11, got %d voxels\n", bcoord, counts[bcoord])
	}
	bcoord = ChunkPoint3d{1, 0, 1}.ToIZYXString()
	if counts[bcoord] != 24 {
		t.Errorf("expected # voxels for %s to be 61, got %d voxels\n", bcoord, counts[bcoord])
	}
	bcoord = ChunkPoint3d{2, 0, 1}.ToIZYXString()
	if counts[bcoord] != 2 {
		t.Errorf("expected # voxels for %s to be 2, got %d voxels\n", bcoord, counts[bcoord])
	}
}

func TestTileExtents(t *testing.T) {
	ext, err := GetTileExtents(ChunkPoint3d{11, 15, 31}, XY, Point3d{32, 32, 32})
	if err != nil {
		t.Error(err)
	}
	if !ext.MinPoint.Equals(Point3d{352, 480, 31}) {
		t.Errorf("bad GetTileExtents compute of MinPoint, got %v\n", ext.MinPoint)
	}
	if !ext.MaxPoint.Equals(Point3d{383, 511, 31}) {
		t.Errorf("bad GetTileExtents compute of MaxPoint, got %v\n", ext.MaxPoint)
	}

	ext, err = GetTileExtents(ChunkPoint3d{11, 15, 31}, XZ, Point3d{32, 32, 32})
	if err != nil {
		t.Error(err)
	}
	if !ext.MinPoint.Equals(Point3d{352, 15, 992}) {
		t.Errorf("bad GetTileExtents compute of MinPoint, got %v\n", ext.MinPoint)
	}
	if !ext.MaxPoint.Equals(Point3d{383, 15, 1023}) {
		t.Errorf("bad GetTileExtents compute of MaxPoint, got %v\n", ext.MaxPoint)
	}

	ext, err = GetTileExtents(ChunkPoint3d{11, 15, 31}, YZ, Point3d{32, 32, 32})
	if err != nil {
		t.Error(err)
	}
	if !ext.MinPoint.Equals(Point3d{11, 480, 992}) {
		t.Errorf("bad GetTileExtents compute of MinPoint, got %v\n", ext.MinPoint)
	}
	if !ext.MaxPoint.Equals(Point3d{11, 511, 1023}) {
		t.Errorf("bad GetTileExtents compute of MaxPoint, got %v\n", ext.MaxPoint)
	}
}
