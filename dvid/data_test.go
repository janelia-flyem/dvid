package dvid

import (
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestVoxelCoord(c *C) {
	a := VoxelCoord{10, 21, 837821}
	b := VoxelCoord{78312, -200, 40123}
	result := a.Add(b)
	c.Assert(result[0], Equals, a[0]+b[0])
	c.Assert(result[1], Equals, a[1]+b[1])
	c.Assert(result[2], Equals, a[2]+b[2])

	result = a.Sub(b)
	c.Assert(result[0], Equals, a[0]-b[0])
	c.Assert(result[1], Equals, a[1]-b[1])
	c.Assert(result[2], Equals, a[2]-b[2])

	result = a.Mod(b)
	c.Assert(result[0], Equals, a[0]%b[0])
	c.Assert(result[1], Equals, a[1]%b[1])
	c.Assert(result[2], Equals, a[2]%b[2])

	result = a.Div(b)
	c.Assert(result[0], Equals, a[0]/b[0])
	c.Assert(result[1], Equals, a[1]/b[1])
	c.Assert(result[2], Equals, a[2]/b[2])

	d := VoxelCoord{1, 1, 1}
	e := VoxelCoord{4, 4, 4}
	dist := d.Distance(e)
	c.Assert(dist, Equals, int32(5))

	c.Assert(a.String(), Equals, "(10,21,837821)")

	result = a.Max(b)
	c.Assert(result, Equals, VoxelCoord{78312, 21, 837821})
	result = b.Max(a)
	c.Assert(result, Equals, VoxelCoord{78312, 21, 837821})

	result = a.Min(b)
	c.Assert(result, Equals, VoxelCoord{10, -200, 40123})
	result = b.Min(a)
	c.Assert(result, Equals, VoxelCoord{10, -200, 40123})

	d = VoxelCoord{111, 213, 678}
	blockSize := Point3d{20, 30, 40}
	g := d.BlockCoord(blockSize)
	c.Assert(g, Equals, BlockCoord{5, 7, 16})

	d = VoxelCoord{111, 213, 680}
	g = d.BlockCoord(blockSize)
	c.Assert(g, Equals, BlockCoord{5, 7, 17})

	d = VoxelCoord{111, 213, 678}
	blockSize = Point3d{20, 30, 1}
	g = d.BlockCoord(blockSize)
	c.Assert(g, Equals, BlockCoord{5, 7, 678})

	result = d.BlockVoxel(blockSize)
	c.Assert(result, Equals, VoxelCoord{11, 3, 0})
}

func (s *MySuite) TestBlockCoord(c *C) {
	a := BlockCoord{123, 8191, 32001}
	b := BlockCoord{2980, 617, 99}
	result := a.Add(b)
	c.Assert(result[0], Equals, a[0]+b[0])
	c.Assert(result[1], Equals, a[1]+b[1])
	c.Assert(result[2], Equals, a[2]+b[2])

	result = a.Sub(b)
	c.Assert(result[0], Equals, a[0]-b[0])
	c.Assert(result[1], Equals, a[1]-b[1])
	c.Assert(result[2], Equals, a[2]-b[2])

	c.Assert(a.String(), Equals, "(123,8191,32001)")

	result = a.Max(b)
	c.Assert(result, Equals, BlockCoord{2980, 8191, 32001})
	result = b.Max(a)
	c.Assert(result, Equals, BlockCoord{2980, 8191, 32001})

	result = a.Min(b)
	c.Assert(result, Equals, BlockCoord{123, 617, 99})
	result = b.Min(a)
	c.Assert(result, Equals, BlockCoord{123, 617, 99})

}
