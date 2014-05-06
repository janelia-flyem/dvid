package dvid

import (
	_ "testing"
	. "github.com/janelia-flyem/go/gocheck"
)

type VolumeTest struct {
	rles     RLEs
	encoding []byte
}

var _ = Suite(&VolumeTest{})

func (s *VolumeTest) SetUpSuite(c *C) {
	var rleTests = []RLE{
		{Point3d{2, 3, 4}, 20},
		{Point3d{4, 4, 4}, 14},
		{Point3d{1, 3, 5}, 20},
	}
	for _, rle := range rleTests {
		s.rles = append(s.rles, rle)
	}
	var err error
	s.encoding, err = s.rles.MarshalBinary()
	c.Assert(err, IsNil)
}

func (s *VolumeTest) TestRLE(c *C) {
	serialization, err := s.rles.MarshalBinary()
	c.Assert(err, IsNil)

	var obtained RLEs
	err = obtained.UnmarshalBinary(serialization)
	c.Assert(err, IsNil)

	for i, _ := range s.rles {
		c.Assert(s.rles[i], DeepEquals, obtained[i])
	}

	numVoxels, numRuns := obtained.Stats()
	c.Assert(numVoxels, Equals, int32(54))
	c.Assert(numRuns, Equals, int32(3))
}

func (s *VolumeTest) TestSparseVol(c *C) {
	var vol SparseVol
	err := vol.AddRLEs(s.encoding)
	c.Assert(err, IsNil)

	c.Assert(vol.Size(), Equals, Point3d{21, 2, 2})
	c.Assert(vol.MinimumPoint3d(), Equals, Point3d{1, 3, 4})
	c.Assert(vol.MaximumPoint3d(), Equals, Point3d{21, 4, 5})

	var rleTests = []RLE{
		{Point3d{32, 43, 54}, 20},
		{Point3d{34, 44, 54}, 14},
	}
	vol.Clear()
	var newrles RLEs
	for _, rle := range rleTests {
		newrles = append(newrles, rle)
	}
	encoding, err := newrles.MarshalBinary()
	c.Assert(err, IsNil)
	err = vol.AddRLEs(encoding)
	c.Assert(err, IsNil)

	c.Assert(vol.Size(), Equals, Point3d{20, 2, 1})
	c.Assert(vol.MinimumPoint3d(), Equals, Point3d{32, 43, 54})
	c.Assert(vol.MaximumPoint3d(), Equals, Point3d{51, 44, 54})
}
