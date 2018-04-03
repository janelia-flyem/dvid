package dvid

import (
	"reflect"
	"testing"

	. "github.com/janelia-flyem/go/gocheck"
)

type VolumeTest struct {
	rles     RLEs
	encoding []byte
}

var _ = Suite(&VolumeTest{})

func (s *VolumeTest) SetUpSuite(c *C) {
	s.rles = RLEs{
		{Point3d{2, 3, 4}, 20},
		{Point3d{4, 4, 4}, 14},
		{Point3d{1, 3, 5}, 20},
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
	c.Assert(numVoxels, Equals, uint64(54))
	c.Assert(numRuns, Equals, int32(3))

	toAdd := RLEs{
		{Point3d{0, 3, 4}, 14},
		{Point3d{10, 3, 4}, 14},
		{Point3d{8, 5, 7}, 13},
	}
	expectedRLEs := RLEs{
		{Point3d{0, 3, 4}, 24},
		{Point3d{4, 4, 4}, 14},
		{Point3d{1, 3, 5}, 20},
		{Point3d{8, 5, 7}, 13},
	}

	voxelsAdded := s.rles.Add(toAdd)
	c.Assert(voxelsAdded, Equals, int64(17))
	c.Assert(s.rles, DeepEquals, expectedRLEs)
}

func (s *VolumeTest) TestSparseVol(c *C) {
	var vol SparseVol
	err := vol.AddSerializedRLEs(s.encoding)
	c.Assert(err, IsNil)

	c.Assert(vol.Size(), Equals, Point3d{21, 2, 2})
	c.Assert(vol.MinimumPoint3d(), Equals, Point3d{1, 3, 4})
	c.Assert(vol.MaximumPoint3d(), Equals, Point3d{21, 4, 5})

	vol.Clear()
	newrles := RLEs{
		{Point3d{32, 43, 54}, 20},
		{Point3d{34, 44, 54}, 14},
	}
	encoding, err := newrles.MarshalBinary()
	c.Assert(err, IsNil)
	err = vol.AddSerializedRLEs(encoding)
	c.Assert(err, IsNil)

	c.Assert(vol.Size(), Equals, Point3d{20, 2, 1})
	c.Assert(vol.MinimumPoint3d(), Equals, Point3d{32, 43, 54})
	c.Assert(vol.MaximumPoint3d(), Equals, Point3d{51, 44, 54})
}

func TestNormalization(t *testing.T) {
	norm := denormRLEs.Normalize()
	if !reflect.DeepEqual(norm, expectedNorm) {
		t.Errorf("Normalization of RLEs failed.  Expected:\n%s\nGot:%s\n", expectedNorm, norm)
	}
}

func TestDownres(t *testing.T) {
	islice := IZYXSlice{
		ChunkPoint3d{1, 0, 1}.ToIZYXString(),
		ChunkPoint3d{1, 1, 1}.ToIZYXString(),
		ChunkPoint3d{3, 2, 3}.ToIZYXString(),
		ChunkPoint3d{7, 5, 9}.ToIZYXString(),
		ChunkPoint3d{9, 9, 9}.ToIZYXString(),
		ChunkPoint3d{10, 10, 10}.ToIZYXString(),
	}
	downres1, err := islice.Downres(1)
	if err != nil {
		t.Fatal(err)
	}
	expected := IZYXSlice{
		ChunkPoint3d{0, 0, 0}.ToIZYXString(),
		ChunkPoint3d{1, 1, 1}.ToIZYXString(),
		ChunkPoint3d{3, 2, 4}.ToIZYXString(),
		ChunkPoint3d{4, 4, 4}.ToIZYXString(),
		ChunkPoint3d{5, 5, 5}.ToIZYXString(),
	}
	if len(downres1) != 5 {
		t.Errorf("Downres scale 1 failed: %v vs expected %v\n", downres1, expected)
	}
	for i, izyx := range downres1 {
		if expected[i] != izyx {
			t.Errorf("Downres scale 1 failed: %v vs expected %v\n", downres1, expected)
		}
	}
	downres2, err := islice.Downres(2)
	if err != nil {
		t.Fatal(err)
	}
	expected2 := IZYXSlice{
		ChunkPoint3d{0, 0, 0}.ToIZYXString(),
		ChunkPoint3d{1, 1, 2}.ToIZYXString(),
		ChunkPoint3d{2, 2, 2}.ToIZYXString(),
	}
	if len(downres2) != 3 {
		t.Errorf("Downres scale 2 failed: %v vs expected %v\n", downres2, expected2)
	}
	for i, izyx := range downres2 {
		if expected2[i] != izyx {
			t.Errorf("Downres scale 2 failed: %v vs expected %v\n", downres2, expected2)
		}
	}
}

func TestIZYXSlice(t *testing.T) {
	islice := make(IZYXSlice, 1000)
	i := 0
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				pt := ChunkPoint3d{x, y, z}
				izyx := pt.ToIZYXString()
				islice[i] = izyx
				i++
			}
		}
	}

	// test serialization/deserialization
	ser, err := islice.MarshalBinary()
	if err != nil {
		t.Fatalf("bad IZYXSlice marshalling: %v\n", err)
	}
	var islice2 IZYXSlice
	if err = islice2.UnmarshalBinary(ser); err != nil {
		t.Fatalf("bad IZYXSlice unmarshaling: %v\n", err)
	}
	if len(islice) != len(islice2) {
		t.Fatalf("Got %d elements after deserialization, not %d elements\n", len(islice2), len(islice))
	}
	for i, izyx := range islice {
		if izyx != islice2[i] {
			t.Fatalf("bad IZYXSlice after unmarshalling at index %d: got %s, expected %s\n", i, islice2[i], izyx)
		}
	}

	// test delete
	deletePts := []ChunkPoint3d{
		{10, 3, 2}, // this one not in islice
		{8, 5, 3},
		{4, 4, 6},
		{5, 4, 6},
		{9, 4, 6},
		{0, 5, 6},
		{7, 9, 8},
	}
	deleteSlice := make(IZYXSlice, len(deletePts))
	for i, pt := range deletePts {
		deleteSlice[i] = pt.ToIZYXString()
	}

	origLen := len(islice)
	islice.Delete(deleteSlice)

	if len(islice) != origLen-len(deletePts)+1 {
		t.Fatalf("IZYXSlice.Delete should have resulted in %d size, got %d instead\n", origLen-len(deletePts)+1, len(islice))
	}
	dpos := 0
	ipos := 0
	deletePt := deletePts[dpos]
	deleteIZYX := deletePt.ToIZYXString()
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				pt := ChunkPoint3d{x, y, z}
				izyx := pt.ToIZYXString()
				for deleteIZYX < izyx {
					dpos++
					if dpos < len(deletePts) {
						deletePt = deletePts[dpos]
						deleteIZYX = deletePt.ToIZYXString()
					} else {
						break
					}
				}
				if pt.Equals(deletePt) {
					continue
				}
				if islice[ipos] != izyx {
					t.Fatalf("Expected %s, got %s in IZYXSlice in position %d\n", izyx, islice[ipos], ipos)
				}
				ipos++
			}
		}
	}

	// test merge of deleted pts back in.
	copy(islice2[:len(islice)], islice)
	islice2 = islice2[:len(islice)]
	islice.Merge(deleteSlice[1:])
	if len(islice) != origLen {
		t.Fatalf("After merge, expected %d elements, got %d instead\n", origLen, len(islice))
	}
	ipos = 0
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				pt := ChunkPoint3d{x, y, z}
				izyx := pt.ToIZYXString()
				if islice[ipos] != izyx {
					t.Fatalf("Expected %s in position (%d,%d,%d), got %s instead\n", izyx, x, y, z, islice[ipos])
				}
				ipos++
			}
		}
	}

	islice3 := islice2.MergeCopy(deleteSlice[1:])
	if len(islice3) != origLen {
		t.Fatalf("After merge, expected %d elements, got %d instead\n", origLen, len(islice3))
	}
	ipos = 0
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				pt := ChunkPoint3d{x, y, z}
				izyx := pt.ToIZYXString()
				if islice3[ipos] != izyx {
					t.Fatalf("Expected %s in position (%d,%d,%d), got %s instead\n", izyx, x, y, z, islice3[ipos])
				}
				ipos++
			}
		}
	}

	// test split
	a := IZYXSlice{
		ChunkPoint3d{2, 1, 1}.ToIZYXString(), ChunkPoint3d{2, 1, 2}.ToIZYXString(),
	}
	b := IZYXSlice{
		ChunkPoint3d{2, 1, 2}.ToIZYXString(),
	}
	c, err := a.Split(b)
	if err != nil {
		t.Fatalf("error on simple IZYXSlice.Split: %v\n", err)
	}
	if len(c) != 1 {
		t.Fatalf("bad result of split: %s\n", c)
	}

	origLen = len(islice3)
	split, err := islice3.Split(deleteSlice[1:])
	if err != nil {
		t.Fatalf("error on IZYXSlice split: %v\n", err)
	}
	if len(split) != origLen-len(deletePts)+1 {
		t.Fatalf("IZYXSlice.Split should have resulted in %d size, got %d instead\n", origLen-len(deletePts)+1, len(split))
	}
	dpos = 1
	ipos = 0
	deletePt = deletePts[dpos]
	deleteIZYX = deletePt.ToIZYXString()
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				pt := ChunkPoint3d{x, y, z}
				izyx := pt.ToIZYXString()
				for deleteIZYX < izyx {
					dpos++
					if dpos < len(deletePts) {
						deletePt = deletePts[dpos]
						deleteIZYX = deletePt.ToIZYXString()
					} else {
						break
					}
				}
				if pt.Equals(deletePt) {
					continue
				}
				if split[ipos] != izyx {
					t.Fatalf("Expected %s, got %s in IZYXSlice in position %s\n", izyx, split[ipos], izyx)
				}
				ipos++
			}
		}
	}
}

var (
	denormRLEs = RLEs{
		NewRLE(Point3d{20, 30, 40}, 10),
		NewRLE(Point3d{30, 30, 40}, 2),
		NewRLE(Point3d{32, 30, 40}, 3),
		NewRLE(Point3d{38, 30, 40}, 7),
		NewRLE(Point3d{45, 30, 41}, 4),
		NewRLE(Point3d{17, 30, 42}, 1),
		NewRLE(Point3d{18, 30, 42}, 5),
	}
	expectedNorm = RLEs{
		NewRLE(Point3d{20, 30, 40}, 15),
		NewRLE(Point3d{38, 30, 40}, 7),
		NewRLE(Point3d{45, 30, 41}, 4),
		NewRLE(Point3d{17, 30, 42}, 6),
	}
)

func TestSplitRLEs(t *testing.T) {
	modified, err := bodyRLEs.Split(splitDupRLEs)
	if err != nil {
		t.Errorf("Bad split on dup case: %v\n", err)
	}
	if len(modified) != 0 {
		t.Errorf("Expected split of duplicate should complete erase original RLEs but it did not.\n")
	}
	if len(modified) != 0 {
		t.Errorf("Expected split to give no modified RLEs, but got %d RLEs\n", len(modified))
	}

	// Test edge cases
	modified, err = bodyRLEs.Split(splitTestRLEs)
	if err != nil {
		t.Errorf("Bad split on edge cases: %v\n", err)
	}
	if len(modified) == 0 {
		t.Errorf("Expected split to say edge cases were not duplicate but got no modifications\n")
	}
	expectedNorm := expectedModRLEs.Normalize()
	if !reflect.DeepEqual(modified, expectedNorm) {
		for i, rle := range modified {
			if !reflect.DeepEqual(rle, expectedNorm[i]) {
				t.Errorf("Edge case error found.  Expected RLE at pos %d: %s, got %s\n", i, expectedNorm[i], rle)
				break
			}
		}
	}
}

var (
	splitTestRLEs = RLEs{
		NewRLE(Point3d{3902, 5007, 6594}, 1),
		NewRLE(Point3d{3904, 5007, 6594}, 1),
		NewRLE(Point3d{3912, 5007, 6594}, 1),

		NewRLE(Point3d{3869, 5018, 6594}, 3),
		NewRLE(Point3d{3872, 5018, 6594}, 2),

		NewRLE(Point3d{3886, 5018, 6594}, 9),
		NewRLE(Point3d{3905, 5018, 6594}, 1),

		NewRLE(Point3d{3912, 5018, 6594}, 3),

		NewRLE(Point3d{3936, 5018, 6594}, 2),

		NewRLE(Point3d{3866, 5019, 6594}, 6),
		NewRLE(Point3d{3872, 5019, 6594}, 2),
	}

	expectedModRLEs = RLEs{
		NewRLE(Point3d{3885, 5001, 6594}, 2),
		NewRLE(Point3d{3922, 5001, 6594}, 2),
		NewRLE(Point3d{3884, 5002, 6594}, 5),
		NewRLE(Point3d{3922, 5002, 6594}, 4),
		NewRLE(Point3d{3928, 5002, 6594}, 2),
		NewRLE(Point3d{3883, 5003, 6594}, 7),
		NewRLE(Point3d{3921, 5003, 6594}, 10),
		NewRLE(Point3d{3881, 5004, 6594}, 9),
		NewRLE(Point3d{3920, 5004, 6594}, 13),
		NewRLE(Point3d{3879, 5005, 6594}, 11),
		NewRLE(Point3d{3920, 5005, 6594}, 14),
		NewRLE(Point3d{3877, 5006, 6594}, 13),
		NewRLE(Point3d{3918, 5006, 6594}, 16),
		NewRLE(Point3d{3875, 5007, 6594}, 13),

		NewRLE(Point3d{3901, 5007, 6594}, 1),
		NewRLE(Point3d{3903, 5007, 6594}, 1),

		NewRLE(Point3d{3914, 5007, 6594}, 19),
		NewRLE(Point3d{3873, 5008, 6594}, 14),
		NewRLE(Point3d{3901, 5008, 6594}, 3),
		NewRLE(Point3d{3904, 5008, 6594}, 29),
		NewRLE(Point3d{3871, 5009, 6594}, 1),
		NewRLE(Point3d{3872, 5009, 6594}, 16),
		NewRLE(Point3d{3901, 5009, 6594}, 3),
		NewRLE(Point3d{3904, 5009, 6594}, 29),
		NewRLE(Point3d{3870, 5010, 6594}, 2),
		NewRLE(Point3d{3872, 5010, 6594}, 16),
		NewRLE(Point3d{3900, 5010, 6594}, 4),
		NewRLE(Point3d{3904, 5010, 6594}, 29),
		NewRLE(Point3d{3868, 5011, 6594}, 4),
		NewRLE(Point3d{3872, 5011, 6594}, 15),
		NewRLE(Point3d{3901, 5011, 6594}, 3),
		NewRLE(Point3d{3904, 5011, 6594}, 30),
		NewRLE(Point3d{3867, 5012, 6594}, 5),
		NewRLE(Point3d{3872, 5012, 6594}, 15),
		NewRLE(Point3d{3901, 5012, 6594}, 3),
		NewRLE(Point3d{3904, 5012, 6594}, 31),
		NewRLE(Point3d{3867, 5013, 6594}, 5),
		NewRLE(Point3d{3872, 5013, 6594}, 16),
		NewRLE(Point3d{3900, 5013, 6594}, 4),
		NewRLE(Point3d{3904, 5013, 6594}, 31),
		NewRLE(Point3d{3867, 5014, 6594}, 5),
		NewRLE(Point3d{3872, 5014, 6594}, 16),
		NewRLE(Point3d{3897, 5014, 6594}, 7),
		NewRLE(Point3d{3904, 5014, 6594}, 31),
		NewRLE(Point3d{3867, 5015, 6594}, 5),
		NewRLE(Point3d{3872, 5015, 6594}, 8),
		NewRLE(Point3d{3884, 5015, 6594}, 9),
		NewRLE(Point3d{3897, 5015, 6594}, 7),
		NewRLE(Point3d{3904, 5015, 6594}, 32),
		NewRLE(Point3d{3867, 5016, 6594}, 5),
		NewRLE(Point3d{3872, 5016, 6594}, 5),
		NewRLE(Point3d{3885, 5016, 6594}, 19),
		NewRLE(Point3d{3904, 5016, 6594}, 32),
		NewRLE(Point3d{3936, 5016, 6594}, 1),
		NewRLE(Point3d{3867, 5017, 6594}, 5),
		NewRLE(Point3d{3872, 5017, 6594}, 3),
		NewRLE(Point3d{3885, 5017, 6594}, 19),
		NewRLE(Point3d{3904, 5017, 6594}, 3),
		NewRLE(Point3d{3908, 5017, 6594}, 28),
		NewRLE(Point3d{3936, 5017, 6594}, 2),

		NewRLE(Point3d{3866, 5018, 6594}, 3),
		NewRLE(Point3d{3874, 5018, 6594}, 1),

		NewRLE(Point3d{3895, 5018, 6594}, 9),
		NewRLE(Point3d{3904, 5018, 6594}, 1),

		NewRLE(Point3d{3909, 5018, 6594}, 3),
		NewRLE(Point3d{3915, 5018, 6594}, 21),

		NewRLE(Point3d{3938, 5018, 6594}, 1),

		NewRLE(Point3d{3881, 5019, 6594}, 2),
		NewRLE(Point3d{3886, 5019, 6594}, 18),
		NewRLE(Point3d{3904, 5019, 6594}, 1),
		NewRLE(Point3d{3910, 5019, 6594}, 26),
		NewRLE(Point3d{3936, 5019, 6594}, 4),
		NewRLE(Point3d{3865, 5020, 6594}, 7),
		NewRLE(Point3d{3872, 5020, 6594}, 2),
		NewRLE(Point3d{3880, 5020, 6594}, 5),
		NewRLE(Point3d{3886, 5020, 6594}, 18),
		NewRLE(Point3d{3904, 5020, 6594}, 2),
		NewRLE(Point3d{3911, 5020, 6594}, 2),
		NewRLE(Point3d{3914, 5020, 6594}, 22),
		NewRLE(Point3d{3936, 5020, 6594}, 5),
		NewRLE(Point3d{3865, 5021, 6594}, 7),
		NewRLE(Point3d{3872, 5021, 6594}, 1),
		NewRLE(Point3d{3879, 5021, 6594}, 25),
		NewRLE(Point3d{3904, 5021, 6594}, 5),
		NewRLE(Point3d{3914, 5021, 6594}, 22),
		NewRLE(Point3d{3936, 5021, 6594}, 5),
		NewRLE(Point3d{3865, 5022, 6594}, 7),
		NewRLE(Point3d{3872, 5022, 6594}, 1),
		NewRLE(Point3d{3879, 5022, 6594}, 25),
		NewRLE(Point3d{3904, 5022, 6594}, 9),
		NewRLE(Point3d{3914, 5022, 6594}, 22),
		NewRLE(Point3d{3936, 5022, 6594}, 6),
		NewRLE(Point3d{3865, 5023, 6594}, 7),
		NewRLE(Point3d{3872, 5023, 6594}, 1),
		NewRLE(Point3d{3880, 5023, 6594}, 24),
		NewRLE(Point3d{3904, 5023, 6594}, 32),
		NewRLE(Point3d{3936, 5023, 6594}, 7),
		NewRLE(Point3d{3864, 5024, 6594}, 8),
		NewRLE(Point3d{3872, 5024, 6594}, 1),
		NewRLE(Point3d{3880, 5024, 6594}, 24),
		NewRLE(Point3d{3904, 5024, 6594}, 32),
		NewRLE(Point3d{3936, 5024, 6594}, 7),
		NewRLE(Point3d{3864, 5025, 6594}, 8),
		NewRLE(Point3d{3872, 5025, 6594}, 1),
		NewRLE(Point3d{3880, 5025, 6594}, 24),
		NewRLE(Point3d{3904, 5025, 6594}, 32),
		NewRLE(Point3d{3936, 5025, 6594}, 8),
		NewRLE(Point3d{3864, 5026, 6594}, 8),
		NewRLE(Point3d{3872, 5026, 6594}, 1),
		NewRLE(Point3d{3880, 5026, 6594}, 24),
		NewRLE(Point3d{3904, 5026, 6594}, 32),
		NewRLE(Point3d{3936, 5026, 6594}, 9),
		NewRLE(Point3d{3864, 5027, 6594}, 8),
		NewRLE(Point3d{3872, 5027, 6594}, 1),
		NewRLE(Point3d{3879, 5027, 6594}, 25),
		NewRLE(Point3d{3904, 5027, 6594}, 32),
		NewRLE(Point3d{3936, 5027, 6594}, 9),
		NewRLE(Point3d{3864, 5028, 6594}, 8),
		NewRLE(Point3d{3872, 5028, 6594}, 2),
		NewRLE(Point3d{3879, 5028, 6594}, 25),
		NewRLE(Point3d{3904, 5028, 6594}, 32),
		NewRLE(Point3d{3936, 5028, 6594}, 9),
		NewRLE(Point3d{3864, 5029, 6594}, 8),
		NewRLE(Point3d{3872, 5029, 6594}, 2),
		NewRLE(Point3d{3880, 5029, 6594}, 24),
		NewRLE(Point3d{3904, 5029, 6594}, 32),
		NewRLE(Point3d{3936, 5029, 6594}, 9),
		NewRLE(Point3d{3863, 5030, 6594}, 9),
		NewRLE(Point3d{3872, 5030, 6594}, 1),
		NewRLE(Point3d{3880, 5030, 6594}, 24),
		NewRLE(Point3d{3904, 5030, 6594}, 32),
		NewRLE(Point3d{3936, 5030, 6594}, 9),
		NewRLE(Point3d{3863, 5031, 6594}, 9),
		NewRLE(Point3d{3872, 5031, 6594}, 1),
		NewRLE(Point3d{3881, 5031, 6594}, 23),
		NewRLE(Point3d{3904, 5031, 6594}, 32),
		NewRLE(Point3d{3936, 5031, 6594}, 9),
		NewRLE(Point3d{3863, 5032, 6594}, 9),
		NewRLE(Point3d{3872, 5032, 6594}, 1),
		NewRLE(Point3d{3881, 5032, 6594}, 23),
		NewRLE(Point3d{3904, 5032, 6594}, 32),
		NewRLE(Point3d{3936, 5032, 6594}, 7),
		NewRLE(Point3d{3862, 5033, 6594}, 10),
		NewRLE(Point3d{3872, 5033, 6594}, 1),
		NewRLE(Point3d{3881, 5033, 6594}, 23),
		NewRLE(Point3d{3904, 5033, 6594}, 32),
		NewRLE(Point3d{3936, 5033, 6594}, 7),
		NewRLE(Point3d{3862, 5034, 6594}, 10),
		NewRLE(Point3d{3872, 5034, 6594}, 2),
		NewRLE(Point3d{3882, 5034, 6594}, 22),
		NewRLE(Point3d{3904, 5034, 6594}, 32),
		NewRLE(Point3d{3936, 5034, 6594}, 7),
		NewRLE(Point3d{3861, 5035, 6594}, 11),
		NewRLE(Point3d{3872, 5035, 6594}, 2),
		NewRLE(Point3d{3882, 5035, 6594}, 22),
		NewRLE(Point3d{3904, 5035, 6594}, 32),
		NewRLE(Point3d{3936, 5035, 6594}, 7),
		NewRLE(Point3d{3861, 5036, 6594}, 11),
		NewRLE(Point3d{3872, 5036, 6594}, 2),
		NewRLE(Point3d{3882, 5036, 6594}, 22),
		NewRLE(Point3d{3904, 5036, 6594}, 32),
		NewRLE(Point3d{3936, 5036, 6594}, 7),
		NewRLE(Point3d{3860, 5037, 6594}, 12),
		NewRLE(Point3d{3872, 5037, 6594}, 1),
		NewRLE(Point3d{3882, 5037, 6594}, 22),
		NewRLE(Point3d{3904, 5037, 6594}, 32),
		NewRLE(Point3d{3936, 5037, 6594}, 7),
		NewRLE(Point3d{3860, 5038, 6594}, 12),
		NewRLE(Point3d{3872, 5038, 6594}, 2),
		NewRLE(Point3d{3883, 5038, 6594}, 21),
		NewRLE(Point3d{3904, 5038, 6594}, 32),
		NewRLE(Point3d{3936, 5038, 6594}, 7),
		NewRLE(Point3d{3860, 5039, 6594}, 12),
		NewRLE(Point3d{3872, 5039, 6594}, 4),
		NewRLE(Point3d{3884, 5039, 6594}, 20),
		NewRLE(Point3d{3904, 5039, 6594}, 32),
		NewRLE(Point3d{3936, 5039, 6594}, 7),
		NewRLE(Point3d{3858, 5040, 6594}, 14),
		NewRLE(Point3d{3872, 5040, 6594}, 7),
		NewRLE(Point3d{3885, 5040, 6594}, 12),
		NewRLE(Point3d{3897, 5040, 6594}, 1),
		NewRLE(Point3d{3898, 5040, 6594}, 6),
		NewRLE(Point3d{3904, 5040, 6594}, 2),
		NewRLE(Point3d{3906, 5040, 6594}, 1),
		NewRLE(Point3d{3907, 5040, 6594}, 29),
		NewRLE(Point3d{3936, 5040, 6594}, 5),
		NewRLE(Point3d{3858, 5041, 6594}, 14),
		NewRLE(Point3d{3872, 5041, 6594}, 8),
		NewRLE(Point3d{3885, 5041, 6594}, 11),
		NewRLE(Point3d{3896, 5041, 6594}, 8),
		NewRLE(Point3d{3904, 5041, 6594}, 3),
		NewRLE(Point3d{3907, 5041, 6594}, 29),
		NewRLE(Point3d{3936, 5041, 6594}, 5),
		NewRLE(Point3d{3858, 5042, 6594}, 14),
		NewRLE(Point3d{3872, 5042, 6594}, 8),
		NewRLE(Point3d{3885, 5042, 6594}, 9),
		NewRLE(Point3d{3894, 5042, 6594}, 10),
		NewRLE(Point3d{3904, 5042, 6594}, 6),
		NewRLE(Point3d{3910, 5042, 6594}, 26),
		NewRLE(Point3d{3936, 5042, 6594}, 4),
		NewRLE(Point3d{3859, 5043, 6594}, 13),
		NewRLE(Point3d{3872, 5043, 6594}, 9),
		NewRLE(Point3d{3884, 5043, 6594}, 8),
		NewRLE(Point3d{3892, 5043, 6594}, 12),
		NewRLE(Point3d{3904, 5043, 6594}, 8),
		NewRLE(Point3d{3912, 5043, 6594}, 24),
		NewRLE(Point3d{3936, 5043, 6594}, 4),
		NewRLE(Point3d{3858, 5044, 6594}, 14),
		NewRLE(Point3d{3872, 5044, 6594}, 10),
		NewRLE(Point3d{3884, 5044, 6594}, 8),
		NewRLE(Point3d{3892, 5044, 6594}, 12),
		NewRLE(Point3d{3904, 5044, 6594}, 12),
		NewRLE(Point3d{3916, 5044, 6594}, 20),
		NewRLE(Point3d{3936, 5044, 6594}, 4),
		NewRLE(Point3d{3858, 5045, 6594}, 14),
		NewRLE(Point3d{3872, 5045, 6594}, 11),
		NewRLE(Point3d{3884, 5045, 6594}, 9),
		NewRLE(Point3d{3893, 5045, 6594}, 11),
		NewRLE(Point3d{3904, 5045, 6594}, 12),
		NewRLE(Point3d{3916, 5045, 6594}, 20),
		NewRLE(Point3d{3936, 5045, 6594}, 2),
		NewRLE(Point3d{3859, 5046, 6594}, 13),
		NewRLE(Point3d{3872, 5046, 6594}, 22),
		NewRLE(Point3d{3894, 5046, 6594}, 10),
		NewRLE(Point3d{3904, 5046, 6594}, 13),
		NewRLE(Point3d{3917, 5046, 6594}, 19),
		NewRLE(Point3d{3936, 5046, 6594}, 1),
		NewRLE(Point3d{3859, 5047, 6594}, 13),
		NewRLE(Point3d{3872, 5047, 6594}, 22),
		NewRLE(Point3d{3894, 5047, 6594}, 10),
		NewRLE(Point3d{3904, 5047, 6594}, 13),
		NewRLE(Point3d{3917, 5047, 6594}, 19),
		NewRLE(Point3d{3858, 5048, 6594}, 14),
		NewRLE(Point3d{3872, 5048, 6594}, 22),
		NewRLE(Point3d{3894, 5048, 6594}, 10),
		NewRLE(Point3d{3904, 5048, 6594}, 13),
		NewRLE(Point3d{3917, 5048, 6594}, 19),
		NewRLE(Point3d{3858, 5049, 6594}, 14),
		NewRLE(Point3d{3872, 5049, 6594}, 22),
		NewRLE(Point3d{3894, 5049, 6594}, 10),
		NewRLE(Point3d{3904, 5049, 6594}, 13),
		NewRLE(Point3d{3917, 5049, 6594}, 17),
		NewRLE(Point3d{3858, 5050, 6594}, 14),
		NewRLE(Point3d{3872, 5050, 6594}, 23),
		NewRLE(Point3d{3895, 5050, 6594}, 9),
		NewRLE(Point3d{3904, 5050, 6594}, 14),
		NewRLE(Point3d{3918, 5050, 6594}, 16),
		NewRLE(Point3d{3858, 5051, 6594}, 14),
		NewRLE(Point3d{3872, 5051, 6594}, 26),
		NewRLE(Point3d{3898, 5051, 6594}, 6),
		NewRLE(Point3d{3904, 5051, 6594}, 8),
		NewRLE(Point3d{3912, 5051, 6594}, 21),
		NewRLE(Point3d{3859, 5052, 6594}, 13),
		NewRLE(Point3d{3872, 5052, 6594}, 25),
		NewRLE(Point3d{3897, 5052, 6594}, 3),
		NewRLE(Point3d{3900, 5052, 6594}, 1),
		NewRLE(Point3d{3901, 5052, 6594}, 2),
		NewRLE(Point3d{3903, 5052, 6594}, 1),
		NewRLE(Point3d{3904, 5052, 6594}, 2),
		NewRLE(Point3d{3906, 5052, 6594}, 25),
		NewRLE(Point3d{3859, 5053, 6594}, 13),
		NewRLE(Point3d{3872, 5053, 6594}, 32),
		NewRLE(Point3d{3904, 5053, 6594}, 27),
		NewRLE(Point3d{3860, 5054, 6594}, 12),
		NewRLE(Point3d{3872, 5054, 6594}, 32),
		NewRLE(Point3d{3904, 5054, 6594}, 26),
		NewRLE(Point3d{3861, 5055, 6594}, 11),
		NewRLE(Point3d{3872, 5055, 6594}, 32),
		NewRLE(Point3d{3904, 5055, 6594}, 25),
		NewRLE(Point3d{3862, 5056, 6594}, 10),
		NewRLE(Point3d{3872, 5056, 6594}, 32),
		NewRLE(Point3d{3904, 5056, 6594}, 24),
		NewRLE(Point3d{3862, 5057, 6594}, 10),
		NewRLE(Point3d{3872, 5057, 6594}, 32),
		NewRLE(Point3d{3904, 5057, 6594}, 22),
		NewRLE(Point3d{3862, 5058, 6594}, 10),
		NewRLE(Point3d{3872, 5058, 6594}, 32),
		NewRLE(Point3d{3904, 5058, 6594}, 21),
		NewRLE(Point3d{3864, 5059, 6594}, 8),
		NewRLE(Point3d{3872, 5059, 6594}, 32),
		NewRLE(Point3d{3904, 5059, 6594}, 21),
		NewRLE(Point3d{3863, 5060, 6594}, 9),
		NewRLE(Point3d{3872, 5060, 6594}, 32),
		NewRLE(Point3d{3904, 5060, 6594}, 20),
		NewRLE(Point3d{3863, 5061, 6594}, 9),
		NewRLE(Point3d{3872, 5061, 6594}, 32),
		NewRLE(Point3d{3904, 5061, 6594}, 19),
		NewRLE(Point3d{3864, 5062, 6594}, 8),
		NewRLE(Point3d{3872, 5062, 6594}, 32),
		NewRLE(Point3d{3904, 5062, 6594}, 19),
		NewRLE(Point3d{3865, 5063, 6594}, 7),
		NewRLE(Point3d{3872, 5063, 6594}, 32),
		NewRLE(Point3d{3904, 5063, 6594}, 17),
		NewRLE(Point3d{3866, 5064, 6594}, 6),
		NewRLE(Point3d{3872, 5064, 6594}, 32),
		NewRLE(Point3d{3904, 5064, 6594}, 17),
		NewRLE(Point3d{3868, 5065, 6594}, 4),
		NewRLE(Point3d{3872, 5065, 6594}, 32),
		NewRLE(Point3d{3904, 5065, 6594}, 17),
		NewRLE(Point3d{3868, 5066, 6594}, 4),
		NewRLE(Point3d{3872, 5066, 6594}, 32),
		NewRLE(Point3d{3904, 5066, 6594}, 16),
		NewRLE(Point3d{3869, 5067, 6594}, 1),
		NewRLE(Point3d{3871, 5067, 6594}, 1),
		NewRLE(Point3d{3872, 5067, 6594}, 32),
		NewRLE(Point3d{3904, 5067, 6594}, 16),
		NewRLE(Point3d{3871, 5068, 6594}, 1),
		NewRLE(Point3d{3872, 5068, 6594}, 1),
		NewRLE(Point3d{3875, 5068, 6594}, 29),
		NewRLE(Point3d{3904, 5068, 6594}, 14),
		NewRLE(Point3d{3872, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5069, 6594}, 28),
		NewRLE(Point3d{3904, 5069, 6594}, 10),
		NewRLE(Point3d{3916, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5070, 6594}, 28),
		NewRLE(Point3d{3904, 5070, 6594}, 8),
		NewRLE(Point3d{3877, 5071, 6594}, 27),
		NewRLE(Point3d{3904, 5071, 6594}, 8),
		NewRLE(Point3d{3879, 5072, 6594}, 25),
		NewRLE(Point3d{3904, 5072, 6594}, 2),
		NewRLE(Point3d{3881, 5073, 6594}, 22),
		NewRLE(Point3d{3883, 5074, 6594}, 18),
		NewRLE(Point3d{3884, 5075, 6594}, 13),
		NewRLE(Point3d{3887, 5076, 6594}, 7),
	}

	bodyRLEs = RLEs{
		NewRLE(Point3d{3885, 5001, 6594}, 2),
		NewRLE(Point3d{3922, 5001, 6594}, 2),
		NewRLE(Point3d{3884, 5002, 6594}, 5),
		NewRLE(Point3d{3922, 5002, 6594}, 4),
		NewRLE(Point3d{3928, 5002, 6594}, 2),
		NewRLE(Point3d{3883, 5003, 6594}, 7),
		NewRLE(Point3d{3921, 5003, 6594}, 10),
		NewRLE(Point3d{3881, 5004, 6594}, 9),
		NewRLE(Point3d{3920, 5004, 6594}, 13),
		NewRLE(Point3d{3879, 5005, 6594}, 11),
		NewRLE(Point3d{3920, 5005, 6594}, 14),
		NewRLE(Point3d{3877, 5006, 6594}, 13),
		NewRLE(Point3d{3918, 5006, 6594}, 16),
		NewRLE(Point3d{3875, 5007, 6594}, 13),
		NewRLE(Point3d{3901, 5007, 6594}, 3),
		NewRLE(Point3d{3904, 5007, 6594}, 1),
		NewRLE(Point3d{3912, 5007, 6594}, 1),
		NewRLE(Point3d{3914, 5007, 6594}, 19),
		NewRLE(Point3d{3873, 5008, 6594}, 14),
		NewRLE(Point3d{3901, 5008, 6594}, 3),
		NewRLE(Point3d{3904, 5008, 6594}, 29),
		NewRLE(Point3d{3871, 5009, 6594}, 1),
		NewRLE(Point3d{3872, 5009, 6594}, 16),
		NewRLE(Point3d{3901, 5009, 6594}, 3),
		NewRLE(Point3d{3904, 5009, 6594}, 29),
		NewRLE(Point3d{3870, 5010, 6594}, 2),
		NewRLE(Point3d{3872, 5010, 6594}, 16),
		NewRLE(Point3d{3900, 5010, 6594}, 4),
		NewRLE(Point3d{3904, 5010, 6594}, 29),
		NewRLE(Point3d{3868, 5011, 6594}, 4),
		NewRLE(Point3d{3872, 5011, 6594}, 15),
		NewRLE(Point3d{3901, 5011, 6594}, 3),
		NewRLE(Point3d{3904, 5011, 6594}, 30),
		NewRLE(Point3d{3867, 5012, 6594}, 5),
		NewRLE(Point3d{3872, 5012, 6594}, 15),
		NewRLE(Point3d{3901, 5012, 6594}, 3),
		NewRLE(Point3d{3904, 5012, 6594}, 31),
		NewRLE(Point3d{3867, 5013, 6594}, 5),
		NewRLE(Point3d{3872, 5013, 6594}, 16),
		NewRLE(Point3d{3900, 5013, 6594}, 4),
		NewRLE(Point3d{3904, 5013, 6594}, 31),
		NewRLE(Point3d{3867, 5014, 6594}, 5),
		NewRLE(Point3d{3872, 5014, 6594}, 16),
		NewRLE(Point3d{3897, 5014, 6594}, 7),
		NewRLE(Point3d{3904, 5014, 6594}, 31),
		NewRLE(Point3d{3867, 5015, 6594}, 5),
		NewRLE(Point3d{3872, 5015, 6594}, 8),
		NewRLE(Point3d{3884, 5015, 6594}, 9),
		NewRLE(Point3d{3897, 5015, 6594}, 7),
		NewRLE(Point3d{3904, 5015, 6594}, 32),
		NewRLE(Point3d{3867, 5016, 6594}, 5),
		NewRLE(Point3d{3872, 5016, 6594}, 5),
		NewRLE(Point3d{3885, 5016, 6594}, 19),
		NewRLE(Point3d{3904, 5016, 6594}, 32),
		NewRLE(Point3d{3936, 5016, 6594}, 1),
		NewRLE(Point3d{3867, 5017, 6594}, 5),
		NewRLE(Point3d{3872, 5017, 6594}, 3),
		NewRLE(Point3d{3885, 5017, 6594}, 19),
		NewRLE(Point3d{3904, 5017, 6594}, 3),
		NewRLE(Point3d{3908, 5017, 6594}, 28),
		NewRLE(Point3d{3936, 5017, 6594}, 2),
		NewRLE(Point3d{3866, 5018, 6594}, 6),
		NewRLE(Point3d{3872, 5018, 6594}, 3),
		NewRLE(Point3d{3886, 5018, 6594}, 18),
		NewRLE(Point3d{3904, 5018, 6594}, 2),
		NewRLE(Point3d{3909, 5018, 6594}, 27),
		NewRLE(Point3d{3936, 5018, 6594}, 3),
		NewRLE(Point3d{3866, 5019, 6594}, 6),
		NewRLE(Point3d{3872, 5019, 6594}, 2),
		NewRLE(Point3d{3881, 5019, 6594}, 2),
		NewRLE(Point3d{3886, 5019, 6594}, 18),
		NewRLE(Point3d{3904, 5019, 6594}, 1),
		NewRLE(Point3d{3910, 5019, 6594}, 26),
		NewRLE(Point3d{3936, 5019, 6594}, 4),
		NewRLE(Point3d{3865, 5020, 6594}, 7),
		NewRLE(Point3d{3872, 5020, 6594}, 2),
		NewRLE(Point3d{3880, 5020, 6594}, 5),
		NewRLE(Point3d{3886, 5020, 6594}, 18),
		NewRLE(Point3d{3904, 5020, 6594}, 2),
		NewRLE(Point3d{3911, 5020, 6594}, 2),
		NewRLE(Point3d{3914, 5020, 6594}, 22),
		NewRLE(Point3d{3936, 5020, 6594}, 5),
		NewRLE(Point3d{3865, 5021, 6594}, 7),
		NewRLE(Point3d{3872, 5021, 6594}, 1),
		NewRLE(Point3d{3879, 5021, 6594}, 25),
		NewRLE(Point3d{3904, 5021, 6594}, 5),
		NewRLE(Point3d{3914, 5021, 6594}, 22),
		NewRLE(Point3d{3936, 5021, 6594}, 5),
		NewRLE(Point3d{3865, 5022, 6594}, 7),
		NewRLE(Point3d{3872, 5022, 6594}, 1),
		NewRLE(Point3d{3879, 5022, 6594}, 25),
		NewRLE(Point3d{3904, 5022, 6594}, 9),
		NewRLE(Point3d{3914, 5022, 6594}, 22),
		NewRLE(Point3d{3936, 5022, 6594}, 6),
		NewRLE(Point3d{3865, 5023, 6594}, 7),
		NewRLE(Point3d{3872, 5023, 6594}, 1),
		NewRLE(Point3d{3880, 5023, 6594}, 24),
		NewRLE(Point3d{3904, 5023, 6594}, 32),
		NewRLE(Point3d{3936, 5023, 6594}, 7),
		NewRLE(Point3d{3864, 5024, 6594}, 8),
		NewRLE(Point3d{3872, 5024, 6594}, 1),
		NewRLE(Point3d{3880, 5024, 6594}, 24),
		NewRLE(Point3d{3904, 5024, 6594}, 32),
		NewRLE(Point3d{3936, 5024, 6594}, 7),
		NewRLE(Point3d{3864, 5025, 6594}, 8),
		NewRLE(Point3d{3872, 5025, 6594}, 1),
		NewRLE(Point3d{3880, 5025, 6594}, 24),
		NewRLE(Point3d{3904, 5025, 6594}, 32),
		NewRLE(Point3d{3936, 5025, 6594}, 8),
		NewRLE(Point3d{3864, 5026, 6594}, 8),
		NewRLE(Point3d{3872, 5026, 6594}, 1),
		NewRLE(Point3d{3880, 5026, 6594}, 24),
		NewRLE(Point3d{3904, 5026, 6594}, 32),
		NewRLE(Point3d{3936, 5026, 6594}, 9),
		NewRLE(Point3d{3864, 5027, 6594}, 8),
		NewRLE(Point3d{3872, 5027, 6594}, 1),
		NewRLE(Point3d{3879, 5027, 6594}, 25),
		NewRLE(Point3d{3904, 5027, 6594}, 32),
		NewRLE(Point3d{3936, 5027, 6594}, 9),
		NewRLE(Point3d{3864, 5028, 6594}, 8),
		NewRLE(Point3d{3872, 5028, 6594}, 2),
		NewRLE(Point3d{3879, 5028, 6594}, 25),
		NewRLE(Point3d{3904, 5028, 6594}, 32),
		NewRLE(Point3d{3936, 5028, 6594}, 9),
		NewRLE(Point3d{3864, 5029, 6594}, 8),
		NewRLE(Point3d{3872, 5029, 6594}, 2),
		NewRLE(Point3d{3880, 5029, 6594}, 24),
		NewRLE(Point3d{3904, 5029, 6594}, 32),
		NewRLE(Point3d{3936, 5029, 6594}, 9),
		NewRLE(Point3d{3863, 5030, 6594}, 9),
		NewRLE(Point3d{3872, 5030, 6594}, 1),
		NewRLE(Point3d{3880, 5030, 6594}, 24),
		NewRLE(Point3d{3904, 5030, 6594}, 32),
		NewRLE(Point3d{3936, 5030, 6594}, 9),
		NewRLE(Point3d{3863, 5031, 6594}, 9),
		NewRLE(Point3d{3872, 5031, 6594}, 1),
		NewRLE(Point3d{3881, 5031, 6594}, 23),
		NewRLE(Point3d{3904, 5031, 6594}, 32),
		NewRLE(Point3d{3936, 5031, 6594}, 9),
		NewRLE(Point3d{3863, 5032, 6594}, 9),
		NewRLE(Point3d{3872, 5032, 6594}, 1),
		NewRLE(Point3d{3881, 5032, 6594}, 23),
		NewRLE(Point3d{3904, 5032, 6594}, 32),
		NewRLE(Point3d{3936, 5032, 6594}, 7),
		NewRLE(Point3d{3862, 5033, 6594}, 10),
		NewRLE(Point3d{3872, 5033, 6594}, 1),
		NewRLE(Point3d{3881, 5033, 6594}, 23),
		NewRLE(Point3d{3904, 5033, 6594}, 32),
		NewRLE(Point3d{3936, 5033, 6594}, 7),
		NewRLE(Point3d{3862, 5034, 6594}, 10),
		NewRLE(Point3d{3872, 5034, 6594}, 2),
		NewRLE(Point3d{3882, 5034, 6594}, 22),
		NewRLE(Point3d{3904, 5034, 6594}, 32),
		NewRLE(Point3d{3936, 5034, 6594}, 7),
		NewRLE(Point3d{3861, 5035, 6594}, 11),
		NewRLE(Point3d{3872, 5035, 6594}, 2),
		NewRLE(Point3d{3882, 5035, 6594}, 22),
		NewRLE(Point3d{3904, 5035, 6594}, 32),
		NewRLE(Point3d{3936, 5035, 6594}, 7),
		NewRLE(Point3d{3861, 5036, 6594}, 11),
		NewRLE(Point3d{3872, 5036, 6594}, 2),
		NewRLE(Point3d{3882, 5036, 6594}, 22),
		NewRLE(Point3d{3904, 5036, 6594}, 32),
		NewRLE(Point3d{3936, 5036, 6594}, 7),
		NewRLE(Point3d{3860, 5037, 6594}, 12),
		NewRLE(Point3d{3872, 5037, 6594}, 1),
		NewRLE(Point3d{3882, 5037, 6594}, 22),
		NewRLE(Point3d{3904, 5037, 6594}, 32),
		NewRLE(Point3d{3936, 5037, 6594}, 7),
		NewRLE(Point3d{3860, 5038, 6594}, 12),
		NewRLE(Point3d{3872, 5038, 6594}, 2),
		NewRLE(Point3d{3883, 5038, 6594}, 21),
		NewRLE(Point3d{3904, 5038, 6594}, 32),
		NewRLE(Point3d{3936, 5038, 6594}, 7),
		NewRLE(Point3d{3860, 5039, 6594}, 12),
		NewRLE(Point3d{3872, 5039, 6594}, 4),
		NewRLE(Point3d{3884, 5039, 6594}, 20),
		NewRLE(Point3d{3904, 5039, 6594}, 32),
		NewRLE(Point3d{3936, 5039, 6594}, 7),
		NewRLE(Point3d{3858, 5040, 6594}, 14),
		NewRLE(Point3d{3872, 5040, 6594}, 7),
		NewRLE(Point3d{3885, 5040, 6594}, 12),
		NewRLE(Point3d{3897, 5040, 6594}, 1),
		NewRLE(Point3d{3898, 5040, 6594}, 6),
		NewRLE(Point3d{3904, 5040, 6594}, 2),
		NewRLE(Point3d{3906, 5040, 6594}, 1),
		NewRLE(Point3d{3907, 5040, 6594}, 29),
		NewRLE(Point3d{3936, 5040, 6594}, 5),
		NewRLE(Point3d{3858, 5041, 6594}, 14),
		NewRLE(Point3d{3872, 5041, 6594}, 8),
		NewRLE(Point3d{3885, 5041, 6594}, 11),
		NewRLE(Point3d{3896, 5041, 6594}, 8),
		NewRLE(Point3d{3904, 5041, 6594}, 3),
		NewRLE(Point3d{3907, 5041, 6594}, 29),
		NewRLE(Point3d{3936, 5041, 6594}, 5),
		NewRLE(Point3d{3858, 5042, 6594}, 14),
		NewRLE(Point3d{3872, 5042, 6594}, 8),
		NewRLE(Point3d{3885, 5042, 6594}, 9),
		NewRLE(Point3d{3894, 5042, 6594}, 10),
		NewRLE(Point3d{3904, 5042, 6594}, 6),
		NewRLE(Point3d{3910, 5042, 6594}, 26),
		NewRLE(Point3d{3936, 5042, 6594}, 4),
		NewRLE(Point3d{3859, 5043, 6594}, 13),
		NewRLE(Point3d{3872, 5043, 6594}, 9),
		NewRLE(Point3d{3884, 5043, 6594}, 8),
		NewRLE(Point3d{3892, 5043, 6594}, 12),
		NewRLE(Point3d{3904, 5043, 6594}, 8),
		NewRLE(Point3d{3912, 5043, 6594}, 24),
		NewRLE(Point3d{3936, 5043, 6594}, 4),
		NewRLE(Point3d{3858, 5044, 6594}, 14),
		NewRLE(Point3d{3872, 5044, 6594}, 10),
		NewRLE(Point3d{3884, 5044, 6594}, 8),
		NewRLE(Point3d{3892, 5044, 6594}, 12),
		NewRLE(Point3d{3904, 5044, 6594}, 12),
		NewRLE(Point3d{3916, 5044, 6594}, 20),
		NewRLE(Point3d{3936, 5044, 6594}, 4),
		NewRLE(Point3d{3858, 5045, 6594}, 14),
		NewRLE(Point3d{3872, 5045, 6594}, 11),
		NewRLE(Point3d{3884, 5045, 6594}, 9),
		NewRLE(Point3d{3893, 5045, 6594}, 11),
		NewRLE(Point3d{3904, 5045, 6594}, 12),
		NewRLE(Point3d{3916, 5045, 6594}, 20),
		NewRLE(Point3d{3936, 5045, 6594}, 2),
		NewRLE(Point3d{3859, 5046, 6594}, 13),
		NewRLE(Point3d{3872, 5046, 6594}, 22),
		NewRLE(Point3d{3894, 5046, 6594}, 10),
		NewRLE(Point3d{3904, 5046, 6594}, 13),
		NewRLE(Point3d{3917, 5046, 6594}, 19),
		NewRLE(Point3d{3936, 5046, 6594}, 1),
		NewRLE(Point3d{3859, 5047, 6594}, 13),
		NewRLE(Point3d{3872, 5047, 6594}, 22),
		NewRLE(Point3d{3894, 5047, 6594}, 10),
		NewRLE(Point3d{3904, 5047, 6594}, 13),
		NewRLE(Point3d{3917, 5047, 6594}, 19),
		NewRLE(Point3d{3858, 5048, 6594}, 14),
		NewRLE(Point3d{3872, 5048, 6594}, 22),
		NewRLE(Point3d{3894, 5048, 6594}, 10),
		NewRLE(Point3d{3904, 5048, 6594}, 13),
		NewRLE(Point3d{3917, 5048, 6594}, 19),
		NewRLE(Point3d{3858, 5049, 6594}, 14),
		NewRLE(Point3d{3872, 5049, 6594}, 22),
		NewRLE(Point3d{3894, 5049, 6594}, 10),
		NewRLE(Point3d{3904, 5049, 6594}, 13),
		NewRLE(Point3d{3917, 5049, 6594}, 17),
		NewRLE(Point3d{3858, 5050, 6594}, 14),
		NewRLE(Point3d{3872, 5050, 6594}, 23),
		NewRLE(Point3d{3895, 5050, 6594}, 9),
		NewRLE(Point3d{3904, 5050, 6594}, 14),
		NewRLE(Point3d{3918, 5050, 6594}, 16),
		NewRLE(Point3d{3858, 5051, 6594}, 14),
		NewRLE(Point3d{3872, 5051, 6594}, 26),
		NewRLE(Point3d{3898, 5051, 6594}, 6),
		NewRLE(Point3d{3904, 5051, 6594}, 8),
		NewRLE(Point3d{3912, 5051, 6594}, 21),
		NewRLE(Point3d{3859, 5052, 6594}, 13),
		NewRLE(Point3d{3872, 5052, 6594}, 25),
		NewRLE(Point3d{3897, 5052, 6594}, 3),
		NewRLE(Point3d{3900, 5052, 6594}, 1),
		NewRLE(Point3d{3901, 5052, 6594}, 2),
		NewRLE(Point3d{3903, 5052, 6594}, 1),
		NewRLE(Point3d{3904, 5052, 6594}, 2),
		NewRLE(Point3d{3906, 5052, 6594}, 25),
		NewRLE(Point3d{3859, 5053, 6594}, 13),
		NewRLE(Point3d{3872, 5053, 6594}, 32),
		NewRLE(Point3d{3904, 5053, 6594}, 27),
		NewRLE(Point3d{3860, 5054, 6594}, 12),
		NewRLE(Point3d{3872, 5054, 6594}, 32),
		NewRLE(Point3d{3904, 5054, 6594}, 26),
		NewRLE(Point3d{3861, 5055, 6594}, 11),
		NewRLE(Point3d{3872, 5055, 6594}, 32),
		NewRLE(Point3d{3904, 5055, 6594}, 25),
		NewRLE(Point3d{3862, 5056, 6594}, 10),
		NewRLE(Point3d{3872, 5056, 6594}, 32),
		NewRLE(Point3d{3904, 5056, 6594}, 24),
		NewRLE(Point3d{3862, 5057, 6594}, 10),
		NewRLE(Point3d{3872, 5057, 6594}, 32),
		NewRLE(Point3d{3904, 5057, 6594}, 22),
		NewRLE(Point3d{3862, 5058, 6594}, 10),
		NewRLE(Point3d{3872, 5058, 6594}, 32),
		NewRLE(Point3d{3904, 5058, 6594}, 21),
		NewRLE(Point3d{3864, 5059, 6594}, 8),
		NewRLE(Point3d{3872, 5059, 6594}, 32),
		NewRLE(Point3d{3904, 5059, 6594}, 21),
		NewRLE(Point3d{3863, 5060, 6594}, 9),
		NewRLE(Point3d{3872, 5060, 6594}, 32),
		NewRLE(Point3d{3904, 5060, 6594}, 20),
		NewRLE(Point3d{3863, 5061, 6594}, 9),
		NewRLE(Point3d{3872, 5061, 6594}, 32),
		NewRLE(Point3d{3904, 5061, 6594}, 19),
		NewRLE(Point3d{3864, 5062, 6594}, 8),
		NewRLE(Point3d{3872, 5062, 6594}, 32),
		NewRLE(Point3d{3904, 5062, 6594}, 19),
		NewRLE(Point3d{3865, 5063, 6594}, 7),
		NewRLE(Point3d{3872, 5063, 6594}, 32),
		NewRLE(Point3d{3904, 5063, 6594}, 17),
		NewRLE(Point3d{3866, 5064, 6594}, 6),
		NewRLE(Point3d{3872, 5064, 6594}, 32),
		NewRLE(Point3d{3904, 5064, 6594}, 17),
		NewRLE(Point3d{3868, 5065, 6594}, 4),
		NewRLE(Point3d{3872, 5065, 6594}, 32),
		NewRLE(Point3d{3904, 5065, 6594}, 17),
		NewRLE(Point3d{3868, 5066, 6594}, 4),
		NewRLE(Point3d{3872, 5066, 6594}, 32),
		NewRLE(Point3d{3904, 5066, 6594}, 16),
		NewRLE(Point3d{3869, 5067, 6594}, 1),
		NewRLE(Point3d{3871, 5067, 6594}, 1),
		NewRLE(Point3d{3872, 5067, 6594}, 32),
		NewRLE(Point3d{3904, 5067, 6594}, 16),
		NewRLE(Point3d{3871, 5068, 6594}, 1),
		NewRLE(Point3d{3872, 5068, 6594}, 1),
		NewRLE(Point3d{3875, 5068, 6594}, 29),
		NewRLE(Point3d{3904, 5068, 6594}, 14),
		NewRLE(Point3d{3872, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5069, 6594}, 28),
		NewRLE(Point3d{3904, 5069, 6594}, 10),
		NewRLE(Point3d{3916, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5070, 6594}, 28),
		NewRLE(Point3d{3904, 5070, 6594}, 8),
		NewRLE(Point3d{3877, 5071, 6594}, 27),
		NewRLE(Point3d{3904, 5071, 6594}, 8),
		NewRLE(Point3d{3879, 5072, 6594}, 25),
		NewRLE(Point3d{3904, 5072, 6594}, 2),
		NewRLE(Point3d{3881, 5073, 6594}, 22),
		NewRLE(Point3d{3883, 5074, 6594}, 18),
		NewRLE(Point3d{3884, 5075, 6594}, 13),
		NewRLE(Point3d{3887, 5076, 6594}, 7),
	}

	splitDupRLEs = RLEs{
		NewRLE(Point3d{3885, 5001, 6594}, 2),
		NewRLE(Point3d{3922, 5001, 6594}, 2),
		NewRLE(Point3d{3884, 5002, 6594}, 5),
		NewRLE(Point3d{3922, 5002, 6594}, 4),
		NewRLE(Point3d{3928, 5002, 6594}, 2),
		NewRLE(Point3d{3883, 5003, 6594}, 7),
		NewRLE(Point3d{3921, 5003, 6594}, 10),
		NewRLE(Point3d{3881, 5004, 6594}, 9),
		NewRLE(Point3d{3920, 5004, 6594}, 13),
		NewRLE(Point3d{3879, 5005, 6594}, 11),
		NewRLE(Point3d{3920, 5005, 6594}, 14),
		NewRLE(Point3d{3877, 5006, 6594}, 13),
		NewRLE(Point3d{3918, 5006, 6594}, 16),
		NewRLE(Point3d{3875, 5007, 6594}, 13),
		NewRLE(Point3d{3901, 5007, 6594}, 4),
		NewRLE(Point3d{3912, 5007, 6594}, 1),
		NewRLE(Point3d{3914, 5007, 6594}, 19),
		NewRLE(Point3d{3873, 5008, 6594}, 14),
		NewRLE(Point3d{3901, 5008, 6594}, 32),
		NewRLE(Point3d{3871, 5009, 6594}, 17),
		NewRLE(Point3d{3901, 5009, 6594}, 32),
		NewRLE(Point3d{3870, 5010, 6594}, 18),
		NewRLE(Point3d{3900, 5010, 6594}, 33),
		NewRLE(Point3d{3868, 5011, 6594}, 19),
		NewRLE(Point3d{3901, 5011, 6594}, 33),
		NewRLE(Point3d{3867, 5012, 6594}, 20),
		NewRLE(Point3d{3901, 5012, 6594}, 34),
		NewRLE(Point3d{3867, 5013, 6594}, 21),
		NewRLE(Point3d{3900, 5013, 6594}, 35),
		NewRLE(Point3d{3867, 5014, 6594}, 21),
		NewRLE(Point3d{3897, 5014, 6594}, 38),
		NewRLE(Point3d{3867, 5015, 6594}, 13),
		NewRLE(Point3d{3884, 5015, 6594}, 9),
		NewRLE(Point3d{3897, 5015, 6594}, 39),
		NewRLE(Point3d{3867, 5016, 6594}, 10),
		NewRLE(Point3d{3885, 5016, 6594}, 52),
		NewRLE(Point3d{3867, 5017, 6594}, 8),
		NewRLE(Point3d{3885, 5017, 6594}, 22),
		NewRLE(Point3d{3908, 5017, 6594}, 30),
		NewRLE(Point3d{3866, 5018, 6594}, 9),
		NewRLE(Point3d{3886, 5018, 6594}, 20),
		NewRLE(Point3d{3909, 5018, 6594}, 30),
		NewRLE(Point3d{3866, 5019, 6594}, 8),
		NewRLE(Point3d{3881, 5019, 6594}, 2),
		NewRLE(Point3d{3886, 5019, 6594}, 19),
		NewRLE(Point3d{3910, 5019, 6594}, 30),
		NewRLE(Point3d{3865, 5020, 6594}, 9),
		NewRLE(Point3d{3880, 5020, 6594}, 5),
		NewRLE(Point3d{3886, 5020, 6594}, 20),
		NewRLE(Point3d{3911, 5020, 6594}, 2),
		NewRLE(Point3d{3914, 5020, 6594}, 27),
		NewRLE(Point3d{3865, 5021, 6594}, 8),
		NewRLE(Point3d{3879, 5021, 6594}, 30),
		NewRLE(Point3d{3914, 5021, 6594}, 27),
		NewRLE(Point3d{3865, 5022, 6594}, 8),
		NewRLE(Point3d{3879, 5022, 6594}, 34),
		NewRLE(Point3d{3914, 5022, 6594}, 28),
		NewRLE(Point3d{3865, 5023, 6594}, 8),
		NewRLE(Point3d{3880, 5023, 6594}, 63),
		NewRLE(Point3d{3864, 5024, 6594}, 9),
		NewRLE(Point3d{3880, 5024, 6594}, 63),
		NewRLE(Point3d{3864, 5025, 6594}, 9),
		NewRLE(Point3d{3880, 5025, 6594}, 64),
		NewRLE(Point3d{3864, 5026, 6594}, 9),
		NewRLE(Point3d{3880, 5026, 6594}, 65),
		NewRLE(Point3d{3864, 5027, 6594}, 9),
		NewRLE(Point3d{3879, 5027, 6594}, 66),
		NewRLE(Point3d{3864, 5028, 6594}, 10),
		NewRLE(Point3d{3879, 5028, 6594}, 66),
		NewRLE(Point3d{3864, 5029, 6594}, 10),
		NewRLE(Point3d{3880, 5029, 6594}, 65),
		NewRLE(Point3d{3863, 5030, 6594}, 10),
		NewRLE(Point3d{3880, 5030, 6594}, 65),
		NewRLE(Point3d{3863, 5031, 6594}, 10),
		NewRLE(Point3d{3881, 5031, 6594}, 64),
		NewRLE(Point3d{3863, 5032, 6594}, 10),
		NewRLE(Point3d{3881, 5032, 6594}, 62),
		NewRLE(Point3d{3862, 5033, 6594}, 11),
		NewRLE(Point3d{3881, 5033, 6594}, 62),
		NewRLE(Point3d{3862, 5034, 6594}, 12),
		NewRLE(Point3d{3882, 5034, 6594}, 61),
		NewRLE(Point3d{3861, 5035, 6594}, 13),
		NewRLE(Point3d{3882, 5035, 6594}, 61),
		NewRLE(Point3d{3861, 5036, 6594}, 13),
		NewRLE(Point3d{3882, 5036, 6594}, 61),
		NewRLE(Point3d{3860, 5037, 6594}, 13),
		NewRLE(Point3d{3882, 5037, 6594}, 61),
		NewRLE(Point3d{3860, 5038, 6594}, 14),
		NewRLE(Point3d{3883, 5038, 6594}, 60),
		NewRLE(Point3d{3860, 5039, 6594}, 16),
		NewRLE(Point3d{3884, 5039, 6594}, 59),
		NewRLE(Point3d{3858, 5040, 6594}, 21),
		NewRLE(Point3d{3885, 5040, 6594}, 56),
		NewRLE(Point3d{3858, 5041, 6594}, 22),
		NewRLE(Point3d{3885, 5041, 6594}, 56),
		NewRLE(Point3d{3858, 5042, 6594}, 22),
		NewRLE(Point3d{3885, 5042, 6594}, 55),
		NewRLE(Point3d{3859, 5043, 6594}, 22),
		NewRLE(Point3d{3884, 5043, 6594}, 56),
		NewRLE(Point3d{3858, 5044, 6594}, 24),
		NewRLE(Point3d{3884, 5044, 6594}, 56),
		NewRLE(Point3d{3858, 5045, 6594}, 25),
		NewRLE(Point3d{3884, 5045, 6594}, 54),
		NewRLE(Point3d{3859, 5046, 6594}, 78),
		NewRLE(Point3d{3859, 5047, 6594}, 77),
		NewRLE(Point3d{3858, 5048, 6594}, 78),
		NewRLE(Point3d{3858, 5049, 6594}, 76),
		NewRLE(Point3d{3858, 5050, 6594}, 76),
		NewRLE(Point3d{3858, 5051, 6594}, 75),
		NewRLE(Point3d{3859, 5052, 6594}, 72),
		NewRLE(Point3d{3859, 5053, 6594}, 72),
		NewRLE(Point3d{3860, 5054, 6594}, 70),
		NewRLE(Point3d{3861, 5055, 6594}, 68),
		NewRLE(Point3d{3862, 5056, 6594}, 66),
		NewRLE(Point3d{3862, 5057, 6594}, 64),
		NewRLE(Point3d{3862, 5058, 6594}, 63),
		NewRLE(Point3d{3864, 5059, 6594}, 61),
		NewRLE(Point3d{3863, 5060, 6594}, 61),
		NewRLE(Point3d{3863, 5061, 6594}, 60),
		NewRLE(Point3d{3864, 5062, 6594}, 59),
		NewRLE(Point3d{3865, 5063, 6594}, 56),
		NewRLE(Point3d{3866, 5064, 6594}, 55),
		NewRLE(Point3d{3868, 5065, 6594}, 53),
		NewRLE(Point3d{3868, 5066, 6594}, 52),
		NewRLE(Point3d{3869, 5067, 6594}, 1),
		NewRLE(Point3d{3871, 5067, 6594}, 49),
		NewRLE(Point3d{3871, 5068, 6594}, 2),
		NewRLE(Point3d{3875, 5068, 6594}, 43),
		NewRLE(Point3d{3872, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5069, 6594}, 38),
		NewRLE(Point3d{3916, 5069, 6594}, 1),
		NewRLE(Point3d{3876, 5070, 6594}, 36),
		NewRLE(Point3d{3877, 5071, 6594}, 35),
		NewRLE(Point3d{3879, 5072, 6594}, 27),
		NewRLE(Point3d{3881, 5073, 6594}, 22),
		NewRLE(Point3d{3883, 5074, 6594}, 18),
		NewRLE(Point3d{3884, 5075, 6594}, 13),
		NewRLE(Point3d{3887, 5076, 6594}, 7),
	}
)
