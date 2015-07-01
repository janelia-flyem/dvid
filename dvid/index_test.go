package dvid

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
)

// Make sure a series of ascending 3d points from negative to positive yields lexicographically
// increasing binary representations.
func TestNegIndicesSequential(t *testing.T) {
	blockSize := Point3d{15, 16, 17}
	lastBytes := make([]byte, 12)
	var lastp Point3d
	for n := 0; n < 100; n++ {
		p := Point3d{15, 64, int32(-50 + n)}
		g := p.Chunk(blockSize).(ChunkPoint3d)
		i := IndexZYX(g)
		ibytes := i.Bytes()
		if n > 0 {
			if bytes.Compare(lastBytes, ibytes) > 0 {
				t.Errorf("%s -> %s yield non-ascending binary: %x > %x\n", lastp, p, lastBytes, ibytes)
			}
		}
		lastp = p.Duplicate().(Point3d)
		copy(lastBytes, ibytes)
	}
}

func TestCounts(t *testing.T) {
	var c BlockCounts
	if !c.Empty() {
		t.Errorf("Expected BlockCounts to be empty")
	}
	i0 := IndexZYX{40, 83, 17}
	i1 := IndexZYX{2001, 3, 45}
	i2 := IndexZYX{1, 2, 3}
	p0 := i0.ToIZYXString()
	p1 := i1.ToIZYXString()
	p2 := i2.ToIZYXString()
	c.Incr(p0)
	c.Incr(p1)
	c.Decr(p1)
	c.Decr(p0)
	if !c.Empty() {
		t.Errorf("Expected BlockCounts to be empty after incr/decr cycles")
	}

	c.Incr(p2)
	if v := c.Value(p2); v != 1 {
		t.Errorf("Bad count.  Expected 1 got %d\n", v)
	}
	// Test thread-safety
	var wg sync.WaitGroup
	wg.Add(500)
	expected := 1
	for i := 0; i < 500; i++ {
		r := rand.Intn(3)
		if r == 0 {
			expected--
			go func() {
				c.Decr(p2)
				wg.Done()
			}()
		} else {
			expected++
			go func() {
				c.Incr(p2)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	if v := c.Value(p2); v != expected {
		t.Errorf("After concurrent, random incr/decr, got %d, expected %d\n", v, expected)
	}

}

func TestDirtyCache(t *testing.T) {
	var c DirtyBlocks

	iv := InstanceVersion{"foobar", 23}
	iv2 := InstanceVersion{"foobar", 24}

	i0 := IndexZYX{40, 83, 17}
	i1 := IndexZYX{2001, 3, 45}
	i2 := IndexZYX{1, 2, 3}
	i3 := IndexZYX{0, 0, 0}
	p0 := i0.ToIZYXString()
	p1 := i1.ToIZYXString()
	p2 := i2.ToIZYXString()
	p3 := i3.ToIZYXString()

	if !c.Empty(iv) && !c.Empty(iv2) {
		t.Errorf("DirtyBlocks should be considered empty but it's not.")
	}
	c.Incr(iv, p0)
	c.Incr(iv, p1)
	c.Incr(iv, p0)
	if c.Empty(iv) {
		t.Errorf("DirtyBlocks should be non-empty.")
	}
	if !c.Empty(iv2) {
		t.Errorf("DirtyBlocks should be empty")
	}
	c.Incr(iv2, p2)
	c.Incr(iv2, p0)
	if c.IsDirty(iv, p3) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv, p1) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	if c.IsDirty(iv, p2) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv2, p2) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	if !c.IsDirty(iv, p0) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	c.Decr(iv, p0)
	if !c.IsDirty(iv, p0) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	c.Decr(iv, p0)
	if c.IsDirty(iv, p0) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv2, p0) {
		t.Errorf("Label is not marked dirty when it is.")
	}
}
