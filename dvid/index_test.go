package dvid

import (
	"bytes"
	. "github.com/janelia-flyem/go/gocheck"
	_ "testing"
)

// Make sure a series of ascending 3d points from negative to positive yields lexicographically
// increasing binary representations.
func (suite *DataSuite) TestNegIndicesSequential(c *C) {
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
				c.Errorf("%s -> %s yield non-ascending binary: %x > %x\n", lastp, p, lastBytes, ibytes)
			}
		}
		lastp = p.Duplicate().(Point3d)
		copy(lastBytes, ibytes)
	}
}
