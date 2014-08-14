package dvid

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/janelia-flyem/go/gocheck"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct{}

var _ = Suite(&DataSuite{})

func (s *DataSuite) TestConversionToBytes(c *C) {

	src := rand.NewSource(time.Now().Unix())
	r := rand.New(src)
	for i := 0; i < 10; i++ {
		localid := LocalID32(r.Uint32())
		b := localid.Bytes()
		localid2, sz := LocalID32FromBytes(b)
		c.Assert(localid, Equals, localid2)
		c.Assert(sz, Equals, LocalID32Size)
	}

	for i := 0; i < 10; i++ {
		localid := InstanceID(r.Uint32())
		b := localid.Bytes()
		localid2 := InstanceIDFromBytes(b)
		c.Assert(localid, Equals, localid2)
	}

	for i := 0; i < 10; i++ {
		localid := RepoID(r.Uint32())
		b := localid.Bytes()
		localid2 := RepoIDFromBytes(b)
		c.Assert(localid, Equals, localid2)
	}

	for i := 0; i < 10; i++ {
		localid := VersionID(r.Uint32())
		b := localid.Bytes()
		localid2 := VersionIDFromBytes(b)
		c.Assert(localid, Equals, localid2)
	}
}
