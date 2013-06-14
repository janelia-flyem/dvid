package test

import (
	. "launchpad.net/gocheck"

	_ "github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	_ "github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
	_ "github.com/janelia-flyem/dvid/datatype/labels32"
	_ "github.com/janelia-flyem/dvid/datatype/labels64"
	_ "github.com/janelia-flyem/dvid/datatype/rgba8"
)

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func MakeSlice(ox, oy, oz, nx, ny int) []byte {
	slice := make([]byte, nx*ny, nx*ny)
	i := 0
	for y := 0; y < ny; y++ {
		sy := y + oy
		mody := sy % 4
		sx := ox
		for x := 0; x < ox; x++ {
			modx := sx % 4
			slice[i] = xdata[modx] ^ ydata[mody] ^ zdata[oz]
			i++
			sx++
		}
	}
	return slice
}

// Make a 3d volume of bytes with top left corner at (ox,oy,oz) and size (nx, ny, nz)
func MakeVolume(ox, oy, oz, nx, ny, nz int) []byte {
	sliceBytes := nx * ny
	volumeBytes := sliceBytes * nz
	volume := make([]byte, volumeBytes, volumeBytes)
	i := 0
	for z := oz; z < oz+nz; z++ {
		copy(volume[i:i+sliceBytes], MakeSlice(ox, oy, z, nx, ny))
		i += sliceBytes
	}
	return volume
}

func (suite *DataSuite) TestGrayscale8(c *C) {
	err := suite.service.NewDataset("grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)
}
