package voxels

import (
	. "github.com/janelia-flyem/go/gocheck"
	_ "testing"

	_ "github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	_ "github.com/janelia-flyem/dvid/server"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
	_ "github.com/janelia-flyem/dvid/datatype/labels32"
	_ "github.com/janelia-flyem/dvid/datatype/labels64"
	_ "github.com/janelia-flyem/dvid/datatype/rgba8"
	"github.com/janelia-flyem/dvid/datatype/voxels"
)

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func MakeSlice(ox, oy, oz, nx, ny int) []byte {
	slice := make([]byte, nx*ny, nx*ny)
	i := 0
	modz := oz % 4
	for y := 0; y < ny; y++ {
		sy := y + oy
		mody := sy % 4
		sx := ox
		for x := 0; x < nx; x++ {
			modx := sx % 4
			slice[i] = xdata[modx] ^ ydata[mody] ^ zdata[modz]
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

func (suite *DataSuite) TestXYSliceGrayscale8(c *C) {
	err := suite.service.NewData("grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)

	dset, err := suite.service.DataService("grayscale")
	c.Assert(err, IsNil)

	grayscale, ok := dset.(*voxels.Data)
	if !ok {
		c.Errorf("Can't cast grayscale8 data service into grayscale8.Data\n")
	}

	// Create a fake 100x100 8-bit grayscale image
	nx := 100
	ny := 100
	ox, oy, oz := 3, 10, 14
	data := []uint8(MakeSlice(ox, oy, oz, nx, ny))
	img := dvid.ImageGrayFromData(data, nx, ny)

	slice := voxels.NewSliceXY(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})

	// Store it into datastore at head version
	versionID, err := suite.service.VersionIDFromUUID(suite.head)
	c.Assert(err, IsNil)

	err = grayscale.PutImage(versionID, img, slice)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := grayscale.GetImage(versionID, slice)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(img.Rect, DeepEquals, retrieved.Bounds())
	retrievedData, _, err := dvid.ImageData(retrieved)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, retrievedData)
}

func (suite *DataSuite) TestXZSliceGrayscale8(c *C) {
	err := suite.service.NewData("grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)

	dset, err := suite.service.DataService("grayscale")
	c.Assert(err, IsNil)

	grayscale, ok := dset.(*voxels.Data)
	if !ok {
		c.Errorf("Can't cast grayscale8 data service into grayscale8.Data\n")
	}

	// Create a fake 100x100 8-bit grayscale image
	nx := 100
	ny := 100
	ox, oy, oz := 31, 10, 14
	data := []uint8(MakeSlice(ox, oy, oz, nx, ny))
	img := dvid.ImageGrayFromData(data, nx, ny)

	slice := voxels.NewSliceXZ(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})

	// Store it into datastore at head version
	versionID, err := suite.service.VersionIDFromUUID(suite.head)
	c.Assert(err, IsNil)

	err = grayscale.PutImage(versionID, img, slice)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := grayscale.GetImage(versionID, slice)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(img.Rect, DeepEquals, retrieved.Bounds())
	retrievedData, _, err := dvid.ImageData(retrieved)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, retrievedData)
}

func (suite *DataSuite) TestYZSliceGrayscale8(c *C) {
	err := suite.service.NewData("grayscale", "grayscale8", dvid.Config{})
	c.Assert(err, IsNil)

	dset, err := suite.service.DataService("grayscale")
	c.Assert(err, IsNil)

	grayscale, ok := dset.(*voxels.Data)
	if !ok {
		c.Errorf("Can't cast grayscale8 data service into grayscale8.Data\n")
	}

	// Create a fake 100x100 8-bit grayscale image
	nx := 100
	ny := 100
	ox, oy, oz := 13, 40, 99
	data := []uint8(MakeSlice(ox, oy, oz, nx, ny))
	img := dvid.ImageGrayFromData(data, nx, ny)

	slice := voxels.NewSliceYZ(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})

	// Store it into datastore at head version
	versionID, err := suite.service.VersionIDFromUUID(suite.head)
	c.Assert(err, IsNil)

	err = grayscale.PutImage(versionID, img, slice)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := grayscale.GetImage(versionID, slice)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(img.Rect, DeepEquals, retrieved.Bounds())
	retrievedData, _, err := dvid.ImageData(retrieved)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, retrievedData)
}
