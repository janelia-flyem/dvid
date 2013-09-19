package grayscale8

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataSuite struct {
	dir     string
	service *server.Service
	head    datastore.UUID
}

var _ = Suite(&DataSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (suite *DataSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	suite.dir = c.MkDir()

	// Create a new datastore.
	err := datastore.Init(suite.dir, true)
	c.Assert(err, IsNil)

	// Open the datastore
	suite.service, err = server.OpenDatastore(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *DataSuite) TearDownSuite(c *C) {
	suite.service.Shutdown()
}

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF', '\x70'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0', '\x4F'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13', '\xCA'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func MakeSlice(ox, oy, oz, nx, ny int) []byte {
	slice := make([]byte, nx*ny, nx*ny)
	i := 0
	modz := oz % len(zdata)
	for y := 0; y < ny; y++ {
		sy := y + oy
		mody := sy % len(ydata)
		sx := ox
		for x := 0; x < nx; x++ {
			modx := sx % len(xdata)
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

func (suite *DataSuite) sliceTest(c *C, slice voxels.Geometry) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add grayscale data
	versioned := true
	err = suite.service.NewData(root, "grayscale8", "grayscale", versioned)
	c.Assert(err, IsNil)

	dataservice, err := suite.service.DataService(root, "grayscale")
	c.Assert(err, IsNil)

	grayscale, ok := dataservice.(*voxels.Data)
	if !ok {
		c.Errorf("Can't cast grayscale data service into voxels.Data\n")
	}

	// Create a fake 100x100 8-bit grayscale image
	nx := int(slice.Width())
	ny := int(slice.Height())
	origin := slice.Origin()
	ox, oy, oz := int(origin[0]), int(origin[1]), int(origin[2])
	data := []uint8(MakeSlice(ox, oy, oz, nx, ny))
	img := dvid.ImageGrayFromData(data, nx, ny)

	// Store it into datastore at root
	_, versionID, err := suite.service.LocalIDFromUUID(root)
	err = grayscale.PutImage(versionID, img, slice)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := grayscale.GetImage(versionID, slice)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(img.Rect, DeepEquals, retrieved.Bounds())
	retrievedData, _, err := dvid.ImageData(retrieved)
	c.Assert(err, IsNil)
	c.Assert(retrievedData, DeepEquals, data)
}

func (suite *DataSuite) TestXYSliceGrayscale8(c *C) {
	nx := 100
	ny := 100
	ox, oy, oz := 3, 13, 24
	slice := voxels.NewSliceXY(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})
	suite.sliceTest(c, slice)
}

func (suite *DataSuite) TestXZSliceGrayscale8(c *C) {
	nx := 100
	ny := 100
	ox, oy, oz := 31, 10, 14
	slice := voxels.NewSliceXZ(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})
	suite.sliceTest(c, slice)
}

func (suite *DataSuite) TestYZSliceGrayscale8(c *C) {
	nx := 100
	ny := 100
	ox, oy, oz := 13, 40, 99
	slice := voxels.NewSliceYZ(voxels.Coord{int32(ox), int32(oy), int32(oz)},
		voxels.Point2d{int32(nx), int32(ny)})
	suite.sliceTest(c, slice)
}
