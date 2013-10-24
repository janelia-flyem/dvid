package voxels

import (
	. "github.com/janelia-flyem/go/gocheck"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
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

// Make sure new grayscale8 data have different IDs.
func (suite *DataSuite) TestNewDataDifferent(c *C) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add grayscale data
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "grayscale8", "image1", config)
	c.Assert(err, IsNil)

	dataservice1, err := suite.service.DataService(root, "image1")
	c.Assert(err, IsNil)

	err = suite.service.NewData(root, "grayscale8", "image2", config)
	c.Assert(err, IsNil)

	dataservice2, err := suite.service.DataService(root, "image2")
	c.Assert(err, IsNil)

	c.Assert(dataservice1.DatasetID(), Equals, dataservice2.DatasetID())

	c.Assert(dataservice1.LocalID(), Not(Equals), dataservice2.LocalID())
}

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF', '\x70'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0', '\x4F'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13', '\xCA'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func MakeSlice(offset dvid.Point3d, size dvid.Point2d) []byte {
	slice := make([]byte, size[0]*size[1], size[0]*size[1])
	i := 0
	modz := offset[2] % int32(len(zdata))
	for y := int32(0); y < size[1]; y++ {
		sy := y + offset[1]
		mody := sy % int32(len(ydata))
		sx := offset[0]
		for x := int32(0); x < size[0]; x++ {
			modx := sx % int32(len(xdata))
			slice[i] = xdata[modx] ^ ydata[mody] ^ zdata[modz]
			i++
			sx++
		}
	}
	return slice
}

// Make a 3d volume of bytes with top left corner at (ox,oy,oz) and size (nx, ny, nz)
func MakeVolume(offset, size dvid.Point3d) []byte {
	sliceBytes := size[0] * size[1]
	volumeBytes := sliceBytes * size[2]
	volume := make([]byte, volumeBytes, volumeBytes)
	var i int32
	size2d := dvid.Point2d{size[0], size[1]}
	for z := offset[2]; z < offset[2]+size[2]; z++ {
		offset[2] = z
		copy(volume[i:i+sliceBytes], MakeSlice(offset, size2d))
		i += sliceBytes
	}
	return volume
}

func (suite *DataSuite) sliceTest(c *C, slice dvid.Geometry) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add grayscale data
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err = suite.service.NewData(root, "grayscale8", "grayscale", config)
	c.Assert(err, IsNil)

	dataservice, err := suite.service.DataService(root, "grayscale")
	c.Assert(err, IsNil)

	grayscale, ok := dataservice.(*Data)
	if !ok {
		c.Errorf("Can't cast grayscale data service into Data\n")
	}

	// Create a fake 100x100 8-bit grayscale image
	nx := slice.Size().Value(0)
	ny := slice.Size().Value(1)
	offset := slice.StartPoint().(dvid.Point3d)
	data := []uint8(MakeSlice(offset, dvid.Point2d{nx, ny}))
	img := dvid.ImageGrayFromData(data, int(nx), int(ny))

	// Store it into datastore at root
	v, err := grayscale.NewVoxelHandler(slice, img)
	c.Assert(err, IsNil)

	err = grayscale.PutImage(root, v)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := grayscale.GetImage(root, v)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(img.Rect, DeepEquals, retrieved.Bounds())
	retrievedData, _, err := dvid.ImageData(retrieved)
	c.Assert(err, IsNil)
	c.Assert(retrievedData, DeepEquals, data)
}

func (suite *DataSuite) TestXYSliceGrayscale8(c *C) {
	offset := dvid.Point3d{3, 13, 24}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}

func (suite *DataSuite) TestXZSliceGrayscale8(c *C) {
	offset := dvid.Point3d{31, 10, 14}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}

func (suite *DataSuite) TestYZSliceGrayscale8(c *C) {
	offset := dvid.Point3d{13, 40, 99}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}
