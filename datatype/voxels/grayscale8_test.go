package voxels

import (
	"testing"
	. "github.com/janelia-flyem/go/gocheck"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	dir     string
	service *server.Service
	head    dvid.UUID
}

var _ = Suite(&TestSuite{})

// This will setup a new datastore and open it up, keeping the UUID and
// service pointer in the DataSuite.
func (suite *TestSuite) SetUpSuite(c *C) {
	// Make a temporary testing directory that will be auto-deleted after testing.
	suite.dir = c.MkDir()

	// Create a new datastore.
	err := datastore.Init(suite.dir, true, dvid.Config{})
	c.Assert(err, IsNil)

	// Open the datastore
	suite.service, err = server.OpenDatastore(suite.dir)
	c.Assert(err, IsNil)
}

func (suite *TestSuite) TearDownSuite(c *C) {
	suite.service.Shutdown()
}

// Make sure new grayscale8 data have different IDs.
func (suite *TestSuite) TestNewDataDifferent(c *C) {
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

	data1, ok := dataservice1.(*Data)
	c.Assert(ok, Equals, true)

	data2, ok := dataservice2.(*Data)
	c.Assert(ok, Equals, true)

	c.Assert(data1.DsetID, Equals, data2.DsetID)
	c.Assert(data1.ID, Not(Equals), data2.ID)
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
	startZ := offset[2]
	endZ := startZ + size[2]
	for z := startZ; z < endZ; z++ {
		offset[2] = z
		copy(volume[i:i+sliceBytes], MakeSlice(offset, size2d))
		i += sliceBytes
	}
	return volume
}

func (suite *TestSuite) makeGrayscale(c *C, root dvid.UUID, name dvid.DataString) *Data {
	config := dvid.NewConfig()
	config.SetVersioned(true)

	err := suite.service.NewData(root, "grayscale8", name, config)
	c.Assert(err, IsNil)

	dataservice, err := suite.service.DataService(root, dvid.DataString(name))
	c.Assert(err, IsNil)

	grayscale, ok := dataservice.(*Data)
	if !ok {
		c.Errorf("Can't cast grayscale8 data service into Data\n")
	}
	return grayscale
}

func (suite *TestSuite) TestSubvolGrayscale8(c *C) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add grayscale data
	grayscale := suite.makeGrayscale(c, root, "grayscale")

	// Create a fake 100x100x100 8-bit grayscale image
	offset := dvid.Point3d{5, 35, 61}
	size := dvid.Point3d{100, 100, 100}
	subvol := dvid.NewSubvolume(offset, size)
	data := MakeVolume(offset, size)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Store it into datastore at root
	v, err := grayscale.NewExtHandler(subvol, data)
	c.Assert(err, IsNil)

	err = PutVoxels(root, grayscale, v)
	c.Assert(err, IsNil)
	c.Assert(v.NumVoxels(), Equals, int64(len(origData)))

	// Read the stored image
	v2, err := grayscale.NewExtHandler(subvol, nil)
	c.Assert(err, IsNil)
	err = GetVoxels(root, grayscale, v2)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	c.Assert(err, IsNil)
	c.Assert(v.Stride(), Equals, v2.Stride())
	c.Assert(v.ByteOrder(), Equals, v2.ByteOrder())
	c.Assert(v.Interpolable(), Equals, v2.Interpolable())
	c.Assert(v.Size(), DeepEquals, v2.Size())
	c.Assert(v.NumVoxels(), Equals, v2.NumVoxels())
	data = v2.Data()
	for i := int64(0); i < v2.NumVoxels(); i++ {
		if data[i] != origData[i] {
			c.Errorf("GET subvol != PUT subvol @ index %d", i)
			break
		}
	}
}

func (suite *TestSuite) sliceTest(c *C, slice dvid.Geometry) {
	// Create a new dataset
	root, _, err := suite.service.NewDataset()
	c.Assert(err, IsNil)

	// Add grayscale data
	grayscale := suite.makeGrayscale(c, root, "grayscale")

	// Create a fake 100x100 8-bit grayscale image
	nx := slice.Size().Value(0)
	ny := slice.Size().Value(1)
	offset := slice.StartPoint().(dvid.Point3d)
	data := []uint8(MakeSlice(offset, dvid.Point2d{nx, ny}))
	img := dvid.ImageGrayFromData(data, int(nx), int(ny))

	// Store it into datastore at root
	v, err := grayscale.NewExtHandler(slice, img)
	c.Assert(err, IsNil)

	err = PutVoxels(root, grayscale, v)
	c.Assert(err, IsNil)

	// Read the stored image
	retrieved, err := GetImage(root, grayscale, v)
	c.Assert(err, IsNil)

	// Make sure the retrieved image matches the original
	goImg := retrieved.Get()
	c.Assert(img.Rect, DeepEquals, goImg.Bounds())
	retrievedData, voxelSize, _, err := dvid.ImageData(goImg)
	c.Assert(err, IsNil)
	c.Assert(retrievedData, DeepEquals, data)
	c.Assert(voxelSize, Equals, int32(1))
}

func (suite *TestSuite) TestXYSliceGrayscale8(c *C) {
	offset := dvid.Point3d{3, 13, 24}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}

func (suite *TestSuite) TestXZSliceGrayscale8(c *C) {
	offset := dvid.Point3d{31, 10, 14}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}

func (suite *TestSuite) TestYZSliceGrayscale8(c *C) {
	offset := dvid.Point3d{13, 40, 99}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, size)
	c.Assert(err, IsNil)
	suite.sliceTest(c, slice)
}
