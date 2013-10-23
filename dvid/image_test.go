package dvid

import (
	"image"

	. "github.com/janelia-flyem/go/gocheck"
)

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF', '\x70'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0', '\x4F'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13', '\xCA'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func makeSlice(offset Point3d, size Point2d) []byte {
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

func (suite *DataSuite) TestSlice(c *C) {
	// Create a fake 100x100 8-bit grayscale image with varying values
	offset := Point3d{3, 13, 24}
	size := Point2d{100, 100}
	data := []uint8(makeSlice(offset, size))
	goImg := ImageGrayFromData(data, int(size[0]), int(size[1]))

	// Create a serializable image and test its de/serialization.
	var img Image
	err := img.Set(goImg)
	c.Assert(err, IsNil)

	serialization, err := img.Serialize(Snappy, CRC32)
	c.Assert(err, IsNil)
	c.Assert(serialization, Not(Equals), nil)

	newImg := new(Image)
	err = newImg.Deserialize(serialization)
	c.Assert(err, IsNil)

	c.Assert(newImg.Which, Equals, uint8(0))
	c.Assert(newImg.Gray, DeepEquals, goImg)
}

func (suite *DataSuite) TestOffsetSlice(c *C) {
	// Create a fake 100x100 8-bit white grayscale image
	// within a larger 200x200 black image.
	goImg := &image.Gray{
		Pix:    make([]uint8, 200*200),
		Stride: 200,
		Rect:   image.Rect(0, 0, 200, 200),
	}
	for y := 50; y <= 150; y++ {
		for x := 50; x <= 150; x++ {
			i := goImg.PixOffset(x, y)
			goImg.Pix[i] = 255
		}
	}

	// Create a serializable image and test its de/serialization.
	var img Image
	err := img.Set(goImg)
	c.Assert(err, IsNil)

	serialization, err := img.Serialize(Snappy, CRC32)
	c.Assert(err, IsNil)
	c.Assert(serialization, Not(Equals), nil)

	newImg := new(Image)
	err = newImg.Deserialize(serialization)
	c.Assert(err, IsNil)

	c.Assert(newImg.Which, Equals, uint8(0))
	c.Assert(newImg.Gray, DeepEquals, goImg)
}

func (suite *DataSuite) TestCompression(c *C) {
	// Create a fake 100x100 8-bit black image
	data := make([]uint8, 100*100)
	goImg := ImageGrayFromData(data, 100, 100)

	// Create a serializable image and test its de/serialization and size
	var img Image
	err := img.Set(goImg)
	c.Assert(err, IsNil)

	serialization, err := img.Serialize(Snappy, CRC32)
	c.Assert(err, IsNil)
	c.Assert(serialization, Not(Equals), nil)

	if len(serialization) >= len(goImg.Pix) {
		c.Errorf("Snappy compressed serialization (%d bytes) of blank image > original %d bytes\n",
			len(serialization), len(goImg.Pix))
	}

	newImg := new(Image)
	err = newImg.Deserialize(serialization)
	c.Assert(err, IsNil)

	c.Assert(newImg.Which, Equals, uint8(0))
	c.Assert(newImg.Gray, DeepEquals, goImg)
}
