/*
	This file supports image operations in DVID.  Images can act as containers for elements that
	can have a number of values per element.

	Standard images are convenient ways to transmit simple data types in 2d/3d arrays because
	clients have good implementations of reading and writing them.  For example, javascript web clients
	can easily GET compressed images.  The Janelia Raveler program used PNG images to hold 24+ bit labels
	so some of their use was through legacy applications.

	DVID supports packaging of data into standard images to some extent.  Once the data being stored per
	pixel/voxel becomes sufficiently complex, it makes no sense to force the data into a standard image
	container.  At that point, a generic binary container, e.g., schema + binary data, or a standard like
	HDF5 should be used.  HDF5 suffers from complexity and the lack of support within javascript clients.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/janelia-flyem/go/go.image/bmp"
	"github.com/janelia-flyem/go/go.image/tiff"

	"github.com/janelia-flyem/go/freetype-go/freetype"
	"github.com/janelia-flyem/go/freetype-go/freetype/raster"
	"github.com/janelia-flyem/go/freetype-go/freetype/truetype"
)

var (
	Font *truetype.Font
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Image{})
	gob.Register(&image.Gray{})
	gob.Register(&image.Gray16{})
	gob.Register(&image.NRGBA{})
	gob.Register(&image.NRGBA64{})

	// Initialize font from inlined ttf font data
	var err error
	Font, err = freetype.ParseFont(fontBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create font from font bytes!")
	}
}

// DefaultJPEGQuality is the quality of images returned if requesting JPEG images
// and an explicit Quality amount is omitted.
const DefaultJPEGQuality = 80

// Image contains a standard Go image as well as a data format description so non-standard
// image values like uint64 labels or uint32 intensities can be handled.  A DVID image also
// knows whether it should be interpolated on resizing or must keep pixel values without
// interpolation, e.g., when using labels.  Better Gob serialization is handled by a union of
// possible image types compared to a generic image.Image interface:
// see https://groups.google.com/d/msg/golang-dev/_t4pqoeuflE/DbqSf41wr5EJ
type Image struct {
	DataFormat   DataValues
	Interpolable bool

	Which   uint8
	Gray    *image.Gray
	Gray16  *image.Gray16
	NRGBA   *image.NRGBA
	NRGBA64 *image.NRGBA64
}

// Get returns an image.Image from the union struct.
func (img Image) Get() image.Image {
	switch img.Which {
	case 0:
		return img.Gray
	case 1:
		return img.Gray16
	case 2:
		return img.NRGBA
	case 3:
		return img.NRGBA64
	default:
		return nil
	}
}

// Get returns an image.Image from the union struct.
func (img Image) GetDrawable() draw.Image {
	switch img.Which {
	case 0:
		return img.Gray
	case 1:
		return img.Gray16
	case 2:
		return img.NRGBA
	case 3:
		return img.NRGBA64
	default:
		return nil
	}
}

// Set initializes a DVID image from a go image and a data format specification.  DVID images
// must have identical data type values within a pixel..
func (img *Image) Set(src image.Image, format DataValues, interpolable bool) error {
	img.DataFormat = format
	img.Interpolable = interpolable

	valuesPerElement := format.ValuesPerElement()
	bytesPerValue, err := format.BytesPerValue()
	if err != nil {
		return err
	}

	switch s := src.(type) {
	case *image.Gray:
		img.Which = 0
		img.Gray = s
		if valuesPerElement != 1 || bytesPerValue != 1 {
			return fmt.Errorf("Tried to use image.Gray (8-bit) to represent %d values of %d bytes/value",
				valuesPerElement, bytesPerValue)
		}
	case *image.Gray16:
		img.Which = 1
		img.Gray16 = s
		if valuesPerElement != 1 || bytesPerValue != 2 {
			return fmt.Errorf("Tried to use image.Gray16 (16-bit) to represent %d values of %d bytes/value",
				valuesPerElement, bytesPerValue)
		}
	case *image.NRGBA:
		img.Which = 2
		img.NRGBA = s
		if !((valuesPerElement == 1 && bytesPerValue == 4) || (valuesPerElement == 4 && bytesPerValue == 1)) {
			return fmt.Errorf("Tried to use image.NRGBA (32-bit) to represent %d values of %d bytes/value",
				valuesPerElement, bytesPerValue)
		}
	case *image.NRGBA64:
		img.Which = 3
		img.NRGBA64 = s
		if !((valuesPerElement == 1 && bytesPerValue == 8) || (valuesPerElement == 4 && bytesPerValue == 2)) {
			return fmt.Errorf("Tried to use image.NRGBA64 (64-bit) to represent %d values of %d bytes/value",
				valuesPerElement, bytesPerValue)
		}
	default:
		return fmt.Errorf("No valid image type received by image.Set(): %s", reflect.TypeOf(src))
	}
	return nil
}

// Data returns a slice of bytes corresponding to the image pixels.
func (img *Image) Data() []uint8 {
	switch img.Which {
	case 0:
		return img.Gray.Pix
	case 1:
		return img.Gray16.Pix
	case 2:
		return img.NRGBA.Pix
	case 3:
		return img.NRGBA64.Pix
	default:
		return nil
	}
}

// SubImage returns an image representing the portion of the image p visible through r.
// The returned image shares pixels with the original image.
func (img *Image) SubImage(r image.Rectangle) (*Image, error) {
	result := new(Image)
	result.DataFormat = img.DataFormat
	result.Which = img.Which
	switch img.Which {
	case 0:
		result.Gray = img.Gray.SubImage(r).(*image.Gray)
	case 1:
		result.Gray16 = img.Gray16.SubImage(r).(*image.Gray16)
	case 2:
		result.NRGBA = img.NRGBA.SubImage(r).(*image.NRGBA)
	case 3:
		result.NRGBA64 = img.NRGBA64.SubImage(r).(*image.NRGBA64)
	default:
		return nil, fmt.Errorf("Unsupported image type %d asked for SubImage()", img.Which)
	}
	return result, nil
}

// Serialize writes optional compressed and checksummed bytes representing image data.
func (img *Image) Serialize(compress Compression, checksum Checksum) ([]byte, error) {
	b, err := img.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return SerializeData(b, compress, checksum)
}

// Deserialze deserializes an Image from a possibly compressioned, checksummed byte slice.
func (img *Image) Deserialize(b []byte) error {
	if img == nil {
		return fmt.Errorf("Error attempting to deserialize into nil Image")
	}

	// Unpackage the data using compression and built-in checksum.
	data, _, err := DeserializeData(b, true)
	if err != nil {
		return err
	}

	return img.UnmarshalBinary(data)
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (img *Image) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer

	// Serialize the data format
	b, err := img.DataFormat.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(b))); err != nil {
		return nil, err
	}
	_, err = buffer.Write(b)

	// Serialize the image portion.
	err = buffer.WriteByte(byte(img.Which))
	if err != nil {
		return nil, err
	}

	var stride, bytesPerPixel int
	var rect image.Rectangle
	var pix, src []uint8
	var pixOffset func(x, y int) int

	switch img.Which {
	case 0:
		stride = img.Gray.Stride
		rect = img.Gray.Rect
		bytesPerPixel = 1
		src = img.Gray.Pix
		pixOffset = img.Gray.PixOffset

	case 1:
		stride = img.Gray16.Stride
		rect = img.Gray16.Rect
		bytesPerPixel = 2
		src = img.Gray16.Pix
		pixOffset = img.Gray16.PixOffset

	case 2:
		stride = img.NRGBA.Stride
		rect = img.NRGBA.Rect
		bytesPerPixel = 4
		src = img.NRGBA.Pix
		pixOffset = img.NRGBA.PixOffset

	case 3:
		stride = img.NRGBA64.Stride
		rect = img.NRGBA64.Rect
		bytesPerPixel = 8
		src = img.NRGBA64.Pix
		pixOffset = img.NRGBA64.PixOffset
	}

	// Make sure the byte slice is compact and not harboring any offsets
	if stride == bytesPerPixel*rect.Dx() && rect.Min.X == 0 && rect.Min.Y == 0 {
		pix = src
	} else {
		dx := rect.Dx()
		dy := rect.Dy()
		rowbytes := bytesPerPixel * dx
		totbytes := rowbytes * dy
		pix = make([]uint8, totbytes)
		dstI := 0
		for y := rect.Min.Y; y < rect.Max.Y; y++ {
			srcI := pixOffset(rect.Min.X, y)
			copy(pix[dstI:dstI+rowbytes], src[srcI:srcI+rowbytes])
			dstI += rowbytes
		}
		stride = rowbytes
		rect = image.Rect(0, 0, dx, dy)
	}

	if err := binary.Write(&buffer, binary.LittleEndian, int32(stride)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, int32(rect.Dx())); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, int32(rect.Dy())); err != nil {
		return nil, err
	}
	_, err = buffer.Write(pix)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (img *Image) UnmarshalBinary(b []byte) error {
	// Deserialize the data format
	lenDataFormat := int(binary.LittleEndian.Uint32(b[0:4]))
	img.DataFormat.UnmarshalBinary(b[4 : 4+lenDataFormat])

	// Get the image type.
	buffer := bytes.NewBuffer(b[4+lenDataFormat:])
	imageType, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	img.Which = uint8(imageType)

	// Get the stride and sizes.
	var stride int32
	if err = binary.Read(buffer, binary.LittleEndian, &stride); err != nil {
		return err
	}

	var dx, dy int32
	err = binary.Read(buffer, binary.LittleEndian, &dx)
	if err != nil {
		return err
	}
	err = binary.Read(buffer, binary.LittleEndian, &dy)
	if err != nil {
		return err
	}
	rect := image.Rect(0, 0, int(dx), int(dy))
	pix := []uint8(buffer.Bytes())

	switch img.Which {
	case 0:
		img.Gray = &image.Gray{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}

	case 1:
		img.Gray16 = &image.Gray16{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}

	case 2:
		img.NRGBA = &image.NRGBA{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}

	case 3:
		img.NRGBA64 = &image.NRGBA64{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}
	}
	return nil
}

// ScaleImage scales a DVID image to the destination geometry size, using nearest-neighbor or
// interpolation depending on the type of data.
func (img *Image) ScaleImage(geom Geometry) (*Image, error) {
	var goImg image.Image
	var err error

	if img.Interpolable {
		goImg, err = img.InterpolateImage(geom)
	} else {
		goImg, err = img.ResizeImage(geom)
	}
	if err != nil {
		return nil, err
	}

	dst := new(Image)
	if err = dst.Set(goImg, img.DataFormat, img.Interpolable); err != nil {
		return nil, err
	}
	return dst, nil
}

// ResizeImage returns an image scaled to the given geometry without doing
// interpolation.
func (img *Image) ResizeImage(geom Geometry) (image.Image, error) {
	if img == nil {
		return nil, fmt.Errorf("Attempted to resize nil DVID image.")
	}

	// Get dimensions
	dstW := int(geom.Size().Value(0))
	dstH := int(geom.Size().Value(1))
	src := img.Get()
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()
	if srcW == dstW && srcH == dstH {
		return src, nil
	}
	if dstW <= 0 || dstH <= 0 {
		return nil, fmt.Errorf("Attempted to resize to %d x %d pixels", dstW, dstH)
	}
	if srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("Attempted to resize source image of %d x %d pixels", srcW, srcH)
	}

	// Perform interpolation based on # values and bytes/value.
	valuesPerElement := img.DataFormat.ValuesPerElement()
	bytesPerValue, err := img.DataFormat.BytesPerValue()
	if err != nil {
		return nil, err
	}
	switch valuesPerElement {
	case 1:
		switch bytesPerValue {
		case 1:
			return resize1x8(img.Gray, dstW, dstH), nil
		case 2:
			return resize1x16(img.Gray16, dstW, dstH), nil
		case 4:
			return resize32(img.NRGBA, dstW, dstH), nil
		case 8:
			return resize64(img.NRGBA64, dstW, dstH), nil
		}
	case 4:
		switch bytesPerValue {
		case 1:
			return resize32(img.NRGBA, dstW, dstH), nil
		case 2:
			return resize64(img.NRGBA64, dstW, dstH), nil
		}
	}
	return nil, fmt.Errorf("Illegal image format for interpolation: %d values with %d bytes/value",
		valuesPerElement, bytesPerValue)
}

func resize1x8(src *image.Gray, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	dstW64, dstH64 := uint64(dstW), uint64(dstH)
	srcW64, srcH64 := uint64(srcW), uint64(srcH)

	dst := image.NewGray(image.Rect(0, 0, dstW, dstH))
	var x, y uint64
	dstI := 0
	for y = 0; y < dstH64; y++ {
		srcY := int(y * srcH64 / dstH64)
		for x = 0; x < dstW64; x++ {
			srcX := int(x * srcW64 / dstW64)
			dst.Pix[dstI] = src.Pix[srcY*srcW+srcX]
			dstI++
		}
	}
	return dst
}

func resize1x16(src *image.Gray16, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	dstW64, dstH64 := uint64(dstW), uint64(dstH)
	srcW64, srcH64 := uint64(srcW), uint64(srcH)

	dst := image.NewGray16(image.Rect(0, 0, dstW, dstH))
	var x, y uint64
	dstI := 0
	for y = 0; y < dstH64; y++ {
		srcY := int(y * srcH64 / dstH64)
		for x = 0; x < dstW64; x++ {
			srcX := int(x * srcW64 / dstW64)
			srcI := 2 * (srcY*srcW + srcX)
			copy(dst.Pix[dstI:dstI+2], src.Pix[srcI:srcI+2])
			dstI += 2
		}
	}
	return dst
}

func resize32(src *image.NRGBA, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	dstW64, dstH64 := uint64(dstW), uint64(dstH)
	srcW64, srcH64 := uint64(srcW), uint64(srcH)

	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	var x, y uint64
	dstI := 0
	for y = 0; y < dstH64; y++ {
		srcY := int(y * srcH64 / dstH64)
		for x = 0; x < dstW64; x++ {
			srcX := int(x * srcW64 / dstW64)
			srcI := 4 * (srcY*srcW + srcX)
			copy(dst.Pix[dstI:dstI+4], src.Pix[srcI:srcI+4])
			dstI += 4
		}
	}
	return dst
}

func resize64(src *image.NRGBA64, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	dstW64, dstH64 := uint64(dstW), uint64(dstH)
	srcW64, srcH64 := uint64(srcW), uint64(srcH)

	dst := image.NewNRGBA64(image.Rect(0, 0, dstW, dstH))
	var x, y uint64
	dstI := 0
	for y = 0; y < dstH64; y++ {
		srcY := int(y * srcH64 / dstH64)
		for x = 0; x < dstW64; x++ {
			srcX := int(x * srcW64 / dstW64)
			srcI := 8 * (srcY*srcW + srcX)
			copy(dst.Pix[dstI:dstI+8], src.Pix[srcI:srcI+8])
			dstI += 8
		}
	}
	return dst
}

// InterpolateImage returns an image scaled to the given geometry using simple
// nearest-neighbor interpolation.
func (img *Image) InterpolateImage(geom Geometry) (image.Image, error) {
	if img == nil {
		return nil, fmt.Errorf("Attempted to interpolate nil DVID image.")
	}

	// Get dimensions
	dstW := int(geom.Size().Value(0))
	dstH := int(geom.Size().Value(1))
	src := img.Get()
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()
	if srcW == dstW && srcH == dstH {
		return src, nil
	}
	if dstW <= 0 || dstH <= 0 {
		return nil, fmt.Errorf("Attempted to interpolate to %d x %d pixels", dstW, dstH)
	}
	if srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("Attempted to interpolate source image of %d x %d pixels", srcW, srcH)
	}

	// Perform interpolation based on # values and bytes/value.
	valuesPerElement := img.DataFormat.ValuesPerElement()
	bytesPerValue, err := img.DataFormat.BytesPerValue()
	if err != nil {
		return nil, err
	}
	switch valuesPerElement {
	case 1:
		switch bytesPerValue {
		case 1:
			return interpolate1x8(img.Gray, dstW, dstH), nil
		case 2:
			return interpolate1x16(img.Gray16, dstW, dstH), nil
		case 4:
			return interpolate1x32(img.NRGBA, dstW, dstH), nil
		case 8:
			return interpolate1x64(img.NRGBA64, dstW, dstH), nil
		}
	case 4:
		switch bytesPerValue {
		case 1:
			return interpolate4x8(img.NRGBA, dstW, dstH), nil
		case 2:
			return interpolate4x16(img.NRGBA64, dstW, dstH), nil
		}
	}
	return nil, fmt.Errorf("Illegal image format for interpolation: %d values with %d bytes/value",
		valuesPerElement, bytesPerValue)
}

// The interpolate code below was adapted from the AppEngine moustachio example and should be among
// the more efficient resizers without resorting to more sophisticated interpolation than
// nearest-neighbor.
//
// http://code.google.com/p/appengine-go/source/browse/example/moustachio/resize/resize.go
//
// The scaling algorithm is to nearest-neighbor magnify the dx * dy source
// to a (ww*dx) * (hh*dy) intermediate image and then minify the intermediate
// image back down to a ww * hh destination with a simple box filter.
// The intermediate image is implied, we do not physically allocate a slice
// of length ww*dx*hh*dy.
// For example, consider a 4*3 source image. Label its pixels from a-l:
//      abcd
//      efgh
//      ijkl
// To resize this to a 3*2 destination image, the intermediate is 12*6.
// Whitespace has been added to delineate the destination pixels:
//      aaab bbcc cddd
//      aaab bbcc cddd
//      eeef ffgg ghhh
//
//      eeef ffgg ghhh
//      iiij jjkk klll
//      iiij jjkk klll
// Thus, the 'b' source pixel contributes one third of its value to the
// (0, 0) destination pixel and two thirds to (1, 0).
// The implementation is a two-step process. First, the source pixels are
// iterated over and each source pixel's contribution to 1 or more
// destination pixels are summed. Second, the sums are divided by a scaling
// factor to yield the destination pixels.
// TODO: By interleaving the two steps, instead of doing all of
// step 1 first and all of step 2 second, we could allocate a smaller sum
// slice of length 4*w*2 instead of 4*w*h, although the resultant code
// would become more complicated.
//
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Interpolate uint8/pixel images.
func interpolate1x8(src *image.Gray, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			val64 := uint64(src.Pix[pixOffset])
			pixOffset++

			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := (py/dy)*ww + (px / dx)
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index] += val64 * qxy
					index++
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewGray(image.Rect(0, 0, dstW, dstH))
	index := 0
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		for x := 0; x < dstW; x++ {
			dst.Pix[pixOffset] = uint8(sum[index] / n)
			pixOffset++
			index++
		}
	}
	return dst
}

// Interpolate uint16/pixel images.
func interpolate1x16(src *image.Gray16, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			val64 := uint64(binary.BigEndian.Uint16([]byte(src.Pix[pixOffset+0 : pixOffset+2])))
			pixOffset += 2

			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := (py/dy)*ww + (px / dx)
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index] += val64 * qxy
					index++
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewGray16(image.Rect(0, 0, dstW, dstH))
	index := 0
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		for x := 0; x < dstW; x++ {
			binary.BigEndian.PutUint16(dst.Pix[pixOffset+0:pixOffset+2], uint16(sum[index]/n))
			pixOffset += 2
			index++
		}
	}
	return dst
}

// Interpolate uint32/pixel images.
func interpolate1x32(src *image.NRGBA, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			val64 := uint64(binary.BigEndian.Uint32([]byte(src.Pix[pixOffset+0 : pixOffset+4])))
			pixOffset += 4

			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := (py/dy)*ww + (px / dx)
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index] += val64 * qxy
					index++
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	index := 0
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		for x := 0; x < dstW; x++ {
			binary.BigEndian.PutUint32(dst.Pix[pixOffset+0:pixOffset+4], uint32(sum[index]/n))
			pixOffset += 4
			index++
		}
	}
	return dst
}

// Interpolate uint64/pixel images.
func interpolate1x64(src *image.NRGBA64, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			val64 := binary.BigEndian.Uint64([]byte(src.Pix[pixOffset+0 : pixOffset+8]))
			pixOffset += 8

			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := (py/dy)*ww + (px / dx)
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index] += val64 * qxy
					index++
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewNRGBA64(image.Rect(0, 0, dstW, dstH))
	index := 0
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		for x := 0; x < dstW; x++ {
			binary.BigEndian.PutUint64(dst.Pix[pixOffset+0:pixOffset+8], sum[index]/n)
			pixOffset += 8
			index++
		}
	}
	return dst
}

// Interpolate 4 interleaved uint8 per pixel images.
func interpolate4x8(src *image.NRGBA, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, 4*dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			r64 := uint64(src.Pix[pixOffset+0])
			g64 := uint64(src.Pix[pixOffset+1])
			b64 := uint64(src.Pix[pixOffset+2])
			a64 := uint64(src.Pix[pixOffset+3])
			pixOffset += 4
			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := 4 * ((py/dy)*ww + (px / dx))
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index+0] += r64 * qxy
					sum[index+1] += g64 * qxy
					sum[index+2] += b64 * qxy
					sum[index+3] += a64 * qxy
					index += 4
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		for x := 0; x < dstW; x++ {
			dst.Pix[pixOffset+0] = uint8(sum[pixOffset+0] / n)
			dst.Pix[pixOffset+1] = uint8(sum[pixOffset+1] / n)
			dst.Pix[pixOffset+2] = uint8(sum[pixOffset+2] / n)
			dst.Pix[pixOffset+3] = uint8(sum[pixOffset+3] / n)
			pixOffset += 4
		}
	}
	return dst
}

// Interpolate 4 interleaved uint16 per pixel images.
func interpolate4x16(src *image.NRGBA64, dstW, dstH int) image.Image {
	srcRect := src.Bounds()
	srcW := srcRect.Dx()
	srcH := srcRect.Dy()

	ww, hh := uint64(dstW), uint64(dstH)
	dx, dy := uint64(srcW), uint64(srcH)

	n, sum := dx*dy, make([]uint64, 4*dstW*dstH)
	for y := 0; y < srcH; y++ {
		pixOffset := src.PixOffset(0, y)
		for x := 0; x < srcW; x++ {
			// Get the source pixel.
			r64 := uint64(binary.BigEndian.Uint16([]byte(src.Pix[pixOffset+0 : pixOffset+2])))
			g64 := uint64(binary.BigEndian.Uint16([]byte(src.Pix[pixOffset+2 : pixOffset+4])))
			b64 := uint64(binary.BigEndian.Uint16([]byte(src.Pix[pixOffset+4 : pixOffset+6])))
			a64 := uint64(binary.BigEndian.Uint16([]byte(src.Pix[pixOffset+6 : pixOffset+8])))
			pixOffset += 8
			// Spread the source pixel over 1 or more destination rows.
			py := uint64(y) * hh
			for remy := hh; remy > 0; {
				qy := dy - (py % dy)
				if qy > remy {
					qy = remy
				}
				// Spread the source pixel over 1 or more destination columns.
				px := uint64(x) * ww
				index := 4 * ((py/dy)*ww + (px / dx))
				for remx := ww; remx > 0; {
					qx := dx - (px % dx)
					if qx > remx {
						qx = remx
					}
					qxy := qx * qy
					sum[index+0] += r64 * qxy
					sum[index+1] += g64 * qxy
					sum[index+2] += b64 * qxy
					sum[index+3] += a64 * qxy
					index += 4
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	dst := image.NewNRGBA64(image.Rect(0, 0, dstW, dstH))
	for y := 0; y < dstH; y++ {
		pixOffset := dst.PixOffset(0, y)
		index := 4 * y * dstW
		for x := 0; x < dstW; x++ {
			binary.BigEndian.PutUint16(dst.Pix[pixOffset+0:pixOffset+2], uint16(sum[index+0]/n))
			binary.BigEndian.PutUint16(dst.Pix[pixOffset+2:pixOffset+4], uint16(sum[index+1]/n))
			binary.BigEndian.PutUint16(dst.Pix[pixOffset+4:pixOffset+6], uint16(sum[index+2]/n))
			binary.BigEndian.PutUint16(dst.Pix[pixOffset+6:pixOffset+8], uint16(sum[index+3]/n))
			pixOffset += 8
			index += 4
		}
	}
	return dst
}

////////////////////////////////////////////////////////////////
//
//  General image support through package functions
//
////////////////////////////////////////////////////////////////

// PlaceholderImage returns an solid image with a message and text describing the shape.
func PlaceholderImage(shape DataShape, imageSize Point, message string) (image.Image, error) {
	dx, dy, err := shape.GetSize2D(imageSize)
	if err != nil {
		return nil, err
	}
	size := float64(12)
	spacing := float64(1.5)

	// Initialize the context.
	fg, bg := image.Black, image.White
	ruler := color.NRGBA{0xdd, 0xdd, 0xdd, 0xff}
	// White on black
	// fg, bg = image.White, image.Black
	// ruler = color.NRGBA{0x22, 0x22, 0x22, 0xff}
	rgba := image.NewNRGBA(image.Rect(0, 0, int(dx), int(dy)))
	draw.Draw(rgba, rgba.Bounds(), bg, image.ZP, draw.Src)
	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(Font)
	c.SetFontSize(size)
	c.SetClip(rgba.Bounds())
	c.SetDst(rgba)
	c.SetSrc(fg)

	// Draw the guidelines.
	for x := 10; x < int(dx)-10; x++ {
		rgba.Set(x, 10, ruler)
	}
	for y := 10; y < int(dy)-10; y++ {
		rgba.Set(10, y, ruler)
	}

	// Write axis labels.
	rasterToInt := func(f32 raster.Fix32) int {
		return int(f32 >> 8)
	}
	fontY := c.PointToFix32(size * spacing)
	y := 10 + rasterToInt(c.PointToFix32(size))
	pt := freetype.Pt(int(dx)-10, y)
	_, err = c.DrawString(shape.AxisName(0), pt)
	if err != nil {
		return nil, err
	}
	pt = freetype.Pt(10, int(dy)-rasterToInt(fontY))
	_, err = c.DrawString(shape.AxisName(1), pt)
	if err != nil {
		return nil, err
	}

	// Draw the text.
	pt = freetype.Pt(15, y+rasterToInt(fontY))
	_, err = c.DrawString(message, pt)
	if err != nil {
		return nil, err
	}
	pt.Y += fontY
	sizeStr := fmt.Sprintf("%d x %d pixels", dx, dy)
	_, err = c.DrawString(sizeStr, pt)
	if err != nil {
		return nil, err
	}

	return rgba, nil
}

// ImageData returns the underlying pixel data for an image or an error if
// the image doesn't have the requisite []uint8 pixel data.
func ImageData(img image.Image) (data []uint8, bytesPerPixel, stride int32, err error) {
	switch typedImg := img.(type) {
	case *image.Gray:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 1
	case *image.Gray16:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 2
	case *image.RGBA:
		fmt.Println("image.RGBA in ImageData()")
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 4
	case *image.NRGBA:
		fmt.Println("image.NRGBA in ImageData()")
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 4
	case *image.NRGBA64:
		fmt.Println("image.NRGBA64 in ImageData()")
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 8
	default:
		err = fmt.Errorf("Illegal image type called ImageData(): %T", typedImg)
	}
	return
}

// ImageFromFile returns an image and its format name given a file name.
func ImageFromFile(filename string) (img image.Image, format string, err error) {
	var file *os.File
	file, err = os.Open(filename)
	if err != nil {
		err = fmt.Errorf("Unable to open image (%s).  Is this visible to server process?",
			filename)
		return
	}
	img, format, err = image.Decode(file)
	if err != nil {
		return
	}
	err = file.Close()
	return
}

// ImageFromForm returns an image and its format name given a key to a POST request.
// The image should be the first file in a POSTed form.
func ImageFromForm(r *http.Request, key string) (img image.Image, format string, err error) {
	f, _, err := r.FormFile(key)
	if err != nil {
		return nil, "none", err
	}
	defer f.Close()
	return image.Decode(f)
}

// ImageFromPOST returns an image and its format name given a POST request.
func ImageFromPOST(r *http.Request) (img image.Image, format string, err error) {
	var data []byte
	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, "none", err
	}
	buf := bytes.NewBuffer(data)
	return image.Decode(buf)
}

// ImageGrayFromData returns a Gray image given data and image size.
func ImageGrayFromData(data []uint8, nx, ny int) (img *image.Gray) {
	img = &image.Gray{
		Pix:    data,
		Stride: nx,
		Rect:   image.Rect(0, 0, nx, ny),
	}
	return
}

// WriteImageHttp writes an image to a HTTP response writer using a format and optional
// compression strength specified in a string, e.g., "png", "jpg:80".
func WriteImageHttp(w http.ResponseWriter, img image.Image, formatStr string) (err error) {
	format := strings.Split(formatStr, ":")
	var compression int = DefaultJPEGQuality
	if len(format) > 1 {
		compression, err = strconv.Atoi(format[1])
		if err != nil {
			return err
		}
	}
	switch format[0] {
	case "", "png":
		w.Header().Set("Content-type", "image/png")
		png.Encode(w, img)
	case "jpg", "jpeg":
		w.Header().Set("Content-type", "image/jpeg")
		jpeg.Encode(w, img, &jpeg.Options{Quality: compression})
	case "tiff", "tif":
		w.Header().Set("Content-type", "image/tiff")
		tiff.Encode(w, img, &tiff.Options{Compression: tiff.Deflate})
	case "bmp":
		w.Header().Set("Content-type", "image/bmp")
		bmp.Encode(w, img)
	default:
		err = fmt.Errorf("Illegal image format requested: %s", format[0])
	}
	return
}

// PrintNonZero prints the number of non-zero bytes in a slice of bytes.
func PrintNonZero(message string, value []byte) {
	nonzero := 0
	for _, b := range value {
		if b != 0 {
			nonzero++
		}
	}
	fmt.Printf("%s> non-zero voxels: %d of %d bytes\n", message, nonzero, len(value))
}
