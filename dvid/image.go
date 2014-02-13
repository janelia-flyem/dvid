/*
	This file supports image operations in DVID.
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

// Image is a union of possible image types for better Gob use compared to
// a generic image.Image interface.  Suggested by Rob Pike in golang-nuts:
// see https://groups.google.com/d/msg/golang-dev/_t4pqoeuflE/DbqSf41wr5EJ
type Image struct {
	Which  uint8
	Gray   *image.Gray
	Gray16 *image.Gray16
	RGBA   *image.RGBA
	RGBA64 *image.RGBA64
}

// Get returns an image.Image from the union struct.
func (img Image) Get() image.Image {
	switch img.Which {
	case 0:
		return img.Gray
	case 1:
		return img.Gray16
	case 2:
		return img.RGBA
	case 3:
		return img.RGBA64
	default:
		return nil
	}
}

// Set places an image into the union struct.
func (img *Image) Set(src image.Image) error {
	switch s := src.(type) {
	case *image.Gray:
		img.Which = 0
		img.Gray = s
	case *image.Gray16:
		img.Which = 1
		img.Gray16 = s
	case *image.RGBA:
		img.Which = 2
		img.RGBA = s
	case *image.RGBA64:
		img.Which = 3
		img.RGBA64 = s
	default:
		return fmt.Errorf("No valid image type received by image.Set(): %s", reflect.TypeOf(src))
	}
	return nil
}

// SubImage returns an image representing the portion of the image p visible through r.
// The returned image shares pixels with the original image.
func (img *Image) SubImage(r image.Rectangle) (*Image, error) {
	result := new(Image)
	result.Which = img.Which
	switch img.Which {
	case 0:
		result.Gray = img.Gray.SubImage(r).(*image.Gray)
	case 1:
		result.Gray16 = img.Gray16.SubImage(r).(*image.Gray16)
	case 2:
		result.RGBA = img.RGBA.SubImage(r).(*image.RGBA)
	case 3:
		result.RGBA64 = img.RGBA64.SubImage(r).(*image.RGBA64)
	default:
		return nil, fmt.Errorf("Unsupported image type %d asked for SubImage()", img.Which)
	}
	return result, nil
}

// Serialize writes compact byte slice representing image data.
func (img *Image) Serialize(compress Compression, checksum Checksum) ([]byte, error) {
	var buffer bytes.Buffer
	err := buffer.WriteByte(byte(img.Which))
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
		stride = img.RGBA.Stride
		rect = img.RGBA.Rect
		bytesPerPixel = 4
		src = img.RGBA.Pix
		pixOffset = img.RGBA.PixOffset

	case 3:
		stride = img.RGBA64.Stride
		rect = img.RGBA64.Rect
		bytesPerPixel = 8
		src = img.RGBA64.Pix
		pixOffset = img.RGBA64.PixOffset
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

	err = binary.Write(&buffer, binary.LittleEndian, int32(stride))
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.LittleEndian, int32(rect.Dx()))
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.LittleEndian, int32(rect.Dy()))
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(pix)
	if err != nil {
		return nil, err
	}

	return SerializeData(buffer.Bytes(), compress, checksum)
}

// Deserialze deserializes an Image from a byte slice.
func (img *Image) Deserialize(b []byte) error {
	if img == nil {
		return fmt.Errorf("Error attempting to deserialize into nil Image")
	}

	data, _, err := DeserializeData(b, true)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(data)

	// Get the image type.
	imageType, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	img.Which = uint8(imageType)

	// Get the stride and sizes.
	var stride int32
	err = binary.Read(buffer, binary.LittleEndian, &stride)
	if err != nil {
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
		img.RGBA = &image.RGBA{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}

	case 3:
		img.RGBA64 = &image.RGBA64{
			Stride: int(stride),
			Rect:   rect,
			Pix:    pix,
		}
	}
	return nil
}

// Register all the image types for gob decoding.
func init() {
	gob.Register(&Image{})
	gob.Register(&image.Gray{})
	gob.Register(&image.Gray16{})
	gob.Register(&image.RGBA{})
	gob.Register(&image.RGBA64{})
}

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
	ruler := color.RGBA{0xdd, 0xdd, 0xdd, 0xff}
	// White on black
	// fg, bg = image.White, image.Black
	// ruler = color.RGBA{0x22, 0x22, 0x22, 0xff}
	rgba := image.NewRGBA(image.Rect(0, 0, int(dx), int(dy)))
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
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 4
	case *image.NRGBA:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
		bytesPerPixel = 4
	case *image.RGBA64:
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

// ImageFromPost returns and image and its format name given a key to a POST request.
// The image should be the first file in a POSTed form.
func ImageFromPost(r *http.Request, key string) (img image.Image, format string, err error) {
	f, _, err := r.FormFile(key)
	if err != nil {
		return nil, "none", err
	}
	defer f.Close()

	return image.Decode(f)
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

// The code below was taken from the AppEngine moustachio example and should be among
// the more efficient resizers without resorting to more sophisticated interpolation
// than nearest-neighbor.
//
// http://code.google.com/p/appengine-go/source/browse/example/moustachio/resize/resize.go
//
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Resize returns a scaled copy of the image slice r of m.
// The returned image has width w and height h.
func Resize(m image.Image, r image.Rectangle, w, h int) image.Image {
	if w < 0 || h < 0 {
		return nil
	}
	if w == 0 || h == 0 || r.Dx() <= 0 || r.Dy() <= 0 {
		return image.NewRGBA64(image.Rect(0, 0, w, h))
	}
	switch m := m.(type) {
	case *image.RGBA:
		return resizeRGBA(m, r, w, h)
	case *image.YCbCr:
		if m, ok := resizeYCbCr(m, r, w, h); ok {
			return m
		}
	}
	ww, hh := uint64(w), uint64(h)
	dx, dy := uint64(r.Dx()), uint64(r.Dy())
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
	n, sum := dx*dy, make([]uint64, 4*w*h)
	for y := r.Min.Y; y < r.Max.Y; y++ {
		for x := r.Min.X; x < r.Max.X; x++ {
			// Get the source pixel.
			r32, g32, b32, a32 := m.At(x, y).RGBA()
			r64 := uint64(r32)
			g64 := uint64(g32)
			b64 := uint64(b32)
			a64 := uint64(a32)
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
					sum[index+0] += r64 * qx * qy
					sum[index+1] += g64 * qx * qy
					sum[index+2] += b64 * qx * qy
					sum[index+3] += a64 * qx * qy
					index += 4
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	return average(sum, w, h, n*0x0101)
}

// average convert the sums to averages and returns the result.
func average(sum []uint64, w, h int, n uint64) image.Image {
	ret := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			index := 4 * (y*w + x)
			ret.SetRGBA(x, y, color.RGBA{
				uint8(sum[index+0] / n),
				uint8(sum[index+1] / n),
				uint8(sum[index+2] / n),
				uint8(sum[index+3] / n),
			})
		}
	}
	return ret
}

// resizeYCbCr returns a scaled copy of the YCbCr image slice r of m.
// The returned image has width w and height h.
func resizeYCbCr(m *image.YCbCr, r image.Rectangle, w, h int) (image.Image, bool) {
	var verticalRes int
	switch m.SubsampleRatio {
	case image.YCbCrSubsampleRatio420:
		verticalRes = 2
	case image.YCbCrSubsampleRatio422:
		verticalRes = 1
	default:
		return nil, false
	}
	ww, hh := uint64(w), uint64(h)
	dx, dy := uint64(r.Dx()), uint64(r.Dy())
	// See comment in Resize.
	n, sum := dx*dy, make([]uint64, 4*w*h)
	for y := r.Min.Y; y < r.Max.Y; y++ {
		Y := m.Y[y*m.YStride:]
		Cb := m.Cb[y/verticalRes*m.CStride:]
		Cr := m.Cr[y/verticalRes*m.CStride:]
		for x := r.Min.X; x < r.Max.X; x++ {
			// Get the source pixel.
			r8, g8, b8 := color.YCbCrToRGB(Y[x], Cb[x/2], Cr[x/2])
			r64 := uint64(r8)
			g64 := uint64(g8)
			b64 := uint64(b8)
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
					sum[index+3] += 0xFFFF * qxy
					index += 4
					px += qx
					remx -= qx
				}
				py += qy
				remy -= qy
			}
		}
	}
	return average(sum, w, h, n), true
}

// resizeRGBA returns a scaled copy of the RGBA image slice r of m.
// The returned image has width w and height h.
func resizeRGBA(m *image.RGBA, r image.Rectangle, w, h int) image.Image {
	ww, hh := uint64(w), uint64(h)
	dx, dy := uint64(r.Dx()), uint64(r.Dy())
	// See comment in Resize.
	n, sum := dx*dy, make([]uint64, 4*w*h)
	for y := r.Min.Y; y < r.Max.Y; y++ {
		pixOffset := m.PixOffset(r.Min.X, y)
		for x := r.Min.X; x < r.Max.X; x++ {
			// Get the source pixel.
			r64 := uint64(m.Pix[pixOffset+0])
			g64 := uint64(m.Pix[pixOffset+1])
			b64 := uint64(m.Pix[pixOffset+2])
			a64 := uint64(m.Pix[pixOffset+3])
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
	return average(sum, w, h, n)
}

// Resample returns a resampled copy of the image slice r of m.
// The returned image has width w and height h.
func Resample(m image.Image, r image.Rectangle, w, h int) image.Image {
	if w < 0 || h < 0 {
		return nil
	}
	if w == 0 || h == 0 || r.Dx() <= 0 || r.Dy() <= 0 {
		return image.NewRGBA64(image.Rect(0, 0, w, h))
	}
	curw, curh := r.Dx(), r.Dy()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			// Get a source pixel.
			subx := x * curw / w
			suby := y * curh / h
			r32, g32, b32, a32 := m.At(subx, suby).RGBA()
			r := uint8(r32 >> 8)
			g := uint8(g32 >> 8)
			b := uint8(b32 >> 8)
			a := uint8(a32 >> 8)
			img.SetRGBA(x, y, color.RGBA{r, g, b, a})
		}
	}
	return img
}
