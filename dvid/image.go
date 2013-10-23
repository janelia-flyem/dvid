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
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/janelia-flyem/go/go.image/bmp"
	"github.com/janelia-flyem/go/go.image/tiff"
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Image{})
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
		for y := rect.Min.Y; y < rect.Min.Y; y++ {
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

	switch img.Which {
	case 0:
		img.Gray = &image.Gray{
			Stride: int(stride),
			Rect:   rect,
			Pix:    []uint8(buffer.Bytes()),
		}

	case 1:
		img.Gray16 = &image.Gray16{
			Stride: int(stride),
			Rect:   rect,
			Pix:    []uint8(buffer.Bytes()),
		}

	case 2:
		img.RGBA = &image.RGBA{
			Stride: int(stride),
			Rect:   rect,
			Pix:    []uint8(buffer.Bytes()),
		}

	case 3:
		img.RGBA64 = &image.RGBA64{
			Stride: int(stride),
			Rect:   rect,
			Pix:    []uint8(buffer.Bytes()),
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

// ImageData returns the underlying pixel data for an image or an error if
// the image doesn't have the requisite []uint8 pixel data.
func ImageData(img image.Image) (data []uint8, stride int32, err error) {
	switch typedImg := img.(type) {
	case *image.Gray:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
	case *image.Gray16:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
	case *image.RGBA:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
	case *image.RGBA64:
		data = typedImg.Pix
		stride = int32(typedImg.Stride)
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
		return
	}
	defer f.Close()

	var buf bytes.Buffer
	io.Copy(&buf, f)
	img, format, err = image.Decode(&buf)
	return
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
