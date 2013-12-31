/*
	This file supports reading and writing Raveler superpixel images.
*/

package labelmap

import (
	"fmt"
	"reflect"

	"image"
	"image/color"
	_ "image/png"
)

// Superpixel is a Raveler-oriented description of a superpixel that
// breaks a unique superpixel id into two components: a slice and a
// unique label within that slice.
type Superpixel struct {
	Slice uint32
	Label uint32
}

// Superpixels is a slice of Superpixel type
type Superpixels []Superpixel

// SuperpixelFormat notes whether superpixel ids, if present,
// are in 16-bit or 24-bit values.
type SuperpixelFormat uint8

// Enumerate the types of superpixel id formats
const (
	SuperpixelNone   SuperpixelFormat = iota
	Superpixel16Bits SuperpixelFormat = iota
	Superpixel24Bits SuperpixelFormat = iota
)

// SuperpixelImage is an image with each pixel encoding a unique
// superpixel id for that plane.  Superpixel values must be
// 16-bit grayscale or 32-bit RGBA.
type SuperpixelImage interface {
	image.Image
}

// GetSuperpixelID returns the superpixel id at a given coord
// within a superpixel image.  This routine handles 24-bit and
// 16-bit superpixel images.
func GetSuperpixelId(superpixels SuperpixelImage, x int, y int,
	format SuperpixelFormat) (id uint32, err error) {

	switch format {
	case Superpixel24Bits:
		colorVal := superpixels.At(x, y)
		switch colorVal.(type) {
		case color.NRGBA:
			v := colorVal.(color.NRGBA)
			id = uint32(v.B)
			id <<= 8
			id |= uint32(v.G)
			id <<= 8
			id |= uint32(v.R)
		default:
			err = fmt.Errorf("Expected 32-bit RGBA superpixels, got",
				reflect.TypeOf(colorVal))
		}
	case Superpixel16Bits, SuperpixelNone:
		gray16 := superpixels.At(x, y)
		id = uint32(gray16.(color.Gray16).Y)
	}
	return
}

// segmentId is a Raveler-specific unique body id per plane
type segmentId uint32
