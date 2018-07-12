/*
	This file supports serialization/deserialization and compression of data.
*/

package dvid

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"image"
	"image/jpeg"
	"io"

	"github.com/golang/snappy"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

// Compression is the format of compression for storing data.
// NOTE: Should be no more than 8 (3 bits) compression types.
type Compression struct {
	format CompressionFormat
	level  CompressionLevel
}

func (c Compression) Format() CompressionFormat {
	return c.format
}

func (c Compression) Level() CompressionLevel {
	return c.level
}

// MarshalJSON implements the json.Marshaler interface.
func (c Compression) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"Format":%d,"Level":%d}`, c.format, c.level)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *Compression) UnmarshalJSON(b []byte) error {
	var m struct {
		Format CompressionFormat
		Level  CompressionLevel
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	c.format = m.Format
	c.level = m.Level
	return nil
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (c Compression) MarshalBinary() ([]byte, error) {
	return []byte{byte(c.format), byte(c.level)}, nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (c *Compression) UnmarshalBinary(data []byte) error {
	if len(data) != 2 {
		return fmt.Errorf("Cannot unmarshal %d bytes into Compression", len(data))
	}
	c.format = CompressionFormat(data[0])
	c.level = CompressionLevel(data[1])
	return nil
}

func (c Compression) String() string {
	return fmt.Sprintf("%s, level %d", c.format, c.level)
}

// NewCompression returns a Compression struct that maps compression-specific details
// to a DVID-wide compression.
func NewCompression(format CompressionFormat, level CompressionLevel) (Compression, error) {
	if level == NoCompression {
		format = Uncompressed
	}
	switch format {
	case Uncompressed:
		return Compression{format, DefaultCompression}, nil
	case Snappy:
		return Compression{format, DefaultCompression}, nil
	case LZ4:
		return Compression{format, DefaultCompression}, nil
	case JPEG:
		return Compression{format, level}, nil
	case Gzip:
		if level != DefaultCompression && (level < 1 || level > 9) {
			return Compression{}, fmt.Errorf("Gzip compression level must be between 1 and 9")
		}
		return Compression{format, level}, nil
	default:
		return Compression{}, fmt.Errorf("Unrecognized compression format requested: %d", format)
	}
}

// CompressionLevel goes from 1 (fastest) to 9 (highest compression)
// as in deflate.  Default compression is -1 so need signed int8.
type CompressionLevel int8

const (
	NoCompression      CompressionLevel = 0
	BestSpeed                           = 1
	BestCompression                     = 9
	DefaultCompression                  = -1
)

// CompressionFormat specifies the compression algorithm and is limited to 3 bits (7 types)
type CompressionFormat uint8

// note that compression constants are legacy from when they were originally defined, incorrectly,
// as powers of two (bit flags).  Since we won't be using multiple compressions without ordering
// info, we reverted to simple numbering.

const (
	Uncompressed CompressionFormat = 0
	Snappy                         = 1
	Gzip                           = 2 // Gzip stores length and checksum automatically.
	LZ4                            = 4
	JPEG                           = 5
)

func (format CompressionFormat) String() string {
	switch format {
	case Uncompressed:
		return "No compression"
	case Snappy:
		return "Go Snappy compression"
	case LZ4:
		return "LZ4 compression"
	case JPEG:
		return "jpeg compression"
	case Gzip:
		return "gzip compression"
	default:
		return "Unknown compression"
	}
}

// Checksum is the type of checksum employed for error checking stored data.
// The maximum number of checksum types is limited to 2 bits (3 types).
type Checksum uint8

const (
	NoChecksum Checksum = 0
	CRC32               = 1
)

// DefaultChecksum is the type of checksum employed for all data operations.
// Note that many database engines already implement some form of corruption test
// and checksum can be set on each datatype instance.
var DefaultChecksum = NoChecksum

func (checksum Checksum) String() string {
	switch checksum {
	case NoChecksum:
		return "No checksum"
	case CRC32:
		return "CRC32 checksum"
	default:
		return "Unknown checksum"
	}
}

// SerializationFormat combines both compression and checksum methods.
// First 3 bits specifies compression, next 2 bits is the checkum, and
// the final 3 bits is reserved for future use.
type SerializationFormat uint8

func EncodeSerializationFormat(compress Compression, checksum Checksum) SerializationFormat {
	a := uint8(compress.format&0x07) << 5
	b := uint8(checksum&0x03) << 3
	return SerializationFormat(a | b)
}

func DecodeSerializationFormat(s SerializationFormat) (CompressionFormat, Checksum) {
	format := CompressionFormat(s >> 5)
	checksum := Checksum(s>>3) & 0x03
	return format, checksum
}

// SerializeData serializes a slice of bytes using optional compression, checksum.
// Checksum will be ignored if the underlying compression already employs checksums, e.g., Gzip.
func SerializeData(data []byte, compress Compression, checksum Checksum) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return []byte{}, nil
	}

	var err error
	var byteData []byte
	switch compress.format {
	case Uncompressed:
		byteData = data
	case Snappy:
		byteData = snappy.Encode(nil, data)
	case LZ4:
		origSize := uint32(len(data))
		byteData = make([]byte, lz4.CompressBound(data)+4)
		binary.LittleEndian.PutUint32(byteData[0:4], origSize)
		var outSize int
		outSize, err = lz4.Compress(data, byteData[4:])
		if err != nil {
			return nil, err
		}
		byteData = byteData[:4+outSize]
	case JPEG:
		origSize := int(len(data))
		length := origSize / int(compress.level)

		if origSize%int(compress.level) != 0 {
			return nil, fmt.Errorf("Illegal block dimensions on compression")
		}
		rect := image.Rectangle{image.Point{0, 0}, image.Point{int(compress.level), int(length)}}
		//rect := image.Rectangle{image.Point{0, 0}, image.Point{int(length), int(compress.level)}}

		graydata := &image.Gray{[]uint8(data), int(compress.level), rect}
		//graydata := &image.Gray{[]uint8(data), int(length), rect}
		var buffer2 bytes.Buffer
		if err := jpeg.Encode(&buffer2, graydata, &jpeg.Options{DefaultJPEGQuality}); err != nil {
			return nil, err
		}
		byteData = buffer2.Bytes()
	case Gzip:
		var b bytes.Buffer
		w, err := gzip.NewWriterLevel(&b, int(compress.level))
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(data); err != nil {
			return nil, err
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		byteData = b.Bytes()
	default:
		return nil, fmt.Errorf("Illegal compression (%s) during serialization", compress)
	}

	return SerializePrecompressedData(byteData, compress, checksum)
}

// SerializePrecompressedData serializes a slice of bytes that have already been compressed
// and adds DVID serialization for discerning optional compression and checksum.
// Checksum will be ignored if the underlying compression already employs checksums, e.g., Gzip.
func SerializePrecompressedData(data []byte, compress Compression, checksum Checksum) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, 5+len(data))

	// Don't duplicate checksum if using Gzip, which already has checksum & length checks.
	if compress.format == Gzip {
		checksum = NoChecksum
	}

	// Store the requested compression and checksum
	buf[0] = byte(EncodeSerializationFormat(compress, checksum))

	// Handle checksum if requested
	added := 1
	switch checksum {
	case NoChecksum:
	case CRC32:
		crcChecksum := crc32.ChecksumIEEE(data)
		binary.LittleEndian.PutUint32(buf[1:5], crcChecksum)
		added += 4
	default:
		return nil, fmt.Errorf("Illegal checksum (%s) in serialize.SerializeData()", checksum)
	}

	copy(buf[added:], data)
	if added == 1 {
		buf = buf[:1+len(data)]
	}
	return buf, nil
}

// Serialize an arbitrary Go object using Gob encoding and optional compression, checksum.
// If your object is []byte, you should preferentially use SerializeData since the Gob encoding
// process adds some overhead in performance as well as size of wire format to describe the
// transmitted types.
func Serialize(object interface{}, compress Compression, checksum Checksum) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(object)
	if err != nil {
		return nil, err
	}
	return SerializeData(buffer.Bytes(), compress, checksum)
}

// DeserializeData deserializes a slice of bytes using stored compression, checksum.
// If uncompress parameter is false, the data is not uncompressed.
func DeserializeData(s []byte, uncompress bool) ([]byte, CompressionFormat, error) {
	if s == nil || len(s) == 0 {
		return []byte{}, Uncompressed, nil
	}
	buffer := bytes.NewBuffer(s)

	// Get the stored compression and checksum
	var format SerializationFormat
	if err := binary.Read(buffer, binary.LittleEndian, &format); err != nil {
		return nil, 0, fmt.Errorf("Could not read serialization format info from %d byte input: %v", len(s), err)
	}
	compression, checksum := DecodeSerializationFormat(format)

	// Get any checksum.
	var storedCrc32 uint32
	switch checksum {
	case NoChecksum:
	case CRC32:
		if err := binary.Read(buffer, binary.LittleEndian, &storedCrc32); err != nil {
			return nil, 0, fmt.Errorf("Error reading checksum: %v", err)
		}
	default:
		return nil, 0, fmt.Errorf("Illegal checksum in deserializing data")
	}

	// Get the possibly compressed data.
	cdata := buffer.Bytes()

	// Perform any requested checksum
	switch checksum {
	case CRC32:
		crcChecksum := crc32.ChecksumIEEE(cdata)
		if crcChecksum != storedCrc32 {
			return nil, 0, fmt.Errorf("Bad checksum.  Stored %x got %x", storedCrc32, crcChecksum)
		}
	}

	// Return data with optional compression
	if !uncompress || compression == Uncompressed {
		return cdata, compression, nil
	}

	switch compression {
	case Snappy:
		data, err := snappy.Decode(nil, cdata)
		if err != nil {
			return nil, 0, err
		}
		return data, compression, nil
	case LZ4:
		origSize := binary.LittleEndian.Uint32(cdata[0:4])
		var data []byte
		if origSize == 0 { // support legacy native Go lz4 stored values
			data = make([]byte, len(cdata)-4)
			copy(data, cdata[4:])
		} else {
			data = make([]byte, int(origSize))
			if err := lz4.Uncompress(cdata[4:], data); err != nil {
				return nil, 0, err
			}
		}
		return data, compression, nil
	case JPEG:
		b := bytes.NewBuffer(cdata)
		imgdata, err := jpeg.Decode(b)
		if err != nil {
			return nil, 0, err
		}

		data2 := imgdata.(*image.Gray)
		return data2.Pix, compression, nil
	case Gzip:
		b := bytes.NewBuffer(cdata)
		var err error
		r, err := gzip.NewReader(b)
		if err != nil {
			return nil, 0, err
		}
		var buffer bytes.Buffer
		_, err = io.Copy(&buffer, r)
		if err != nil {
			return nil, 0, err
		}
		err = r.Close()
		if err != nil {
			return nil, 0, err
		}
		return buffer.Bytes(), compression, nil
	default:
		return nil, 0, fmt.Errorf("Illegal compression format (%d) in deserialization", compression)
	}
}

// Deserialize a Go object using Gob encoding
func Deserialize(s []byte, object interface{}) error {
	// Get the bytes for the Gob-encoded object
	data, _, err := DeserializeData(s, true)
	if err != nil {
		return err
	}

	// Decode the bytes
	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	return dec.Decode(object)
}
