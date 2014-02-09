/*
	This file supports serialization/deserialization and compression of data.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	_ "log"

	lz4 "github.com/janelia-flyem/go/golz4"
	"github.com/janelia-flyem/go/snappy-go/snappy"
)

// Redirection tracks whether a value can be used as is or requires a redirection, i.e., contains
// a local version ID of where the value is actually stored.
type Redirection uint8

const (
	// Value can be used as is without redirection.
	UnversionedValue Redirection = 0

	// Value is a local version ID where the actual value is stored.
	VersionedValue Redirection = 1
)

// Compression is the format of compression for storing data.
// NOTE: Should be no more than 8 (3 bits) of compression types.
type Compression uint8

const (
	Uncompressed Compression = 0
	Snappy                   = 1 << iota
	LZ4
)

func (compress Compression) String() string {
	switch compress {
	case Uncompressed:
		return "No compression"
	case Snappy:
		return "Go Snappy compression"
	case LZ4:
		return "Go LZ4 compression"
	default:
		return "Unknown compression"
	}
}

// Checksum is the type of checksum employed for error checking stored data.
// NOTE: Should be no more than 4 (2 bits) of checksum types.
type Checksum uint8

const (
	NoChecksum Checksum = 0
	CRC32               = 1 << iota
)

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

// SerializationFormat is a single byte combining both compression and checksum methods.
type SerializationFormat uint8

func EncodeSerializationFormat(compress Compression, checksum Checksum) SerializationFormat {
	a := (uint8(compress) & 0x07) << 5
	b := (uint8(checksum) & 0x03) << 3
	return SerializationFormat(a | b)
}

func DecodeSerializationFormat(s SerializationFormat) (compress Compression, checksum Checksum) {
	compress = Compression(uint8(s) >> 5)
	checksum = Checksum((uint8(s) >> 3) & 0x03)
	return
}

// Serialize a slice of bytes using optional compression, checksum
func SerializeData(data []byte, compress Compression, checksum Checksum) (s []byte, err error) {
	var buffer bytes.Buffer

	// Store the requested compression and checksum
	format := EncodeSerializationFormat(compress, checksum)
	err = binary.Write(&buffer, binary.LittleEndian, format)
	if err != nil {
		return
	}

	// Handle compression if requested
	var byteData []byte
	switch compress {
	case Uncompressed:
		byteData = data
	case Snappy:
		byteData, err = snappy.Encode(nil, data)
	case LZ4:
		origSize := uint32(len(data))
		byteData = make([]byte, lz4.CompressBound(data)+4)
		binary.LittleEndian.PutUint32(byteData[0:4], origSize)
		var outSize int
		outSize, err = lz4.Compress(data, byteData[4:])
		if err == nil {
			byteData = byteData[:4+outSize]
		}
	default:
		err = fmt.Errorf("Illegal compression (%s) during serialization", compress)
	}
	if err != nil {
		return
	}

	// Handle checksum if requested
	switch checksum {
	case NoChecksum:
	case CRC32:
		crcChecksum := crc32.ChecksumIEEE(byteData)
		err = binary.Write(&buffer, binary.LittleEndian, crcChecksum)
	default:
		err = fmt.Errorf("Illegal checksum (%s) in serialize.SerializeData()", checksum)
	}
	if err == nil {
		// Note the actual data is written last, after any checksum so we don't have to
		// worry about length when deserializing.
		_, err = buffer.Write(byteData)
		if err == nil {
			s = buffer.Bytes()
		}
	}
	return
}

// Serializes an arbitrary Go object using Gob encoding and optional compression, checksum.
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
func DeserializeData(s []byte, uncompress bool) (data []byte, compress Compression, err error) {
	buffer := bytes.NewBuffer(s)

	// Get the stored compression and checksum
	var format SerializationFormat
	err = binary.Read(buffer, binary.LittleEndian, &format)
	if err != nil {
		return
	}
	var checksum Checksum
	compress, checksum = DecodeSerializationFormat(format)

	// Get any checksum.
	var storedCrc32 uint32
	switch checksum {
	case NoChecksum:
	case CRC32:
		err = binary.Read(buffer, binary.LittleEndian, &storedCrc32)
	default:
		err = fmt.Errorf("Illegal checksum in deserializing data")
	}
	if err != nil {
		return
	}

	// Get the possibly compressed data.
	cdata := buffer.Bytes()

	// Perform any requested checksum
	switch checksum {
	case CRC32:
		crcChecksum := crc32.ChecksumIEEE(cdata)
		if crcChecksum != storedCrc32 {
			err = fmt.Errorf("Bad checksum.  Stored %x got %x", storedCrc32, crcChecksum)
		}
	}

	// Uncompress if needed
	if uncompress {
		switch compress {
		case Uncompressed:
			data = cdata
		case Snappy:
			data, err = snappy.Decode(nil, cdata)
		case LZ4:
			origSize := binary.LittleEndian.Uint32(cdata[0:4])
			data = make([]byte, int(origSize))
			err = lz4.Uncompress(cdata[4:], data)
		default:
			err = fmt.Errorf("Illegal compressiont format (%d) in deserialization", compress)
		}
	}
	return
}

// Deserializes a Go object using Gob encoding
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
