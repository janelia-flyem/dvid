package dvid

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

func TestLocalID(t *testing.T) {
	id := LocalID(41)
	b := id.Bytes()
	id2, length := LocalIDFromBytes(b)

	if id != id2 {
		t.Error("bad conversion")
	}
	if length != LocalIDSize {
		t.Errorf("bad length for local id: %d\n", length)
	}
}

func TestSerialization(t *testing.T) {
	stringObj := "Hi there!"
	var returnObj string

	type ComplexObj struct {
		Title string
		MyMap map[interface{}]interface{}
	}
	complexObj := ComplexObj{
		Title: "my complex object",
		MyMap: map[interface{}]interface{}{
			42:           []byte("here's another string"),
			"some index": []byte{'\x33', '\x18', '\xD0', '\x92', '\x01'},
			32.1:         []string{"It's ", "amazing", " what ", "we", " put ", "here"},
		},
	}

	for _, format := range []CompressionFormat{Uncompressed, Snappy, LZ4, Gzip} {
		for _, checksum := range []Checksum{NoChecksum, CRC32} {
			compression, err := NewCompression(format, DefaultCompression)
			if err != nil {
				t.Error(err)
			}

			// Check simple object
			var csum Checksum
			if format == Gzip {
				csum = NoChecksum
			} else {
				csum = checksum
			}
			s, err := Serialize(stringObj, compression, csum)
			if err != nil {
				t.Error(err)
			}
			if len(s) == 0 {
				t.Errorf("Bad Serialize() - output length 0\n")
			}

			if err = Deserialize(s, &returnObj); err != nil {
				t.Errorf("Bad Deserialize() for %q: %v\n", stringObj, err)
			}
			if returnObj != stringObj {
				t.Errorf("expected %s, got %s\n", stringObj, returnObj)
			}

			// Check more complex object
			s, err = Serialize(complexObj, compression, csum)
			if err != nil {
				t.Error(err)
			}

			var returnComplexObj ComplexObj
			if err = Deserialize(s, &returnComplexObj); err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(returnComplexObj, complexObj) {
				t.Errorf("expected %v, got %v\n", complexObj, returnComplexObj)
			}

			if csum != NoChecksum || format == Gzip {
				// Check Checksum on complex object with many bit flips.  If only one or two,
				// the gzip header might be impervious.
				for i := 0; i < len(s); i++ {
					s[i] = s[i] ^ 0x04
				}
				if err = Deserialize(s, &returnComplexObj); err == nil {
					t.Errorf("for format %s, checksum %s, compresion %s: %v\n", format, checksum, compression, err)
				}
			}
		}
	}
}

func readData(t *testing.T, filepath string) []byte {
	f, err := os.Open(filepath)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestIncompressibleLZ4(t *testing.T) {
	incompressibleData := make([]byte, 30)
	for i := 0; i < 30; i++ {
		incompressibleData[i] = byte(rand.Int() % 255)
	}
	t.Logf("incompressible data size: %d\n", len(incompressibleData))
	compressed := make([]byte, lz4.CompressBound(incompressibleData))
	outSize, err := lz4.Compress(incompressibleData, compressed)
	if err != nil {
		t.Fatal(err)
	}
	compressed = compressed[:outSize]
	t.Logf("lz4 compress was %d bytes\n", outSize)

	out := make([]byte, 30)
	if err = lz4.Uncompress(compressed, out); err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(out, incompressibleData) != 0 {
		t.Fatal("bad uncompress lz4")
	}
}

func testUncompressed(b *testing.B, checksum Checksum) {
	stringObj := "Hi there!"
	var returnObj string

	type ComplexObj struct {
		Title string
		MyMap map[interface{}]interface{}
	}
	complexObj := ComplexObj{
		Title: "my complex object",
		MyMap: map[interface{}]interface{}{
			42:           []byte("here's another string"),
			"some index": []byte{'\x33', '\x18', '\xD0', '\x92', '\x01'},
			32.1:         []string{"It's ", "amazing", " what ", "we", " put ", "here"},
		},
	}
	compression, err := NewCompression(Uncompressed, DefaultCompression)
	if err != nil {
		b.Error(err)
	}

	s, _ := Serialize(stringObj, compression, checksum)
	_ = Deserialize(s, &returnObj)

	s, _ = Serialize(complexObj, compression, checksum)
	var returnComplexObj ComplexObj
	_ = Deserialize(s, &returnComplexObj)
}

func BenchmarkUncompressedNoChecksum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testUncompressed(b, NoChecksum)
	}
}
