package dvid

import (
	"bytes"
	stdgzip "compress/gzip"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	klausgzip "github.com/klauspost/compress/gzip"

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
					t.Errorf("for format %s, checksum %s, compression %s: %v\n", format, checksum, compression, err)
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

	data, err := io.ReadAll(f)
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

// TestGzipCompatibility verifies that klauspost/compress/gzip produces byte-identical
// decompressed output to stdlib compress/gzip. This ensures the gzip library swap in
// serialize.go doesn't subtly alter any data path.
func TestGzipCompatibility(t *testing.T) {
	// Create a representative payload: label block-sized data (64^3 uint64 values).
	rng := rand.New(rand.NewSource(42))
	payload := make([]byte, 64*64*64*8) // ~2 MB
	for i := range payload {
		payload[i] = byte(rng.Intn(256))
	}

	// Compress with stdlib gzip
	var stdBuf bytes.Buffer
	stdWriter, err := stdgzip.NewWriterLevel(&stdBuf, stdgzip.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stdWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := stdWriter.Close(); err != nil {
		t.Fatal(err)
	}
	stdCompressed := stdBuf.Bytes()

	// Decompress with klauspost gzip (the library now used in serialize.go)
	klausReader, err := klausgzip.NewReader(bytes.NewReader(stdCompressed))
	if err != nil {
		t.Fatal(err)
	}
	klausDecompressed, err := io.ReadAll(klausReader)
	if err != nil {
		t.Fatal(err)
	}
	if err := klausReader.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(klausDecompressed, payload) {
		t.Errorf("klauspost gzip decompressed %d bytes, expected %d", len(klausDecompressed), len(payload))
	}

	// Also test the reverse: compress with klauspost, decompress with stdlib
	var klausBuf bytes.Buffer
	klausWriter, err := klausgzip.NewWriterLevel(&klausBuf, klausgzip.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := klausWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := klausWriter.Close(); err != nil {
		t.Fatal(err)
	}
	klausCompressed := klausBuf.Bytes()

	stdReader, err := stdgzip.NewReader(bytes.NewReader(klausCompressed))
	if err != nil {
		t.Fatal(err)
	}
	stdDecompressed, err := io.ReadAll(stdReader)
	if err != nil {
		t.Fatal(err)
	}
	if err := stdReader.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(stdDecompressed, payload) {
		t.Errorf("stdlib gzip decompressed %d bytes from klauspost-compressed data, expected %d", len(stdDecompressed), len(payload))
	}
}

// TestGzipSerializeDataRoundTrip verifies that SerializeData/DeserializeData produces
// identical results after swapping to klauspost gzip.
func TestGzipSerializeDataRoundTrip(t *testing.T) {
	// Create realistic block-like data with some structure (not purely random)
	payload := make([]byte, 100000)
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < len(payload); i++ {
		// Mix of repeated and random bytes to give gzip something to compress
		if i%8 < 4 {
			payload[i] = byte(i % 256)
		} else {
			payload[i] = byte(rng.Intn(256))
		}
	}

	compression, err := NewCompression(Gzip, DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}

	// Serialize with gzip compression (now uses klauspost)
	serialized, err := SerializeData(payload, compression, NoChecksum)
	if err != nil {
		t.Fatalf("SerializeData with gzip failed: %v", err)
	}

	// Deserialize (also uses klauspost)
	deserialized, format, err := DeserializeData(serialized, true)
	if err != nil {
		t.Fatalf("DeserializeData with gzip failed: %v", err)
	}
	if format != Gzip {
		t.Errorf("expected Gzip format, got %v", format)
	}
	if !bytes.Equal(deserialized, payload) {
		t.Errorf("gzip round-trip mismatch: got %d bytes, expected %d", len(deserialized), len(payload))
	}

	// Also verify that stdlib can decompress what klauspost produced
	// (skip the DVID format byte to get raw gzip data)
	rawGzip := serialized[1:] // format byte is 1 byte, no checksum for gzip
	stdReader, err := stdgzip.NewReader(bytes.NewReader(rawGzip))
	if err != nil {
		t.Fatalf("stdlib gzip couldn't read klauspost-compressed data: %v", err)
	}
	stdResult, err := io.ReadAll(stdReader)
	if err != nil {
		t.Fatalf("stdlib gzip decompress failed: %v", err)
	}
	stdReader.Close()
	if !bytes.Equal(stdResult, payload) {
		t.Errorf("stdlib gzip decompressed %d bytes from klauspost SerializeData output, expected %d", len(stdResult), len(payload))
	}
}

func BenchmarkUncompressedNoChecksum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testUncompressed(b, NoChecksum)
	}
}
