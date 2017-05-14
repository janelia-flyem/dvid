package labels

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"

	lz4 "github.com/janelia-flyem/go/golz4"
)

type testData struct {
	u        []uint64
	b        []byte
	filename string
}

func (d testData) String() string {
	return filepath.Base(d.filename)
}

var testFiles = []string{
	"../../../test_data/fib19-64x64x64-sample1.dat.gz",
	"../../../test_data/fib19-64x64x64-sample2.dat.gz",
	"../../../test_data/cx-64x64x64-sample1.dat.gz",
	"../../../test_data/cx-64x64x64-sample2.dat.gz",
}

func loadData(filename string) (td testData, err error) {
	td.b, err = readGzipFile(filename)
	if err != nil {
		return
	}
	td.u, err = dvid.ByteToUint64(td.b)
	if err != nil {
		return
	}
	td.filename = filename
	return
}

func loadTestData(t *testing.T, filename string) (td testData) {
	var err error
	td.b, err = readGzipFile(filename)
	if err != nil {
		t.Fatalf("unable to open test data file %q: %v\n", filename, err)
	}
	td.u, err = dvid.ByteToUint64(td.b)
	if err != nil {
		t.Fatalf("unable to create alias []uint64 for data file %q: %v\n", filename, err)
	}
	td.filename = filename
	return
}

func readGzipFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer fz.Close()

	data, err := ioutil.ReadAll(fz)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func checkBytes(t *testing.T, text string, expected, got []byte) {
	if len(expected) != len(got) {
		t.Errorf("%s byte mismatch: expected %d bytes, got %d bytes\n", text, len(expected), len(got))
	}
	for i, val := range got {
		if expected[i] != val {
			t.Errorf("%s byte mismatch found at index %d, expected %d, got %d\n", text, i, expected[i], val)
			return
		}
	}
}

func checkLabels(t *testing.T, text string, expected, got []byte) {
	if len(expected) != len(got) {
		t.Errorf("%s byte mismatch: expected %d bytes, got %d bytes\n", text, len(expected), len(got))
	}
	expectLabels, err := dvid.ByteToUint64(expected)
	if err != nil {
		t.Fatal(err)
	}
	gotLabels, err := dvid.ByteToUint64(got)
	if err != nil {
		t.Fatal(err)
	}
	errmsg := 0
	for i, val := range gotLabels {
		if expectLabels[i] != val {
			t.Errorf("%s label mismatch found at index %d, expected %d, got %d\n", text, i, expectLabels[i], val)
			errmsg++
			if errmsg > 5 {
				return
			}
		} else if errmsg > 0 {
			t.Errorf("yet no mismatch at index %d, expected %d, got %d\n", i, expectLabels[i], val)
		}
	}
}

func printRatio(t *testing.T, curDesc string, cur, orig int, compareDesc string, compare int) {
	p1 := 100.0 * float32(cur) / float32(orig)
	p2 := float32(cur) / float32(compare)
	t.Logf("%s compression result: %d (%4.2f%% orig, %4.2f x %s)\n", curDesc, cur, p1, p2, compareDesc)
}

func blockTest(t *testing.T, d testData) {
	var err error
	var origLZ4Size int
	origLZ4 := make([]byte, lz4.CompressBound(d.b))
	if origLZ4Size, err = lz4.Compress(d.b, origLZ4); err != nil {
		t.Fatal(err)
	}
	printRatio(t, "Orig LZ4", origLZ4Size, len(d.b), "Orig", len(d.b))

	labels := make(map[uint64]int)
	for _, label := range d.u {
		labels[label]++
	}
	for label, voxels := range labels {
		t.Logf("Label %12d: %8d voxels\n", label, voxels)
	}

	cseg, err := oldCompressUint64(d.u, dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatal(err)
	}
	csegBytes := dvid.Uint32ToByte([]uint32(cseg))
	block, err := MakeBlock(d.b, dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatal(err)
	}
	dvidBytes, _ := block.MarshalBinary()

	printRatio(t, "Goog", len(csegBytes), len(d.b), "DVID", len(dvidBytes))
	printRatio(t, "DVID", len(dvidBytes), len(d.b), "Goog", len(csegBytes))

	var gzipOut bytes.Buffer
	zw := gzip.NewWriter(&gzipOut)
	if _, err = zw.Write(csegBytes); err != nil {
		t.Fatal(err)
	}
	zw.Flush()
	csegGzip := make([]byte, gzipOut.Len())
	copy(csegGzip, gzipOut.Bytes())
	gzipOut.Reset()
	if _, err = zw.Write(dvidBytes); err != nil {
		t.Fatal(err)
	}
	zw.Flush()
	dvidGzip := make([]byte, gzipOut.Len())
	copy(dvidGzip, gzipOut.Bytes())

	printRatio(t, "Goog GZIP", len(csegGzip), len(d.b), "DVID GZIP", len(dvidGzip))
	printRatio(t, "DVID GZIP", len(dvidGzip), len(d.b), "Goog GZIP", len(csegGzip))

	var csegLZ4Size, dvidLZ4Size int
	csegLZ4 := make([]byte, lz4.CompressBound(csegBytes))
	if csegLZ4Size, err = lz4.Compress(csegBytes, csegLZ4); err != nil {
		t.Fatal(err)
	}
	dvidLZ4 := make([]byte, lz4.CompressBound(dvidBytes))
	if dvidLZ4Size, err = lz4.Compress(dvidBytes, dvidLZ4); err != nil {
		t.Fatal(err)
	}

	printRatio(t, "Goog LZ4 ", csegLZ4Size, len(d.b), "DVID LZ4 ", dvidLZ4Size)
	printRatio(t, "DVID LZ4 ", dvidLZ4Size, len(d.b), "Goog LZ4 ", csegLZ4Size)

	// test DVID block output result
	redata, size := block.MakeLabelVolume()

	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Errorf("expected DVID compression size to be 64x64x64, got %s\n", size)
	}
	checkLabels(t, "DVID compression", d.b, redata)

	// make sure the blocks properly Marshal and Unmarshal
	serialization, _ := block.MarshalBinary()
	datacopy := make([]byte, len(serialization))
	copy(datacopy, serialization)
	var block2 Block
	if err := block2.UnmarshalBinary(datacopy); err != nil {
		t.Fatal(err)
	}
	redata2, size2 := block2.MakeLabelVolume()
	if size2[0] != 64 || size2[1] != 64 || size2[2] != 64 {
		t.Errorf("expected DVID compression size to be 64x64x64, got %s\n", size2)
	}
	checkLabels(t, "marshal/unmarshal copy", redata, redata2)
}

func TestBlockCompression(t *testing.T) {
	for _, filename := range testFiles {
		blockTest(t, loadTestData(t, filename))
	}

	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		testvol[i] = uint64(i)
	}

	block, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	testvol2, size := block.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("error in size after making block: %v\n", size)
	}
	checkLabels(t, "block compress/uncompress", dvid.Uint64ToByte(testvol), testvol2)
}

func BenchmarkGoogleCompress(b *testing.B) {
	d := make([]testData, len(testFiles))
	for i, filename := range testFiles {
		var err error
		d[i], err = loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := oldCompressUint64(d[i%len(d)].u, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDvidCompress(b *testing.B) {
	d := make([]testData, len(testFiles))
	for i, filename := range testFiles {
		var err error
		d[i], err = loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MakeBlock(d[i%len(d)].b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDvidCompressGzip(b *testing.B) {
	d := make([]testData, len(testFiles))
	for i, filename := range testFiles {
		var err error
		d[i], err = loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block, err := MakeBlock(d[i%len(d)].b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
		var gzipOut bytes.Buffer
		zw := gzip.NewWriter(&gzipOut)
		if _, err = zw.Write(block.data); err != nil {
			b.Fatal(err)
		}
		zw.Flush()
	}
}

func BenchmarkDvidCompressLZ4(b *testing.B) {
	d := make([]testData, len(testFiles))
	for i, filename := range testFiles {
		var err error
		d[i], err = loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block, err := MakeBlock(d[i%len(d)].b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
		var n int
		dvidLZ4 := make([]byte, lz4.CompressBound(block.data))
		if n, err = lz4.Compress(block.data, dvidLZ4); err != nil {
			b.Fatal(err)
		}
		dvidLZ4 = dvidLZ4[:n]
	}
}

func BenchmarkDvidMarshalGzip(b *testing.B) {
	var block [4]*Block
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		block[i%4], err = MakeBlock(d.b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	var totbytes int
	for i := 0; i < b.N; i++ {
		var gzipOut bytes.Buffer
		zw := gzip.NewWriter(&gzipOut)
		if _, err := zw.Write(block[i%4].data); err != nil {
			b.Fatal(err)
		}
		zw.Flush()
		zw.Close()
		compressed := gzipOut.Bytes()
		totbytes += len(compressed)
	}
}

func BenchmarkDvidUnmarshalGzip(b *testing.B) {
	var compressed [4][]byte
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		block, err := MakeBlock(d.b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
		var gzipOut bytes.Buffer
		zw := gzip.NewWriter(&gzipOut)
		if _, err = zw.Write(block.data); err != nil {
			b.Fatal(err)
		}
		zw.Flush()
		zw.Close()
		compressed[i%4] = make([]byte, gzipOut.Len())
		copy(compressed[i%4], gzipOut.Bytes())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gzipIn := bytes.NewBuffer(compressed[i%4])
		zr, err := gzip.NewReader(gzipIn)
		if err != nil {
			b.Fatal(err)
		}
		data, err := ioutil.ReadAll(zr)
		if err != nil {
			b.Fatal(err)
		}
		var block Block
		if err := block.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
		zr.Close()
	}
}

func BenchmarkDvidMarshalLZ4(b *testing.B) {
	var block [4]*Block
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		block[i%4], err = MakeBlock(d.b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	var totbytes int
	for i := 0; i < b.N; i++ {
		data := make([]byte, lz4.CompressBound(block[i%4].data))
		var outsize int
		var err error
		if outsize, err = lz4.Compress(block[i%4].data, data); err != nil {
			b.Fatal(err)
		}
		compressed := data[:outsize]
		totbytes += len(compressed)
	}
}

func BenchmarkDvidUnmarshalLZ4(b *testing.B) {
	var origsize [4]int
	var compressed [4][]byte
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		block, err := MakeBlock(d.b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
		data := make([]byte, lz4.CompressBound(block.data))
		var outsize int
		if outsize, err = lz4.Compress(block.data, data); err != nil {
			b.Fatal(err)
		}
		compressed[i%4] = data[:outsize]
		origsize[i%4] = len(block.data)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := make([]byte, origsize[i%4])
		if err := lz4.Uncompress(compressed[i%4], data); err != nil {
			b.Fatal(err)
		}
		var block Block
		if err := block.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoogleReturnArray(b *testing.B) {
	var compressed [4]oldCompressedSegData
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		compressed[i%4], err = oldCompressUint64(d.u, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := oldDecompressUint64(compressed[i%4], dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDvidReturnArray(b *testing.B) {
	var block [4]*Block
	for i, filename := range testFiles {
		d, err := loadData(filename)
		if err != nil {
			b.Fatal(err)
		}
		block[i%4], err = MakeBlock(d.b, dvid.Point3d{64, 64, 64})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		labelarray, size := block[i%4].MakeLabelVolume()
		if int64(len(labelarray)) != size.Prod()*8 {
			b.Fatalf("expected label volume returned is %d bytes, got %d instead\n", size.Prod()*8, len(labelarray))
		}
	}
}
