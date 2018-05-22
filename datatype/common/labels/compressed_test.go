package labels

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

type testData struct {
	u        []uint64
	b        []byte
	filename string
}

func (d testData) String() string {
	return filepath.Base(d.filename)
}

func solidTestData(label uint64) (td testData) {
	numVoxels := 64 * 64 * 64
	td.u = make([]uint64, numVoxels)
	td.b = dvid.Uint64ToByte(td.u)
	td.filename = fmt.Sprintf("solid volume of label %d", label)
	for i := 0; i < numVoxels; i++ {
		td.u[i] = label
	}
	return
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
			_, fn, line, _ := runtime.Caller(1)
			t.Errorf("%s label mismatch found at index %d, expected %d, got %d [%s:%d]\n", text, i, expectLabels[i], val, fn, line)
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
	solid2serialization := make([]byte, 24)
	binary.LittleEndian.PutUint32(solid2serialization[0:4], 8)
	binary.LittleEndian.PutUint32(solid2serialization[4:8], 8)
	binary.LittleEndian.PutUint32(solid2serialization[8:12], 8)
	binary.LittleEndian.PutUint32(solid2serialization[12:16], 1)
	binary.LittleEndian.PutUint64(solid2serialization[16:24], 2)
	var block Block
	if err := block.UnmarshalBinary(solid2serialization); err != nil {
		t.Fatal(err)
	}
	if block.Value(dvid.Point3d{34, 22, 12}) != 2 {
		t.Errorf("Expected 2, got %d\n", block.Value(dvid.Point3d{34, 22, 12}))
	}
	if block.Value(dvid.Point3d{58, 4, 31}) != 2 {
		t.Errorf("Expected 2, got %d\n", block.Value(dvid.Point3d{58, 4, 31}))
	}

	solid2, size := block.MakeLabelVolume()
	if !size.Equals(dvid.Point3d{64, 64, 64}) {
		t.Errorf("Expected solid2 block size of %s, got %s\n", "(64,64,64)", size)
	}
	if len(solid2) != 64*64*64*8 {
		t.Errorf("Expected solid2 uint64 array to have 64x64x64x8 bytes, got %d bytes instead", len(solid2))
	}
	uint64array, err := dvid.ByteToUint64(solid2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64*64*64; i++ {
		if uint64array[i] != 2 {
			t.Fatalf("Expected solid2 label volume to have all 2's, found %d at pos %d\n", uint64array[i], i)
		}
	}

	blockTest(t, solidTestData(2))
	for _, filename := range testFiles {
		blockTest(t, loadTestData(t, filename))
	}

	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		testvol[i] = uint64(i)
	}

	bptr, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	testvol2, size := bptr.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("error in size after making block: %v\n", size)
	}
	checkLabels(t, "block compress/uncompress", dvid.Uint64ToByte(testvol), testvol2)
	i := uint64(12*64*64 + 22*64 + 34)
	if bptr.Value(dvid.Point3d{34, 22, 12}) != i {
		t.Errorf("Expected %d, got %d\n", bptr.Value(dvid.Point3d{34, 22, 12}), i)
	}
	i = 31*64*64 + 4*64 + 58
	if bptr.Value(dvid.Point3d{58, 4, 31}) != i {
		t.Errorf("Expected %d, got %d\n", bptr.Value(dvid.Point3d{58, 4, 31}), i)
	}
}

func TestBlockDownres(t *testing.T) {
	// Test solid blocks
	solidOctants := [8]*Block{
		MakeSolidBlock(3, dvid.Point3d{64, 64, 64}),
		MakeSolidBlock(3, dvid.Point3d{64, 64, 64}),
		MakeSolidBlock(9, dvid.Point3d{64, 64, 64}),
		nil,
		MakeSolidBlock(12, dvid.Point3d{64, 64, 64}),
		MakeSolidBlock(21, dvid.Point3d{64, 64, 64}),
		MakeSolidBlock(83, dvid.Point3d{64, 64, 64}),
		MakeSolidBlock(129, dvid.Point3d{64, 64, 64}),
	}

	// make reference volume that is downres.
	gt := make([]uint64, 64*64*64)
	gtarr := dvid.Uint64ToByte(gt)
	for z := 0; z < 32; z++ {
		for y := 0; y < 32; y++ {
			for x := 0; x < 32; x++ {
				// octant (0,0,0)
				i := z*64*64 + y*64 + x
				gt[i] = 3

				// octant (1,0,0)
				i = z*64*64 + y*64 + x + 32
				gt[i] = 3

				// octant (0,1,0)
				i = z*64*64 + (y+32)*64 + x
				gt[i] = 9

				// octant (1,1,0)
				i = z*64*64 + (y+32)*64 + x + 32
				gt[i] = 0

				// octant (0,0,1)
				i = (z+32)*64*64 + y*64 + x
				gt[i] = 12

				// octant (1,0,1)
				i = (z+32)*64*64 + y*64 + x + 32
				gt[i] = 21

				// octant (0,1,1)
				i = (z+32)*64*64 + (y+32)*64 + x
				gt[i] = 83

				// octant (1,1,1)
				i = (z+32)*64*64 + (y+32)*64 + x + 32
				gt[i] = 129
			}
		}
	}

	// run tests using solid block octants
	var b Block
	b.Size = dvid.Point3d{64, 64, 64}
	if err := b.DownresSlow(solidOctants); err != nil {
		t.Fatalf("bad downres: %v\n", err)
	}
	resultBytes, size := b.MakeLabelVolume()
	if !size.Equals(b.Size) {
		t.Fatalf("expected downres block size to be %s, got %s\n", b.Size, size)
	}
	if len(resultBytes) != 64*64*64*8 {
		t.Fatalf("expected downres block volume bytes to be 64^3 * 8, not %d\n", len(resultBytes))
	}
	checkLabels(t, "checking DownresSlow with ground truth", gtarr, resultBytes)

	var b2 Block
	b2.Size = dvid.Point3d{64, 64, 64}
	if err := b2.Downres(solidOctants); err != nil {
		t.Fatalf("bad downres: %v\n", err)
	}
	resultBytes, size = b2.MakeLabelVolume()
	if !size.Equals(b.Size) {
		t.Fatalf("expected downres block size to be %s, got %s\n", b2.Size, size)
	}
	if len(resultBytes) != 64*64*64*8 {
		t.Fatalf("expected downres block volume bytes to be 64^3 * 8, not %d\n", len(resultBytes))
	}
	checkLabels(t, "checking Downres with ground truth", gtarr, resultBytes)
}

func setLabel(vol []uint64, size, x, y, z int, label uint64) {
	i := z*size*size + y*size + x
	vol[i] = label
}

func TestSolidBlockRLE(t *testing.T) {
	var buf bytes.Buffer
	lbls := Set{}
	lbls[3] = struct{}{}
	outOp := NewOutputOp(&buf)
	go WriteRLEs(lbls, outOp, dvid.Bounds{})

	block := MakeSolidBlock(3, dvid.Point3d{64, 64, 64})
	pb := PositionedBlock{
		Block:  *block,
		BCoord: dvid.ChunkPoint3d{2, 1, 2}.ToIZYXString(),
	}
	outOp.Process(&pb)

	pb2 := PositionedBlock{
		Block:  *block,
		BCoord: dvid.ChunkPoint3d{3, 1, 2}.ToIZYXString(),
	}
	outOp.Process(&pb2)

	// include a solid half-block so we can make sure RLE pasting across blocks works.
	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	for z := 0; z < 64; z++ {
		for y := 0; y < 32; y++ {
			for x := 0; x < 64; x++ {
				i := z*64*64 + y*64 + x
				testvol[i] = 3
			}
		}
	}
	block2, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	pb3 := PositionedBlock{
		Block:  *block2,
		BCoord: dvid.ChunkPoint3d{4, 1, 2}.ToIZYXString(),
	}
	outOp.Process(&pb3)

	if err := outOp.Finish(); err != nil {
		t.Fatalf("error writing RLEs: %v\n", err)
	}
	output, err := ioutil.ReadAll(&buf)
	if err != nil {
		t.Fatalf("error on reading WriteRLEs: %v\n", err)
	}

	expectedNumRLEs := 64 * 64 * 16
	if len(output) != expectedNumRLEs {
		t.Fatalf("expected %d RLEs (%d bytes), got %d bytes\n", expectedNumRLEs/16, expectedNumRLEs, len(output))
	}
	var rles dvid.RLEs
	if err = rles.UnmarshalBinary(output); err != nil {
		t.Fatalf("unable to parse binary RLEs: %v\n", err)
	}
	var i int
	expected := make(dvid.RLEs, 64*64)
	for z := int32(0); z < 64; z++ {
		for y := int32(0); y < 32; y++ {
			expected[i] = dvid.NewRLE(dvid.Point3d{128, y + 64, z + 128}, 192)
			i++
		}
	}
	for z := int32(0); z < 64; z++ {
		for y := int32(32); y < 64; y++ {
			expected[i] = dvid.NewRLE(dvid.Point3d{128, y + 64, z + 128}, 128)
			i++
		}
	}
	expected = expected.Normalize()
	for i, rle := range rles.Normalize() {
		if rle != expected[i] {
			t.Errorf("Expected RLE %d: %s, got %s\n", i, expected[i], rle)
		}
	}
}

func TestBlockMerge(t *testing.T) {
	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	labels := make([]uint64, 5)
	mergeSet := make(Set, 3)
	for i := 0; i < 5; i++ {
		var newlabel uint64
		for {
			newlabel = uint64(rand.Int63())
			ok := true
			for j := 0; j < i; j++ {
				if newlabel == labels[j] {
					ok = false
				}
			}
			if ok {
				break
			}
		}
		labels[i] = newlabel
		if i > 1 {
			mergeSet[labels[i]] = struct{}{}
		}
	}
	for i := 0; i < numVoxels; i++ {
		testvol[i] = labels[i%5]
	}
	block, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	op := MergeOp{
		Target: labels[0],
		Merged: mergeSet,
	}
	mergedBlock, err := block.MergeLabels(op)
	if err != nil {
		t.Fatalf("error merging block: %v\n", err)
	}
	volbytes, size := mergedBlock.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("bad merged block size returned: %s\n", size)
	}
	uint64arr, err := dvid.ByteToUint64(volbytes)
	if err != nil {
		t.Fatalf("error converting label byte array: %v\n", err)
	}
	for i := 0; i < numVoxels; i++ {
		curlabel := uint64arr[i]
		_, wasMerged := mergeSet[curlabel]
		if wasMerged {
			t.Fatalf("found voxel %d had label %d when it %s merged -> %d\n", i, curlabel, mergeSet, labels[0])
		} else if curlabel != labels[0] && curlabel != labels[1] {
			t.Fatalf("found voxel %d had label %d which is not expected labels %d or %d\n", i, curlabel, labels[0], labels[1])
		}
	}
}

func TestBlockReplaceLabel(t *testing.T) {
	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		testvol[i] = 4
	}
	block, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	_, replaceSize, err := block.ReplaceLabel(2, 1)
	if err != nil {
		t.Fatalf("error replacing block: %v\n", err)
	}
	if replaceSize != 0 {
		t.Errorf("expected replaced # voxels to be zero, got %d\n", replaceSize)
	}
	tmpblock, replaceSize, err := block.ReplaceLabel(4, 1)
	if err != nil {
		t.Fatalf("error replacing block: %v\n", err)
	}
	if replaceSize != uint64(numVoxels) {
		t.Errorf("expected replaced # voxels to be %d, got %d\n", numVoxels, replaceSize)
	}
	volbytes, size := tmpblock.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("bad replaced block size returned: %s\n", size)
	}
	uint64arr, err := dvid.ByteToUint64(volbytes)
	if err != nil {
		t.Fatalf("error converting label byte array: %v\n", err)
	}
	for i, label := range uint64arr {
		if label != 1 {
			t.Errorf("expected label 1 after replacement in voxel %d, got %d\n", i, label)
			break
		}
	}

	labels := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		var newlabel uint64
		for {
			newlabel = uint64(rand.Int63())
			ok := true
			for j := 0; j < i; j++ {
				if newlabel == labels[j] {
					ok = false
				}
			}
			if ok {
				break
			}
		}
		labels[i] = newlabel
	}
	for i := 0; i < numVoxels; i++ {
		testvol[i] = labels[i%4]
	}
	block, err = MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	tmpblock, replaceSize, err = block.ReplaceLabel(labels[0], labels[4])
	if err != nil {
		t.Fatalf("error merging block: %v\n", err)
	}
	if replaceSize != uint64(numVoxels/4) {
		t.Errorf("expected replaced # voxels with %d = %d, got %d\n", labels[0], numVoxels/4, replaceSize)
	}
	replacedBlock, replaceSize, err := tmpblock.ReplaceLabel(labels[1], labels[2])
	if err != nil {
		t.Fatalf("error merging block: %v\n", err)
	}
	volbytes, size = replacedBlock.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("bad replaced block size returned: %s\n", size)
	}
	uint64arr, err = dvid.ByteToUint64(volbytes)
	if err != nil {
		t.Fatalf("error converting label byte array: %v\n", err)
	}
	for i := 0; i < numVoxels; i++ {
		switch uint64arr[i] {
		case labels[0]:
			t.Fatalf("bad label at voxel %d: %d, should have been replaced by label %d\n", i, uint64arr[i], labels[4])
		case labels[1]:
			t.Fatalf("bad label at voxel %d: %d, should have been replaced by label %d\n", i, uint64arr[i], labels[2])
		case labels[2], labels[3], labels[4]:
			// good
		default:
			t.Fatalf("bad label at voxel %d: %d, not in expected labels %v\n", i, uint64arr[i], labels)
		}
	}
}

func TestBlockReplaceLabels(t *testing.T) {
	numVoxels := 64 * 64 * 64
	testvol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		testvol[i] = uint64(i) % 4
	}
	block, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}
	mapping := map[uint64]uint64{
		0: 5,
		1: 6,
		2: 7,
		3: 8,
	}
	replaceBlock, replaced, err := block.ReplaceLabels(mapping)
	if err != nil {
		t.Fatalf("error replacing block: %v\n", err)
	}
	if !replaced {
		t.Errorf("expected replacement and got none\n")
	}
	volbytes, size := replaceBlock.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("bad replaced block size returned: %s\n", size)
	}
	uint64arr, err := dvid.ByteToUint64(volbytes)
	if err != nil {
		t.Fatalf("error converting label byte array: %v\n", err)
	}
	for i, label := range uint64arr {
		if label != uint64(i%4)+5 {
			t.Errorf("expected label %d after replacement in voxel %d, got %d\n", (i%4)+5, i, label)
			break
		}
	}
}

func TestBlockSplitAndRLEs(t *testing.T) {
	numVoxels := 32 * 32 * 32
	testvol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		testvol[i] = uint64(i)
	}
	label := uint64(numVoxels * 10) // > previous labels
	for x := 11; x < 31; x++ {
		setLabel(testvol, 32, x, 8, 16, label)
	}
	block, err := MakeBlock(dvid.Uint64ToByte(testvol), dvid.Point3d{32, 32, 32})
	if err != nil {
		t.Fatalf("error making block: %v\n", err)
	}

	splitRLEs := dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{81, 40, 80}, 6),
		dvid.NewRLE(dvid.Point3d{90, 40, 80}, 3),
	}
	op := SplitOp{NewLabel: label + 1, Target: label, RLEs: splitRLEs}
	pb := PositionedBlock{
		Block:  *block,
		BCoord: dvid.ChunkPoint3d{2, 1, 2}.ToIZYXString(),
	}
	split, keptSize, splitSize, err := pb.Split(op)
	if err != nil {
		t.Fatalf("error doing split: %v\n", err)
	}
	if keptSize != 11 {
		t.Errorf("Expected kept size of 11, got %d\n", keptSize)
	}
	if splitSize != 9 {
		t.Errorf("Expected split size of 9, got %d\n", splitSize)
	}

	var buf bytes.Buffer
	lbls := Set{}
	lbls[label] = struct{}{}
	outOp := NewOutputOp(&buf)
	go WriteRLEs(lbls, outOp, dvid.Bounds{})
	pb = PositionedBlock{
		Block:  *split,
		BCoord: dvid.ChunkPoint3d{2, 1, 2}.ToIZYXString(),
	}
	outOp.Process(&pb)
	if err = outOp.Finish(); err != nil {
		t.Fatalf("error writing RLEs: %v\n", err)
	}
	output, err := ioutil.ReadAll(&buf)
	if err != nil {
		t.Fatalf("error on reading WriteRLEs: %v\n", err)
	}
	if len(output) != 3*16 {
		t.Fatalf("expected 3 RLEs (48 bytes), got %d bytes\n", len(output))
	}
	var rles dvid.RLEs
	if err = rles.UnmarshalBinary(output); err != nil {
		t.Fatalf("unable to parse binary RLEs: %v\n", err)
	}
	expected := dvid.RLEs{
		dvid.NewRLE(dvid.Point3d{75, 40, 80}, 6),
		dvid.NewRLE(dvid.Point3d{87, 40, 80}, 3),
		dvid.NewRLE(dvid.Point3d{93, 40, 80}, 2),
	}
	for i, rle := range rles {
		if rle != expected[i] {
			t.Errorf("Expected RLE %d: %s, got %s\n", i, expected[i], rle)
		}
	}
}

func TestSolidBlockSize(t *testing.T) {
	testLabel := uint64(9174832)
	numVoxels := 64 * 64 * 64
	blockVol := make([]uint64, numVoxels)
	for i := 0; i < numVoxels; i++ {
		blockVol[i] = testLabel
	}
	block, err := MakeBlock(dvid.Uint64ToByte(blockVol), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}
	compressed, err := block.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) > 24 {
		t.Errorf("solid block had %d bytes instead of optimal 24 bytes\n", len(compressed))
	}
	solidBlock := MakeSolidBlock(testLabel, dvid.Point3d{64, 64, 64})
	uint64array, size := solidBlock.MakeLabelVolume()
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Fatalf("solid block didn't return appropriate size: %v\n", size)
	}
	uint64array2, size2 := block.MakeLabelVolume()
	if size2[0] != 64 || size2[1] != 64 || size2[2] != 64 {
		t.Fatalf("one label block didn't return appropriate size: %v\n", size2)
	}
	if !bytes.Equal(uint64array, uint64array2) {
		t.Fatalf("solid block path does not equal single label encoded path\n")
	}
	labelVol, err := dvid.ByteToUint64(uint64array)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numVoxels; i++ {
		if labelVol[i] != testLabel {
			t.Fatalf("uncompressed label volume had label %d instead of expected label %d @ position %d\n", labelVol[i], testLabel, i)
		}
	}
}

func TestBinaryBlocks(t *testing.T) {
	numVoxels := 64 * 64 * 64
	blockVol0 := make([]uint64, numVoxels)
	blockVol1 := make([]uint64, numVoxels)

	background := uint64(1000)
	target := uint64(7)
	var i, x, y, z int32
	for z = 0; z < 64; z++ {
		for y = 0; y < 64; y++ {
			for x = 0; x < 64; x++ {
				if x >= 23 && x <= 34 && y >= 37 && y <= 50 {
					blockVol0[i] = target
				} else {
					blockVol0[i] = background
				}
				i++
			}
		}
	}
	i = 0
	for z = 0; z < 64; z++ {
		for y = 0; y < 64; y++ {
			for x = 0; x < 64; x++ {
				if x >= 26 && x <= 31 && y >= 39 && y <= 45 {
					blockVol1[i] = target
				} else {
					blockVol1[i] = background
				}
				i++
			}
		}
	}

	block0, err := MakeBlock(dvid.Uint64ToByte(blockVol0), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}
	block1, err := MakeBlock(dvid.Uint64ToByte(blockVol1), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}

	pb0 := PositionedBlock{
		Block:  *block0,
		BCoord: dvid.ChunkPoint3d{1, 1, 1}.ToIZYXString(),
	}
	pb1 := PositionedBlock{
		Block:  *block1,
		BCoord: dvid.ChunkPoint3d{1, 1, 2}.ToIZYXString(),
	}

	var buf bytes.Buffer
	lbls := Set{}
	lbls[target] = struct{}{}
	outOp := NewOutputOp(&buf)
	go WriteBinaryBlocks(target, lbls, outOp, dvid.Bounds{})
	outOp.Process(&pb0)
	outOp.Process(&pb1)
	if err = outOp.Finish(); err != nil {
		t.Fatalf("error writing binary blocks: %v\n", err)
	}
	got, err := ReceiveBinaryBlocks(&buf)
	if err != nil {
		t.Fatalf("error on reading binary blocks: %v\n", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 binary blocks got %d\n", len(got))
	}
	if got[0].Label != target {
		t.Errorf("expected label %d for binary block 0, got %s\n", target, got[0])
	}
	if got[1].Label != target {
		t.Errorf("expected label %d for binary block 1, got %s\n", target, got[0])
	}
	i = 0
	for z = 0; z < 64; z++ {
		for y = 0; y < 64; y++ {
			for x = 0; x < 64; x++ {
				if x >= 23 && x <= 34 && y >= 37 && y <= 50 {
					if !got[0].Voxels[i] {
						t.Fatalf("error in binary block 0: bad unset bit @ (%d,%d,%d)\n", x, y, z)
					}
				} else {
					if got[0].Voxels[i] {
						t.Fatalf("error in binary block 0: bad set bit @ (%d,%d,%d)\n", x, y, z)
					}
				}
				i++
			}
		}
	}
	i = 0
	for z = 0; z < 64; z++ {
		for y = 0; y < 64; y++ {
			for x = 0; x < 64; x++ {
				if x >= 26 && x <= 31 && y >= 39 && y <= 45 {
					if !got[1].Voxels[i] {
						t.Errorf("error in binary block 1: bad unset bit @ (%d,%d,%d)\n", x, y, z)
					}
				} else {
					if got[1].Voxels[i] {
						t.Errorf("error in binary block 1: bad set bit @ (%d,%d,%d)\n", x, y, z)
					}
				}
				i++
			}
		}
	}
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
