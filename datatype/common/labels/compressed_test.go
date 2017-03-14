package labels

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

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
			t.Error("%s byte mismatch found at index %d, expected %d, got %d\n", text, expected[i], val)
			return
		}
	}
}

func printRatio(t *testing.T, curDesc string, cur, orig int, compareDesc string, compare int) {
	p1 := 100.0 * float32(cur) / float32(orig)
	p2 := float32(cur) / float32(compare)
	t.Logf("%s compression result: %d (%4.2f%% orig, %4.2f x %s)\n", curDesc, cur, p1, p2, compareDesc)
}

func blockTest(t *testing.T, filename string) {
	data, err := readGzipFile(filename)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("Loaded %q: %d bytes\n", filename, len(data))
	labeldata, err := byteToUint64(data)
	if err != nil {
		t.Fatal(err)
	}
	labels := make(map[uint64]int)
	for _, label := range labeldata {
		labels[label]++
	}
	for label, voxels := range labels {
		t.Logf("Label %12d: %8d voxels\n", label, voxels)
	}

	cseg, err := CompressUint64(labeldata, dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatal(err)
	}

	block, err := MakeBlock(data, dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatal(err)
	}
	blockdata, _ := block.MarshalBinary()
	printRatio(t, "Plaza", len(cseg)*4, len(data), "Katz", len(blockdata))
	printRatio(t, " Katz", len(blockdata), len(data), "Plaza", len(cseg)*4)
	redata, size, err := block.MakeLabelVolume()
	if err != nil {
		t.Error(err)
	}
	if size[0] != 64 || size[1] != 64 || size[2] != 64 {
		t.Errorf("expected katz compression size to be 64x64x64, got %s\n", size)
	}
	checkBytes(t, "katz compression", data, redata)
}

func TestBlockCompression(t *testing.T) {
	blockTest(t, "../../../test_data/fib19-64x64x64-sample1.dat.gz")
	blockTest(t, "../../../test_data/fib19-64x64x64-sample2.dat.gz")
	blockTest(t, "../../../test_data/cx-64x64x64-sample1.dat.gz")
	blockTest(t, "../../../test_data/cx-64x64x64-sample2.dat.gz")
}
