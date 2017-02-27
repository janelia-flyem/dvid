package labels

import (
	"fmt"
	"math/rand"
	"testing"
)

func dataEqual(d1, d2 []uint64) error {
	if len(d1) != len(d2) {
		return fmt.Errorf("array mismatch")
	}
	for index, val := range d1 {
		if val != d2[index] {
			return fmt.Errorf("array mismatch %d %d", val, d2[index])
		}
	}

	return nil
}

func TestCompressEmpty(t *testing.T) {
	volsize := NewVolSize(32, 32, 32)
	data := make([]uint64, 32*32*32)

	for index, _ := range data {
		data[index] = 13113523135
	}

	cseg, err := CompressUint64(data, volsize)
	if err != nil {
		t.Error(err)
		return
	}

	data2, err := DecompressUint64(cseg, volsize)
	if err != nil {
		t.Error(err)
		return
	}
	if err = dataEqual(data, data2); err != nil {
		t.Error(err)
		return
	}
}

func TestCompressBig(t *testing.T) {
	volsize := NewVolSize(128, 128, 128)
	data := make([]uint64, 128*128*128)

	for index, _ := range data {
		data[index] = 13113523135
	}
	data[0] = 5
	data[8] = 6
	data[9] = 7

	cseg, err := CompressUint64(data, volsize)
	if err != nil {
		t.Error(err)
		return
	}

	data2, err := DecompressUint64(cseg, volsize)
	if err != nil {
		t.Error(err)
		return
	}
	if err = dataEqual(data, data2); err != nil {
		t.Error(err)
		return
	}
}

func TestCompressRand(t *testing.T) {
	volsize := NewVolSize(32, 32, 32)
	data := make([]uint64, 32*32*32)

	r := rand.New(rand.NewSource(1))
	for index, _ := range data {
		data[index] = uint64(r.Int63())
	}

	cseg, err := CompressUint64(data, volsize)
	if err != nil {
		t.Error(err)
		return
	}

	data2, err := DecompressUint64(cseg, volsize)
	if err != nil {
		t.Error(err)
		return
	}
	if err = dataEqual(data, data2); err != nil {
		t.Error(err)
		return
	}
}
