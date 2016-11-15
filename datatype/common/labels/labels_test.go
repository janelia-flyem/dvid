package labels

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestMapping(t *testing.T) {
	var m Mapping
	m.set(1, 4)
	m.set(2, 5)
	m.set(20, 6)
	m.set(6, 32)
	m.set(15, 3)
	m.set(3, 32)
	m.set(8, 32)
	m.set(32, 21)
	if v, ok := m.Get(1); v != 4 || !ok {
		t.Errorf("Incorrect mapping on Get.  Got %d, %t\n", v, ok)
	}
	if v, ok := m.Get(2); v != 5 || !ok {
		t.Errorf("Incorrect mapping on Get.  Got %d, %t\n", v, ok)
	}
	if v, ok := m.Get(20); v != 6 || !ok {
		t.Errorf("Incorrect mapping on Get.  Got %d, %t\n", v, ok)
	}
	if v, ok := m.Get(32); v != 21 || !ok {
		t.Errorf("Incorrect mapping on Get.  Got %d, %t\n", v, ok)
	}
	if v, ok := m.Get(10); ok {
		t.Errorf("Got mapping for 10 when none was inserted.  Received %d, %t\n", v, ok)
	}
	if v, ok := m.FinalLabel(20); v != 21 || !ok {
		t.Errorf("Couldn't get final mapping label from 20->6->32->21.  Got %d, %t\n", v, ok)
	}

	c := m.ConstituentLabels(21)

	expected := []uint64{20, 6, 32, 3, 8, 15, 21}
	if len(c) != len(expected) {
		t.Errorf("Expected %d constituent labels, got %d instead: %s\n", len(expected), len(c), c)
	}
	for _, label := range expected {
		if _, found := c[label]; !found {
			t.Errorf("Expected label %d as constituent but wasn't found.\n", label)
		}
	}
}

func TestCounts(t *testing.T) {
	var c Counts
	if !c.Empty() {
		t.Errorf("Expected Counts to be empty")
	}
	c.Incr(7)
	c.Incr(21)
	c.Decr(21)
	c.Decr(7)
	if !c.Empty() {
		t.Errorf("Expected Counts to be empty after incr/decr cycles")
	}

	c.Incr(9)
	if v := c.Value(9); v != 1 {
		t.Errorf("Bad count.  Expected 1 got %d\n", v)
	}
	// Test thread-safety
	var wg sync.WaitGroup
	wg.Add(500)
	expected := 1
	for i := 0; i < 500; i++ {
		r := rand.Intn(3)
		if r == 0 {
			expected--
			go func() {
				c.Decr(9)
				wg.Done()
			}()
		} else {
			expected++
			go func() {
				c.Incr(9)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	if v := c.Value(9); v != expected {
		t.Errorf("After concurrent, random incr/decr, got %d, expected %d\n", v, expected)
	}

}

func TestMergeCache(t *testing.T) {
	merges := []MergeTuple{
		MergeTuple{4, 1, 2, 3},
		MergeTuple{9, 10, 11, 12},
		MergeTuple{21, 100, 18, 85, 97, 45},
	}
	merges2 := []MergeTuple{
		MergeTuple{9, 5, 11, 6},
		MergeTuple{9, 100, 3, 22},
		MergeTuple{21, 44, 55, 66, 77, 88},
	}
	expectmap := map[uint64]uint64{
		1:   4,
		2:   4,
		3:   4,
		10:  9,
		11:  9,
		12:  9,
		100: 21,
		18:  21,
		85:  21,
		97:  21,
		45:  21,
	}

	expectmap2 := map[uint64]uint64{
		5:   9,
		6:   9,
		11:  9,
		3:   9,
		22:  9,
		100: 9,
		44:  21,
		55:  21,
		66:  21,
		77:  21,
		88:  21,
	}

	iv := dvid.InstanceVersion{"foobar", 23}
	for _, tuple := range merges {
		op, err := tuple.Op()
		if err != nil {
			t.Errorf("Error converting tuple %v to MergeOp: %v\n", tuple, err)
		}
		if err := MergeStart(iv, op); err != nil {
			t.Errorf("Error on starting merge (%v): %v\n", op, err)
		}
	}
	badop, _ := MergeTuple{72, 9, 47}.Op()
	if err := MergeStart(iv, badop); err == nil {
		t.Errorf("Expected error on starting merge (%v): %v\n", badop, err)
	}

	iv2 := dvid.InstanceVersion{"foobar", 24}
	for _, tuple := range merges2 {
		op, err := tuple.Op()
		if err != nil {
			t.Errorf("Error converting tuple %v to MergeOp: %v\n", tuple, err)
		}
		if err := MergeStart(iv2, op); err != nil {
			t.Errorf("Error on starting merge (%v): %v\n", op, err)
		}
	}

	mapping := LabelMap(iv)
	for a, b := range expectmap {
		c, ok := mapping.FinalLabel(a)
		if !ok || c != b {
			t.Errorf("Expected mapping of %d -> %d, got %d (%t) instead\n", a, b, c, ok)
		}
	}
	if _, ok := mapping.FinalLabel(66); ok {
		t.Errorf("Got mapping even though none existed for this version.")
	}
	if label, ok := mapping.FinalLabel(1); !ok || label != 4 {
		t.Errorf("Bad final mapping of label 1.  Got %d, expected 4\n", label)
	}

	mapping2 := LabelMap(iv2)
	for a, b := range expectmap2 {
		c, ok := mapping2.Get(a)
		if !ok || c != b {
			t.Errorf("Expected mapping of %d -> %d, got %d (%t) instead\n", a, b, c, ok)
		}
	}
	if _, ok := mapping2.Get(12); ok {
		t.Errorf("Got mapping even though none existed for this version.")
	}
	if label, ok := mapping2.Get(100); !ok || label != 9 {
		t.Errorf("Bad final mapping of label 100.  Got %d, expected 9\n", label)
	}

	// Add another merge into label 4
	op1, err := MergeTuple{4, 1000, 1001, 1002}.Op()
	if err != nil {
		t.Errorf("Error converting [4, 1000, 1001, 1002] to MergeOp: %v\n", err)
	}
	if err := MergeStart(iv, op1); err != nil {
		t.Fatalf("Couldn't start merge op %v: %v\n", op1, err)
	}

	constituents := mapping.ConstituentLabels(4)
	if len(constituents) != 7 {
		t.Errorf("Expected 7 merged labels into label 4, got: %s\n", constituents)
	}
	for _, label := range []uint64{4, 1, 2, 3, 1000, 1001, 1002} {
		_, found := constituents[label]
		if !found {
			t.Errorf("Expected a constituent label for 4 to be %d but wasn't found: %v\n", label, constituents)
		}
		if !labelsMerging.IsDirty(iv, label) {
			t.Errorf("Expected label %d to be marked Dirty but wasn't.\n", label)
		}
	}

	// Remove the first merge (1,2,3 -> 4) and test that it's gone.
	op2, err := merges[0].Op()
	if err != nil {
		t.Errorf("Error converting tuple %v to MergeOp: %v\n", merges[0], err)
	}
	MergeStop(iv, op2)

	constituents = mapping.ConstituentLabels(4)
	if len(constituents) != 4 {
		t.Errorf("Stoped merge %v for iv %v, but found weird constituents: %s\n", op2, iv, constituents)
	}
	for _, label := range []uint64{4, 1000, 1001, 1002} {
		_, found := constituents[label]
		if !found {
			t.Errorf("Expected constituent label for 4 to be %d but wasn't found: %v\n", label, constituents)
		}
		if !labelsMerging.IsDirty(iv, label) {
			t.Errorf("Expected label %d to be marked Dirty but wasn't.\n", label)
		}
	}

	for _, label := range []uint64{1, 2, 3} {
		_, found := mapping.Get(label)
		if found {
			t.Errorf("Found mapping for label %d when it should have been removed\n", label)
		}
		_, found = mapping.FinalLabel(label)
		if found {
			t.Errorf("Found final mapping for label %d when it should have been removed\n", label)
		}
		if labelsMerging.IsDirty(iv, label) {
			t.Errorf("Expected label %d to be marked NOT Dirty but was marked Dirty.\n", label)
		}
	}

	// Remove other merge op into 4 and test there are no more mappings.
	MergeStop(iv, op1)

	constituents = mapping.ConstituentLabels(4)
	if len(constituents) != 1 {
		t.Errorf("Stoped merge %v for iv %v, but found weird constituents: %s\n", op1, iv, constituents)
	}
	_, found := constituents[4]
	if !found {
		t.Errorf("Expected constituent label for 4 to be just 4 but found: %v\n", constituents)
	}
	if labelsMerging.IsDirty(iv, 4) {
		t.Errorf("Expected label 4 to be marked NOT Dirty but was marked Dirty.\n")
	}

	for _, label := range []uint64{1001, 1002, 1004} {
		_, found := mapping.Get(label)
		if found {
			t.Errorf("Found mapping for label %d when it should have been removed\n", label)
		}
		_, found = mapping.FinalLabel(label)
		if found {
			t.Errorf("Found final mapping for label %d when it should have been removed\n", label)
		}
		if labelsMerging.IsDirty(iv, label) {
			t.Errorf("Expected label %d to be marked NOT Dirty but was marked Dirty.\n", label)
		}
	}
}

func TestDirtyCache(t *testing.T) {
	var c dirtyCache
	iv := dvid.InstanceVersion{"foobar", 23}
	iv2 := dvid.InstanceVersion{"foobar", 24}
	if !c.Empty(iv) && !c.Empty(iv2) {
		t.Errorf("DirtyCache should be considered empty but it's not.")
	}
	c.Incr(iv, 390)
	c.Incr(iv, 84)
	c.Incr(iv, 390)
	if c.Empty(iv) {
		t.Errorf("DirtyCache should be non-empty.")
	}
	if !c.Empty(iv2) {
		t.Errorf("DirtyCache should be empty")
	}
	c.Incr(iv2, 24)
	c.Incr(iv2, 390)
	if c.IsDirty(iv, 1) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv, 84) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	if c.IsDirty(iv, 24) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv2, 24) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	if !c.IsDirty(iv, 390) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	c.Decr(iv, 390)
	if !c.IsDirty(iv, 390) {
		t.Errorf("Label is not marked dirty when it is.")
	}
	c.Decr(iv, 390)
	if c.IsDirty(iv, 390) {
		t.Errorf("Label is marked dirty when it's not.")
	}
	if !c.IsDirty(iv2, 390) {
		t.Errorf("Label is not marked dirty when it is.")
	}
}
