/*
	Package labels supports label-based data types like labelblk, labelvol, labelsurf, labelsz, etc.
	Basic 64-bit label data and deltas are kept here so all label-based data types can use them without
	cyclic package dependencies, especially when writing code to synchronize across data instances.
*/
package labels

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

// Mapping is an immutable thread-safe mapping of labels to labels.
type Mapping struct {
	sync.RWMutex
	m map[uint64]uint64
}

// FinalLabel follows mappings from a start label until
// a final mapped label is reached.
func (m Mapping) FinalLabel(start uint64) (uint64, bool) {
	m.RLock()
	defer m.RUnlock()

	if m.m == nil {
		return start, false
	}
	cur := start
	found := false
	for {
		v, ok := m.m[cur]
		if !ok {
			break
		}
		cur = v
		found = true
	}
	return cur, found
}

// Get returns the mapping or false if no mapping exists.
func (m Mapping) Get(label uint64) (uint64, bool) {
	m.RLock()
	defer m.RUnlock()

	if m.m == nil {
		return 0, false
	}
	mapped, found := m.m[label]
	if found {
		return mapped, true
	}
	return 0, false
}

func (m *Mapping) set(a, b uint64) {
	m.Lock()
	defer m.Unlock()

	if m.m == nil {
		m.m = make(map[uint64]uint64)
	}
	m.m[a] = b
}

func (m *Mapping) delete(label uint64) {
	m.Lock()
	defer m.Unlock()

	if m.m != nil {
		delete(m.m, label)
	}
}

// Set is a set of labels.
type Set map[uint64]struct{}

func (s Set) String() string {
	var str string
	for k := range s {
		str += fmt.Sprintf("%d ", k)
	}
	return str
}

// Counts is a thread-safe type for counting label references.
type Counts struct {
	sync.RWMutex
	m map[uint64]int
}

// Incr increments the count for a label.
func (c *Counts) Incr(label uint64) {
	if c.m == nil {
		c.m = make(map[uint64]int)
	}
	c.Lock()
	defer c.Unlock()
	c.m[label] = c.m[label] + 1
}

// Decr decrements the count for a label.
func (c *Counts) Decr(label uint64) {
	if c.m == nil {
		c.m = make(map[uint64]int)
	}
	c.Lock()
	defer c.Unlock()
	c.m[label] = c.m[label] - 1
	if c.m[label] == 0 {
		delete(c.m, label)
	}
}

// Value returns the count for a label.
func (c *Counts) Value(label uint64) int {
	if c.m == nil {
		return 0
	}
	c.RLock()
	defer c.RUnlock()
	return c.m[label]
}

// Empty returns true if there are no counts.
func (c *Counts) Empty() bool {
	if len(c.m) == 0 {
		return true
	}
	return false
}

// MergeCache is a thread-safe cache for merge operations that provides
// mapping at any point in time.
type MergeCache struct {
	sync.RWMutex
	m map[dvid.InstanceVersion]Mapping
}

func (mc *MergeCache) Add(iv dvid.InstanceVersion, op MergeOp) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]Mapping)
	}
	mapping, found := mc.m[iv]
	if !found {
		mapping = Mapping{m: make(map[uint64]uint64, len(op.Merged))}
	}
	for merged := range op.Merged {
		mapping.set(merged, op.Target)
	}
	mc.m[iv] = mapping
}

func (mc *MergeCache) Remove(iv dvid.InstanceVersion, op MergeOp) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]Mapping)
		return
	}
	mapping, found := mc.m[iv]
	if !found {
		return
	}
	for merged := range op.Merged {
		mapping.delete(merged)
	}
	mc.m[iv] = mapping
}

// LabelMap returns a label mapping for a version of a data instance.
// If no label mapping is available, a nil is returned.
func (mc *MergeCache) LabelMap(iv dvid.InstanceVersion) *Mapping {
	mc.RLock()
	defer mc.RUnlock()

	if mc.m == nil {
		return nil
	}
	mapping, found := mc.m[iv]
	if found {
		if len(mapping.m) == 0 {
			return nil
		}
		return &mapping
	}
	return nil
}

// DirtyCache tracks dirty labels across versions
type DirtyCache struct {
	sync.RWMutex
	dirty map[dvid.InstanceVersion]*Counts
}

func (d *DirtyCache) Incr(iv dvid.InstanceVersion, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}
	d.incr(iv, label)
}

func (d *DirtyCache) Decr(iv dvid.InstanceVersion, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}
	d.decr(iv, label)
}

func (d *DirtyCache) IsDirty(iv dvid.InstanceVersion, label uint64) bool {
	d.RLock()
	defer d.RUnlock()

	if d.dirty == nil {
		return false
	}

	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		return false
	}
	if cnts.Value(label) == 0 {
		return false
	}
	return true
}

func (d *DirtyCache) Empty(iv dvid.InstanceVersion) bool {
	d.RLock()
	defer d.RUnlock()

	if len(d.dirty) == 0 {
		return true
	}
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		return true
	}
	return cnts.Empty()
}

func (d *DirtyCache) AddMerge(iv dvid.InstanceVersion, op MergeOp) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}

	d.incr(iv, op.Target)
	for label := range op.Merged {
		d.incr(iv, label)
	}
}

func (d *DirtyCache) RemoveMerge(iv dvid.InstanceVersion, op MergeOp) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}

	d.decr(iv, op.Target)
	for label := range op.Merged {
		d.decr(iv, label)
	}
}

func (d *DirtyCache) incr(iv dvid.InstanceVersion, label uint64) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		cnts = new(Counts)
		d.dirty[iv] = cnts
	}
	cnts.Incr(label)
}

func (d *DirtyCache) decr(iv dvid.InstanceVersion, label uint64) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		dvid.Errorf("decremented non-existant count for label %d, version %v\n", label, iv)
		return
	}
	cnts.Decr(label)
}
