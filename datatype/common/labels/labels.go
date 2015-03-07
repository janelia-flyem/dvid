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

// Mapping is a mapping of labels to labels.
type Mapping map[uint64]uint64

// FinalLabel follows mappings from a start label until
// a final mapped label is reached.
func (m Mapping) FinalLabel(start uint64) uint64 {
	if m == nil {
		return start
	}
	cur := start
	for {
		var found bool
		cur, found = m[cur]
		if !found {
			break
		}
	}
	return cur
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

// MergeCache is a thread-safe cache for merge operations that provides
// mapping at any point in time.
type MergeCache struct {
	sync.RWMutex
	m map[dvid.InstanceVersion]Mapping
}

func (mc *MergeCache) Add(v dvid.InstanceVersion, op MergeOp) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]Mapping)
	}
	mapping, found := mc.m[v]
	if !found {
		mapping = make(Mapping, len(op.Merged))
	}
	for merged := range op.Merged {
		mapping[merged] = op.Target
	}
	mc.m[v] = mapping
}

func (mc *MergeCache) Remove(v dvid.InstanceVersion, op MergeOp) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]Mapping)
		return
	}
	mapping, found := mc.m[v]
	if !found {
		return
	}
	for merged := range op.Merged {
		delete(mapping, merged)
	}
	mc.m[v] = mapping
}

// LabelMap returns a label mapping for a version of a data instance.
// If no label mapping is available, a nil is returned.
func (mc *MergeCache) LabelMap(v dvid.InstanceVersion) Mapping {
	mc.RLock()
	defer mc.RUnlock()

	if mc.m == nil {
		return nil
	}
	mapping, found := mc.m[v]
	if found {
		if len(mapping) == 0 {
			return nil
		}
		return mapping
	}
	return nil
}

// DirtyCache tracks dirty labels across versions
type DirtyCache struct {
	sync.RWMutex
	dirty map[dvid.InstanceVersion]*Counts
}

func (d *DirtyCache) Incr(name dvid.InstanceName, v dvid.VersionID, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}

	iv := dvid.InstanceVersion{name, v}
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		cnts = &Counts{}
	}
	cnts.Incr(label)
}

func (d *DirtyCache) Decr(name dvid.InstanceName, v dvid.VersionID, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}

	iv := dvid.InstanceVersion{name, v}
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		dvid.Errorf("decremented non-existant count for label %d, data %q, version %d\n", label, name, v)
		return
	}
	cnts.Decr(label)
}

func (d *DirtyCache) IsDirty(name dvid.InstanceName, v dvid.VersionID, label uint64) bool {
	d.RLock()
	defer d.RUnlock()

	if d.dirty == nil {
		return false
	}

	iv := dvid.InstanceVersion{name, v}
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		return false
	}
	if cnts.Value(label) == 0 {
		return false
	}
	return true
}
