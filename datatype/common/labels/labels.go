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

var (
	mc              mergeCache
	labelsMerging   dirtyCache
	labelsSplitting dirtyCache
)

const (
	// MaxAllowedLabel is the largest label that should be allowed by DVID if we want to take
	// into account the maximum integer size within Javascript (due to its underlying use of
	// a double float for numbers, leading to max int = 2^53 - 1).
	// This would circumvent the need to use strings within JSON (e.g., the Google solution)
	// to represent integer labels that could exceed the max javascript number.  It would
	// require adding a value check on each label voxel of a mutation request, which might
	// be too much of a hit to handle an edge case.
	MaxAllowedLabel = 9007199254740991
)

// LabelMap returns a label mapping for a version of a data instance.
// If no label mapping is available, a nil is returned.
func LabelMap(iv dvid.InstanceVersion) *Mapping {
	return mc.LabelMap(iv)
}

// MergeStart handles label map caches during an active merge operation.  Note that if there are
// multiple synced label instances, the InstanceVersion always be the labelblk instance.
func MergeStart(iv dvid.InstanceVersion, op MergeOp) error {
	// Don't allow a merge to start in the middle of a concurrent split.
	if labelsSplitting.IsDirty(iv, op.Target) { // we might be able to relax this one.
		return fmt.Errorf("can't merge into label %d while it has an ongoing split", op.Target)
	}
	for merged := range op.Merged {
		if labelsSplitting.IsDirty(iv, merged) {
			return fmt.Errorf("can't merge label %d while it has an ongoing split", merged)
		}
	}

	// Add the merge to the mapping.
	if err := mc.Add(iv, op); err != nil {
		return err
	}

	// Adjust the dirty counts on the involved labels.
	labelsMerging.AddMerge(iv, op)

	return nil
}

// MergeStop marks the end of a merge operation.
func MergeStop(iv dvid.InstanceVersion, op MergeOp) {
	// Adjust the dirty counts on the involved labels.
	labelsMerging.RemoveMerge(iv, op)

	// If the instance version's dirty cache is empty, we can delete the merge cache.
	if labelsMerging.Empty(iv) {
		mc.DeleteMap(iv)
	}
}

// SplitStart checks current label map to see if the split conflicts.
func SplitStart(iv dvid.InstanceVersion, op DeltaSplitStart) error {
	if labelsMerging.IsDirty(iv, op.NewLabel) {
		return fmt.Errorf("can't split into label %d while it is undergoing a merge", op.NewLabel)
	}
	if labelsMerging.IsDirty(iv, op.OldLabel) {
		return fmt.Errorf("can't split label %d while it is undergoing a merge", op.OldLabel)
	}
	labelsSplitting.Incr(iv, op.NewLabel)
	labelsSplitting.Incr(iv, op.OldLabel)
	return nil
}

// SplitStop marks the end of a split operation.
func SplitStop(iv dvid.InstanceVersion, op DeltaSplitEnd) {
	labelsSplitting.Decr(iv, op.NewLabel)
	labelsSplitting.Decr(iv, op.OldLabel)
}

type mergeCache struct {
	sync.RWMutex
	m map[dvid.InstanceVersion]Mapping
}

// Add adds a merge operation to the given InstanceVersion's cache.
func (mc *mergeCache) Add(iv dvid.InstanceVersion, op MergeOp) error {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]Mapping)
	}
	mapping, found := mc.m[iv]
	if !found {
		mapping = Mapping{
			f: make(map[uint64]uint64, len(op.Merged)),
			r: make(map[uint64]Set),
		}
	}
	for merged := range op.Merged {
		if err := mapping.set(merged, op.Target); err != nil {
			return err
		}
	}
	mc.m[iv] = mapping
	return nil
}

// LabelMap returns a label mapping for a version of a data instance.
// If no label mapping is available, a nil is returned.
func (mc *mergeCache) LabelMap(iv dvid.InstanceVersion) *Mapping {
	mc.RLock()
	defer mc.RUnlock()

	if mc.m == nil {
		return nil
	}
	mapping, found := mc.m[iv]
	if found {
		if len(mapping.f) == 0 {
			return nil
		}
		return &mapping
	}
	return nil
}

// DeleteMap removes a mapping of the given InstanceVersion.
func (mc *mergeCache) DeleteMap(iv dvid.InstanceVersion) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m != nil {
		delete(mc.m, iv)
	}
}

// Mapping is a thread-safe, mapping of labels to labels in both forward and backward direction.
// Mutation of a Mapping instance can only be done through labels.MergeCache.
type Mapping struct {
	sync.RWMutex
	f map[uint64]uint64
	r map[uint64]Set
}

// ConstituentLabels returns a set of labels that will be mapped to the given label.
// The function will return a set of just the given label if there are no other
// original labels that map to it.
func (m Mapping) ConstituentLabels(final uint64) Set {
	m.RLock()
	defer m.RUnlock()

	if m.r == nil {
		return Set{final: struct{}{}}
	}
	// We need to return all labels that will eventually have the given final label
	// including any intermediate ones that were subsequently merged.
	s, found := m.r[final]
	if !found {
		return Set{final: struct{}{}}
	}
	return s
}

// FinalLabel follows mappings from a start label until
// a final mapped label is reached.
func (m Mapping) FinalLabel(start uint64) (uint64, bool) {
	m.RLock()
	defer m.RUnlock()

	if m.f == nil {
		return start, false
	}
	cur := start
	found := false
	for {
		v, ok := m.f[cur]
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

	if m.f == nil {
		return 0, false
	}
	mapped, found := m.f[label]
	if found {
		return mapped, true
	}
	return 0, false
}

// set returns error if b is currently being mapped to another label.
func (m *Mapping) set(a, b uint64) error {
	m.Lock()
	defer m.Unlock()

	if m.f == nil {
		m.f = make(map[uint64]uint64)
		m.r = make(map[uint64]Set)
	} else {
		if c, found := m.f[b]; found {
			return fmt.Errorf("label %d is currently getting merged into label %d", b, c)
		}
	}
	m.f[a] = b
	m.r[b] = Set{a: struct{}{}}
	return nil
}

func (m *Mapping) delete(label uint64) {
	m.Lock()
	defer m.Unlock()

	if m.f != nil {
		mapped, found := m.f[label]
		if !found {
			return
		}
		delete(m.f, label)
		s, found := m.r[mapped]
		if found {
			delete(s, label)
			m.r[mapped] = s
		}
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

// dirtyCache is a thread-safe cache for tracking dirty labels across versions, which is necessary when we
// don't know exactly how a label is being transformed.  For example, when merging
// we can easily track what a label will be, however during a split, we don't know whether
// a particular voxel with label X will become label Y unless we also store the split
// voxels.  So DirtyCache is good for tracking "changing" status in splits while MergeCache
// can give us complete label transformation of non-dirty labels.
type dirtyCache struct {
	sync.RWMutex
	dirty map[dvid.InstanceVersion]*Counts
}

func (d *dirtyCache) Incr(iv dvid.InstanceVersion, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}
	d.incr(iv, label)
}

func (d *dirtyCache) Decr(iv dvid.InstanceVersion, label uint64) {
	d.Lock()
	defer d.Unlock()

	if d.dirty == nil {
		d.dirty = make(map[dvid.InstanceVersion]*Counts)
	}
	d.decr(iv, label)
}

func (d *dirtyCache) IsDirty(iv dvid.InstanceVersion, label uint64) bool {
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

func (d *dirtyCache) Empty(iv dvid.InstanceVersion) bool {
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

func (d *dirtyCache) AddMerge(iv dvid.InstanceVersion, op MergeOp) {
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

func (d *dirtyCache) RemoveMerge(iv dvid.InstanceVersion, op MergeOp) {
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

func (d *dirtyCache) incr(iv dvid.InstanceVersion, label uint64) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		cnts = new(Counts)
		d.dirty[iv] = cnts
	}
	cnts.Incr(label)
}

func (d *dirtyCache) decr(iv dvid.InstanceVersion, label uint64) {
	cnts, found := d.dirty[iv]
	if !found || cnts == nil {
		dvid.Errorf("decremented non-existant count for label %d, version %v\n", label, iv)
		return
	}
	cnts.Decr(label)
}
