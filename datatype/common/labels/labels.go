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
	// MaxAllowedLabel is the largest label that should be allowed by DVID if we want
	// to take into account the maximum integer size within Javascript (due to its
	// underlying use of a double float for numbers, leading to max int = 2^53 - 1).
	// This would circumvent the need to use strings within JSON (e.g., the Google
	// solution) to represent integer labels that could exceed the max javascript
	// number.  It would require adding a value check on each label voxel of a
	// mutation request, which might be too much of a hit to handle an edge case.
	MaxAllowedLabel = 9007199254740991
)

// LabelMap returns a label mapping for a version of a data instance.
// If no label mapping is available, a nil is returned.
func LabelMap(iv dvid.InstanceVersion) *Mapping {
	return mc.LabelMap(iv)
}

// MergeStart handles label map caches during an active merge operation.  Note that if there are
// multiple synced label instances, the InstanceVersion will always be the labelblk instance.
// Multiple merges into a single label are allowed, but chained merges are not.  For example,
// you can merge label 1, 2, and 3 into 4, then later merge 6 into 4.  However, you cannot
// concurrently merge label 4 into some other label because there can be a race condition between
// 3 -> 4 and 4 -> X.
func MergeStart(iv dvid.InstanceVersion, op MergeOp) error {
	// Don't allow a merge to start in the middle of a concurrent merge/split.
	if labelsSplitting.IsDirty(iv, op.Target) { // we might be able to relax this one.
		return fmt.Errorf("can't merge into label %d while it has an ongoing split", op.Target)
	}
	if mc.MergingToOther(iv, op.Target) {
		dvid.Errorf("can't merge label %d while it is currently merging into another label", op.Target)
		return fmt.Errorf("can't merge label %d while it is currently merging into another label", op.Target)
	}
	for merged := range op.Merged {
		if labelsSplitting.IsDirty(iv, merged) {
			return fmt.Errorf("can't merge label %d while it has an ongoing split", merged)
		}
		if labelsMerging.IsDirty(iv, merged) {
			dvid.Errorf("can't merge label %d while it is currently involved in a merge", merged)
			return fmt.Errorf("can't merge label %d while it is currently involved in a merge", merged)
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

	// Remove the merge from the mapping.
	mc.Remove(iv, op)

	// If the instance version's dirty cache is empty, we can delete the merge cache.
	if labelsMerging.Empty(iv) {
		dvid.Debugf("Merge cache now empty for %s\n", iv)
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
	m map[dvid.InstanceVersion]*Mapping
}

// Add adds a merge operation to the given InstanceVersion's cache.
func (mc *mergeCache) Add(iv dvid.InstanceVersion, op MergeOp) error {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		mc.m = make(map[dvid.InstanceVersion]*Mapping)
	}
	mapping, found := mc.m[iv]
	if !found {
		mapping = &Mapping{
			f: make(map[uint64]uint64, len(op.Merged)),
			r: make(map[uint64]Set),
		}
		mc.m[iv] = mapping
	}
	for merged := range op.Merged {
		if err := mapping.set(merged, op.Target); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes a merge operation from the given InstanceVersion's cache,
// allowing us to return no mapping if it's already been processed.
func (mc *mergeCache) Remove(iv dvid.InstanceVersion, op MergeOp) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m == nil {
		dvid.Errorf("mergeCache.Remove() called with iv %s, op %v there are no mappings for any iv.\n", iv, op)
		return
	}
	mapping, found := mc.m[iv]
	if !found {
		dvid.Errorf("mergeCache.Remove() called with iv %s, op %v and there is no mapping for that iv.\n", iv, op)
		return
	}
	for merged := range op.Merged {
		mapping.delete(merged)
	}
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
		return mapping
	}
	return nil
}

// MergingToOther returns true if the label is currently being merged into another label.
func (mc *mergeCache) MergingToOther(iv dvid.InstanceVersion, label uint64) bool {
	mc.RLock()
	defer mc.RUnlock()

	if mc.m == nil {
		return false
	}
	mapping, found := mc.m[iv]
	if found {
		_, merging := mapping.f[label]
		return merging
	}
	return false
}

// DeleteMap removes a mapping of the given InstanceVersion.
func (mc *mergeCache) DeleteMap(iv dvid.InstanceVersion) {
	mc.Lock()
	defer mc.Unlock()

	if mc.m != nil {
		delete(mc.m, iv)
	}
}

// SVSplit provides labels after a supervoxel split
type SVSplit struct {
	Split  uint64 // label corresponding to split sparse volume
	Remain uint64 // relabeling of supervoxel that remains after split
}

// SVSplitCount provides both labels and the # voxels after a supervoxel split.
type SVSplitCount struct {
	SVSplit
	Voxels uint32 // number of voxels split
}

// SVSplitMap is a thread-safe mapping of supervoxels labels to their new split labels.
type SVSplitMap struct {
	sync.RWMutex
	Splits map[uint64]SVSplit
}

// returns a new mapped label or the previously mapped one.
func (m *SVSplitMap) getMapping(label uint64, newLabelFunc func() (uint64, error)) (relabel SVSplit, found bool, err error) {
	m.Lock()
	defer m.Unlock()
	if m.Splits == nil {
		m.Splits = make(map[uint64]SVSplit)
	} else {
		relabel, found = m.Splits[label]
	}
	if !found {
		if relabel.Split, err = newLabelFunc(); err != nil {
			return
		}
		if relabel.Remain, err = newLabelFunc(); err != nil {
			return
		}
		m.Splits[label] = relabel
	}
	return
}

// Mapping is a thread-safe, mapping of labels to labels in both forward and backward direction.
// Mutation of a Mapping instance can only be done through labels.MergeCache.
type Mapping struct {
	sync.RWMutex
	f map[uint64]uint64
	r map[uint64]Set
}

// ConstituentLabels returns a set of labels that will be mapped to the given label.
// The set will always include the given label.
func (m *Mapping) ConstituentLabels(final uint64) Set {
	m.RLock()
	defer m.RUnlock()

	if m.r == nil {
		return Set{final: struct{}{}}
	}

	// We need to return all labels that will eventually have the given final label
	// including any intermediate ones that were subsequently merged.
	constituents := Set{}
	toCheck := []uint64{final}
	for {
		endI := len(toCheck) - 1
		label := toCheck[endI]
		toCheck = toCheck[:endI]

		constituents[label] = struct{}{}

		s, found := m.r[label]
		if found {
			// push these labels onto stack
			for c := range s {
				toCheck = append(toCheck, c)
			}
		}
		if len(toCheck) == 0 {
			break
		}
	}
	return constituents
}

// FinalLabel follows mappings from a start label until
// a final mapped label is reached.
func (m *Mapping) FinalLabel(start uint64) (uint64, bool) {
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
func (m *Mapping) Get(label uint64) (uint64, bool) {
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
	s, found := m.r[b]
	if found {
		s[a] = struct{}{}
		m.r[b] = s
	} else {
		m.r[b] = Set{a: struct{}{}}
	}
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

// Merge returns a set made of the given labels.
func NewSet(lbls ...uint64) Set {
	s := make(Set, len(lbls))
	for _, label := range lbls {
		s[label] = struct{}{}
	}
	return s
}

// Merge adds the elements in the given set to the receiver.
func (s Set) Merge(s2 Set) {
	for label := range s2 {
		s[label] = struct{}{}
	}
}

// Exists returns true if the given uint64 is present in the Set.
func (s Set) Exists(i uint64) bool {
	if s == nil {
		return false
	}
	_, found := s[i]
	return found
}

// Copy returns a duplicate of the Set.
func (s Set) Copy() Set {
	dup := make(Set, len(s))
	for k := range s {
		dup[k] = struct{}{}
	}
	return dup
}

func (s Set) String() string {
	var str string
	i := 1
	for k := range s {
		str += fmt.Sprintf("%d", k)
		if i < len(s) {
			str += ", "
		}
		i++
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
		dvid.Errorf("decremented non-existent count for label %d, version %v\n", label, iv)
		return
	}
	cnts.Decr(label)
}
