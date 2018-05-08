/*
	Package downres provides a system for computing multi-scale 3d arrays given mutations.
	Two workflows are provided: (1) mutation-based with on-the-fly downres operations activated
	by the end of a mutation, and (2) larger ingestions where it is not possible to
	retain all changed data in memory.  #2 is TODO.
*/
package downres

import (
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

type BlockMap map[dvid.IZYXString]interface{}

type Updater interface {
	StartScaleUpdate(scale uint8)
	StopScaleUpdate(scale uint8)
	ScaleUpdating(scale uint8) bool
	AnyScaleUpdating() bool
}

type Downreser interface {
	Updater

	DataName() dvid.InstanceName
	GetMaxDownresLevel() uint8

	// StoreDownres computes and stores the down-res for the given blocks, returning
	// the computed down-res blocks at 1/2 resolution.
	StoreDownres(v dvid.VersionID, hiresScale uint8, hires BlockMap) (BlockMap, error)
}

// BlockOnUpdating blocks until the given data is not updating from any normal updates or
// also downres operations.  Primarily used during testing.
func BlockOnUpdating(uuid dvid.UUID, name dvid.InstanceName) error {
	time.Sleep(100 * time.Millisecond)
	if err := datastore.BlockOnUpdating(uuid, name); err != nil {
		return err
	}
	d, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return err
	}

	updater, isUpdater := d.(Updater)
	for isUpdater && updater.AnyScaleUpdating() {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// Mutation is a stash of changes that will be handled in down-resolution scaling.
type Mutation struct {
	d          Downreser
	v          dvid.VersionID
	mutID      uint64
	hiresCache BlockMap

	sync.RWMutex
}

// NewMutation returns a new Mutation for stashing changes.
func NewMutation(d Downreser, v dvid.VersionID, mutID uint64) *Mutation {
	for scale := uint8(1); scale <= d.GetMaxDownresLevel(); scale++ {
		d.StartScaleUpdate(scale)
	}
	m := Mutation{
		d:          d,
		v:          v,
		mutID:      mutID,
		hiresCache: make(BlockMap),
	}
	return &m
}

// MutationID returns the mutation ID associated with this mutation.
func (m *Mutation) MutationID() uint64 {
	if m == nil {
		return 0
	}
	return m.mutID
}

// BlockMutated caches mutations at the highest resolution (scale 0)
func (m *Mutation) BlockMutated(bcoord dvid.IZYXString, block interface{}) error {
	if m.hiresCache == nil {
		return fmt.Errorf("bad attempt to mutate block %s when mutation already closed", bcoord)
	}
	m.Lock()
	m.hiresCache[bcoord] = block
	m.Unlock()
	return nil
}

// Done asynchronously computes lower-res scales for all altered blocks in a mutation.
func (m *Mutation) Done() {
	go func() {
		m.Lock()
		bm := m.hiresCache
		var err error
		for scale := uint8(0); scale < m.d.GetMaxDownresLevel(); scale++ {
			bm, err = m.d.StoreDownres(m.v, scale, bm)
			if err != nil {
				dvid.Errorf("Mutation %d for data %q: %v\n", m.mutID, m.d.DataName(), err)
				break
			}
			dvid.Infof("Finished down-resolution processing for data %q at scale %d.\n", m.d.DataName(), scale+1)
			m.d.StopScaleUpdate(scale + 1)
		}
		m.hiresCache = nil
		m.Unlock()
	}()
}
