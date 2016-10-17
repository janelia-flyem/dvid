package labelvol

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// PushData does a labelvol-specific push using optional ROI filters.
func (d *Data) PushData(p *datastore.PushSession) error {
	return datastore.PushData(d, p)
}

// --- dvid.Filterer implementatino -----

// NewFilter returns a Filter for use with a push of key-value pairs.
func (d *Data) NewFilter(fs storage.FilterSpec) (storage.Filter, error) {
	filter := &Filter{Data: d, fs: fs}

	// Get associated labelblk.  If none, we can't use roi filter so just do standard data send.
	lblk, err := d.GetSyncedLabelblk()
	if err != nil {
		dvid.Infof("Unable to get synced labelblk for labelvol %q.  Unable to do any ROI-based filtering.\n", d.DataName())
		return nil, nil
	}

	roiIterator, _, found, err := roi.NewIteratorBySpec(fs, lblk)
	if err != nil {
		return nil, err
	}
	if !found || roiIterator == nil {
		dvid.Debugf("No ROI found so using generic data push for data %q.\n", d.DataName())
		return nil, nil
	}
	filter.it = roiIterator

	return filter, nil
}

// --- dvid.Filter implementation ----

type Filter struct {
	*Data
	fs       storage.FilterSpec
	it       *roi.Iterator
	curLabel uint64
}

func (f *Filter) Check(tkv *storage.TKeyValue) (skip bool, err error) {
	if f.Data == nil {
		return false, fmt.Errorf("bad filter %q: no data", f.fs)
	}
	if f.it == nil {
		return true, nil
	}

	// Only filter label block RLE kv pairs.
	class, err := tkv.K.Class()
	if err != nil {
		return true, err
	}
	if class != keyLabelBlockRLE {
		return false, nil
	}

	// Check the label block against any ROI.
	label, block, err := DecodeTKey(tkv.K)
	if err != nil {
		return true, fmt.Errorf("key (%v) cannot be decoded as labelvol key: %v", tkv.K, err)
	}
	indexZYX, err := block.IndexZYX()
	if err != nil {
		return true, fmt.Errorf("unable to convert labelvol block %s into IndexZYX", block)
	}
	if label != f.curLabel {
		f.it.Reset()
		f.curLabel = label
	}
	if !f.it.InsideFast(indexZYX) {
		return true, nil
	}
	return false, nil
}
