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

	_, v, found, err := roi.ParseFilterSpec(fs)
	if err != nil {
		return nil, fmt.Errorf("can't parse filter spec (%s): %v\n", fs, err)
	}
	if !found {
		dvid.Infof("no ROI filtering found for labelvol %q.\n", d.DataName())
		return nil, nil
	}

	// Get associated labelblk.  If none, we can't use roi filter so just do standard data send.
	lblk, err := d.GetSyncedLabelblk(v)
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
		return true, fmt.Errorf("unable to convert labelvol block %s into IndexZYX", block.Print())
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

func (f Filter) EndInfof(kvSent, kvTotal int) {
	dvid.Infof("Sent %d %s labelvol blocks (out of %d total) with filter %q\n",
		kvSent, f.DataName(), kvTotal, f.fs)
}
