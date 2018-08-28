package imageblk

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// PushData does an imageblk-specific push using optional ROI filters.
func (d *Data) PushData(p *datastore.PushSession) error {
	return datastore.PushData(d, p)
}

// --- dvid.Filterer implementation -----

// NewFilter returns a Filter for use with a push of key-value pairs.
func (d *Data) NewFilter(fs storage.FilterSpec) (storage.Filter, error) {
	roiIterator, _, found, err := roi.NewIteratorBySpec(fs, d)
	if err != nil {
		dvid.Debugf("No filter found that was parsable: %s\n", fs)
		return nil, err
	}
	if !found || roiIterator == nil {
		dvid.Debugf("No ROI found so using generic data push for data %q.\n", d.DataName())
		return nil, nil
	}
	return &Filter{d, fs, roiIterator}, nil
}

// --- dvid.Filter implementation ----

type Filter struct {
	*Data
	fs storage.FilterSpec
	it *roi.Iterator
}

func (f Filter) Check(tkv *storage.TKeyValue) (skip bool, err error) {
	indexZYX, err := DecodeTKey(tkv.K)
	if err != nil {
		return true, fmt.Errorf("key (%v) cannot be decoded as block coord: %v", tkv.K, err)
	}
	if !f.it.InsideFast(*indexZYX) {
		return true, nil
	}
	return false, nil
}
