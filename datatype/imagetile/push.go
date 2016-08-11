package imagetile

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// PushData does an imagetile-specific push using optional ROI and tile filters.
func (d *Data) PushData(p *datastore.PushSession) error {
	return datastore.PushData(d, p)
}

// --- dvid.Filterer implementatino -----

// NewFilter returns a Filter for use with a push of key-value pairs.
func (d *Data) NewFilter(fs storage.FilterSpec) (storage.Filter, error) {
	filter := &Filter{Data: d, fs: fs}

	// if there's no filter, just use base Data send.
	roidata, roiV, roiFound, err := roi.DataByFilter(fs)
	if err != nil {
		return nil, fmt.Errorf("No filter found that was parsable (%s): %v\n", fs, err)
	}
	filter.roi = roidata
	tilespec, tilespecFound := fs.GetFilterSpec("tile")

	if (!roiFound || roidata == nil) && !tilespecFound {
		dvid.Debugf("No ROI or tile filter found for imagetile push, so using generic data push.\n")
		return nil, nil
	}
	if tilespecFound {
		filter.planes = strings.Split(tilespec, ",")
	}

	// Get the spans once from datastore.
	filter.spans, err = roidata.GetSpans(roiV)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

// --- dvid.Filter implementation ----

type Filter struct {
	*Data
	fs     storage.FilterSpec
	roi    *roi.Data
	spans  dvid.Spans
	planes []string
}

func (f *Filter) Check(tkv *storage.TKeyValue) (skip bool, err error) {
	tileCoord, plane, scale, err := DecodeTKey(tkv.K)
	if err != nil {
		return true, fmt.Errorf("key (%v) cannot be decoded as tile: %v", tkv.K, err)
	}
	if len(f.planes) != 0 {
		var allowed bool
		for _, allowedPlane := range f.planes {
			if allowedPlane == "xy" && plane.Equals(dvid.XY) {
				allowed = true
				break
			}
			if allowedPlane == "xz" && plane.Equals(dvid.XZ) {
				allowed = true
				break
			}
			if allowedPlane == "yz" && plane.Equals(dvid.YZ) {
				allowed = true
				break
			}
		}
		if !allowed {
			return true, nil
		}
	}
	extents, err := f.computeVoxelBounds(tileCoord, plane, scale)
	if err != nil {
		return true, fmt.Errorf("Error computing voxel bounds of tile: %v\n", err)
	}
	if inside, err := roi.VoxelBoundsInside(extents, f.roi.BlockSize, f.spans); err != nil {
		return true, fmt.Errorf("can't determine intersection of tile and roi: %v\n", err)
	} else if !inside {
		return true, nil
	}
	return false, nil
}
