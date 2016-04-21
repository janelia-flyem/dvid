package storage

import (
	"strings"
)

// FilterSpec is a string specification of type-specific filters to apply to key-value pairs
// before sending them to a remote DVID.  For example, a FilterSpec could look like:
//
//    roi:seven_column,3f8a/tile:xy,xz
//
// The above specifies two filters joined by a forward slash.  The first is an "roi" filters
// that lists a ROI data instance name ("seven_column") and its version as a partial, unique
// UUID.  The second is a "tile" filter that specifies two types of tile plane: xy and xz.
type FilterSpec string

// GetFilterSpec parses a FilterSpec and returns the filter spec of given type.
// If no filter spec of ftype is available, the second argument is false.
func (f FilterSpec) GetFilterSpec(ftype string) (value string, found bool) {
	// Separate filters.
	fs := strings.Split(string(f), "/")
	if len(fs) == 0 {
		return
	}

	// Scan each fspec to see if its the given ftype.
	for _, fspec := range fs {
		parts := strings.Split(fspec, ":")
		if parts[0] == ftype {
			if len(parts) != 2 {
				return
			}
			return parts[1], true
		}
	}
	return
}

// Filterer is an interface that can provide a send filter given a spec.
// Datatypes can fulfill this interface if they want to filter key-values
// sent to peer DVID servers.  An example is the use of ROIs to filter
// kv pairs based on spatial constraints.
//
// See datatype/imageblk/distributed.go
type Filterer interface {
	NewFilter(FilterSpec) (Filter, error)
}

// Filter can filter key-value pairs based on some criteria.
type Filter interface {
	// Check filters type-specific key-value pairs.
	Check(tkv *TKeyValue) (skip bool, err error)

	// EndInfof logs the end of a data instance push using supplied kv pairs sent vs total.
	EndInfof(kvSent, kvTotal int)
}
