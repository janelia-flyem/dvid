package datatype

import (
	"log"
)

// Format uniquely identifies a DVID-supported data type and how it is arranged
// within a DVID datastore instance.
type Format struct {
	// Name describes a data type & may not be as unique (e.g., "grayscale")
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url string

	// IsolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// Supported is the set of registered formats
var Supported map[string]Format

// RegisterFormat registers a data type for DVID use.
func RegisterFormat(name, url string, isolate bool) {
	log.Printf("Registering data type format: %s (%s)", name, url)
	if Supported == nil {
		Supported = make(map[string]Format)
	}
	Supported[url] = Format{name, url, isolate}
}
