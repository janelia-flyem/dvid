package datatype

import (
	"log"
)

// FormatID uniquely identifies a DVID-supported data type
type FormatId struct {
	// Name describes a data type & may not be as unique (e.g., "grayscale")
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url string
}

// A format holds essential information to identify, decode, and encode
// data to the DVID datastore layer.
type Format struct {
	FormatId

	// WithinBlock is true if the data should be next to other data types within
	// a block.  If true, it's assumed that we usually want to retrieve this data 
	// type with other block-level data.  If false, we are optimizing for
	// retrieving this data type independently, e.g., all the label->label maps
	// across blocks to make a subvolume map on the fly.
	WithinBlock bool
}

// Supported is the set of registered formats
var Supported map[FormatId]Format

// RegisterFormat registers a data type for DVID use.
func RegisterFormat(name, url string, withinBlock bool) {
	log.Printf("Registering data type format: %s (%s)\n",
		name, url)
	if Supported == nil {
		Supported = make(map[FormatId]Format)
	}
	id := FormatId{name, url}
	format := Format{id, withinBlock}
	Supported[id] = format
}
