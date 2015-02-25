/*
	Package datatype provides interfaces for arbitrary datatypes supported in DVID.
	Each datatype implements the interface and handles conversion of its data into
	the key-value pairs or other base storage elements.  Each data type provides
	a domain-specific HTTP API and possibly command-line actions.

	Data types are declared in the main dvid.go file under imports.  An example:

	import (
		// Declare the data types this DVID executable will support
		_ "github.com/janelia-flyem/dvid/datatype/imageblk"
		_ "github.com/janelia-flyem/dvid/datatype/labelblk"
		_ "randomsite.org/datatypes/myfoo"
	)

	Philosophy note: Rather than creating base packages that handle all sorts of
	similar data (e.g., uint8, uint16, rgab8, float32, n-dimensional float32),
	DVID code tries to be as simple as possible even if it means copying and replacing
	types.  This will lend itself to using Go generate versus generics-style of
	programming.  For now, though, it will simplify the code of each package.
*/
package datatype
