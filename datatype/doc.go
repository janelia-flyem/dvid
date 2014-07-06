/*
	Package datatype provides interfaces for arbitrary datatypes supported in DVID.
	Each datatype implements the interface and handles conversion of its data into
	the key/value pairs or other base storage elements.

	Data types are declared in the main dvid.go file under imports.  An example:

	import (
		// Declare the data types this DVID executable will support
		_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
		_ "github.com/janelia-flyem/dvid/datatype/label64"
		_ "randomsite.org/datatypes/myfoo"
	)
*/
package datatype
