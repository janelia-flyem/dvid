/*
Package datastore provides a DVID-specific API that supports storage and
retrieval of supported data types, versioning, and maintenance of
volume configuration data like volume extents and resolution.

Data types are declared in the main dvid.go file under imports.  An example:

	import (
		// Declare the data types this DVID executable will support
		_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
		_ "github.com/janelia-flyem/dvid/datatype/label64"
		_ "randomsite.org/datatypes/myfoo"
	)	

*/
package datastore
