/*

Package datatype handles registration and processing of arbitrary data types
with the DVID datastore.

Data types are declared in the main dvid.go file under imports.  An example:

	import (
		// Declare the data types this DVID executable will support
		_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
		_ "github.com/janelia-flyem/dvid/datatype/label64"
		_ "randomsite.org/datatypes/myfoo"
	)	

*/
package datatype
