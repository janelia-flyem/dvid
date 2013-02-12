/* 
DVID is a distributed, versioned image datastore that uses leveldb for data storage and 
a Go language layer that provides http and command-line access.

Documentation can be found nicely formatted at http://godoc.org/github.com/janelia-flyem/dvid

Philosophy

DVID is a lightweight image datastore that gains much of its power by allowing distributed
versioning, allowing you to process subvolumes of data and push resulting changes to a
larger DVID volume.  

User-defined data types can be added to DVID.  Standard data types (8-bit grayscale images,
64-bit label data, and label-to-label maps) will be included with the base DVID installation,
although their packaging is identical to any other user-defined data type.

Dependencies

DVID uses a key-value datastore as its back end.  We use one of two implementations of 
leveldb initially:

  • C++ leveldb plus levigo Go bindings.  This is a more industrial-strength solution and 
    would be the preferred installation for production runs at Janelia.
  • Pure Go leveldb implementations.
	  • syndtr's rewrite from C++ version (https://github.com/syndtr/goleveldb)
	  • nigel tao's implementation still in progress (http://code.google.com/p/leveldb-go)

A pure Go leveldb implementation would greatly simplify cross-platform development since 
Go code can be cross-compiled to Linux, Windows, and Mac targets.  It would also allow simpler 
inspection of issues.  However, there is no substitute for having large numbers of users 
testing the product, so the C++ leveldb implementation will be tough to beat in terms of 
uptime and performance.
*/
package main
