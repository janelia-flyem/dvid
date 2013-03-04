/* 
DVID is a distributed, versioned image datastore that uses leveldb for data storage and 
a Go language layer that provides http and command-line access.

	NOTE: This system is still in development and not ready for use.

Documentation can be found nicely formatted at http://godoc.org/github.com/janelia-flyem/dvid

Philosophy

DVID is an image datastore that gains much of its power by coupling a lightweight,
embedded storage (via Google's leveldb project) to distributed versioning, 
allowing you to process subvolumes of data and push resulting changes to a
larger DVID volume.  

User-defined data types can be added to DVID.  Standard data types (8-bit grayscale 
images, 64-bit label data, and label-to-label maps) will be included with the base 
DVID installation, although their packaging is identical to any other user-defined 
data type.

Although DVID operations can be run from the command line by knowledgeable users,
we assume that clients will build upon DVID layers to hide the complexity from
users.

Standard DVID commands that can be performed without a running server

In the following documentation, the type of brackets designate 
<required parameter> and [optional parameter].

	dvid version

Prints the version number of the DVID datastore software and the version of each
supported data type.

	dvid init [config=/path/to/json/config] [dir=/path/to/datastore/dir]

Initialize a datastore (in current or optionally specified directory) using 
arguments in <JSON config file>.  If the optional JSON config file is not provided,
DVID will prompt the user for configuration data.  Configuration includes volume extents, 
resolution, resolution unit, and the supported data types.  Returns a UUID representing 
the intial volume version,  i.e., an unlocked root node in the version 
directed acyclic graph (DAG).

	dvid serve [dir=/path/to/datastore] [web=localhost:4000] [rpc=localhost:6000]

Starts a DVID server that maintains exclusive control over the datastore.
Creates both web and rpc servers that can accept connections from web browsers
and independent DVID commands as below.


Standard DVID commands that require connection to a running server

The following commands assume there is a running DVID rpc server at 
localhost:6000 or an optionally specified url/port.  Each command also
accepts a "uuid=..." option, and if there's any possibility of
another user interacting with the same DVID datastore, you *should* specify 
a UUID.

	dvid types [rpc=localhost:6000]

The 'types' command lists all supported data types for this DVID datastore.
You can also use the "--types" option to list all data types that have been
compiled into your current DVID executable.

	dvid branch [uuid=...] [rpc=localhost:6000]

Create a child of the current HEAD node or the node specified by the optional
UUID.  This automatically creates an unlocked node that you can use to add 
data and sets the current datastore HEAD to this unlocked node.  Returns the 
UUID of the child node.  Note: This command will fail if you attempt to 
create a child off an unlocked node.  You must "lock" a node before using
the "child" command.

	dvid lock [uuid=...] [rpc=localhost:6000]

Locks the current HEAD node or the node specified by an optional UUID.  Once
a node is locked, it can be used with the "child" command.

	dvid pull  (TODO)
	dvid push  (TODO)


DVID commands available through supported data types

You can specify a data type name followed by a type-specific command:

	dvid <data type> <type command> <arguments> [uuid=...] [rpc=localhost:6000]

For example, the "grayscale8" data type supports the "server_load" command to insert
2d images to the DVID datastore on the server side:

	dvid grayscale8 server-add  1,2,23  /path/to/images/*.png

The above example inserts a series of XY images where the lexicographically 
smallest filename holds voxels at (x+1, y+2, 23) and each following image increases
the z-coordinate by 1.  The image data is inserted into the current HEAD node
or the node specified via the "uuid=..." option.  Attempts to add data to a
locked node will result in an error.

	dvid <datatype> help [rpc=localhost:6000]

Returns a help message for all commands specific to this data type.

Dependencies

DVID uses a key-value datastore as its back end.  We use one of two implementations of 
leveldb initially:

  • C++ leveldb plus levigo Go bindings.  This is a more industrial-strength solution and 
    would be the preferred installation for production runs at Janelia.
  • Pure Go leveldb implementations.
	  • Suryandaru Triandana's rewrite from C++ version (https://github.com/syndtr/goleveldb)
	  • Nigel Tao's implementation still in progress (http://code.google.com/p/leveldb-go)

A pure Go leveldb implementation would greatly simplify cross-platform development since 
Go code can be cross-compiled to Linux, Windows, and Mac targets.  It would also allow simpler 
inspection of issues.  However, there is no substitute for having large numbers of users 
testing the product, so the C++ leveldb implementation will be tough to beat in terms of 
uptime and performance.   We might also look at sqlite4's LSM system as a third key-value
datastore.
*/
package main
