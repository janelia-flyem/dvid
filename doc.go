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

DVID operations can be run from the command line by knowledgeable users, but
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

    dvid log [rpc=localhost:6000]

Prints information on the provenance of the image volume, particularly the UUIDs for
each version.

	dvid types [rpc=localhost:6000]

The 'types' command lists all supported data types for this DVID datastore.
You can also use the "--types" option to list all data types that have been
compiled into your current DVID executable.

    dvid dataset <dataset name> <data type name>

Creates a new dataset for thie DVID datastore, e.g., "dvid dataset mygrayscale grayscale8". 
You must have datasets and their type defined before you can add or retrieve data.


Commands on roadmap but not yet implemented


    dvid comment <uuid> "..."

Appends a note to the provenance field of a version node.  These comments can be
reviewed using the "log" command.

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

	dvid pull <remote dvid address> <uuid> [extents]
	dvid push <remote dvid address> <uuid> [extents]

The push and pull commands allow syncing of two DVID instances with respect to
a version node.

	dvid archive <uuid>

Compacts a version, removing all denormalized data and making sure normalized
data uses deltas from parents if possible.

DVID commands available through supported data types

You can specify a data set name followed by a type-specific command:

	dvid <data set name> <type command> <arguments> [uuid=...] [rpc=localhost:6000]

For example, the "grayscale8" data type supports the "server-add" command to insert
2d images to the DVID datastore on the server side.  Assuming a dataset called
"mygrayscale" with grayscale8 data type has been added to a DVID instance, we 
could do the following:

	dvid mygrayscale server-add 3f80a 1,2,23 /path/to/images/*.png

The above example inserts a series of XY images where the lexicographically 
smallest filename holds voxels offset from (x+1, y+2, 23) and each following image 
increases the z-coordinate by 1.  The image data is inserted into the specified version
node with UUID beginning with "3f80a".

	dvid <data set name> help [rpc=localhost:6000]

Returns a help message for all commands specific to this data set's type.

Dependencies

DVID uses a key-value datastore as its back end.  We currently use Google's C++ leveldb 
plus the levigo Go bindings.  We can easily add pure Go leveldb implementations and
initially attempted use of Suryandaru Triandana's rewrite from C++ version 
(https://github.com/syndtr/goleveldb).  We are keeping tabs on Nigel Tao's implementation 
still in progress (http://code.google.com/p/leveldb-go).

A pure Go leveldb implementation would greatly simplify cross-platform development since 
Go code can be cross-compiled to Linux, Windows, and Mac targets.  It would also allow simpler 
inspection of issues.  However, there is no substitute for having large numbers of users 
testing the product, so the C++ leveldb implementation will be tough to beat in terms of 
uptime and performance.   We might also look at sqlite4's LSM system as a third key-value
datastore.
*/
package main
