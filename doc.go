/*
DVID is a distributed, versioned image datastore written in Go that supports different
storage backends, a Level 2 REST HTTP API, and command-line access.

	NOTE: This system is still in development and not ready for use.
	Significant changes are still being made to interfaces.

Documentation can be found nicely formatted at http://godoc.org/github.com/janelia-flyem/dvid

Philosophy

DVID is an image datastore that gains much of its power by coupling an abstracted
database backend (typically lightweight, embedded storage like Google's leveldb project)
to distributed versioning, allowing you to process subvolumes of data and push
resulting changes to a larger DVID volume.  Because a DVID instance is lightweight,
DVID can scale out by deploying large numbers of DVID instances, each handling a subvolume.
DVID can also scale up by swapping its default embedded key/value datastore with a
cluster-ready system like Cassandra.

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

	dvid about

Prints the version number of the DVID datastore software and the version of each
supported data type.

	dvid [-datastore=/path/to/db] init

Initializes a datastore in the current or optionally specified directory. Returns a UUID
representing the intial volume version,  i.e., an unlocked root node in the version
directed acyclic graph (DAG).

	dvid [-datastore=/path/to/db] [-webclient=/path/to/webclient] serve

Starts a DVID server that maintains exclusive control over the datastore.
Creates both web and rpc servers that can accept connections from web browsers
and independent DVID commands as below.


Standard DVID commands that require connection to a running server

The following commands assume there is a running DVID rpc server at
localhost:8000.  Multiple DVID instances can be run on a single server by
specifying different url and rpc addresses via the -http and -rpc options.

    dvid log

Prints information on the provenance of the image volume, particularly the UUIDs for
each version.

	dvid types

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

	dvid branch <uuid>

Create a child of the current HEAD node or the node specified by the optional
UUID.  This automatically creates an unlocked node that you can use to add
data and sets the current datastore HEAD to this unlocked node.  Returns the
UUID of the child node.  Note: This command will fail if you attempt to
create a child off an unlocked node.  You must "lock" a node before using
the "child" command.

	dvid lock <uuid>

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

	dvid <data set name> <type command> <arguments>

For example, the "grayscale8" data type supports the "server-add" command to insert
2d images to the DVID datastore on the server side.  Assuming a dataset called
"mygrayscale" with grayscale8 data type has been added to a DVID instance, we
could do the following:

	dvid mygrayscale server-add 3f8 1,2,23 /path/to/images/*.png

The above example inserts a series of XY images where the lexicographically
smallest filename holds voxels offset from (x+1, y+2, 23) and each following image
increases the z-coordinate by 1.  The image data is inserted into the specified version
node with UUID beginning with "3f8".

	dvid <data set name> help

Returns a help message for all commands specific to this data set's type.

Dependencies

DVID uses a key-value datastore as its back end.  We currently use Google's C++ leveldb
plus the levigo Go bindings.  We can easily add pure Go leveldb implementations and
initially attempted use of Suryandaru Triandana's rewrite from C++ version
(https://github.com/syndtr/goleveldb).  We are keeping tabs on Nigel Tao's implementation
still in progress (http://code.google.com/p/leveldb-go).

DVID tries to be agnostic about its backend database, and we will be experimenting with
a number of other databases like couchbase and lightning MDB.
*/
package main
