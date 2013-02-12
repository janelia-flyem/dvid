/*
Package cli supports command line level interaction with DVID.

In the following documentation, the type of brackets designate 
<required parameter> and [optional parameter].  All commands accept
a "--path" option that sets the datastore directory, otherwise the
datastore directory is assumed to be the current directory.

Initializing a DVID Datastore

	init <config file>

Initialize a datastore in current or optionally specified directory using 
parameters in <config file>.  The config file specifies volume extents, 
resolution, and the supported data types.  Returns a UUID representing 
the intial volume version,  i.e., an unlocked root node in the version 
directed acyclic graph (DAG).

Commands on Established DVID Datastore

The following commands assume there is a running DVID server at 
localhost:4000 or an optionally specified url/port.  

	child [UUID]

Create a child off the current HEAD node or the node specified by an optional
UUID.  This automatically creates an unlocked node that you can use to add 
data and sets the current datastore HEAD to this unlocked node.  Returns the 
UUID of the child node.  Note: This command will fail if you attempt to 
create a child off an unlocked node.  You must "lock" a node before using
the "child" command.

	add <datatype name> <filenames glob> [uuid=UUID] [params]

	Example adding series of XY images where lexicographically smallest
	filename holds voxels at (x, y, 10) and each filename increases the
	z-coordinate by 1:

	add grayscale8 /path/to/images/*.png z=10

Add data specified by the filenames glob (e.g., "*.png") into the current HEAD
node or the node specified by an optional UUID.  The <datatype name> should
correspond with one of the data types specified in the configuration file
supplied to this datastore's "init" command.  The optional [params] field
can specify location of the data within the volume, e.g. "z=23".  The format
of the [params] field depends on the datatype.  Attempts to add data to a
locked node will result in an error.

	lock [UUID]

Locks the current HEAD node or the node specified by an optional UUID.  Once
a node is locked, it can be used with the "child" command.

	help add <datatype name>

Returns a help message for adding data for this particular datatype.
*/
package cli
