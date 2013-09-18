/*
DVID is a distributed, versioned, image-oriented datastore written in Go that supports
different storage backends, a Level 2 REST HTTP API, and command-line access.

	NOTE: This system is still in development and not ready for use.
	Significant changes are still being made to interfaces.

Documentation can be found nicely formatted at http://godoc.org/github.com/janelia-flyem/dvid

Philosophy

DVID gains much of its power by assuming a key-value persistence layer, allowing pluggable
storage engines, and creating conventions of how to organize key space for arbitrary
type-specific indexing and distributed versioning.

User-defined data types can be added to DVID.  Standard data types (8-bit grayscale
images, 64-bit label data, and label-to-label maps) will be included with the base
DVID installation, although their packaging is identical to any other user-defined
data type.

DVID operations can be run from the command line by knowledgeable users, but
we assume that clients will build upon DVID layers to hide the complexity from
users.  DVID currently uses a Level 2 REST HTTP API and plans to incorporate
Apache Thrift APIs.

Standard DVID commands that can be performed without a running server

In the following documentation, the type of brackets designate
<required parameter> and [optional parameter].

	dvid help

Prints serverless commands and server commands if a DVID server is available.

	dvid about

Prints the version number of the DVID datastore software and the version of each
supported data type.

	dvid [-datastore=/path/to/db] init

Initializes a datastore in the current or optionally specified directory.

	dvid [-datastore=/path/to/db] [-webclient=/path/to/webclient] serve

Starts a DVID server that maintains exclusive control over the datastore.
Creates both web and rpc servers that can accept connections from web browsers
and independent DVID commands as below.

Please use the "dvid help" command to determine commands available for the particular
data types of your DVID installation.

Build Process

DVID uses the buildem system to automatically download and build all dependencies.
Please see the README.md file for more information.

Dependencies

DVID provides a storage API that can use a variety of storage engines as backends, as long
as they can model a key-value store.  The first and simplest storage engine was Google's
embedded C++ leveldb plus the levigo Go bindings.  Drivers for different storage engines
can be found in the storage package.  A particular storage engine is selected by modifying
the CMake variables DVID_BACKEND and DVID_BACKEND_DEPEND in the CMakeLists.txt file.

DVID can use Snappy compression at a data type granularity.  For example, the voxels datatype
and its derivatives use Snappy compression by default.
*/
package main
