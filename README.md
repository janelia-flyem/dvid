DVID
====

*Status: In development, not ready for use.*

[![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest)

DVID is a *distributed, versioned, image-oriented datastore* written in Go that supports different
storage backends, a Level 2 REST HTTP API, and command-line access.

Documentation is [available here](http://godoc.org/github.com/janelia-flyem/dvid).

Command-line and REST API documentation is currently distributed over data types and can be 
found in help constants:

[general commands and HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/server#pkg-constants)

[voxels HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/datatype/voxels#pkg-constants)
[multichan16 HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/datatype/multichan16#pkg-constants)

## Philosophy

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


## Build Process

DVID uses the [buildem system](http://github.com/janelia-flyem/buildem#readme) to automatically 
download and build the specified storage engine (e.g., leveldb), Go language support, and all 
required Go packages.

You may choose a storage engine via the CMake variables DVID_BACKEND and DVID_BACKEND_DEPEND in 
the CMakeLists.txt file.

To build DVID using buildem, do the following steps:

    % cd /path/to/dvid/dir
    % mkdir build
    % cd build
    % cmake -D BUILDEM_DIR=/path/to/buildem/dir ..

If you haven't built with that buildem directory before, do the additional steps:

    % make
    % cmake -D BUILDEM_DIR=/path/to/buildem/dir ..

To build DVID, assuming you are still in the CMake build directory from above:

    % make dvid

This will install a DVID executable 'dvid' in the buildem bin directory.

To build DVID executable without built-in web client:

    % make dvid-exe

Tests are run with gocheck:

    % make test

