DVID: Distributed, Versioned Image Datastore
====

*Status: In development, not ready for use.*

[![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest)

DVID is a distributed, versioned image datastore written in Go that supports different
storage backends (e.g., leveldb).

Documentation is [available here](http://godoc.org/github.com/janelia-flyem/dvid).

## Build Process

DVID uses the [buildem system](http://github.com/janelia-flyem/buildem#readme) to 
automatically download and build leveldb, Go language support, and all required Go packages.  

To build DVID using buildem, do the following steps:

    % cd /path/to/dvid/dir
    % mkdir build
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

