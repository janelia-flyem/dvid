DVID
====

*Status: In development, not ready for use.*

[![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest)

DVID is a *distributed, versioned, image-oriented datastore* written in Go that supports different
storage backends, a Level 2 REST HTTP API, and command-line access.  It has been tested on both
MacOS X and Linux (Fedora 16, CentOS 6) but not on Windows.

Documentation is [available here](http://godoc.org/github.com/janelia-flyem/dvid).

Command-line and REST API documentation is currently distributed over data types and can be 
found in help constants:

* [general commands and HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/server#pkg-constants)
* [voxels HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/datatype/voxels#pkg-constants)
* [multichan16 HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/datatype/multichan16#pkg-constants)

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

First, make sure you have a proper GOPATH environment variable as described in the 
[Go environment page](http://golang.org/doc/code.html).  The GOPATH should point to the location 
of your Go workspace with bin/, pkg/, src/, etc.  If you've never worked with Go before, just
use an empty directory like so:

    % export GOPATH=/path/to/go/workspace
    % mkdir $GOPATH
    
The DVID repo should be cloned to $GOPATH/src/github.com/janelia-flyem/dvid or you can use
"go get http://github.com/janelia-flyem/dvid" if your GOPATH is set and you already use go.

You should also have a BUILDEM_DIR, either an empty directory or your previous buildem directory 
where you'll compile all the required software and eventually place the compiled dvid executable.
You'll want to set your environment variables like so:

    % export BUILDEM_DIR=/path/to/buildem/dir
    % export PATH=$BUILDEM_DIR/bin:$PATH
    % export LD_LIBRARY_PATH=$BUILDEM_DIR/lib:$LD_LIBRARY_PATH

In the above, we are saying to use the executables created in the $BUILDEM_DIR/bin directory first,
which should include the DVID executable, and also use buildem-created libraries.

To build DVID using buildem, do the following steps:

    % cd $GOPATH/src/github.com/janelia-flyem/dvid
    % mkdir build
    % cd build
    % cmake -D BUILDEM_DIR=/path/to/buildem/dir ..

If you haven't built with that buildem directory before, do the additional steps:

    % make
    % cmake -D BUILDEM_DIR=/path/to/buildem/dir ..

To build DVID without a built-in web client, assuming you are still in the CMake build directory from above:

    % make dvid-exe

This will install a DVID executable 'dvid' in the buildem bin directory.

Tests are run with gocheck:

    % make test

You could generate a dvid executable with a built-in web client, appended to the executable
binary and used by dvid's web server during operation:

    % make dvid

For development, I suggest just doing "make dvid-exe" and specifying your choice of web client 
using "-webclient":

    % dvid -webclient=/path/to/dvid-webclient -datastore=/path/to/datastore/dir serve
    
You can then modify the web client code and simply refresh the browser.

## Simple Example

In this example, we'll initialize a new datastore, create a grayscale data type and data,
load some images, and then use a web browser to see the results. 

### Getting help

If DVID was installed correctly, you should be able to ask dvid for help:

    % dvid help

### Create the datastore

Depending on the storage engine, DVID will store data into a directory.  We must initialize
the datastore by specifying a datastore directory.

    % dvid -datastore=/path/to/datastore/dir init

### Start the DVID server

The "-debug" option lets you see how DVID is processing requests.

    % dvid -datastore=/path/to/datastore/dir -debug serve

If dvid wasn't compiled with a built-in web client, you'll see some complaints and ways you can 
specify a web client.  For our purposes, though, we don't need the web console for this simple
example.  We will be accessing the standard HTTP API directly through a web browser.

Open another terminal and run "dvid help" again.  You'll see more information because dvid can
detect a running server and describe the data types available.  You can also ask for just the
data types supported by this DVID server:

    % dvid types

### Create a new dataset

One DVID server can manage many different datasets.   We create a dataset like so:

    % dvid datasets new

A hexadecimal string will be printed in response.  Datasets, like versions of data within a dataset, 
are identified by a global UUID, usually printed and read in hexadecimal format (e.g., "c78a0").
When supplying a UUID, you only need enough letters to uniquely identify the UUID within that
DVID server.  For example, if there are only two datasets, each with only one version, and the root
versions for each dataset are "c78a0..." and "da0a4...", respectively, then you can uniquely specify
the datasets using "c7" and "da".  (We might use one letter, but generally two or more letters 
are better.)

    % dvid types
    
Entering the above command will show the new dataset but there are no data under it.

### Create new data for a dataset

We can create an instance of a supported data type for the new dataset:

    % dvid dataset c7 new grayscale8 mygrayscale

Replace "c7" with the first two letters of whatever UUID was printed when you created a new dataset.
You can also see the UUIDs for datasets by using the "dvid types" command.

After adding new data, you can see the result via the "dvid types" command.  It will show new data
named "mygrayscale" that is of grayscale8 data type.  DVID allows a variety of data types, each
implemented by some code that determines that data type's commands, HTTP API, and method of storing
and retrieving data.  The data type implementation is uniquely identified by the URL of that
implementation's package or file.

### Get data type-specific help

Since each data type has its own set of commands and HTTP API, we need to know what's available
from each type:

    % dvid types grayscale8 help

The above asks the grayscale8 data type implementation for its usage.  In the next step, we will
use the "load local" command described in the help response.

### Loading some sample data

Download a [small stack of grayscale images from github](https://github.com/janelia-flyem/sample-grayscale).

You can either clone it using git or use the "Download ZIP" button on the right.  Once you've downloaded
that 250 x 250 x 250 grayscale volume, enter the following:

    % dvid node c7 mygrayscale load local xy 100,100,2600 /path/to/sample/*.png

Once again, replace the "c7" with a UUID string for your dataset.  Note that you have to specify
the full path to the PNG images.  If you started the DVID server using the "-debug" option, 
you'll see a series of messages from the server on reading each image and storing it into the datastore.
This command loads all image filenames in alphanumeric order.  Since we specified "xy" (and could
have used the multidimensional specification "0,1"), each succeeding image is loaded with an offset
incremented by one in the Z (3rd) dimension.

After this command completes, the image data has been ingested into small chunks of data in DVID.

### Use web browser to explore data

You might have noticed a few HTTP API calls listed in the grayscale8 help text.  We are going to
use one of these to look at slices of data orthogonal to the volume axes.  Launch a web browser
and enter the following URL:

    localhost:8000/api/node/c7/mygrayscale/xy/250,250/100,100,2600
    
You should see a small grayscale image appear in your browser.  It will be 250 x 250 pixels, taken
from data in the XY plane using an offset of (100,100,2600).  Note that when we did the "load local"
we specified "100,100,2600".  This loaded the first image with (100,100,2600) as an offset.

Change the Z offset to 2800 to see a different portion of the volume:

    localhost:8000/api/node/c7/mygrayscale/xy/250,250/100,100,2800

We can see the extent of the loaded image using the following resectioning:

    localhost:8000/api/node/c7/mygrayscale/xz/500,500/0,0,2500

A larger 500 x 500 pixel image should now appear in the browser with black areas surrounding your
loaded data.  This is a slice along XZ, an orientation not present in the originally loaded
XY images.
