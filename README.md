DVID
====

*Status: In development, not ready for use.*

[![GoDoc](https://godoc.org/github.com/janelia-flyem/dvid?status.png)](https://godoc.org/github.com/janelia-flyem/dvid) [![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest) [![Stories in Ready](https://badge.waffle.io/janelia-flyem/dvid.png?label=ready)](https://waffle.io/janelia-flyem/dvid)

DVID is a *distributed, versioned, image-oriented datastore* written to support 
(Janelia Farm Reseach Center's)[http://www.janelia.org] brain imaging, analysis and 
visualization efforts.  DVID's initial focus is on efficiently storing and retrieving 
3d grayscale and label data in a variety of ways, e.g., subvolumes, images in XY, XZ, and YZ 
orientation, multiscale tiles (quadtree and octree forms), and sparse volumes determined by a label.

DVID is written in Go and supports different storage backends, a Level 2 REST HTTP API, 
command-line access, and a FUSE frontend to at least one of its data types.  It has been 
tested on both MacOS X and Linux (Fedora 16, CentOS 6) but not on Windows.

Command-line and HTTP API documentation is currently distributed over data types and can be 
found in help constants:

* [general commands and HTTP API](http://godoc.org/github.com/janelia-flyem/dvid/server#pkg-constants)
* [keyvalue](http://godoc.org/github.com/janelia-flyem/dvid/datatype/keyvalue#pkg-constants)
* [voxels](http://godoc.org/github.com/janelia-flyem/dvid/datatype/voxels#pkg-constants)
* [labels64](http://godoc.org/github.com/janelia-flyem/dvid/datatype/labels64#pkg-constants)
* [labelmap](http://godoc.org/github.com/janelia-flyem/dvid/datatype/labelmap#pkg-constants)
* [multichan16](http://godoc.org/github.com/janelia-flyem/dvid/datatype/multichan16#pkg-constants)
* [quadtree](http://godoc.org/github.com/janelia-flyem/dvid/datatype/quadtree#pkg-constants)

## Table of Contents

* [Philosophy](#philosophy)
* [Build Process](#build-process)
* [Simple Example](#simple-example)

## Philosophy

DVID (Distributed, Versioned, Image­-oriented Datastore) was designed as a datastore that can be 
easily installed and managed locally, yet still scale to the available storage system and number of 
compute nodes. If data transmission or computer memory is an issue, it allows us to choose a local 
first­ class datastore that will eventually (or continuously) be synced with remote datastores. By 
“first class”, we mean that each DVID server, even on laptops, behaves identically to larger 
institutional DVID servers save for resource limitations like the size of the data that can be 
managed.  Our vision is to have something like a "git" for image-oriented data, although there are a 
number of differences due to the size and typing of data.

Scalability can be achieved in at least two ways:

* **Scale­-up**: Run DVID on a computer with more cores, larger memory, and faster/larger storage 
(e.g., use a cluster­-ready backend system like couchbase, Cassandra, or a Facebook Haystack­-inspired 
system).
* **Scale­-out**: Subdivide (by sharding) the data and run DVID servers on each subdivision, perhaps 
eventually aggregating the subdivisions to a larger DVID server in a map/reduce fashion.

Why is distributed versioning central to DVID instead of a centralized approach?

* **Significant processing can occur in small subsets of the data or using alternative, compact 
representations**: FlyEM mostly works in portions of the data after using full data to establish 
context.  We can't even see the cells if we zoom out to the scale of our volumes. And if we want to 
work on neurons, it's a sparse volume that can be relatively compact and proofreading occurs in that 
sparse volume and its neighboring structures. Frequently, we can also transform voxel­-level data to 
more compact data structures like region adjacency graphs.
* **Research groups may want to silo data but eventually share and sync that data**: It's not clear 
researchers want just "one" centralized datastore but might require one for each institution/group. 
Researchers don't always want to share data. So as soon as you support more than one centralized 
location, and think about syncing, you are basically looking at a distributed data problem or you'll be 
doing some ad hoc solution instead of more elegant git-­like techniques. And sometimes, researchers want 
to only share a particular version of their dataset, e.g., the state of the dataset at the time of a 
publication that requires open access to the data, yet they want to continue to work on the dataset 
privately.
* **As computers increase in power, forcing centralization leads to significant wasted resources**: 
Since significant workflows require only relatively small subsets of data, we can move data to 
workstations and laptops and use graphics/computation resources of those systems. Also, allowing 
distributed data persistence lets us explore other funding mechanisms, not just having deep­-pocketed 
institutions footing the bill for all storage and computation.
* **Large, multi­tenancy datastores can be difficult to optimize for particular use cases and guarantee 
throughput/latency**. Shared resources can be exhausted if many users hit the resource when working 
toward seasonal deadlines. Aside from this timing issue, certain applications require tight bounds on 
availability, e.g., image acquisition. Since data access optimization via caching and other techniques 
is very specific to an application, datastore systems should be (1) relatively simple so systems 
exclusive to an application can be created and (2) have well­-defined interfaces both to the storage 
engine and the client, so application­-specific optimizations can be made. A research group can buy 
servers and deploy a relatively simple system that is dedicated for a particular use case or run 
applications in lock­step so optimizations are easier to make, e.g., the formatting of data to suit a 
particular data access pattern.

Planned and Existing Features for DVID:

* **Distributed operation**: Once a DVID dataset is created and loaded with data, it can be cloned to 
remote sites using user­-defined spatial extents. Each DVID server chooses how much of the data set is 
held locally. (_Status: Planned Q1 2014_)
* **Versioning**: Each version of a DVID dataset corresponds to a node in a version DAG (Directed Acyclic 
Graph). Versions are identified through a UUID that can be composed locally yet are unique globally. 
Versioning and distribution follow patterns similar to distributed version control systems like git and 
mercurial. Provenance is kept in the DAG.  (_Status: Simple DAG and UUID support implemented.  
Versioned compression schemes have been worked out with implementation ~Q2 2014._)
* **Denormalized Views**: For any node in the version DAG, we can choose to create denormalized 
views that accelerate particular access patterns. For example, quad trees can be created for XY, XZ, 
and YZ orthogonal views or sparse volumes can compactly describe a neuron. The extra denormalized data 
is kept in the datastore until a node is archived, which removes all denormalized key­-value pairs
associated with that version node. Views of the same data can be eventually consistent.
(_Status: Multi-scale quadtree, sparse volumes implemented.  Framework for syncing of 
denormalized views planned Q1-2 2014._)
* **Flexible Data Types**: DVID provides a well­-defined interface to data type code that can be 
easily added by users. A DVID server provides HTTP and RPC APIs, authentication, authorization, 
versioning, provenance, and storage engines. It delegates datatype­-specific commands and processing to 
data type code. As long as a DVID type can return data for its implemented commands, we don’t care how 
its implemented. (_Status: Variety of voxel types, quadtree, label maps, and key-value implemented. 
FUSE interface for key-value type working but not heavily used.  Authentication and authorization support planned Q2 2014, likely using Mozilla Persona + auth tokens similar to github API._)
* **Scalable Storage Engine**: Although we may have DVID support polyglot persistence
(i.e., allow use of relational, graph, or NoSQL databases), we are initially focused on 
key­-value stores. DVID has an abstract key­-value interface to its swappable storage engine. 
We choose a key­-value interface because (1) there are a large number of high­-performance, open­-source 
implementations that run from embedded to clustered systems, (2) the surface area of the API is very 
small, even after adding important cases like bulk loads or sequential key read/write, and (3) novel technology tends to match key­-value interfaces, e.g., [groupcache](https://github.com/golang/groupcache)
and [Seagate's Kinetic Open Storage Platform(https://developers.seagate.com/display/KV/Kinetic+Open+Storage+Documentation+Wiki)]  
(_Status: Use of standard leveldb and 
HyperLevelDB implemented.  Lightning MDB to be added soon._)

DVID promotes the view of data as a collection of key­-value pairs where each key is composed of 
global identifiers for versioning and data identification as well as a datatype­-specific index 
(e.g., a spatial index) that allows large data to be broken into chunks. DVID focuses on how to 
break data into these key­-value pairs in a way that facilitates distributed systems as well as 
optimization of data access for various clients.

A DVID server is limited to local resources and the user determines what datasets, subvolume, and 
versions are held within that DVID server. Overwrites are allowed but once a version is locked, no 
further edits are allowed on that particular version. This allows manual or automated editing to be 
done during a period without accumulation of unnecessary deltas.

## Build Process

DVID uses the [buildem system](http://github.com/janelia-flyem/buildem#readme) to automatically 
download and build the specified storage engine (e.g., leveldb), Go language support, and all 
required Go packages.

### One-time Setup

First, setup the proper directory structure that adheres to 
[Go standards](http://golang.org/doc/code.html) and clone the dvid repo:

    % export GOPATH=/path/to/go/workspace
    % export DVIDSRC=$GOPATH/src/github.com/janelia-flyem/dvid
    % mkdir -p $DVIDSRC
    % cd $DVIDSRC
    % git clone https://github.com/janelia-flyem/dvid .

You should also have a BUILDEM_DIR, either an empty directory or your previous buildem directory 
where you'll compile all the required software and eventually place the compiled dvid executable.
You'll want to set your environment variables like so:

    % export BUILDEM_DIR=/path/to/buildem/dir
    % export PATH=$BUILDEM_DIR/bin:$PATH

For Linux, export your library path:

    % export LD_LIBRARY_PATH=$BUILDEM_DIR/lib:$LD_LIBRARY_PATH

For Mac, the library path is specified as DYLD_LIBRARY_PATH:

    % export DYLD_LIBRARY_PATH=$BUILDEM_DIR/lib:$LD_LIBRARY_PATH

In the above, we are saying to use the executables created in the $BUILDEM_DIR/bin directory first,
which should include the DVID executable, and also use buildem-created libraries.

Create an empty build directory used to build just dvid:  

    % cd $DVIDSRC
    % mkdir build
    % cd build
    % cmake -D BUILDEM_DIR=$BUILDEM_DIR ..

The example above creates a build directory in the dvid repo directory, which also has a .gitignore
that ignores all "build" and "build-*" files/directories.

If you haven't built with that buildem directory before, do the additional steps:

    % make
    % cmake -D BUILDEM_DIR=/path/to/buildem/dir ..

### Making and testing DVID

Make dvid:

    % make dvid

This will install a DVID executable 'dvid' in the buildem bin directory.

Tests are run with gocheck:

    % make test

Specifying your choice of [web client](http://github.com/janelia-flyem/dvid-webclient) 
using "-webclient":

    % dvid -webclient=/path/to/dvid-webclient -datastore=/path/to/datastore/dir serve
    
You can then modify the web client code and refresh the browser to see the changes.

## Simple Example

In this example, we'll initialize a new datastore, create grayscale data,
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

Download a 
[small stack of grayscale images from github](https://github.com/janelia-flyem/sample-grayscale).

You can either clone it using git or use the "Download ZIP" button on the right.  Once you've downloaded
that 250 x 250 x 250 grayscale volume, enter the following:

    % dvid node c7 mygrayscale load xy 100,100,2600 "/path/to/sample/*.png"

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

    localhost:8000/api/node/c7/mygrayscale/xy/250_250/100_100_2600
    
You should see a small grayscale image appear in your browser.  It will be 250 x 250 pixels, taken
from data in the XY plane using an offset of (100,100,2600).  Note that when we did the "load local"
we specified "100,100,2600".  This loaded the first image with (100,100,2600) as an offset.  Also 
note the HTTP APIs replace the comma separator with an underscore because commas are reserved URI
characters.

Change the Z offset to 2800 to see a different portion of the volume:

    localhost:8000/api/node/c7/mygrayscale/xy/250_250/100_100_2800

We can see the extent of the loaded image using the following resectioning:

    localhost:8000/api/node/c7/mygrayscale/xz/500_500/0_0_2500

A larger 500 x 500 pixel image should now appear in the browser with black areas surrounding your
loaded data.  This is a slice along XZ, an orientation not present in the originally loaded
XY images.

### Adding a quadtree

Let's precompute multi-scale XY, XZ, and YZ quadtree for our grayscale image.  First, we add an
instance of a quadtree data type under our previous dataset UUID:

    % dvid dataset c7 new quadtree myquadtree source=mygrayscale TileSize=128

Note that we set type-specific parameters, "source" to "mygrayscale", which is the name of the
data we wish to tile, and "TileSize" to "128", which causes all future tile generation to be 128x128
pixels.  Now that we have quadtree data, we generate the quadtree using this command:

    % dvid node c7 myquadtree generate tilespec.json

This will kick off the tile precomputation (about 30 seconds on my MacBook Pro).  Since our
loaded grayscale is 250 x 250 x 250, we will have two different scales in the quadtree.  The original
scale is "0" and can be accessed through the quadtree HTTP API.  Visit this URL in your browser:

    localhost:8000/api/node/c7/myquadtree/tile/xy/0/0_0_0

This will return a 128x128 pixel tile, basically the upper left quadrant of the first slice of our
test data.  By replacing the "0_0_0" (0,0,0) portion with "1_0_0", you can see the upper right
quadrant (tile x=1, y=0) of the first 250x250 image.  Tile space has its origin in the upper left
corner.

We can zoom out a bit by going to scale "1" where returned quadtree have reduced the size of the
original image by 2.

    localhost:8000/api/node/c7/myquadtree/tile/xy/1/0_0_0

The above URL will return a 128x128 pixel tile that covers the original 250x250 image so you see
a bit of black space at the edges.   DVID automatically creates as many scales as necessary
until one tile fits the source image extents.  In this test case, we only need two scales because
at scale "1", our specified tile size can cover the original data.

As an exercise, look at the XZ and YZ quadtree as well.
