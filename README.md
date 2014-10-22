DVID [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/jfrc_grey_180x40.png)](http://www.janelia.org)
====

*Status: In development, being tested at Janelia, and not ready for external use due to possible breaking changes.*

[![GoDoc](https://godoc.org/github.com/janelia-flyem/dvid?status.png)](https://godoc.org/github.com/janelia-flyem/dvid) [![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest)

![Web app for 3d inspection being served from and sending requests to DVID](/images/webapp.png)

DVID is a *distributed, versioned, image-oriented datastore* written to support 
[Janelia Farm Research Center's](http://www.janelia.org) brain imaging, analysis and 
visualization efforts.  

DVID aspires to be a "github for large image-oriented data" because each DVID
server can manage multiple repositories, each of which contains an image-oriented repo
with related data like an image volume, labels, and skeletons.  The goal is to provide scientists 
with a github-like web client + server that can push/pull data to a collaborator's DVID server.

Although DVID is easily extensible by adding custom *data types*, each of which fulfill a
minimal interface (e.g., HTTP request handling), DVID's initial focus is on efficiently storing 
and retrieving 3d grayscale and label data in a variety of ways:

* subvolumes
* images in XY, XZ, YZ, and arbitrary orientation
* multiscale 2d and 3d, similar to quadtrees and octrees
* sparse volumes determined by a label
* label maps that handle mapping of labels X -> Y
* label graphs
* regions of interest represented via a coarse subdivision of space using block indices

Each of the above is handled by built-in data types via a
[Level 2 REST HTTP API](http://martinfowler.com/articles/richardsonMaturityModel.html)
implemented by Go language packages within the *datatype* directory.  When dealing with novel data,
we typically use the generic *keyvalue* data type and store JSON-encoded or binary data
until we understand the desired access patterns and API.  When we outgrow the *keyvalue* type's
GET, POST, and DELETE operations, we create a custom data type package with a specialized HTTP API.

DVID is primarily written in Go and supports different storage backends, a REST HTTP API,
command-line access (likely minimized in near future), and a FUSE frontend to at least 
one of its data types.  Some components are written in C, e.g., storage engines like Leveldb and
fast codecs like lz4.  DVID has been tested on both MacOS X and Linux (Fedora 16, CentOS 6, Ubuntu) 
but not on Windows.

Command-line and HTTP API documentation is currently distributed over data types and can be 
found in [help constants within packages](https://github.com/janelia-flyem/dvid/blob/master/datatype/labels64/labels64.go#L39) or by visiting the **/api/help**
HTTP endpoint on a running DVID server.  We are in the process of 
figuring out a nice way to document the APIs either through RAML or Swagger.

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
managed.  Our vision is to have something like a [github](http://github.com) for image-oriented data, 
although there are a number of differences due to the size and typing of data as well as the approach 
to transferring versioned data between DVID servers.  We hope to leverage the significant experience in 
crafting workflows and management tools for distributed, versioned operations.

DVID has been designed foremost as a malleable system with exchangeable components:

* Data type packages -- allow easy addition of new data and access patterns.
* Storage engines -- allow tradeoffs in access speed, data size, and concurrency.

DVID promotes the view of data as a collection of key­-value pairs where each key is composed of 
global identifiers for versioning and data identification as well as a datatype­-specific index 
(e.g., a spatial index) that allows large data to be broken into chunks. DVID focuses on how to 
break data into these key­-value pairs in a way that optimizes data access for various clients.

A DVID server is limited to local resources and the user determines what repos, data extents, and 
versions are held within that DVID server. Overwrites are allowed but once a version is locked, no 
further edits are allowed on that particular version. This allows manual or automated editing to be 
done during a period without accumulation of unnecessary deltas.

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
to only share a particular version of their repo, e.g., the state of the repo at the time of a 
publication that requires open access to the data, yet they want to continue to work on the repo 
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
particular data access pattern.  After a particular use case is addressed like image acquisition,
some or all of the data can be synced to another DVID server that may be optimized for different 
uses like parallel proofreading operations in disjoint but small subvolumes.

Planned and Existing Features for DVID:

**Distributed operation**: Once a DVID repo is created and loaded with data, it can be pushed to 
remote sites using an optional ROI as well as pulled. Each DVID server chooses how much of the data set is 
held locally. 

_Status: Repo push with optional data instance specification added in September 2014.  See [published one-column repo](http://emdata.janelia.org).  Pull ability to be added Q4 2014.  Improved testing and throughput as time permits._

**Versioning**: Each version of a DVID repo corresponds to a node in a version DAG 
(Directed Acyclic Graph). Versions are identified through a UUID that can be composed locally 
yet are unique globally. 
Versioning and distribution follow patterns similar to distributed version control systems like git and 
mercurial. Provenance is kept in the DAG.

_Status: Simple DAG and UUID support implemented.  "Sparse" Version compression was introduced in August 2014 and 
is undergoing testing/refinement.  There are plans to implement slower but more git-like versioning support via
content-addressable hashes when adding support for unordered key-value stores like Scality._

**Denormalized Views**: For any node in the version DAG, we can choose to create denormalized 
views that accelerate particular access patterns. For example, multiscale2d (quadtree) data 
can be created for XY, XZ, and YZ orthogonal views or sparse volumes can compactly describe a neuron. 
The extra denormalized data is kept in the datastore until a node is archived, which removes all 
denormalized key­-value pairs associated with that version node. Views of the same data will be eventually 
consistent.

_Status: Multi-scale 2d images in XY, XZ, YZ, surface voxels and sparse volumes implemented. 
Multi-scale 3d is planned with no set timeline.  Framework for syncing of denormalized views planned Q4 2014._

**Flexible Data Types**: DVID provides a well­-defined interface to data type code that can be 
easily added by users. A DVID server provides HTTP and RPC APIs, authentication, authorization, 
versioning, provenance, and storage engines. It delegates datatype­-specific commands and processing to 
data type code. As long as a DVID type can return data for its implemented commands, we don’t care how 
its implemented. 

_Status: Variety of voxel types, multiscale2d, label map, label graph, key-value, and ROI implemented. FUSE 
interface for key-value type working but not heavily used.  Authentication and authorization support planned 
Q4 2014, likely using Google or other provider authentication + tokens similar to github API._

**Scalable Storage Engine**: Although DVID may support polyglot persistence
(i.e., allow use of relational, graph, or NoSQL databases), we are initially focused on 
key­-value stores. DVID has an abstract key­-value interface to its swappable storage engine. 
We choose a key­-value interface because (1) there are a large number of high­-performance, open­-source 
implementations that run from embedded to clustered systems, (2) the surface area of the API is very 
small, even after adding important cases like bulk loads or sequential key read/write, and 
(3) novel technology tends to match key­-value interfaces, 
e.g., [groupcache](https://github.com/golang/groupcache)
and [Seagate's Kinetic Open Storage Platform](https://developers.seagate.com/display/KV/Kinetic+Open+Storage+Documentation+Wiki).
As storage becomes more log structured, the key-value API becomes a more natural fit.

_Status: Currently built with [Basho-tuned leveldb](https://github.com/basho/leveldb) and
other leveldb variants have been tested successfully in past:
[Google's open source version](https://code.google.com/p/leveldb/) and
[HyperLevelDB](https://github.com/rescrv/HyperLevelDB).  RocksDB support is planned. Added 
[Lightning MDB](http://symas.com/mdb/) and also experimental use of 
[Bolt](https://github.com/boltdb/bolt), although neither have been tuned to work as well as
the leveldb variants.  Google datastore support planned Q4 2014.
Clustered DB or object-value store support planned Q4 2014 (Scality and possibly FoundationDB drivers).
Direct support of Seagate Kinetic drives via their
[protobuf protocol](https://github.com/Seagate/kinetic-protocol) is planned Q4 2014 or Q1 2015, 
depending on availability of drives._

## Build Process

DVID uses the [buildem system](http://github.com/janelia-flyem/buildem#readme) to automatically 
download and build the specified storage engine (e.g., leveldb), Go language support, and all 
required Go packages.

### One-time Setup

Make sure you have the basic requirements:

* C/C++ compiler
* [CMake](http://www.cmake.org/cmake/resources/software.html)
* [git](http://git-scm.com/downloads)

Before downloading DVID, setup the proper directory structure that adheres to 
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

You can specify a particular storage engine for DVID by adding a `-D DVID_BACKEND=...` option
to the above cmake command.  It currently defaults to `basholeveldb` (the Basho-tuned leveldb)
but could be run with the `lmdb` setting to install [Lightning MDB](http://symas.com/mdb/).

### Making and testing DVID

Make dvid:

    % make dvid

This will install a DVID executable 'dvid' in the buildem bin directory.

Tests are run with gocheck:

    % make test

Specifying your choice of [web client](http://github.com/janelia-flyem/dvid-webclient) 
using "-webclient":

    % dvid -webclient=/path/to/dvid-webclient serve /path/to/datastore/dir
    
You can then modify the web client code and refresh the browser to see the changes.

### Server tuning for big data (optional but recommended)

Particularly when using leveldb variants, we recommend modifying default "max open files" and
also avoiding extra disk head seeks by turning off *noatime*, which records the last accessed
time for all files.  See [this explanation on the basho page](http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/#Tuning-LevelDB)

First, make sure you allow a sufficient number of open files.  This can be checked via the 
"ulimit -n" command in Linux and Mac.  We suggest raising this to 65535 on Linux and at least
8192 on a Mac.  You might have to modify the
(1024 has proven sufficient for Teravoxel repos on 64-bit Linux using standard leveldb but 
this had to be raised to several thousand even for 50 Gigavoxel repos on Mac.)

    % ulimit -n 65535

Second, disable access-time updates for the mount with your DVID data.  In Linux, you can
add the *noatime* mounting option to /etc/fstab for the partition holding your data.
The line for the mount holding your DVID data should like something like this:

    /dev/mapper/vg0-lv_data    /dvid/data     xfs      noatime,nobarrier     1 2

Then remount the disk:

    % mount /dvid/data -o remount


## Simple Example

In this example, we'll initialize a new datastore, create grayscale data,
load some images, and then use a web browser to see the results.  Although this demo
uses the command line, in the near future we'll likely deprecate all command line
interaction (save *create*, *repair*, and *serve* commands) and force interaction
through HTTP requests.

### Getting help

If DVID was installed correctly, you should be able to ask dvid for help:

    % dvid help

### Create the datastore

Depending on the storage engine, DVID will store data into a directory.  We must initialize
the datastore by specifying a datastore directory.

    % dvid init /path/to/datastore/dir

### Start the DVID server

The "-debug" option lets you see how DVID is processing requests.

    % dvid -debug serve /path/to/datastore/dir

If dvid wasn't compiled with a built-in web client, you'll see some complaints and ways you can 
specify a web client.  For our purposes, though, we don't need the web console for this simple
example.  We will be accessing the standard HTTP API directly through a web browser.

Open another terminal and run "dvid help" again.  You'll see more information because dvid can
detect a running server and describe the data types available.  You can also ask for just the
data types supported by this DVID server:

    % dvid types

### Create a new repository (repo)

One DVID server can manage different repositories (repos), each of which correspond to a set of
possibly versioned data in a consistent coordinate system.   We think of a repo as all the data
associated with a particular alignment of images.  If the underlying image data is changed
dramatically, e.g., through realignment, warping, etc, it should be given a separate repo.
We create a new repo like so:

    % dvid repos new

A hexadecimal string will be printed in response.  Repos are identified by the global UUID of
its root version, usually printed and read in hexadecimal format (e.g., "c78a0").
When supplying a UUID, you only need enough letters to uniquely identify the UUID within that
DVID server.  For example, if there are only two repos, each with only one version, and the root
versions for each repo are "c78a0..." and "da0a4...", respectively, then you can uniquely specify
the repos using "c7" and "da".  (We might use one letter, but generally two or more letters 
are better.)

    % dvid types
    
Entering the above command will show the new repo but there are no data under it.

### Create new data for a repo

We can create an instance of a supported data type for the new repo:

    % dvid repo c7 new grayscale8 mygrayscale

Replace "c7" with the first two letters of whatever UUID was printed when you created a new repo.
You can also see the UUIDs for repos by using the "dvid types" command.

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

    % dvid node c7 mygrayscale load 100,100,2600 "/path/to/sample/*.png"

Once again, replace the "c7" with a UUID string for your repo.  Note that you have to specify
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

    <api URL>/node/c7/mygrayscale/xy/raw/250_250/100_100_2600

where `<api URL>` is typically `localhost:8000/api` where the hostname can be set by the dvid option
`-web` as in `dvid -web=:8080 server /path/to/db`.
    
You should see a small grayscale image appear in your browser.  It will be 250 x 250 pixels, taken
from data in the XY plane using an offset of (100,100,2600).  Note that when we did the "load local"
we specified "100,100,2600".  This loaded the first image with (100,100,2600) as an offset.  Also 
note the HTTP APIs replace the comma separator with an underscore because commas are reserved URI
characters.

Change the Z offset to 2800 to see a different portion of the volume:

    <api URL>/node/c7/mygrayscale/xy/raw/250_250/100_100_2800

We can see the extent of the loaded image using the following resectioning:

    <api URL>/node/c7/mygrayscale/xz/raw/500_500/0_0_2500

A larger 500 x 500 pixel image should now appear in the browser with black areas surrounding your
loaded data.  This is a slice along XZ, an orientation not present in the originally loaded
XY images.

### Adding multi-scale 2d images

Let's precompute XY, XZ, and YZ multiscale2d for our grayscale image.  First, we add an
instance of a multiscale2d data type under our previous repo UUID:

    % dvid repo c7 new multiscale2d mymultiscale2d source=mygrayscale TileSize=128

Note that we set type-specific parameters, "source" to "mygrayscale", which is the name of the
data we wish to tile, and "TileSize" to "128", which causes all future tile generation to be 128x128
pixels.  Now that we have multiscale2d data, we generate the multiscale2d using this command:

    % dvid node c7 mymultiscale2d generate

This will kick off the tile precomputation (about 30 seconds on my MacBook Pro).  Since our
loaded grayscale is 250 x 250 x 250, we will have two different scales in the multiscale2d.  The original
scale is "0" and can be accessed through the multiscale2d HTTP API.  Visit this URL in your browser:

    <api URL>/node/c7/mymultiscale2d/tile/xy/0/0_0_0

This will return a 128x128 pixel PNG tile, basically the upper left quadrant of the first slice of our
test data.  By replacing the "0_0_0" (0,0,0) portion with "1_0_0", you can see the upper right
quadrant (tile x=1, y=0) of the first 250x250 image.  Tile space has its origin in the upper left
corner.

We can zoom out a bit by going to scale "1" where returned multiscale2d have reduced the size of the
original image by 2.

    <api URL>/node/c7/mymultiscale2d/tile/xy/1/0_0_0

The above URL will return a 128x128 pixel tile that covers the original 250x250 image so you see
a bit of black space at the edges.   DVID automatically creates as many scales as necessary
until one tile fits the source image extents.  In this test case, we only need two scales because
at scale "1", our specified tile size can cover the original data.

As an exercise, look at the XZ and YZ multiscale2d as well.
