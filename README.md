DVID       [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)
====

[![Go Report Card](https://goreportcard.com/badge/github.com/janelia-flyem/dvid)](https://goreportcard.com/report/github.com/janelia-flyem/dvid)
[![GoDoc](https://godoc.org/github.com/janelia-flyem/dvid?status.png)](https://godoc.org/github.com/janelia-flyem/dvid) 

DVID is a ***D**istributed, **V**ersioned, **I**mage-oriented **D**ataservice* written to support 
neural reconstruction, analysis and visualization efforts at 
[HHMI Janelia Research Center](http://www.janelia.org) using teravoxel-scale image volumes.

Its goal is to provide:

* A framework for thinking of distribution and versioning of large-scale scientific data 
similar to distributed version control systems like [git](http://git-scm.com).
* Easily extensible *data types* that allow tailoring of APIs, access speeds, and storage space.
* The ability to use a variety of storage systems by either creating a data type for that system 
or using a storage engine, currently limited to ordered key/value databases.
* A stable science-driven HTTP API that can be implemented either by native DVID data types or by proxying to other services.

![High-level architecture of DVID](/images/dvid-highlevel.png)

While much of the effort has been focused on the needs of the 
[Janelia FlyEM Team](https://www.janelia.org/project-team/flyem), DVID can be used as a general-purpose
branched versioning file system that handles billions of files and terabytes of data by creating instances of 
the **keyvalue** datatype. Our team uses the **keyvalue** datatype for branched versioning of JSON, configuration, 
and other files using the simple key-value HTTP API.

DVID aspires to be a "github for large-scale scientific data" because a variety of interrelated data
(like image volume, labels, annotations, skeletons, meshes, and JSON data) can be versioned together.
DVID currently handles branched versioning of large-scale data and does not provide domain-specific diff 
tools to compare data from versions, which would be a necessary step for user-friendly pull requests and 
truly collaborative data editing.

## Table of Contents

- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [More Information](#more-information)
- [Monitoring](#monitoring)
- [DVID Clients](#known-clients-with-dvid-support)

## Installation

Users should install DVID from the [releases](https://github.com/janelia-flyem/dvid/releases). 
The main branch of DVID may include breaking changes required by
our research work. 

Developers should consult the [install README](https://github.com/janelia-flyem/dvid/blob/master/GUIDE.md)
where our conda-based process is described.

DVID has been tested on MacOS X, Linux (Fedora 16, CentOS 6, Ubuntu), and 
[Windows 10+ Bash Shell](https://msdn.microsoft.com/en-us/commandline/wsl/about). 
It comes out-of-the-box with a several embedded key-value databases (Badger, Basho's leveldb)
for storage although you can configure other storage backends.

Before launching DVID, you'll have to [create a configuration file](https://github.com/janelia-flyem/dvid/wiki/Configuring-DVID)
describing ports, the types of storage engines, and where the data should be stored. 
Both simple and complex sample configuration files are provided in the `scripts/distro-files`
directory.

## Basic Usage

Some documentation is available on the DVID wiki for how to start the DVID server.
While the [wiki's User Guide](https://github.com/janelia-flyem/dvid/wiki)
provides simple console-based toy examples, please note that
how our team uses the DVID services is much more complex due to our variety of clients
and script-based usage.  Please see the [neuclease python library](https://github.com/janelia-flyem/neuclease)
for more realistic ways to use DVID at scale and, in particular, for larger image volumes.

## More Information

Both high-level and detailed descriptions of DVID and its ecosystem can found here:

* A high-level description of [Data Management in Connectomics](https://www.janelia.org/project-team/flyem/blog/data-management-in-connectomics) that includes DVID's use in the [Janelia FlyEM Team](https://www.janelia.org/project-team/flyem).
* [Paper on DVID](https://www.frontiersin.org/article/10.3389/fncir.2019.00005)
describing its motivation and architecture, including how versioning works at the key-value
level.  
* The main place for DVID documentation and information is [dvid.io](http://dvid.io).  The [DVID Wiki](https://github.com/janelia-flyem/dvid/wiki) in this github repository will be updated and moved to the website.

DVID is easily extensible by adding custom *data types*, each of which fulfill a
minimal interface (e.g., HTTP request handling), DVID's initial focus is on efficiently 
handling data essential for Janelia's connectomics research:

* image and 64-bit label 3d volumes, including multiscale support
* 2d images in XY, XZ, YZ, and arbitrary orientation
* multiscale 2d images in XY, XZ, and YZ, similar to quadtrees
* sparse volumes, corresponding to each unique label in a volume, that can be merged or split
* point annotations (e.g., synapse elements) that can be quickly accessed via subvolumes or labels
* label graphs
* regions of interest represented via a coarse subdivision of space using block indices
* 2d and 3d image and label data using Google BrainMaps API and other cloud-based services

Each of the above is handled by built-in data types via a
[Level 2 REST HTTP API](http://martinfowler.com/articles/richardsonMaturityModel.html)
implemented by Go language packages within the *datatype* directory.  When dealing with novel data,
we typically use the generic *keyvalue* datatype and store JSON-encoded or binary data
until we understand the desired access patterns and API.  When we outgrow the *keyvalue* type's
GET, POST, and DELETE operations, we create a custom datatype package with a specialized HTTP API.

DVID allows you to assign different storage systems to data instances within a single repo, 
which allows great flexibility in optimizing storage for particular use cases.  For example, easily 
compressed label data can be store in fast, expensive SSDs while larger, 
immutable grayscale image data can be stored in petabyte-scale read-optimized systems.

DVID is written in Go and supports pluggable storage backends, a REST HTTP API,
and command-line access (likely minimized in near future).  Some components written in 
C, e.g., storage engines like Leveldb and fast codecs like lz4, are embedded or linked as a library.

## Monitoring

Mutations and activity logging can be sent to a Kafka server.  We use kafka activity topics to feed Kibana
for analyzing DVID performance.

![Snapshot of Kibana web page for DVID metrics](https://raw.githubusercontent.com/janelia-flyem/dvid/master/images/dvid-kibana-example.png)


Command-line and HTTP API documentation can be 
found in [help constants within packages](https://github.com/janelia-flyem/dvid/blob/master/datatype/labelvol/labelvol.go#L34) or by visiting the **/api/help**
HTTP endpoint on a running DVID server.

# Known Clients with DVID Support

Programmatic clients:
* [neuclease](https://github.com/janelia-flyem/neuclease), python library from HHMI Janelia
* [intern](https://bossdb.org/tools/intern), python library from Johns Hopkins APL 
* [natverse](https://natverse.org/), R library from Jefferis Lab
* [libdvid-cpp](https://github.com/janelia-flyem/libdvid-cpp), C++ library from HHMI Janelia FlyEM 

GUI clients:
* [Google neuroglancer](https://github.com/google/neuroglancer)
* [neuTu](https://janelia-flyem.gitbook.io/neutu)
* [Clio](https://github.com/clio-janelia/clio_website)

Screenshot of an early web app prototype pulling neuron data and 2d slices from 3d grayscale data:

![Web app for 3d inspection being served from and sending requests to DVID](https://raw.githubusercontent.com/janelia-flyem/dvid/master/images/webapp.png)

