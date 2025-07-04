DVID       [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)
====

[![Go Report Card](https://goreportcard.com/badge/github.com/janelia-flyem/dvid)](https://goreportcard.com/report/github.com/janelia-flyem/dvid)
[![GoDoc](https://godoc.org/github.com/janelia-flyem/dvid?status.png)](https://godoc.org/github.com/janelia-flyem/dvid) 
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/janelia-flyem/dvid)

DVID is a ***D**istributed, **V**ersioned, **I**mage-oriented **D**ataservice* written to support 
neural reconstruction, analysis and visualization efforts at 
[HHMI Janelia Research Center](http://www.janelia.org). It provides storage with branched versioning of a variety of data necessary for our research including teravoxel-scale image volumes, JSON descriptions of objects, sparse volumes, point annotations with relationships (like synapses), etc.

Its goal is to provide:

* A framework for thinking of distribution and versioning of large-scale scientific data 
similar to distributed version control systems like [git](http://git-scm.com).
* Easily extensible *data types* (e.g., *annotation*, *keyvalue*, and *labelmap* in figure below) that allow tailoring of APIs, access speeds, and storage space for different kinds of data.
* The ability to use a variety of storage systems via plugin storage engines, currently limited to systems that can be viewed as (preferably ordered) key-value stores.
* A stable science-driven HTTP API that can be implemented either by native DVID data types or by proxying to other services.

![High-level architecture of DVID](/images/dvid-highlevel.png)

How it's different from other forms of versioned data systems:

* DVID handles large-scale data as in billions or more discrete units of data. Once you get to this scale, storing so many files can be difficult on a local file system or impose a lot of load even on shared file systems.  Cloud storage is always an option (and available in some DVID backends) but that adds latency and doesn't reduce transfer time of such large numbers of files or data chunks. Database systems (including embedded ones) handle this by consolidating many bits of data into larger files. This can also be described as a [sharded data approach](https://github.com/google/neuroglancer/blob/master/src/neuroglancer/datasource/precomputed/sharded.md).
* All versions are available for queries. There is no checkout to read committed data.
* The high-level science API uses pluggable datatypes.  This allows clients to operate on domain-specific data and operations rather than operations on generic files.
* Data can be flexibly assigned to different types of storage, so tera- to peta-scale immutable imaging data can be kept in cloud storage while smaller, frequently mutated label data can be kept on fast local NVMe SSDs. DVID allows data instances to be assigned to different datastores, so large datasets can be spread across multiple local embedded databases as well as cloud stores. Our recent datasets primarily hold local data in [Badger embedded databases](https://github.com/dgraph-io/badger), also written in the Go language.

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

- [DVID       ](#dvid-------)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Basic Usage](#basic-usage)
  - [More Information](#more-information)
  - [Monitoring](#monitoring)
- [Known Clients with DVID Support](#known-clients-with-dvid-support)

## Installation

Users should install DVID from the [releases](https://github.com/janelia-flyem/dvid/releases). 
The main branch of DVID may include breaking changes required by
our research work. 

Developers should consult the [install README](https://github.com/janelia-flyem/dvid/blob/master/GUIDE.md)
where our conda-based process is described.

DVID has been tested on MacOS X, Linux (Fedora 16, CentOS 6, Ubuntu), and 
[Windows Subsystem for Linux (WSL2)](https://msdn.microsoft.com/en-us/commandline/wsl/about). 
It comes out-of-the-box with several embedded key-value databases (Badger, Basho's leveldb)
for storage although you can configure other storage backends.

Before launching DVID, you'll have to [create a configuration file](https://github.com/janelia-flyem/dvid/wiki/Configuring-DVID)
describing ports, the types of storage engines, and where the data should be stored. 
Both simple and complex sample configuration files are provided in the `scripts/distro-files`
directory.

## Basic Usage

Some documentation is available on the DVID [wiki's User Guide](https://github.com/janelia-flyem/dvid/wiki).
When using DVID at scale, our team writes scripts using the [neuclease python library](https://github.com/janelia-flyem/neuclease).
There are also other [DVID access libraries](#known-clients-with-dvid-support) used by our collaborators.

For simple scenarios like just using DVID for branched versioning of key-value data, reading and writing data can be done with
a few simple HTTP requests.

## More Information

Both high-level and detailed descriptions of DVID and its ecosystem can found here:

* A high-level description of [Data Management in Connectomics](https://www.janelia.org/project-team/flyem/blog/data-management-in-connectomics) that includes DVID's use in the [Janelia FlyEM Team](https://www.janelia.org/project-team/flyem).
* [Paper on DVID](https://www.frontiersin.org/article/10.3389/fncir.2019.00005)
describing its motivation and architecture, including how versioning works at the key-value
level.  
* The main place for DVID documentation and information is the [DVID Wiki](https://github.com/janelia-flyem/dvid/wiki) in this github repository. 
* An AI-generated summary of DVID, including architecture diagrams, can be found on [DeepWiki](https://deepwiki.com/janelia-flyem/dvid).
* There is also some documentation and blog posts on [dvid.io](http://dvid.io) pertinent to published datasets like the hemibrain.

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

DVID allows you to assign data instances in a repo to different storage systems, 
which allows great flexibility in optimizing storage for particular use cases.  For example, easily 
compressed label data can be store in fast, expensive SSDs while larger, 
immutable grayscale image data can be stored in petabyte-scale read-optimized systems like
Google Cloud Storage.

DVID is written in Go and supports pluggable storage backends, a REST HTTP API,
and command-line access (likely minimized in near future).  Some components written in 
C, e.g., storage engines like Leveldb and fast codecs like lz4, are embedded or linked as a library.

Command-line and HTTP API documentation can be 
found in [help constants within packages](https://github.com/janelia-flyem/dvid/blob/master/datatype/labelmap/labelmap.go#L34) or by visiting the **/api/help**
HTTP endpoint on a running DVID server.

Over time, a number of built-in data types and backends have not gained traction or sunset. As of 2025, these are the preferred data types and backends used by the Janelia developers for recent & new datasets:

### Data types
* `labelmap` for segmentation
* `labelsz` for label-indexed synapse and other structure counts
* `annotation` for synapses, TO-DO markers, and other 3d point annotations
* `roi` for describing the various regions of interest
* `neuronjson` for more powerful queries and in-memory speedup of JSON per neuron
* `uint8blk` for proxying massive grayscale data stored in Google Cloud (autocreated via config)
* `keyvalue` for any data (e.g., meshes, UI data) stored like a branched versioned file system

### Storage backends
* `badger`: our default embedded key-value store. We typically use multiple DBs for one dataset.
* `filelog`: logs on file system for mutation logs, etc.
* `ngprecomputed`: GCS buckets containing neuroglancer precomputed volumes that DVID proxies.

## Monitoring

Mutations and activity logging can be sent to a Kafka server.  We use kafka activity topics to feed Kibana
for analyzing DVID performance.

![Snapshot of Kibana web page for DVID metrics](https://raw.githubusercontent.com/janelia-flyem/dvid/master/images/dvid-kibana-example.png)


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

