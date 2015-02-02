DVID       [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)
====

*Status: In development, being tested at Janelia, and not ready for external use due to possible breaking changes.*

[![GoDoc](https://godoc.org/github.com/janelia-flyem/dvid?status.png)](https://godoc.org/github.com/janelia-flyem/dvid) [![Build Status](https://drone.io/github.com/janelia-flyem/dvid/status.png)](https://drone.io/github.com/janelia-flyem/dvid/latest)

See the [DVID Wiki](https://github.com/janelia-flyem/dvid/wiki) for more information including installation and examples of use.

![Web app for 3d inspection being served from and sending requests to DVID](/images/webapp.png)

DVID is a *distributed, versioned, image-oriented datastore* written to support 
[Janelia Farm Research Center's](http://www.janelia.org) brain imaging, analysis and 
visualization efforts.  It's goal is to provide:

* Easily extensible *data types* that allow tailoring of access speeds, storage space, and APIs.
* The ability to use a variety of storage systems by either creating a data type for that system or using an ordered key/value datastore interface.
* A framework for thinking of distribution and versioning of data similar to distributed version 
control systems like [git](http://git-scm.com).

Long-term, DVID aspires to be a "github for large image-oriented data" because each DVID
server can manage multiple repositories, each of which contains an image-oriented repo
with related data like an image volume, labels, and skeletons.  The goal is to provide scientists 
with a github-like web client + server that can push/pull data to a collaborator's DVID server.

DVID's initial focus is on efficiently storing and retrieving 3d grayscale and label data in a 
variety of ways:

* subvolumes
* images in XY, XZ, YZ, and arbitrary orientation
* multiscale 2d and 3d, similar to quadtrees and octrees
* sparse volumes determined by a label
* label maps that handle mapping of labels X -> Y
* label graphs
* regions of interest represented via a coarse subdivision of space using block indices
* tiles and images in XY, XZ, and YZ orientation using Google BrainMaps API

Each of the above is handled by built-in data types via a
[Level 2 REST HTTP API](http://martinfowler.com/articles/richardsonMaturityModel.html)
implemented by Go language packages within the *datatype* directory.

DVID is primarily written in Go and supports different storage backends, a REST HTTP API,
command-line access (likely minimized in near future), and a FUSE frontend to at least 
one of its data types.  Some components are written in C, e.g., storage engines like Leveldb and
fast codecs like lz4.  DVID has been tested on both MacOS X and Linux (Fedora 16, CentOS 6, Ubuntu) 
but not on Windows.

Command-line and HTTP API documentation is currently distributed over data types and can be 
found in [help constants within packages](https://github.com/janelia-flyem/dvid/blob/master/datatype/labels64/labels64.go#L39) or by visiting the **/api/help**
HTTP endpoint on a running DVID server.  We are in the process of 
figuring out a nice way to document the APIs either through RAML or Swagger.
