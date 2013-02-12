DVID: Distributed, Versioned Image Datastore
====

*Status: In development, not ready for use.*

DVID is a distributed, versioned image datastore that uses leveldb for data storage and a Go language layer that provides http and command-line access.

Documentation is [available here](http://godoc.org/github.com/janelia-flyem/dvid).

### Requirements

* Store label volumes and optional label->label maps
* Handle many thousands of connections per second.   Human-operated and automatic segmentation clients will access the image datatore.
* Versioning of volume data and ability to access different versions simultaneously.
* Master database of all volumes and all of their versions.  Ability to see the sequence of operations that led to a version.
* “Lightweight” image datastore that is more similar to sqlite than to a clustered system with multiple databases.  Can be used on one computer, started with one command.  Very simple deployment and installation.
* Distributed version control support where we can “clone” an image volume and then “pull” and “push” versions to other datastores that contain the same volume.  Take inspiration from git distributed version control system.
* Some data compression when dealing with versions that are mostly similar, i.e., identical in many subvolumes.  New branches preferably should reuse some of the older data rather than duplicate data for every new node.
* Fast read/write of views that can be selected for each volume version:
  * Orthogonal xy (with optional label-label map)
  * Orthogonal xz (with optional label-label map)
  * Orthogonal yz (with optional label-label map)
  * Subvolume of a given extent (with optional label-label map)
* Body-centric data of selected labels or those bodies meeting some condition (e.g., proximity to synapses)
* Multi-client reads and writes with atomic writes.  We do not want state of volume at any point to be a mix of one client’s writes and another client’s writes.
* To make it easier to merge labels across versions, the image datastore can provide a new label that is unique across all versions despite simultaneous unique label requests.  (See first-pass implementation section for more details of issue.)
* Graceful degradation with respect to hardware.  System will be more powerful with Janelia computing resources (faster and more CPU cores, larger storage capacity, better networking) but still capable of running on a single Linux workstation with reasonable dedicated hard drive at Dalhousie.  Example: Datastore will be faster and more scalable on Janelia’s future HPC storage (~1 GB/sec r/w) than on 7200 rpm SATA 3 TB hard drive.  Ultimately, the system should allow one to run our thin clients on his/her local machines with some stripped down version of the datastore.
* Clients can subscribe to a version and be notified of changes.  This could be as simple as registering for change notifications on a given subvolume or label.  It could be as complex as monitoring any set of points (e.g., T-bars and PSDs) for label changes or registering for notifications if a labeled body exceeds a specified size.  This may be required to support body-centric data, which is already a requirement, or quality control operations.

### Desired Features

* Allow reasonably fast read/write of arbitrary cut planes given extents.  In most cases, the clients will retrieve a subvolume and create arbitrary cut planes from the loaded subvolume.  However, the image datastore can retrieve just the necessary cuboids that intersect the cut plane and therefore do one-off arbitrary cut planes more efficiently.
* Running DVID executable via “serve” command will establish a server where local browser is a decent GUI interface to datastore system:
  * View directory listing of image volumes and versions in datastore.
  * View and navigate through image volumes using web canvas
  * View documentation:
      * RESTful API through http
      * RPC API, which CLI commands use (see next).
  * Possibly remote datastore contents.
* DVID executable also operates in command-line interface mode where all commands are relayed to DVID server.  Commands should include equivalents to ls, clone, pull, push,  add, commit, log as well as other DVID operations.

### What we don’t need

* 24/7 operation.  We can shut down for short periods of time without catastrophic consequences.
* Ability to instantly start operations on a new volume version.  However, once a new volume version is created, reading and writing to it should be fast and incur minimal latency.

## Dependencies

DVID uses a key-value datastore as its back end.  We plan on using one of two implementations of leveldb initially:

* C++ leveldb plus levigo Go bindings.  This is a more industrial-strength solution and would be the preferred installation for production runs at Janelia.
* Pure Go leveldb implementations.
    * [syndtr's rewrite from C++ version](https://github.com/syndtr/goleveldb)
    * [nigel tao's implementation still in progress](http://code.google.com/p/leveldb-go/)
    
A pure Go leveldb implementation would greatly simplify cross-platform development since Go code can be cross-compiled to Linux, Windows, and Mac targets.  It would also allow simpler inspection of issues.  However, there is no substitute for having large numbers of users testing the product, so the C++ leveldb implementation will be tough to beat in terms of uptime and performance.
