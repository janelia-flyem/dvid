/*

Package server provides an HTTP interface to DVID operations.  The server can
be accessed via a web browser or through a HTTP API.  For web browser access,
the goal is to provide a GUI for monitoring and performing a subset of operations
in a nicely formatted view.  

Web browser access is enabled by running "dvid serve <datastore directory>" 
from the command line.  You can then visit "localhost:4000" or specify a
different port number as an option to the "dvid serve" command.

Examples of planned browser-based features in order of likely implementation:

* Peruse documentation on DVID operation and APIs (HTTP and eventually Thrift).

* Graphically show the image version DAG and allow the viewer to examine the 
provenance along the edges.  (GUI for 'log' command.)

* View slices of an image version via a javascript (or Dart) program.  This could
be extended to allow interactive editing and n-dimensional visualization.

* Query remote DVID servers regarding the state of their volume.

*/

package server
