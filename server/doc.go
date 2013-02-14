/*
Package server provides web and rpc interfaces to DVID operations.  For web browser 
access, the goal is to provide a GUI for monitoring and performing a subset of 
operations in a nicely formatted view.  

DVID command line interaction occurs via the rpc interface to a running server.
Please see the main DVID documentation:

http://godoc.org/github.com/janelia-flyem/dvid

Examples of planned browser-based features in order of likely implementation:

  • Peruse documentation on DVID operation and APIs (HTTP and eventually Thrift).

  • Graphically show the image version DAG and allow the viewer to examine the 
    provenance along the edges.  (GUI for 'log' command.)

  • View slices of an image version via a javascript (or Dart) program.  
    This could be extended to allow interactive editing and n-dimensional visualization.

  • Query remote DVID servers regarding the state of their volume.
*/
package server
