/*
	Package dvid provides types, constants, and functions that have no other dependencies
	and can be used by all packages within DVID.  This includes core data structures,
	command string handling, and command packaging.  Since these elements are used at
	multiple DVID layers, we separate them here and allow reuse in layer-specific types
	through embedding.
*/
package dvid
