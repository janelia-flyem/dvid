/*

Package keyvalue provides a unified interface to a number of leveldb
implementations.  Application-specific use of keys and values should
be handled by layers above this package.

Currently, two implementations are supported:

    * github.com/jmhodges/levigo: binds to C++ leveldb library
    * github.com/syndtr/goleveldb/leveldb: pure Go implementation

The former is more tested while the latter allows cross-platform single file 
executables.

*/

package keyvalue
