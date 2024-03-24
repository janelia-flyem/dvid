/*
Package storage provides a unified interface to a number of storage engines.
Since each storage engine has different capabilities, this package defines a
number of interfaces in addition to the core Engine interface, which all
storage engines should satisfy.

Keys are specified as a combination of Context and a datatype-specific byte slice,
typically called an "type-specific key" (TKey) in DVID docs and code.  The Context
provides DVID-wide namespacing and as such, must use one of the Context implementations
within the storage package.  (This is enforced by making Context a Go opaque interface.)
The type-specific key formatting is entirely up to the datatype designer, although
use of dvid.Index is suggested.

Initially we are concentrating on key-value backends but expect to support
graph and perhaps relational databases, either using specialized databases
or software layers on top of an ordered key-value store.

Although we assume lexicographically ordering for range queries, there is some
variation in how variable size keys are treated.  We assume all storage engines,
after appropriate DVID drivers, use the following definition of ordering:

	A string s precedes a string t in lexicographic order if:

	* s is a prefix of t, or
	* if c and d are respectively the first character of s and t in which s and t differ,
	  then c precedes d in character order.
	* if s and t are equivalent for all of s, but t is longer

	Note: For the characters that are alphabetical letters, the character order coincides
	with the alphabetical order. Digits precede letters, and uppercase letters precede
	lowercase ones.

	Examples:

	composer precedes computer
	house precedes household
	Household precedes house
	H2O precedes HOTEL
	mydex precedes mydexterity

	Note that the above is different than shortlex order, which would group strings
	based on length first.

The above lexicographical ordering is used by default for levedb variants.
*/
package storage

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

// Key is the slice of bytes used to store a value in a storage engine.  It internally
// represents a number of DVID features like a data instance ID, version, and a
// type-specific key component.
type Key []byte

// KeyRange is a range of keys that is closed at the beginning and open at the end.
type KeyRange struct {
	Start   Key // Range includes this Key.
	OpenEnd Key // Range extend to but does not include this Key.
}

// TKey is the type-specific component of a key.  Each data instance will insert
// key components into a class of TKey.  Typically, the first byte is unique for
// a given class of keys within a datatype.  For a given class of type-specific keys,
// they must either be (1) identical length or (2) if varying length, not share a
// common prefix.  The second case can be trivially fulfilled by ending a
// type-specific key with a unique (not occurring within these type-specific keys)
// termination byte like 0x00.
type TKey []byte

const (
	tkeyMinByte      = 0x00
	tkeyStandardByte = 0x01
	tkeyMaxByte      = 0xFF

	TKeyMinClass = 0x00
	TKeyMaxClass = 0xFF
)

// TKeyClass partitions the TKey space into a maximum of 256 classes.
type TKeyClass byte

func NewTKey(class TKeyClass, tkey []byte) TKey {
	b := make([]byte, 2+len(tkey))
	b[0] = byte(class)
	b[1] = tkeyStandardByte
	if tkey != nil {
		copy(b[2:], tkey)
	}
	return TKey(b)
}

// MinTKey returns the lexicographically smallest TKey for this class.
func MinTKey(class TKeyClass) TKey {
	return TKey([]byte{byte(class), tkeyMinByte})
}

// MaxTKey returns the lexicographically largest TKey for this class.
func MaxTKey(class TKeyClass) TKey {
	return TKey([]byte{byte(class), tkeyMaxByte})
}

// Class returns the TKeyClass of a TKey.
func (tk TKey) Class() (TKeyClass, error) {
	if len(tk) == 0 {
		return 0, fmt.Errorf("can't get class of length 0 TKey")
	}
	return TKeyClass(tk[0]), nil
}

// ClassBytes returns the bytes for a class of TKey, suitable for decoding by
// each data instance.
func (tk TKey) ClassBytes(class TKeyClass) ([]byte, error) {
	if tk[0] != byte(class) {
		return nil, fmt.Errorf("bad type-specific key: expected class %v got %v", class, tk[0])
	}
	return tk[2:], nil
}

// KeyValue stores a full storage key-value pair.
type KeyValue struct {
	K Key
	V []byte
}

// KeyValues is a slice of key-value pairs that can be sorted.
type KeyValues []KeyValue

func (kv KeyValues) Len() int      { return len(kv) }
func (kv KeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K, kv[j].K) <= 0
}

// TKeyValue stores a type-specific key-value pair.
type TKeyValue struct {
	K TKey
	V []byte
}

// Deserialize returns a type key-value pair where the value has been deserialized.
func (kv TKeyValue) Deserialize(uncompress bool) (TKeyValue, error) {
	value, _, err := dvid.DeserializeData(kv.V, uncompress)
	return TKeyValue{kv.K, value}, err
}

// TKeyValues is a slice of type-specific key-value pairs that can be sorted.
type TKeyValues []TKeyValue

func (kv TKeyValues) Len() int      { return len(kv) }
func (kv TKeyValues) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv TKeyValues) Less(i, j int) bool {
	return bytes.Compare(kv[i].K, kv[j].K) <= 0
}

// Op enumerates the types of single key-value operations that can be performed for storage engines.
type Op uint8

const (
	GetOp Op = iota
	PutOp
	DeleteOp
	CommitOp
)

// ChunkOp is a type-specific operation with an optional WaitGroup to
// sync mapping before reduce.
type ChunkOp struct {
	Op interface{}
	Wg *sync.WaitGroup
}

// Chunk is the unit passed down channels to chunk handlers.  Chunks can be passed
// from lower-level database access functions to type-specific chunk processing.
type Chunk struct {
	*ChunkOp
	*TKeyValue
}

// ChunkFunc is a function that accepts a Chunk.
type ChunkFunc func(*Chunk) error

// PatchFunc is a function that accepts a value and patches that value in place
type PatchFunc func([]byte) ([]byte, error)

// KeyChan is a channel of full (not type-specific) keys.
type KeyChan chan Key

// ---- Storage interfaces ------

// DAGStoreGetter provides ways to read key-value pairs individually and in
// groups, where the latter can also be ordered by key or more quickly streamed
// in unordered fashion.
type DAGStoreGetter interface {
	// Get returns a value given a key.
	Get(ctx Context, k TKey) ([]byte, error)

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(ctx Context, kStart, kEnd TKey) ([]*TKeyValue, error)

	// GetRangeStream sends range of values spanning (kStart, kEnd) key-values.
	GetRangeStream(ctx Context, kStart, kEnd TKey, ordered, keysOnly bool, out chan *KeyValue) error

	// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key-value reads.
	// If the ChunkFunc returns an error, it is expected that the ProcessRange should
	// immediately terminate and propagate the error.
	ProcessRange(ctx Context, kStart, kEnd TKey, ordered bool, op *ChunkOp, f ChunkFunc) error
}

// BlobStore allows writing and retrieving unversioned data.  It differs from KeyValueDB in that
// it auto-generates and returns a key on writing, typically the content hash.
type BlobStore interface {
	PutBlob([]byte) (ref string, err error)
	GetBlob(ref string) ([]byte, error)
}

// KeyValueIngestable implementations allow ingestion of data without necessarily allowing
// immediate reads of the ingested data.
type KeyValueIngestable interface {
	// KeyValueIngest accepts mutations without any guarantee that the ingested key value
	// will be immediately readable.  This allows immutable stores to accept data that will
	// be later processed into its final readable, immutable form.
	KeyValueIngest(Context, TKey, []byte) error
}

type KeyValueTimestampGetter interface {
	// Get returns a value as well as last modification time
	GetWithTimestamp(ctx Context, k TKey) ([]byte, time.Time, error)
}

type KeyValueGetter interface {
	// Get returns a value given a key.
	Get(ctx Context, k TKey) ([]byte, error)

	// Exists returns true if a key has been set.
	Exists(ctx Context, k TKey) (bool, error)
}

type OrderedKeyValueGetter interface {
	KeyValueGetter

	// GetRange returns a range of values spanning (kStart, kEnd) keys.
	GetRange(ctx Context, kStart, kEnd TKey) ([]*TKeyValue, error)

	// KeysInRange returns a range of type-specific key components spanning (kStart, kEnd).
	KeysInRange(ctx Context, kStart, kEnd TKey) ([]TKey, error)

	// SendKeysInRange sends a range of keys down a key channel.
	SendKeysInRange(ctx Context, kStart, kEnd TKey, ch KeyChan) error

	// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
	// allowing chunk processing to be concurrent with key-value sequential reads.
	// Since the chunks are typically sent during sequential read iteration, the
	// receiving function can be organized as a pool of chunk handling goroutines.
	// See datatype/imageblk.ProcessChunk() for an example.  If the ChunkFunc returns
	// an error, it is expected that the ProcessRange should immediately terminate and
	// propagate the error.
	ProcessRange(ctx Context, kStart, kEnd TKey, op *ChunkOp, f ChunkFunc) error

	// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
	// retrieval like DVID-to-DVID communication and should not be used by data type
	// implementations if possible because each version's key-value pairs are sent
	// without filtering by the current version and its ancestor graph.  A nil is sent
	// down the channel when the range is complete.  The query can be cancelled by sending
	// a value down the cancel channel.
	RawRangeQuery(kStart, kEnd Key, keysOnly bool, out chan *KeyValue, cancel <-chan struct{}) error
}

type KeyValueSetter interface {
	// Put writes a value with given key in a possibly versioned context.
	Put(Context, TKey, []byte) error

	// Delete deletes a key-value pair so that subsequent Get on the key returns nil.
	// For versioned data in mutable stores, Delete() will create a tombstone for the version
	// unlike RawDelete or DeleteAll.
	Delete(Context, TKey) error

	// RawPut is a low-level function that puts a key-value pair using full keys.
	// This can be used in conjunction with RawRangeQuery.  It does not automatically
	// delete any associated tombstone, unlike the Delete() function, so tombstone
	// deletion must be handled via RawDelete().
	RawPut(Key, []byte) error

	// RawDelete is a low-level function.  It deletes a key-value pair using full keys
	// without any context.  This can be used in conjunction with RawRangeQuery.
	RawDelete(Key) error
}

type OrderedKeyValueSetter interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(Context, []TKeyValue) error

	// DeleteRange removes all key-value pairs with keys in the given range.
	// If versioned data in mutable stores, this will create tombstones in the version
	// unlike RawDelete or DeleteAll.
	DeleteRange(ctx Context, kStart, kEnd TKey) error

	// DeleteAll removes all key-value pairs for the context.
	DeleteAll(ctx Context) error
}

// KeyValueDB provides an interface to the simplest storage API: a key-value store.
type KeyValueDB interface {
	dvid.Store
	KeyValueGetter
	KeyValueSetter
}

// OrderedKeyValueDB adds range queries and range puts to a base KeyValueDB.
type OrderedKeyValueDB interface {
	dvid.Store
	OrderedKeyValueGetter
	OrderedKeyValueSetter
}

// KeyValueBatcher allow batching operations into an atomic update or transaction.
// For example: "Atomic Updates" in http://leveldb.googlecode.com/svn/trunk/doc/index.html
type KeyValueBatcher interface {
	NewBatch(ctx Context) Batch
}

// KeyValueRequester allows operations to be queued so that
// they can be handled as a batch job.  (See RequestBuffer for
// more information.)
type KeyValueRequester interface {
	NewBuffer(ctx Context) RequestBuffer
}

// RequestBufferSubset implements a subset of the ordered key/value interface.
// It declares interface common to both ordered key value and RequestBuffer
type BufferableOps interface {
	KeyValueSetter

	// Put key-value pairs.  Note that it could be more efficient to use the Batcher
	// interface so you don't have to create and keep a slice of KeyValue.  Some
	// databases like leveldb will copy on batch put anyway.
	PutRange(Context, []TKeyValue) error

	// DeleteRange removes all key-value pairs with keys in the given range.
	// If versioned data in mutable stores, this will create tombstones in the version
	// unlike RawDelete or DeleteAll.
	DeleteRange(ctx Context, kStart, kEnd TKey) error

	// ProcessRange will process all gets when flush is called
	ProcessRange(ctx Context, kStart, kEnd TKey, op *ChunkOp, f ChunkFunc) error
}

// RequestBuffer allows one to queue up several requests and then process
// them all as a batch operation (if the driver supports batch
// operations).  There is no guarantee on the order or atomicity
// of these operations.
type RequestBuffer interface {
	BufferableOps

	// ProcessList will process all gets when flush is called
	ProcessList(ctx Context, tkeys []TKey, op *ChunkOp, f ChunkFunc) error

	// PutCallback writes a value with given key in a possibly versioned context and signals the callback
	PutCallback(Context, TKey, []byte, chan error) error

	// Flush processes all the queued jobs
	Flush() error
}

// Batch groups operations into a transaction.
// Clear() and Close() were removed due to how other key-value stores implement batches.
// It's easier to implement cross-database handling of a simple write/delete batch
// that commits then closes rather than something that clears.
type Batch interface {
	// Delete removes from the batch a put using the given key.
	Delete(TKey)

	// Put adds to the batch a put using the given key-value.
	Put(k TKey, v []byte)

	// Commits a batch of operations and closes the write batch.
	Commit() error
}

func getNextInstance(db OrderedKeyValueGetter, curID dvid.InstanceID) (nextID dvid.InstanceID, finished bool, err error) {
	begKey := constructDataKey(curID+1, 0, 0, minTKey)
	endKey := constructDataKey(dvid.MaxInstanceID, dvid.MaxVersionID, dvid.MaxClientID, maxTKey)

	ch := make(chan *KeyValue)
	cancel := make(chan struct{})

	// Process each key received by range query.
	ctx := DataContext{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			kv := <-ch
			if kv == nil || kv.K == nil {
				finished = true
				return
			}
			nextID, err = ctx.InstanceFromKey(kv.K)
			if err != nil {
				cancel <- struct{}{}
				return
			}
			if nextID != curID {
				cancel <- struct{}{}
				return
			}
		}
	}()

	keysOnly := true
	if queryErr := db.RawRangeQuery(begKey, endKey, keysOnly, ch, cancel); queryErr != nil {
		err = queryErr
	}
	wg.Wait()
	return
}

func getInstanceSizes(sv SizeViewer, instances []dvid.InstanceID) (map[dvid.InstanceID]uint64, error) {
	ranges := make([]KeyRange, len(instances))
	for i, curID := range instances {
		beg := constructDataKey(curID, 0, 0, minTKey)
		end := constructDataKey(curID+1, 0, 0, minTKey)
		ranges[i] = KeyRange{Start: beg, OpenEnd: end}
	}
	s, err := sv.GetApproximateSizes(ranges)
	if err != nil {
		return nil, err
	}
	if len(s) != len(instances) {
		return nil, fmt.Errorf("only got back %d instance sizes, not the requested %d instances", len(s), len(instances))
	}
	sizes := make(map[dvid.InstanceID]uint64, len(instances))
	for i, curID := range instances {
		sizes[curID] = s[i]
	}
	return sizes, nil
}

func getKeyUsage(vw KeyUsageViewer, instances []dvid.InstanceID) (map[dvid.InstanceID]map[int]int, error) {
	ranges := make([]KeyRange, len(instances))
	for i, curID := range instances {
		beg := constructDataKey(curID, 0, 0, minTKey)
		end := constructDataKey(curID+1, 0, 0, minTKey)
		ranges[i] = KeyRange{Start: beg, OpenEnd: end}
	}
	s, err := vw.GetKeyUsage(ranges)
	if err != nil {
		return nil, err
	}
	if len(s) != len(instances) {
		return nil, fmt.Errorf("only got back %d instance key usages, not the requested %d instances", len(s), len(instances))
	}
	keyUsage := make(map[dvid.InstanceID]map[int]int, len(instances))
	for i, curID := range instances {
		keyUsage[curID] = s[i]
	}
	return keyUsage, nil
}
