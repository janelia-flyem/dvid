// +build gbucket

package gbucket

/*
Note:
* Range calls are slow require a list query to find potentially matching objects.  After this
call, specific objects can be fetched.
* Lists are eventually consistent and objects are strongly consistent after object post/change.
It is possible to post an object and not see the object when searching the list.
*/

/* Notes on VALUEVERSION refactor

* Some raw queries used for cloning/distribution have been disabled.  Specialized functionality
is probably better for transferring data on the cloud.
* A versioned key/value designated to a versioned address cannot be moved back to the master key

TODO

* Fetch generation id and value in parallel.
* Split gbucket.go into several files.
* Refactor datatype code to use standard key value interface (not bufferable interface).
Throttling is done by gbucket but datatype code can also implement independent throttling.
* Implement functionality to support lowel-level operations.
* Allow datatypes to provid PUT with a likely exists hint.
* Move version encoding to the beginning of the key to potentially accelerate range
queries and distribution.
*/

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/semver"
	"google.golang.org/api/iterator"

	"net/http"

	api "cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in gbucket: %v\n", err)
	}
	e := Engine{"gbucket", "Google's Storage Bucket", ver}
	storage.RegisterEngine(e)
}

const (
	// limit the number of total parallel requests
	MAXCONNECTIONS = 10000

	// limit the number of parallel network ops
	MAXNETOPS = 1000

	// init file for master gbucket
	INITKEY = "initialized"

	// total connection tries
	NUM_TRIES = 3

	// version where multi-bucket was enabled
	// (separate bucket for each repo)
	MULTIBUCKET = 1.1

	// value versioning
	VALUEVERSION = 1.2

	// Repo root bucket name
	REPONAME = "-dvidrepo-"

	// current version of gbucket (must be >1 byte string)
	CURVER = "1.2"

	// first gbucket version (must be >1 byte string)
	ORIGVER = "1.0"

	// stopping value for version encoded in value prefix
	STOPVERSIONPREFIX = dvid.VersionID(0)
)

type GCredentials struct {
	ProjectID string `json:"project_id"`
}

var ErrCondFail = errors.New("gbucket: condition failed")

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) IsDistributed() bool {
	return true
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

// NewStore returns a storage bucket suitable as a general storage engine.
// The passed Config must contain:
// "bucket": name of bucket
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newGBucket(config)
}

// parseConfig initializes GBucket from config
func parseConfig(config dvid.StoreConfig) (*GBucket, error) {
	c := config.GetAll()
	v, found := c["bucket"]
	if !found {
		return nil, fmt.Errorf("%q must be specified for gbucket configuration", "bucket")
	}
	bucket, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%q setting must be a string (%v)", "bucket", v)
	}
	gb := &GBucket{
		bname:          bucket,
		ctx:            context.Background(),
		activeRequests: make(chan chan int, MAXCONNECTIONS),
		activeOps:      make(chan interface{}, MAXNETOPS),
	}
	return gb, nil
}

// newGBucket sets up admin client (bucket must already exist)
func (e *Engine) newGBucket(config dvid.StoreConfig) (*GBucket, bool, error) {
	gb, err := parseConfig(config)
	if err != nil {
		return nil, false, fmt.Errorf("Error in newGBucket() %s\n", err)
	}

	// NewClient uses Application Default Credentials to authenticate.
	credval := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credval == "" {
		gb.client, err = api.NewClient(gb.ctx, option.WithHTTPClient(http.DefaultClient))
	} else {
		// use saved credentials
		gb.client, err = api.NewClient(gb.ctx)

		// grab project_id
		var credconfig GCredentials
		rawcred, err := ioutil.ReadFile(credval)
		if err != nil {
			return nil, false, err
		}
		json.Unmarshal(rawcred, &credconfig)

		gb.projectid = credconfig.ProjectID
	}

	if err != nil {
		return nil, false, err
	}

	// bucket must already exist -- check existence
	gb.bucket = gb.client.Bucket(gb.bname)
	gb.bucket_attrs, err = gb.bucket.Attrs(gb.ctx)
	if err != nil {
		return nil, false, err
	}

	var created bool
	created = false

	val, err := gb.getV(nil, storage.Key(INITKEY))
	var version string
	// check if value exists
	if val == nil {
		created = true

		// conditional put is probably not necessary since
		// all workers should be posting same version
		err = gb.putV(nil, storage.Key(INITKEY), []byte(CURVER))
		if err != nil {
			return nil, false, err
		}
		version = CURVER
	} else if len(val) == 1 {
		// set default version (versionless original wrote out 1 byte)
		version = ORIGVER
	} else {
		version = string(val)
	}

	// save version as float
	if gb.version, err = strconv.ParseFloat(version, 64); err != nil {
		return nil, false, err
	}

	// size of version datatype (this is being kept constant in case the type is changed in dvid, 4 bytes should be sufficient)
	//gb.vsize := unsafe.Sizeof(ctx.VersionID())
	gb.vsize = 4

	// check version enables repo
	if gb.version >= MULTIBUCKET {
		gb.repo2bucket = make(map[string]*api.BucketHandle)
		// TODO: pre-populate based on custom repo names
	}

	// launch background job handler
	go gb.backgroundRequestHandler()

	// assume if not newly created that data exists
	// 'created' not meaningful if concurrent calls
	return gb, created, nil
}

// cannot Delete bucket from API
func (e Engine) Delete(config dvid.StoreConfig) error {
	return nil
}

type GBucket struct {
	bname          string
	bucket         *api.BucketHandle
	bucket_attrs   *api.BucketAttrs
	activeRequests chan chan int
	activeOps      chan interface{}
	ctx            context.Context
	client         *api.Client
	version        float64
	vsize          int32
	// repo2bucket assumes unique bucket names for now
	// TODO: handle bucket name conflicts; allow renaming of master bucket
	repo2bucket map[string]*api.BucketHandle
	// mutex is needed to lock repo2bucket
	mutex     sync.Mutex
	projectid string
}

// ---- HELPER FUNCTIONS ----

func grabPrefix(key1 storage.Key, key2 storage.Key) string {
	var prefixe storage.Key
	key1m := hex.EncodeToString(key1)
	key2m := hex.EncodeToString(key2)
	for spot := range key1m {
		if key1m[spot] != key2m[spot] {
			break
		}
		prefixe = append(prefixe, key1m[spot])
	}
	return string(prefixe)
}

type KeyArray []storage.Key

func (k KeyArray) Less(i, j int) bool {
	return string(k[i]) < string(k[j])
}

func (k KeyArray) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func (k KeyArray) Len() int {
	return len(k)
}

// backgroundReuqestHandler is a background routine for handling queuing
func (db *GBucket) backgroundRequestHandler() {
	jobid := int(0)
	// get request from queue
	for true {
		request := <-db.activeRequests
		// add to job queue
		db.activeOps <- nil

		// if succesfully added, signal requester
		request <- jobid
		jobid += 1
	}
}

// grabOpResource is a blocking function to grab a resource and returns jobid
func (db *GBucket) grabOpResource() int {
	waitchan := make(chan int, 1)
	db.activeRequests <- waitchan
	jobid := <-waitchan
	return jobid
}

func (db *GBucket) releaseOpResource() {
	<-db.activeOps
}

// bucketHandle retrieves the bucket handle based on the context
func (db *GBucket) bucketHandle(ctx storage.Context) (*api.BucketHandle, error) {
	// lock will cause ~1 second slowness if accessing a new repo
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// grab root uuid
	rootuuid, err := ctx.RepoRoot()
	if err != nil {
		return nil, err
	}
	if string(rootuuid) == "" || db.version < MULTIBUCKET {
		// unversioned context should be sent to master bucket
		return db.bucket, nil
	}

	// create bucket if it does not exist
	if bucket, ok := db.repo2bucket[string(rootuuid)]; !ok {
		// !! assume name not previously created

		// choose initial name
		bucketname := db.bname + REPONAME + string(rootuuid)
		// create bucket handle
		bucket = db.client.Bucket(bucketname)

		// check if bucket exists
		if _, err = bucket.Attrs(db.ctx); err != nil {
			// create bucket and reuse permissions
			// ignore error, assume already created
			bucket.Create(db.ctx, db.projectid, db.bucket_attrs)
		}

		// if name doesn't exist create new bucket handle and load new key value
		db.repo2bucket[string(rootuuid)] = bucket
		return bucket, nil
	} else {
		return bucket, nil
	}
}

func (db *GBucket) String() string {
	return fmt.Sprintf("google cloud storage, bucket %s", db.bname)
}

func (db *GBucket) initValueVersion(version dvid.VersionID, val []byte) []byte {
	val2 := make([]byte, 3*db.vsize, 3*db.vsize+int32(len(val)))
	binary.LittleEndian.PutUint32(val2, uint32(version))
	binary.LittleEndian.PutUint32(val2[db.vsize:2*db.vsize], uint32(version))
	binary.LittleEndian.PutUint32(val2[2*db.vsize:3*db.vsize], 0)

	val2 = append(val2, val...)
	return val2
}

// valuePatch patches the value at the given key with function f using valueversion prefix
// The patching function should work on uninitialized data.
// TODO: accelerate patch conflicts by avoiding unnecessary master reading
func (db *GBucket) valuePatch(ctx storage.Context, tk storage.TKey, f storage.PatchFunc) error {
	var bucket *api.BucketHandle
	var err error
	if bucket, err = db.bucketHandle(ctx); err != nil {
		return err
	}
	basekey := ctx.ConstructKeyVersion(tk, dvid.VersionID(0))

	initval := false

	// loop until success (similar to POST loop but adds patching code and offval write protection)
	for true {
		var genid int64
		offgenid := int64(0)
		var offval []byte

		// if the value was already inited (even by accident) -- grab id
		if initval {
			_, offgenid, err = db.getValueGen(ctx, ctx.ConstructKey(tk))
			if err != nil {
				return err
			}
		}

		// perform genid get
		baseval, genid, err := db.getValueGen(ctx, basekey)
		if err != nil {
			return err
		}

		// check if the value already likely exists and
		// fetch that value with a genid
		if baseval != nil {
			baseversionid := db.baseVersion(baseval)
			relevantver := dvid.VersionID(0)

			// find most relevant key
			allkeys := db.extractPrefixKeys(ctx, tk, baseval)
			tkvs := []*storage.KeyValue{}
			for _, key := range allkeys {
				tkvs = append(tkvs, &storage.KeyValue{key, []byte{}})
			}
			kv, _ := ctx.(storage.VersionedCtx).VersionedKeyValue(tkvs)
			if kv != nil {
				// strip version
				relevantver, _ = ctx.(storage.VersionedCtx).VersionFromKey(kv.K)
			}

			// perform another get if another version is more relevant
			if baseversionid != relevantver {
				var offgenid2 int64
				offval, offgenid2, err = db.getValueGen(ctx, ctx.ConstructKeyVersion(tk, relevantver))
				if err != nil {
					return err
				}

				// if current version is what was fetched, set the genid
				if relevantver == ctx.VersionID() {
					offgenid = offgenid2
				}
			} else {
				offval = db.sliceDataVersionPrefix(baseval)
			}
		}

		// treat it as a post of offval data
		baseval, offval, offversion, err := db.updateValueVersion(ctx, baseval, offval)

		// set to nil in case these values are len 0 slices
		if len(offval) == 0 {
			offval = nil
		}
		if len(baseval) == 0 {
			baseval = nil
		}

		// offval or baseval must be modified
		if offversion == ctx.VersionID() {
			// apply patch to val (offval could be empty)
			offval, err = f(offval)
			if err != nil {
				return err
			}
		} else {
			baseprefix, basevalripped := db.extractValueVersion(baseval)
			// set to nil if there is no value since that is expected by patch function
			if len(basevalripped) == 0 {
				basevalripped = nil

			}
			basevalripped, err = f(basevalripped)
			if err != nil {
				return err
			}
			baseval = append(baseprefix, basevalripped...)
		}

		// post off value
		if offval != nil {
			obj_handle := bucket.Object(hex.EncodeToString(ctx.ConstructKey(tk)))
			// set condition if offdata is patch
			if offversion == ctx.VersionID() {
				var conditions api.Conditions
				conditions.GenerationMatch = offgenid
				if conditions.GenerationMatch == 0 {
					conditions.DoesNotExist = true
				}
				obj_handle = obj_handle.If(conditions)

				// note that value already exists
				initval = true
			}
			err = db.putVhandle(obj_handle, offval)
			if err == ErrCondFail {
				continue
			} else if err != nil {
				return err
			}

		}

		// post main value
		if baseval != nil {
			obj_handle := bucket.Object(hex.EncodeToString(basekey))
			var conditions api.Conditions
			conditions.GenerationMatch = genid
			if conditions.GenerationMatch == 0 {
				conditions.DoesNotExist = true
			}
			obj_handle = obj_handle.If(conditions)

			err = db.putVhandle(obj_handle, baseval)
			if err == nil {
				break
			} else if err != ErrCondFail {
				return err
			}

		} else {
			break
		}
	}

	return err
}

// updateValueVersion updates the base value and provides an off value and relevant version
// Note: returned basevil will be nil if no change; off value will be nil if overwritten
func (db *GBucket) updateValueVersion(ctx storage.Context, baseval []byte, newval []byte) ([]byte, []byte, dvid.VersionID, error) {
	ismasterhead := ctx.(storage.VersionedCtx).Head()

	// grab current head prefix
	currentmaster := dvid.VersionID(0)

	if baseval != nil {
		currentmaster = db.baseVersion(baseval)
	}

	// check if master
	ismaster := false
	if currentmaster != 0 {
		ismaster = ctx.(storage.VersionedCtx).MasterVersion(currentmaster)
	}

	// check if new data
	if baseval == nil {
		// newval will also be nil
		basevalmod := db.initValueVersion(ctx.VersionID(), newval)
		return basevalmod, nil, 0, nil
	} else if ctx.VersionID() == currentmaster {
		// overwrite if same version
		// replace old value with new value
		exists, hastombstone := db.hasVersion(baseval, ctx.VersionID())
		baseprefix, _ := db.extractValueVersion(baseval)

		if !exists {
			// add key if deleted before
			baseprefix = db.appendValueVersion(baseprefix, currentmaster)
		} else if hastombstone {
			// remove tombstone
			db.unDeleteValueVersion(baseprefix, currentmaster)
		}

		// concatentate data to value prefix
		basevalmod := baseprefix
		basevalmod = append(baseprefix, newval...)

		// return mod base val
		return basevalmod, nil, 0, nil
	} else if exists, hastombstone := db.hasVersion(baseval, ctx.VersionID()); exists {
		// Data already exists outside of base
		var basevalmod []byte

		if hastombstone {
			// if tombstone, untombstone and load new baseval
			db.unDeleteValueVersion(baseval, ctx.VersionID())
			basevalmod = baseval
		}
		// return basevalmod (might be nil) and off version
		return basevalmod, newval, ctx.VersionID(), nil
	} else if ismasterhead || !ismaster {
		// Eviction if new data has higher priority
		// create new prefix and data
		baseprefix, evictvalmod := db.extractValueVersion(baseval)
		baseprefix = db.appendValueVersion(baseprefix, ctx.VersionID())
		db.replaceMaster(baseprefix, ctx.VersionID())
		basevalmod := append(baseprefix, newval...)

		// return new base data and evicted previous master
		return basevalmod, evictvalmod, currentmaster, nil
	}
	// else create a new off critical value version

	// add version to base header (mod base val)
	baseprefix, baseval := db.extractValueVersion(baseval)
	baseprefix = db.appendValueVersion(baseprefix, ctx.VersionID())
	basevalmod := append(baseprefix, baseval...)

	// return new val with version id
	return basevalmod, newval, ctx.VersionID(), nil

}

// Extracts the base version encoded in the valeu
func (db *GBucket) baseVersion(val []byte) dvid.VersionID {
	return dvid.VersionID(binary.LittleEndian.Uint32(val[0:db.vsize]))
}

// append version to supplied prefix
func (db *GBucket) appendValueVersion(val []byte, version dvid.VersionID) []byte {
	tempintbuffer := make([]byte, db.vsize)
	newprefix := make([]byte, 0, len(val)+int(db.vsize))
	binary.LittleEndian.PutUint32(tempintbuffer, uint32(version))
	newprefix = append(newprefix, val[0:len(val)-int(db.vsize)]...)
	newprefix = append(newprefix, tempintbuffer...)
	newprefix = append(newprefix, val[len(val)-int(db.vsize):len(val)]...)

	return newprefix
}

// extractValueVersion extracts and copies the header from the value and returns slice of the value
func (db *GBucket) extractValueVersion(val []byte) ([]byte, []byte) {
	currpos := db.vsize
	for ; currpos < int32(len(val)); currpos += db.vsize {
		currval := binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize])
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			currpos += db.vsize
			break
		}
	}
	valueversion_header := make([]byte, 0, currpos)
	valueversion_header = append(valueversion_header, val[0:currpos]...)

	return valueversion_header, val[currpos:len(val)]
}

// replaceMaster replace master with provided version (in-place modification of array)
func (db *GBucket) replaceMaster(val []byte, version dvid.VersionID) {
	tempintbuffer := make([]byte, db.vsize)
	binary.LittleEndian.PutUint32(tempintbuffer, uint32(version))
	//  overwrite master
	for i := int32(0); i < db.vsize; i++ {
		val[i] = tempintbuffer[i]
	}
}

// unDeleteValueVersion removes tombstone if it exists
func (db *GBucket) unDeleteValueVersion(val []byte, version dvid.VersionID) {
	for currpos := db.vsize; currpos < int32(len(val)); currpos += db.vsize {
		currval := int32(binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize]))
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			break
		}

		if currval < 0 {
			currval *= -1
		}
		if dvid.VersionID(currval) == version {
			tempintbuffer := make([]byte, db.vsize)
			binary.LittleEndian.PutUint32(tempintbuffer, uint32(currval))
			//  overwrite tombstone
			for i := int32(0); i < db.vsize; i++ {
				val[currpos+i] = tempintbuffer[i]
			}
			break
		}
	}
}

// onlyBaseVersion returns true if only version in the value version is the master
func (db *GBucket) onlyBaseVersion(val []byte) bool {
	masterval := int32(binary.LittleEndian.Uint32(val[0:db.vsize]))
	currpos := db.vsize
	for ; currpos < int32(len(val)); currpos += db.vsize {
		currval := int32(binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize]))
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			break
		}
		if currval < 0 {
			currval *= -1
		}
		if currval != masterval {
			return false
		}
	}

	return true
}

// hasVersion indicates whether version exists and whether it is tombstoned
func (db *GBucket) hasVersion(val []byte, version dvid.VersionID) (bool, bool) {
	currpos := db.vsize
	exists := false
	hastombstone := false
	for ; currpos < int32(len(val)); currpos += db.vsize {
		currval := int32(binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize]))
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			break
		}

		temptombstone := false
		if currval < 0 {
			currval *= -1
			temptombstone = true
		}
		if dvid.VersionID(currval) == version {
			hastombstone = temptombstone
			exists = true
			break
		}
	}

	return exists, hastombstone
}

func (db *GBucket) sliceDataVersionPrefix(val []byte) []byte {
	currpos := db.vsize
	for ; currpos < int32(len(val)); currpos += db.vsize {
		currval := dvid.VersionID(binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize]))
		if currval == STOPVERSIONPREFIX {
			currpos += db.vsize
			break
		}
	}
	return val[currpos:len(val)]
}

// Retrieve value at base address and perform another get if data is not there
// Note: for now, assume writes will guarantee existence of non-master key
func (db *GBucket) versionedgetV(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	var bucket *api.BucketHandle
	var err error
	var val []byte

	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return nil, err
		}
	}

	// get base key
	basekey := ctx.ConstructKeyVersion(tk, 0)

	// gets handle (no network op)
	obj_handle := bucket.Object(hex.EncodeToString(basekey))
	val, err = db.getVhandle(obj_handle)

	// preserve interface where missing value is not an error
	if err == api.ErrObjectNotExist {
		return nil, nil
	}

	// grab the most relevant version
	relevantver := dvid.VersionID(0)

	allkeys := db.extractPrefixKeys(ctx, tk, val)
	// find most relevant key
	tkvs := []*storage.KeyValue{}
	for _, key := range allkeys {
		tkvs = append(tkvs, &storage.KeyValue{key, []byte{}})
	}
	kv, _ := ctx.(storage.VersionedCtx).VersionedKeyValue(tkvs)
	if kv != nil {
		// strip version
		relevantver, _ = ctx.(storage.VersionedCtx).VersionFromKey(kv.K)
	}

	// check if the current base version is the most relevant version
	basever := db.baseVersion(val)
	if relevantver == basever && basever != 0 {
		// strip header from value
		val = db.sliceDataVersionPrefix(val)
		// empty base value
		if len(val) == 0 {
			val = nil
		}
	} else if relevantver > 0 {
		// TODO: if a non-head node, the value should probably always exist
		// so could continue to poll
		// if match exists but not base, do tradional get
		// (if it doesn't exist here, should be from legitimate delete)
		speckey := ctx.ConstructKeyVersion(tk, relevantver)
		val, err = db.getV(ctx, speckey)
	} else {
		// else return blank as per default
		val = nil
	}

	return val, err
}

// get retrieves a value from a given key or an error if nothing exists
func (db *GBucket) getV(ctx storage.Context, k storage.Key) ([]byte, error) {
	var bucket *api.BucketHandle
	var err error

	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return nil, err
		}
	}

	// gets handle (no network op)
	obj_handle := bucket.Object(hex.EncodeToString(k))
	val, err := db.getVhandle(obj_handle)

	// preserve interface where missing value is not an error
	if err == api.ErrObjectNotExist {
		return nil, nil
	}

	if db.version >= VALUEVERSION && ctx.Versioned() {
		currver, _ := ctx.(storage.VersionedCtx).VersionFromKey(k)
		if val != nil && int32(currver) == 0 {
			val = db.sliceDataVersionPrefix(val)
			// empty base value
			if len(val) == 0 {
				val = nil
			}
		}
	}

	return val, err
}

// getVhandle retrieves a value from a given key or an error if nothing exists
func (db *GBucket) getVhandle(obj_handle *api.ObjectHandle) ([]byte, error) {
	var err error
	for i := 0; i < NUM_TRIES; i++ {
		// returns error if it doesn't exist
		obj, err2 := obj_handle.NewReader(db.ctx)

		// if object not found return err
		if err2 == api.ErrObjectNotExist {
			return nil, err2
		}

		// no error, read value and return
		if err2 == nil {
			value, err2 := ioutil.ReadAll(obj)
			return value, err2
		}

		// keep trying if there is another error
		err = err2
	}
	return nil, err
}

// put value from a given key or an error if nothing exists
func (db *GBucket) deleteV(ctx storage.Context, k storage.Key) error {
	// retrieve repo bucket
	var bucket *api.BucketHandle
	var err error
	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return err
		}
	}

	// gets handle (no network op)
	obj_handle := bucket.Object(hex.EncodeToString(k))

	return obj_handle.Delete(db.ctx)
}

// put value from a given key or an error if nothing exists
func (db *GBucket) putV(ctx storage.Context, k storage.Key, value []byte) (err error) {
	// retrieve repo bucket
	var bucket *api.BucketHandle
	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return err
		}
	}

	// gets handle (no network op)
	obj_handle := bucket.Object(hex.EncodeToString(k))
	return db.putVhandle(obj_handle, value)

}

// putVTKey puts data using valueversion
/* Note: currently requires 5 network op latency in worst case with no conflicts
(1 op if first version).
    1 speculateive put if empty -- TODO provide hint that data is not-empty
    if put failed
	-2 ops (fetch generation id and then basevalue) -- TODO should be one op
	-1 op write evicted/non-base value (if necessary), run before basevalue update unless
	overwriting old data -- TODO paralelize with basevalue update if evicted data is from
	a locked node, this requires the GET routine to be slightly smarter
	-1 op update basevalue (if a change exist for current keys)
*/
func (db *GBucket) putVTKey(ctx storage.Context, tk storage.TKey, value []byte) error {
	// retrieve repo bucket
	var bucket *api.BucketHandle
	var err error
	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return err
		}
	}

	// try a speculative write
	basekey := ctx.ConstructKeyVersion(tk, dvid.VersionID(0))
	obj_handle := bucket.Object(hex.EncodeToString(basekey))
	var conditions api.Conditions
	conditions.DoesNotExist = true
	obj_handle = obj_handle.If(conditions)

	// add versionid, versionid, 0 to val
	value2 := db.initValueVersion(ctx.VersionID(), value)

	err = db.putVhandle(obj_handle, value2)
	if err != ErrCondFail {
		return err
	}

	for true {
		// retrieve current valueversion (need entire value to re-post data)
		baseval, genid, err := db.getValueGen(ctx, basekey)
		if err != nil {
			return err
		}
		offversion := dvid.VersionID(0)
		var offval []byte

		// modify the valueversion header and return data to be posted as necessary
		baseval, offval, offversion, err = db.updateValueVersion(ctx, baseval, value)
		if err != nil {
			return err
		}

		// either old data (if new version data has been posted) or
		// new data if old data is master and new data is not
		if offval != nil {
			// evicted data should not exist beforehand in most cases
			obj_handle := bucket.Object(hex.EncodeToString(ctx.ConstructKeyVersion(tk, offversion)))
			if err := db.putVhandle(obj_handle, offval); err != nil {
				return err
			}
		}

		// post base data if overwritten, caused eviction, or if there is a prefix change
		// this is only nil if non-master data is directly posting to already existing data
		if baseval != nil {
			obj_handle := bucket.Object(hex.EncodeToString(basekey))
			var conditions api.Conditions
			conditions.GenerationMatch = genid
			obj_handle_temp := obj_handle.If(conditions)
			err = db.putVhandle(obj_handle_temp, baseval)
			if err == nil {
				break
			} else if err != ErrCondFail {
				return err
			}
		} else {
			break
		}
	}

	return nil
}

// put value from a given handle or an error if nothing exists
func (db *GBucket) putVhandle(obj_handle *api.ObjectHandle, value []byte) (err error) {
	// gets handle (no network op)
	for i := 0; i < NUM_TRIES; i++ {
		// returns error if it doesn't exist
		obj := obj_handle.NewWriter(db.ctx)

		// write data to buffer
		numwrite, err2 := obj.Write(value)

		// close will flush buffer
		err = obj.Close()

		if err != nil && strings.Contains(err.Error(), "conditionNotMet") {
			// failure to write due to generation id mismatch
			return ErrCondFail
		}

		// other errors should cause a restart (is this handled properly in the api)
		if err != nil || err2 != nil || numwrite != len(value) {
			err = fmt.Errorf("Error writing object to google bucket")
			time.Sleep(time.Duration(i+1) * time.Second)
		} else {
			break
		}
	}

	return err
}

// extractVers finds all the versions stored in the provide base key value and returns prefix
func (db *GBucket) extractVers(ctx storage.Context, baseKey storage.Key) ([]storage.Key, []byte, error) {
	var bucket *api.BucketHandle
	var err error

	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return nil, nil, err
		}
	}

	// gets handle (no network op)
	obj_handle := bucket.Object(hex.EncodeToString(baseKey))
	var value []byte

	for i := 0; i < NUM_TRIES; i++ {
		// fetch a little more data than the max prefix size in case a new node is opened
		// Note: technically someone could rapidly open a lot of nodes and do concurrent
		// writes that cause evictions that could technically lead to an inconsistent view of data
		offset := int32(ctx.(storage.VersionedCtx).NumVersions()+2)*db.vsize + (db.vsize)

		// returns error if it doesn't exist
		obj, err2 := obj_handle.NewRangeReader(db.ctx, 0, int64(offset))

		// if object not found return err
		if err2 == api.ErrObjectNotExist {
			return nil, nil, err2
		}

		// no error, read value and return
		if err2 == nil {
			value, err2 = ioutil.ReadAll(obj)
			if err2 != nil {
				return nil, nil, err2
			}
		}

		// keep trying if there is another error
		err = err2
	}

	if err != nil {
		return nil, nil, err
	}

	// process value retrieve and extract keys
	tk, _ := storage.TKeyFromKey(baseKey)
	vkeys := db.extractPrefixKeys(ctx, tk, value)

	return vkeys, value, nil
}

// extract keys from value prefix (assume first value is the head and always exists)
func (db *GBucket) extractPrefixKeys(ctx storage.Context, tk storage.TKey, val []byte) []storage.Key {
	vkeys := make([]storage.Key, 0)
	foundend := false

	// read until STOPVERSIONPREFIX or finished byte array
	for i := db.vsize; i < int32(len(val)); i += db.vsize {
		currval := int32(binary.LittleEndian.Uint32(val[i : i+db.vsize]))
		// stop reading if at end of prefix
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			foundend = true
			break
		}

		// deleted values are actually converted to signed ints and should make tombstone
		if currval > 0 {
			vkeys = append(vkeys, ctx.(storage.VersionedCtx).ConstructKeyVersion(tk, dvid.VersionID(currval)))
		} else {
			currval *= -1
			vkeys = append(vkeys, ctx.(storage.VersionedCtx).TombstoneKeyVersion(tk, dvid.VersionID(currval)))
		}
	}

	// should always find the end of the prefix
	if !foundend {
		dvid.Criticalf("End of valueversion prefix not found")
	}

	return vkeys
}

// getKeysInRangeRaw returns all keys in a range (including multiple keys and tombstones)
func (db *GBucket) getKeysInRangeRaw(ctx storage.Context, minKey, maxKey storage.Key) ([]storage.Key, error) {
	var bucket *api.BucketHandle
	var err error

	if ctx == nil {
		// use default buckert
		bucket = db.bucket
	} else {
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return nil, err
		}
	}

	keys := make(KeyArray, 0)
	// extract common prefix
	prefix := grabPrefix(minKey, maxKey)

	query := &api.Query{Prefix: prefix}
	// query objects
	object_list := bucket.Objects(db.ctx, query)
	// filter keys that fall into range
	object_attr, done := object_list.Next()
	for done != iterator.Done {
		decstr, err := hex.DecodeString(object_attr.Name)
		if err != nil {
			return nil, err
		}
		if bytes.Compare(decstr, minKey) >= 0 && bytes.Compare(decstr, maxKey) <= 0 {
			keys = append(keys, decstr)
		}
		object_attr, done = object_list.Next()
	}

	// sort keys
	sort.Sort(keys)

	return []storage.Key(keys), nil
}

// getKeysInRange returns all the latest keys in a range (versioned or unversioned)
func (db *GBucket) getKeysInRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, hasresource bool) ([]storage.Key, error) {
	var minKey storage.Key
	var maxKey storage.Key
	var err error

	// extract min and max key
	if !ctx.Versioned() {
		minKey = ctx.ConstructKey(TkBeg)
		maxKey = ctx.ConstructKey(TkEnd)
	} else {

		if db.version >= VALUEVERSION {
			minKey = ctx.ConstructKeyVersion(TkBeg, dvid.VersionID(0))
			maxKey = ctx.ConstructKeyVersion(TkEnd, dvid.VersionID(0))
		} else {
			minKey, err = ctx.(storage.VersionedCtx).MinVersionKey(TkBeg)
			if err != nil {
				return nil, err
			}
			maxKey, err = ctx.(storage.VersionedCtx).MaxVersionKey(TkEnd)
			if err != nil {
				return nil, err
			}
		}
	}

	keys, _ := db.getKeysInRangeRaw(ctx, minKey, maxKey)
	if !ctx.Versioned() {
		return keys, nil
	}
	// grab latest version if versioned
	vctx, ok := ctx.(storage.VersionedCtx)
	if !ok {
		return nil, fmt.Errorf("Bad Get(): context is versioned but doesn't fulfill interface: %v", ctx)
	}

	if db.version >= VALUEVERSION {
		matchingkeys := make([]storage.Key, 0)

		if hasresource {
			db.releaseOpResource()
		}
		var wg sync.WaitGroup
		// grab actual master keys
		for _, key := range keys {
			if ver, _ := vctx.VersionFromKey(key); ver == 0 {
				tk, _ := storage.TKeyFromKey(key)
				// fetch keys in parallel
				wg.Add(1)
				go func(baseKey storage.Key) {
					db.grabOpResource()
					defer func() {
						wg.Done()
						db.releaseOpResource()
					}()

					vkeys, prefix, _ := db.extractVers(ctx, baseKey)
					//if err != nil {
					//	return nil, err
					//}

					// extract relevant version
					tkvs := []*storage.KeyValue{}
					for _, key := range vkeys {
						tkvs = append(tkvs, &storage.KeyValue{key, []byte{}})
					}
					kv, _ := vctx.VersionedKeyValue(tkvs)

					if kv != nil {
						// check if base
						relevantver, _ := vctx.VersionFromKey(kv.K)
						baseversionid := db.baseVersion(prefix)
						if baseversionid == relevantver {
							matchingkeys = append(matchingkeys, ctx.ConstructKeyVersion(tk, 0))
						} else {
							matchingkeys = append(matchingkeys, kv.K)
						}
					}
				}(key)

			}
		}
		wg.Wait()
		if hasresource {
			db.grabOpResource()
		}

		sort.Sort(KeyArray(matchingkeys))
		return matchingkeys, nil
	} else {
		vkeys := make([]storage.Key, 0)
		versionkeymap := make(map[string][]storage.Key)
		for _, key := range keys {
			tk, _ := storage.TKeyFromKey(key)
			versionkeymap[string(tk)] = append(versionkeymap[string(tk)], key)
		}

		for _, keylist := range versionkeymap {
			tkvs := []*storage.KeyValue{}
			for _, key := range keylist {
				tkvs = append(tkvs, &storage.KeyValue{key, []byte{}})
			}

			// extract relevant version
			kv, _ := vctx.VersionedKeyValue(tkvs)

			if kv != nil {
				vkeys = append(vkeys, kv.K)
			}
		}
		keys = vkeys

		sort.Sort(KeyArray(keys))

		return keys, nil
	}
}

// getVTKey extract a value for the tkey given the context
func (db *GBucket) getVTKey(ctx storage.Context, tk storage.TKey, hasresource bool) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call Get() on nil GBucket")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in Get()")
	}
	var val []byte

	if db.version < VALUEVERSION || !ctx.Versioned() {
		var currkey storage.Key
		if ctx.Versioned() {
			data := ctx.(storage.VersionedCtx).Data()
			rootversion, _ := data.RootVersionID()
			contextversion := ctx.VersionID()

			if !data.Versioned() {
				// construct key from root
				newctx := storage.NewDataContext(data, rootversion)
				currkey = newctx.ConstructKey(tk)

			} else {
				if rootversion != contextversion {
					// grab all keys within versioned range
					keys, _ := db.getKeysInRange(ctx, tk, tk, hasresource)
					if len(keys) == 0 {
						return nil, nil
					}
					currkey = keys[0]
				} else {
					// construct key from root
					newctx := storage.NewDataContext(data, rootversion)
					currkey = newctx.ConstructKey(tk)
				}
			}

		} else {
			currkey = ctx.ConstructKey(tk)
		}
		// retrieve value, if error, return as not found
		val, _ = db.getV(ctx, currkey)
	} else {
		// implement double get
		val, _ = db.versionedgetV(ctx, tk)
	}

	return val, nil
}

type keyvalue_t struct {
	key   storage.Key
	value []byte
}

// retrieveKey finds the latest relevant version for the given Tkey.
// This runs slowly if the request is not at the root version.
func (db *GBucket) retrieveKey(ctx storage.Context, tk storage.TKey, hasresource bool) (storage.Key, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in Get()")
	}

	var currkey storage.Key
	if ctx.Versioned() {
		data := ctx.(storage.VersionedCtx).Data()
		rootversion, _ := data.RootVersionID()
		contextversion := ctx.VersionID()

		if !data.Versioned() {
			// construct key from root
			newctx := storage.NewDataContext(data, rootversion)
			currkey = newctx.ConstructKey(tk)

		} else {
			if rootversion != contextversion {
				// grab all keys within versioned range
				keys, _ := db.getKeysInRange(ctx, tk, tk, hasresource)
				if len(keys) == 0 {
					return nil, nil
				}
				currkey = keys[0]
			} else {
				// construct key from root
				newctx := storage.NewDataContext(data, rootversion)
				currkey = newctx.ConstructKey(tk)
			}
		}

	} else {
		currkey = ctx.ConstructKey(tk)
	}

	return currkey, nil
}

// getValue gen retrieves the value and the generation id for the given key
// TODO: fetch generation id and the same time
func (db *GBucket) getValueGen(ctx storage.Context, basekey storage.Key) ([]byte, int64, error) {
	var val []byte
	generationid := int64(0)
	var err error

	// get object
	var bucket *api.BucketHandle
	if bucket, err = db.bucketHandle(ctx); err != nil {
		return val, generationid, err
	}
	obj_handle := bucket.Object(hex.EncodeToString(basekey))

	for true {
		// grab generation id
		attrs, err := obj_handle.Attrs(db.ctx)
		// don't return error if does not exist
		if err == api.ErrObjectNotExist {
			return val, generationid, nil
		}
		if err != nil {
			// possible that object does not exist
			return val, generationid, err
		}

		generationid = attrs.Generation
		obj_handletemp := obj_handle.Generation(generationid)

		val, err = db.getVhandle(obj_handletemp)
		if err == nil || err != ErrCondFail {
			// refetch if generation id differs or value deleted
			break
		}
	}

	return val, generationid, err
}

// deleteHeaderVersion removes or tombstones the provided key from the value and indicates whether the specific version needs deletion
// NOTE: it is currently impossible to self delete completely, but value will be removed
func (db *GBucket) deleteHeaderVersion(ctx storage.Context, val []byte, version dvid.VersionID, useTombstone bool) ([]byte, bool) {
	masterver := dvid.VersionID(binary.LittleEndian.Uint32(val[0:db.vsize]))
	deletemaster := false

	// create a new buffer to store
	buffersize := int32(len(val))
	if masterver == version {
		deletemaster = true
		if buffersize > int32((int32(ctx.(storage.VersionedCtx).NumVersions())+int32(2))*db.vsize) {
			buffersize = int32(int32(ctx.(storage.VersionedCtx).NumVersions())+2) * db.vsize
		}
	}
	newbuffer := make([]byte, 0, buffersize)

	// copyheader
	if deletemaster && !useTombstone {
		// make ownerless if no tombstone and head deleted
		tempintbuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(tempintbuffer, 0)
		newbuffer = append(newbuffer, tempintbuffer...)
	} else {
		// retain owner
		newbuffer = append(newbuffer, val[0:db.vsize]...)
	}

	currpos := db.vsize
	foundversion := false

	for ; currpos < int32(len(val)); currpos += db.vsize {
		currval := int32(binary.LittleEndian.Uint32(val[currpos : currpos+db.vsize]))

		// stop reading if at end of prefix
		if dvid.VersionID(currval) == STOPVERSIONPREFIX {
			// version not found but tombstone should still be added
			// (technically not necessary if no previous version is an ancestor)
			if !foundversion && useTombstone {
				tempintbuffer := make([]byte, 4)
				delver := int32(-1 * int32(version))
				binary.LittleEndian.PutUint32(tempintbuffer, uint32(delver))
				newbuffer = append(newbuffer, tempintbuffer...)
			}

			newbuffer = append(newbuffer, val[currpos:currpos+db.vsize]...)
			currpos += db.vsize
			break
		}
		// if it is alreaedy negated then nothing needs to be done
		if currval == int32(version) {
			foundversion = true
			if useTombstone {
				// append negate
				currval *= -1
				tempintbuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(tempintbuffer, uint32(currval))
				newbuffer = append(newbuffer, tempintbuffer...)
			} else {
				continue
			}
		} else {
			// append
			newbuffer = append(newbuffer, val[currpos:currpos+db.vsize]...)
		}
	}

	// copy the rest of the data
	if !deletemaster {
		newbuffer = append(newbuffer, val[currpos:len(val)]...)
	}

	// if a version for deletion was found and it is not the master, a delete will be required
	return newbuffer, foundversion && !deletemaster
}

// deleteVersion provide value version safe deletion for given basekey and version id
func (db *GBucket) deleteVersion(ctx storage.Context, baseKey storage.Key, version dvid.VersionID, useTombstone bool) error {
	/* Note: Don't worry about delete race conditions because if deleletd with tombstone
	either the tombstone will be found or an empty value will be found which is the same.
	If the value is permanently deleted, it probably should have obliterated the version
	and no writes should have been done.  Worst-case behavior is the delete will behave
	like a tombstone when it should have been a complete delete.*/

	// grab handle
	var bucket *api.BucketHandle
	var err error
	if bucket, err = db.bucketHandle(ctx); err != nil {
		return err
	}
	obj_handle := bucket.Object(hex.EncodeToString(baseKey))

	// the loop is necessary for potential conflicting, concurrent operations
	for true {
		// fetch data
		val, genid, err := db.getValueGen(ctx, baseKey)
		if err != nil {
			return err
		}
		if val == nil {
			// no value, nothing to delete
			return nil
		}

		// if no tombstone and only version, do a conditional delete
		if !useTombstone {
			if db.onlyBaseVersion(val) && version == db.baseVersion(val) {
				// transactional delete
				var conditions api.Conditions
				conditions.GenerationMatch = genid
				if conditions.GenerationMatch == 0 {
					conditions.DoesNotExist = true
				}
				obj_handle_temp := obj_handle.If(conditions)

				err = obj_handle_temp.Delete(db.ctx)
				if err != ErrCondFail {
					break
				} else {
					continue
				}
			}
		}

		// modify header for version (add tombstone, remove if no tombstone)
		val, hasexternalchange := db.deleteHeaderVersion(ctx, val, version, useTombstone)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// transaction write val (non-block wait group)
			var conditions api.Conditions
			conditions.GenerationMatch = genid
			if conditions.GenerationMatch == 0 {
				conditions.DoesNotExist = true
			}
			obj_handle_temp := obj_handle.If(conditions)
			// set main function scope error
			err = db.putVhandle(obj_handle_temp, val)
		}()

		// Note: could speculatively delete earlier but this saves network ops and also still need to wait for base value to be written
		// the delete can be done in parallel because in the case of a delete/post
		// if the value does not exist on either value it is a delete
		// potentially change behavior in the future!!
		if hasexternalchange {
			// delete specific key
			tk, _ := storage.TKeyFromKey(baseKey)
			tempkey := ctx.ConstructKeyVersion(tk, version)
			wg.Add(1)
			go func(key storage.Key) {
				defer wg.Done()
				db.rawDelete(ctx, tempkey)
			}(tempkey)
		}

		// wait and check for errors (only repeat if conditional fail
		wg.Wait()
		if err != ErrCondFail {
			break
		}
	}
	return err
}

func (db *GBucket) rawDelete(ctx storage.Context, fullKey storage.Key) error {
	// !! Only called now internally (no protection for value versioning)
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.RawDelete(fullKey)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (db *GBucket) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	db.grabOpResource()
	defer db.releaseOpResource()

	// fetch data
	val, err := db.getVTKey(ctx, tk, true)
	if err != nil {
		return nil, err
	}

	storage.StoreValueBytesRead <- len(val)
	return val, nil
}

// KeysInRange returns a range of type-specific key components spanning (TkBeg, TkEnd).
func (db *GBucket) KeysInRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) ([]storage.TKey, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call KeysInRange() on nil Google bucket")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in KeysInRange()")
	}

	// grab keys
	keys, _ := db.getKeysInRange(ctx, TkBeg, TkEnd, false)
	tKeys := make([]storage.TKey, 0)

	// grab only object names within range
	for _, key := range keys {
		tk, err := storage.TKeyFromKey(key)
		if err != nil {
			return nil, err
		}
		tKeys = append(tKeys, tk)
	}

	return tKeys, nil
}

// SendKeysInRange sends a range of full keys down a key channel.
func (db *GBucket) SendKeysInRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, ch storage.KeyChan) error {
	dvid.Criticalf("GBucket does not support SnedKeysInRange")
	return nil
}

// GetRange returns a range of values spanning (TkBeg, kEnd) keys.
func (db *GBucket) GetRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) ([]*storage.TKeyValue, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetRange() on nil GBucket")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in GetRange()")
	}

	values := make([]*storage.TKeyValue, 0)

	// grab keys
	keys, _ := db.getKeysInRange(ctx, TkBeg, TkEnd, false)

	keyvalchan := make(chan keyvalue_t, len(keys))
	for _, key := range keys {
		go func(lkey storage.Key) {
			// check if base call then rip out prefix
			value, err := db.getV(ctx, lkey)
			if value == nil || err != nil {
				keyvalchan <- keyvalue_t{lkey, nil}
			} else {
				keyvalchan <- keyvalue_t{lkey, value}
			}
		}(key)
	}

	kvmap := make(map[string][]byte)
	for range keys {
		keyval := <-keyvalchan
		kvmap[string(keyval.key)] = keyval.value
	}

	var err error
	// return keyvalues
	for _, key := range keys {
		val := kvmap[string(key)]
		tk, err := storage.TKeyFromKey(storage.Key(key))
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, fmt.Errorf("Could not retrieve value")
		}

		tkv := storage.TKeyValue{tk, val}
		values = append(values, &tkv)
	}

	return values, err
}

// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
// allowing chunk processing to be concurrent with key-value sequential reads.
// Since the chunks are typically sent during sequential read iteration, the
// receiving function can be organized as a pool of chunk handling goroutines.
// See datatype/imageblk.ProcessChunk() for an example.
func (db *GBucket) ProcessRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.ProcessRange(ctx, TkBeg, TkEnd, op, f)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// RawRangeQuery sends a range of full keys [not implemneted).
// This is to be used for low-level data
// retrieval like DVID-to-DVID communication and should not be used by data type
// implementations if possible because each version's key-value pairs are sent
// without filtering by the current version and its ancestor graph.
func (db *GBucket) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error {
	dvid.Criticalf("GBucket does not support RawRangeQuery")
	return nil
}

// Put writes a value with given key in a possibly versioned context.
func (db *GBucket) Put(ctx storage.Context, tkey storage.TKey, value []byte) error {
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.Put(ctx, tkey, value)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (db *GBucket) RawPut(k storage.Key, v []byte) error {
	dvid.Criticalf("GBucket does not support RawPut")
	return nil
}

// Delete deletes a key-value pair so that subsequent Get on the key returns nil.
func (db *GBucket) Delete(ctx storage.Context, tkey storage.TKey) error {
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.Delete(ctx, tkey)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.  This can be used in conjunction with RawRangeQuery.
func (db *GBucket) RawDelete(fullKey storage.Key) error {
	dvid.Criticalf("GBucket does not support RawDelete")
	return nil
}

// ---- TransactionalDB interface ------

// Lockkey tries to write to a new file and if it fails, it keeps trying to query until it is free.
func (db *GBucket) LockKey(k storage.Key) error {
	var err error
	currdelay := 1
	// conditional put on generation id if it does not exist
	obj_handle_orig := db.bucket.Object(hex.EncodeToString(k))
	var conditions api.Conditions
	conditions.DoesNotExist = true
	obj_handle := obj_handle_orig.If(conditions)
	generationid := int64(0)
	// 30 second lock held limit
	timelimit := float64(30)

	var locktime time.Time
	for true {
		// post blank data
		// try putting againt with a new generation id
		err = db.putVhandle(obj_handle, []byte(""))
		if err != ErrCondFail {
			break
		}
		// check to make sure lock is not stale
		attrs, err := obj_handle_orig.Attrs(db.ctx)
		if err == nil {
			generationid2 := attrs.Generation
			if generationid != generationid2 {
				generationid = generationid2
				locktime = time.Now()
			}
			// delete lock if not released in a reasonable amount of time
			lockheldtime := time.Since(locktime).Seconds()
			if lockheldtime > timelimit {
				var delconditions api.Conditions
				delconditions.GenerationMatch = generationid
				obj_handledel := obj_handle_orig.If(delconditions)
				obj_handledel.Delete(db.ctx)
				continue
			}
		}

		// wait some time and retry
		time.Sleep(time.Duration(currdelay) * time.Second)
	}

	return err
}

// UnlockKey delete the key/value and releases the lock
func (db *GBucket) UnlockKey(k storage.Key) error {
	return db.deleteV(nil, k)
}

// Patch patches the value at the given key with function f.
// The patching function should work on uninitialized data.
func (db *GBucket) Patch(ctx storage.Context, tk storage.TKey, f storage.PatchFunc) error {
	db.grabOpResource()
	defer db.releaseOpResource()

	var err error
	if db.version >= VALUEVERSION && ctx.Versioned() {
		err = db.valuePatch(ctx, tk, f)
	} else {

		key := ctx.ConstructKey(tk)

		var bucket *api.BucketHandle
		if bucket, err = db.bucketHandle(ctx); err != nil {
			return err
		}
		obj_handle := bucket.Object(hex.EncodeToString(key))

		// check current open node version of a value
		// cost: >=1 object meta request,
		// cost: >=1 request for data if object exists
		// cost: >=1 put data
		// Each write will cost at least 3 back-and-forth reqeuests, and
		// a key search for the current versioning approach
		// Note: finding proper version to get can be slow
		for true {
			var val []byte
			generationid := int64(0)
			for true {
				attrs, err := obj_handle.Attrs(db.ctx)

				if err == api.ErrObjectNotExist {
					// fetch from another version
					// !! this should occur at most once
					prevkey, err2 := db.retrieveKey(ctx, tk, true)
					if err2 != nil {
						return err2
					}

					// key must exist and be different
					if prevkey != nil && strings.Compare(string(prevkey), string(key)) != 0 {
						// fetch previous data
						val, err = db.getV(ctx, prevkey)
						if err != nil {
							// this value should exist
							return err
						}
					}

					break
				} else if err == nil {
					generationid = attrs.Generation
					// fetch data if object exists
					obj_handletemp := obj_handle.Generation(generationid)

					val, err = db.getVhandle(obj_handletemp)
					if err != nil {
						// refetch if generation id differs or value deleted
						continue
					}
					break
				} else {
					// if there is an error grabbing meta from an existing object, return
					return err
				}
			}

			// apply patch to val
			val, err = f(val)
			if err != nil {
				return err
			}

			// conditional put on generation id or does not exist
			var conditions api.Conditions
			conditions.GenerationMatch = generationid
			if conditions.GenerationMatch == 0 {
				conditions.DoesNotExist = true
			}
			obj_handletemp := obj_handle.If(conditions)

			// try putting againt with a new generation id
			err = db.putVhandle(obj_handletemp, val)
			if err != ErrCondFail {
				break
			}
		}

	}
	return err
}

// ---- OrderedKeyValueSetter interface ------

// Put key-value pairs.  This is currently executed with parallel requests.
func (db *GBucket) PutRange(ctx storage.Context, kvs []storage.TKeyValue) error {
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.PutRange(ctx, kvs)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// DeleteRange removes all key-value pairs with keys in the given range.
func (db *GBucket) DeleteRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) error {
	// use buffer interface
	buffer := db.NewBuffer(ctx)

	err := buffer.DeleteRange(ctx, TkBeg, TkEnd)

	if err != nil {
		return err
	}

	// commit changes
	err = buffer.Flush()

	return err
}

// DeleteAll removes all key-value pairs for the context.  If allVersions is true,
// then all versions of the data instance are deleted.  Will not produce any tombstones.
func (db *GBucket) DeleteAll(ctx storage.Context, allVersions bool) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteAll() on nil Google bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteAll()")
	}

	var err error
	if allVersions {
		// do batched RAW delete of latest version
		vctx, versioned := ctx.(storage.VersionedCtx)
		var minKey, maxKey storage.Key
		if versioned {
			minTKey := storage.MinTKey(storage.TKeyMinClass)
			maxTKey := storage.MaxTKey(storage.TKeyMaxClass)
			minKey, err = vctx.MinVersionKey(minTKey)
			if err != nil {
				return err
			}
			maxKey, err = vctx.MaxVersionKey(maxTKey)
			if err != nil {
				return err
			}
		} else {
			minKey, maxKey = ctx.KeyRange()
		}

		// fetch all matching keys for context
		keys, _ := db.getKeysInRangeRaw(ctx, minKey, maxKey)

		// wait for all deletes to complete -- batch??
		var wg sync.WaitGroup
		for _, key := range keys {
			wg.Add(1)
			db.grabOpResource()
			go func(lkey storage.Key) {
				defer func() {
					wg.Done()
					db.releaseOpResource()
				}()
				db.rawDelete(ctx, lkey)
			}(key)
		}
		wg.Wait()
	} else {
		vctx, versioned := ctx.(storage.VersionedCtx)
		if !versioned {
			return fmt.Errorf("Can't ask for versioned delete from unversioned context: %s", ctx)
		}
		minTKey := storage.MinTKey(storage.TKeyMinClass)
		maxTKey := storage.MaxTKey(storage.TKeyMaxClass)
		minKey, err := vctx.MinVersionKey(minTKey)
		if err != nil {
			return err
		}
		maxKey, err := vctx.MaxVersionKey(maxTKey)
		if err != nil {
			return err
		}

		// fetch all matching keys for context
		keys, err := db.getKeysInRangeRaw(ctx, minKey, maxKey)

		// wait for all deletes to complete -- batch??
		var wg sync.WaitGroup

		currversion := ctx.VersionID()
		for _, key := range keys {
			// filter keys that are not current version
			keyversion, _ := ctx.(storage.VersionedCtx).VersionFromKey(key)
			if db.version >= VALUEVERSION && ctx.Versioned() {
				// if base version, delete or return key to exact version
				if keyversion == 0 {
					wg.Add(1)
					db.grabOpResource()
					go func(lkey storage.Key) {
						defer func() {
							wg.Done()
							db.releaseOpResource()
						}()
						// delete specific version
						db.deleteVersion(ctx, key, currversion, false)
					}(key)
				}
			} else if keyversion == currversion {
				wg.Add(1)
				db.grabOpResource()
				go func(lkey storage.Key) {
					defer func() {
						wg.Done()
						db.releaseOpResource()
					}()
					db.rawDelete(ctx, lkey)
				}(key)
			}
		}
		wg.Wait()
	}

	return nil
}

func (db *GBucket) Close() {
	if db == nil {
		dvid.Errorf("Can't call Close() on nil Google Bucket")
		return
	}

	if db.client != nil {
		db.client.Close()
	}
}

func (db *GBucket) Equal(c dvid.StoreConfig) bool {
	gb, err := parseConfig(c)
	if err != nil {
		return false
	}
	if db.bname == gb.bname {
		return true
	}
	return false
}

// --- Batcher interface ----

type goBatch struct {
	db  storage.RequestBuffer
	ctx storage.Context
}

// NewBatch returns an implementation that allows batch writes
func (db *GBucket) NewBatch(ctx storage.Context) storage.Batch {
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}
	return &goBatch{db.NewBuffer(ctx), ctx}
}

// --- Batch interface ---

func (batch *goBatch) Delete(tkey storage.TKey) {

	batch.db.Delete(batch.ctx, tkey)
}

func (batch *goBatch) Put(tkey storage.TKey, value []byte) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Put()\n")
		return
	}

	batch.db.Put(batch.ctx, tkey, value)
}

// Commit flushes the buffer
func (batch *goBatch) Commit() error {
	return batch.db.Flush()
}

// --- Buffer interface ----

// opType defines different DB operation types
type opType int

const (
	delOp opType = iota
	delOpIgnoreExists
	delRangeOp
	putOp
	putOpCallback
	getOp
)

// dbOp is element for buffered db operations
type dbOp struct {
	op        opType
	key       storage.Key
	value     []byte
	tkBeg     storage.TKey
	tkEnd     storage.TKey
	chunkop   *storage.ChunkOp
	chunkfunc storage.ChunkFunc
	readychan chan error
}

// goBuffer allow operations to be submitted in parallel
type goBuffer struct {
	db    *GBucket
	ctx   storage.Context
	ops   []dbOp
	mutex sync.Mutex
}

// NewBatch returns an implementation that allows batch writes
func (db *GBucket) NewBuffer(ctx storage.Context) storage.RequestBuffer {
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}
	return &goBuffer{db: db, ctx: ctx}
}

// --- implement RequestBuffer interface ---

// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
// allowing chunk processing to be concurrent with key-value sequential reads.
// Since the chunks are typically sent during sequential read iteration, the
// receiving function can be organized as a pool of chunk handling goroutines.
// See datatype/imageblk.ProcessChunk() for an example.
func (db *goBuffer) ProcessRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if db == nil {
		return fmt.Errorf("Can't call GetRange() on nil GBucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in GetRange()")
	}

	db.mutex.Lock()
	db.ops = append(db.ops, dbOp{getOp, nil, nil, TkBeg, TkEnd, op, f, nil})
	db.mutex.Unlock()

	return nil
}

// ProcessList sends a list of specific key valuues to type-specific chunk handlers,
// allowing chunk processing to be concurrent with key-value sequential reads.
// See datatype/imageblk.ProcessChunk() for an example.
func (db *goBuffer) ProcessList(ctx storage.Context, tkeys []storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if db == nil {
		return fmt.Errorf("Can't call GetRange() on nil GBucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in GetRange()")
	}
	db.mutex.Lock()
	for _, tkey := range tkeys {
		db.ops = append(db.ops, dbOp{getOp, nil, nil, tkey, nil, op, f, nil})
	}
	db.mutex.Unlock()

	return nil
}

// Put writes a value with given key in a possibly versioned context.
func (db *goBuffer) Put(ctx storage.Context, tkey storage.TKey, value []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call Put() on nil Google Bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}

	var err error
	key := ctx.ConstructKey(tkey)
	if !ctx.Versioned() {
		db.mutex.Lock()
		db.ops = append(db.ops, dbOp{op: putOp, key: key, value: value})
		db.mutex.Unlock()
	} else {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Put(): %v", ctx)
		}
		db.mutex.Lock()
		if db.db.version < VALUEVERSION || !ctx.Versioned() {
			// only use tombstones in old versioning
			tombstoneKey := vctx.TombstoneKey(tkey)
			db.ops = append(db.ops, dbOp{op: delOpIgnoreExists, key: tombstoneKey})
		}
		db.ops = append(db.ops, dbOp{op: putOp, key: key, value: value})
		db.mutex.Unlock()
		if err != nil {
			dvid.Criticalf("Error on data put: %v\n", err)
			err = fmt.Errorf("Error on data put: %v", err)
		}
	}

	return err

}

// PutCallback writes a value with given key in a possibly versioned context and notify the callback function.
func (db *goBuffer) PutCallback(ctx storage.Context, tkey storage.TKey, value []byte, ready chan error) error {
	if db == nil {
		return fmt.Errorf("Can't call Put() on nil Google Bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}

	var err error
	key := ctx.ConstructKey(tkey)
	if !ctx.Versioned() {
		db.mutex.Lock()
		db.ops = append(db.ops, dbOp{op: putOpCallback, key: key, value: value, readychan: ready})
		db.mutex.Unlock()
	} else {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Put(): %v", ctx)
		}
		db.mutex.Lock()
		if db.db.version < VALUEVERSION {
			tombstoneKey := vctx.TombstoneKey(tkey)
			db.ops = append(db.ops, dbOp{op: delOpIgnoreExists, key: tombstoneKey})
		}
		db.ops = append(db.ops, dbOp{op: putOpCallback, key: key, value: value, readychan: ready})
		db.mutex.Unlock()
		if err != nil {
			dvid.Criticalf("Error on data put: %v\n", err)
			err = fmt.Errorf("Error on data put: %v", err)
		}
	}

	return err

}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (db *goBuffer) RawPut(k storage.Key, v []byte) error {
	dvid.Criticalf("GBucket does not support RawPut")
	return nil
}

// Delete deletes a key-value pair so that subsequent Get on the key returns nil.
func (db *goBuffer) Delete(ctx storage.Context, tkey storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call Delete on nil LevelDB")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Delete()")
	}

	key := ctx.ConstructKey(tkey)
	if !ctx.Versioned() {
		db.mutex.Lock()
		db.ops = append(db.ops, dbOp{op: delOp, key: key})
		db.mutex.Unlock()
	} else {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return fmt.Errorf("Non-versioned context that says it's versioned received in Delete(): %v", ctx)
		}
		db.mutex.Lock()
		db.ops = append(db.ops, dbOp{op: delOp, key: key})

		// older versions use explicit tombstones
		if db.db.version < VALUEVERSION {
			tombstoneKey := vctx.TombstoneKey(tkey)
			db.ops = append(db.ops, dbOp{op: putOp, key: tombstoneKey, value: dvid.EmptyValue()})
		}
		db.mutex.Unlock()
	}

	return nil
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.  This can be used in conjunction with RawRangeQuery.
// Note: this function should no longer be called externally
func (db *goBuffer) RawDelete(fullKey storage.Key) error {
	//dvid.Criticalf("GBucket does not support RawDelete")
	if db == nil {
		return fmt.Errorf("Can't call RawDelete on nil LevelDB")
	}

	db.mutex.Lock()
	db.ops = append(db.ops, dbOp{op: delOpIgnoreExists, key: fullKey})
	db.mutex.Unlock()
	return nil
}

// Put key-value pairs.  This is currently executed with parallel requests.
func (db *goBuffer) PutRange(ctx storage.Context, kvs []storage.TKeyValue) error {
	if db == nil {
		return fmt.Errorf("Can't call PutRange() on nil Google Bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in PutRange()")
	}

	for _, kv := range kvs {
		db.Put(ctx, kv.K, kv.V)
	}

	return nil
}

// DeleteRange removes all key-value pairs with keys in the given range.
func (db *goBuffer) DeleteRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteRange() on nil Google bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}
	db.mutex.Lock()
	db.ops = append(db.ops, dbOp{delRangeOp, nil, nil, TkBeg, TkEnd, nil, nil, nil})
	db.mutex.Unlock()

	return nil
}

// Flush the buffer
func (buffer *goBuffer) Flush() error {
	retVals := make(chan error, len(buffer.ops))
	// limits the number of simultaneous requests (should this be global)

	for itnum, operation := range buffer.ops {
		opid := buffer.db.grabOpResource()
		go func(opdata dbOp, currnum int) {
			defer func() {
				buffer.db.releaseOpResource()
			}()
			var err error
			if opdata.op == delOp {
				if buffer.db.version >= VALUEVERSION && buffer.ctx.Versioned() {
					tk, _ := storage.TKeyFromKey(opdata.key)
					basekey := buffer.ctx.ConstructKeyVersion(tk, dvid.VersionID(0))
					// always assume tombstone and in current version context
					err = buffer.db.deleteVersion(buffer.ctx, basekey, buffer.ctx.VersionID(), true)
				} else {
					err = buffer.db.deleteV(buffer.ctx, opdata.key)
				}
			} else if opdata.op == delOpIgnoreExists {
				buffer.db.deleteV(buffer.ctx, opdata.key)
			} else if opdata.op == delRangeOp {
				err = buffer.deleteRangeLocalResource(buffer.ctx, opdata.tkBeg, opdata.tkEnd)
			} else if opdata.op == putOp {
				if buffer.db.version >= VALUEVERSION && buffer.ctx.Versioned() {
					tk, _ := storage.TKeyFromKey(opdata.key)
					err = buffer.db.putVTKey(buffer.ctx, tk, opdata.value)
				} else {
					err = buffer.db.putV(buffer.ctx, opdata.key, opdata.value)
				}
				storage.StoreKeyBytesWritten <- len(opdata.key)
				storage.StoreValueBytesWritten <- len(opdata.value)
			} else if opdata.op == putOpCallback {
				if buffer.db.version >= VALUEVERSION && buffer.ctx.Versioned() {
					tk, _ := storage.TKeyFromKey(opdata.key)
					err = buffer.db.putVTKey(buffer.ctx, tk, opdata.value)
				} else {
					err = buffer.db.putV(buffer.ctx, opdata.key, opdata.value)
				}
				storage.StoreKeyBytesWritten <- len(opdata.key)
				storage.StoreValueBytesWritten <- len(opdata.value)
				opdata.readychan <- err
			} else if opdata.op == getOp {
				if opdata.tkEnd == nil {
					err = buffer.processGetLocalResource(buffer.ctx, opdata.tkBeg, opdata.chunkop, opdata.chunkfunc)
				} else {
					err = buffer.processRangeLocalResource(buffer.ctx, opdata.tkBeg, opdata.tkEnd, opdata.chunkop, opdata.chunkfunc)
				}
			} else {
				err = fmt.Errorf("Incorrect buffer operation specified")
			}

			// !! temporary hack
			if opid%MAXCONNECTIONS == (MAXCONNECTIONS - 1) {
				runtime.GC()
			}

			// add errors to queue
			retVals <- err
		}(operation, itnum)
	}

	var err error
	// check return values (should wait until all routines are finished)
	for range buffer.ops {
		errjob := <-retVals
		if errjob != nil {
			err = errjob
		}
	}

	return err
}

// deleteRangeLocalResource implements DeleteRange but with workQueue awareness.
func (db *goBuffer) deleteRangeLocalResource(ctx storage.Context, TkBeg, TkEnd storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteRange() on nil Google bucket")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}

	// get all the keys within range, latest version, no tombstone
	// !! assumes that function has resource lock
	keys, err := db.db.getKeysInRange(ctx, TkBeg, TkEnd, true)
	if err != nil {
		return err
	}

	// hackish -- release resource
	db.db.releaseOpResource()

	// wait for all deletes to complete
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		// use available threads
		db.db.grabOpResource()
		go func(lkey storage.Key) {
			defer func() {
				db.db.releaseOpResource()
				wg.Done()
			}()
			tk, _ := storage.TKeyFromKey(lkey)

			if db.db.version >= VALUEVERSION && ctx.Versioned() {
				basekey := db.ctx.ConstructKeyVersion(tk, dvid.VersionID(0))
				// always assume tombstone and in current version context
				db.db.deleteVersion(db.ctx, basekey, ctx.VersionID(), true)
			} else {
				// fix (cannot call Delete now)
				tombstoneKey := ctx.(storage.VersionedCtx).TombstoneKey(tk)
				err = db.db.putV(db.ctx, tombstoneKey, dvid.EmptyValue())
				db.db.deleteV(db.ctx, lkey)
			}
		}(key)
	}
	wg.Wait()

	// hackish -- reask for resource
	db.db.grabOpResource()

	return nil
}

// processGetLocal implements ProcessRange functionality but with workQueue awareness for
// just one key/value GET
func (db *goBuffer) processGetLocalResource(ctx storage.Context, tkey storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	// !! assumes that a resource
	val, err := db.db.getVTKey(ctx, tkey, true)
	if err != nil {
		return err
	}

	// callback might not be thread safe
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if op != nil && op.Wg != nil {
		op.Wg.Add(1)
	}

	tkv := storage.TKeyValue{tkey, val}
	chunk := &storage.Chunk{op, &tkv}

	if err := f(chunk); err != nil {
		return err
	}

	return nil
}

// processRangeLocalResource implements ProcessRange functionality but with workQueue awareness
func (db *goBuffer) processRangeLocalResource(ctx storage.Context, TkBeg, TkEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	// grab keys
	// !! assumes that function has resource lock
	keys, _ := db.db.getKeysInRange(ctx, TkBeg, TkEnd, true)

	// hackish -- release resource
	db.db.releaseOpResource()

	keyvalchan := make(chan keyvalue_t, len(keys))
	for _, key := range keys {
		// use available threads
		db.db.grabOpResource()
		go func(lkey storage.Key) {
			defer func() {
				db.db.releaseOpResource()
			}()
			value, err := db.db.getV(ctx, lkey)
			if value == nil || err != nil {
				keyvalchan <- keyvalue_t{lkey, nil}
			} else {
				keyvalchan <- keyvalue_t{lkey, value}
			}

		}(key)

	}

	kvmap := make(map[string][]byte)
	for range keys {
		keyval := <-keyvalchan
		kvmap[string(keyval.key)] = keyval.value
	}

	// hackish -- reask for resource
	db.db.grabOpResource()

	var err error
	for _, key := range keys {
		val := kvmap[string(key)]
		tk, err := storage.TKeyFromKey(key)
		if err != nil {
			return err
		}
		if val == nil {
			return fmt.Errorf("Could not retrieve value")
		}

		if op != nil && op.Wg != nil {
			op.Wg.Add(1)
		}

		tkv := storage.TKeyValue{tk, val}
		chunk := &storage.Chunk{op, &tkv}
		if err := f(chunk); err != nil {
			return err
		}
	}

	return err
}
