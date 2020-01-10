// +build !clustered,!gcloud

/*
	This file contains local server code supporting local data instance copying with
	optional delimiting using datatype-specific filters, and migration between storage
	engines.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	humanize "github.com/janelia-flyem/go/go-humanize"
)

type txStats struct {
	name string
	// num key-value pairs
	numKV uint64

	// stats on value sizes on logarithmic scale to 10 MB
	numV0, numV1, numV10, numV100, numV1k, numV10k, numV100k, numV1m, numV10m uint64

	// some stats for timing
	firstTime  time.Time
	lastTime   time.Time
	lastBytes  uint64 // bytes received since lastTime
	totalBytes uint64
}

// record stats on size of values
func (t *txStats) addKV(k, v []byte) {
	if t.numKV == 0 {
		t.firstTime = time.Now()
	}
	t.numKV++

	vBytes := len(v)
	kBytes := len(k)
	curBytes := uint64(kBytes + vBytes)
	t.lastBytes += curBytes
	t.totalBytes += curBytes

	switch {
	case vBytes == 0:
		t.numV0++
	case vBytes < 10:
		t.numV1++
	case vBytes < 100:
		t.numV10++
	case vBytes < 1000:
		t.numV100++
	case vBytes < 10000:
		t.numV1k++
	case vBytes < 100000:
		t.numV10k++
	case vBytes < 1000000:
		t.numV100k++
	case vBytes < 10000000:
		t.numV1m++
	default:
		t.numV10m++
	}

	// Print progress?
	if elapsed := time.Since(t.lastTime); elapsed > time.Minute {
		mb := float64(t.lastBytes) / 1000000
		sec := elapsed.Seconds()
		throughput := mb / sec
		dvid.Debugf("Transfer throughput (%s): %5.2f MB/s (%s in %4.1f seconds).  Total %s\n", t.name, throughput, humanize.Bytes(t.lastBytes), sec, humanize.Bytes(t.totalBytes))

		t.lastTime = time.Now()
		t.lastBytes = 0
	}
}

func (t *txStats) printStats() {
	dvid.Infof("Total time: %s\n", time.Since(t.firstTime))
	dvid.Infof("Total size: %s\n", humanize.Bytes(t.totalBytes))
	dvid.Infof("# kv pairs: %d\n", t.numKV)
	dvid.Infof("Size of values transferred (bytes):\n")
	dvid.Infof(" key only:   %d\n", t.numV0)
	dvid.Infof(" [1,9):      %d\n", t.numV1)
	dvid.Infof(" [10,99):    %d\n", t.numV10)
	dvid.Infof(" [100,999):  %d\n", t.numV100)
	dvid.Infof(" [1k,10k):   %d\n", t.numV1k)
	dvid.Infof(" [10k,100k): %d\n", t.numV10k)
	dvid.Infof(" [100k,1m):  %d\n", t.numV100k)
	dvid.Infof(" [1m,10m):   %d\n", t.numV1m)
	dvid.Infof("  >= 10m:    %d\n", t.numV10m)
}

type flattenConfig struct {
	Versions    []dvid.UUID
	Instances   dvid.InstanceNames
	Alias       string
	Description string
	RepoLog     []string
	NodeNote    string
	NodeLog     []string
}

func getFlattenConfig(configFName string) (fc flattenConfig, err error) {
	var f *os.File
	if f, err = os.Open(configFName); err != nil {
		return
	}
	var data []byte
	if data, err = ioutil.ReadAll(f); err != nil {
		return
	}
	if err = json.Unmarshal(data, &fc); err != nil {
		return
	}
	return
}

// FlattenMetadata stores the metadata of a single node (the given UUID) into the destination
// store, optionally adjusting the settings based on the JSON in the configuration file.
func FlattenMetadata(uuid dvid.UUID, dstStore dvid.Store, configFName string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	// Get flatened metadata configuration from optional JSON file
	fc, err := getFlattenConfig(configFName)
	if err != nil {
		return err
	}

	// Create duplicate repo with changes
	origRepo, err := manager.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	var versions map[dvid.VersionID]struct{}
	repoToUUID := make(map[dvid.RepoID]dvid.UUID)
	versionToUUID := make(map[dvid.VersionID]dvid.UUID)
	if len(fc.Versions) != 0 { // if none specified, use all versions
		repoToUUID[origRepo.id] = fc.Versions[0]
		versions = make(map[dvid.VersionID]struct{}, len(fc.Versions))
		for _, uuid := range fc.Versions {
			v, err := manager.versionFromUUID(uuid)
			if err != nil {
				return err
			}
			versions[v] = struct{}{}
			versionToUUID[v] = uuid
		}
	} else {
		repoToUUID[origRepo.id] = origRepo.uuid
		for v, node := range origRepo.dag.nodes {
			versionToUUID[v] = node.uuid
		}
	}
	flattenRepo, err := origRepo.duplicate(versions, fc.Instances)
	if err != nil {
		return err
	}
	if fc.Alias != "" {
		flattenRepo.alias = fc.Alias
	}
	if fc.Description != "" {
		flattenRepo.description = fc.Description
	}
	if len(fc.RepoLog) != 0 {
		flattenRepo.log = fc.RepoLog
	}
	if fc.NodeNote != "" {
		node, found := flattenRepo.dag.nodes[flattenRepo.version]
		if found {
			node.note = fc.NodeNote
		} else {
			dvid.Errorf("unable to get node for version ID %d in flattened repo\n", flattenRepo.version)
		}
	}
	if len(fc.NodeLog) != 0 {
		node, found := flattenRepo.dag.nodes[flattenRepo.version]
		if found {
			node.log = fc.NodeLog
		} else {
			dvid.Errorf("unable to get node for version ID %d in flattened repo\n", flattenRepo.version)
		}
	}
	jsonBytes, err := flattenRepo.MarshalJSON()
	if err == nil {
		dvid.Infof("Flattened Repo Metadata:\n%s\n", string(jsonBytes))
	}

	// Store into destination store.
	dstKV, ok := dstStore.(storage.OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("unable to get destination store %q that is an ordered kv db", dstStore)
	}
	var ctx storage.MetadataContext
	if err := putData(dstKV, repoToUUIDKey, repoToUUID); err != nil {
		return err
	}
	if err := putData(dstKV, versionToUUIDKey, versionToUUID); err != nil {
		return err
	}
	value := append(manager.repoID.Bytes(), manager.versionID.Bytes()...)
	value = append(value, manager.instanceID.Bytes()...)
	if err := dstKV.Put(ctx, storage.NewTKey(newIDsKey, nil), value); err != nil {
		return err
	}
	return flattenRepo.saveToStore(dstKV)
}

func putData(kv storage.OrderedKeyValueDB, t storage.TKeyClass, data interface{}) error {
	var ctx storage.MetadataContext
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return err
	}
	return kv.Put(ctx, storage.NewTKey(t, nil), buf.Bytes())
}

// MigrateInstance migrates a data instance locally from an old storage
// engine to the current configured storage.
func MigrateInstance(uuid dvid.UUID, source dvid.InstanceName, srcStore, dstStore dvid.Store, c dvid.Config, done chan bool) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	transmitStr, _, err := c.GetString("transmit")
	if err != nil {
		return err
	}
	var uuids []dvid.UUID
	var flatten bool
	switch transmitStr {
	case "flatten":
		flatten = true
	case "all":
		flatten = false
	default: // parse a list of uuids
		uuidStrs := strings.Split(transmitStr, ",")
		if len(uuidStrs) == 0 {
			return fmt.Errorf("transmit setting needs to be 'all', 'flatten', or a list of UUIDs separated by a comma")
		}
		uuids = make([]dvid.UUID, len(uuidStrs))
		for i, uuidStr := range uuidStrs {
			validUUID, _, err := MatchingUUID(uuidStr)
			if err != nil {
				return fmt.Errorf("bad UUID %q in transmit list: %v", uuidStr, err)
			}
			uuids[i] = validUUID
		}
	}
	if transmitStr == "flatten" {
		flatten = true
	}

	// Get the source data instance.
	d, err := manager.getDataByUUIDName(uuid, source)
	if err != nil {
		return err
	}

	// Abort if the two stores are the same.
	if dstStore == srcStore {
		return fmt.Errorf("old store for data %q cannot be same as current store", source)
	}

	// Migrate data asynchronously.
	go func() {
		if len(uuids) == 0 {
			// Get the destination store.
			dstKV, ok := dstStore.(storage.OrderedKeyValueDB)
			if !ok {
				dvid.Errorf("unable to get destination store %q: %v", dstStore, err)
				return
			}

			// Get the src store.
			srcKV, ok := srcStore.(storage.OrderedKeyValueDB)
			if !ok {
				dvid.Errorf("unable to migrate data %q from store %s which isn't ordered kv store", source, srcStore)
				return
			}

			err := copyData(srcKV, dstKV, d, nil, uuid, nil, flatten)
			if err != nil {
				dvid.Errorf("error in migration of data %q: %v\n", source, err)
				return
			}
		} else {
			err := copyVersions(srcStore, dstStore, d, nil, uuids)
			if err != nil {
				dvid.Errorf("error in migration of data %q using uuids %v: %v\n", source, uuids, err)
				return
			}
		}
		if done != nil {
			done <- true
		}
	}()

	dvid.Infof("Migrating data %q from store %q to store %q (flatten %t)...\n", d.DataName(), srcStore, dstStore, flatten)
	return nil
}

type transferConfig struct {
	Versions []dvid.UUID
	Metadata bool
}

func getTransferConfig(configFName string) (tc transferConfig, okVersions map[dvid.VersionID]bool, err error) {
	var f *os.File
	if f, err = os.Open(configFName); err != nil {
		return
	}
	var data []byte
	if data, err = ioutil.ReadAll(f); err != nil {
		return
	}
	if err = json.Unmarshal(data, &tc); err != nil {
		return
	}
	okVersions = make(map[dvid.VersionID]bool, len(tc.Versions))
	for _, uuid := range tc.Versions {
		var v dvid.VersionID
		if v, err = VersionFromUUID(uuid); err != nil {
			return
		}
		okVersions[v] = true
	}
	return
}

// LimitVersions removes versions from the metadata that are not present in a
// configuration file.
func LimitVersions(uui dvid.UUID, configFName string) error {
	if manager == nil {
		return fmt.Errorf("can't limit versions with uninitialized manager")
	}
	f, err := os.Open(configFName)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	var tc transferConfig
	if err := json.Unmarshal(data, &tc); err != nil {
		return err
	}
	okUUIDs := make(map[dvid.UUID]bool, len(tc.Versions))
	okVersions := make(map[dvid.VersionID]bool, len(tc.Versions))
	for _, uuid := range tc.Versions {
		okUUIDs[uuid] = true
		if v, found := manager.uuidToVersion[uuid]; found {
			ancestry, err := manager.getAncestry(v)
			if err != nil {
				return err
			}
			for _, ancestorV := range ancestry {
				ancestorUUID, found := manager.versionToUUID[ancestorV]
				if !found {
					return fmt.Errorf("version %d has no UUID equivalent", ancestorV)
				}
				okUUIDs[ancestorUUID] = true
				okVersions[ancestorV] = true
			}
		}
	}
	manager.repoMutex.Lock()
	manager.idMutex.Lock()
	var repo *repoT
	for uuid, r := range manager.repos {
		if _, found := okUUIDs[uuid]; found {
			if repo == nil {
				repo = r
			}
		} else {
			delete(manager.repos, uuid)
			delete(manager.uuidToVersion, uuid)
		}
	}
	for v := range manager.versionToUUID {
		if !okVersions[v] {
			delete(manager.versionToUUID, v)
		}
	}
	for v, node := range repo.dag.nodes {
		if !okVersions[v] {
			delete(repo.dag.nodes, v)
		} else {
			var parents, children []dvid.VersionID
			for _, parent := range node.parents {
				if okVersions[parent] {
					parents = append(parents, parent)
				}
			}
			node.parents = parents
			for _, child := range node.children {
				if okVersions[child] {
					children = append(children, child)
				}
			}
			node.children = children
		}
	}
	manager.idMutex.Unlock()
	manager.repoMutex.Unlock()
	return nil
}

// TransferData copies key-value pairs from one repo to store and apply filtering as specified
// by the JSON configuration in the file specified by configFName.
// An example of the transfer JSON configuration file format:
// {
// 	"Versions": [
// 		"8a90ec0d257c415cae29f8c46603bcae",
// 		"a5682904bb824c06aba470c0a0cbffab",
// 		...
// 	},
// 	"Metadata": true,
// }
//
// All ancestors of desired leaf nodes should be specified because
// key-value pair transfer only occurs if the version in which
// it was saved is specified on the list.  This is useful for editing
// a preexisting store with new versions.
//
// If Metadata property is true, then if metadata exists in the old store,
// it is transferred to the new store with only the versions specified
// appearing in the DAG.
func TransferData(uuid dvid.UUID, srcStore, dstStore dvid.Store, configFName string) error {
	tc, okVersions, err := getTransferConfig(configFName)
	if err != nil {
		return err
	}
	if len(okVersions) == 0 {
		dvid.Infof("no specific versions specified for data transfer, so full copy will be done\n")
	}
	srcKVDB, ok := srcStore.(storage.OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("source store %q is not an ordered keyvalue store", srcStore)
	}
	dstKVDB, ok := dstStore.(storage.KeyValueDB)
	if !ok {
		return fmt.Errorf("destination store %q must at least be a key-value store", dstStore)
	}
	var wg sync.WaitGroup
	wg.Add(1)

	stats := new(txStats)
	stats.lastTime = time.Now()
	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64

	ch := make(chan *storage.KeyValue, 1000)
	go func() {
		for {
			kv := <-ch
			if kv == nil {
				wg.Done()
				dvid.Infof("Sent %d key-value pairs (%s, out of %d kv pairs, %s)\n",
					kvSent, humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
				stats.printStats()
				return
			}
			kvTotal++
			curBytes := uint64(len(kv.V) + len(kv.K))
			bytesTotal += curBytes
			if len(okVersions) != 0 {
				if kv.K.IsMetadataKey() {
					// transmit it all even though we might be filtering versions
				} else if kv.K.IsDataKey() {
					v, err := storage.VersionFromDataKey(kv.K)
					if err != nil {
						dvid.Errorf("couldn't get version from Key %v: %v\n", kv.K, err)
						continue
					}
					if !okVersions[v] {
						continue
					}
				}
			}
			kvSent++
			bytesSent += curBytes
			if err := dstKVDB.RawPut(kv.K, kv.V); err != nil {
				dvid.Errorf("can't put k/v pair to store %q: %v\n", dstStore, err)
			}
			stats.addKV(kv.K, kv.V)
		}
	}()

	var begKey storage.Key
	endKey := storage.ConstructBlobKey([]byte{})
	if tc.Metadata {
		begKey, _ = storage.MetadataContext{}.KeyRange()
	} else {
		begKey = storage.MinDataKey()
	}
	if err = srcKVDB.RawRangeQuery(begKey, endKey, false, ch, nil); err != nil {
		return fmt.Errorf("transfer data range query: %v", err)
	}
	wg.Wait()
	return nil
}

// CopyInstance copies a data instance locally, perhaps to a different storage
// engine if the new instance uses a different backend per a data instance-specific configuration.
// (See sample config.example.toml file in root dvid source directory.)
func CopyInstance(uuid dvid.UUID, source, target dvid.InstanceName, c dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	if source == "" || target == "" {
		return fmt.Errorf("both source and cloned name must be provided")
	}

	// Get any filter spec
	fstxt, found, err := c.GetString("filter")
	if err != nil {
		return err
	}
	var fs storage.FilterSpec
	if found {
		fs = storage.FilterSpec(fstxt)
	}

	// Get flatten or not
	transmit, found, err := c.GetString("transmit")
	if err != nil {
		return err
	}
	var flatten bool
	if transmit == "flatten" {
		flatten = true
	}

	// Get the source data instance.
	d1, err := manager.getDataByUUIDName(uuid, source)
	if err != nil {
		return err
	}

	// Create the target instance.
	t, err := TypeServiceByName(d1.TypeName())
	if err != nil {
		return err
	}
	d2, err := manager.newData(uuid, t, target, c)
	if err != nil {
		return err
	}

	// Populate the new data instance properties from source.
	copier, ok := d2.(PropertyCopier)
	if ok {
		if err := copier.CopyPropertiesFrom(d1, fs); err != nil {
			return err
		}
		if err := SaveDataByUUID(uuid, d2); err != nil {
			return err
		}
	}

	// We should be able to get the backing store (only ordered kv for now)
	oldKV, err := GetOrderedKeyValueDB(d1)
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v", d1.DataName(), err)
	}
	newKV, err := GetOrderedKeyValueDB(d2)
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v", d2.DataName(), err)
	}

	dvid.Infof("Copying data %q (%s) to data %q (%s)...\n", d1.DataName(), oldKV, d2.DataName(), newKV)

	// See if this data instance implements a Send filter.
	var filter storage.Filter
	filterer, ok := d1.(storage.Filterer)
	if ok && fs != "" {
		var err error
		filter, err = filterer.NewFilter(fs)
		if err != nil {
			return err
		}
	}

	// copy data with optional datatype-specific filtering.
	return copyData(oldKV, newKV, d1, d2, uuid, filter, flatten)
}

// copyData copies all key-value pairs pertinent to the given data instance d2.  If d2 is nil,
// the destination data instance is d1, useful for migration of data to a new store.
// Each datatype can implement filters that can restrict the transmitted key-value pairs
// based on the given FilterSpec.
func copyData(oldKV, newKV storage.OrderedKeyValueDB, d1, d2 dvid.Data, uuid dvid.UUID, f storage.Filter, flatten bool) error {
	// Get data context for this UUID.
	v, err := VersionFromUUID(uuid)
	if err != nil {
		return err
	}
	srcCtx := NewVersionedCtx(d1, v)
	var dstCtx *VersionedCtx
	if d2 == nil {
		d2 = d1
		dstCtx = srcCtx
	} else {
		dstCtx = NewVersionedCtx(d2, v)
	}

	// Send this instance's key-value pairs
	var wg sync.WaitGroup
	wg.Add(1)

	stats := new(txStats)
	stats.lastTime = time.Now()
	stats.name = fmt.Sprintf("copy of %s", d1.DataName())

	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64
	keysOnly := false
	if flatten {
		// Start goroutine to receive flattened key-value pairs and store them.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					wg.Done()
					dvid.Infof("Copied %d %q key-value pairs (%s, out of %d kv pairs, %s) [flattened]\n",
						kvSent, d1.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					stats.printStats()
					return
				}
				kvTotal++
				curBytes := uint64(len(tkv.V) + len(tkv.K))
				bytesTotal += curBytes
				if f != nil {
					skip, err := f.Check(tkv)
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d1.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if err := newKV.Put(dstCtx, tkv.K, tkv.V); err != nil {
					dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
				}
				stats.addKV(tkv.K, tkv.V)
			}
		}()

		begKey, endKey := srcCtx.TKeyRange()
		err := oldKV.ProcessRange(srcCtx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("received nil chunk in flatten push for data %s", d1.DataName())
			}
			ch <- c.TKeyValue
			return nil
		})
		ch <- nil
		if err != nil {
			return fmt.Errorf("error in flatten push for data %q: %v", d1.DataName(), err)
		}
	} else {
		// Start goroutine to receive all key-value pairs and store them.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s)\n",
						kvSent, d1.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					stats.printStats()
					return
				}
				tkey, err := storage.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("couldn't get %q TKey from Key %v: %v\n", d1.DataName(), kv.K, err)
					continue
				}

				kvTotal++
				curBytes := uint64(len(kv.V) + len(kv.K))
				bytesTotal += curBytes
				if f != nil {
					skip, err := f.Check(&storage.TKeyValue{K: tkey, V: kv.V})
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d1.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if dstCtx != nil {
					err := dstCtx.UpdateInstance(kv.K)
					if err != nil {
						dvid.Errorf("can't update raw key to new data instance %q: %v\n", d2.DataName(), err)
					}
				}
				if err := newKV.RawPut(kv.K, kv.V); err != nil {
					dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
				}
				stats.addKV(kv.K, kv.V)
			}
		}()

		begKey, endKey := srcCtx.KeyRange()
		if err = oldKV.RawRangeQuery(begKey, endKey, keysOnly, ch, nil); err != nil {
			return fmt.Errorf("push voxels %q range query: %v", d1.DataName(), err)
		}
	}
	wg.Wait()
	return nil
}

// for a list of UUIDs on a DAG path, get map of all versions on the path from root to ancestor
// and a list of the versions to be stored.
func calcVersionPath(uuids []dvid.UUID) (versionsOnPath map[dvid.VersionID]struct{}, versionsToStore []dvid.VersionID, err error) {
	if len(uuids) == 0 {
		err = fmt.Errorf("can't make version map with empty uuid list")
		return
	}
	versionsToStore = make([]dvid.VersionID, len(uuids))
	for i, uuid := range uuids {
		var v dvid.VersionID
		v, err = VersionFromUUID(uuid)
		if err != nil {
			return
		}
		versionsToStore[i] = v
	}
	lastStoredV := versionsToStore[len(versionsToStore)-1]
	var ancestorVersions []dvid.VersionID
	if ancestorVersions, err = GetAncestry(lastStoredV); err != nil {
		return
	}
	numV := len(ancestorVersions)
	versionsOnPath = make(map[dvid.VersionID]struct{}, numV)

	lastStoredV++
	for _, v := range ancestorVersions {
		if lastStoredV <= v {
			err = fmt.Errorf("Ancestor path had non-decreasing version IDs: %v", ancestorVersions)
			return
		}
		versionsOnPath[v] = struct{}{}
	}
	return
}

type rawQueryDB interface {
	// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
	// retrieval like DVID-to-DVID communication and should not be used by data type
	// implementations if possible because each version's key-value pairs are sent
	// without filtering by the current version and its ancestor graph.  A nil is sent
	// down the channel when the range is complete.  The query can be cancelled by sending
	// a value down the cancel channel.
	RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error
}

type rawPutDB interface {
	// RawPut is a low-level function that puts a key-value pair using full keys.
	// This can be used in conjunction with RawRangeQuery.  It does not automatically
	// delete any associated tombstone, unlike the Delete() function, so tombstone
	// deletion must be handled via RawDelete().
	RawPut(storage.Key, []byte) error
}

// copyVersions copies the minimal kv pairs necessary to fulfill passed versions.  If d2 is nil,
// the destination data instance is d1, useful for migration of data to a new store.
func copyVersions(srcStore, dstStore dvid.Store, d1, d2 dvid.Data, uuids []dvid.UUID) error {
	if len(uuids) == 0 {
		dvid.Infof("no versions given for copy... aborting\n")
		return nil
	}
	srcDB, ok := srcStore.(rawQueryDB)
	if !ok {
		return fmt.Errorf("source store %q doesn't have required raw range query", srcStore)
	}
	dstDB, ok := dstStore.(rawPutDB)
	if !ok {
		return fmt.Errorf("destination store %q doesn't have raw Put query", dstStore)
	}
	var dataInstanceChanged bool
	if d2 == nil {
		d2 = d1
	} else {
		dataInstanceChanged = true
	}
	versionsOnPath, versionsToStore, err := calcVersionPath(uuids)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)

	statsTotal := new(txStats)
	statsTotal.lastTime = time.Now()
	statsTotal.name = fmt.Sprintf("%q total", d1.DataName())
	statsStored := new(txStats)
	statsStored.lastTime = time.Now()
	statsStored.name = fmt.Sprintf("stored into %q", d2.DataName())
	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64

	// Start goroutine to receive all key-value pairs, process, and store them.
	rawCh := make(chan *storage.KeyValue, 5000)
	go func() {
		var maxVersionKey storage.Key
		var numStoredKV int
		kvsToStore := make(map[dvid.VersionID]*storage.KeyValue, len(versionsToStore))
		for _, v := range versionsToStore {
			kvsToStore[v] = nil
		}
		for {
			kv := <-rawCh
			if kv != nil && !storage.Key(kv.K).IsDataKey() {
				dvid.Infof("Skipping non-data key-value %x ...\n", []byte(kv.K))
				continue
			}
			if kv == nil || maxVersionKey == nil || bytes.Compare(kv.K, maxVersionKey) > 0 {
				if numStoredKV > 0 {
					var lastKV *storage.KeyValue
					for _, v := range versionsToStore {
						curKV := kvsToStore[v]
						if lastKV == nil || (curKV != nil && bytes.Compare(lastKV.V, curKV.V) != 0) {
							if curKV != nil {
								keybuf := make(storage.Key, len(curKV.K))
								copy(keybuf, curKV.K)
								if dataInstanceChanged {
									err = storage.ChangeDataKeyInstance(keybuf, d2.InstanceID())
									if err != nil {
										dvid.Errorf("could not change instance ID of key to %d: %v\n", d2.InstanceID(), err)
										continue
									}
								}
								storage.ChangeDataKeyVersion(keybuf, v)
								kvSent++
								bytesSent += uint64(len(curKV.V) + len(keybuf))
								if err := dstDB.RawPut(keybuf, curKV.V); err != nil {
									dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
								}
								statsStored.addKV(keybuf, curKV.V)
								lastKV = curKV
							}
						}
					}
				}
				if kv == nil {
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s)\n",
						kvSent, d1.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					dvid.Infof("Total KV Stats for %q:\n", d1.DataName())
					statsTotal.printStats()
					dvid.Infof("Total KV Stats for newly stored %q:\n", d2.DataName())
					statsStored.printStats()
					return
				}
				tk, err := storage.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("couldn't get %q TKey from Key %v: %v\n", d1.DataName(), kv.K, err)
					continue
				}
				maxVersionKey, err = storage.MaxVersionDataKey(d1.InstanceID(), tk)
				if err != nil {
					dvid.Errorf("couldn't get max version key from Key %v: %v\n", kv.K, err)
					continue
				}
				for _, v := range versionsToStore {
					kvsToStore[v] = nil
				}
				numStoredKV = 0
			}
			curV, err := storage.VersionFromDataKey(kv.K)
			if err != nil {
				dvid.Errorf("unable to get version from key-value: %v\n", err)
				continue
			}
			curBytes := uint64(len(kv.V) + len(kv.K))
			if _, onPath := versionsOnPath[curV]; onPath {
				tk, _ := storage.TKeyFromKey(kv.K)
				for _, v := range versionsToStore {
					if curV <= v {
						kvsToStore[v] = kv
						dvid.Infof("adding key %q, value %q, version %d to version %d store\n", string(tk), string(kv.V), curV, v)
						numStoredKV++
						break
					}
				}
			}

			kvTotal++
			bytesTotal += curBytes
			statsTotal.addKV(kv.K, kv.V)
		}
	}()

	// Send all kv pairs for the source data instance down the channel.
	begKey, endKey := storage.DataInstanceKeyRange(d1.InstanceID())
	keysOnly := false
	if err := srcDB.RawRangeQuery(begKey, endKey, keysOnly, rawCh, nil); err != nil {
		return fmt.Errorf("push voxels %q range query: %v", d1.DataName(), err)
	}
	wg.Wait()
	return nil
}
