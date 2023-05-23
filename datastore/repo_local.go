//go:build !clustered && !gcloud
// +build !clustered,!gcloud

/*
	This file contains code for handling a Repo, the basic unit of versioning in DVID,
	and Manager, a collection of Repo.  A Repo consists of a DAG where nodes can be
	optionally locked.

	For non-clustered, non-cloud ("local") DVID servers, we can get away with a simple
	in-memory implementation that persists to the MetadataStore when needed.

	Locks are held at the public function and repoManager struct level.  They are not
	held at the repo, dag, or node structure level except for public functions.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// RepoFormatVersion is the current repo metadata format version
const RepoFormatVersion = 1

// InitialMutationID is the initial mutation ID for all new repositories.
// It was chosen to be a large number to distinguish between mutations with
// repo-wide persistent IDs and the legacy data instance-specific mutation IDs
// that would reset on server restart.
const InitialMutationID = 1000000000 // billion

// StrideMutationID is a way to minimize cost of persisting mutation IDs for a repo.
// We persist the current mutation ID plus the stride, so that we don't have to update
// the persisted mutation ID frequently.  The downside is that there may be jumps
// in mutation IDs on restarts, since the persisted ID is loaded on next restart.
const StrideMutationID = 100

// Key space handling for metadata
const (
	keyUnknown storage.TKeyClass = iota
	repoToUUIDKey
	versionToUUIDKey
	newIDsKey
	repoKey
	formatKey
	ServerLockKey // name of key for locking metadata globally
	mutidKey
)

// Config specifies new instance and mutation ID generation
type Config struct {
	InstanceGen   string
	InstanceStart dvid.InstanceID
	MutationStart uint64
	ReadOnly      bool
}

// Initialize creates a repositories manager that is handled through package functions.
func Initialize(initMetadata bool, iconfig Config) error {
	m := &repoManager{
		repoToUUID:      make(map[dvid.RepoID]dvid.UUID),
		versionToUUID:   make(map[dvid.VersionID]dvid.UUID),
		uuidToVersion:   make(map[dvid.UUID]dvid.VersionID),
		branchToUUID:    make(map[string]dvid.UUID),
		repos:           make(map[dvid.UUID]*repoT),
		repoID:          1,
		versionID:       1,
		iids:            make(map[dvid.InstanceID]DataService),
		dataByUUID:      make(map[dvid.UUID]DataService),
		instanceIDGen:   iconfig.InstanceGen,
		instanceIDStart: iconfig.InstanceStart,
		mutationIDStart: InitialMutationID,
		readOnly:        iconfig.ReadOnly,
	}
	if iconfig.InstanceGen == "" {
		m.instanceIDGen = "sequential"
	}
	if iconfig.InstanceStart > 1 {
		m.instanceID = iconfig.InstanceStart
	} else {
		m.instanceID = 1
	}
	if iconfig.MutationStart > m.mutationIDStart {
		m.mutationIDStart = iconfig.MutationStart
	}

	var err error
	m.store, err = storage.MetaDataKVStore()
	if err != nil {
		return err
	}

	// TODO: parallelize metadata init fetches for high-latency backends
	if initMetadata {
		// Initialize repo management data in storage
		dvid.TimeInfof("Initializing repo management data in storage\n")
		if err := m.putNewIDs(); err != nil {
			return err
		}
		if err := m.putCaches(); err != nil {
			return err
		}
		m.formatVersion = RepoFormatVersion
		if err := m.putData(formatKey, &(m.formatVersion)); err != nil {
			return err
		}
	} else {
		// Load the repo metadata
		dvid.TimeInfof("Loading metadata from storage (read-only %t)\n", m.readOnly)
		if err = m.loadMetadata(); err != nil {
			return fmt.Errorf("Error loading metadata: %v", err)
		}
		dvid.TimeInfof("Finished loading metadata from storage (read-only %t)\n", m.readOnly)
	}
	// Set the package variable.  We are good to go...
	manager = m

	// Add ephemeral data instances if a store wants it.
	stores, err := storage.AllStores()
	for alias, store := range stores {
		istore, ok := store.(dvid.AutoInstanceStore)
		dvid.Infof("Store %q auto instance store %t\n", store, ok)
		if ok {
			name, uuidStr, n := istore.AutoInstances()
			if name == "" || n == 0 {
				dvid.Infof("Store %q could require automatic instances but didn't.\n", store)
				continue
			}
			typeservice, err := TypeServiceByName(dvid.TypeString("uint8blk"))
			if err != nil {
				return err
			}
			var uuid dvid.UUID
			if uuidStr == "" {
				for _, repoUUID := range m.repoToUUID {
					uuid = repoUUID
					break
				}
				if uuid == "" {
					dvid.Infof("Unable to get uuid for repo to create auto instance... skipping\n")
					continue
				}
			} else {
				uuid, _, err = m.matchingUUID(uuidStr)
				if err != nil {
					dvid.Infof("Instance %q was assigned uuid %s that cannot be matched... skipping\n", name, uuidStr)
					continue
				}
			}
			config := dvid.NewConfig()
			config.Set("compression", "jpeg")
			config.Set("versioned", "true")
			config.Set("GridStore", string(alias))
			for i := 0; i < n; i++ {
				var suffix string
				if i > 0 {
					suffix = fmt.Sprintf("_%d", i)
				}
				config.Set("ScaleLevel", fmt.Sprintf("%d", i))
				dataname := dvid.InstanceName(name + suffix)
				if _, err := NewData(uuid, typeservice, dataname, config); err != nil {
					dvid.Infof("Couldn't auto-create data instance %q for uuid %s: %v\n", dataname, uuid, err)
				} else {
					dvid.Infof("Auto-created data instance %q for uuid %s: %v\n", dataname, uuid, config)
				}
			}
		}
	}

	// Allow data instance to initialize if desired.
	for _, data := range m.iids {
		if data.IsDeleted() {
			continue
		}
		dvid.TimeInfof("Initializing data instance %q, type %q\n", data.DataName(), data.TypeName())
		logwriter, ok := data.(storage.LogWritable)
		if ok && logwriter.WriteLogRequired() {
			writeLog := logwriter.GetWriteLog()
			if writeLog == nil {
				return fmt.Errorf("data %q (type %s) requires a write log yet has none assigned", data.DataName(), data.TypeName())
			}
		}
		logreader, ok := data.(storage.LogReadable)
		if ok && logreader.ReadLogRequired() {
			readLog := logreader.GetReadLog()
			if readLog == nil {
				return fmt.Errorf("data %q (type %s) requires a read log yet has none assigned", data.DataName(), data.TypeName())
			}
		}
		d, ok := data.(Initializer)
		if ok {
			d.Initialize() // Should be done sequentially in case its necessary to start receiving requests.
		}
		dvid.TimeInfof("Finished initializing data instance %q\n", data.DataName())
	}

	return nil
}

// InitializeGrayscale initializes datastore with ephemeral grayscale data, not reading
// or storing anything to persistent storage.
func InitializeGrayscale(grayscaleRef string) error {
	uuidStr := "00000000000000000000000000000000"
	uuid, _ := dvid.StringToUUID(uuidStr)
	versionID := dvid.VersionID(1)
	repoID := dvid.RepoID(1)

	m := &repoManager{
		repoToUUID:      map[dvid.RepoID]dvid.UUID{repoID: uuid},
		versionToUUID:   map[dvid.VersionID]dvid.UUID{versionID: uuid},
		uuidToVersion:   map[dvid.UUID]dvid.VersionID{uuid: versionID},
		branchToUUID:    map[string]dvid.UUID{uuidStr + "master": uuid},
		repos:           map[dvid.UUID]*repoT{uuid: newRepo(uuid, versionID, repoID, "")},
		repoID:          repoID,
		versionID:       versionID,
		iids:            map[dvid.InstanceID]DataService{},
		dataByUUID:      map[dvid.UUID]DataService{},
		instanceIDGen:   "sequential",
		instanceIDStart: 1,
		mutationIDStart: 1,
		readOnly:        true,
	}
	manager = m

	// Add grayscale
	typeservice, err := TypeServiceByName(dvid.TypeString("uint8blk"))
	if err != nil {
		return err
	}

	stores, err := storage.AllStores()
	if err != nil {
		return err
	}
	for alias, store := range stores {
		istore, ok := store.(dvid.AutoInstanceStore)
		dvid.Infof("Store %q auto instance store %t\n", store, ok)
		if ok {
			name, uuidStr, n := istore.AutoInstances()
			if name == "" || n == 0 {
				dvid.Infof("Store %q could require automatic instances but didn't.\n", store)
				continue
			}
			var uuid dvid.UUID
			if uuidStr == "" {
				for _, repoUUID := range m.repoToUUID {
					uuid = repoUUID
					break
				}
				if uuid == "" {
					dvid.Infof("Unable to get uuid for repo to create auto instance... skipping\n")
					continue
				}
			} else {
				uuid, _, err = m.matchingUUID(uuidStr)
				if err != nil {
					dvid.Infof("Instance %q was assigned uuid %s that cannot be matched... skipping\n", name, uuidStr)
					continue
				}
			}
			config := dvid.NewConfig()
			config.Set("compression", "jpeg")
			config.Set("versioned", "true")
			config.Set("GridStore", string(alias))
			for i := 0; i < n; i++ {
				var suffix string
				if i > 0 {
					suffix = fmt.Sprintf("_%d", i)
				}
				config.Set("ScaleLevel", fmt.Sprintf("%d", i))
				dataname := dvid.InstanceName(name + suffix)
				dataservice, err := m.newData(uuid, typeservice, dataname, config, false)
				if err != nil {
					dvid.Infof("Couldn't auto-create data instance %q for uuid %s: %v\n", dataname, uuid, err)
				} else {
					instanceID := dataservice.InstanceID()
					dataUUID := dataservice.DataUUID()
					dvid.Infof("Auto-created data instance %q for uuid %s, instance id %d, data UUID %s\n",
						dataname, uuid, instanceID, dataUUID)
					m.iids[instanceID] = dataservice
					m.dataByUUID[dataUUID] = dataservice
				}
			}
		}
	}

	return nil
}

// ReloadMetadata reloads the repositories manager from an existing metadata store.
// This shuts off all other dvid requests.
func ReloadMetadata() error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	dvid.DenyRequests() // draconian step to make sure no HTTP or RPC requests go through
	defer dvid.AllowRequests()

	m := &repoManager{
		repoToUUID:      make(map[dvid.RepoID]dvid.UUID),
		versionToUUID:   make(map[dvid.VersionID]dvid.UUID),
		uuidToVersion:   make(map[dvid.UUID]dvid.VersionID),
		branchToUUID:    make(map[string]dvid.UUID),
		repos:           make(map[dvid.UUID]*repoT),
		repoID:          manager.repoID,
		versionID:       manager.versionID,
		iids:            make(map[dvid.InstanceID]DataService),
		dataByUUID:      make(map[dvid.UUID]DataService),
		instanceIDGen:   manager.instanceIDGen,
		instanceIDStart: manager.instanceIDStart,
	}

	var err error
	m.store, err = storage.MetaDataKVStore()
	if err != nil {
		return err
	}

	// Load the repo metadata
	dvid.TimeInfof("Reloading metadata from storage...\n")
	if err = m.loadMetadata(); err != nil {
		return fmt.Errorf("Error loading metadata: %v", err)
	}

	// Swap the manager out.  This is dangerous and is why no requests should be ongoing
	// at time of this function.
	manager = m

	return nil
}

// --- In the case of a single DVID process, return new ids requires only a lock.
// --- This becomes more tricky when dealing with multiple DVID processes working
// --- off shared storage engines.

// repoManager manages all the repos in the datastore.
type repoManager struct {
	readOnly bool

	// Mapping of all UUIDs to the repositories where that node sits.
	repos     map[dvid.UUID]*repoT
	repoMutex sync.RWMutex

	// Allows versioning of metadata format
	formatVersion uint64

	// Mutex for concurrent use of all maps and ids below.
	idMutex sync.RWMutex

	// Map local RepoID to root UUID
	repoToUUID map[dvid.RepoID]dvid.UUID

	// Map local VersionID to UUID.  This also lets us know which nodes are available
	// in this DVID server since a subset of data can be pulled.
	versionToUUID map[dvid.VersionID]dvid.UUID

	// Map UUID to local VersionID -- this is not stored but generated on load
	uuidToVersion map[dvid.UUID]dvid.VersionID

	// Map branch name to HEAD UUID.
	branchToUUID map[string]dvid.UUID
	branchMutex  sync.RWMutex

	// Counters that provide the local IDs of the next new repo, version, or data instance.
	// Valid counters should be >= 1, so we can distinguish between valid ids and the
	// default zero value.
	repoID     dvid.RepoID
	versionID  dvid.VersionID
	instanceID dvid.InstanceID // Can be set by the configuration file.

	// Mapping of all instance IDs to the data service they represent.
	// Not persisted but created on load and maintained.
	iids map[dvid.InstanceID]DataService

	// Mapping of all data UUIDs to the data service they represent.
	// Not persisted but created on load and maintained.
	dataByUUID map[dvid.UUID]DataService

	// instance id generation
	instanceIDGen   string
	instanceIDStart dvid.InstanceID

	// mutation id generation
	mutationIDStart uint64

	// Verified metadata storage for ease of use.
	store storage.OrderedKeyValueDB
}

func (m *repoManager) Shutdown() {
	wg := new(sync.WaitGroup)
	for _, data := range m.iids {
		d, ok := data.(Shutdowner)
		if ok {
			wg.Add(1)
			go d.Shutdown(wg)
		}
	}
	wg.Wait()
	dvid.Infof("All %d data instances shutdown.\n", len(m.iids))
}

// MarshalJSON returns JSON of object where each repo is a property with root UUID name
// and value corresponding to repo info.
func (m *repoManager) MarshalJSON() ([]byte, error) {
	repos := make(map[dvid.UUID]*repoT, len(m.repoToUUID))
	m.idMutex.RLock()
	for _, uuid := range m.repoToUUID {
		m.repoMutex.RLock()
		repos[uuid] = m.repos[uuid]
		m.repoMutex.RUnlock()
	}
	m.idMutex.RUnlock()
	return json.Marshal(repos)
}

// We don't store repoManager via Gob as a single unit.  Rather, we persist
// parts of it to different key/value pairs in the metadata store, so there's
// more granualarity in I/O, e.g., at the single repo level rather than all
// repos at once.
//
// The Gob (de)serialization allows transmission over the network if doing p2p.

func (m *repoManager) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(&(m.repoToUUID)); err != nil {
		return err
	}
	if err := dec.Decode(&(m.versionToUUID)); err != nil {
		return err
	}
	// Generate the inverse UUID to VersionID mapping.
	for versionID, uuid := range m.versionToUUID {
		m.uuidToVersion[uuid] = versionID
	}
	if err := dec.Decode(&(m.repoID)); err != nil {
		return err
	}
	if err := dec.Decode(&(m.versionID)); err != nil {
		return err
	}
	if err := dec.Decode(&(m.instanceID)); err != nil {
		return err
	}

	if err := dec.Decode(&(m.repos)); err != nil {
		return err
	}
	return nil
}

func (m *repoManager) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m.repoToUUID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.versionToUUID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.repoID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.versionID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.instanceID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.repos); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---- RepoManager persistence to MetaData storage -----

func (m *repoManager) loadData(t storage.TKeyClass, data interface{}) (found bool, err error) {
	var ctx storage.MetadataContext
	value, err := m.store.Get(ctx, storage.NewTKey(t, nil))
	if err != nil {
		return false, fmt.Errorf("Bad metadata GET: %v", err)
	}
	if value == nil {
		return false, nil
	}
	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(data); err != nil {
		return false, fmt.Errorf("Could not decode Gob encoded metadata (len %d): %v", len(value), err)
	}
	return true, nil
}

func (m *repoManager) putData(t storage.TKeyClass, data interface{}) error {
	if m.readOnly {
		dvid.Infof("Server in read-only mode: will not write metadata data.\n")
		return nil
	}
	var ctx storage.MetadataContext
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return err
	}
	return m.store.Put(ctx, storage.NewTKey(t, nil), buf.Bytes())
}

// Load the next ids to be used for RepoID, VersionID, and InstanceID.
func (m *repoManager) loadNewIDs() error {
	var ctx storage.MetadataContext
	value, err := m.store.Get(ctx, storage.NewTKey(newIDsKey, nil))
	if err != nil {
		return err
	}
	if len(value) != dvid.RepoIDSize+dvid.VersionIDSize+dvid.InstanceIDSize {
		return fmt.Errorf("bad value returned for new ids.  Length %d bytes", len(value))
	}
	pos := 0
	m.repoID = dvid.RepoIDFromBytes(value[pos : pos+dvid.RepoIDSize])
	pos += dvid.RepoIDSize
	m.versionID = dvid.VersionIDFromBytes(value[pos : pos+dvid.VersionIDSize])
	pos += dvid.VersionIDSize
	m.instanceID = dvid.InstanceIDFromBytes(value[pos : pos+dvid.InstanceIDSize])
	return nil
}

func (m *repoManager) putNewIDs() error {
	if m.readOnly {
		dvid.Infof("Server in read-only mode: will not write metadata new version and instance IDs.\n")
		return nil
	}
	var ctx storage.MetadataContext
	value := append(m.repoID.Bytes(), m.versionID.Bytes()...)
	value = append(value, m.instanceID.Bytes()...)
	return m.store.Put(ctx, storage.NewTKey(newIDsKey, nil), value)
}

func (m *repoManager) putCaches() error {
	if m.readOnly {
		dvid.Infof("Server in read-only mode: will not write metadata caches.\n")
		return nil
	}
	m.idMutex.RLock()
	if err := m.putData(repoToUUIDKey, m.repoToUUID); err != nil {
		m.idMutex.RUnlock()
		return err
	}
	if err := m.putData(versionToUUIDKey, m.versionToUUID); err != nil {
		m.idMutex.RUnlock()
		return err
	}
	m.idMutex.RUnlock()
	return nil
}

func (m *repoManager) loadVersion0() error {
	// Load the maps
	if _, err := m.loadData(repoToUUIDKey, &(m.repoToUUID)); err != nil {
		return fmt.Errorf("Error loading repo to UUID map: %s", err)
	}
	if _, err := m.loadData(versionToUUIDKey, &(m.versionToUUID)); err != nil {
		return fmt.Errorf("Error loading version to UUID map: %s", err)
	}
	if err := m.loadNewIDs(); err != nil {
		return fmt.Errorf("Error loading new local ids: %s", err)
	}

	// Generate the inverse UUID to VersionID mapping.
	for v, uuid := range m.versionToUUID {
		m.uuidToVersion[uuid] = v
	}

	// Load all the repo data
	var ctx storage.MetadataContext
	minRepo := dvid.RepoID(0)
	maxRepo := dvid.RepoID(dvid.MaxRepoID)

	minTKey := storage.NewTKey(repoKey, minRepo.Bytes())
	maxTKey := storage.NewTKey(repoKey, maxRepo.Bytes())
	kvList, err := m.store.GetRange(ctx, minTKey, maxTKey)
	if err != nil {
		return err
	}

	var saveCache bool
	for _, kv := range kvList {
		var saveRepo bool

		ibytes, err := kv.K.ClassBytes(repoKey)
		if err != nil {
			return err
		}
		repoID := dvid.RepoIDFromBytes(ibytes)

		// Load each repo
		rootUUID, found := m.repoToUUID[repoID]
		if !found {
			return fmt.Errorf("Retrieved repo with id %d that is not in map.  Corrupt DB?", repoID)
		}
		r := &repoT{
			log:        []string{},
			properties: make(map[string]interface{}),
			data:       make(map[dvid.InstanceName]DataService),
		}
		if err = dvid.Deserialize(kv.V, r); err != nil {
			return fmt.Errorf("Error gob decoding repo %d: %v", repoID, err)
		}

		// Cache all UUID from nodes into our high-level cache
		var dagVersions []dvid.VersionID
		for v, node := range r.dag.nodes {
			dagVersions = append(dagVersions, v)
			uuid, found := m.versionToUUID[v]
			if !found {
				dvid.TimeErrorf("Version id %d found in repo %s (id %d) not in cache map. Adding it...", v, r.uuid, r.id)
				m.versionToUUID[v] = node.uuid
				m.uuidToVersion[node.uuid] = v
				uuid = node.uuid
				saveCache = true
			}
			m.repos[uuid] = r
		}

		// Populate the instance id -> dataservice map and convert/upgrade any deprecated data instance.
		for dataname, dataservice := range r.data {

			// hack to allow flattened metadata save to function without full dataservice copies
			// as per repo.duplicate() function.
			if _, found := m.uuidToVersion[dataservice.RootUUID()]; !found {
				dvid.TimeInfof("Data instance %q has root UUID %q not present, so setting it to current root UUID %q\n")
				dataservice.SetRootUUID(rootUUID)
				saveRepo = true
			}

			dataUUID := dataservice.DataUUID()
			if dataUUID == "" {
				dataUUID = dvid.NewUUID()
				dataservice.SetDataUUID(dataUUID)
				dvid.TimeInfof("Assigned data %q to data UUID %s.\n", dataname, dataservice.DataUUID())
				saveRepo = true
			}

			migrator, doMigrate := dataservice.(TypeMigrator)
			if doMigrate {
				dvid.TimeInfof("Migrating instance %q of type %q to ...\n", dataservice.DataName(), dataservice.TypeName())
				dataservice, err = migrator.MigrateData(dagVersions)
				if err != nil {
					return fmt.Errorf("Error migrating data instance: %v", err)
				}
				r.data[dataname] = dataservice
				saveRepo = true
				dvid.TimeInfof("Now instance %q of type %q ...\n", dataservice.DataName(), dataservice.TypeName())
			}

			upgrader, upgradable := dataservice.(TypeUpgrader)
			if upgradable {
				oldV := dataservice.TypeVersion()
				dvid.TimeInfof("Upgrading instance %q, type %q from version %s...\n", dataservice.DataName(), dataservice.TypeName(), oldV)
				upgraded, err := upgrader.UpgradeData()
				if err != nil {
					return fmt.Errorf("Error upgrading data instance %q: %v", dataservice.DataName(), err)
				}
				if upgraded {
					saveRepo = true
					dvid.TimeInfof("Upgraded instance %q, type %q from version %s to %s\n", dataservice.DataName(), dataservice.TypeName(), oldV, dataservice.TypeVersion())
				}
			}

			m.iids[dataservice.InstanceID()] = dataservice
			m.dataByUUID[dataservice.DataUUID()] = dataservice

			// Cache the assigned store and log
			store, err := storage.GetAssignedStore(dataservice)
			if err != nil {
				return err
			}
			dataservice.SetKVStore(store)
			lstore, err := storage.GetAssignedLog(dataservice)
			if err != nil {
				return err
			}
			dataservice.SetLogStore(lstore)

			// Initialize any dataservice that's initializable, e.g., start sync processing goroutines.
			initializer, initializable := dataservice.(DataInitializer)
			if initializable {
				err := initializer.InitDataHandlers()
				if err != nil {
					return err
				}
				dvid.TimeInfof("Initialized data handlers for instance %q on repo load.\n", dataservice.DataName())
			}
		}

		// Recreate the sync graph for this repo, taking into account possible legacy sync names.
		for _, dataservice := range r.data {
			syncer, syncable := dataservice.(Syncer)
			if syncable {
				syncUUIDs := syncer.SyncedData()
				if len(syncUUIDs) != 0 {
					for u := range syncUUIDs {
						// get the dataservice associated with this synced data.
						syncedData, found := m.dataByUUID[u]
						if found {
							subs, err := syncer.GetSyncSubs(syncedData)
							if err != nil {
								dvid.TimeCriticalf("Skipping bad sync of data %q to data %q: %v\n", dataservice.DataName(), syncedData.DataName(), err)
								continue
							}
							r.addSyncGraph(subs)
						} else {
							dvid.TimeErrorf("Skipping bad sync of %q with missing data uuid %s", dataservice.DataName(), u)
						}
					}
				} else {
					// TODO: Remove when we no longer have to support legacy dvid installs.
					syncNames := syncer.SyncedNames()
					if len(syncNames) == 0 {
						continue
					}
					dvid.TimeInfof("Converting data %q %d legacy sync names to data UUIDs...\n", dataservice.DataName(), len(syncNames))
					syncs := dvid.UUIDSet{}
					for _, name := range syncNames {
						// get the dataservice associated with this synced data.
						syncedData, found := r.data[name]
						if found {
							subs, err := syncer.GetSyncSubs(syncedData)
							if err != nil {
								dvid.Criticalf("Skipping bad sync of data %q to data %q: %v\n", dataservice.DataName(), syncedData.DataName(), err)
								continue
							}
							r.addSyncGraph(subs)
							// convert the sync names to data UUIDs
							syncs[syncedData.DataUUID()] = struct{}{}
							dvid.TimeInfof("  Converted synced data %q to its UUID: %s\n", name, syncedData.DataUUID())
						} else {
							dvid.TimeErrorf(" Skipping sync of %q with missing data %q for repo @ %s", dataservice.DataName(), name, r.uuid)
						}
					}
					dvid.TimeInfof("After conversion data %q has syncs: %v\n", dataservice.DataName(), syncs)
					dataservice.SetSync(syncs)
					dvid.TimeInfof("After calling SetSync we get back: %v\n", syncer.SyncedData())
					saveRepo = true
				}
			}
		}

		// Load any mutable properties for the data instances.
		for _, dataservice := range r.data {
			mutator, mutable := dataservice.(InstanceMutator)
			if mutable {
				modified, err := mutator.LoadMutable(r.version, m.formatVersion, RepoFormatVersion)
				if err != nil {
					return err
				}
				if modified {
					saveRepo = true
				}
			}
		}

		// If updates had to be made, save the migrated repo metadata.
		if saveRepo {
			dvid.TimeInfof("Re-saved repo with root %s due to migrations.\n", r.uuid)
			if err := r.saveToStore(m.store); err != nil {
				return err
			}
		}
	}
	if err := m.verifyCompiledTypes(); err != nil {
		return err
	}

	for id, uuid := range m.repoToUUID {
		if m.repos[uuid] == nil {
			dvid.TimeInfof("Found empty repo id %d (uuid %s)... deleting.\n", id, uuid)
			delete(m.repoToUUID, id)
			saveCache = true
		}
	}

	// If we noticed missing or corrupt cache entries, save current metadata.
	if saveCache {
		if err := m.putCaches(); err != nil {
			return err
		}
	}

	if m.formatVersion != RepoFormatVersion {
		dvid.TimeInfof("Updated metadata from version %d to version %d\n", m.formatVersion, RepoFormatVersion)
		m.formatVersion = RepoFormatVersion
		if err := m.putData(formatKey, &(m.formatVersion)); err != nil {
			return err
		}
	}
	dvid.TimeInfof("Loaded %d repositories from metadata store.\n", len(m.repos))

	// make sure any in-process deletions restart
	if !m.readOnly {
		for repoID, root := range m.repoToUUID {
			r, found := m.repos[root]
			if !found {
				return fmt.Errorf("could not find repo %s (repo ID %d)", root, repoID)
			}
			for name, data := range r.data {
				if data.IsDeleted() {
					if err := r.deleteData(name); err != nil {
						dvid.TimeCriticalf("tried to restart deletion of data %q but failed: %v\n", name, err)
					}
				}
			}
		}
	}
	return nil
}

func (m *repoManager) loadMetadata() error {
	// Check the version of the metadata
	found, err := m.loadData(formatKey, &(m.formatVersion))
	if err != nil {
		return fmt.Errorf("error in loading metadata format version: %v", err)
	}
	if found {
		dvid.TimeInfof("Loading metadata with format version %d...\n", m.formatVersion)
	} else {
		dvid.TimeInfof("Loading metadata without format version. Setting it to format version 0.\n")
		m.formatVersion = 0
	}

	switch m.formatVersion {
	case 0, 1:
		err = m.loadVersion0()
	default:
		err = fmt.Errorf("Unknown metadata format %d", m.formatVersion)
	}
	if err != nil {
		return err
	}
	for repoID, root := range m.repoToUUID {
		r, found := m.repos[root]
		if !found {
			return fmt.Errorf("could not find repo %s (repo ID %d)", root, repoID)
		}
		if err := r.initMutationID(m.store, m.mutationIDStart, m.readOnly); err != nil {
			return err
		}
		branchHeads := r.branchHeads()
		if len(branchHeads) > 0 {
			dvid.Infof("Caching branch heads for repo with root %s:\n", root)
			for branch, headUUID := range branchHeads {
				if branch == "" {
					branch = "master"
				}
				desc := string(root) + branch
				leaf, found := m.branchToUUID[desc]
				if found && leaf != headUUID {
					dvid.Errorf("Branch %q multiple leaves: %s and %s\n", branch, leaf, headUUID)
				} else {
					m.branchToUUID[desc] = headUUID
					dvid.Infof("Branch %q: UUID %s\n", branch, headUUID)
				}
			}
		}
	}
	saveIDs := false

	// Handle instance ID management
	if m.instanceIDGen == "sequential" && m.instanceIDStart > m.instanceID {
		m.instanceID = m.instanceIDStart
		saveIDs = true
	}

	// Handle version ID management, making sure our internal local version ID is
	// always greater than whatever we currently have.  (Corrects issues in metadata from early bug.)
	for v := range m.versionToUUID {
		if v > m.versionID {
			dvid.TimeErrorf("Found data version %d >= current new local version ID %d.  Correcting metadata...\n", v, m.versionID)
			m.versionID = v + 1
			saveIDs = true
		}
	}

	if saveIDs {
		return m.putNewIDs()
	}
	return nil
}

// TODO: Verify that the datatypes used by the repo data have been compiled into this server.
func (m *repoManager) verifyCompiledTypes() error {
	// Iterate over all data in all repo and check if present in Compiled
	return nil
}

// generates new instance ID.
func (m *repoManager) newInstanceID() (dvid.InstanceID, error) {
	var err error
	var curid dvid.InstanceID
	invalidID := true
	m.idMutex.Lock()
	for invalidID {
		switch m.instanceIDGen {
		case "sequential":
			curid = m.instanceID
			m.instanceID++
			err = m.putNewIDs()
		case "random":
			s1 := rand.NewSource(time.Now().UnixNano())
			r1 := rand.New(s1)
			curid = dvid.InstanceID(r1.Uint32())
		}
		_, found := m.iids[curid]
		if !found {
			invalidID = false
		}
	}
	m.idMutex.Unlock()
	return curid, err
}

func (m *repoManager) newRepoID() (dvid.RepoID, error) {
	m.idMutex.Lock()
	curid := m.repoID
	m.repoID++
	m.idMutex.Unlock()
	return curid, m.putNewIDs()
}

// newVersionID returns a new local VersionID for the given UUID.  Will return an error if
// the given UUID already exists locally, so mainly used in p2p transmission of data that
// keeps the remote UUID.  If save is true, will modify the repoManager mappings and persist.
func (m *repoManager) newVersionID(uuid dvid.UUID, save bool) (dvid.VersionID, error) {
	m.idMutex.RLock()
	_, found := m.uuidToVersion[uuid]
	m.idMutex.RUnlock()
	if found {
		return 0, fmt.Errorf("UUID %s already has a local version ID", uuid)
	}

	m.idMutex.Lock()
	curid := m.versionID
	m.versionID++
	if save {
		m.versionToUUID[curid] = uuid
		m.uuidToVersion[uuid] = curid
		m.idMutex.Unlock()
		if err := m.putCaches(); err != nil {
			return curid, err
		}
	} else {
		m.idMutex.Unlock()
	}
	return curid, m.putNewIDs()
}

// newUUID a local VersionID for either a provided UUID or if none is a provided, an
// automatically generated one.
func (m *repoManager) newUUID(assign *dvid.UUID) (dvid.UUID, dvid.VersionID, error) {
	var uuid dvid.UUID
	if assign == nil {
		uuid = dvid.NewUUID()
	} else {
		uuid = *assign
	}
	m.idMutex.Lock()
	curid := m.versionID
	m.versionToUUID[curid] = uuid
	m.uuidToVersion[uuid] = curid
	m.versionID++
	m.idMutex.Unlock()

	if err := m.putCaches(); err != nil {
		return uuid, curid, err
	}
	return uuid, curid, m.putNewIDs()
}

func (m *repoManager) uuidFromVersion(versionID dvid.VersionID) (dvid.UUID, error) {
	m.idMutex.RLock()
	uuid, found := m.versionToUUID[versionID]
	m.idMutex.RUnlock()
	if !found {
		return dvid.NilUUID, ErrInvalidVersion
	}
	return uuid, nil
}

func (m *repoManager) versionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	m.idMutex.RLock()
	versionID, found := m.uuidToVersion[uuid]
	m.idMutex.RUnlock()
	if !found {
		return 0, ErrInvalidUUID
	}
	return versionID, nil
}

// matchingUUID returns a local version ID and the full UUID from a potentially shortened UUID
// string. Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE...,
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
// If the passed string has a colon, the string after the colon is parsed as a
// case-sensitive branch name of the repo with the given UUID, and the UUID returned
// will be the HEAD or uncommitted leaf of that branch.
// Example 1: "3FA22:master" returns the leaf UUID of branch "master" for the repo containing
//
//	the 3FA22 UUID.
//
// Example 2: ":master" returns the leaf UUID of branch "master" if there is only one repo.
func (m *repoManager) matchingUUID(str string) (dvid.UUID, dvid.VersionID, error) {
	splits := strings.Split(str, ":")
	var branch string
	if len(splits) == 2 {
		branch = splits[1]
		if len(splits[0]) == 0 {
			return m.getBranchVersion(dvid.NilUUID, branch)
		}
		str = splits[0]
	} else if len(splits) > 2 {
		return dvid.NilUUID, 0, fmt.Errorf("bad UUID specification %q", str)
	}

	var bestVersion dvid.VersionID
	var bestUUID dvid.UUID
	numMatches := 0
	m.idMutex.RLock()
	for uuid, versionID := range m.uuidToVersion {
		if strings.HasPrefix(string(uuid), str) {
			numMatches++
			bestVersion = versionID
			bestUUID = uuid
		}
	}
	m.idMutex.RUnlock()

	if numMatches > 1 {
		return dvid.NilUUID, 0, fmt.Errorf("more than one UUID matches %s", str)
	} else if numMatches == 0 {
		return dvid.NilUUID, 0, fmt.Errorf("could not find UUID with partial match to %s", str)
	}

	if len(branch) != 0 {
		return m.getBranchVersion(bestUUID, branch)
	}
	return bestUUID, bestVersion, nil
}

// addRepo adds a preallocated repo with valid local instance and version IDs to
// the repoManager.
func (m *repoManager) addRepo(r *repoT) error {
	m.repoMutex.Lock()
	m.repos[r.uuid] = r
	m.repoMutex.Unlock()

	m.idMutex.Lock()
	m.repoToUUID[r.id] = r.uuid
	for _, dataservice := range r.data {
		iid := dataservice.InstanceID()
		m.iids[iid] = dataservice
		m.dataByUUID[dataservice.DataUUID()] = dataservice
	}
	for v, node := range r.dag.nodes {
		m.versionToUUID[v] = node.uuid
		m.uuidToVersion[node.uuid] = v
	}
	m.idMutex.Unlock()

	// Persist the changes
	if err := m.putCaches(); err != nil {
		return err
	}
	return r.save()
}

func (m *repoManager) deleteRepo(uuid dvid.UUID, passcode string) error {
	m.repoMutex.Lock()
	r, found := m.repos[uuid]
	m.repoMutex.Unlock()
	if !found {
		return ErrInvalidUUID
	}

	r.RLock()
	uuidMismatch := r.uuid != uuid
	passcodeMismatch := r.passcode != "" && r.passcode != passcode
	r.RUnlock()
	if uuidMismatch {
		return fmt.Errorf("UUID for repo deletion must match UUID of repo's root")
	} else if passcodeMismatch {
		return fmt.Errorf("Passcode does not match repo %s passcode", uuid)
	}

	// Start deletion of all data instances.
	r.RLock()
	for _, data := range r.data {
		go func(data dvid.Data) {
			if err := storage.DeleteDataInstance(data); err != nil {
				dvid.Errorf("Error trying to do async data instance %q deletion: %v\n", data.DataName(), err)
			}
		}(data)
	}
	r.RUnlock()

	// Delete the repo off the datastore.
	if err := r.delete(); err != nil {
		return fmt.Errorf("Unable to delete repo from datastore: %v", err)
	}

	// Delete all UUIDs in this repo from metadata
	m.idMutex.Lock()
	delete(m.repoToUUID, r.id)
	for v := range r.dag.nodes {
		u, found := m.versionToUUID[v]
		if !found {
			m.idMutex.Unlock()
			dvid.Errorf("Found version id %d with no corresponding UUID on delete of repo %s!\n", v, uuid)
			continue
		}
		m.repoMutex.Lock()
		delete(m.repos, u)
		m.repoMutex.Unlock()

		delete(m.uuidToVersion, u)
		delete(m.versionToUUID, v)
	}
	m.idMutex.Unlock()
	return nil
}

// ---- Repo-level properties functions -------

// repoFromUUID returns a repo given a UUID.  It will return an error if not found.
func (m *repoManager) repoFromUUID(uuid dvid.UUID) (*repoT, error) {
	m.repoMutex.RLock()
	repo, found := m.repos[uuid]
	m.repoMutex.RUnlock()
	if !found {
		return nil, fmt.Errorf("repo %s not found", uuid)
	}
	return repo, nil
}

// repoFromID returns a repo given a version id.
func (m *repoManager) repoFromVersion(v dvid.VersionID) (*repoT, error) {
	m.idMutex.RLock()
	uuid, found := m.versionToUUID[v]
	m.idMutex.RUnlock()
	if !found {
		return nil, ErrInvalidVersion
	}
	return m.repoFromUUID(uuid)
}

// repoFromID returns a repo given an id.
func (m *repoManager) repoFromID(repoID dvid.RepoID) (*repoT, error) {
	m.idMutex.RLock()
	uuid, found := m.repoToUUID[repoID]
	m.idMutex.RUnlock()
	if !found {
		return nil, ErrInvalidRepoID
	}

	m.repoMutex.RLock()
	repo, found := m.repos[uuid]
	m.repoMutex.RUnlock()
	if !found {
		return nil, ErrInvalidUUID
	}
	return repo, nil
}

// newRepo creates a new Repo with a new unique UUID unless one is provided as last parameter.
func (m *repoManager) newRepo(alias, description string, assign *dvid.UUID, passcode string) (*repoT, error) {
	if assign != nil {
		m.repoMutex.RLock()
		// Make sure there's not already a repo with this UUID.
		if _, found := m.repos[*assign]; found {
			m.repoMutex.RUnlock()
			return nil, ErrExistingUUID
		}
		m.repoMutex.RUnlock()
	}
	uuid, v, err := m.newUUID(assign)
	if err != nil {
		return nil, err
	}
	id, err := m.newRepoID()
	if err != nil {
		return nil, err
	}
	r := newRepo(uuid, v, id, passcode)

	m.idMutex.Lock()
	m.repoToUUID[id] = uuid
	m.idMutex.Unlock()
	if err := m.putCaches(); err != nil {
		return nil, err
	}

	m.repoMutex.Lock()
	m.repos[uuid] = r
	m.repoMutex.Unlock()

	r.alias = alias
	r.description = description

	if err := r.save(); err != nil {
		return r, err
	}
	if err := r.initMutationID(m.store, m.mutationIDStart, m.readOnly); err != nil {
		return r, err
	}
	dvid.Infof("Created and saved new repo %q, id %d\n", uuid, id)
	return r, nil
}

func (m *repoManager) saveRepoByUUID(uuid dvid.UUID) error {
	m.repoMutex.RLock()
	r, found := m.repos[uuid]
	m.repoMutex.RUnlock()
	if !found {
		return ErrInvalidUUID
	}
	return r.save()
}

func (m *repoManager) saveRepoByVersion(v dvid.VersionID) error {
	m.idMutex.RLock()
	uuid, found := m.versionToUUID[v]
	m.idMutex.RUnlock()
	if !found {
		return ErrInvalidVersion
	}
	return m.saveRepoByUUID(uuid)
}

// types returns a list of TypeService needed for this set of repositories
func (m *repoManager) types() (map[dvid.URLString]TypeService, error) {
	m.repoMutex.RLock()
	defer m.repoMutex.RUnlock()

	combinedMap := make(map[dvid.URLString]TypeService)
	for repoID, root := range m.repoToUUID {
		repo, found := m.repos[root]
		if !found {
			return nil, fmt.Errorf("could not find repo %s (repo ID %d)", root, repoID)
		}
		repoMap, err := repo.types()
		if err != nil {
			return combinedMap, err
		}
		for url, t := range repoMap {
			combinedMap[url] = t
		}
	}
	return combinedMap, nil
}

func (m *repoManager) getRepoRoot(uuid dvid.UUID) (dvid.UUID, error) {
	m.repoMutex.RLock()
	r, found := m.repos[uuid]
	m.repoMutex.RUnlock()
	if !found {
		return "", ErrInvalidUUID
	}
	return r.uuid, nil
}

func (m *repoManager) getRepoRootVersion(v dvid.VersionID) (dvid.VersionID, error) {
	m.idMutex.RLock()
	uuid, found := m.versionToUUID[v]
	m.idMutex.RUnlock()
	if !found {
		return 0, ErrInvalidVersion
	}

	m.repoMutex.RLock()
	r, found := m.repos[uuid]
	m.repoMutex.RUnlock()
	if !found {
		return 0, ErrInvalidVersion
	}
	return r.version, nil
}

func (m *repoManager) getRepoJSON(uuid dvid.UUID) (string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return "", err
	}

	jsonBytes, err := r.MarshalJSON()
	return string(jsonBytes), err
}

func (m *repoManager) getBranchVersionsJSON(uuid dvid.UUID, name string) (string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return "", err
	}
	ancestry, err := r.dag.getAncestryByBranch(name)
	if err != nil {
		return "", err
	}
	jsonStr := "["
	for i, ancestor := range ancestry {
		jsonStr += `"` + string(ancestor) + `"`
		if i != len(ancestry)-1 {
			jsonStr += ","
		}
	}
	jsonStr += "]"
	return jsonStr, nil
}

func (m *repoManager) getBranchVersions(uuid dvid.UUID, name string) ([]dvid.UUID, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	return r.dag.getAncestryByBranch(name)
}

// parse implicit HEAD request as well as positional parents via "~X" for the Xth parent of HEAD.
func (m *repoManager) getBranchVersion(uuid dvid.UUID, name string) (dvid.UUID, dvid.VersionID, error) {
	var r *repoT
	var err error
	if uuid == dvid.NilUUID {
		if len(m.repoToUUID) > 1 {
			return dvid.NilUUID, 0, fmt.Errorf("UUID must be specified if more than one repo exists")
		}
		for _, r = range m.repos {
			break
		}
	} else {
		r, err = m.repoFromUUID(uuid)
		if err != nil {
			return dvid.NilUUID, 0, err
		}
	}

	var branchUUID dvid.UUID
	splits := strings.Split(name, "~")
	if len(splits) == 2 {
		name = splits[0]
		uuidAncestry, err := r.dag.getAncestryByBranch(name)
		if err != nil {
			return dvid.NilUUID, 0, err
		}
		parent, err := strconv.Atoi(splits[1])
		if err != nil {
			return dvid.NilUUID, 0, fmt.Errorf("can't parse parent %q in branch %q", splits[1], name)
		}
		if parent >= len(uuidAncestry) {
			return dvid.NilUUID, 0, fmt.Errorf("parent %d is out of range for branch %q", parent, name)
		}
		branchUUID = uuidAncestry[parent]
	} else {
		var found bool
		m.branchMutex.RLock()
		branchUUID, found = m.branchToUUID[string(r.uuid)+name]
		m.branchMutex.RUnlock()
		if !found {
			return dvid.NilUUID, 0, fmt.Errorf("branch %q not found in repo %q", name, uuid)
		}
	}
	branchV, found := m.uuidToVersion[branchUUID]
	if !found {
		err := fmt.Errorf("branch %q had leaf UUID (%s) without a version ID", name, branchUUID)
		return dvid.NilUUID, 0, err
	}
	return branchUUID, branchV, nil
}

func (m *repoManager) getRepoAlias(uuid dvid.UUID) (string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return "", err
	}
	return r.alias, nil
}

func (m *repoManager) setRepoAlias(uuid dvid.UUID, alias string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	r.Lock()
	r.updated = time.Now()
	r.alias = alias
	r.Unlock()

	return r.save()
}

func (m *repoManager) getRepoDescription(uuid dvid.UUID) (string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return "", err
	}
	return r.description, nil
}

func (m *repoManager) setRepoDescription(uuid dvid.UUID, desc string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	r.Lock()
	r.updated = time.Now()
	r.description = desc
	r.Unlock()
	return r.save()
}

func (m *repoManager) getRepoProperty(uuid dvid.UUID, name string) (interface{}, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	r.RLock()
	value, found := r.properties[name]
	r.RUnlock()
	if !found {
		return nil, nil
	}
	return value, nil
}

func (m *repoManager) getRepoProperties(uuid dvid.UUID) (map[string]interface{}, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	r.RLock()
	props := make(map[string]interface{}, len(r.properties))
	for k, v := range r.properties {
		props[k] = v
	}
	r.RUnlock()
	return props, nil
}

func (m *repoManager) setRepoProperty(uuid dvid.UUID, name string, value interface{}) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	r.Lock()
	r.updated = time.Now()
	r.properties[name] = value
	r.Unlock()
	return r.save()
}

func (m *repoManager) setRepoProperties(uuid dvid.UUID, props map[string]interface{}) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	r.Lock()
	r.updated = time.Now()
	for k, v := range props {
		r.properties[k] = v
	}
	r.Unlock()
	return r.save()
}

func (m *repoManager) getRepoLog(uuid dvid.UUID) ([]string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}

	r.RLock()
	msgs := make([]string, len(r.log))
	copy(msgs, r.log)
	r.RUnlock()
	return msgs, nil
}

func (m *repoManager) addToRepoLog(uuid dvid.UUID, msgs []string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	t := time.Now()
	r.Lock()
	r.updated = t
	for _, msg := range msgs {
		message := fmt.Sprintf("%s  %s", t.Format(time.RFC3339), msg)
		r.log = append(r.log, message)
	}
	r.Unlock()
	return r.save()
}

// Node-level functions

func (m *repoManager) getNodeByVersion(v dvid.VersionID) (*nodeT, error) {
	uuid, err := m.uuidFromVersion(v)
	if err != nil {
		return nil, err
	}
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return nil, fmt.Errorf("version %d not found in repo %q", v, uuid)
	}
	return node, nil
}

func (m *repoManager) getNodeNote(uuid dvid.UUID) (string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return "", err
	}

	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return "", err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	if !found {
		r.RUnlock()
		return "", ErrInvalidVersion
	}
	note := node.note
	r.RUnlock()
	return note, nil
}

func (m *repoManager) setNodeNote(uuid dvid.UUID, note string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return ErrInvalidVersion
	}

	node.Lock()
	node.note = note
	t := time.Now()
	r.Lock()
	r.updated, node.updated = t, t
	r.Unlock()
	node.Unlock()
	return r.save()
}

func (m *repoManager) getNodeLog(uuid dvid.UUID) ([]string, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return nil, err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return nil, ErrInvalidVersion
	}

	node.RLock()
	msgs := make([]string, len(node.log))
	copy(msgs, node.log)
	node.RUnlock()
	return msgs, nil
}

func (m *repoManager) addToNodeLog(uuid dvid.UUID, msgs []string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return ErrInvalidVersion
	}

	if err := node.addToLog(msgs); err != nil {
		return err
	}
	t := time.Now()
	r.Lock()
	node.Lock()
	r.updated, node.updated = t, t
	node.Unlock()
	r.Unlock()
	return r.save()
}

// ---- Repo-level DAG functions -------

func (m *repoManager) getParentsByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	r, err := m.repoFromVersion(v)
	if err != nil {
		return nil, err
	}
	return r.dag.getParents(v)
}

func (m *repoManager) getChildrenByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	r, err := m.repoFromVersion(v)
	if err != nil {
		return nil, err
	}
	return r.dag.getChildren(v)
}

func (m *repoManager) lockedUUID(uuid dvid.UUID) (bool, error) {
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return false, err
	}
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return false, err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return false, ErrInvalidVersion
	}
	node.RLock()
	locked := node.locked
	node.RUnlock()
	return locked, nil
}

func (m *repoManager) lockedVersion(v dvid.VersionID) (bool, error) {
	r, err := m.repoFromVersion(v)
	if err != nil {
		return false, err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return false, ErrInvalidVersion
	}
	node.RLock()
	locked := node.locked
	node.RUnlock()
	return locked, nil
}

func (m *repoManager) commit(uuid dvid.UUID, note string, log []string) error {
	v, err := m.versionFromUUID(uuid)
	if err != nil {
		return err
	}
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	r.Lock()
	node, found := r.dag.nodes[v]
	r.Unlock()
	if !found {
		return ErrInvalidVersion
	}

	t := time.Now()

	node.Lock()
	node.locked = true
	if len(note) != 0 {
		node.note = note
	}
	node.Unlock()

	if len(log) != 0 {
		if err := node.addToLog(log); err != nil {
			return err
		}
	}

	// Notify any data instances in this repo that wants notification on node commit.
	r.RLock()
	for _, dataservice := range r.data {
		d, syncable := dataservice.(CommitSyncer)
		if syncable {
			go d.SyncOnCommit(uuid, v)
		}
	}
	r.RUnlock()

	r.Lock()
	node.Lock()
	r.updated, node.updated = t, t
	node.Unlock()
	r.Unlock()
	return r.save()
}

func (m *repoManager) hideBranch(uuid dvid.UUID, branch string) error {
	if branch == "" {
		return fmt.Errorf("cannot hide master branch")
	}
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	m.repoMutex.Lock()
	r.Lock()
	del_set := make(map[dvid.VersionID]struct{})
	for v, node := range r.dag.nodes {
		if node.branch == branch {
			del_set[v] = struct{}{}
			del_uuid := m.versionToUUID[v]
			delete(m.versionToUUID, v)
			delete(m.uuidToVersion, del_uuid)
			delete(r.dag.nodes, v)
			delete(m.repos, del_uuid)
		}
	}
	for _, node := range r.dag.nodes {
		var children []dvid.VersionID
		for _, cv := range node.children {
			if _, found := del_set[cv]; !found {
				children = append(children, cv)
			}
		}
		if len(children) < len(node.children) {
			node.children = children
		}
	}
	r.Unlock()
	m.repoMutex.Unlock()
	return r.save()
}

func (m *repoManager) makeMaster(newMasterUUID dvid.UUID, oldMasterBranchName string) error {
	if oldMasterBranchName == "" {
		return fmt.Errorf("renamed master branch cannot be empty string")
	}
	r, err := m.repoFromUUID(newMasterUUID)
	if err != nil {
		return err
	}
	newMasterV, err := m.versionFromUUID(newMasterUUID)
	if err != nil {
		return err
	}

	r.RLock()
	defer r.RUnlock()
	newMasterNode, found := r.dag.nodes[newMasterV]
	if !found {
		return fmt.Errorf("unable to find node with version %d, UUID %s", newMasterV, newMasterUUID)
	}
	if newMasterNode.branch == "" {
		return fmt.Errorf("designated node is already on the master branch")
	}

	// Get master branch that must be sibling.
	if len(newMasterNode.parents) == 0 {
		return fmt.Errorf("given UUID must be the first node of the branch to make master")
	}
	parentV := newMasterNode.parents[0]
	oldMasterNode, err := r.getChildBranchNode(parentV, "")
	if err != nil {
		return err
	}
	if oldMasterNode == nil {
		return fmt.Errorf("no sibling master branch found for UUID %s", newMasterUUID)
	}

	// Change the master nodes to the appropriate rename
	for {
		oldMasterNode.branch = oldMasterBranchName
		childNode, err := r.getChildBranchNode(oldMasterNode.version, "")
		if err != nil {
			return err
		}
		if childNode == nil {
			break
		}
		oldMasterNode = childNode
	}

	// Change the new master nodes to the master branch.
	oldBranchName := newMasterNode.branch
	for {
		newMasterNode.branch = ""
		childNode, err := r.getChildBranchNode(newMasterNode.version, oldBranchName)
		if err != nil {
			return err
		}
		if childNode == nil {
			break
		}
		newMasterNode = childNode
	}

	return r.save()
}

// newVersion creates a new version as a child of the given parent.  If the
// assign parameter is not nil, the new node is given the UUID.
func (m *repoManager) newVersion(parent dvid.UUID, note string, branchname string, assign *dvid.UUID) (dvid.UUID, error) {
	r, err := m.repoFromUUID(parent)
	if err != nil {
		return dvid.NilUUID, err
	}
	v, err := m.versionFromUUID(parent)
	if err != nil {
		return dvid.NilUUID, err
	}

	r.RLock()
	node, found := r.dag.nodes[v]
	r.RUnlock()
	if !found {
		return dvid.NilUUID, ErrInvalidVersion
	}

	node.RLock()
	defer node.RUnlock()
	if !node.locked {
		return dvid.NilUUID, ErrBranchUnlockedNode
	}

	// check to make sure there are not already
	// children with the same branch
	if branchname == "" || branchname == node.branch {
		// check other children nodes
		branchname = node.branch
		for _, sister := range node.children {
			// check if there is already a branch here
			r.RLock()
			r.dag.RLock()
			sisternode, found := r.dag.nodes[sister]
			r.dag.RUnlock()
			r.RUnlock()
			if !found {
				return dvid.NilUUID, fmt.Errorf("cannot find sibling nodes")
			}
			if sisternode.branch == branchname {
				return dvid.NilUUID, ErrBranchUnique
			}
		}
	} else { // check if branch name used anywhere in DAG
		r.RLock()
		for _, othernode := range r.dag.nodes {
			if othernode.branch == branchname {
				r.RUnlock()
				return dvid.NilUUID, ErrBranchUnique
			}
		}
		r.RUnlock()
	}

	// Add the child node.  Since it's new and unavailable, no need to lock it.
	childUUID, childV, err := m.newUUID(assign)
	if err != nil {
		return dvid.NilUUID, err
	}
	child := newNode(childUUID, childV)
	child.parents = []dvid.VersionID{v}
	child.note = note
	child.branch = branchname

	m.branchMutex.Lock()
	if branchname == "" {
		m.branchToUUID[string(r.uuid)+"master"] = childUUID
	} else {
		m.branchToUUID[string(r.uuid)+branchname] = childUUID
	}
	m.branchMutex.Unlock()

	m.repoMutex.Lock()
	m.repos[childUUID] = r
	m.repoMutex.Unlock()

	node.children = append(node.children, childV)
	node.updated = time.Now()

	r.Lock()
	r.dag.Lock()
	r.dag.nodes[childV] = child
	r.dag.Unlock()
	r.updated = time.Now()
	r.Unlock()

	// Notify data instances that we have a new child in case they have to do some kind of initialization.
	r.RLock()
	for _, dataservice := range r.data {
		initializer, ok := dataservice.(VersionInitializer)
		if ok {
			if err := initializer.InitVersion(childUUID, childV); err != nil {
				r.RUnlock()
				return dvid.NilUUID, err
			}
		}
	}
	r.RUnlock()

	return child.uuid, r.save()
}

func (m *repoManager) merge(parents []dvid.UUID, note string, mt MergeType) (dvid.UUID, error) {
	if len(parents) < 2 {
		return dvid.NilUUID, ErrInvalidUUID
	}

	m.repoMutex.RLock()
	r, found := m.repos[parents[0]]
	if !found {
		m.repoMutex.RUnlock()
		return dvid.NilUUID, ErrInvalidUUID
	}
	m.repoMutex.RUnlock()

	// Add the child node.  Since it's new and unavailable, no need to lock it.
	childUUID, childV, err := m.newUUID(nil)
	if err != nil {
		return dvid.NilUUID, err
	}
	child := newNode(childUUID, childV)
	child.note = note

	m.repoMutex.Lock()
	m.repos[childUUID] = r
	m.repoMutex.Unlock()

	r.Lock()
	r.dag.nodes[childV] = child
	r.Unlock()

	// Set up pointers with parents
	for _, parent := range parents {
		v, err := m.versionFromUUID(parent)
		if err != nil {
			return dvid.NilUUID, err
		}
		r.RLock()
		node, found := r.dag.nodes[v]
		r.RUnlock()
		if !found {
			return dvid.NilUUID, ErrInvalidVersion
		}

		node.Lock()
		if !node.locked {
			node.Unlock()
			return dvid.NilUUID, ErrBranchUnlockedNode
		}

		// Add this parent node
		child.parents = append(child.parents, v)
		node.children = append(node.children, childV)
		node.updated = time.Now()
		node.Unlock()
	}

	// Notify data instances that we have a new child in case they have to do some kind of initialization.
	r.RLock()
	for _, dataservice := range r.data {
		initializer, ok := dataservice.(VersionInitializer)
		if ok {
			if err := initializer.InitVersion(childUUID, childV); err != nil {
				r.RUnlock()
				return dvid.NilUUID, err
			}
		}
	}
	r.RUnlock()

	// TODO: we'd like to lock this child node but locked nodes have other
	//  side effects like the ability to be branched or cloned.  Perhaps add
	//  another node-level property saying it's read-only at this time, not
	//  for all time.  Could require separate API call to retrieve final child
	//  UUID given an immediately returned token.
	switch mt {
	case MergeConflictFree:
		// No processing needs to be done except for metadata changes.
		// Any issues will be noted during key-value lookup while traversing the DAG.

	case MergeTypeSpecificAuto:
		return dvid.NilUUID, fmt.Errorf("the type-specific auto merge has not been implemented yet")
		// go r.asyncMerge(parentNode1, parentNode2, child)

	case MergeExternalData:
		return dvid.NilUUID, fmt.Errorf("merging with external data has not been implemented yet")

	default:
		return dvid.NilUUID, ErrBadMergeType
	}

	r.Lock()
	r.updated = time.Now()
	r.Unlock()
	return child.uuid, r.save()
}

func (m *repoManager) invalidateAncestors(kvv kvVersions, v dvid.VersionID) error {
	parents, err := m.getParentsByVersion(v)
	if err != nil {
		return err
	}
	for _, parent := range parents {
		n, found := kvv[parent]
		if found {
			if n.invalid {
				continue
			}
			n.invalid = true
			kvv[parent] = n
		}
		if err := m.invalidateAncestors(kvv, parent); err != nil {
			return err
		}
	}
	return nil
}

// generate ancestor path from current version to root.
func (m *repoManager) getAncestry(v dvid.VersionID) ([]dvid.VersionID, error) {
	ancestors := []dvid.VersionID{v}
	cur := v
	for {
		parents, err := m.getParentsByVersion(cur)
		if err != nil {
			return nil, err
		}
		if len(parents) == 0 {
			break
		}
		cur = parents[0]
		ancestors = append(ancestors, cur)
	}
	return ancestors, nil
}

// recursive ancestor path following used to determine appropriate k/v pairs for given version.
func (m *repoManager) findMatch(kvv kvVersions, v dvid.VersionID) (*storage.KeyValue, dvid.VersionID, error) {
	// If we have a kv for this version, we're done.
	n, found := kvv[v]
	if found {
		if n.invalid {
			return nil, v, nil
		}
		if err := m.invalidateAncestors(kvv, v); err != nil {
			return nil, v, err
		}
		if n.kv.K.IsTombstone() {
			return nil, v, nil
		}
		return n.kv, v, nil
	}

	// If we have a single parent, ascend.
	parents, err := m.getParentsByVersion(v)
	if err != nil {
		return nil, v, err
	}
	switch len(parents) {
	case 0:
		// No kv here.
		return nil, 0, nil
	case 1:
		// Ascend the graph
		return m.findMatch(kvv, parents[0])
	default:
		// We have multiple parents so this is a merge.  Traverse each path up.
		var foundKV *storage.KeyValue
		var foundV dvid.VersionID
		foundVs := make(map[dvid.VersionID]struct{})
		for _, parent := range parents {
			matchKV, matchV, err := m.findMatch(kvv, parent)
			if err != nil {
				return nil, parent, err
			}
			if matchKV != nil && matchKV.K != nil && !matchKV.K.IsTombstone() {
				foundKV = matchKV
				foundV = matchV
				foundVs[matchV] = struct{}{}
			}
		}
		// Remove any matches that are in invalidated versions.
		badV := []dvid.VersionID{}
		for fv := range foundVs {
			n, found := kvv[fv]
			if !found {
				return nil, 0, fmt.Errorf("Got match (version %d) that wasn't in possible k/v!", fv)
			}
			if n.invalid {
				badV = append(badV, fv)
			}
		}
		if len(badV) > 0 {
			for _, bv := range badV {
				delete(foundVs, bv)
			}
		}
		// Make sure we have only one kv on all paths up because if we do not,
		// it's a failure in the past merge -- we should've had a kv at this
		// or lower nodes.
		switch len(foundVs) {
		case 0:
			return nil, 0, nil
		case 1:
			if foundKV.K == nil {
				return nil, 0, fmt.Errorf("found nil key in ascending version path for kv: %v", foundKV)
			}
			// Return nil if tombstone
			if foundKV.K.IsTombstone() {
				return nil, v, nil
			}
			// Else return found kv pair
			return foundKV, foundV, nil
		default:
			return nil, 0, fmt.Errorf("found multiple kv for key %v among parents: versions %v", foundKV.K, foundVs)
		}
	}
}

// ----- Repo-level data instance functions -----

func (m *repoManager) newData(uuid dvid.UUID, t TypeService, name dvid.InstanceName, c dvid.Config, save bool) (DataService, error) {
	id, err := m.newInstanceID()
	if err != nil {
		return nil, err
	}
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}

	// Only allow unique data name per repo
	r.RLock()
	if _, found := r.data[name]; found {
		r.RUnlock()
		return nil, fmt.Errorf("Data named %q already exists in repo (root %s)", name, r.uuid)
	}
	r.RUnlock()

	dataservice, err := t.NewDataService(uuid, id, name, c)
	if err != nil {
		return nil, err
	}

	m.idMutex.Lock()
	m.iids[id] = dataservice
	m.dataByUUID[dataservice.DataUUID()] = dataservice
	m.idMutex.Unlock()

	r.Lock()
	r.data[name] = dataservice
	tm := time.Now()
	r.updated = tm
	msg := fmt.Sprintf("New data instance %q of type %q with config %v", name, dataservice.TypeName(), c)
	message := fmt.Sprintf("%s  %s", tm.Format(time.RFC3339), msg)
	r.log = append(r.log, message)
	r.Unlock()

	// If it can be initialized (e.g., start sync handlers, etc), do it.
	initializer, initializable := dataservice.(DataInitializer)
	if initializable {
		err := initializer.InitDataHandlers()
		if err != nil {
			return nil, err
		}
		dvid.Infof("Initialized data handlers for instance %q on creating of new data.\n", dataservice.DataName())
	}

	if save {
		err = r.save()
	}
	return dataservice, err
}

// Replaces or appends to any previous syncs the given ones and sets up the sync graph for pub/sub.
func (m *repoManager) setSync(d dvid.Data, syncs dvid.UUIDSet, replace bool) error {
	r, err := m.repoFromUUID(d.RootUUID())
	if err != nil {
		return err
	}

	// handle case where we are deleting all syncs.
	newSyncs := make(dvid.UUIDSet)
	if len(syncs) == 0 {
		if !replace {
			return nil
		}
		d.SetSync(dvid.UUIDSet{})
		r.deleteSyncGraph(d, true)
		dvid.Infof("Removed all syncs from data instance %q...\n", d.DataName())
	} else {
		// handle case where we are modifying syncs.
		syncer, syncable := d.(Syncer)
		if !syncable {
			return fmt.Errorf("Can't create syncs for instance %q, which is not syncable: %v", d.DataName(), d)
		}

		var errmsg string
		newSubs := []SyncSubs{}
		newSyncs.Add(syncs)

		if !replace {
			newSyncs.Add(syncer.SyncedData())
		}

		for uuid := range newSyncs {
			syncedData, found := m.dataByUUID[uuid]
			if !found || syncedData.IsDeleted() {
				return ErrInvalidDataUUID
			}
			subs, err := syncer.GetSyncSubs(syncedData)
			if err != nil {
				errmsg = errmsg + "\n" + err.Error()
			} else {
				newSubs = append(newSubs, subs)
			}
		}

		if errmsg != "" {
			return fmt.Errorf("Unable to set syncs for data %q: %v\n", d.DataName(), errmsg)
		}
		r.deleteSyncGraph(d, true)
		d.SetSync(newSyncs)
		for _, subs := range newSubs {
			r.addSyncGraph(subs)
		}
	}

	r.Lock()
	tm := time.Now()
	r.updated = tm
	msg := fmt.Sprintf("Data instance %q set to sync with %s", d.DataName(), newSyncs)
	message := fmt.Sprintf("%s  %s", tm.Format(time.RFC3339), msg)
	r.log = append(r.log, message)
	r.Unlock()
	return r.save()
}

func (m *repoManager) getDataByInstanceID(id dvid.InstanceID) (DataService, error) {
	m.idMutex.RLock()
	d, found := m.iids[id]
	m.idMutex.RUnlock()
	if !found || d.IsDeleted() {
		return nil, ErrInvalidDataInstance
	}
	return d, nil
}

func (m *repoManager) getDataByDataUUID(dataUUID dvid.UUID) (DataService, error) {
	m.idMutex.RLock()
	d, found := m.dataByUUID[dataUUID]
	m.idMutex.RUnlock()
	if !found || d.IsDeleted() {
		return nil, ErrInvalidDataUUID
	}
	return d, nil
}

// Since only one data instance name can exist per repo, we can get repo from any uuid in DAG,
// then lookup by name.
func (m *repoManager) getDataByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (DataService, error) {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return nil, err
	}

	r.RLock()
	data, found := r.data[name]
	r.RUnlock()
	if !found || data.IsDeleted() {
		return nil, ErrInvalidDataName
	}
	return data, nil
}

func (m *repoManager) getDataByVersionName(v dvid.VersionID, name dvid.InstanceName) (DataService, error) {
	r, err := m.repoFromVersion(v)
	if err != nil {
		return nil, err
	}

	r.RLock()
	data, found := r.data[name]
	r.RUnlock()
	if !found || data.IsDeleted() {
		return nil, ErrInvalidDataName
	}
	return data, nil
}

func (m *repoManager) renameDataByName(uuid dvid.UUID, oldname, newname dvid.InstanceName, passcode string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	r.RLock()
	if r.passcode != "" && r.passcode != passcode {
		r.RUnlock()
		return fmt.Errorf("incorrect passcode for repo %s", r.uuid)
	}
	data, found := r.data[oldname]
	if !found || data.IsDeleted() {
		r.RUnlock()
		return ErrInvalidDataName
	}
	_, found = r.data[newname]
	if found {
		r.RUnlock()
		return ErrExistingDataName
	}
	r.RUnlock()

	// Rename this data instance in the repository and persist.
	r.Lock()
	tm := time.Now()
	r.updated = tm
	msg := fmt.Sprintf("Renamed data instance %q to %q", oldname, newname)
	message := fmt.Sprintf("%s  %s", tm.Format(time.RFC3339), msg)
	r.log = append(r.log, message)
	r.data[newname] = r.data[oldname]
	r.data[newname].SetName(newname)
	delete(r.data, oldname)
	r.Unlock()

	return r.save()
}

// deleteDataByName deletes all data associated with the data instance and removes
// it from the Repo.
func (m *repoManager) deleteDataByName(uuid dvid.UUID, name dvid.InstanceName, passcode string) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}
	return r.deleteDataWithPasscode(name, passcode)
}

func (m *repoManager) deleteDataByVersion(v dvid.VersionID, name dvid.InstanceName, passcode string) error {
	r, err := m.repoFromVersion(v)
	if err != nil {
		return err
	}
	return r.deleteDataWithPasscode(name, passcode)
}

// modifyData modifies preexisting Data within a Repo.  Settings can be passed
// via the 'config' argument.  Only settings within the passed config are modified.
func (m *repoManager) modifyDataByName(uuid dvid.UUID, name dvid.InstanceName, config dvid.Config) error {
	r, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	r.RLock()
	data, found := r.data[name]
	r.RUnlock()
	if !found || data.IsDeleted() {
		return ErrInvalidDataName
	}
	if err := data.ModifyConfig(config); err != nil {
		return err
	}
	return r.save()
}
