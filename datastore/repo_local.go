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
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
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
}

// Initialize creates a repositories manager that is handled through package functions.
func Initialize(initMetadata bool, iconfig Config) error {
	m := &repoManager{
		repoToUUID:      make(map[dvid.RepoID]dvid.UUID),
		versionToUUID:   make(map[dvid.VersionID]dvid.UUID),
		uuidToVersion:   make(map[dvid.UUID]dvid.VersionID),
		repos:           make(map[dvid.UUID]*repoT),
		repoID:          1,
		versionID:       1,
		iids:            make(map[dvid.InstanceID]DataService),
		dataByUUID:      make(map[dvid.UUID]DataService),
		instanceIDGen:   iconfig.InstanceGen,
		instanceIDStart: iconfig.InstanceStart,
		mutationIDStart: InitialMutationID,
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
	_, hastrans := m.store.(storage.TransactionDB)

	if hastrans {
		// check if metadata exists (globally locked so no other requests possible)
		// cannot trust initial value because of race conditions
		if found, _ := m.loadData(repoToUUIDKey, &(m.repoToUUID)); !found {
			initMetadata = true
		} else {
			initMetadata = false
		}
	}

	// TODO: parallelize metadata init fetches for high-latency backends
	if initMetadata {
		// Initialize repo management data in storage
		dvid.TimeInfof("Initializing repo management data in storage...\n")
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
		dvid.TimeInfof("Loading metadata from storage...\n")
		if err = m.loadMetadata(); err != nil {
			return fmt.Errorf("Error loading metadata: %v", err)
		}
	}
	for _, data := range m.iids {
		if data.IsDeleted() {
			continue
		}
		d, ok := data.(Initializer)
		if ok {
			go d.Initialize()
		}
	}
	// Set the package variable.  We are good to go...
	manager = m

	return nil
}

// MetadataUniversalLock locks shared databases (currently those implementing transactions)
func MetadataUniversalLock() error {
	// if db supports transaction, apply a system-wide lock and reload meta
	store, _ := storage.MetaDataKVStore()
	transdb, hastrans := store.(storage.TransactionDB)
	if hastrans {
		var ctx storage.MetadataContext
		key := ctx.ConstructKey(storage.NewTKey(ServerLockKey, nil))
		// TODO: automatically remove stale locks
		// to prevent accidentally getting locked out
		transdb.LockKey(key)

		if err := ReloadMetadata(); err != nil {
			transdb.UnlockKey(key)
			dvid.Criticalf("Can't reload metadata: %v\n", err)
			return fmt.Errorf("Can't reload metadata: %v\n", err)
		}
	}

	return nil
}

// MetadataUniversalUnlock releases the shared lock
func MetadataUniversalUnlock() {
	store, _ := storage.MetaDataKVStore()
	transdb, hastrans := store.(storage.TransactionDB)
	if hastrans {
		var ctx storage.MetadataContext
		key := ctx.ConstructKey(storage.NewTKey(ServerLockKey, nil))
		transdb.UnlockKey(key)
	}
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
	dvid.TimeInfof("Loading metadata from storage...\n")
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
		return fmt.Errorf("Bad value returned for new ids.  Length %d bytes!", len(value))
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
	var ctx storage.MetadataContext
	value := append(m.repoID.Bytes(), m.versionID.Bytes()...)
	value = append(value, m.instanceID.Bytes()...)
	return m.store.Put(ctx, storage.NewTKey(newIDsKey, nil), value)
}

func (m *repoManager) putCaches() error {
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
		_, found := m.repoToUUID[repoID]
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

			// Cache the assigned store.
			typename := dataservice.TypeName()
			store, err := storage.GetAssignedStore(dataname, dataservice.RootUUID(), dataservice.Tags(), typename)
			if err != nil {
				return err
			}
			dataservice.SetKVStore(store)

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
		if err := r.initMutationID(m.store, m.mutationIDStart); err != nil {
			return err
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
func (m *repoManager) matchingUUID(str string) (dvid.UUID, dvid.VersionID, error) {
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
	var err error
	if numMatches > 1 {
		err = fmt.Errorf("more than one UUID matches %s", str)
	} else if numMatches == 0 {
		err = fmt.Errorf("could not find UUID with partial match to %s", str)
	}
	return bestUUID, bestVersion, err
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
	if r.uuid != uuid {
		r.RUnlock()
		return fmt.Errorf("UUID for repo deletion must match UUID of repo's root")
	}
	if r.passcode != "" && r.passcode != passcode {
		r.RUnlock()
		return fmt.Errorf("Passcode does not match repo %s passcode", uuid)
	}

	// Start deletion of all data instances.
	for _, data := range r.data {
		go func(data dvid.Data) {
			if err := storage.DeleteDataInstance(data); err != nil {
				dvid.Errorf("Error trying to do async data instance %q deletion: %v\n", data.DataName(), err)
			}
		}(data)
	}
	r.Unlock()

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
	if err := r.initMutationID(m.store, m.mutationIDStart); err != nil {
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

	m.repoMutex.Lock()
	m.repos[childUUID] = r
	m.repoMutex.Unlock()

	node.children = append(node.children, childV)
	node.updated = time.Now()

	r.Lock()
	r.dag.nodes[childV] = child
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

func (m *repoManager) newData(uuid dvid.UUID, t TypeService, name dvid.InstanceName, c dvid.Config) (DataService, error) {
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

	return dataservice, r.save()
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

// repoT encapsulates everything we need to know about a repository.
// Note that changes to the DAG, e.g., adding a child node, will need updates
// to the cached maps in the RepoManager, so there is a pointer to it.
type repoT struct {
	sync.RWMutex // Currently, we lock entire repo for any changes since repo mods should be relatively infrequent

	id      dvid.RepoID
	uuid    dvid.UUID
	version dvid.VersionID

	// passcode, if supplied, must be used during deletes of repo
	// or data instances.
	passcode string

	// alias is an optional user-supplied string to identify this repo
	// in a more friendly way than a UUID.  There are no guarantees that
	// this string is unique across all repos.
	alias       string
	description string
	log         []string

	properties map[string]interface{}

	created time.Time
	updated time.Time

	dag *dagT

	data map[dvid.InstanceName]DataService

	// subs holds subscriptions to change events for each data instance.
	// This is not persisted.  It is built on load or modification of syncs.
	subs map[SyncEvent]SyncSubs

	// an atomic operation ID monotonically incremented per mutation and stored in separate kv
	mutCurID   uint64
	mutSavedID uint64
	mutMu      sync.RWMutex
}

// newRepo creates a new repository given a UUID, version, and RepoID,
// setting up the initial DAG with root node.
func newRepo(uuid dvid.UUID, v dvid.VersionID, id dvid.RepoID, passcode string) *repoT {
	t := time.Now()
	t = t.Round(0)
	dvid.Infof("new repo with passcode %s\n", passcode)
	repo := &repoT{
		id:         id,
		uuid:       uuid,
		version:    v,
		passcode:   passcode,
		log:        []string{},
		properties: make(map[string]interface{}),
		data:       make(map[dvid.InstanceName]DataService),
		created:    t,
		updated:    t,
	}
	repo.dag = newDAG(uuid, v)
	return repo
}

func (r *repoT) deleteDataWithPasscode(name dvid.InstanceName, passcode string) error {
	r.RLock()
	if r.passcode != "" && r.passcode != passcode {
		r.RUnlock()
		return fmt.Errorf("incorrect passcode for repo %s", r.uuid)
	}
	r.RUnlock()
	return r.deleteData(name)
}

func (r *repoT) deleteData(name dvid.InstanceName) error {
	r.RLock()
	data, found := r.data[name]
	r.RUnlock()
	if !found {
		return ErrInvalidDataName
	}
	data.SetDeleted(true)

	// For all data tiers of storage, remove data kv pairs associated with this instance id.
	go func() {
		if err := storage.DeleteDataInstance(data); err != nil {
			dvid.Errorf("Error trying to do async data instance deletion: %v\n", err)
		}

		// Delete entries in the sync graph if this data needs to be synced with another data instance.
		_, syncable := data.(Syncer)
		if syncable {
			r.deleteSyncGraph(data, false)
		}

		// Remove this data instance from the repository and persist.
		r.Lock()
		tm := time.Now()
		r.updated = tm
		msg := fmt.Sprintf("Delete data instance '%s' of type '%s'", name, data.TypeName())
		message := fmt.Sprintf("%s  %s", tm.Format(time.RFC3339), msg)
		r.log = append(r.log, message)
		delete(r.data, name)
		r.Unlock()
		r.save()
	}()

	return nil
}

// duplicate returns a duped repo optionally limited by the given
// version ID set and a list of data instance names.  Note that the
// underlying data instances aren't duplicated.
func (r *repoT) duplicate(versions map[dvid.VersionID]struct{}, names dvid.InstanceNames) (*repoT, error) {
	dup := new(repoT)
	dup.id = r.id

	// if the root is no longer an allowed version, we know it's a flattened.
	r.RLock()
	defer r.RUnlock()

	if _, found := versions[r.version]; found {
		dup.uuid = r.uuid
		dup.version = r.version
	} else {
		// Since this needs to be rerooted, data instances rerooted on remote reception.
		if len(versions) > 1 {
			dvid.Criticalf("r.duplicate() called with %d versions (> 1) but none are root\n", len(versions))
		}
		var v dvid.VersionID
		for v = range versions {
			break
		}
		dup.version = v
		dup.uuid = r.dag.nodes[v].uuid
		dvid.Debugf("duplicated restricted repo without root %s; using root %s\n", r.uuid, dup.uuid)
	}

	dup.alias = r.alias
	dup.description = r.description

	dup.log = make([]string, len(r.log))
	copy(dup.log, r.log)

	dup.properties = make(map[string]interface{}, len(r.properties))
	for k, v := range r.properties {
		dup.properties[k] = v
	}

	dup.created = r.created
	dup.updated = r.updated

	dup.dag = r.dag.duplicate(versions)

	if len(names) == 0 {
		dup.data = make(map[dvid.InstanceName]DataService, len(r.data))
		for k, v := range r.data {
			dup.data[k] = v
		}
	} else {
		dup.data = make(map[dvid.InstanceName]DataService, len(names))
		for _, name := range names {
			d, found := r.data[name]
			if !found {
				return nil, fmt.Errorf("cannot duplicate data instance %q which cannot be found", name)
			}
			dup.data[name] = d
		}
	}

	dup.subs = make(map[SyncEvent]SyncSubs, len(r.subs))
	for k, v := range r.subs {
		dup.subs[k] = v
	}

	return dup, nil
}

func (r *repoT) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(r.id)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.uuid)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.alias)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.description)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.log)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.properties)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.created)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.updated)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.data)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.dag)); err != nil {
		return err
	}
	// passcode may not exist.
	if err := dec.Decode(&(r.passcode)); err != nil {
		r.passcode = ""
	}
	r.version = r.dag.rootV
	return nil
}

func (r *repoT) GobEncode() ([]byte, error) {
	r.RLock()
	r.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.id); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.uuid); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.alias); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.description); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.log); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.properties); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.created); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.updated); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.data); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.dag); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.passcode); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *repoT) MarshalJSON() (b []byte, err error) {
	r.RLock()
	b, err = json.Marshal(struct {
		Root            dvid.UUID
		Alias           string
		Description     string
		Log             []string
		Properties      map[string]interface{}
		Data            map[dvid.InstanceName]DataService `json:"DataInstances"`
		DAG             *dagT
		MutationID      uint64
		SavedMutationID uint64
		Created         time.Time
		Updated         time.Time
	}{
		r.uuid,
		r.alias,
		r.description,
		r.log,
		r.properties,
		r.data,
		r.dag,
		r.mutCurID,
		r.mutSavedID,
		r.created,
		r.updated,
	})
	r.RUnlock()
	return
}

func (r *repoT) String() string {
	json, err := r.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("Repo print error: %v", err)
	}
	return string(json)
}

func (r *repoT) types() (map[dvid.URLString]TypeService, error) {
	datatypes := make(map[dvid.URLString]TypeService)
	r.RLock()
	for _, dataservice := range r.data {
		t := dataservice.GetType()
		datatypes[t.GetTypeURL()] = t
	}
	r.RUnlock()
	return datatypes, nil
}

// notifySubscribers sends a message to any data instances subscribed to the event.
func (r *repoT) notifySubscribers(e SyncEvent, m SyncMessage) error {
	r.RLock()
	subs, found := r.subs[e]
	r.RUnlock()
	if !found {
		return nil
	}
	for _, sub := range subs {
		sub.Ch <- m
	}
	return nil
}

func (r *repoT) save() error {
	if manager == nil {
		return fmt.Errorf("cannot use repo.save() before manager is initialized")
	}
	return r.saveToStore(manager.store)
}

func (r *repoT) saveToStore(db storage.OrderedKeyValueDB) error {
	if db == nil {
		return fmt.Errorf("cannot save repo to nil store")
	}
	r.RLock()
	compression, err := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	if err != nil {
		return err
	}
	serialization, err := dvid.Serialize(r, compression, dvid.CRC32)
	if err != nil {
		return err
	}
	tk := r.id.Bytes()
	r.RUnlock()

	var ctx storage.MetadataContext
	return db.Put(ctx, storage.NewTKey(repoKey, tk), serialization)
}

// deletes a Repo from the datastore
func (r *repoT) delete() error {
	var ctx storage.MetadataContext
	r.RLock()
	tk := storage.NewTKey(repoKey, r.id.Bytes())
	r.RUnlock()
	return manager.store.Delete(ctx, tk)
}

func (r *repoT) initMutationID(store storage.KeyValueDB, mutationIDStart uint64) error {
	var ctx storage.MetadataContext
	tk := storage.NewTKey(mutidKey, r.id.Bytes())
	mutdata, err := store.Get(ctx, tk)
	if err != nil {
		return err
	}
	if len(mutdata) == 8 {
		r.mutCurID = binary.LittleEndian.Uint64(mutdata)
		dvid.Infof("Loaded mutation ID for repo %s: %d\n", r.uuid, r.mutCurID)
	}
	if r.mutCurID < mutationIDStart {
		r.mutCurID = mutationIDStart
		dvid.Infof("Set mutation ID for repo %s to minimum set: %d\n", r.uuid, r.mutCurID)
	}
	r.mutSavedID = r.mutCurID + StrideMutationID
	mutdata = make([]byte, 8)
	binary.LittleEndian.PutUint64(mutdata, r.mutSavedID)
	if err := store.Put(ctx, tk, mutdata); err != nil {
		return err
	}
	return nil
}

func (r *repoT) getMutationID() (mutID uint64) {
	r.mutMu.RLock()
	mutID = r.mutCurID
	r.mutMu.RUnlock()
	return
}

func (r *repoT) newMutationID() (mutID uint64) {
	if manager == nil || manager.store == nil {
		dvid.Criticalf("Bad new mutation ID request.  Manager or store nil.\n")
		return
	}
	var ctx storage.MetadataContext
	r.mutMu.Lock()
	mutID = r.mutCurID
	r.mutCurID++
	if r.mutCurID >= r.mutSavedID {
		r.mutSavedID += StrideMutationID
		mutdata := make([]byte, 8)
		binary.LittleEndian.PutUint64(mutdata, r.mutSavedID)
		tk := storage.NewTKey(mutidKey, r.id.Bytes())
		if err := manager.store.Put(ctx, tk, mutdata); err != nil {
			dvid.Criticalf("Unable to persist new mutation ID for repo %s: %v\n", r.uuid, err)
		}
	}
	r.mutMu.Unlock()
	return
}

// relatively slow function compared to manager's cache, but can be used for
// shadow repos not tied into manager.
func (r *repoT) versionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	r.RLock()
	for v, node := range r.dag.nodes {
		if node.uuid == uuid {
			r.RUnlock()
			return v, nil
		}
	}
	r.RUnlock()
	return 0, ErrInvalidUUID
}

// Given a transmitted repo where you assume all local IDs (instance and version ids)
// are incorrect, make new local IDs and keep track of the mapping for later key updates.
// The current repo manager is NOT modified until addRepo().
func (r *repoT) remapLocalIDs() (dvid.InstanceMap, dvid.VersionMap, error) {
	if manager == nil {
		return nil, nil, ErrManagerNotInitialized
	}
	r.Lock()
	defer r.Unlock()

	// Convert the transmitted local ids to this DVID server's local ids.
	modifyManager := false
	instanceMap := make(dvid.InstanceMap, len(r.data))
	for dataname, dataservice := range r.data {
		instanceID, err := manager.newInstanceID()
		if err != nil {
			return nil, nil, err
		}
		instanceMap[dataservice.InstanceID()] = instanceID
		r.data[dataname].SetInstanceID(instanceID)
	}

	// Pass 1 on DAG: copy the nodes with new ids
	newNodes := make(map[dvid.VersionID]*nodeT, len(r.dag.nodes))
	versionMap := make(dvid.VersionMap, len(r.dag.nodes))
	for oldVersionID, nodePtr := range r.dag.nodes {
		// keep the old uuid but get a new version id
		newVersionID, err := manager.newVersionID(nodePtr.uuid, modifyManager)
		if err != nil {
			return nil, nil, err
		}
		versionMap[oldVersionID] = newVersionID
		newNodes[newVersionID] = nodePtr
	}

	// Pass 2 on DAG: now that we know the version mapping, modify all nodes.
	for _, nodePtr := range r.dag.nodes {
		nodePtr.version = versionMap[nodePtr.version]
		for i, oldVersionID := range nodePtr.parents {
			nodePtr.parents[i] = versionMap[oldVersionID]
		}
		for i, oldVersionID := range nodePtr.children {
			nodePtr.children[i] = versionMap[oldVersionID]
		}
	}
	r.dag.nodes = newNodes
	return instanceMap, versionMap, nil
}

// Adds subscriptions for data instance events. making sure that duplicates are avoided.
func (r *repoT) addSyncGraph(subs SyncSubs) {
	r.Lock()
	if r.subs == nil {
		r.subs = make(map[SyncEvent]SyncSubs)
	}
	for _, sub := range subs {
		_, found := r.subs[sub.Event]
		if !found {
			r.subs[sub.Event] = SyncSubs{sub}
		} else {
			r.subs[sub.Event] = r.subs[sub.Event].Add(sub)
		}
	}
	r.Unlock()
}

// Deletes subscriptions to and from a data instance unless the onlyFor parameter is true.
// This does not close whatever event handlers are running in a data instance, since
// these are closed on server Shutdown.
func (r *repoT) deleteSyncGraph(data dvid.Data, onlyFor bool) {
	if r.subs == nil {
		return
	}
	r.Lock()
	todelete := []SyncEvent{}
	for evt, subs := range r.subs {
		// Remove all subs to the named instance
		if !onlyFor && evt.Data == data.DataUUID() {
			r.subs[evt] = nil
			todelete = append(todelete, evt)
			continue
		}

		// Remove all subs for the named instance
		var deletions int
		for _, sub := range subs {
			if sub.Notify == data.DataUUID() {
				deletions++
			}
		}
		if len(subs) == deletions {
			r.subs[evt] = nil
			todelete = append(todelete, evt)
			continue
		}
		if deletions > 0 {
			newsubs := make([]SyncSub, len(subs)-deletions)
			j := 0
			for _, sub := range subs {
				if sub.Notify != data.DataUUID() {
					newsubs[j] = sub
					j++
				}
			}
			r.subs[evt] = newsubs
		}
	}
	for _, evt := range todelete {
		delete(r.subs, evt)
	}
	r.Unlock()
}

// makes a set of VersionID out of the current DAG
func (r *repoT) versionSet() map[dvid.VersionID]struct{} {
	r.RLock()
	if r.dag == nil || r.dag.nodes == nil || len(r.dag.nodes) == 0 {
		r.RUnlock()
		return nil
	}
	vset := make(map[dvid.VersionID]struct{}, len(r.dag.nodes))
	for v := range r.dag.nodes {
		vset[v] = struct{}{}
	}
	r.RUnlock()
	return vset
}

// --------------------------------------

// DataAvail gives the availability of data within a node or whether parent nodes
// must be traversed to check for key-value pairs.
type DataAvail uint8

const (
	// DataDelta = For any query, we must also traverse this node's ancestors in the DAG
	// up to any DataComplete ancestor.  Used if a node is marked as archived.
	DataDelta DataAvail = iota

	// DataComplete = All key-value pairs are available within this node.
	DataComplete

	// DataRoot = Queries are redirected to Root since this is unversioned.
	DataRoot

	// DataDeleted = key-value pairs have been explicitly deleted at this node and is no longer available.
	DataDeleted
)

func (avail DataAvail) String() string {
	switch avail {
	case DataDelta:
		return "Delta"
	case DataComplete:
		return "Complete Copy"
	case DataRoot:
		return "Unversioned"
	case DataDeleted:
		return "Deleted"
	default:
		dvid.Criticalf("Unknown DataAvail code %d in DataAvail.String()\n", avail)
		return "Unknown"
	}
}

// dagT implements a Directed Acyclic Graph where each node manages information
// about a version of data.
type dagT struct {
	sync.RWMutex
	root  dvid.UUID
	rootV dvid.VersionID
	nodes map[dvid.VersionID]*nodeT
}

func newDAG(uuid dvid.UUID, v dvid.VersionID) *dagT {
	return &dagT{
		root:  uuid,
		rootV: v,
		nodes: map[dvid.VersionID]*nodeT{
			v: newNode(uuid, v),
		},
	}
}

func (d *dagT) getAncestryByBranch(branch string) (ancestry []dvid.UUID, err error) {
	d.RLock()
	defer d.RUnlock()

	// find leaf for this branch.
	var leaf *nodeT
	for _, node := range d.nodes {
		if node.branch == branch || (branch == "master" && node.branch == "") {
			if len(node.children) == 0 {
				leaf = node
			}
		}
	}

	// start from leaf and work way up to root
	cur := leaf
	for {
		if cur == nil {
			break
		}
		ancestry = append(ancestry, cur.uuid)
		if len(cur.parents) == 0 {
			break
		}
		for i, parentV := range cur.parents {
			parent, found := d.nodes[parentV]
			if !found {
				err = fmt.Errorf("branch %q node %s has parent version %d that doesn't exist", cur.branch, cur.uuid, parentV)
				return
			}
			if i < len(cur.parents)-1 {
				ancestry = append(ancestry, parent.uuid)
			} else {
				cur = parent // we ascend the last parent in case of merged parents
			}
		}
	}
	return
}

// returns duplicate of DAG limited by any set of version IDs.  If the root UUID is not in the
// list of allowed versions, there must be only one version allowed (flattened)
func (d *dagT) duplicate(versions map[dvid.VersionID]struct{}) *dagT {
	dup := new(dagT)

	// if the root is no longer an allowed version, we know it's a flattened
	d.RLock()
	if _, found := versions[d.rootV]; found {
		dup.root = d.root
		dup.rootV = d.rootV
	} else {
		if len(versions) > 1 {
			dvid.Criticalf("dag.duplicate() called with %d versions but none are root\n", len(versions))
		}
		var v dvid.VersionID
		for v = range versions {
			break
		}
		dup.rootV = v
		dup.root = d.nodes[v].uuid
		dvid.Debugf("duplicated restricted DAG without root %s; using root %s\n", d.root, dup.root)
	}

	dup.nodes = make(map[dvid.VersionID]*nodeT, len(versions))
	for v := range versions {
		node, found := d.nodes[v]
		if !found {
			continue
		}
		dup.nodes[v] = node.duplicate(versions)
	}
	d.RUnlock()
	return dup
}

// ------  Serializations ----------

func (d *dagT) GobDecode(b []byte) error {
	d.nodes = make(map[dvid.VersionID]*nodeT)

	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.root)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.nodes)); err != nil {
		return err
	}
	// set the version of root by checking nodes
	var found bool
	for v, node := range d.nodes {
		if node.uuid == d.root {
			d.rootV = v
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("could not find node/versionID matching root UUID %s", d.root)
	}
	return nil
}

func (d *dagT) GobEncode() ([]byte, error) {
	d.RLock()
	defer d.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.root); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.nodes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *dagT) MarshalJSON() (b []byte, err error) {
	d.RLock()
	uuidMap := make(map[dvid.UUID]*nodeT)
	for _, node := range d.nodes {
		uuidMap[node.uuid] = node
	}
	b, err = json.Marshal(struct {
		Root  dvid.UUID
		Nodes map[dvid.UUID]*nodeT
	}{
		d.root,
		uuidMap,
	})
	d.RUnlock()
	return
}

func (d *dagT) String() string {
	json, err := d.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("DAG print error: %v", err)
	}
	return string(json)
}

func (d *dagT) getChildren(v dvid.VersionID) ([]dvid.VersionID, error) {
	d.RLock()
	node, found := d.nodes[v]
	if !found {
		d.RUnlock()
		return nil, fmt.Errorf("could not find version id %d", v)
	}
	children := make([]dvid.VersionID, len(node.children))
	copy(children, node.children)
	d.RUnlock()
	return children, nil
}

func (d *dagT) getParents(v dvid.VersionID) ([]dvid.VersionID, error) {
	d.RLock()
	node, found := d.nodes[v]
	if !found {
		d.RUnlock()
		return nil, fmt.Errorf("no version %d\n  d %s", v, d)
	}
	parents := make([]dvid.VersionID, len(node.parents))
	copy(parents, node.parents)
	d.RUnlock()
	return parents, nil
}

type nodeT struct {
	sync.RWMutex

	branch string
	note   string
	log    []string

	uuid    dvid.UUID
	version dvid.VersionID
	locked  bool

	// In the case of multiple parents, parents[0] is the default traversal for
	// an ancestor path.  It's assumed that any merger operation either creates
	// a DataComplete node or any delta is off one of the parents.
	parents  []dvid.VersionID
	children []dvid.VersionID

	created time.Time
	updated time.Time
}

// duplicate creates a duplicate node, limiting the data instances
// to passed versions if provided.  If a parent or child is
// not included in the versions, it is not copied.  Therefore if versions
// are supplied, they must be contiguous and not random nodes in DAG.
func (node *nodeT) duplicate(versions map[dvid.VersionID]struct{}) *nodeT {
	node.RLock()

	dup := new(nodeT)
	dup.branch = node.branch
	dup.note = node.note
	dup.log = make([]string, len(node.log))
	copy(dup.log, node.log)

	dup.uuid = node.uuid
	dup.version = node.version
	dup.locked = node.locked

	dup.parents = make([]dvid.VersionID, len(node.parents))
	dup.children = make([]dvid.VersionID, len(node.children))

	if len(versions) == 0 {
		copy(dup.parents, node.parents)
		copy(dup.children, node.children)
	} else {
		n := 0
		for _, parent := range node.parents {
			if _, found := versions[parent]; found {
				dup.parents[n] = parent
				n++
			}
		}
		dup.parents = dup.parents[:n]
		n = 0
		for _, child := range node.children {
			if _, found := versions[child]; found {
				dup.children[n] = child
				n++
			}
		}
		dup.children = dup.children[:n]
	}

	dup.created = node.created
	dup.updated = node.updated

	node.RUnlock()
	return dup
}

func (node *nodeT) GobDecode(b []byte) error {
	// Set zero values since gob doesn't transmit zero values down wire.
	node.log = []string{}
	node.parents = []dvid.VersionID{}
	node.children = []dvid.VersionID{}

	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(&(node.note)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.log)); err != nil {
		return err
	}

	// TODO - Deprecated and to be removed with full refactor of metadata
	avail := make(map[dvid.InstanceName]DataAvail)
	if err := dec.Decode(&avail); err != nil {
		return err
	}

	if err := dec.Decode(&(node.uuid)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.version)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.locked)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.parents)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.children)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.created)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.updated)); err != nil {
		return err
	}

	// support unspecified branches for legacy dvid instances
	dec.Decode(&(node.branch))

	return nil
}

func (node *nodeT) GobEncode() ([]byte, error) {
	node.RLock()
	defer node.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(node.note); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.log); err != nil {
		return nil, err
	}

	// Deprecated and to be removed with full refactor of metadata
	avail := make(map[dvid.InstanceName]DataAvail)
	if err := enc.Encode(avail); err != nil {
		return nil, err
	}

	if err := enc.Encode(node.uuid); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.version); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.locked); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.parents); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.children); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.created); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.updated); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.branch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (node *nodeT) MarshalJSON() (b []byte, err error) {
	node.RLock()
	b, err = json.Marshal(struct {
		Branch    string
		Note      string
		Log       []string
		UUID      dvid.UUID
		VersionID dvid.VersionID
		Locked    bool
		Parents   []dvid.VersionID
		Children  []dvid.VersionID
		Created   time.Time
		Updated   time.Time
	}{
		node.branch,
		node.note,
		node.log,
		node.uuid,
		node.version,
		node.locked,
		node.parents,
		node.children,
		node.created,
		node.updated,
	})
	node.RUnlock()
	return
}

func newNode(uuid dvid.UUID, versionID dvid.VersionID) *nodeT {
	t := time.Now()
	return &nodeT{
		log:      []string{},
		uuid:     uuid,
		version:  versionID,
		parents:  []dvid.VersionID{},
		children: []dvid.VersionID{},
		created:  t,
		updated:  t,
	}
}

func (node *nodeT) addToLog(msgs []string) error {
	node.Lock()
	t := time.Now()
	for _, msg := range msgs {
		message := fmt.Sprintf("%s  %s", t.Format(time.RFC3339), msg)
		node.log = append(node.log, message)
	}
	node.updated = t
	node.Unlock()
	return nil
}
