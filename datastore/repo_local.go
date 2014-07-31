// +build !clustered,!gcloud

/*
	This file contains code for handling a Repo, the basic unit of versioning in DVID,
	and Manager, a collection of Repo.  A Repo consists of a DAG where nodes can be
	optionally locked.

	For non-clustered, non-cloud ("local") DVID servers, we can get away with a simple
	in-memory implementation that persists to the MetadataStore when needed.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/dvid/storage/local"
)

func init() {
	gob.Register(&repoManager{})
	gob.Register(&repoT{})
	gob.Register(&dagT{})
	gob.Register(&nodeT{})
}

// --- In the case of a single DVID process, return new ids requires only a lock.
// --- This becomes more tricky when dealing with multiple DVID processes working
// --- off shared storage engines.

// repoManager manages all the repos in the datastore.
type repoManager struct {
	sync.Mutex // broad mutex should be sufficient since metadata is infrequently updated.

	// Map local RepoID to root UUID
	repoToUUID map[dvid.RepoID]dvid.UUID

	// Map local VersionID to UUID.  This also lets us know which nodes are available
	// in this DVID server since a subset of data can be pulled.
	versionToUUID map[dvid.VersionID]dvid.UUID

	// Map UUID to local VersionID -- this is not stored but generated on load
	UUIDToVersion map[dvid.UUID]dvid.VersionID

	// Counters that provide the local IDs of the next new repo, version, or data instance.
	newRepoID     dvid.RepoID
	newVersionID  dvid.VersionID
	newInstanceID dvid.InstanceID

	// Mapping of all UUIDs to the repositories where that node sits.
	repos map[dvid.UUID]*repoT

	// Verified metadata storage for ease of use.
	store storage.MetaDataStorer
}

// Create creates a local key-value store and if it is designated for
// metadata storage (metadata = true), also stores a blank RepoManager
// into the newly created key-value store.
func Create(path string, metadata bool, config dvid.Config) error {
	// Make the local key value store
	create := true
	kvEngine, err := local.NewKeyValueStore(path, create, config)
	if err != nil {
		return err
	}

	// Put a blank RepoManager onto the key value store.
	if metadata {
		m := &repoManager{
			repoToUUID:    make(map[dvid.RepoID]dvid.UUID),
			versionToUUID: make(map[dvid.VersionID]dvid.UUID),
			repos:         make(map[dvid.UUID]*repoT),
		}
		// Verify that our new kv engine satisfies a MetaDataStorer.
		var ok bool
		m.store, ok = kvEngine.(storage.MetaDataStorer)
		if !ok {
			return fmt.Errorf("Key-value store at %q cannot satisfy MetaData store", path)
		}
		// Store repo management data
		if err := m.putNewIDs(); err != nil {
			return err
		}
		if err := m.putCaches(); err != nil {
			return err
		}
	}
	return nil
}

// Repair repairs the datastore.  Currently this just launchs repair of the underlying
// storage engine.
func Repair(path string, config dvid.Config) error {
	return local.RepairStore(path, config)
}

// Initialize returns a RepoManager implementation suitable for managing repositories.
func Initialize() (RepoManager, error) {
	m := &repoManager{
		repoToUUID:    make(map[dvid.RepoID]dvid.UUID),
		versionToUUID: make(map[dvid.VersionID]dvid.UUID),
		repos:         make(map[dvid.UUID]*repoT),
	}

	var err error
	m.store, err = storage.MetaDataStore()
	if err != nil {
		return nil, err
	}

	// Try to load metadata from the MetaData store.
	if err = m.loadMetadata(); err != nil {
		return nil, err
	}
	return m, nil
}

// ---- RepoManager persistence to MetaData storage -----

func (m *repoManager) getData(t keyType, data interface{}) error {
	var ctx storage.MetadataContext
	idx := metadataIndex{t: t}
	value, err := m.store.Get(ctx, idx.Bytes())
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(data); err != nil {
		return err
	}
	return nil
}

func (m *repoManager) putData(t keyType, data interface{}) error {
	var ctx storage.MetadataContext
	idx := metadataIndex{t: t}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return err
	}
	return m.store.Put(ctx, idx.Bytes(), buf.Bytes())
}

// Load the next ids to be used for RepoID, VersionID, and InstanceID.
func (m *repoManager) getNewIDs() error {
	var ctx storage.MetadataContext
	idx := metadataIndex{t: newIDsKey}
	value, err := m.store.Get(ctx, idx.Bytes())
	if err != nil {
		return err
	}
	if len(value) != dvid.RepoIDSize+dvid.VersionIDSize+dvid.InstanceIDSize {
		return fmt.Errorf("Bad value returned for new ids.  Length %d bytes!", len(value))
	}
	pos := 0
	m.newRepoID = dvid.RepoIDFromBytes(value[pos : pos+dvid.RepoIDSize])
	pos += dvid.RepoIDSize
	m.newVersionID = dvid.VersionIDFromBytes(value[pos : pos+dvid.VersionIDSize])
	pos += dvid.VersionIDSize
	m.newInstanceID = dvid.InstanceIDFromBytes(value[pos : pos+dvid.InstanceIDSize])
	return nil
}

func (m *repoManager) putNewIDs() error {
	var ctx storage.MetadataContext
	idx := metadataIndex{t: newIDsKey}
	value := append(m.newRepoID.Bytes(), m.newVersionID.Bytes()...)
	value = append(value, m.newInstanceID.Bytes()...)
	return m.store.Put(ctx, idx.Bytes(), value)
}

func (m *repoManager) putCaches() error {
	if err := m.putData(repoToUUIDKey, m.repoToUUID); err != nil {
		return err
	}
	if err := m.putData(versionToUUIDKey, m.versionToUUID); err != nil {
		return err
	}
	return nil
}

// Loads all data necessary for repoManager.
func (m *repoManager) loadMetadata() error {
	// Load the maps
	if err := m.getData(repoToUUIDKey, &(m.repoToUUID)); err != nil {
		return err
	}
	if err := m.getNewIDs(); err != nil {
		return err
	}
	if err := m.getData(versionToUUIDKey, &(m.versionToUUID)); err != nil {
		return err
	}

	// Generate the inverse UUID to VersionID mapping.
	for versionID, uuid := range m.versionToUUID {
		m.UUIDToVersion[uuid] = versionID
	}

	// Load all the repo data
	var ctx storage.MetadataContext
	minIndex := metadataIndex{t: repoKey, repoID: dvid.RepoID(0)}
	maxIndex := metadataIndex{t: repoKey, repoID: dvid.MaxRepoID}
	kvList, err := m.store.GetRange(ctx, minIndex.Bytes(), maxIndex.Bytes())
	if err != nil {
		return err
	}

	var index metadataIndex
	for _, kv := range kvList {
		indexBytes, err := storage.DataContextIndex(kv.K)
		if err != nil {
			return err
		}
		err = index.IndexFromBytes(indexBytes)
		if err != nil {
			return err
		}
		// Load each repo
		_, found := m.repoToUUID[index.repoID]
		if !found {
			return fmt.Errorf("Retrieved repo with id %d that is not in map.  Corrupt DB?", index.repoID)
		}
		buf := bytes.NewBuffer(kv.V)
		dec := gob.NewDecoder(buf)
		repo := new(repoT)
		if err = dec.Decode(repo); err != nil {
			return err
		}
		// Cache all UUID from nodes into our high-level cache
		for versionID, _ := range repo.dag.nodes {
			uuid, found := m.versionToUUID[versionID]
			if !found {
				return fmt.Errorf("Version id %d found in repo %s (id %d) not in cache map",
					versionID, repo.rootID, repo.repoID)
			}
			m.repos[uuid] = repo
		}
	}
	dvid.Infof("Loaded %d repositories from metadata store.", len(m.repos))
	return m.verifyCompiledDatatypes()
}

// TODO: Verify that the datatypes used by the repo data have been compiled into this server.
func (m *repoManager) verifyCompiledDatatypes() error {
	// Iterate over all data in all repo and check if present in Compiled
	return nil
}

// ---- IDManager implementation -----------

func (m *repoManager) NewInstanceID() (dvid.InstanceID, error) {
	m.Lock()
	defer m.Unlock()
	curid := m.newInstanceID
	m.newInstanceID++
	return curid, m.putNewIDs()
}

func (m *repoManager) NewRepoID() (dvid.RepoID, error) {
	m.Lock()
	defer m.Unlock()
	curid := m.newRepoID
	m.newRepoID++
	return curid, m.putNewIDs()
}

// NewVersionID returns an atomically generated UUID and its associated local VersionID.
func (m *repoManager) NewVersionID() (dvid.UUID, dvid.VersionID, error) {
	m.Lock()
	defer m.Unlock()
	uuid := dvid.NewUUID()
	curid := m.newVersionID
	m.versionToUUID[curid] = uuid
	m.newVersionID++
	return uuid, curid, m.putNewIDs()
}

func (m *repoManager) UUIDFromVersion(versionID dvid.VersionID) (dvid.UUID, error) {
	uuid, found := m.versionToUUID[versionID]
	if !found {
		return dvid.NilUUID, fmt.Errorf("No UUID found for version id %d", versionID)
	}
	return uuid, nil
}

func (m *repoManager) VersionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	versionID, found := m.UUIDToVersion[uuid]
	if !found {
		return 0, fmt.Errorf("No version ID found for uuid %s", uuid)
	}
	return versionID, nil
}

// ---- RepoManager implementation

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
		m.UUIDToVersion[uuid] = versionID
	}
	if err := dec.Decode(&(m.newRepoID)); err != nil {
		return err
	}
	if err := dec.Decode(&(m.newVersionID)); err != nil {
		return err
	}
	if err := dec.Decode(&(m.newInstanceID)); err != nil {
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
	if err := enc.Encode(m.newRepoID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.newVersionID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.newInstanceID); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.repos); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *repoManager) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.repos)
}

// MatchingUUID returns a local version ID and the full UUID from a potentially shortened UUID
// string. Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE...,
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (m *repoManager) MatchingUUID(str string) (dvid.UUID, dvid.VersionID, error) {
	var bestVersion dvid.VersionID
	var bestUUID dvid.UUID
	numMatches := 0
	for uuid, versionID := range m.UUIDToVersion {
		if strings.HasPrefix(string(uuid), str) {
			numMatches++
			bestVersion = versionID
			bestUUID = uuid
		}
	}
	var err error
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", str)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", str)
	}
	return bestUUID, bestVersion, err
}

// RepoFromUUID returns a repo given a UUID.
func (m *repoManager) RepoFromUUID(uuid dvid.UUID) (Repo, error) {
	repo, found := m.repos[uuid]
	if !found {
		return nil, fmt.Errorf("RepoFromUUID(): Illegal UUID (%s) not found", uuid)
	}
	return repo, nil
}

// RepoFromUUID returns a repo given a UUID.
func (m *repoManager) RepoFromID(repoID dvid.RepoID) (Repo, error) {
	uuid, found := m.repoToUUID[repoID]
	if !found {
		return nil, fmt.Errorf("RepoFromID(): Illegal RepoID (%d) used, not found.", repoID)
	}
	repo, found := m.repos[uuid]
	if !found {
		return nil, fmt.Errorf("RepoFromID(): Illegal UUID (%s) not found", uuid)
	}
	return repo, nil
}

// NewRepo creates a new Repo with a unique UUID
func (m *repoManager) NewRepo() (Repo, error) {
	repo, versionID, err := newRepo(m)
	if err != nil {
		return nil, err
	}
	m.Lock()
	defer m.Unlock()
	uuid := repo.RootUUID()
	m.repos[uuid] = repo
	m.versionToUUID[versionID] = uuid
	m.UUIDToVersion[uuid] = versionID
	return repo, m.putCaches()
}

// Datatypes returns a list of TypeService needed for this set of repositories
func (m *repoManager) Datatypes() (map[URLString]TypeService, error) {
	combinedMap := make(map[URLString]TypeService)
	for _, repo := range m.repos {
		repoMap, err := repo.Datatypes()
		if err != nil {
			return combinedMap, err
		}
		for url, t := range repoMap {
			combinedMap[url] = t
		}
	}
	return combinedMap, nil
}

// repoT encapsulates everything we need to know about a repository.
// Note that changes to the DAG, e.g., adding a child node, will need updates
// to the cached maps in the RepoManager, so there is a pointer to it.
type repoT struct {
	repoID dvid.RepoID
	rootID dvid.UUID

	// alias is an optional user-supplied string to identify this repo
	// in a more friendly way than a UUID.  There are no guarantees that
	// this string is unique across all repos.
	alias       string
	description string
	log         []string

	properties map[string]interface{}
	dag        *dagT

	// data holds instances of data types.
	data map[dvid.DataString]DataService

	// necessary to update cached maps based on changes to DAG and data instances.
	manager *repoManager
	mu      sync.Mutex
}

// newRepo creates a new repository, updating the version id within the RepoManager.
func newRepo(m *repoManager) (*repoT, dvid.VersionID, error) {
	repoID, err := m.NewRepoID()
	if err != nil {
		return nil, 0, err
	}
	uuid, versionID, err := m.NewVersionID()
	if err != nil {
		return nil, 0, err
	}
	repo := &repoT{
		repoID:     repoID,
		rootID:     uuid,
		log:        []string{},
		properties: make(map[string]interface{}),
		data:       make(map[dvid.DataString]DataService),
		manager:    m,
	}
	repo.dag, err = repo.newDAG(uuid, versionID)
	return repo, versionID, err
}

func (r *repoT) store() error {
	var ctx storage.MetadataContext
	idx := metadataIndex{t: repoKey, repoID: r.repoID}
	serialization, err := r.GobEncode()
	if err != nil {
		return err
	}
	return r.manager.store.Put(ctx, idx.Bytes(), serialization)
}

// ---- Describer interface implementation ----------

func (r *repoT) GetAlias() string {
	return r.alias
}

func (r *repoT) SetAlias(alias string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alias = alias
	return r.store()
}

func (r *repoT) GetDescription() string {
	return r.description
}

func (r *repoT) SetDescription(desc string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.description = desc
	return r.store()
}

// For local implementation, no error is possible, just whether it's found or not
func (r *repoT) GetProperty(name string) (interface{}, error) {
	value, found := r.properties[name]
	if !found {
		return nil, nil
	}
	return value, nil
}

func (r *repoT) GetProperties() (map[string]interface{}, error) {
	return r.properties, nil
}

func (r *repoT) SetProperty(name string, value interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.properties[name] = value
	return r.store()
}

func (r *repoT) SetProperties(props map[string]interface{}) error {
	for k, v := range props {
		r.properties[k] = v
	}
	return nil
}

func (r *repoT) GetLog() ([]string, error) {
	return r.log, nil
}

func (r *repoT) AddToLog(hx string) error {
	r.mu.Lock()
	r.log = append(r.log, hx)
	r.mu.Unlock()
	return nil
}

// ---- Repo interface implementation -----------

func (r *repoT) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(r.alias)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.description)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.repoID)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.rootID)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.properties)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.data)); err != nil {
		return err
	}
	if err := dec.Decode(&(r.dag)); err != nil {
		return err
	}
	return nil
}

func (r *repoT) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.alias); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.description); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.repoID); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.rootID); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.properties); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.data); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.dag); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *repoT) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Root        dvid.UUID
		Alias       string
		Description string
		Log         []string
		Properties  map[string]interface{}
		Data        map[dvid.DataString]DataService
		DAG         *dagT
	}{
		r.rootID,
		r.alias,
		r.description,
		r.log,
		r.properties,
		r.data,
		r.dag,
	})
}

func (r *repoT) RepoID() dvid.RepoID {
	return r.repoID
}

func (r *repoT) RootUUID() dvid.UUID {
	return r.rootID
}

func (r *repoT) GetDataByName(name dvid.DataString) (DataService, error) {
	elements := strings.Split(string(name), "-")
	stem := elements[0]
	data, found := r.data[dvid.DataString(stem)]
	if !found {
		return nil, nil
	}
	return data, nil
}

func (r *repoT) GetIterator(versionID dvid.VersionID) (dvid.VersionIterator, error) {
	return r.dag.GetIterator(versionID)
}

func (r *repoT) NewData(t TypeService, name dvid.DataString, c dvid.Config) (DataService, error) {
	// Only allow unique data name per repo
	if _, found := r.data[name]; found {
		return nil, fmt.Errorf("Data named %q already exists in repo (root %s)", name, r.rootID)
	}
	instanceID, err := r.manager.NewInstanceID()
	if err != nil {
		return nil, err
	}
	dataservice, err := t.NewDataService(r, instanceID, name, c)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[name] = dataservice
	return dataservice, r.store()
}

// ModifyData modifies preexisting Data within a Repo.  Settings can be passed
// via the 'config' argument.  Only settings within the passed config are modified.
func (r *repoT) ModifyData(name dvid.DataString, config dvid.Config) error {
	dataservice, err := r.GetDataByName(name)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return dataservice.ModifyConfig(config)
}

func (r *repoT) NewChild(uuid dvid.UUID) (dvid.UUID, error) {
	// Make sure parent is available and locked.
	parentVersionID, found := r.manager.UUIDToVersion[uuid]
	if !found {
		return dvid.NilUUID, fmt.Errorf("No parent version found with uuid %s", uuid)
	}
	parentNode, found := r.dag.nodes[parentVersionID]
	if !found {
		return dvid.NilUUID, fmt.Errorf("No parent version found with uuid %s (version %d)", uuid,
			parentVersionID)
	}
	if !parentNode.locked {
		return dvid.NilUUID, fmt.Errorf("Cannot create child on unlocked parent node %s", uuid)
	}

	// Add the child node.  Since it's new and unavailable, no need to lock it.
	childNode, err := r.newNode()
	if err != nil {
		return dvid.NilUUID, err
	}
	childNode.parents = []dvid.VersionID{parentVersionID}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dag.nodes[childNode.versionID] = childNode

	parentNode.Lock()
	parentNode.children = append(parentNode.children, childNode.versionID)
	parentNode.updated = time.Now()
	parentNode.Unlock()
	return childNode.uuid, r.store()
}

func (r *repoT) Lock(uuid dvid.UUID) error {
	versionID, found := r.manager.UUIDToVersion[uuid]
	if !found {
		return fmt.Errorf("Could not LOCK missing version (uuid %s)", uuid)
	}
	node, found := r.dag.nodes[versionID]
	if !found {
		return fmt.Errorf("Could not LOCK missing version (id %d)", versionID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node.locked = true
	return r.store()
}

func (r *repoT) Datatypes() (map[URLString]TypeService, error) {
	datatypes := make(map[URLString]TypeService)
	for _, dataservice := range r.data {
		t := dataservice.DataType()
		datatypes[t.TypeURL()] = t
	}
	return datatypes, nil
}

func (r *repoT) newDAG(uuid dvid.UUID, versionID dvid.VersionID) (*dagT, error) {
	dag := &dagT{root: uuid}
	node, err := r.newNode()
	if err != nil {
		return nil, err
	}
	dag.nodes = map[dvid.VersionID]*nodeT{
		versionID: node,
	}
	return dag, nil
}

func (r *repoT) newNode() (*nodeT, error) {
	uuid, versionID, err := r.manager.NewVersionID()
	if err != nil {
		return nil, err
	}
	t := time.Now()
	node := &nodeT{
		uuid:      uuid,
		versionID: versionID,
		created:   t,
		updated:   t,
	}
	r.manager.Lock()
	r.manager.repos[uuid] = r
	r.manager.Unlock()
	return node, nil
}

// --------------------------------------

// DataAvail gives the availability of data within a node or whether parent nodes
// must be traversed to check for key-value pairs.
type DataAvail uint8

const (
	// For any query, we must also traverse this node's ancestors in the DAG
	// up to any DataComplete ancestor.  Used if a node is marked as archived.
	DataDelta DataAvail = iota

	// All key-value pairs are available within this node.
	DataComplete

	// Queries are redirected to Root since this is unversioned.
	DataRoot

	// Data has been explicitly deleted at this node and is no longer available.
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
		dvid.Criticalf("Unknown DataAvail (%v) with String()\n", avail)
		return "Unknown"
	}
}

// dagT implements a Directed Acyclic Graph where each node manages information
// about a version of data.
type dagT struct {
	root  dvid.UUID
	nodes map[dvid.VersionID]*nodeT
}

func (dag *dagT) GetIterator(versionID dvid.VersionID) (dvid.VersionIterator, error) {
	node, found := dag.nodes[versionID]
	if !found {
		return nil, fmt.Errorf("No version found with id %d", versionID)
	}
	return &versionIterator{dag, true, versionID, node}, nil
}

func (dag *dagT) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(dag.root)); err != nil {
		return err
	}
	if err := dec.Decode(&(dag.nodes)); err != nil {
		return err
	}
	return nil
}

func (dag *dagT) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(dag.root); err != nil {
		return nil, err
	}
	if err := enc.Encode(dag.nodes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (dag *dagT) MarshalJSON() ([]byte, error) {
	// Create temporary map using UUID instead of local version IDs.
	uuidMap := make(map[dvid.UUID]*nodeT)
	for _, node := range dag.nodes {
		uuidMap[node.uuid] = node
	}
	return json.Marshal(struct {
		Root  dvid.UUID
		Nodes map[dvid.UUID]*nodeT
	}{
		dag.root,
		uuidMap,
	})
}

type nodeT struct {
	sync.Mutex

	note string
	log  []string

	// avail is used for data compression/deltas in version DAG, depending on
	// type of data (e.g., versioned) and whether nodes are archived or not.
	// If there is no map or data availability is not explicitly set, we use
	// the default for that data, e.g., DataComplete if versioned or DataRoot
	// if unversioned.
	avail map[dvid.DataString]DataAvail

	uuid      dvid.UUID
	versionID dvid.VersionID
	locked    bool

	// In the case of multiple parents, parents[0] is the default traversal for
	// an ancestor path.  It's assumed that any merger operation either creates
	// a DataComplete node or any delta is off one of the parents.
	parents  []dvid.VersionID
	children []dvid.VersionID

	created time.Time
	updated time.Time
}

func (node *nodeT) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(node.note)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.log)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.avail)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.uuid)); err != nil {
		return err
	}
	if err := dec.Decode(&(node.versionID)); err != nil {
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
	return nil
}

func (node *nodeT) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(node.note); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.log); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.avail); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.uuid); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.versionID); err != nil {
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
	return buf.Bytes(), nil
}

func (node *nodeT) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Note      string
		Log       []string
		Data      map[dvid.DataString]DataAvail
		UUID      dvid.UUID
		VersionID dvid.VersionID
		Locked    bool
		Parents   []dvid.VersionID
		Children  []dvid.VersionID
		Created   time.Time
		Updated   time.Time
	}{
		node.note,
		node.log,
		node.avail,
		node.uuid,
		node.versionID,
		node.locked,
		node.parents,
		node.children,
		node.created,
		node.updated,
	})
}

// ----- dvid.VersionIterator implementation

type versionIterator struct {
	dag        *dagT
	valid      bool
	curVersion dvid.VersionID
	curNode    *nodeT
}

func (it *versionIterator) Valid() bool {
	return it.valid
}

func (it *versionIterator) VersionID() dvid.VersionID {
	return it.curVersion
}

func (it *versionIterator) Next() {
	if len(it.curNode.parents) == 0 {
		it.valid = false
		return
	}
	curVersion := it.curNode.parents[0]
	node, found := it.dag.nodes[curVersion]
	if found {
		it.curNode = node
		it.curVersion = curVersion
	} else {
		it.valid = false
	}
}
