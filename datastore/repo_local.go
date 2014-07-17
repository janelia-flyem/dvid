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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datatype"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	ErrModifyLockedNode = errors.New("can't modify locked node")
)

// --- In the case of a single DVID process, return new ids requires only a lock.
// --- This becomes more tricky when dealing with multiple DVID processes working
// --- off shared storage engines.

func NewInstanceID() (InstanceID, error) {

}

func NewRepoID() (RepoID, error) {

}

func NewVersionID() (VersionID, error) {

}

// Manager manages all the repos in the datastore.
type Manager struct {
	sync.Mutex

	// Keep list of available Repo.  Could be much smaller than mapUUID which contains
	// all versions of all Repo.
	available []*Repo

	// Map all UUIDs to the repos from which they came.
	mapUUID map[dvid.UUID]*Repo

	// Map all local ids for repo nodes to the repos from which they came.
	nodeIDs map[dvid.NodeID]*Repo

	// Counters that provide the local IDs of the next new repo, version, or data instance.
	newRepoID     dvid.RepoID
	newVersionID  dvid.VersionID
	newInstanceID dvid.InstanceID
}

// DataServiceByUUID returns a service for data of a given name under a Repo referenced by UUID.
func (dsets *Manager) DataServiceByUUID(u dvid.UUID, name dvid.DataString) (DataService, error) {
	// Determine the repo that contains the node with this UUID
	repo, found := dsets.mapUUID[u]
	if !found {
		return nil, fmt.Errorf("No node with UUID %s found", u)
	}
	dataservice, err := repo.DataService(name)
	if err != nil {
		return nil, fmt.Errorf("No data named '%s' at node with UUID %s: %s", name, u, err.Error())
	}
	return dataservice, nil
}

// DataServiceByLocalID returns a service for data of a given name under a Repo referenced by local ID.
func (dsets *Manager) DataServiceByLocalID(id dvid.RepoLocalID, name dvid.DataString) (DataService, error) {
	// Determine the repo that contains the node with this UUID
	repo, found := dsets.dsetIDs[id]
	if !found {
		return nil, fmt.Errorf("No repo with local ID '%d' found", id)
	}
	dataservice, err := repo.DataService(name)
	if err != nil {
		return nil, fmt.Errorf("No data named '%s' at local repo ID %d: %s", name, id, err.Error())
	}
	return dataservice, nil
}

// NOTE: Alterations of Manager should be approached through datastore.Service since it
// will coordinate persistence of in-memory Manager as well as multiple storage engines.

// RepoFromUUID returns a repo given a UUID.
func (dsets *Manager) RepoFromUUID(u dvid.UUID) (*Repo, error) {
	repo, found := dsets.mapUUID[u]
	if !found {
		return nil, fmt.Errorf("RepoFromUUID(): Illegal UUID (%s) not found", u)
	}
	return repo, nil
}

// RepoFromLocalID returns a repo from a local repo ID.
func (dsets *Manager) RepoFromLocalID(id dvid.RepoLocalID) (*Repo, error) {
	repo, found := dsets.dsetIDs[id]
	if !found {
		return nil, fmt.Errorf("RepoFromLocalID(): Illegal local repo ID (%d) not found", id)
	}
	return repo, nil
}

// RepoFromString returns a repo from a UUID string.
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE...,
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (dsets *Manager) RepoFromString(str string) (repo *Repo, u dvid.UUID, err error) {
	numMatches := 0
	for dsetUUID, dset := range dsets.mapUUID {
		if strings.HasPrefix(string(dsetUUID), str) {
			numMatches++
			repo = dset
			u = dsetUUID
		}
	}
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", str)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", str)
	}
	return
}

// Datatypes returns a map of all unique data types where the key is the
// unique URL identifying the data type.  Since type names can collide
// across repos, we do not return the abbreviated data type names.
func (dsets *Manager) Datatypes() map[UrlString]datatype.Service {
	typemap := make(map[UrlString]datatype.Service)
	for _, dset := range dsets.list {
		for _, dataservice := range dset.DataMap {
			typemap[dataservice.DatatypeUrl()] = dataservice
		}
	}
	return typemap
}

// VerifyCompiledTypes will return an error if any required data type in the datastore
// configuration was not compiled into DVID executable.  Check is done by more exact
// URL and not the data type name.
func (dsets *Manager) VerifyCompiledTypes() error {
	var errMsg string
	for _, dset := range dsets.list {
		for name, data := range dset.DataMap {
			_, found := CompiledTypes[data.DatatypeUrl()]
			if !found {
				errMsg += fmt.Sprintf("DVID not compiled with support for %s, data type %s [%s]\n",
					name, data.DatatypeName(), data.DatatypeUrl())
			}
		}
	}
	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}
	return nil
}

// newRepo creates a new Repo, which constitutes a version DAG and allows storing
// arbitrary data within the nodes of the DAG.
func (dsets *Manager) newRepo() (dset *Repo, err error) {
	dsets.writeLock.Lock()

	dset = &Repo{
		VersionDAG: NewVersionDAG(),
		RepoID:     dsets.newRepoID,
	}
	dsets.newRepoID++
	dsets.list = append(dsets.list, dset)
	dsets.mapUUID[dset.Root] = dset
	dsets.dsetIDs[dset.RepoID] = dset

	dsets.writeLock.Unlock()
	return
}

// newChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dsets *Manager) newChild(parent dvid.UUID) (dset *Repo, u dvid.UUID, err error) {
	// Find the Repo with this UUID
	var found bool
	dset, found = dsets.mapUUID[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}

	// Create the child in this Repo's DAG
	u, err = dset.VersionDAG.newChild(parent)
	if err != nil {
		return
	}
	dsets.mapUUID[u] = dset
	return
}

// -- Manager Serialization and Deserialization ---

type serializableRepos struct {
	ReposUUID []dvid.UUID
	NewRepoID dvid.RepoLocalID
}

func (dsets *Manager) serializableStruct() (sdata *serializableRepos) {
	sdata = &serializableRepos{
		ReposUUID: []dvid.UUID{},
		NewRepoID: dsets.newRepoID,
	}
	for _, dset := range dsets.list {
		sdata.ReposUUID = append(sdata.ReposUUID, dset.Root)
	}
	return
}

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (dsets *Manager) MarshalBinary() ([]byte, error) {
	compression, err := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	if err != nil {
		return nil, err
	}
	return dvid.Serialize(dsets.serializableStruct(), compression, dvid.CRC32)
}

// Deserialize converts a serialization to Manager
func (dsets *Manager) deserialize(s []byte) (*serializableRepos, error) {
	deserialization := new(serializableRepos)
	err := dvid.Deserialize(s, deserialization)
	if err != nil {
		return nil, fmt.Errorf("Error in deserializing repos: %s", err.Error())
	}
	return deserialization, nil
}

// MarshalJSON returns the JSON of just the list of Repo.
func (dsets *Manager) MarshalJSON() (m []byte, err error) {
	return json.Marshal(dsets.serializableStruct())
}

// AllJSON returns JSON of all the repos information.
func (dsets *Manager) AllJSON() (m []byte, err error) {
	data := struct {
		Manager []*Repo
	}{
		dsets.list,
	}
	return json.Marshal(data)
}

// Load retrieves Manager and all referenced Repo from the storage engine.
func (dsets *Manager) Load(db storage.OrderedKeyValueGetter) (err error) {
	// Get the the map of all UUIDs to local repo IDs
	var data []byte
	data, err = db.Get(&ReposKey{})
	if err != nil {
		return
	}
	deserialization, err := dsets.deserialize(data)
	if err != nil {
		return err
	}

	// Get every Repo (range query)
	keyvalues, err := db.GetRange(MinRepoKey(), MaxRepoKey())
	if err != nil {
		return err
	}

	// Check our expected # of Repo == actually loaded # of Repo.
	if len(keyvalues) != len(deserialization.ReposUUID) {
		return fmt.Errorf("Stored Manager does not agree with the # of Repo entries: %d vs %d",
			len(deserialization.ReposUUID), len(keyvalues))
	}
	if int(deserialization.NewRepoID) < len(keyvalues) {
		return fmt.Errorf("Unexpected stored new repo ID %d < current # repos (%d)!",
			deserialization.NewRepoID, len(keyvalues))
	}
	dsets.newRepoID = deserialization.NewRepoID

	// Reconstruct the Manager by associating UUIDs.
	dsets.list = []*Repo{}
	dsets.mapUUID = make(map[dvid.UUID]*Repo)
	dsets.dsetIDs = make(map[dvid.RepoLocalID]*Repo)
	for _, value := range keyvalues {
		repo := new(Repo)
		err := dvid.Deserialize(value.V, repo)
		if err != nil {
			return err
		}
		dsets.list = append(dsets.list, repo)
		for u, _ := range repo.Nodes {
			dsets.mapUUID[u] = repo
		}
		dsets.dsetIDs[repo.RepoID] = repo
	}
	return
}

// Put stores Manager, overwriting whatever was there before.
func (dsets *Manager) Put(db storage.OrderedKeyValueSetter) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	// Get serialization
	serialization, err := dsets.MarshalBinary()
	if err != nil {
		return err
	}

	// Put data
	return db.Put(&ReposKey{}, serialization)
}

// Repo is a set of Data with an associated version DAG.
type Repo struct {
	dag *VersionDAG

	// alias is an optional user-supplied string to identify this repo
	// in a more friendly way than a UUID.  There are no guarantees that
	// this string is unique across all repos.
	alias string

	// instances keeps the repo-specific names for instances of data types.
	instances map[dvid.DataString]DataService
}

/*
// AddChildNode adds a new version as a child of the node with given UUID.
func (r *Repo) AddChildNode(uuid dvid.UUID) error {

}

// AddDataInstance adds a new instance of the given datatype.Service to the repo.
func (r *Repo) AddDataInstance(name dvid.DataString, ts datatype.Service, config dvid.Config) error {

}

// ModifyUnversionedInstance modifies unversioned properties of a data instance.
func (r *Repo) ModifyUnversionedInstance(name dvid.DataString, config dvid.Config) error {

}

// ModifyVersionedInstance modifies unversioned properties of a data instance.
func (r *Repo) ModifyVersionedInstance(name dvid.DataString, uuid dvid.UUID, config dvid.Config) error {
	// If locked node, return error.
}
*/

// TypeService returns the datatype.Service underlying data of a given name.
func (dset *Repo) TypeService(name dvid.DataString) (t datatype.Service, err error) {
	data, err := dset.DataService(name)
	if err != nil {
		err = fmt.Errorf("Cannot get type of unknown data '%s'", name)
		return
	}
	t = data.(datatype.Service)
	return
}

// DataService returns a DataService for data of a given name.
func (dset *Repo) DataService(name dvid.DataString) (dataservice DataService, err error) {
	var found bool
	dataservice, found = dset.DataMap[name]
	if !found {
		// Also allow numerical suffixes on names.
		for basename, service := range dset.DataMap {
			if strings.HasPrefix(string(name), string(basename)) {
				return service, nil
			}
		}
		err = fmt.Errorf("Cannot find data '%s'", name)
		return
	}
	return
}

// JSONString returns the JSON for this Data's configuration
func (dset *Repo) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(dset)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// Key returns a Key for this Repo
func (dset *Repo) Key() storage.Key {
	return &RepoKey{dset.RepoID}
}

// Put stores a Repo into a storage engine, overwriting whatever was there before.
func (dset *Repo) Put(db storage.OrderedKeyValueSetter) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	// Get serialization
	compression, err := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	serialization, err := dvid.Serialize(dset, compression, dvid.CRC32)
	if err != nil {
		return err
	}

	// Put data
	return db.Put(dset.Key(), serialization)
}

// newData adds a new, named instance of a data type to repo.  Settings can be passed
// via the 'config' argument.  For example, config["versioned"] will specify whether
// the data is mutable across nodes in the version DAG or is simply unversioned.
func (dset *Repo) newData(name dvid.DataString, typeName dvid.TypeString, config dvid.Config) error {
	// Only allow unique data names per repo.
	// TODO -- Do more elaborate check that prevents prefixing data names using
	// data types that allow different suffixes, e.g., multichannel data.
	dataservice, found := dset.DataMap[name]
	if found {
		return fmt.Errorf("Data named '%s' already exists in repo %s", name, dset.Root)
	}

	// Create new data for this repo.
	typeService, err := TypeServiceByName(typeName)
	if err != nil {
		return fmt.Errorf("No data type '%s' found [%s]", typeName, err)
	}

	dset.mapLock.Lock()
	defer dset.mapLock.Unlock()

	dataID := &Data{name, dset.NewDataID, dset.RepoID}
	dset.NewDataID++
	dataservice, err = typeService.NewDataService(dataID, config)
	if err != nil {
		return err
	}
	if dset.DataMap == nil {
		dset.DataMap = make(map[dvid.DataString]DataService)
	}
	dset.DataMap[name] = dataservice
	return nil
}

// modifyData modifies preexisting Data within a Repo.  Settings can be passed
// via the 'config' argument.  Only settings within the passed config are modified.
func (dset *Repo) modifyData(name dvid.DataString, config dvid.Config) error {
	dataservice, found := dset.DataMap[name]
	if !found {
		return fmt.Errorf("Data '%s' not found in repo %s", name, dset.Root)
	}

	dset.mapLock.Lock()
	defer dset.mapLock.Unlock()

	return dataservice.ModifyConfig(config)
}

// DataAvail gives the availability of data within a node or whether parent nodes
// must be traversed to check for key/value pairs.
type DataAvail int

const (
	// All key/value pairs are available within this node.
	DataComplete DataAvail = iota

	// For any range query, we must also traverse this node's ancestors in the DAG
	// up to any NodeComplete ancestor.  Used if a node is marked as archived.
	DataDelta

	// Queries are redirected to Root since this is unversioned.
	DataRoot

	// Data has been explicitly deleted at this node and is no longer available.
	DataDeleted
)

// NodeVersion contains all information for a node in the version DAG like its parents,
// children, and provenance.
type NodeVersion struct {
	// GlobalID is a globally unique id.
	GlobalID dvid.UUID

	// VersionID is a Repo-specific id for each UUID, so we can compress the UUIDs.
	VersionID dvid.VersionLocalID

	// Locked nodes are read-only and can be branched.
	Locked bool

	// Parents is an ordered list of parent nodes.
	Parents []dvid.UUID

	// Children is a list of child nodes.
	Children []dvid.UUID

	Created time.Time
	Updated time.Time
}

// NodeText holds provenance and other information useful for analysis.  It's
// possible that these structs could get large if useful provenance is large.
type NodeText struct {
	// Note holds general information on this node.
	Note string

	// Provenance describes the operations performed between the locking of
	// this node's parents and its current state.
	Provenance string
}

// Node contains all information needed at each node of the version DAG
type Node struct {
	*NodeVersion
	*NodeText

	// Avail is used for data compression/deltas in version DAG, depending on
	// type of data (e.g., versioned) and whether nodes are archived or not.
	// If there is no map or data availability is not explicitly set, we use
	// the default for that data, e.g., DataComplete if versioned or DataRoot
	// if unversioned.
	Avail map[dvid.DataString]DataAvail

	writeLock sync.Mutex
}

// VersionDAG is the directed acyclic graph of NodeVersion and an index by UUID into
// the graph.
type VersionDAG struct {
	Root  dvid.UUID
	Nodes map[dvid.UUID]*Node

	// VersionMap is used to accelerate mapping global UUID to DVID server-specific
	// and smaller ID for a version.
	VersionMap map[dvid.UUID]dvid.VersionLocalID

	NewVersionID dvid.VersionLocalID
	NewDataID    dvid.DataLocalID

	mapLock sync.Mutex // guards the VersionDAG maps
}

// NewVersionDAG creates a version DAG and initializes the first unlocked node,
// assigning its UUID.
func NewVersionDAG() *VersionDAG {
	dag := VersionDAG{
		Root:       dvid.NewUUID(),
		Nodes:      make(map[dvid.UUID]*Node),
		VersionMap: make(map[dvid.UUID]dvid.VersionLocalID),
	}
	t := time.Now()
	version := &NodeVersion{
		GlobalID:  dag.Root,
		VersionID: 0,
		Created:   t,
		Updated:   t,
	}
	dag.Nodes[dag.Root] = &Node{NodeVersion: version}
	dag.VersionMap[dag.Root] = 0
	dag.NewVersionID = 1
	return &dag
}

// Lock locks a node.  This is an irreversible operation since some nodes
// can be cloned externally.
func (dag *VersionDAG) Lock(u dvid.UUID) error {
	node, found := dag.Nodes[u]
	if !found {
		return fmt.Errorf("No node found with UUID %s", u)
	}
	node.Locked = true
	return nil
}

// newChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dag *VersionDAG) newChild(parent dvid.UUID) (u dvid.UUID, err error) {
	node, found := dag.Nodes[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}
	if !node.Locked {
		err = fmt.Errorf("Cannot create a child of an unlocked node %s", parent)
		return
	}

	u = dvid.NewUUID()
	t := time.Now()

	node.writeLock.Lock()
	node.Children = append(node.Children, u)
	node.Updated = t
	node.writeLock.Unlock()

	dag.mapLock.Lock()
	version := &NodeVersion{
		GlobalID:  u,
		VersionID: dag.NewVersionID,
		Created:   t,
		Updated:   t,
		Parents:   []dvid.UUID{parent},
	}
	dag.Nodes[u] = &Node{NodeVersion: version}
	dag.VersionMap[u] = version.VersionID
	dag.NewVersionID++
	dag.mapLock.Unlock()
	return
}

// LogInfo returns provenance information for all the version nodes.
func (dag *VersionDAG) LogInfo() string {
	text := "Versions:\n"
	for _, node := range dag.Nodes {
		text += fmt.Sprintf("%s  (%d)\n", node.GlobalID, node.VersionID)
	}
	return text
}

// Versions returns a slice of UUID within this version DAG.
func (dag *VersionDAG) Versions() []dvid.UUID {
	uuids := []dvid.UUID{}
	for u, _ := range dag.Nodes {
		uuids = append(uuids, u)
	}
	return uuids
}
