/*
	This file contains code for the Dataset, a version DAG and all the Data within its
	nodes.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/go-uuid/uuid"
)

type nodeID struct {
	Dataset DatasetLocalID
	Data    DataLocalID
	Version VersionLocalID
}

// Map of mutexes at the granularity of dataset/data/version
var versionMutexes map[nodeID]*sync.Mutex

func init() {
	versionMutexes = make(map[nodeID]*sync.Mutex)
}

func VersionMutex(data DataService, versionID VersionLocalID) (vmutex *sync.Mutex) {
	var mutex sync.Mutex
	mutex.Lock()
	id := nodeID{data.DatasetID(), data.LocalID(), versionID}
	var found bool
	vmutex, found = versionMutexes[id]
	if !found {
		vmutex = new(sync.Mutex)
		versionMutexes[id] = vmutex
	}
	mutex.Unlock()
	return
}

// Datasets holds information on all the Dataset available.
type Datasets struct {
	writeLock sync.Mutex // guards the fields below

	// Keep list of Dataset.  Could be much smaller than mapUUID which contains
	// all versions of all Dataset.
	list []*Dataset

	// Efficiently maps all UUIDs to the version DAG from which it came.
	mapUUID map[UUID]*Dataset

	// Counter that provides the local ID of the next new dataset.
	newDatasetID DatasetLocalID
}

// DataService returns a service for data of a given name under a Dataset.
func (dsets *Datasets) DataService(u UUID, name DataString) (dataservice DataService, err error) {
	// Determine the dataset that contains the node with this UUID
	dataset, found := dsets.mapUUID[u]
	if !found {
		err = fmt.Errorf("No node with UUID %s found", u)
		return
	}
	dataservice, err = dataset.DataService(name)
	if err != nil {
		err = fmt.Errorf("No data named '%s' at node with UUID %s: %s", name, u, err.Error())
	}
	return
}

// NOTE: Alterations of Datasets should be approached through datastore.Service since it
// will coordinate persistence of in-memory Datasets as well as multiple storage engines.

// DatasetFromUUID returns a dataset given a UUID.
func (dsets *Datasets) DatasetFromUUID(u UUID) (dataset *Dataset, err error) {
	dataset, found := dsets.mapUUID[u]
	if !found {
		err = fmt.Errorf("DatasetFromUUID(): Illegal UUID (%s) not found", u)
	}
	return
}

// DatasetFromString returns a dataset from a UUID string.
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE...,
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (dsets *Datasets) DatasetFromString(str string) (dataset *Dataset, u UUID, err error) {
	numMatches := 0
	for dsetUUID, dset := range dsets.mapUUID {
		if strings.HasPrefix(string(dsetUUID), str) {
			numMatches++
			dataset = dset
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
// across datasets, we do not return the abbreviated data type names.
func (dsets *Datasets) Datatypes() map[UrlString]TypeService {
	typemap := make(map[UrlString]TypeService)
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
func (dsets *Datasets) VerifyCompiledTypes() error {
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

// newDataset creates a new Dataset, which constitutes a version DAG and allows storing
// arbitrary data within the nodes of the DAG.
func (dsets *Datasets) newDataset() (dset *Dataset, err error) {
	dsets.writeLock.Lock()
	defer dsets.writeLock.Unlock()

	dset = &Dataset{
		VersionDAG: NewVersionDAG(),
		DatasetID:  dsets.newDatasetID,
	}
	dsets.newDatasetID++
	dsets.list = append(dsets.list, dset)
	dsets.mapUUID[dset.Root] = dset
	return
}

// newChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dsets *Datasets) newChild(parent UUID) (dset *Dataset, u UUID, err error) {
	// Find the Dataset with this UUID
	var found bool
	dset, found = dsets.mapUUID[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}

	// Create the child in this Dataset's DAG
	u, err = dset.VersionDAG.newChild(parent)
	if err != nil {
		return
	}
	dsets.mapUUID[u] = dset
	return
}

// -- Datasets Serialization and Deserialization ---

type serializableDatasets struct {
	DatasetsUUID []UUID
	NewDatasetID DatasetLocalID
}

func (dsets *Datasets) serializableStruct() (sdata *serializableDatasets) {
	sdata = &serializableDatasets{
		DatasetsUUID: []UUID{},
		NewDatasetID: dsets.newDatasetID,
	}
	for _, dset := range dsets.list {
		sdata.DatasetsUUID = append(sdata.DatasetsUUID, dset.Root)
	}
	return
}

// Serialize returns a serialization of Datasets with Snappy compression and
// CRC32 checksum.
func (dsets *Datasets) serialize() ([]byte, error) {
	return dvid.Serialize(dsets.serializableStruct(), dvid.Snappy, dvid.CRC32)
}

// Deserialize converts a serialization to Datasets
func (dsets *Datasets) deserialize(s []byte) (*serializableDatasets, error) {
	deserialization := new(serializableDatasets)
	err := dvid.Deserialize(s, deserialization)
	if err != nil {
		return nil, fmt.Errorf("Error in deserializing datasets: %s", err.Error())
	}
	return deserialization, nil
}

// MarshalJSON returns the JSON of just the list of Dataset.
func (dsets *Datasets) MarshalJSON() (m []byte, err error) {
	return json.Marshal(dsets.serializableStruct())
}

// AllJSON returns JSON of all the datasets information.
func (dsets *Datasets) AllJSON() (m []byte, err error) {
	data := struct {
		Datasets []*Dataset
	}{
		dsets.list,
	}
	return json.Marshal(data)
}

// Load retrieves Datasets and all referenced Dataset from the KeyValueDB.
func (dsets *Datasets) Load(db storage.KeyValueDB) (err error) {
	// Get the the map of all UUIDs to local dataset IDs
	var data []byte
	data, err = db.Get(&DatasetsKey{})
	if err != nil {
		return
	}
	deserialization, err := dsets.deserialize(data)
	if err != nil {
		return err
	}

	// Get every Dataset (range query)
	keyvalues, err := db.GetRange(MinDatasetKey(), MaxDatasetKey())
	if err != nil {
		return err
	}

	// Check our expected # of Dataset == actually loaded # of Dataset.
	if len(keyvalues) != len(deserialization.DatasetsUUID) {
		return fmt.Errorf("Stored Datasets does not agree with the # of Dataset entries: %d vs %d",
			len(deserialization.DatasetsUUID), len(keyvalues))
	}
	if int(deserialization.NewDatasetID) < len(keyvalues) {
		return fmt.Errorf("Unexpected stored new dataset ID %d < current # datasets (%d)!",
			deserialization.NewDatasetID, len(keyvalues))
	}
	dsets.newDatasetID = deserialization.NewDatasetID

	// Reconstruct the Datasets by associating UUIDs.
	dsets.list = []*Dataset{}
	dsets.mapUUID = make(map[UUID]*Dataset)
	for _, value := range keyvalues {
		dataset := new(Dataset)
		err := dvid.Deserialize(value.V, dataset)
		if err != nil {
			return err
		}
		dsets.list = append(dsets.list, dataset)
		for u, _ := range dataset.Nodes {
			dsets.mapUUID[u] = dataset
		}
	}
	return
}

// Put stores Datasets into a KeyValueDB, overwriting whatever was there before.
func (dsets *Datasets) Put(db storage.KeyValueDB) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	// Get serialization
	serialization, err := dsets.serialize()
	if err != nil {
		return err
	}

	// Put data
	return db.Put(&DatasetsKey{}, serialization)
}

// Dataset is a set of Data with an associated version DAG.
type Dataset struct {
	*VersionDAG

	// Alias is an optional user-supplied string to identify this dataset
	// in a more friendly way than a UUID.  There are no guarantees that
	// this string is unique across all datasets.
	Alias string

	// DatasetID is the 32-bit identifier that is DVID server-specific.
	DatasetID DatasetLocalID

	// DataMap keeps the dataset-specific names for instances of data types
	// in this dataset.  Although this is public, access should be through
	// the DataService(name) function to also match possible prefix data names,
	// e.g., multichannel types.
	DataMap map[DataString]DataService
}

// TypeService returns the TypeService underlying data of a given name.
func (dset *Dataset) TypeService(name DataString) (t TypeService, err error) {
	data, err := dset.DataService(name)
	if err != nil {
		err = fmt.Errorf("Cannot get type of unknown data '%s'", name)
		return
	}
	t = data.(TypeService)
	return
}

// DataService returns a DataService for data of a given name.
func (dset *Dataset) DataService(name DataString) (dataservice DataService, err error) {
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
func (dset *Dataset) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(dset)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// Key returns a Key for this Dataset
func (dset *Dataset) Key() storage.Key {
	return &DatasetKey{dset.DatasetID}
}

// Put stores a Dataset into a KeyValueDB, overwriting whatever was there before.
func (dset *Dataset) Put(db storage.KeyValueDB) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	// Get serialization
	serialization, err := dvid.Serialize(dset, dvid.Snappy, dvid.CRC32)
	if err != nil {
		return err
	}

	// Put data
	return db.Put(dset.Key(), serialization)
}

// newData adds a new, named instance of a data type to dataset.  Settings can be passed
// via the 'config' argument.  For example, config["versioned"] will specify whether
// the data is mutable across nodes in the version DAG or is simply unversioned.
func (dset *Dataset) newData(name DataString, typeName string, config dvid.Config) error {
	// Only allow unique data names per dataset.
	// TODO -- Do more elaborate check that prevents prefixing data names using
	// data types that allow different suffixes, e.g., multichannel data.
	dataservice, found := dset.DataMap[name]
	if found {
		return fmt.Errorf("Data named '%s' already exists in dataset %s", name, dset.Root)
	}

	// Create new data for this dataset.
	typeService, err := TypeServiceByName(typeName)
	if err != nil {
		return fmt.Errorf("No data type '%s' found [%s]", typeName, err)
	}

	dset.mapLock.Lock()
	defer dset.mapLock.Unlock()

	dataID := &DataID{name, dset.NewDataID, dset.DatasetID}
	dset.NewDataID++
	dataservice, err = typeService.NewDataService(dset, dataID, config)
	if err != nil {
		return err
	}
	if dset.DataMap == nil {
		dset.DataMap = make(map[DataString]DataService)
	}
	dset.DataMap[name] = dataservice
	return nil
}

// UUID is a 32 character hexidecimal string ("" if invalid) that uniquely identifies
// nodes in a datastore's DAG.  We need universally unique identifiers to prevent collisions
// during creation of child nodes by distributed DVIDs:
// http://en.wikipedia.org/wiki/Universally_unique_identifier
type UUID string

// NewUUID returns a UUID
func NewUUID() UUID {
	u := uuid.NewUUID()
	if u == nil || len(u) != 16 {
		return UUID("")
	}
	return UUID(fmt.Sprintf("%032x", []byte(u)))
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
	GlobalID UUID

	// VersionID is a Dataset-specific id for each UUID, so we can compress the UUIDs.
	VersionID VersionLocalID

	// Locked nodes are read-only and can be branched.
	Locked bool

	// Parents is an ordered list of parent nodes.
	Parents []UUID

	// Children is a list of child nodes.
	Children []UUID

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
	Avail map[DataString]DataAvail

	writeLock sync.Mutex
}

// VersionDAG is the directed acyclic graph of NodeVersion and an index by UUID into
// the graph.
type VersionDAG struct {
	Root  UUID
	Nodes map[UUID]*Node

	// VersionMap is used to accelerate mapping global UUID to DVID server-specific
	// and smaller ID for a version.
	VersionMap map[UUID]VersionLocalID

	NewVersionID VersionLocalID
	NewDataID    DataLocalID

	mapLock sync.Mutex // guards the VersionDAG maps
}

// NewVersionDAG creates a version DAG and initializes the first unlocked node,
// assigning its UUID.
func NewVersionDAG() *VersionDAG {
	dag := VersionDAG{
		Root:       NewUUID(),
		Nodes:      make(map[UUID]*Node),
		VersionMap: make(map[UUID]VersionLocalID),
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
func (dag *VersionDAG) Lock(u UUID) error {
	node, found := dag.Nodes[u]
	if !found {
		return fmt.Errorf("No node found with UUID %s", u)
	}
	node.Locked = true
	return nil
}

// newChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dag *VersionDAG) newChild(parent UUID) (u UUID, err error) {
	node, found := dag.Nodes[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}
	if !node.Locked {
		err = fmt.Errorf("Cannot create a child of an unlocked node %s", parent)
		return
	}

	u = NewUUID()
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
		Parents:   []UUID{parent},
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
func (dag *VersionDAG) Versions() []UUID {
	uuids := []UUID{}
	for u, _ := range dag.Nodes {
		uuids = append(uuids, u)
	}
	return uuids
}
