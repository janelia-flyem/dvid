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

// KeyDatasets is the key for storing Datasets information.
var KeyDatasets = storage.Key{
	Dataset: dvid.KeyDatasetGlobal,
	Data:    dvid.KeyDataDVID,
	Index:   dvid.IndexUint8(1),
}

type nodeID struct {
	Dataset dvid.LocalID32
	Data    dvid.LocalID
	Version dvid.LocalID
}

// Map of mutexes at the granularity of dataset/data/version
var versionMutexes map[nodeID]*sync.Mutex

func init() {
	versionMutexes = make(map[nodeID]*sync.Mutex)
}

func VersionMutex(data DataService, versionID dvid.LocalID) (vmutex *sync.Mutex) {
	var mutex sync.Mutex
	mutex.Lock()
	id := nodeID{data.DatasetLocalID(), data.DataLocalID(), versionID}
	var found bool
	vmutex, found = versionMutexes[id]
	if !found {
		vmutex = new(sync.Mutex)
		versionMutexes[id] = vmutex
	}
	mutex.Unlock()
	return
}

// Datasets are group of Dataset available within the datastore.
type Datasets struct {
	Datasets []*Dataset

	// Always incremented counter that provides local dataset ID so we can use
	// smaller # of bytes (dvid.LocalID size) instead of full identifier.
	NewDatasetID dvid.LocalID32

	// Efficiently maps UUIDs to the version DAG from which it came.
	// Not persisted to disk and must be recreated when loading from disk.
	versionMap map[UUID]*Dataset

	writeLock sync.Mutex
}

// Data returns a service for data of a given name under a Dataset.
func (dsets *Datasets) Data(u UUID, name DataString) (dataService DataService, err error) {
	// Determine the dataset that contains the node with this UUID
	dataset, found := dsets.versionMap[u]
	if !found {
		err = fmt.Errorf("No node with UUID %s found", u)
		return
	}
	dataService, found = dataset.nameMap[name]
	if !found {
		err = fmt.Errorf("No data named '%s' at node with UUID %s", name, u)
	}
	return
}

// NewDataset creates a new Dataset, which constitutes a version DAG and allows storing
// arbitrary data within the nodes of the DAG.
func (dsets *Datasets) NewDataset() (dset *Dataset, err error) {
	dsets.writeLock.Lock()

	dset = &Dataset{
		VersionDAG: NewVersionDAG(),
		DatasetID:  dsets.NewDatasetID,
	}
	dset.NewDataID = dvid.KeyDataStart

	dsets.NewDatasetID++
	dsets.Datasets = append(dsets.Datasets, dset)
	dsets.versionMap[dset.Root] = dset

	dsets.writeLock.Unlock()
	return
}

// NewChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dsets *Datasets) NewChild(parent UUID) (u UUID, err error) {
	// Find the Dataset with this UUID
	dset, found := dsets.versionMap[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}

	// Create the child in this Dataset's DAG
	u, err = dset.VersionDAG.NewChild(parent)
	if err != nil {
		return
	}
	dsets.versionMap[u] = dset
	return
}

// NewData registers a new instance of a given data type within a dataset.
func (dsets *Datasets) NewData(u UUID, name DataString, typeName string, config dvid.Config) error {
	// Find the Dataset with this UUID
	dset, found := dsets.versionMap[u]
	if !found {
		return fmt.Errorf("No node found with UUID %s", u)
	}

	// Construct new data
	return dset.NewData(u, name, typeName, config)
}

// Get retrieves Datasets from a KeyValueDB.
func (dsets *Datasets) Get(db storage.KeyValueDB) (err error) {
	var data []byte
	data, err = db.Get(&KeyDatasets)
	if err != nil {
		return
	}

	// Deserialize into object
	err = dsets.Deserialize(data)
	return
}

// Put stores Datasets into a KeyValueDB, overwriting whatever was there before.
// This assumes only one Dataservice for a given datastore.
func (dsets *Datasets) Put(db storage.KeyValueDB) error {
	// Get serialization
	serialization, err := dsets.Serialize()
	if err != nil {
		return err
	}

	// Put data
	return db.Put(&KeyDatasets, serialization)
}

// Serialize returns a serialization of Datasets with Snappy compression and
// CRC32 checksum.
func (dsets *Datasets) Serialize() ([]byte, error) {
	return dvid.Serialize(dsets, dvid.Snappy, dvid.CRC32)
}

// Deserialize converts a serialization to Datasets
func (dsets *Datasets) Deserialize(s []byte) error {
	dsets.Datasets = []*Dataset{}
	err := dvid.Deserialize(s, dsets)
	if err != nil {
		return fmt.Errorf("Error in deserializing datasets: %s", err.Error())
	}
	dsets.versionMap = make(map[UUID]*Dataset)
	for _, dset := range dsets.Datasets {
		for _, u := range dset.Versions() {
			dsets.versionMap[u] = dset
		}
	}
	return nil
}

// DatasetFromUUID returns a dataset given a UUID.
func (dsets *Datasets) DatasetFromUUID(u UUID) (dataset *Dataset, err error) {
	dataset, found := dsets.versionMap[u]
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
	for dsetUUID, dset := range dsets.versionMap {
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

// LocalIDFromUUID when supplied a UUID string, returns smaller sized local IDs that identify a
// dataset and a version.
func (dsets *Datasets) LocalIDFromUUID(u UUID) (datasetID dvid.LocalID32, versionID dvid.LocalID, err error) {
	var dataset *Dataset
	dataset, err = dsets.DatasetFromUUID(u)
	if err != nil {
		return
	}
	datasetID = dataset.DatasetID
	var found bool
	versionID, found = dataset.VersionMap[u]
	if !found {
		err = fmt.Errorf("UUID (%s) not found in dataset", u)
	}
	return
}

// LocalIDFromString when supplied a UUID string, returns smaller sized local IDs that identify a
// dataset and a version.  Partial matches are allowed, similar to DatasetFromString.
func (dsets *Datasets) LocalIDFromString(str string) (datasetID dvid.LocalID32, versionID dvid.LocalID, err error) {
	dset, u, err := dsets.DatasetFromString(str)
	if err != nil {
		return
	}
	datasetID = dset.DatasetID
	versionID = dset.VersionMap[u]
	return
}

// Datatypes returns a map of all unique data types where the key is the
// unique URL identifying the data type.  Since type names can collide
// across datasets, we do not return the abbreviated data type names.
func (dsets *Datasets) Datatypes() map[UrlString]TypeService {
	typemap := make(map[UrlString]TypeService)
	for _, dset := range dsets.Datasets {
		for _, node := range dset.Nodes {
			for _, nodeData := range node.Data {
				typeservice, ok := nodeData.DataService.(TypeService)
				if ok {
					typemap[nodeData.DatatypeUrl()] = typeservice
				}
			}
		}
	}
	return typemap
}

// VerifyCompiledTypes will return an error if any required data type in the datastore
// configuration was not compiled into DVID executable.  Check is done by more exact
// URL and not the data type name.
func (dsets *Datasets) VerifyCompiledTypes() error {
	var errMsg string
	for _, dset := range dsets.Datasets {
		datamap := dset.AvailableData()
		for name, data := range datamap {
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

// About returns a chart of the code versions of compile-time DVID datastore
// and the runtime data types.
func (dsets *Datasets) About() string {
	var text string
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Storage backend", storage.Version)
	for _, dtype := range dsets.Datatypes() {
		writeLine(dtype.DatatypeName(), dtype.DatatypeVersion())
	}
	return text
}

// AboutJSON returns the components and versions of DVID software.
func (dsets *Datasets) AboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"DVID datastore":  Version,
		"Storage backend": storage.Version,
	}
	for _, dtype := range dsets.Datatypes() {
		data[dtype.DatatypeName()] = dtype.DatatypeVersion()
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// DataChart returns a text chart of data names and their types for this DVID server.
func (dsets *Datasets) DataChart() string {
	var text string
	if len(dsets.Datasets) == 0 {
		return "  No datasets have been added to this datastore.\n"
	}
	writeLine := func(name DataString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	for num, dset := range dsets.Datasets {
		text += fmt.Sprintf("\nDataset %d (UUID = %s):\n\n", num+1, dset.RootUUID())
		writeLine("Name", "Type Name", "Url")
		for name, data := range dset.AvailableData() {
			writeLine(name, data.DatatypeName()+" ("+data.DatatypeVersion()+")",
				data.DatatypeUrl())
		}
	}
	return text
}

// JSON returns JSON-encoded exportable Datasets information.
func (dsets *Datasets) JSON() (jsonStr string, err error) {
	m, err := json.Marshal(dsets)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// Dataset is a set of Data with an associated version DAG.
type Dataset struct {
	*VersionDAG

	// Alias is an optional user-supplied string to identify this dataset
	// in a more friendly way than a UUID.  There are no guarantees that
	// this string is unique across all datasets.
	Alias string

	// DatasetID is the 32-bit identifier that is DVID server-specific.
	DatasetID dvid.LocalID32

	// private fields must be recreated when loading from disk, etc.
	nameMap map[DataString]DataService
}

// RootUUID returns the UUID of the root node of this Dataset's version DAG.
func (dset *Dataset) RootUUID() UUID {
	return dset.Root
}

func (dset *Dataset) Versions() []UUID {
	return dset.Versions()
}

func (dset *Dataset) TypeServiceForData(name DataString) (t TypeService, err error) {
	data, found := dset.nameMap[name]
	if !found {
		err = fmt.Errorf("Cannot get type of unknown data '%s'", name)
		return
	}
	t = data.(TypeService)
	return
}

func (dset *Dataset) Data(name DataString) (dataservice DataService, err error) {
	var found bool
	dataservice, found = dset.nameMap[name]
	if !found {
		err = fmt.Errorf("Cannot find data '%s'", name)
		return
	}
	return
}

// NewData adds a new, named instance of a data type to a node.  Settings can be passed
// via the 'config' argument.  For example, config["versioned"] will specify whether
// the data is mutable across nodes in the version DAG or is simply unversioned.
func (dset *Dataset) NewData(u UUID, name DataString, typeName string, config dvid.Config) error {
	typeService, err := TypeServiceByName(typeName)
	if err != nil {
		return fmt.Errorf("No data type '%s' found [%s]", typeName, err)
	}

	// Get the node in this dataset's version DAG.
	node, found := dset.Nodes[u]
	if !found {
		return fmt.Errorf("Could not add data '%s' to nonexistant node %s", name, u)
	}

	// Lock dataset during changes.
	dset.mapLock.Lock()
	defer dset.mapLock.Unlock()

	// Reuse data if already in the dataset, else make a new one.
	var dataservice DataService
	dataservice, found = dset.nameMap[name]
	if !found {
		dataID := &DataID{name, dset.NewDataID, dset.DatasetID}
		dataservice, err = typeService.NewData(dataID, config)
		if err != nil {
			return err
		}
		if dset.nameMap == nil {
			dset.nameMap = make(map[DataString]DataService)
		}
		dset.nameMap[name] = dataservice
	}

	// Add this data to the specified node.
	avail := DataComplete
	if dataservice.IsVersioned() {
		// TODO -- Allow delta compression on versioned nodes.
		// avail = DataDelta
	} else {
		// If we are unversioned, just alias all data to one UUID.
		// This works well for things like write-once, read-many grayscale images.
		avail = DataAliased
	}
	node.AddData(&NodeData{dataservice, avail})
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
	// up to any NodeComplete ancestor.
	DataDelta

	// Queries are redirected to another node based on UUID.
	DataAliased

	// Data has been explicitly deleted at this node and is no longer available.
	DataDeleted
)

// NodeData describes Data and its availability within a node of the version DAG.
type NodeData struct {
	DataService
	Avail DataAvail
}

// NodeVersion contains all information for a node in the version DAG like its parents,
// children, and provenance.
type NodeVersion struct {
	// GlobalID is a globally unique id.
	GlobalID UUID

	// VersionID is a Dataset-specific id for each UUID, so we can compress the UUIDs.
	VersionID dvid.LocalID

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

	Data []*NodeData

	writeLock sync.Mutex
}

// AddData adds data to a node
func (n *Node) AddData(data *NodeData) {
	n.writeLock.Lock()
	if n.Data == nil {
		n.Data = []*NodeData{data}
	} else {
		n.Data = append(n.Data, data)
	}
	n.writeLock.Unlock()
}

// VersionDAG is the directed acyclic graph of NodeVersion and an index by UUID into
// the graph.
type VersionDAG struct {
	Root  UUID
	Nodes map[UUID]*Node

	// VersionMap is used to accelerate mapping global UUID to DVID server-specific
	// and smaller ID for a version.
	VersionMap map[UUID]dvid.LocalID

	NewVersionID dvid.LocalID
	NewDataID    dvid.LocalID

	mapLock sync.Mutex
}

// NewVersionDAG creates a version DAG and initializes the first unlocked node,
// assigning its UUID.
func NewVersionDAG() *VersionDAG {
	dag := VersionDAG{
		Root:       NewUUID(),
		Nodes:      make(map[UUID]*Node),
		VersionMap: make(map[UUID]dvid.LocalID),
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

// NewChild creates a new child node off a LOCKED parent node.  Will return
// an error if the parent node has not been locked.
func (dag *VersionDAG) NewChild(parent UUID) (u UUID, err error) {
	node, found := dag.Nodes[parent]
	if !found {
		err = fmt.Errorf("No node found with UUID %s", parent)
		return
	}
	if !node.Locked {
		err = fmt.Errorf("NewChild() cannot be called on an unlocked node %s", parent)
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
		Parents:   []UUID{u},
	}
	dag.Nodes[u] = &Node{NodeVersion: version}
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

// AvailableData returns a map of all data present in a version DAG where the
// key is the data name.
func (dag *VersionDAG) AvailableData() map[DataString]DataService {
	datamap := make(map[DataString]DataService)
	for _, node := range dag.Nodes {
		for _, data := range node.Data {
			datamap[data.DataName()] = data.DataService
		}
	}
	return datamap
}
