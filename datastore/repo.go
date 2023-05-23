/*
	This file contains platform-independent code for handling DVID metadata describing
	the collection of repos, each of which is a versioned dataset that holds a DAG and
	a list of data instances.  DVID access to metadata should be through package-level
	functions.
*/

package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// MergeType describes the expectation of processing for the merge, e.g., is it
// expected to be free of conflicts at the key-value level, require automated
// type-specific key-value merging, or require external guidance.
type MergeType uint8

const (
	// MergeConflictFree assumes that changes in nodes-to-be-merged are disjoint
	// at the key-value level.
	MergeConflictFree MergeType = iota

	// MergeTypeSpecificAuto requires datatype-specific code for merging at each
	// key-value pair.
	MergeTypeSpecificAuto

	// MergeExternalData requires external data to reconcile merging of nodes.
	MergeExternalData
)

var (
	ErrManagerNotInitialized = errors.New("datastore repo manager not initialized")
	ErrBadMergeType          = errors.New("bad merge type")

	ErrInvalidUUID         = errors.New("UUID is not present in datastore")
	ErrInvalidVersion      = errors.New("server-specific version id is invalid")
	ErrInvalidRepoID       = errors.New("server-specific repo id is invalid")
	ErrExistingUUID        = errors.New("UUID already exists in datastore")
	ErrExistingDataName    = errors.New("data name already exists")
	ErrInvalidDataName     = errors.New("invalid data instance name")
	ErrInvalidDataInstance = errors.New("invalid data instance id")
	ErrInvalidDataUUID     = errors.New("invalid data instance UUID")

	ErrInvalidStore = errors.New("invalid data store")

	ErrModifyLockedNode   = errors.New("can't modify locked node")
	ErrBranchUnlockedNode = errors.New("can't branch an unlocked node")
	ErrBranchUnique       = errors.New("branch already exists with given name")
)

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

func (r *repoT) branchHeads() map[string]dvid.UUID {
	branchToUUID := make(map[string]dvid.UUID)
	for _, node := range r.dag.nodes {
		if len(node.children) == 0 {
			branchToUUID[node.branch] = node.uuid
		}
	}
	return branchToUUID
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

// duplicate returns a duped repo optionally limited by the given version ID set and
// a list of data instance names, or if excluded is passed, all instances but those
// are returned.  Note that the underlying data instances aren't duplicated.
func (r *repoT) duplicate(versions map[dvid.VersionID]struct{}, keep, exclude dvid.InstanceNames) (*repoT, error) {
	dup := new(repoT)
	dup.id = r.id

	// if the root is no longer an allowed version, we know it's a flattened.
	r.RLock()
	defer r.RUnlock()

	if _, found := versions[r.version]; found {
		dup.uuid = r.uuid
		dup.version = r.version
	} else {
		// get the earliest version as the root, since in any path, the versions escalate.
		first := true
		var minV dvid.VersionID
		for v := range versions {
			if first || v < minV {
				minV = v
				first = false
			}
		}
		dup.version = minV
		dup.uuid = r.dag.nodes[minV].uuid
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

	var isExcluded map[dvid.InstanceName]struct{}
	if len(exclude) > 0 {
		isExcluded = make(map[dvid.InstanceName]struct{}, len(exclude))
		for _, name := range exclude {
			isExcluded[name] = struct{}{}
		}
	}

	if len(keep) == 0 {
		dup.data = make(map[dvid.InstanceName]DataService, len(r.data))
		for k, v := range r.data {
			if _, excluded := isExcluded[k]; !excluded {
				dup.data[k] = v
			}
		}
	} else {
		dup.data = make(map[dvid.InstanceName]DataService, len(keep))
		for _, name := range keep {
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

func (r *repoT) initMutationID(store storage.KeyValueDB, mutationIDStart uint64, readOnly bool) error {
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
	if !readOnly {
		r.mutSavedID = r.mutCurID + StrideMutationID
		mutdata = make([]byte, 8)
		binary.LittleEndian.PutUint64(mutdata, r.mutSavedID)
		if err := store.Put(ctx, tk, mutdata); err != nil {
			return err
		}
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
	if manager.readOnly {
		dvid.Criticalf("Cannot give new mutation ID in read-only mode.\n")
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

// get the given branch child from this node.
func (r *repoT) getChildBranchNode(parentV dvid.VersionID, branch string) (branchNode *nodeT, err error) {
	parent, found := r.dag.nodes[parentV]
	if !found {
		return nil, ErrInvalidVersion
	}
	for _, v := range parent.children {
		node, found := r.dag.nodes[v]
		if !found {
			continue
		}
		if node.branch == branch {
			branchNode = node
			break
		}
	}
	return
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
	var branchName string
	var branchNode *nodeT
	for _, node := range d.nodes {
		if node.branch == branch || (branch == "master" && node.branch == "") {
			branchNode = node
			branchName = node.branch
		}
	}
	if branchNode == nil {
		ancestry = []dvid.UUID{}
		return
	}
	for {
		leaf := true
		for _, childV := range branchNode.children {
			child, found := d.nodes[childV]
			if !found {
				err = fmt.Errorf("branch %q node %s has child version %d that doesn't exist", branchName, branchNode.uuid, childV)
				return
			}
			if child.branch == branchName {
				if !leaf {
					err = fmt.Errorf("branch %q has more than 1 child: %s and %s", branchName, child.uuid, branchNode.uuid)
					return
				}
				branchNode = child
				leaf = false
			}
		}
		if leaf {
			break
		}
	}

	// start from leaf and work way up to root
	cur := branchNode
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

// Replaces entry of "replace" id with substitute slice and returns result in a new slice.
func editVersionSlice(orig []dvid.VersionID, replace dvid.VersionID, subst []dvid.VersionID) (edited []dvid.VersionID) {
	pos := -1
	for i, v := range orig {
		if v == replace {
			pos = i
			break
		}
	}
	if pos < 0 {
		edited = make([]dvid.VersionID, len(orig))
		copy(edited, orig)
		return
	}
	edited = make([]dvid.VersionID, len(orig)-1+len(subst))
	dvid.Infof(" orig: %v\n", orig)
	dvid.Infof("subst: %v\n", subst)
	dvid.Infof("replace: %d\n", replace)
	j := 0
	for _, v := range orig {
		if v != replace {
			edited[j] = v
			j++
		}
	}
	for _, v := range subst {
		edited[j] = v
		j++
	}
	dvid.Infof("edited: %v\n", edited)
	return
}

// returns duplicate of DAG limited by any set of version IDs or if versions is nil,
// duplicates the entire DAG.  If the root UUID is not in the list of allowed versions,
// the earliest version is made the root.
func (d *dagT) duplicate(versions map[dvid.VersionID]struct{}) *dagT {
	dup := new(dagT)
	d.RLock()

	dup.nodes = make(map[dvid.VersionID]*nodeT, len(d.nodes))
	for v, node := range d.nodes {
		dup.nodes[v] = node.duplicate(nil)
	}

	if len(versions) != 0 {
		// prune the DAG of any version not on our list, adjusting the
		// parents/children pointers as we go.
		for v, node := range dup.nodes {
			if _, keep := versions[v]; !keep {
				for _, childV := range node.children {
					if dupNode, exists := dup.nodes[childV]; exists {
						dupNode.parents = editVersionSlice(dupNode.parents, v, node.parents)
					} else {
						dvid.Criticalf("found node %d had child %d that didn't exist on pruning: %v\n", v, childV, node.children)
					}
				}
				for _, parentV := range node.parents {
					if dupNode, exists := dup.nodes[parentV]; exists {
						dupNode.children = editVersionSlice(dupNode.children, v, node.children)
					} else {
						dvid.Criticalf("found node %d had parent %d that didn't exist on pruning: %v\n", v, parentV, node.parents)
					}
				}
				delete(dup.nodes, v)
			}
		}
	}

	// get the earliest version as the root, since in any path, the versions escalate.
	first := true
	var minV dvid.VersionID
	for v := range versions {
		if first || v < minV {
			minV = v
			first = false
		}
	}
	dup.rootV = minV
	dup.root = d.nodes[minV].uuid
	if minV != d.rootV {
		dvid.Debugf("duplicated restricted DAG moved root %s -> %s\n", d.root, dup.root)
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

func (node *nodeT) String() string {
	s := fmt.Sprintf("UUID: %s\n", node.uuid)
	s += fmt.Sprintf("Version: %d\n", node.version)
	s += fmt.Sprintf("Locked: %t\n", node.locked)
	s += fmt.Sprintf("Parents: %v\n", node.parents)
	s += fmt.Sprintf("Children: %v\n", node.children)
	return s
}

// duplicate creates a duplicate node, limiting the data instances
// to passed versions if provided.  If versions is nil, all versions are used.
// If a parent or child is not included in the versions, it is not copied.
// Therefore if versions are supplied, they must be contiguous and not random
// nodes in DAG.
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
