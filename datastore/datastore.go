/*
	This file provides the exported view of the datastore metadata handling functions.
	All platform-specific code is isolated to the *_local, *_cluster, and similarly named files.

	The repo management functions are package-level functions to avoid lower-level exported
	types like Repo, which invariably depend on global version ids and coordination with the
	singleton repo manager.
*/

package datastore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.10.0"
)

var (
	// manager provides high-level repository management for DVID and is initialized
	// on start.  Package functions provide a quick alias to this platform-specific repo manager.
	manager *repoManager
)

// Shutdown sends signal for all goroutines for data processing to be terminated.
func Shutdown() {
	if manager == nil {
		return
	}
	manager.Shutdown()
}

// Types returns the types currently within the DVID server.
func Types() (map[dvid.URLString]TypeService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.types()
}

// MarshalJSON returns JSON of object where each repo is a property with root UUID name
// and value corresponding to repo info.
func MarshalJSON() ([]byte, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.MarshalJSON()
}

// ---- Datastore ID functions ----------

func NewUUID(assign *dvid.UUID) (dvid.UUID, dvid.VersionID, error) {
	if manager == nil {
		return dvid.NilUUID, 0, ErrManagerNotInitialized
	}
	return manager.newUUID(assign)
}

func UUIDFromVersion(v dvid.VersionID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.uuidFromVersion(v)
}

func VersionFromUUID(uuid dvid.UUID) (dvid.VersionID, error) {
	if manager == nil {
		return 0, ErrManagerNotInitialized
	}
	return manager.versionFromUUID(uuid)
}

// MatchingUUID returns version identifiers that uniquely matches a uuid string.
func MatchingUUID(uuidStr string) (dvid.UUID, dvid.VersionID, error) {
	if manager == nil {
		return dvid.NilUUID, 0, ErrManagerNotInitialized
	}
	return manager.matchingUUID(uuidStr)
}

// ----- Repo functions -----------

// NewRepo creates a new Repo and returns its UUID, either an assigned UUID if
// provided or creating a new UUID.
func NewRepo(alias, description string, assign *dvid.UUID, passcode string) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	r, err := manager.newRepo(alias, description, assign, passcode)
	if err != nil {
		return dvid.NilUUID, err
	}
	return r.uuid, err
}

// DeleteRepo deletes a Repo holding a node with UUID.
func DeleteRepo(uuid dvid.UUID, passcode string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.deleteRepo(uuid, passcode)
}

func GetRepoRoot(uuid dvid.UUID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.getRepoRoot(uuid)
}

func GetRepoRootVersion(v dvid.VersionID) (dvid.VersionID, error) {
	if manager == nil {
		return 0, ErrManagerNotInitialized
	}
	return manager.getRepoRootVersion(v)
}

func GetRepoJSON(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoJSON(uuid)
}

func GetRepoAlias(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoAlias(uuid)
}

func SetRepoAlias(uuid dvid.UUID, alias string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setRepoAlias(uuid, alias)
}

func GetRepoDescription(uuid dvid.UUID) (string, error) {
	if manager == nil {
		return "", ErrManagerNotInitialized
	}
	return manager.getRepoDescription(uuid)
}

func SetRepoDescription(uuid dvid.UUID, desc string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setRepoDescription(uuid, desc)
}

func GetRepoLog(uuid dvid.UUID) ([]string, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getRepoLog(uuid)
}

func AddToRepoLog(uuid dvid.UUID, msgs []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.addToRepoLog(uuid, msgs)
}

func SetNodeNote(uuid dvid.UUID, note string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setNodeNote(uuid, note)
}

func GetNodeLog(uuid dvid.UUID) ([]string, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getNodeLog(uuid)
}

func AddToNodeLog(uuid dvid.UUID, msgs []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.addToNodeLog(uuid, msgs)
}

// ----- Repo-level DAG functions ----------

// NewVersion creates a new version as a child of the given parent.  If the
// assign parameter is not nil, the new node is given the UUID.
func NewVersion(parent dvid.UUID, note string, assign *dvid.UUID) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.newVersion(parent, note, assign)
}

// GetParents returns the parent nodes of the given version id.
func GetParentsByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getParentsByVersion(v)
}

// GetChildren returns the child nodes of the given version id.
func GetChildrenByVersion(v dvid.VersionID) ([]dvid.VersionID, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getChildrenByVersion(v)
}

// LockedUUID returns true if a given UUID is locked.
func LockedUUID(uuid dvid.UUID) (bool, error) {
	if manager == nil {
		return false, ErrManagerNotInitialized
	}
	return manager.lockedUUID(uuid)
}

// LockedVersion returns true if a given version is locked.
func LockedVersion(v dvid.VersionID) (bool, error) {
	if manager == nil {
		return false, ErrManagerNotInitialized
	}
	return manager.lockedVersion(v)
}

func Commit(uuid dvid.UUID, note string, log []string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.commit(uuid, note, log)
}

func Merge(parents []dvid.UUID, note string, mt MergeType) (dvid.UUID, error) {
	if manager == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}
	return manager.merge(parents, note, mt)
}

// ----- Data Instance functions -----------

// NewData adds a new, named instance of a datatype to repo.  Settings can be passed
// via the 'config' argument.  For example, config["versioned"] with a bool value
// will specify whether the data is versioned.
func NewData(uuid dvid.UUID, t TypeService, name dvid.InstanceName, c dvid.Config) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.newData(uuid, t, name, c)
}

// SaveDataByUUID persists metadata for a data instance with given uuid.
// TODO -- Make this more efficient by storing data metadata separately from repo.
//   Currently we save entire repo.
func SaveDataByUUID(uuid dvid.UUID, data DataService) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.saveRepoByUUID(uuid)
}

// SaveDataByVersion persists metadata for a data instance with given version.
// TODO -- Make this more efficient by storing data metadata separately from repo.
//   Currently we save entire repo.
func SaveDataByVersion(v dvid.VersionID, data DataService) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.saveRepoByVersion(v)
}

// getDataByInstanceID returns a data service given a server-specific instance ID.
func getDataByInstanceID(id dvid.InstanceID) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByInstanceID(id)
}

// GetDataByDataUUID returns a data service given a data UUID.
func GetDataByDataUUID(dataUUID dvid.UUID) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByDataUUID(dataUUID)
}

// GetDataByUUIDName returns a data service given an instance name and UUID.
func GetDataByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByUUIDName(uuid, name)
}

// GetDataByVersionName returns a data service given an instance name and version.
func GetDataByVersionName(v dvid.VersionID, name dvid.InstanceName) (DataService, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	return manager.getDataByVersionName(v, name)
}

// DeleteDataByName returns a data service given an instance name and UUID.
func DeleteDataByName(uuid dvid.UUID, name dvid.InstanceName, passcode string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.deleteDataByName(uuid, name, passcode)
}

// RenameData renames a data service given an old instance name and UUID.
func RenameData(uuid dvid.UUID, oldname, newname dvid.InstanceName, passcode string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.renameDataByName(uuid, oldname, newname, passcode)
}

// DeleteDataByVersion returns a data service given an instance name and UUID.
func DeleteDataByVersion(v dvid.VersionID, name dvid.InstanceName, passcode string) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.deleteDataByVersion(v, name, passcode)
}

func ModifyDataConfigByName(uuid dvid.UUID, name dvid.InstanceName, c dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.modifyDataByName(uuid, name, c)
}

// ------ Cross-platform k/v pair matching for given version, necessary for versioned get.

type kvvNode struct {
	kv      *storage.KeyValue
	invalid bool
}
type kvVersions map[dvid.VersionID]kvvNode

// FindMatch returns the correct key-value pair for a given version and which version
// that key-value pair came from.
func (kvv kvVersions) FindMatch(v dvid.VersionID) (*storage.KeyValue, dvid.VersionID, error) {
	if manager == nil {
		return nil, 0, ErrManagerNotInitialized
	}

	// Start from current version and traverse the ancestor graph.  Whenever there's a branch, make
	// sure we only have one matching key.
	return manager.findMatch(kvv, v)
}

// FindConflicts returns any keys that would conflict for the given parents ordered by priority,
// where first parent takes most precendence, second parent is second most important, etc.
func (kvv kvVersions) FindConflicts(parents []dvid.VersionID) (toDelete map[dvid.VersionID]storage.Key, err error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	if len(parents) < 2 {
		return nil, fmt.Errorf("Must have more than one parent to find conflicts for future merge.")
	}

	// Get kv-pair for each parent in priority order.  If highest priority parent has conflict/error, it's an error
	// since it should've been handled earlier.  Otherwise, we will put this on our toDelete list of keys.
	toDelete = make(map[dvid.VersionID]storage.Key)
	var first *storage.KeyValue
	for _, parentV := range parents {
		kv, _, err := manager.findMatch(kvv, parentV)
		if err != nil {
			return nil, fmt.Errorf("error retrieving k/v with precendence: %v", err)
		}
		if first == nil {
			if kv != nil && kv.K != nil {
				first = kv // we have a valid kv that gets priority
			}
		} else if kv != nil && kv.K != nil && !bytes.Equal(kv.K, first.K) {
			toDelete[parentV] = kv.K
		}
	}
	return
}

// Describes an extra node that we can apply deletions.
type extensionNode struct {
	oldUUID dvid.UUID
	newUUID dvid.UUID
	newV    dvid.VersionID
}

func deleteConflict(data DataService, extnode *extensionNode, k storage.Key) error {
	store, err := getOrderedKeyValueDB(data)
	if err != nil {
		return err
	}

	// Create new node if necessary
	if extnode.newUUID == dvid.NilUUID {
		childUUID, err := manager.newVersion(extnode.oldUUID, "Version for deleting conflicts before merge", nil)
		if err != nil {
			return err
		}
		extnode.newUUID = childUUID
		childV, err := manager.versionFromUUID(childUUID)
		if err != nil {
			return err
		}
		extnode.newV = childV
	}

	// Perform the deletion.
	tk, err := storage.TKeyFromKey(k)
	if err != nil {
		return err
	}
	ctx := NewVersionedCtx(data, extnode.newV)
	return store.Delete(ctx, tk)
}

// DeleteConflicts removes all conflicted kv pairs for the given data instance using the priority
// established by parents.  As a side effect, newParents are modified by new children of parents.
func DeleteConflicts(uuid dvid.UUID, data DataService, oldParents, newParents []dvid.UUID) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	// Convert UUIDs to versions + bool for whether it's a child suitable for add deletions.
	parents := make(map[dvid.VersionID]*extensionNode, len(oldParents))
	parentsV := make([]dvid.VersionID, len(oldParents))
	for i, oldUUID := range oldParents {
		oldV, err := manager.versionFromUUID(oldUUID)
		if err != nil {
			return err
		}
		parentsV[i] = oldV
		if newParents[i] != dvid.NilUUID {
			newV, err := manager.versionFromUUID(newParents[i])
			if err != nil {
				return err
			}
			parents[oldV] = &extensionNode{oldUUID, newParents[i], newV}
		} else {
			parents[oldV] = &extensionNode{oldUUID, dvid.NilUUID, 0}
		}
	}

	// Process stream of incoming kv pair for this data instance.
	baseCtx := NewVersionedCtx(data, 0)
	ch := make(chan *storage.KeyValue, 1000)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		var curV dvid.VersionID
		var curTK, batchTK storage.TKey
		kvv := kvVersions{}
		for {
			kv := <-ch
			if kv == nil {
				curTK = nil
			} else {
				curV, err = baseCtx.VersionFromKey(kv.K)
				if err != nil {
					dvid.Errorf("Can't decode key when deleting conflicts for %s", data.DataName())
					continue
				}

				// If we have a different TKey, then process the batch of versions.
				curTK, err = storage.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("Error in processing kv pairs in DeleteConflicts: %v\n", err)
					continue
				}
				if batchTK == nil {
					batchTK = curTK
				}
			}
			if !bytes.Equal(curTK, batchTK) {
				// Get conflicts.
				toDelete, err := kvv.FindConflicts(parentsV)
				if err != nil {
					dvid.Errorf("Error finding conflicts: %v\n", err)
					continue
				}

				// Create new node if necessary to apply deletions, and if so, store new node.
				for v, k := range toDelete {
					if err := deleteConflict(data, parents[v], k); err != nil {
						dvid.Errorf("Unable to delete conflict: %v\n", err)
						continue
					}
				}

				// Delete the stash of kv pairs
				kvv = kvVersions{}

				batchTK = curTK
			}
			if kv == nil {
				return
			}
			kvv[curV] = kvvNode{kv: kv}
		}
	}()

	// Iterate through all k/v for this data instance.
	store, err := getOrderedKeyValueDB(data)
	if err != nil {
		return err
	}

	minKey, maxKey := baseCtx.KeyRange()
	keysOnly := true
	if err := store.RawRangeQuery(minKey, maxKey, keysOnly, ch, nil); err != nil {
		return err
	}
	wg.Wait()

	// Return the new parents which were needed for deletions.
	//newParents = make([]dvid.UUID, len(oldParents))
	for i, oldV := range parentsV {
		if parents[oldV].newUUID == dvid.NilUUID {
			newParents[i] = parents[oldV].oldUUID
		} else {
			newParents[i] = parents[oldV].newUUID
		}
	}

	return nil
}

// GetStorageBreakdown returns JSON for all the data instances in the stores.
func GetStorageBreakdown() (string, error) {
	stores, err := storage.AllStores()
	if err != nil {
		return "", err
	}

	breakdown := make(map[string]map[uint32]interface{}, len(stores))
	for alias, store := range stores {
		s, err := storage.GetDataSizes(store, nil)
		if err != nil {
			return "", err
		}

		// For each instance ID, populate the instance info if available.
		sdata := make(map[uint32]interface{}, len(s))
		for instanceID, size := range s {
			idata := struct {
				Name     string
				DataType string
				DataUUID string
				RootUUID string
				Bytes    uint64
			}{
				Bytes: size,
			}
			d, err := getDataByInstanceID(instanceID)
			if err != nil {
				// we have no data instance so use placeholders.
				idata.Name = fmt.Sprintf("unknown-%d", instanceID)
			} else {
				idata.Name = string(d.DataName())
				idata.DataType = string(d.TypeName())
				idata.DataUUID = string(d.DataUUID())
				idata.RootUUID = string(d.RootUUID())
			}
			sdata[uint32(instanceID)] = idata
		}
		breakdown[string(alias)] = sdata
	}

	// Convert data to JSON string
	m, err := json.Marshal(breakdown)
	if err != nil {
		return "", err
	}
	return string(m), nil
}
