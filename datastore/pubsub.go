/*
   This file provides the internal pub/sub report.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
)

// SyncEvent identifies an event in which a data instance has modified its data
type SyncEvent struct {
	Data  dvid.UUID
	Event string
}

// SyncMessage describes changes to a data instance for a given version.
type SyncMessage struct {
	Event   string
	Version dvid.VersionID
	Delta   interface{}
}

// SyncSub is a subscription request from an instance to be notified via a channel when
// a given data instance has a given event.
type SyncSub struct {
	Event  SyncEvent
	Notify dvid.UUID // the data UUID of data instance to notify
	Ch     chan SyncMessage
}

// SyncSubs is a slice of sync subscriptions.
type SyncSubs []SyncSub

// Add returns a SyncSubs with the added SyncSub, making sure that only one subscription exists for any
// (Event, Notify) tuple.  If a previous (Event, Notify) exists, it is replaced by the passed SyncSub.
func (subs SyncSubs) Add(added SyncSub) SyncSubs {
	if len(subs) == 0 {
		return SyncSubs{added}
	}
	for i, sub := range subs {
		if sub.Event == added.Event && sub.Notify == added.Notify {
			subs[i] = added
			return subs
		}
	}
	return append(subs, added)
}

// Syncer types are typically DataService that know how to sync with other data.
type Syncer interface {
	// GetSyncSubs returns the subscriptions that need to be created to keep this data
	// synced and may launch goroutines that will consume inbound channels of changes
	// from associated data.
	GetSyncSubs(dvid.Data) (SyncSubs, error)

	// SyncedNames returns the set of data instance UUIDs to which the data is synced.
	SyncedNames() []dvid.InstanceName

	// SyncedNames returns the set of data instance UUIDs to which the data is synced.
	SyncedData() dvid.UUIDSet

	// SyncPending returns true if there are outstanding sync events in this instance's subscription.
	SyncPending() bool
}

// -- Syncer interface partial implementation.  Base Data needs to be supplemented with
// -- GetSyncSubs() to be a Syncer datatype.

// SyncedNames returns a set of data instance names to which it is synced.
// Legacy and will be removed after metadata refactor.
func (d *Data) SyncedNames() []dvid.InstanceName {
	return d.syncNames
}

// SyncedData returns a set of data UUIDs to which it is synced.
func (d *Data) SyncedData() dvid.UUIDSet {
	return d.syncData
}

// SyncPending returns true if there are outstanding sync events in this instance's subscription.
func (d *Data) SyncPending() bool {
	if manager == nil {
		return false
	}
	r, err := manager.repoFromUUID(d.rootUUID)
	if err != nil {
		return false
	}
	// Check if this data instance has any subscriptions and if so, are there messages in the channel.
	for _, subs := range r.subs {
		for _, sub := range subs {
			if sub.Notify == d.dataUUID && len(sub.Ch) > 0 {
				return true
			}
		}
	}
	return false
}

// CommitSyncer want to be notified when a node is committed.
type CommitSyncer interface {
	// SyncOnCommit is an asynchronous function that should be called when a node is committed.
	SyncOnCommit(dvid.UUID, dvid.VersionID)
}

// SetSyncByJSON takes a JSON object of sync names and UUID, and creates the sync graph
// and sets the data instance's sync.  If replace is false (default), the new sync
// is appended to the current syncs.
func SetSyncByJSON(d dvid.Data, uuid dvid.UUID, replace bool, in io.ReadCloser) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	jsonData := make(map[string]string)
	decoder := json.NewDecoder(in)
	if err := decoder.Decode(&jsonData); err != nil && err != io.EOF {
		return fmt.Errorf("Malformed JSON request in sync request: %v", err)
	}
	syncedCSV, ok := jsonData["sync"]
	if !ok {
		return fmt.Errorf("Could not find 'sync' value in POSTed JSON to sync request.")
	}

	syncedNames := strings.Split(syncedCSV, ",")
	if len(syncedNames) == 0 || (len(syncedNames) == 1 && syncedNames[0] == "") {
		syncedNames = []string{}
	}

	if len(syncedNames) == 0 && !replace {
		dvid.Infof("Ignored attempt to append no syncs to instance %q.\n", d.DataName())
		return nil
	}

	// Make sure all synced names currently exist under this UUID, then transform to data UUIDs.
	syncs := make(dvid.UUIDSet)
	for _, name := range syncedNames {
		data, err := GetDataByUUIDName(uuid, dvid.InstanceName(name))
		if err != nil {
			return fmt.Errorf("issue with requested sync name %q: %v", name, err)
		}
		syncs[data.DataUUID()] = struct{}{}
	}

	if err := SetSyncData(d, syncs, replace); err != nil {
		return err
	}
	return nil
}

// SetSyncData modifies the manager sync graphs and data instance's sync list.
// If replace is false (default), the new sync is appended to the current syncs.
func SetSyncData(data dvid.Data, syncs dvid.UUIDSet, replace bool) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	return manager.setSync(data, syncs, replace)
}

// NotifySubscribers sends a message to any data instances subscribed to the event.
func NotifySubscribers(e SyncEvent, m SyncMessage) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	// Get the repo from the version.
	repo, err := manager.repoFromVersion(m.Version)
	if err != nil {
		return err
	}

	// Use the repo notification system to notify internal subscribers.
	return repo.notifySubscribers(e, m)
}
