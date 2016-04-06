/*
   This file provides the internal pub/sub report.
*/

package datastore

import (
	"io"

	"github.com/janelia-flyem/dvid/dvid"
)

// SyncEvent identifies an event in which a data instance has modified its data
type SyncEvent struct {
	Instance dvid.InstanceName
	Event    string
}

// SyncMessage describes changes to a data instance for a given version.
type SyncMessage struct {
	Version dvid.VersionID
	Delta   interface{}
}

// SyncSub is a subscription request from an instance to be notified via a channel when
// a given data instance has a given event.
type SyncSub struct {
	Event  SyncEvent
	Notify dvid.InstanceName
	Ch     chan SyncMessage
	Done   chan struct{}
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
	// SetSync establishes a sync link between the receiver data instance and a list of named
	// data instances.
	SetSync(uuid dvid.UUID, in io.ReadCloser) error

	// GetSyncSubs returns the subscriptions that need to be created to keep this data
	// synced and may launch goroutines that will consume inbound channels of changes
	// from associated data.
	GetSyncSubs(dvid.Data) SyncSubs

	// SyncedNames returns a slice of instance names to which the receiver is synced.
	SyncedNames() []dvid.InstanceName
}

// CommitSyncer want to be notified when a node is committed.
type CommitSyncer interface {
	// SyncOnCommit is an asynchronous function that should be called when a node is committed.
	SyncOnCommit(dvid.UUID, dvid.VersionID)
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
