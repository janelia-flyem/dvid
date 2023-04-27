//go:build !clustered && !gcloud
// +build !clustered,!gcloud

/*
	This file contains local server code supporting push/pull with optional delimiting
	using datatype-specific filters.

    TODO: When we actually have multiple varieties of target platform (cluster, gcloud managed vm, amzn ec2),
    the repo/DAG data structure needs to be cross-platform since the receiving DVID could be an entirely
    different platform.  For example, clustered DVID frontends could use an in-memory distributed
    kv store.
*/

package datastore

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/valyala/gorpc"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
)

// PushRepo pushes a Repo to a remote DVID server at the target address.
func PushRepo(uuid dvid.UUID, target string, config dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	if target == "" {
		target = rpc.DefaultAddress
		dvid.Infof("No target specified for push, defaulting to %q\n", rpc.DefaultAddress)
	}

	// Get the full local repo
	thisRepo, err := manager.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	// Get any filter
	filter, _, err := config.GetString("filter")
	if err != nil {
		return err
	}

	// Create a repo that is tailored by the push configuration, e.g.,
	// keeping just given data instances, etc.
	v, found := manager.uuidToVersion[uuid]
	if !found {
		return ErrInvalidUUID
	}
	txRepo, transmit, err := thisRepo.customize(v, config)
	if err != nil {
		return err
	}

	// Establish session with target, which may be itself
	s, err := rpc.NewSession(target, pushMessageID)
	if err != nil {
		return fmt.Errorf("unable to connect (%s) for push: %s", target, err.Error())
	}
	defer s.Close() // TODO -- check if can hang if error occurs during job

	// Send the repo metadata, transmit type, and other useful data for remote server.
	dvid.Infof("Sending repo %s data to %q\n", uuid, target)
	repoSerialization, err := txRepo.GobEncode()
	if err != nil {
		return err
	}
	repoMsg := repoTxMsg{
		Session:  s.ID(),
		Transmit: transmit,
		UUID:     uuid,
		Repo:     repoSerialization,
	}
	resp, err := s.Call()(sendRepoMsg, repoMsg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("push unnecessary -- versions at remote are already present")
	}

	// We should get back a version set to send, or nil = send all versions.
	versions, ok := resp.(map[dvid.VersionID]struct{})
	if !ok {
		return fmt.Errorf("received response during repo push that wasn't expected set of delta versions")
	}
	dvid.Debugf("Remote sent list of %d versions to send\n", len(versions))

	// For each data instance, send the data with optional datatype-specific filtering.
	ps := &PushSession{storage.FilterSpec(filter), versions, s, transmit}
	for _, d := range txRepo.data {
		dvid.Infof("Sending instance %q data to %q\n", d.DataName(), target)
		if err := d.PushData(ps); err != nil {
			dvid.Errorf("Aborting send of instance %q data\n", d.DataName())
			return err
		}
	}

	return nil
}

/*
func Pull(repo Repo, target string, config dvid.Config) error {
	// To Pull() we initiate a push from target.
	return nil
}
*/

// PushSession encapsulates parameters necessary for DVID-to-DVID push/pull processing.
type PushSession struct {
	Filter   storage.FilterSpec
	Versions map[dvid.VersionID]struct{}

	s rpc.Session
	t rpc.Transmit
}

// StartInstancePush initiates a data instance push.  After some number of Send
// calls, the EndInstancePush must be called.
func (p *PushSession) StartInstancePush(d dvid.Data) error {
	dmsg := DataTxInit{
		Session:    p.s.ID(),
		DataName:   d.DataName(),
		TypeName:   d.TypeName(),
		InstanceID: d.InstanceID(),
		Tags:       d.Tags(),
	}
	if _, err := p.s.Call()(StartDataMsg, dmsg); err != nil {
		return fmt.Errorf("couldn't send data instance %q start: %v\n", d.DataName(), err)
	}
	return nil
}

// SendKV sends a key-value pair.  The key-values may be buffered before sending
// for efficiency of transmission.
func (p *PushSession) SendKV(kv *storage.KeyValue) error {
	kvmsg := KVMessage{Session: p.s.ID(), KV: *kv, Terminate: false}
	if _, err := p.s.Call()(PutKVMsg, kvmsg); err != nil {
		return fmt.Errorf("error sending key-value to remote: %v", err)
	}
	return nil
}

// EndInstancePush terminates a data instance push.
func (p *PushSession) EndInstancePush() error {
	endmsg := KVMessage{Session: p.s.ID(), Terminate: true}
	if _, err := p.s.Call()(PutKVMsg, endmsg); err != nil {
		return fmt.Errorf("error sending terminate data to remote: %v", err)
	}
	return nil
}

// PushData transfers all key-value pairs pertinent to the given data instance.
// Each datatype can implement filters that can restrict the transmitted key-value pairs
// based on the given FilterSpec.  Note that because of the generality of this function,
// a particular datatype implementation could be much more efficient when implementing
// filtering.  For example, the imageblk datatype could scan its key-values using the ROI
// to generate keys (since imageblk keys will likely be a vast superset of ROI spans),
// while this generic routine will scan every key-value pair for a data instance and
// query the ROI to see if this key is ok to send.
func PushData(d dvid.Data, p *PushSession) error {
	// We should be able to get the backing store (only ordered kv for now)
	store, err := GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v", d.DataName(), err)
	}

	// See if this data instance implements a Send filter.
	var filter storage.Filter
	filterer, ok := d.(storage.Filterer)
	if ok {
		var err error
		filter, err = filterer.NewFilter(p.Filter)
		if err != nil {
			return err
		}
	}

	// pick any version because flatten transmit will only have one version, and all or branch transmit will
	// be looking at all versions anyway.
	if len(p.Versions) == 0 {
		return fmt.Errorf("need at least one version to send")
	}
	var v dvid.VersionID
	for v = range p.Versions {
		break
	}
	ctx := NewVersionedCtx(d, v)

	// Send the initial data instance start message
	if err := p.StartInstancePush(d); err != nil {
		return err
	}

	// Send this instance's key-value pairs
	var wg sync.WaitGroup
	wg.Add(1)

	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64
	keysOnly := false
	if p.t == rpc.TransmitFlatten {
		// Start goroutine to receive flattened key-value pairs and transmit to remote.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					if err := p.EndInstancePush(); err != nil {
						dvid.Errorf("Bad data %q termination: %v\n", d.DataName(), err)
					}
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s) [flattened]\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					return
				}
				kvTotal++
				curBytes := uint64(len(tkv.V) + len(tkv.K))
				bytesTotal += curBytes
				if filter != nil {
					skip, err := filter.Check(tkv)
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				kv := storage.KeyValue{
					K: ctx.ConstructKey(tkv.K),
					V: tkv.V,
				}
				if err := p.SendKV(&kv); err != nil {
					dvid.Errorf("Bad data %q send KV: %v", d.DataName(), err)
				}
			}
		}()

		begKey, endKey := ctx.TKeyRange()
		err := store.ProcessRange(ctx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("received nil chunk in flatten push for data %s", d.DataName())
			}
			ch <- c.TKeyValue
			return nil
		})
		ch <- nil
		if err != nil {
			return fmt.Errorf("error in flatten push for data %q: %v", d.DataName(), err)
		}
	} else {
		// Start goroutine to receive all key-value pairs and transmit to remote.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					if err := p.EndInstancePush(); err != nil {
						dvid.Errorf("Bad data %q termination: %v\n", d.DataName(), err)
					}
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s)\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					return
				}
				if !ctx.ValidKV(kv, p.Versions) {
					continue
				}
				kvTotal++
				curBytes := uint64(len(kv.V) + len(kv.K))
				bytesTotal += curBytes
				if filter != nil {
					tkey, err := storage.TKeyFromKey(kv.K)
					if err != nil {
						dvid.Errorf("couldn't get %q TKey from Key %v: %v\n", d.DataName(), kv.K, err)
						continue
					}
					skip, err := filter.Check(&storage.TKeyValue{K: tkey, V: kv.V})
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if err := p.SendKV(kv); err != nil {
					dvid.Errorf("Bad data %q send KV: %v", d.DataName(), err)
				}
			}
		}()

		begKey, endKey := ctx.KeyRange()
		if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch, nil); err != nil {
			return fmt.Errorf("push voxels %q range query: %v", d.DataName(), err)
		}
	}
	wg.Wait()
	return nil
}

var (
	pushMessageID rpc.MessageID = "datastore.Push"
)

const (
	sendRepoMsg  = "datastore.sendRepo"
	StartDataMsg = "datastore.startData"
	PutKVMsg     = "datastore.putKV"
)

func init() {
	rpc.RegisterSessionMaker(pushMessageID, rpc.NewSessionHandlerFunc(makePushSession))

	d := rpc.Dispatcher()
	d.AddFunc(sendRepoMsg, handleSendRepo)
	d.AddFunc(StartDataMsg, handleStartData)
	d.AddFunc(PutKVMsg, handlePutKV)

	gorpc.RegisterType(&repoTxMsg{})
	gorpc.RegisterType(&DataTxInit{})
	gorpc.RegisterType(&KVMessage{})
}

type repoTxMsg struct {
	Session  rpc.SessionID
	Transmit rpc.Transmit
	UUID     dvid.UUID // either the version to send if flatten, the child of a branch, or an identifier for remote root
	Repo     []byte    // serialized repo
}

type DataTxInit struct {
	Session    rpc.SessionID
	DataName   dvid.InstanceName
	DataUUID   dvid.UUID
	RootUUID   dvid.UUID
	TypeName   dvid.TypeString
	InstanceID dvid.InstanceID
	Tags       map[string]string
}

// ---- fulfills storage.DataSpec interface ----

type DataSpec struct {
	tx *DataTxInit
}

func (d *DataSpec) DataName() dvid.InstanceName {
	return d.tx.DataName
}

func (d *DataSpec) DataUUID() dvid.UUID {
	return d.tx.DataUUID
}

func (d *DataSpec) RootUUID() dvid.UUID {
	return d.tx.RootUUID
}

func (d *DataSpec) Tags() map[string]string {
	return d.tx.Tags
}

func (d *DataSpec) TypeName() dvid.TypeString {
	return d.tx.TypeName
}

// KVMessage packages a key-value pair for transmission to a remote DVID as well as control
// of the receiving FSM.
type KVMessage struct {
	Session   rpc.SessionID
	KV        storage.KeyValue
	Terminate bool // true if this message is the last txn for this data instance and KV is invalid.
}

func getPusherSession(s rpc.SessionID) (*pusher, error) {
	handler, err := rpc.GetSessionHandler(s)
	if err != nil {
		return nil, err
	}
	p, ok := handler.(*pusher)
	if !ok {
		return nil, fmt.Errorf("handler for session %d is not expected pusher type: %v", s, handler)
	}
	return p, nil
}

func handleSendRepo(m *repoTxMsg) (map[dvid.VersionID]struct{}, error) {
	p, err := getPusherSession(m.Session)
	if err != nil {
		return nil, err
	}
	return p.readRepo(m)
}

func handleStartData(m *DataTxInit) error {
	p, err := getPusherSession(m.Session)
	if err != nil {
		return err
	}
	return p.startData(m)
}

func handlePutKV(m *KVMessage) error {
	p, err := getPusherSession(m.Session)
	if err != nil {
		return err
	}
	return p.putData(m)
}

// --- The following is the server side of a push command ----

// TODO -- If we are actively reading instead of passively taking messages, consider
// the way lexer is written:
// http://golang.org/src/pkg/text/template/parse/lex.go?h=lexer#L86

type pusher struct {
	sessionID rpc.SessionID
	uuid      dvid.UUID
	repo      *repoT

	instanceMap dvid.InstanceMap // map from pushed to local instance ids
	versionMap  dvid.VersionMap  // map from pushed to local version ids

	// current stats for data instance transfer
	dname dvid.InstanceName
	stats *txStats
	store storage.KeyValueDB

	startTime time.Time
	received  uint64 // bytes received over entire push
}

func (p *pusher) printStats() {
	dvid.Infof("Stats for transfer of data %q:\n", p.dname)
	p.stats.printStats()
}

func makePushSession(rpc.MessageID) (rpc.SessionHandler, error) {
	dvid.Debugf("Creating push session...\n")
	return new(pusher), nil
}

// --- rpc.SessionHandler interface implementation ---

func (p *pusher) ID() rpc.SessionID {
	return p.sessionID
}

func (p *pusher) Open(sid rpc.SessionID) error {
	dvid.Debugf("Push start, session %d\n", sid)
	p.sessionID = sid
	p.startTime = time.Now()
	return nil
}

func (p *pusher) Close() error {
	gb := float64(p.received) / 1000000000
	dvid.Debugf("Closing push of uuid %s: received %.1f GBytes in %s\n", p.repo.uuid, gb, time.Since(p.startTime))

	// Add this repo to current DVID server
	if err := manager.addRepo(p.repo); err != nil {
		return err
	}
	return nil
}

func (p *pusher) readRepo(m *repoTxMsg) (map[dvid.VersionID]struct{}, error) {
	dvid.Debugf("Reading repo for push of %s...\n", m.UUID)

	if manager == nil {
		return nil, ErrManagerNotInitialized
	}
	p.received += uint64(len(m.Repo))

	// Get the repo metadata
	p.repo = new(repoT)
	if err := p.repo.GobDecode(m.Repo); err != nil {
		return nil, err
	}

	// TODO: Check to see if the transmitted repo is in current metadata.

	p.uuid = m.UUID
	remoteV, err := p.repo.versionFromUUID(m.UUID) // do this before we remap the repo's IDs
	if err != nil {
		return nil, err
	}
	repoID, err := manager.newRepoID()
	if err != nil {
		return nil, err
	}
	p.repo.id = repoID

	p.instanceMap, p.versionMap, err = p.repo.remapLocalIDs()
	if err != nil {
		return nil, err
	}

	// After getting remote repo, adjust data instances for local settings.
	for _, d := range p.repo.data {
		// see if it needs to adjust versions.
		dv, needsUpdate := d.(VersionRemapper)
		if needsUpdate {
			if err := dv.RemapVersions(p.versionMap); err != nil {
				return nil, err
			}
		}

		// check if we have an assigned store for this data instance.
		store, err := storage.GetAssignedStore(d)
		if err != nil {
			return nil, err
		}
		d.SetKVStore(store)
		dvid.Debugf("Assigning as default store of data instance %q @ %s: %s\n", d.DataName(), d.RootUUID(), store)
	}

	var versions map[dvid.VersionID]struct{}
	switch m.Transmit {
	case rpc.TransmitFlatten:
		versions = map[dvid.VersionID]struct{}{
			remoteV: struct{}{},
		}
		// Also have to make sure any data instances are rerooted if the root
		// no longer exists.
		for name, d := range p.repo.data {
			_, found := manager.uuidToVersion[d.RootUUID()]
			if !found {
				p.repo.data[name].SetRootUUID(m.UUID)
			}
		}
	case rpc.TransmitAll:
		versions, err = getDeltaAll(p.repo, m.UUID)
		if err != nil {
			return nil, err
		}
	case rpc.TransmitBranch:
		versions, err = getDeltaBranch(p.repo, m.UUID)
		if err != nil {
			return nil, err
		}
	}
	if versions == nil {
		return nil, fmt.Errorf("no push required -- remote has necessary versions")
	}
	dvid.Debugf("Finished comparing repos -- requesting %d versions from source.\n", len(versions))
	return versions, nil
}

// compares remote Repo with local one, determining a list of versions that
// need to be sent from remote to bring the local DVID up-to-date.
func getDeltaAll(remote *repoT, uuid dvid.UUID) (map[dvid.VersionID]struct{}, error) {
	// Determine all version ids of remote DAG nodes that aren't in the local DAG.
	// Since VersionID can differ among DVID servers, we need to compare using UUIDs
	// then convert to VersionID.
	delta := make(map[dvid.VersionID]struct{})
	for _, rnode := range remote.dag.nodes {
		lv, found := manager.uuidToVersion[rnode.uuid]
		if found {
			dvid.Debugf("Both remote and local have uuid %s... skipping\n", rnode.uuid)
		} else {
			dvid.Debugf("Found version %s in remote not in local: sending local version id %d\n", rnode.uuid, lv)
			delta[lv] = struct{}{}
		}
	}
	return delta, nil
}

// compares remote Repo with local one, determining a list of versions that
// need to be sent from remote to bring the local DVID up-to-date.
func getDeltaBranch(remote *repoT, branch dvid.UUID) (map[dvid.VersionID]struct{}, error) {
	// TODO branch
	return nil, fmt.Errorf("Branch transmission not currently supported in DVID")
}

func (p *pusher) startData(d *DataTxInit) error {
	p.stats = new(txStats)
	p.stats.lastTime = time.Now()
	p.stats.lastBytes = 0

	p.dname = d.DataName

	// Get the store associated with this data instance.
	ds := &DataSpec{
		tx: d,
	}
	store, err := storage.GetAssignedStore(ds)
	if err != nil {
		return err
	}
	var ok bool
	p.store, ok = store.(storage.KeyValueDB)
	if !ok {
		return fmt.Errorf("backend store %q for data type %q of tx data %q is not KeyValueDB-compatable", p.store, d.TypeName, d.DataName)
	}
	dvid.Debugf("Push (session %d) starting transfer of data %q...\n", p.sessionID, d.DataName)
	return nil
}

func (p *pusher) putData(kvmsg *KVMessage) error {
	// If this is a termination token
	if kvmsg.Terminate {
		p.printStats()
		return nil
	}

	// Process the key-value pair
	kv := &kvmsg.KV
	oldInstance, oldVersion, _, err := storage.DataKeyToLocalIDs(kv.K)
	if err != nil {
		return err
	}

	// Modify the transmitted key-value to have local instance and version ids.
	newInstanceID, found := p.instanceMap[oldInstance]
	if !found {
		return fmt.Errorf("Received key with instance id (%d) not present in repo: %v", oldInstance, p.instanceMap)
	}
	newVersionID, found := p.versionMap[oldVersion]
	if !found {
		return fmt.Errorf("Received key with version id (%d) not present in repo: %v", oldVersion, p.versionMap)
	}

	// Compute the updated key-value
	// TODO: When client IDs are used, need to transmit list of pertinent clients and their IDs or just use 0 as here.
	if err := storage.UpdateDataKey(kv.K, newInstanceID, newVersionID, 0); err != nil {
		return fmt.Errorf("Unable to update data key %v: %v", kv.K, err)
	}
	p.stats.addKV(kv.K, kv.V)
	p.store.RawPut(kv.K, kv.V)
	p.received += uint64(len(kv.V) + len(kv.K))
	return nil
}

// Make a copy of a repository, customizing it via config.
// TODO -- modify data instance properties based on filters.
func (r *repoT) customize(v dvid.VersionID, config dvid.Config) (*repoT, rpc.Transmit, error) {
	// Since we can have names separated by commas, split them
	namesStr, found, err := config.GetString("data")
	if err != nil {
		return nil, rpc.TransmitUnknown, err
	}
	var datanames dvid.InstanceNames
	if found {
		for _, name := range strings.Split(namesStr, ",") {
			datanames = append(datanames, dvid.InstanceName(strings.TrimSpace(name)))
		}
	}

	// Check the transmission behavior: all, flatten, or deltas.
	var versions map[dvid.VersionID]struct{}

	transmitStr, found, err := config.GetString("transmit")
	if err != nil {
		return nil, rpc.TransmitUnknown, err
	}
	if !found {
		transmitStr = "all"
	}
	var transmit rpc.Transmit
	switch transmitStr {
	case "flatten":
		transmit = rpc.TransmitFlatten
		versions = map[dvid.VersionID]struct{}{
			v: struct{}{},
		}
	case "all":
		transmit = rpc.TransmitAll
		versions = r.versionSet()
	case "branch":
		transmit = rpc.TransmitBranch
		versions = r.versionSet()
	default:
		return nil, rpc.TransmitUnknown, fmt.Errorf("unknown transmit %s", transmitStr)
	}

	// Make a copy filtering by allowed data instances.
	r.RLock()
	defer r.RUnlock()

	dup, err := r.duplicate(versions, datanames, nil)
	return dup, transmit, err
}
