// +build !clustered,!gcloud

/*
	This file contains local server code supporting push/pull with
	optional delimiting using an ROI.  Note that the actual sending of data is implemented at the
    datatype level (if supporting ROI filtering) or the generic datastore package level if
    no ROI filtering is necessary.  In the latter case, we do not have to interpret the keys
    to determine their spatial context.

    TODO: When we actually have multiple varieties of target platform (cluster, gcloud managed vm, amzn ec2),
    the repo/DAG data structure needs to be cross-platform since the receiving DVID could be an entirely
    different platform.  For example, clustered DVID frontends could use an in-memory distributed
    kv store.
*/

package datastore

import (
	"fmt"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/valyala/gorpc"
)

var (
	pushMessageID rpc.MessageID = "datastore.Push"
)

const (
	sendRepoMsg  = "datastore.sendRepo"
	StartDataMsg = "datastore.startData"
	PutKVMsg     = "datastore.putKV"
)

const MaxBatchSize = 1000

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
	TypeName   dvid.TypeString
	InstanceID dvid.InstanceID
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

type txStats struct {
	// num key-value pairs
	numKV uint64

	// stats on value sizes on logarithmic scale to 10 MB
	numV0, numV1, numV10, numV100, numV1k, numV10k, numV100k, numV1m, numV10m uint64

	// some stats for timing
	lastTime  time.Time
	lastBytes uint64 // bytes received since lastTime
}

// record stats on size of values
func (t *txStats) addKV(kv *storage.KeyValue) {
	t.numKV++

	sz := len(kv.V)
	t.lastBytes += uint64(sz + len(kv.K))

	if sz == 0 {
		t.numV0++
		return
	}
	if sz < 10 {
		t.numV1++
		return
	}
	if sz < 100 {
		t.numV10++
		return
	}
	if sz < 1000 {
		t.numV100++
		return
	}
	if sz < 10000 {
		t.numV1k++
		return
	}
	if sz < 100000 {
		t.numV10k++
		return
	}
	if sz < 1000000 {
		t.numV100k++
		return
	}
	if sz < 10 {
		t.numV1m++
		return
	}
	t.numV10m++

	// Print progress?
	if elapsed := time.Since(t.lastTime); elapsed > 20 * time.Second {
		mb := float64(t.lastBytes) / 1000000
		sec := elapsed.Seconds()
		throughput := mb / sec
		dvid.Debugf("Push throughput: %5.2f MB/s (%.1f MB in %3f seconds)\n", throughput, mb, sec)

		t.lastTime = time.Now()
		t.lastBytes = 0
	}
}

func (p *pusher) printStats() {
	dvid.Infof("Stats for transfer of data %q:\n", p.dname)
	dvid.Infof("# kv pairs: %d\n", p.stats.numKV)
	dvid.Infof("Size of values transferred (bytes):\n")
	dvid.Infof(" key only:   %d", p.stats.numV0)
	dvid.Infof(" [1,9):      %d", p.stats.numV1)
	dvid.Infof(" [10,99):    %d\n", p.stats.numV10)
	dvid.Infof(" [100,999):  %d\n", p.stats.numV100)
	dvid.Infof(" [1k,10k):   %d\n", p.stats.numV1k)
	dvid.Infof(" [10k,100k): %d\n", p.stats.numV10k)
	dvid.Infof(" [100k,1m):  %d\n", p.stats.numV100k)
	dvid.Infof(" [1m,10m):   %d\n", p.stats.numV1m)
	dvid.Infof("  >= 10m:    %d\n", p.stats.numV10m)
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

// --- the functions at each state of FSM

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

	var versions map[dvid.VersionID]struct{}
	switch m.Transmit {
	case rpc.TransmitFlatten:
		versions = map[dvid.VersionID]struct{}{
			remoteV: struct{}{},
		}
		// Also have to make sure any data instances are rerooted if the root
		// no longer exists.
		for name, d := range p.repo.data {
			_, found := manager.uuidToVersion[d.UUID()]
			if !found {
				p.repo.data[name].SetUUID(m.UUID)
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
	store, err := storage.GetAssignedStore(d.TypeName)
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
	if err := storage.UpdateDataKey(kv.K, newInstanceID, newVersionID, 0); err != nil {
		return fmt.Errorf("Unable to update data key %v: %v", kv.K, err)
	}
	p.stats.addKV(kv)
	p.store.RawPut(kv.K, kv.V)
	p.received += uint64(len(kv.V) + len(kv.K))
	return nil
}

// ---- The following is the client side of a push command ----

// Push pushes a Repo to a remote DVID server at the target address.  An ROI delimiter
// can be specified in the Config.
func Push(uuid dvid.UUID, target string, config dvid.Config) error {
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
	filter, found, err := config.GetString("filter")
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
		return fmt.Errorf("Unable to connect (%s) for push: %s", target, err.Error())
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

	// For each data instance, send the data delimited by the roi
	for _, d := range txRepo.data {
		dvid.Infof("Sending instance %q data to %q\n", d.DataName(), target)
		if err := d.Send(s, transmit, filter, versions); err != nil {
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

// Make a copy of a repository, customizing it via config.
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

	dup, err := r.duplicate(versions, datanames)
	return dup, transmit, err
}
