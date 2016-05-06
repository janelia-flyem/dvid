// +build kvautobus

package kvautobus

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/semver"

	"github.com/tinylib/msgp/msgp"
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in kvautobus: %v\n", err)
	}
	e := Engine{"kvautobus", "Janelia KVAutobus", ver}
	storage.RegisterEngine(e)
}

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

func parseConfig(config dvid.StoreConfig) (path string, timeout time.Duration, owner string, collection dvid.UUID, err error) {
	c := config.GetAll()

	v, found := c["path"]
	if !found {
		err = fmt.Errorf("%q must be specified for kvautobus configuration", "path")
		return
	}
	var ok bool
	path, ok = v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string (%v)", "path", v)
		return
	}

	v, found = c["timeout"]
	if found {
		t, ok := v.(int64)
		if !ok {
			err = fmt.Errorf("%q setting must be an int64 for # seconds, not %s (%v)", "timeout", reflect.TypeOf(v), v)
			return
		}
		if t != 0 {
			timeout = time.Duration(t) * time.Second
		}
	}

	v, found = c["collection"]
	if !found {
		err = fmt.Errorf("kvautobus store must have collection specification for billing.")
		return
	}
	cstr, ok := v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string, not %s (%v)", "collection", reflect.TypeOf(v), v)
		return
	}
	collection = dvid.UUID(cstr)

	v, found = c["owner"]
	if !found {
		err = fmt.Errorf("kvautobus store must have owner specification for billing.")
		return
	}
	owner, ok = v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string, not %s (%v)", "owner", reflect.TypeOf(v), v)
		return
	}
	return
}

// NewStore returns KVAutobus store.  The passed StoreConfig must have a valid Path to the
// KVAutobus server.
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	path, timeout, owner, collection, err := parseConfig(config)
	if err != nil {
		return nil, false, err
	}
	kv := &KVAutobus{
		host:       path,
		config:     config,
		client:     http.Client{Timeout: timeout},
		owner:      owner,
		collection: collection,
	}
	if err := kv.sendOwnership(); err != nil {
		return nil, false, err
	}
	return kv, false, nil
}

func encodeKey(k []byte) string {
	return base64.URLEncoding.EncodeToString(k)
}

func decodeKey(b64key string) ([]byte, error) {
	return base64.URLEncoding.DecodeString(b64key)
}

type KVAutobus struct {
	host string

	// Config at time of Open()
	config dvid.StoreConfig

	// http client for KVAutobus service
	client http.Client

	// owner id for billing
	owner string

	// collection id for this store
	collection dvid.UUID
}

// notify KVAutobus what the current billing ids are for the registered datasets.
func (db *KVAutobus) sendOwnership() error {
	payload := fmt.Sprintf("{%s: %s}", db.owner, db.collection)

	url := fmt.Sprintf("%s/kvautobus/api/billing", db.host)
	resp, err := db.client.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status code returned (%d) from POST to billing map: %s", resp.StatusCode, url)
	}
	return nil
}

func (db *KVAutobus) String() string {
	return fmt.Sprintf("KVAutobus @ %s", db.host)
}

func (db *KVAutobus) Close() {
	// no op
}

func (db *KVAutobus) Equal(c dvid.StoreConfig) bool {
	path, _, _, collection, err := parseConfig(c)
	if err != nil {
		return false
	}
	if db.host == path && db.collection == collection {
		return true
	}
	return false
}

func (db *KVAutobus) RawGet(key storage.Key) ([]byte, error) {
	b64key := encodeKey(key)
	url := fmt.Sprintf("%s/kvautobus/api/value/%s/%s/", db.host, db.collection, b64key)

	timedLog := dvid.NewTimeLog()
	resp, err := db.client.Get(url)
	if err != nil {
		return nil, err
	}
	timedLog.Infof("PROXY get to %s returned %d\n", db.host, resp.StatusCode)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Handle no key found.
	}

	r := msgp.NewReader(bufio.NewReader(resp.Body))
	var bin Binary
	if err := bin.DecodeMsg(r); err != nil {
		return nil, err
	}
	storage.StoreValueBytesRead <- len(bin)
	return []byte(bin), nil
}

// call KVAutobus key_range API
func (db *KVAutobus) getKeyRange(kStart, kEnd storage.Key) (Ks, error) {
	b64key1 := encodeKey(kStart)
	b64key2 := encodeKey(kEnd)
	url := fmt.Sprintf("%s/kvautobus/api/key_range/%s/%s/%s/", db.host, db.collection, b64key1, b64key2)

	timedLog := dvid.NewTimeLog()
	resp, err := db.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Handle no keys found.
	}

	r := msgp.NewReader(bufio.NewReader(resp.Body))
	var mks Ks
	if err := mks.DecodeMsg(r); err != nil {
		return nil, err
	}
	for _, mk := range mks {
		storage.StoreKeyBytesRead <- len(mk)
	}
	timedLog.Infof("PROXY key_range to %s returned %d (%d keys)\n", db.host, resp.StatusCode, len(mks))
	return mks, nil
}

// call KVAutobus keyvalue_range API
func (db *KVAutobus) getKVRange(ctx storage.Context, kStart, kEnd storage.Key) (KVs, error) {
	// Get any request context and pass to kvautobus for tracking.
	reqctx, ok := ctx.(storage.RequestCtx)
	var reqID string
	if ok {
		parts := strings.Split(reqctx.GetRequestID(), "/")
		if len(parts) > 0 {
			reqID = parts[len(parts)-1]
		}
	}

	// Construct the KVAutobus URL
	b64key1 := encodeKey(kStart)
	b64key2 := encodeKey(kEnd)
	url := fmt.Sprintf("%s/kvautobus/api/keyvalue_range/%s/%s/%s/?=%s", db.host, db.collection, b64key1, b64key2, reqID)

	timedLog := dvid.NewTimeLog()
	resp, err := db.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Handle no keys found.
	}

	r := msgp.NewReader(bufio.NewReader(resp.Body))
	var mkvs KVs
	if err := mkvs.DecodeMsg(r); err != nil {
		dvid.Errorf("Couldn't decode getKVRange return\n")
		return nil, err
	}
	for _, mkv := range mkvs {
		storage.StoreKeyBytesRead <- len(mkv[0])
		storage.StoreValueBytesRead <- len(mkv[1])
	}

	timedLog.Infof("[%s] PROXY keyvalue_range to %s returned %d (%d kv pairs)\n", reqID, db.host, resp.StatusCode, len(mkvs))
	return mkvs, nil
}

// call KVAutobus keyvalue_range API and covert to slice of storage.KeyValue
func (db *KVAutobus) getRange(ctx storage.Context, kStart, kEnd storage.Key) ([]*storage.KeyValue, error) {
	mkvs, err := db.getKVRange(ctx, kStart, kEnd)
	if err != nil {
		return nil, err
	}
	if mkvs == nil {
		return nil, nil
	}
	kvs := make([]*storage.KeyValue, len(mkvs))
	for i, mkv := range mkvs {
		kvs[i] = &storage.KeyValue{storage.Key(mkv[0]), []byte(mkv[1])}
	}
	return kvs, nil
}

func (db *KVAutobus) putRange(kvs []storage.KeyValue) error {
	// Transform to an encodable type
	mkvs := make(KVs, len(kvs))
	for i, kv := range kvs {
		storage.StoreKeyBytesWritten <- len(kv.K)
		storage.StoreValueBytesWritten <- len(kv.V)
		mkvs[i] = KV{Binary(kv.K), Binary(kv.V)}
	}

	// Create pipe from encoding to posting
	pr, pw := io.Pipe()
	w := msgp.NewWriter(pw)
	go func() {
		mkvs.EncodeMsg(w)
		w.Flush()
		pw.Close()
	}()

	// Send the data
	url := fmt.Sprintf("%s/kvautobus/api/keyvalue_range/%s", db.host, db.collection)
	resp, err := db.client.Post(url, "application/x-msgpack", pr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("Can't POST to an already stored key.  KVAutobus returned status %d (%s)", resp.StatusCode, url)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status code returned (%d) from put range request: %s", resp.StatusCode, url)
	}
	return nil
}

func (db *KVAutobus) deleteRange(kStart, kEnd storage.Key) error {
	b64key1 := encodeKey(kStart)
	b64key2 := encodeKey(kEnd)
	url := fmt.Sprintf("%s/kvautobus/api/keyvalue_range/%s/%s/%s/", db.host, db.collection, b64key1, b64key2)

	timedLog := dvid.NewTimeLog()
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := db.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	timedLog.Infof("PROXY delete keyvalue_range to %s returned %d\n", db.host, resp.StatusCode)
	return nil
}

// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
// retrieval like DVID-to-DVID communication and should not be used by data type
// implementations if possible.  A nil is sent down the channel when the
// range is complete.
func (db *KVAutobus) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue) error {
	var value []byte
	if keysOnly {
		keys, err := db.getKeyRange(kStart, kEnd)
		if err != nil {
			return err
		}
		for _, key := range keys {
			out <- &storage.KeyValue{storage.Key(key), value}
		}
	} else {
		kvs, err := db.getKVRange(nil, kStart, kEnd)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			out <- &storage.KeyValue{storage.Key(kv[0]), []byte(kv[1])}
		}
	}
	return nil
}

func (db *KVAutobus) RawPut(key storage.Key, value []byte) error {
	b64key := encodeKey(key)
	url := fmt.Sprintf("%s/kvautobus/api/value/%s/%s/", db.host, db.collection, b64key)
	bin := Binary(value)

	storage.StoreKeyBytesWritten <- len(key)
	storage.StoreValueBytesWritten <- len(value)

	// Create pipe from encoding to posting
	pr, pw := io.Pipe()
	w := msgp.NewWriter(pw)
	go func() {
		bin.EncodeMsg(w)
		w.Flush()
		pw.Close()
	}()

	resp, err := db.client.Post(url, "application/x-msgpack", pr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("Can't POST to an already stored key.  KVAutobus returned status %d (%s)", resp.StatusCode, url)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status code returned (%d) from put request: %s", resp.StatusCode, url)
	}
	return nil
}

func (db *KVAutobus) RawDelete(key storage.Key) error {
	return db.deleteRange(key, key)
}

// ---- OrderedKeyValueGetter interface ------

// Get returns a value given a key.
func (db *KVAutobus) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in Get()")
	}
	if ctx.Versioned() {
		vctx, ok := ctx.(storage.VersionedCtx)
		if !ok {
			return nil, fmt.Errorf("Bad Get(): context is versioned but doesn't fulfill interface: %v", ctx)
		}

		// Get all versions of this key and return the most recent
		// log.Printf("  kvautobus versioned get of key %v\n", k)
		key := ctx.ConstructKey(tk)
		dvid.Infof("   Get on key: %s\n", hex.EncodeToString(key))
		values, err := db.getSingleKeyVersions(vctx, tk)
		// log.Printf("            got back %v\n", values)
		if err != nil {
			return nil, err
		}
		kv, err := vctx.VersionedKeyValue(values)
		// log.Printf("  after deversioning: %v\n", kv)
		if kv != nil {
			return kv.V, err
		}
		return nil, err
	} else {
		key := ctx.ConstructKey(tk)
		// log.Printf("  kvautobus unversioned get of key %v\n", key)
		v, err := db.RawGet(key)
		storage.StoreValueBytesRead <- len(v)
		return v, err
	}
}

// getSingleKeyVersions returns all versions of a key.  These key-value pairs will be sorted
// in ascending key order and could include a tombstone key.
func (db *KVAutobus) getSingleKeyVersions(vctx storage.VersionedCtx, k []byte) ([]*storage.KeyValue, error) {
	kStart, err := vctx.MinVersionKey(k)
	if err != nil {
		return nil, err
	}
	kEnd, err := vctx.MaxVersionKey(k)
	if err != nil {
		return nil, err
	}
	kvs, err := db.getRange(vctx, kStart, kEnd)
	if err != nil {
		return nil, err
	}
	return kvs, nil
}

type errorableKV struct {
	*storage.KeyValue
	error
}

func sendKV(vctx storage.VersionedCtx, versions []*storage.KeyValue, ch chan errorableKV) {
	if len(versions) != 0 {
		kv, err := vctx.VersionedKeyValue(versions)
		if err != nil {
			ch <- errorableKV{nil, err}
			return
		}
		if kv != nil {
			// fmt.Printf("Sending kv: %v\n", kv)
			ch <- errorableKV{kv, nil}
		}
	}
}

// versionedRange sends a range of key-value pairs for a particular version down a channel.
func (db *KVAutobus) versionedRange(vctx storage.VersionedCtx, kStart, kEnd storage.TKey, ch chan errorableKV, keysOnly bool) {
	minKey, err := vctx.MinVersionKey(kStart)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	maxKey, err := vctx.MaxVersionKey(kEnd)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	maxVersionKey, err := vctx.MaxVersionKey(kStart)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}

	kvs, err := db.getRange(vctx, minKey, maxKey)
	if err != nil {
		ch <- errorableKV{nil, err}
		return
	}
	versions := []*storage.KeyValue{}
	for _, kv := range kvs {
		// Did we pass all versions for last key read?
		if bytes.Compare(kv.K, maxVersionKey) > 0 {
			indexBytes, err := storage.TKeyFromKey(kv.K)
			if err != nil {
				ch <- errorableKV{nil, err}
				return
			}
			maxVersionKey, err = vctx.MaxVersionKey(indexBytes)
			if err != nil {
				ch <- errorableKV{nil, err}
				return
			}
			// log.Printf("->maxVersionKey %v (transmitting %d values)\n", maxVersionKey, len(values))
			sendKV(vctx, versions, ch)
			versions = []*storage.KeyValue{}
		}
		// Did we pass the final key?
		if bytes.Compare(kv.K, maxKey) > 0 {
			if len(versions) > 0 {
				sendKV(vctx, versions, ch)
			}
			break
		}
		// log.Printf("Appending value with key %v\n", itKey)
		versions = append(versions, kv)
	}
	if len(versions) >= 0 {
		sendKV(vctx, versions, ch)
	}
	ch <- errorableKV{nil, nil}
}

// unversionedRange sends a range of key-value pairs down a channel.
func (db *KVAutobus) unversionedRange(ctx storage.Context, kStart, kEnd storage.TKey, ch chan errorableKV, keysOnly bool) {
	keyBeg := ctx.ConstructKey(kStart)
	keyEnd := ctx.ConstructKey(kEnd)

	kvs, err := db.getRange(ctx, keyBeg, keyEnd)
	if err != nil {
		ch <- errorableKV{nil, err}
	}
	for _, kv := range kvs {
		ch <- errorableKV{kv, nil}
	}
	ch <- errorableKV{nil, nil}
}

// KeysInRange returns a range of present keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
func (db *KVAutobus) KeysInRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]storage.TKey, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in KeysInRange()")
	}
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, true)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, true)
		}
	}()

	// Consume the keys.
	values := []storage.TKey{}
	for {
		result := <-ch
		if result.KeyValue == nil {
			return values, nil
		}
		if result.error != nil {
			return nil, result.error
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return nil, err
		}
		values = append(values, tk)
	}
}

// SendKeysInRange sends a range of keys spanning (kStart, kEnd).  Values
// associated with the keys are not read.   If the keys are versioned, only keys
// in the ancestor path of the current context's version will be returned.
// End of range is marked by a nil key.
func (db *KVAutobus) SendKeysInRange(ctx storage.Context, kStart, kEnd storage.TKey, kch storage.KeyChan) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in SendKeysInRange()")
	}
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, true)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, true)
		}
	}()

	// Consume the keys.
	for {
		result := <-ch
		if result.KeyValue == nil {
			kch <- nil
			return nil
		}
		if result.error != nil {
			kch <- nil
			return result.error
		}
		kch <- result.KeyValue.K
	}
}

// GetRange returns a range of values spanning (kStart, kEnd) keys.  These key-value
// pairs will be sorted in ascending key order.  If the keys are versioned, all key-value
// pairs for the particular version will be returned.
func (db *KVAutobus) GetRange(ctx storage.Context, kStart, kEnd storage.TKey) ([]*storage.TKeyValue, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in GetRange()")
	}
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx == nil || !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, false)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, false)
		}
	}()

	// Consume the key-value pairs.
	values := []*storage.TKeyValue{}
	for {
		result := <-ch
		if result.KeyValue == nil {
			return values, nil
		}
		if result.error != nil {
			return nil, result.error
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return nil, err
		}
		tkv := storage.TKeyValue{tk, result.KeyValue.V}
		values = append(values, &tkv)
	}
}

// ProcessRange sends a range of key-value pairs to chunk handlers.  If the keys are versioned,
// only key-value pairs for kStart's version will be transmitted.
func (db *KVAutobus) ProcessRange(ctx storage.Context, kStart, kEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in ProcessRange()")
	}
	ch := make(chan errorableKV)

	// Run the range query on a potentially versioned key in a goroutine.
	go func() {
		if ctx == nil || !ctx.Versioned() {
			db.unversionedRange(ctx, kStart, kEnd, ch, false)
		} else {
			db.versionedRange(ctx.(storage.VersionedCtx), kStart, kEnd, ch, false)
		}
	}()

	// Consume the key-value pairs.
	for {
		result := <-ch
		if result.KeyValue == nil {
			return nil
		}
		if result.error != nil {
			return result.error
		}
		if op.Wg != nil {
			op.Wg.Add(1)
		}
		tk, err := storage.TKeyFromKey(result.KeyValue.K)
		if err != nil {
			return err
		}
		tkv := storage.TKeyValue{tk, result.KeyValue.V}
		chunk := &storage.Chunk{op, &tkv}
		if err := f(chunk); err != nil {
			return err
		}
	}
}

// ---- KeyValueSetter interface ------

// Put writes a value with given key.  Since KVAutobus is immutable, we do not
// have to worry about a PUT on a previously deleted key.
func (db *KVAutobus) Put(ctx storage.Context, tk storage.TKey, v []byte) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}

	key := ctx.ConstructKey(tk)
	err := db.RawPut(key, v)
	if err != nil {
		return err
	}
	return nil
}

// Delete removes a value with given key.
func (db *KVAutobus) Delete(ctx storage.Context, tk storage.TKey) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in Delete()")
	}
	key := ctx.ConstructKey(tk)
	return db.RawDelete(key)
}

// ---- OrderedKeyValueSetter interface ------

// PutRange puts type key-value pairs that have been sorted in sequential key order.
func (db *KVAutobus) PutRange(ctx storage.Context, tkvs []storage.TKeyValue) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in PutRange()")
	}
	kvs := make([]storage.KeyValue, len(tkvs))
	for i, tkv := range tkvs {
		kvs[i] = storage.KeyValue{ctx.ConstructKey(tkv.K), tkv.V}
	}
	return db.putRange(kvs)
}

// DeleteRange removes all key-value pairs with keys in the given range.  Versioned
// contexts cannot use immutable stores to delete ranges.
func (db *KVAutobus) DeleteRange(ctx storage.Context, kStart, kEnd storage.TKey) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}
	if ctx.Versioned() {
		return fmt.Errorf("DeleteRange() not supported for versioned contexts in immutable store")
	}
	keyBeg := ctx.ConstructKey(kStart)
	keyEnd := ctx.ConstructKey(kEnd)
	return db.deleteRange(keyBeg, keyEnd)
}

// DeleteAll deletes all key-value associated with a context (data instance and version).
func (db *KVAutobus) DeleteAll(ctx storage.Context, allVersions bool) error {
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteAll()")
	}
	if !allVersions {
		return fmt.Errorf("DeleteAll() not supported for immutable store unless for all versions")
	}
	keyBeg, keyEnd := ctx.KeyRange()
	return db.deleteRange(keyBeg, keyEnd)
}

// --- Batcher interface ----

type goBatch struct {
	db   *KVAutobus
	ctx  storage.Context
	vctx storage.VersionedCtx
	kvs  []storage.KeyValue
}

// NewBatch returns an implementation that allows batch writes
func (db *KVAutobus) NewBatch(ctx storage.Context) storage.Batch {
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}
	var vctx storage.VersionedCtx
	var ok bool
	vctx, ok = ctx.(storage.VersionedCtx)
	if !ok {
		vctx = nil
	}
	return &goBatch{db, ctx, vctx, []storage.KeyValue{}}
}

// --- Batch interface ---

func (batch *goBatch) Delete(tk storage.TKey) {
	dvid.Criticalf("batch.Delete() attempted in immutable store\n")
}

func (batch *goBatch) Put(tk storage.TKey, v []byte) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Put()\n")
		return
	}
	key := batch.ctx.ConstructKey(tk)
	batch.kvs = append(batch.kvs, storage.KeyValue{key, v})
}

func (batch *goBatch) Commit() error {
	return batch.db.putRange(batch.kvs)
}
