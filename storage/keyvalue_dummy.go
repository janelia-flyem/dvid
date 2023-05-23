// Implements a dummy key-value store

package storage

import "github.com/janelia-flyem/dvid/dvid"

func DummyKVStore() OrderedKeyValueDB {
	return dummyKVStore{}
}

type dummyKVStore struct{}

// --- dvid.Store interface ---

func (kv dummyKVStore) String() string {
	return "dummy kv store"
}

func (kv dummyKVStore) Close() {}

func (kv dummyKVStore) Equal(dvid.StoreConfig) bool {
	return true
}

// --- storage.OrderedKeyValueGetter interface ---

func (kv dummyKVStore) Get(ctx Context, k TKey) ([]byte, error) {
	return nil, nil
}

func (kv dummyKVStore) Exists(ctx Context, k TKey) (bool, error) {
	return false, nil
}

func (kv dummyKVStore) GetRange(ctx Context, kStart, kEnd TKey) ([]*TKeyValue, error) {
	return nil, nil
}

func (kv dummyKVStore) KeysInRange(ctx Context, kStart, kEnd TKey) ([]TKey, error) {
	return nil, nil
}

func (kv dummyKVStore) SendKeysInRange(ctx Context, kStart, kEnd TKey, ch KeyChan) error {
	return nil
}

func (kv dummyKVStore) ProcessRange(ctx Context, kStart, kEnd TKey, op *ChunkOp, f ChunkFunc) error {
	return nil
}

func (kv dummyKVStore) RawRangeQuery(kStart, kEnd Key, keysOnly bool, out chan *KeyValue, cancel <-chan struct{}) error {
	return nil
}

// --- storage.OrderedKeyValueSetter interface ---

func (kv dummyKVStore) Put(ctx Context, k TKey, v []byte) error {
	return nil
}

func (kv dummyKVStore) Delete(ctx Context, k TKey) error {
	return nil
}

func (kv dummyKVStore) RawPut(key Key, value []byte) error {
	return nil
}

func (kv dummyKVStore) RawDelete(key Key) error {
	return nil
}

func (kv dummyKVStore) PutRange(ctx Context, kvs []TKeyValue) error {
	return nil
}

func (kv dummyKVStore) DeleteRange(ctx Context, kStart, kEnd TKey) error {
	return nil
}

func (kv dummyKVStore) DeleteAll(ctx Context) error {
	return nil
}
