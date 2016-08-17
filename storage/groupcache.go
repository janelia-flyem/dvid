package storage

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/golang/groupcache"
	"github.com/janelia-flyem/dvid/dvid"
)

// GroupcacheCtx packages both a storage.Context and a KeyValueDB for GETs.
type GroupcacheCtx struct {
	Context
	KeyValueDB
}

type groupcacheT struct {
	cache     *groupcache.Group
	supported map[dvid.DataSpecifier]struct{} // set if the given data instance has groupcache support.
}

func setupGroupcache(config GroupcacheConfig) error {
	if config.GB == 0 {
		return nil
	}
	var cacheBytes int64
	cacheBytes = int64(config.GB) << 30

	peers := groupcache.NewHTTPPool(config.Host)
	if peers != nil {
		dvid.Infof("Initializing groupcache with %d GB at %s...\n", config.GB, config.Host)
		manager.gcache.cache = groupcache.NewGroup("immutable", cacheBytes, groupcache.GetterFunc(
			func(c groupcache.Context, key string, dest groupcache.Sink) error {
				// Use KeyValueDB defined as context.
				gctx, ok := c.(GroupcacheCtx)
				if !ok {
					return fmt.Errorf("bad groupcache context: expected GroupcacheCtx, got %v", c)
				}

				// First four bytes of key is instance ID to isolate groupcache collisions.
				tk := TKey(key[4:])
				data, err := gctx.KeyValueDB.Get(gctx.Context, tk)
				if err != nil {
					return err
				}
				return dest.SetBytes(data)
			}))
		manager.gcache.supported = make(map[dvid.DataSpecifier]struct{})
		for _, dataspec := range config.Instances {
			name := strings.Trim(dataspec, "\"")
			parts := strings.Split(name, ":")
			switch len(parts) {
			case 2:
				dataid := dvid.GetDataSpecifier(dvid.InstanceName(parts[0]), dvid.UUID(parts[1]))
				manager.gcache.supported[dataid] = struct{}{}
			default:
				dvid.Errorf("bad data instance specification %q given for groupcache support in config file\n", dataspec)
			}
		}
	}
	return nil
}

// returns a store that tries groupcache before resorting to passed Store.
func wrapGroupcache(store dvid.Store, cache *groupcache.Group) (dvid.Store, error) {
	kvstore, ok := store.(KeyValueDB)
	if !ok {
		return store, fmt.Errorf("store %s doesn't implement KeyValueDB", store)
	}
	return groupcacheStore{KeyValueDB: kvstore, cache: cache}, nil
}

type instanceProvider interface {
	InstanceID() dvid.InstanceID
}

type groupcacheStore struct {
	KeyValueDB
	cache *groupcache.Group
}

// only need to override the Get function of the wrapped KeyValueDB.
func (g groupcacheStore) Get(ctx Context, k TKey) ([]byte, error) {
	gctx := GroupcacheCtx{
		Context:    ctx,
		KeyValueDB: g.KeyValueDB,
	}
	// we only provide this server for data contexts that have InstanceID().
	ip, ok := ctx.(instanceProvider)
	if !ok {
		dvid.Criticalf("groupcache Get passed a non-data context %v, falling back on normal kv store Get\n", ctx)
		return g.KeyValueDB.Get(ctx, k)
	}

	// the groupcache key has an instance identifier in first 4 bytes.
	instanceBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(instanceBytes, uint32(ip.InstanceID()))
	gkey := string(instanceBytes) + string(k)

	// Try to get data from groupcache, which if fails, will call the original KeyValueDB in passed Context.
	var data []byte
	err := g.cache.Get(groupcache.Context(gctx), gkey, groupcache.AllocatingByteSliceSink(&data))
	return data, err
}
