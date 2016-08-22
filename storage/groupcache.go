package storage

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang/groupcache"
	"github.com/janelia-flyem/dvid/dvid"
)

// GroupcacheStats returns all the stats of interest for this groupcache service.
type GroupcacheStats struct {
	Gets           int64 // any GET request, including from peers
	CacheHits      int64 // either cache was good
	PeerLoads      int64 // either remote load or remote cache hit (not an error)
	PeerErrors     int64
	Loads          int64 // gets - cacheHits
	LoadsDeduped   int64 // after singleflight
	LocalLoads     int64 // total good local loads
	LocalLoadErrs  int64 // total bad local loads
	ServerRequests int64 // gets that came over network from peers

	// Cache stats for items owned by the host.
	MainCache groupcache.CacheStats

	// Cache stats for replicated items from a peer.
	HotCache groupcache.CacheStats
}

// GetGroupcacheStats returns all kinds of stats about the groupcache system.
func GetGroupcacheStats() (stats GroupcacheStats, err error) {
	if !manager.setup {
		err = fmt.Errorf("Storage manager not initialized before requesting groupcache stats")
		return
	}
	if manager.gcache.cache == nil {
		return
	}
	s := manager.gcache.cache.Stats
	stats.Gets = int64(s.Gets)
	stats.CacheHits = int64(s.CacheHits)
	stats.PeerLoads = int64(s.PeerLoads)
	stats.PeerErrors = int64(s.PeerErrors)
	stats.Loads = int64(s.Loads)
	stats.LoadsDeduped = int64(s.LoadsDeduped)
	stats.LocalLoads = int64(s.LocalLoads)
	stats.LocalLoadErrs = int64(s.LocalLoadErrs)
	stats.ServerRequests = int64(s.ServerRequests)

	stats.MainCache = manager.gcache.cache.CacheStats(groupcache.MainCache)
	stats.HotCache = manager.gcache.cache.CacheStats(groupcache.HotCache)
	return
}

// GroupcacheConfig handles settings for the groupcache library.
type GroupcacheConfig struct {
	GB        int
	Host      string   // The http address of this DVID server's groupcache port.
	Peers     []string // The http addresses of the peer groupcache group.
	Instances []string // Data instances that use groupcache in form "<name>:<uuid>""
}

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

	pool := groupcache.NewHTTPPool(config.Host)
	if pool != nil {
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

		// If we have additional peers, add them and start a listener via the HTTP port.
		if len(config.Peers) > 0 {
			peers := []string{config.Host}
			peers = append(peers, config.Peers...)
			pool.Set(peers...)
			dvid.Infof("Groupcache configuration has %d peers in addition to local host.\n", len(config.Peers))
			dvid.Infof("Starting groupcache HTTP server on %s\n", config.Host)
			http.ListenAndServe(config.Host, http.HandlerFunc(pool.ServeHTTP))
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
