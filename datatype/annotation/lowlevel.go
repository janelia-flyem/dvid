package annotation

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// scan does a range query on all blocks of annotations and compiles stats.
func (d *Data) scan(ctx *datastore.VersionedCtx, w http.ResponseWriter, byCoord, keysOnly bool) error {
	timedLog := dvid.NewTimeLog()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	var minTKey, maxTKey storage.TKey
	if byCoord {
		minTKey, maxTKey = BlockTKeyRange()
	} else {
		minTKey = storage.MinTKey(keyBlock)
		maxTKey = storage.MaxTKey(keyBlock)
	}

	var numKV, numEmpty uint64
	if keysOnly {
		keyChan := make(storage.KeyChan)
		go func() {
			store.SendKeysInRange(ctx, minTKey, maxTKey, keyChan)
			close(keyChan)
		}()
		for key := range keyChan {
			if key != nil {
				numKV++
			}
		}
	} else {
		err = store.ProcessRange(ctx, minTKey, maxTKey, nil, func(chunk *storage.Chunk) error {
			numKV++
			if len(chunk.V) == 0 {
				numEmpty++
			}
			return nil
		})
	}
	if err != nil {
		return err
	}
	jsonBytes, err := json.Marshal(struct {
		NumKV    uint64 `json:"num kv pairs"`
		NumEmpty uint64 `json:"num empty blocks"`
	}{numKV, numEmpty})
	if err != nil {
		return err
	}
	fmt.Fprint(w, string(jsonBytes))
	timedLog.Infof("Scanned %d blocks (%d empty) in a /scan request (byCoord = %t, keysOnly = %t)",
		numKV, numEmpty, byCoord, keysOnly)
	return nil
}
