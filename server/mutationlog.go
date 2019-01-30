package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	mutCfg MutationsConfig

	mutOrderID  uint64
	mutOrderMux sync.RWMutex
)

// MutationsConfig specifies handling of mutation logs, which are composed of
// an append-only logs of mutations for each version and one log for repository
// changes.  A blobstore must also be specified to hold large mutation data that
// is
type MutationsConfig struct {
	Logstore  string        // examples: "kafka:mytopic", "logstore:myStoreAlias"
	Blobstore storage.Alias // alias to a store
}

func logMutationPayload(data []byte) (ref string, err error) {
	var store dvid.Store
	if store, err = storage.GetStoreByAlias(mutCfg.Blobstore); err != nil {
		return
	}
	blobstore, ok := store.(storage.BlobStore)
	if !ok {
		err = fmt.Errorf("mutation blobstore %q is not a valid blob store", mutCfg.Blobstore)
		return
	}
	return blobstore.PutBlob(data)
}

// LogMutation logs a HTTP mutation request to the mutation log specific in the config.
func LogMutation(versionID, dataID dvid.UUID, r *http.Request, data []byte) (err error) {
	if mutCfg.Blobstore == "" || mutCfg.Logstore == "" {
		return nil
	}
	mutation := map[string]interface{}{
		"TimeUnix":    time.Now().Unix(),
		"Method":      r.Method,
		"URI":         r.RequestURI,
		"RemoteAddr":  r.RemoteAddr,
		"ContentType": r.Header.Get("Content-Type"),
	}
	if dataID != "" {
		mutation["DataUUID"] = dataID
	}
	if len(data) != 0 {
		var postRef string
		if postRef, err = logMutationPayload(data); err != nil {
			return fmt.Errorf("unable to store mutation payload (%s): %v", r.RequestURI, err)
		}
		mutation["DataBytes"] = len(data)
		mutation["DataRef"] = postRef
	}
	mutOrderMux.Lock()
	mutOrderID++
	mutation["MutationOrderID"] = mutOrderID
	mutOrderMux.Unlock()

	jsonmsg, err := json.Marshal(mutation)
	if err != nil {
		return fmt.Errorf("error marshaling JSON for mutation (%s): %v", r.RequestURI, err)
	}

	parts := strings.Split(mutCfg.Logstore, ":")
	if len(parts) != 2 {
		return fmt.Errorf("bad logstore specification %q", mutCfg.Logstore)
	}
	store := parts[0]
	spec := parts[1]
	switch store {
	case "kafka":
		topic := spec + "-" + string(versionID)
		if err = storage.KafkaProduceMsg(jsonmsg, topic); err != nil {
			return fmt.Errorf("error on sending mutation (%s) to kafka: %v", r.RequestURI, err)
		}
	case "logstore":
		store, err := storage.GetStoreByAlias(storage.Alias(spec))
		if err != nil {
			return fmt.Errorf("bad mutation logstore specification %q", spec)
		}
		logable, ok := store.(storage.LogWritable)
		if !ok {
			return fmt.Errorf("mutation logstore %q was not a valid write log", spec)
		}
		log := logable.GetWriteLog()
		if log == nil {
			return fmt.Errorf("unable to get write log from store %s", store)
		}
		return log.TopicAppend(string(versionID), storage.LogMessage{Data: jsonmsg})
	default:
		return fmt.Errorf("unknown store %q in logstore specification %q", store, mutCfg.Logstore)
	}
	return nil
}
