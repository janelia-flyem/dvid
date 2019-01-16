package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// MutationsConfig specifies handling of mutation logs, which are composed of
// an append-only logs of mutations for each version and one log for repository
// changes.  A blobstore must also be specified to hold large mutation data that
// is
type MutationsConfig struct {
	Logstore  string        // example: "kafka:mytopic".  TODO -- support non-kafka log store.
	Blobstore storage.Alias // alias to a store
}

func logMutationPayload(cfg MutationsConfig, data []byte) (ref string, err error) {
	var store dvid.Store
	if store, err = storage.GetStoreByAlias(cfg.Blobstore); err != nil {
		return
	}
	blobstore, ok := store.(storage.BlobStore)
	if !ok {
		err = fmt.Errorf("mutation blobstore %q is not a valid blob store", cfg.Blobstore)
		return
	}
	return blobstore.PutBlob(data)
}

// LogMutation logs a HTTP mutation request to the mutation log specific in the config.
func LogMutation(cfg MutationsConfig, uuid dvid.UUID, r *http.Request, data []byte) (err error) {
	var postRef string
	if postRef, err = logMutationPayload(cfg, data); err != nil {
		return fmt.Errorf("unable to store mutation payload (%s): %v", r.RequestURI, err)
	}
	mutation := map[string]interface{}{
		"Time":        time.Now().Unix(),
		"Method":      r.Method,
		"URI":         r.RequestURI,
		"BytesIn":     r.ContentLength,
		"RemoteAddr":  r.RemoteAddr,
		"ContentType": r.Header.Get("Content-Type"),
		"DataRef":     postRef,
	}
	jsonmsg, err := json.Marshal(mutation)
	if err != nil {
		return fmt.Errorf("error marshaling JSON for mutation (%s): %v", r.RequestURI, err)
	}

	parts := strings.Split(cfg.Logstore, ":")
	if len(parts) != 2 {
		return fmt.Errorf("bad logstore specification %q", cfg.Logstore)
	}
	store := parts[0]
	spec := parts[1]
	switch store {
	case "kafka":
		topic := spec + "-" + string(uuid)
		if err = storage.KafkaProduceMsg(jsonmsg, topic); err != nil {
			return fmt.Errorf("error on sending mutation (%s) to kafka: %v", r.RequestURI, err)
		}
	default:
		return fmt.Errorf("unknown store %q in logstore specification %q", store, cfg.Logstore)
	}
	return nil
}
