package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/protolog"
)

const jsonMsgTypeID uint16 = 1 // used for protolog

var (
	mutOrderID  uint64
	mutOrderMux sync.RWMutex

	jsonLogFiles    map[string]*logFile
	jsonLogFilesMux sync.Mutex
)

type logFile struct {
	sync.RWMutex
	f *os.File
}

func init() {
	jsonLogFiles = make(map[string]*logFile)
}

// MutationsConfig specifies handling of mutation logs, which are composed of
// an append-only logs of mutations.  There are several types of mutation logs.
// Httpstore is where all mutation HTTP requests are logged with large binary
// payloads being stored in the designated Blobstore.
// Jsonstore is where kafka-like JSON mutation records are logged.
type MutationsConfig struct {
	Httpstore string        // examples: "kafka:mytopic", "logstore:myStoreAlias"
	Jsonstore string        // path to directory of protolog files split by Data+Version UUID
	Blobstore storage.Alias // alias to a store
}

func logMutationPayload(data []byte) (ref string, err error) {
	var store dvid.Store
	if store, err = storage.GetStoreByAlias(tc.Mutations.Blobstore); err != nil {
		return
	}
	blobstore, ok := store.(storage.BlobStore)
	if !ok {
		err = fmt.Errorf("mutation blobstore %q is not a valid blob store", tc.Mutations.Blobstore)
		return
	}
	return blobstore.PutBlob(data)
}

func getJSONLogFile(versionID, dataID dvid.UUID) (lf *logFile, err error) {
	fname := path.Join(tc.Mutations.Jsonstore, string(dataID)+"-"+string(versionID)+".plog")
	jsonLogFilesMux.Lock()
	defer jsonLogFilesMux.Unlock()
	var found bool
	lf, found = jsonLogFiles[fname]
	if !found {
		var f *os.File
		f, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
		if err != nil {
			dvid.Errorf("Could not open new JSON mutation log: %v\n", err)
			return nil, err
		}
		dvid.Infof("Created mutation JSON log for data %s, version %s\n", dataID, versionID)
		lf = &logFile{f: f}
		jsonLogFiles[fname] = lf
	}
	return
}

// LogJSONMutation logs a JSON mutation record to the Jsonstore directory in the config.
func LogJSONMutation(versionID, dataID dvid.UUID, jsondata []byte) error {
	if tc.Mutations.Jsonstore == "" {
		dvid.Errorf("Cannot write mutation with non-existant Jsonstore config: %s\n", string(jsondata))
		return nil
	}
	lf, err := getJSONLogFile(versionID, dataID)
	if err != nil {
		return err
	}
	lf.Lock()
	w := protolog.NewTypedWriter(jsonMsgTypeID, lf.f)
	_, err = w.Write(jsondata)
	lf.Unlock()
	return err
}

// ReadJSONMutations streams a JSON of mutation records to the writer
func ReadJSONMutations(w io.Writer, versionID, dataID dvid.UUID) error {
	if tc.Mutations.Jsonstore == "" {
		return fmt.Errorf("No jsonstore configured in [mutations] section of TOML config")
	}
	lf, err := getJSONLogFile(versionID, dataID)
	if err != nil {
		return err
	}
	if lf == nil {
		return fmt.Errorf("Got nil mutation log for data %s, version %s", dataID, versionID)
	}
	lf.RLock()
	defer func() {
		lf.RUnlock()
		if _, err := lf.f.Seek(0, 2); err != nil {
			dvid.Criticalf("unable to seek to end of file for data %s, version %s: %v", dataID, versionID, err)
		}
	}()
	if _, err := lf.f.Seek(0, 0); err != nil {
		return fmt.Errorf("unable to seek to beginning of file for data %s, version %s: %v", dataID, versionID, err)
	}
	r := protolog.NewReader(lf.f)
	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}
	numMutations := 0
	for {
		typeID, jsondata, err := r.Next()
		if err == io.EOF {
			break
		}
		if numMutations != 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if typeID != jsonMsgTypeID {
			dvid.Criticalf("Unknown message type in mutation log: %s\n", string(jsondata))
		} else {
			if _, err := w.Write(jsondata); err != nil {
				return err
			}
		}
		numMutations++
	}
	_, err = w.Write([]byte("]"))
	dvid.Infof("Read %d JSON mutations for data %s, version %s\n", numMutations, dataID, versionID)
	return err
}

// LogHTTPMutation logs a HTTP mutation request to the mutation log specific in the config.
func LogHTTPMutation(versionID, dataID dvid.UUID, r *http.Request, data []byte) (err error) {
	if tc.Mutations.Blobstore == "" || tc.Mutations.Httpstore == "" {
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

	parts := strings.Split(tc.Mutations.Httpstore, ":")
	if len(parts) != 2 {
		return fmt.Errorf("bad logstore specification %q", tc.Mutations.Httpstore)
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
		return fmt.Errorf("unknown store %q in logstore specification %q", store, tc.Mutations.Httpstore)
	}
	return nil
}
