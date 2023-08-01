package neuronjson

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	reflect "reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type QueryJSON map[string]interface{}
type ListQueryJSON []QueryJSON

type FieldExistence bool // field is present or not

// UnmarshalJSON parses JSON with numbers preferentially converted to uint64
// or int64 if negative, and strings with "re/" as prefix are compiled as
// a regular expression.
func (qj *QueryJSON) UnmarshalJSON(jsonText []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &raw); err != nil {
		return err
	}
	*qj = make(QueryJSON, len(raw))

	dvid.Infof("query unmarshal on: %s\n", string(jsonText))

	for key, val := range raw {
		s := string(val)
		u, err := strconv.ParseUint(s, 10, 64)
		if err == nil {
			(*qj)[key] = u
			continue
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			(*qj)[key] = i
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			(*qj)[key] = f
			continue
		}
		var int64list []int64
		if err = json.Unmarshal(val, &int64list); err == nil {
			(*qj)[key] = int64list
			continue
		}
		if len(s) > 4 && strings.HasPrefix(s, `"re/`) {
			re, err := regexp.Compile(s[4 : len(s)-1])
			if err == nil {
				(*qj)[key] = re
				continue
			}
		}
		if len(s) == 10 && strings.HasPrefix(s, `"exists/`) {
			if s[8] == '0' {
				(*qj)[key] = FieldExistence(false)
			} else {
				(*qj)[key] = FieldExistence(true)
			}
			continue
		}
		var strlist []string
		if err = json.Unmarshal(val, &strlist); err == nil {
			hasRegex := false
			iflist := make([]interface{}, len(strlist))
			for i, s := range strlist {
				if len(s) > 3 && strings.HasPrefix(s, "re/") {
					hasRegex = true
					if re, err := regexp.Compile(s[3:]); err == nil {
						iflist[i] = re
					}
				}
				if iflist[i] == nil {
					iflist[i] = s
				}
			}
			if hasRegex {
				(*qj)[key] = iflist
			} else {
				(*qj)[key] = strlist
			}
			continue
		}
		var listVal interface{}
		if err = json.Unmarshal(val, &listVal); err == nil {
			(*qj)[key] = listVal
			continue
		}
		return fmt.Errorf("unable to parse JSON value %q: %v", s, err)
	}
	return nil
}

func checkIntMatch(query int64, field []int64) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if fieldValue == query {
			return true
		}
	}
	return false
}

func checkFloatMatch(query float64, field []float64) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if fieldValue == query {
			return true
		}
	}
	return false
}

func checkStrMatch(query string, field []string) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if fieldValue == query {
			return true
		}
	}
	return false
}

func checkRegexMatch(query *regexp.Regexp, field []string) bool {
	if len(field) == 0 {
		return false
	}
	for _, fieldValue := range field {
		if query.Match([]byte(fieldValue)) {
			return true
		}
	}
	return false
}

// given a query on this field composed of one or a list of values of unknown type,
// see if any of the field's values match a query value regardless of slightly
// different integer typing.
func checkField(queryValue, fieldValue interface{}) bool {
	// if field value is integer of some kind, convert to []int64 assuming MSB not
	// needed for our data.
	// if field value is string, make it []string.
	// Field can be (single or list of) number, string, other.
	var fieldNumList []int64
	var fieldFloatList []float64
	var fieldStrList []string
	switch v := fieldValue.(type) {
	case int64:
		fieldNumList = []int64{v}
	case []int64:
		fieldNumList = v
	case uint64:
		fieldNumList = []int64{int64(v)}
	case []uint64:
		fieldNumList = make([]int64, len(v))
		for i, val := range v {
			fieldNumList[i] = int64(val)
		}
	case string:
		fieldStrList = []string{v}
	case []string:
		fieldStrList = v
	case float64:
		if v == float64(int(v)) {
			fieldNumList = []int64{int64(v)}
		} else {
			fieldFloatList = []float64{v}
		}

	default:
		dvid.Errorf("Unknown field value of type %s: %v\n", reflect.TypeOf(v), v)
		return false
	}
	if len(fieldNumList) == 0 && len(fieldStrList) == 0 && len(fieldFloatList) == 0 {
		return false
	}

	// convert query value to list of types as above for field value.
	switch v := queryValue.(type) {
	case int64:
		if checkIntMatch(v, fieldNumList) {
			return true
		}
	case []int64:
		for _, i := range v {
			if checkIntMatch(i, fieldNumList) {
				return true
			}
		}
	case uint64:
		if checkIntMatch(int64(v), fieldNumList) {
			return true
		}
	case []uint64:
		for _, val := range v {
			if checkIntMatch(int64(val), fieldNumList) {
				return true
			}
		}
	case string:
		if checkStrMatch(v, fieldStrList) {
			return true
		}
	case []string:
		for _, s := range v {
			if checkStrMatch(s, fieldStrList) {
				return true
			}
		}
	case *regexp.Regexp:
		if checkRegexMatch(v, fieldStrList) {
			return true
		}
	case []interface{}:
		elem := v[0]
		switch e := elem.(type) {
		case int:
			for _, val := range v {
				if checkIntMatch(int64(val.(int)), fieldNumList) {
					return true
				}
			}
		case float64:
			for _, val := range v {
				if checkFloatMatch(val.(float64), fieldFloatList) {
					return true
				}
			}
		case string, *regexp.Regexp:
			for _, val := range v {
				switch query := val.(type) {
				case string:
					if checkStrMatch(query, fieldStrList) {
						return true
					}
				case *regexp.Regexp:
					if checkRegexMatch(query, fieldStrList) {
						return true
					}
				}
			}
		default:
			var t = reflect.TypeOf(e)
			dvid.Errorf("neuronjson query value %v has elements of illegal type %v\n", v, t)
		}
	default:
		var t = reflect.TypeOf(v)
		dvid.Errorf("neuronjson query value %v has illegal type %v\n", v, t)
	}
	return false
}

func fieldMatch(queryValue, fieldValue interface{}) bool {
	if queryValue == nil {
		return false
	}
	if fieldValue == nil {
		return false
	}
	return checkField(queryValue, fieldValue)
}

// --- Data Query support ---

// returns true if at least one query on the list matches the value.
func queryMatch(queryList ListQueryJSON, value map[string]interface{}) (matches bool, err error) {
	if len(queryList) == 0 {
		matches = false
		return
	}
	for _, query := range queryList {
		and_match := true
		for queryKey, queryValue := range query { // all query keys must be present and match
			// field existence check
			recordValue, found := value[queryKey]
			switch v := queryValue.(type) {
			case FieldExistence:
				found = found && recordValue != nil
				dvid.Infof("checking existence of field %s: %v where field %t (%v)", queryKey, v, found, recordValue)
				if (bool(v) && !found) || (!bool(v) && found) {
					and_match = false
				}
			default:
				// if field exists, check if it matches query
				if !found || !fieldMatch(queryValue, recordValue) {
					and_match = false
				}
			}
			if !and_match {
				break
			}
		}
		if and_match {
			return true, nil
		}
	}
	return false, nil
}

func (d *Data) queryInMemory(mdb *memdb, w http.ResponseWriter, queryL ListQueryJSON, fieldMap map[string]struct{}, showFields Fields) (err error) {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()

	dvid.Infof("in-memory query using mdb with queryL: %v\n", queryL)
	showUser, showTime := showFields.Bools()
	numMatches := 0
	var jsonBytes []byte
	for _, bodyid := range mdb.ids {
		value := mdb.data[bodyid]
		var matches bool
		if matches, err = queryMatch(queryL, value); err != nil {
			return
		} else if matches {
			out := selectFields(value, fieldMap, showUser, showTime)
			if jsonBytes, err = json.Marshal(out); err != nil {
				break
			}
			if numMatches > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprint(w, string(jsonBytes))
			numMatches++
		}
	}
	return
}

func (d *Data) queryBackingStore(ctx storage.VersionedCtx, w http.ResponseWriter,
	queryL ListQueryJSON, fieldMap map[string]struct{}, showFields Fields) (err error) {

	dvid.Infof("store query using mdb with queryL: %v\n", queryL)
	numMatches := 0
	process_func := func(key string, value map[string]interface{}) {
		if matches, err := queryMatch(queryL, value); err != nil {
			dvid.Errorf("error in matching process: %v\n", err) // TODO: alter d.processRange to allow return of err
			return
		} else if !matches {
			return
		}
		out := removeReservedFields(value, showFields)
		jsonBytes, err := json.Marshal(out)
		if err != nil {
			dvid.Errorf("error in JSON encoding: %v\n", err)
			return
		}
		if numMatches > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprint(w, string(jsonBytes))
		numMatches++
	}
	return d.processStoreRange(ctx, process_func)
}

// Query reads POSTed data and returns JSON.
func (d *Data) Query(ctx *datastore.VersionedCtx, w http.ResponseWriter, uuid dvid.UUID, onlyid bool, fieldMap map[string]struct{}, showFields Fields, in io.ReadCloser) (err error) {
	var queryBytes []byte
	if queryBytes, err = io.ReadAll(in); err != nil {
		return
	}
	// Try to parse as list of queries and if fails, try as object and make it a one-item list.
	var queryL ListQueryJSON
	if err = json.Unmarshal(queryBytes, &queryL); err != nil {
		var queryObj QueryJSON
		if err = queryObj.UnmarshalJSON(queryBytes); err != nil {
			err = fmt.Errorf("unable to parse JSON query: %s", string(queryBytes))
			return
		}
		queryL = ListQueryJSON{queryObj}
	}

	// Perform the query
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, "[")
	mdb, found := d.getMemDBbyVersion(ctx.VersionID())
	if found {
		if err = d.queryInMemory(mdb, w, queryL, fieldMap, showFields); err != nil {
			return
		}
	} else {
		if err = d.queryBackingStore(ctx, w, queryL, fieldMap, showFields); err != nil {
			return
		}
	}
	fmt.Fprint(w, "]")
	return
}
