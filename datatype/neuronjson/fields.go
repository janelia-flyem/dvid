package neuronjson

import (
	"net/http"
	"strings"
)

type Fields uint8

const (
	ShowBasic Fields = iota
	ShowUsers
	ShowTime
	ShowAll
)

func (f Fields) Bools() (showUser, showTime bool) {
	switch f {
	case ShowBasic:
		return false, false
	case ShowUsers:
		return true, false
	case ShowTime:
		return false, true
	case ShowAll:
		return true, true
	default:
		return false, false
	}
}

// parse query string "show" parameter into a Fields value.
func showFields(r *http.Request) Fields {
	switch r.URL.Query().Get("show") {
	case "user":
		return ShowUsers
	case "time":
		return ShowTime
	case "all":
		return ShowAll
	default:
		return ShowBasic
	}
}

// parse query string "fields" parameter into a list of field names
func fieldList(r *http.Request) (fields []string) {
	fieldsString := r.URL.Query().Get("fields")
	if fieldsString != "" {
		fields = strings.Split(fieldsString, ",")
	}
	return
}

// get a map of fields (none if all) from query string "fields"
func fieldMap(r *http.Request) (fields map[string]struct{}) {
	fields = make(map[string]struct{})
	for _, field := range fieldList(r) {
		fields[field] = struct{}{}
	}
	return
}

// Remove any fields that have underscore prefix.
func removeReservedFields(data NeuronJSON, showFields Fields) NeuronJSON {
	var showUser, showTime bool
	switch showFields {
	case ShowBasic:
		// don't show either user or time -- default values
	case ShowUsers:
		showUser = true
	case ShowTime:
		showTime = true
	case ShowAll:
		return data
	}
	out := data.copy()
	for field := range data {
		if (!showUser && strings.HasSuffix(field, "_user")) || (!showTime && strings.HasSuffix(field, "_time")) {
			delete(out, field)
		}
	}
	return out
}

// Return a subset of fields from the NeuronJSON where fieldMap is a map of field names to include.
func selectFields(data NeuronJSON, fieldMap map[string]struct{}, showUser, showTime bool) NeuronJSON {
	out := NeuronJSON{}

	// Always include bodyid if it exists.
	if v, ok := data["bodyid"]; ok {
		out["bodyid"] = v
	}

	if len(fieldMap) > 0 {
		for key := range fieldMap {
			// Skip "bodyid", already copied.
			if key == "bodyid" {
				continue
			}
			// Add the primary field if present.
			if v, ok := data[key]; ok {
				out[key] = v
			}
			// Optionally include accompanying _user and _time fields.
			if showUser {
				if v, ok := data[key+"_user"]; ok {
					out[key+"_user"] = v
				}
			}
			if showTime {
				if v, ok := data[key+"_time"]; ok {
					out[key+"_time"] = v
				}
			}
		}
	} else {
		// No fieldMap provided, copy all keys except suppressed ones.
		for key, v := range data {
			// If we're not showing user info, skip fields ending in _user.
			if !showUser && strings.HasSuffix(key, "_user") {
				continue
			}
			// If we're not showing time info, skip fields ending in _time.
			if !showTime && strings.HasSuffix(key, "_time") {
				continue
			}
			out[key] = v
		}
	}

	return out
}
