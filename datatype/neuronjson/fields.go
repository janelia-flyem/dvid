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

// Return a subset of fields from the NeuronJSON
func selectFields(data NeuronJSON, fieldMap map[string]struct{}, showUser, showTime bool) NeuronJSON {
	out := data.copy()
	if len(fieldMap) > 0 {
		for field := range data {
			if field == "bodyid" {
				continue
			}
			if _, found := fieldMap[field]; found {
				if !showUser {
					delete(out, field+"_user")
				}
				if !showTime {
					delete(out, field+"_time")
				}
			} else {
				delete(out, field)
				delete(out, field+"_time")
				delete(out, field+"_user")
			}
		}
	} else {
		if !showUser {
			for field := range data {
				if strings.HasSuffix(field, "_user") {
					delete(out, field)
				}
			}
		}
		if !showTime {
			for field := range data {
				if strings.HasSuffix(field, "_time") {
					delete(out, field)
				}
			}
		}
	}
	return out
}
