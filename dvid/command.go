/*
	This file holds types and functions supporting command-related activity in DVID.
	These Command types bundle operation specification and data payloads for use in
	RPC and HTTP APIs.
*/

package dvid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is a map of keyword to arbitrary data to specify configurations via keyword.
// Keywords are case-insensitive.
type Config struct {
	values map[string]interface{} // Make private so we can control case-insensitivity
}

func NewConfig() Config {
	c := Config{make(map[string]interface{})}
	c.values["versioned"] = "false"
	return c
}

func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.values)
}

func (c Config) Read(p []byte) (n int, err error) {
	b, err := c.MarshalJSON()
	if err != nil {
		return 0, err
	}
	buf := bytes.NewBuffer(b)
	return buf.Read(p)
}

// Sets a configuration using valid JSON.  Since Config is case-insensitive, JSON
// object names are converted to lower case.
func (c *Config) SetByJSON(jsonData io.Reader) error {
	if c.values == nil {
		c.values = make(map[string]interface{})
	}
	decoder := json.NewDecoder(jsonData)
	if err := decoder.Decode(&(c.values)); err != nil && err != io.EOF {
		return fmt.Errorf("Malformed JSON request in body: %s", err.Error())
	}
	// Convert all keys to lower case.
	for key, _ := range c.values {
		lowerkey := strings.ToLower(key)
		if key != lowerkey {
			c.values[lowerkey] = c.values[key]
			delete(c.values, key)
		}
	}
	return nil
}

func (c *Config) Set(key string, value interface{}) {
	if c.values == nil {
		c.values = make(map[string]interface{})
	}
	lowerkey := strings.ToLower(key)
	c.values[lowerkey] = value
}

func (c *Config) Get(key string) (interface{}, bool) {
	lowerkey := strings.ToLower(key)
	value, ok := c.values[lowerkey]
	return value, ok
}

func (c Config) GetAll() map[string]interface{} {
	return c.values
}

// GetString returns a string value of the given key.  If setting of key is not
// a string, returns an error.  Returns zero value string ("") if not found.
func (c Config) GetString(key string) (s string, found bool, err error) {
	if c.values == nil {
		found = false
		return
	}
	var param interface{}
	lowerkey := strings.ToLower(key)
	if param, found = c.values[lowerkey]; found {
		var ok bool
		s, ok = param.(string)
		if !ok {
			err = fmt.Errorf("Setting for '%s' was not a string: %s", key, param)
		}
		return
	}
	return
}

// GetInt returns an int value of the given key.  If setting of key is not
// parseable as an int, returns an error.
func (c Config) GetInt(key string) (i int, found bool, err error) {
	var s string
	s, found, err = c.GetString(key)
	if err != nil || !found {
		return
	}
	i, err = strconv.Atoi(s)
	return
}

// GetBool returns a bool value of the given key.  If setting of key is not
// parseable as a bool ("false", "true", "0", or "1"), returns an error.  If the key
// is not found, it will also return a false bool (the Go zero value for bool).
func (c Config) GetBool(key string) (value, found bool, err error) {
	var s string
	s, found, err = c.GetString(key)
	if err != nil || !found {
		return
	}
	boolStr := strings.ToLower(s)
	switch boolStr {
	case "false", "0":
		value = false
	case "true", "1":
		value = true
	default:
		err = fmt.Errorf("Cannot parse '%s' as a boolean.  Use 'true', 'false', '0', or '1'.")
	}
	return
}

// Remove removes the key/value pairs with the given keys.
func (c *Config) Remove(keys ...string) {
	toDelete := []string{}
	for _, key := range keys {
		if _, found := c.values[key]; found {
			toDelete = append(toDelete, key)
		}
	}
	for _, key := range toDelete {
		delete(c.values, key)
	}
}

// Response provides a few string fields to pass information back from
// a remote operation.
type Response struct {
	ContentType string
	Text        string
	Status      string
}

// Command supports command-line interaction with DVID.
// The first item in the string slice is the command, which may be "help"
// or the name of DVID data name ("uint8").  If the first item is the name
// of a data type, the second item will have a type-specific command like "get".
// The other arguments are command arguments or optional settings of the form
// "<key>=<value>".
type Command []string

// String returns a space-separated command line
func (cmd Command) String() string {
	return strings.Join([]string(cmd), " ")
}

// Returns the argument at the given position using zero indexing.  Settings are not
// considered arguments.  If no argument is at the given position, this returns
// the empty string.
func (cmd Command) Argument(pos int) string {
	argPos := 0
	for _, arg := range cmd {
		elems := strings.Split(arg, "=")
		if len(elems) != 2 {
			// This is an argument
			if argPos == pos {
				return arg
			}
			argPos++
		}
	}
	return ""
}

// Name returns the first argument of the command (in lower case) which is assumed to
// be the name of the command.
func (cmd Command) Name() string {
	if len(cmd) == 0 {
		return ""
	}
	return strings.ToLower(cmd[0])
}

// TypeCommand returns the name of a type-specific command (in lower case).
func (cmd Command) TypeCommand() string {
	if len(cmd) < 4 {
		return ""
	}
	return strings.ToLower(cmd[3])
}

// Setting scans a command for any "key=value" argument and returns
// the value of the passed 'key'.  Key is case sensitive for this function.
func (cmd Command) Setting(key string) (value string, found bool) {
	if len(cmd) > 1 {
		for _, arg := range cmd[1:] {
			elems := strings.Split(arg, "=")
			if len(elems) == 2 && elems[0] == key {
				value = elems[1]
				found = true
				return
			}
		}
	}
	return
}

// Settings scans a command for any "key=value" argument and returns
// a Config, which is a map of key-value data.  All keys are converted
// to lower case for case-insensitive matching.
func (cmd Command) Settings() Config {
	config := NewConfig()
	if len(cmd) > 1 {
		for _, arg := range cmd[1:] {
			elems := strings.Split(arg, "=")
			if len(elems) == 2 {
				lowerkey := strings.ToLower(elems[0])
				config.values[lowerkey] = elems[1]
			}
		}
	}
	return config
}

// FilenameArgs is similar to CommandArgs except it can take filename glob patterns
// at the end of the string, and will find matches and return those.
func (cmd Command) FilenameArgs(startPos int, targets ...*string) (filenames []string, err error) {
	filenames = []string{}
	for _, target := range targets {
		*target = ""
	}
	if len(cmd) > startPos {
		numTargets := len(targets)
		curTarget := 0
		for _, arg := range cmd[startPos:] {
			elems := strings.Split(arg, "=")
			if len(elems) != 2 {
				if curTarget >= numTargets {
					matches, err := filepath.Glob(arg)
					if err != nil {
						return nil, err
					} else {
						filenames = append(filenames, matches...)
					}
				} else {
					*(targets[curTarget]) = arg
				}
				curTarget++
			}
		}
	}
	return
}

// CommandArgs sets a variadic argument set of string pointers to data
// command arguments, ignoring setting arguments of the form "<key>=<value>".
// If there aren't enough arguments to set a target, the target is set to the
// empty string.  It returns an 'overflow' slice that has all arguments
// beyond those needed for targets.
//
// Example: Given the command string "add param1 param2 42 data/*.png"
//
//   var s1, s2, s3, s4 string
//   filenames := CommandArgs(0, &s1, &s2, &s3, &s4)
//   fmt.Println(filenames)
//   fmt.Println(s1)
//   fmt.Println(s2, s3)
//   fmt.Println(s4)
//
//   Would print out:
//      ["data/foo-1.png", "data/foo-2.png", "data/foo-3.png"]
//      add
//      param1 param2
//      42
func (cmd Command) CommandArgs(startPos int, targets ...*string) []string {
	return getArgs(cmd, startPos, targets...)
}

func getArgs(cmd Command, startPos int, targets ...*string) []string {
	overflow := []string{}
	for _, target := range targets {
		*target = ""
	}
	if len(cmd) > startPos {
		numTargets := len(targets)
		curTarget := 0
		for _, arg := range cmd[startPos:] {
			elems := strings.Split(arg, "=")
			if len(elems) != 2 {
				if curTarget >= numTargets {
					overflow = append(overflow, arg)
				} else {
					*(targets[curTarget]) = arg
				}
				curTarget++
			}
		}
	}
	return overflow
}
