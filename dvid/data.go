/*
	This file contains data types and functions that support a number of layers in DVID.
*/

package dvid

import (
	"fmt"
)

// Config is a map of keyword to arbitrary data to specify configurations via keyword.
type Config map[string]interface{}

// IsVersioned returns true if we want this data versioned.
func (c Config) IsVersioned() (versioned bool, err error) {
	param, ok := c["versioned"]
	if !ok {
		err = fmt.Errorf("No 'versioned' key in DVID configuration")
		return
	}
	versioned, ok = param.(bool)
	if !ok {
		err = fmt.Errorf("Illegal 'versioned' value in DVID configuration")
	}
	return
}
