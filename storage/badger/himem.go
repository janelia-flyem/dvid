// +build !lowmem

package badger

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/janelia-flyem/dvid/dvid"
)

func getOptions(path string, config dvid.Config) (*badger.Options, error) {
	opts := badger.DefaultOptions(path)

	readOnly, found, err := config.GetBool("ReadOnly")
	if err != nil {
		return nil, err
	}
	if found {
		opts.ReadOnly = readOnly
	}

	valueSizeThresh, found, err := config.GetInt("ValueThreshold")
	if err != nil {
		return nil, err
	}
	if found {
		opts = opts.WithValueThreshold(valueSizeThresh)
	}

	vlogSize, found, err := config.GetInt("ValueLogFileSize")
	if err != nil {
		return nil, err
	}
	if found {
		opts = opts.WithValueLogFileSize(int64(vlogSize))
	}

	return &opts, nil
}
