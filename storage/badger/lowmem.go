// +build lowmem

package badger

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
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
	} else {
		opts = opts.WithValueThreshold(DefaultValueThreshold)
	}

	vlogSize, found, err := config.GetInt("ValueLogFileSize")
	if err != nil {
		return nil, err
	}
	if found {
		opts = opts.WithValueLogFileSize(int64(vlogSize))
	}

	// Low-memory options
	dvid.Infof("Using Badger with low memory options.\n")
	opts = opts.WithValueLogLoadingMode(options.FileIO)
	opts = opts.WithTableLoadingMode(options.FileIO)
	opts = opts.WithValueLogFileSize(16 << 20) // 16 MB value log file
	opts = opts.WithMaxCacheSize(8 << 20)
	opts = opts.WithMaxTableSize(8 << 20)
	opts = opts.WithKeepL0InMemory(false)
	return &opts, nil
}
