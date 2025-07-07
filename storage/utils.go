package storage

import (
	"io"
	"os"
)

// DataFromFile returns data from a file.
func DataFromFile(filename string) ([]byte, error) {
	var data []byte
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err = io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	FileBytesRead <- len(data)
	return data, nil
}
