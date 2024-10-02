package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/protolog"
)

const helpMessage = `

Create a filtered copy of json mutation plog files.

Usage: filtered-mutations [options] <data-uuid> <input-dir> <output-dir>

Example: filtered-mutations -filter bad-muts.json 74bc44bad8834aeaa8439cd5f8de830c /tmp/mutlog /tmp/filtered

      -filter    (string)    JSON file containing list of objects with "MutationID" and "UUID" fields
  -h, -help        (flag)    Show help message

`

var (
	showHelp   = flag.Bool("help", false, "Show help message")
	filterJSON = flag.String("filter", "", "JSON file containing list of objects with 'MutationID' and 'UUID' fields")

	dataUUID, inPath, outPath string
)

var usage = func() {
	fmt.Print(helpMessage)
}

type FilterEntry struct {
	MutationID uint64 `json:"MutationID"`
	UUID       string `json:"UUID"`
}

type IDSet map[uint64]struct{}

func readFilterJSON(filterJSON string) (map[string]IDSet, error) {
	// Open and read the JSON file
	file, err := os.ReadFile(filterJSON)
	if err != nil {
		return nil, fmt.Errorf("error reading JSON file: %w", err)
	}

	// Unmarshal JSON data into a slice of FilterEntry
	var entries []FilterEntry
	err = json.Unmarshal(file, &entries)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	// Create and populate the map
	filterMap := make(map[string]IDSet)
	for _, entry := range entries {
		if _, exists := filterMap[entry.UUID]; !exists {
			filterMap[entry.UUID] = make(IDSet)
		}
		filterMap[entry.UUID][entry.MutationID] = struct{}{}
	}

	return filterMap, nil
}

func processPlogFiles(dataUUID string, filterMap map[string]IDSet) error {
	files, err := os.ReadDir(inPath)
	if err != nil {
		return fmt.Errorf("error reading directory: %w", err)
	}

	for _, file := range files {
		fileName := file.Name()
		parts := strings.Split(fileName, "-")
		if len(parts) != 2 || !strings.HasSuffix(parts[1], ".plog") {
			continue // Skip files that don't match the X-Y.plog pattern
		}

		fileDataUUID := parts[0]
		fileVersionUUID := strings.TrimSuffix(parts[1], ".plog")

		// Check if the file matches the dataUUID and is in the filterMap
		if fileDataUUID == dataUUID {
			if _, exists := filterMap[fileVersionUUID]; exists {
				err := processPlogFile(fileName, filterMap[fileVersionUUID])
				if err != nil {
					return fmt.Errorf("error processing file %s: %w", fileName, err)
				}
			}
		}
	}

	return nil
}

const jsonMsgTypeID uint16 = 1 // used for protolog

func processPlogFile(fileName string, filteredIDs IDSet) error {
	fr, err := os.Open(filepath.Join(inPath, fileName))
	if err != nil {
		return fmt.Errorf("error opening input plog file: %w", err)
	}
	defer fr.Close()

	fw, err := os.Create(filepath.Join(outPath, fileName))
	if err != nil {
		return fmt.Errorf("error creating output plog file: %w", err)
	}
	defer fw.Close()

	reader := protolog.NewReader(fr)
	writer := protolog.NewTypedWriter(jsonMsgTypeID, fw)
	numWritten := 0
	numFiltered := 0
	numTotal := 0
	for {
		typeID, jsonbytes, err := reader.Next()
		if err == io.EOF {
			break
		}
		numTotal++
		if typeID != jsonMsgTypeID {
			fmt.Printf("Unknown message type skipped in mutation log %q: %s\n", fileName, string(jsonbytes))
		} else {
			var jsonData map[string]interface{}
			err = json.Unmarshal(jsonbytes, &jsonData)
			if err != nil {
				return fmt.Errorf("error parsing JSON record: %w", err)
			}

			if mutationID, ok := jsonData["MutationID"].(float64); ok {
				if _, exists := filteredIDs[uint64(mutationID)]; !exists {
					if _, err = writer.Write(jsonbytes); err != nil {
						return fmt.Errorf("error writing record in log %q, mutid %d: %w", fileName, mutationID, err)
					}
					numWritten++
				} else {
					numFiltered++
				}
			}
		}
	}
	fmt.Printf("Wrote %d records to %q, filtered %d (total %d) records\n", numWritten, fileName, numFiltered, numTotal)
	return nil
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 3 || *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	args := flag.Args()
	dataUUID = args[0]
	inPath = args[1]
	outPath = args[2]

	if *filterJSON == "" {
		fmt.Print("-filter must be followed by the CSV filename containing mutation IDs to remove from log")
		os.Exit(1)
	}
	filterMap, err := readFilterJSON(*filterJSON)
	if err != nil {
		fmt.Printf("Error retrieving filtered mutations from %q: %v\n", *filterJSON, err)
		os.Exit(1)
	}

	// Create the output directory if it doesn't exist.
	err = os.MkdirAll(outPath, 0755)
	if err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}

	// Process the plog files
	err = processPlogFiles(dataUUID, filterMap)
	if err != nil {
		fmt.Printf("Error processing plog files: %v\n", err)
		return
	}
}
