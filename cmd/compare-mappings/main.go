// Command compare-mappings checks whether two DVID /mappings endpoint dumps
// are equivalent (same supervoxel->label pairs) regardless of line order.
//
// Usage:
//
//	compare-mappings <file-a> <file-b>
//
// Each file has lines of the form "supervoxel_id label_id" (space-separated uint64s).
// Exits 0 if the files are equivalent, 1 if they differ, 2 on error.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file-a> <file-b>\n", os.Args[0])
		os.Exit(2)
	}
	fileA, fileB := os.Args[1], os.Args[2]

	fmt.Fprintf(os.Stderr, "Loading %s ...\n", fileA)
	mapA, nA, err := loadMappings(fileA)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", fileA, err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d mappings from %s\n", nA, fileA)

	fmt.Fprintf(os.Stderr, "Comparing against %s ...\n", fileB)
	nB, onlyB, mismatched, err := compareMappings(mapA, fileB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", fileB, err)
		os.Exit(2)
	}

	// After compareMappings, mapA contains entries only in A (not found in B).
	onlyA := len(mapA)

	fmt.Fprintf(os.Stderr, "Loaded %d mappings from %s\n", nB, fileB)
	fmt.Printf("File A: %d mappings\n", nA)
	fmt.Printf("File B: %d mappings\n", nB)
	fmt.Printf("Only in A: %d\n", onlyA)
	fmt.Printf("Only in B: %d\n", onlyB)
	fmt.Printf("Mismatched values: %d\n", mismatched)

	if onlyA == 0 && onlyB == 0 && mismatched == 0 {
		fmt.Println("PASS: mappings are equivalent")
		os.Exit(0)
	}

	fmt.Println("FAIL: mappings differ")

	// Print a sample of differences (up to 10 each).
	if onlyA > 0 {
		fmt.Printf("\nSample entries only in A (up to 10):\n")
		i := 0
		for sv, label := range mapA {
			fmt.Printf("  %d %d\n", sv, label)
			i++
			if i >= 10 {
				break
			}
		}
	}

	os.Exit(1)
}

// loadMappings reads all supervoxel->label pairs into a map.
func loadMappings(path string) (map[uint64]uint64, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	m := make(map[uint64]uint64, 350_000_000) // pre-size for large files
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256), 256)
	n := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		sv, label, err := parseLine(line)
		if err != nil {
			return nil, 0, fmt.Errorf("line %d: %w", n+1, err)
		}
		if existing, ok := m[sv]; ok {
			return nil, 0, fmt.Errorf("line %d: duplicate supervoxel %d (labels %d and %d)", n+1, sv, existing, label)
		}
		m[sv] = label
		n++
		if n%50_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "  read %dM mappings...\n", n/1_000_000)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return m, n, nil
}

// compareMappings streams file B, checking each entry against mapA.
// Matching entries are deleted from mapA. Returns counts of entries only in B
// and entries with mismatched values. After return, mapA contains only entries
// not found in B.
func compareMappings(mapA map[uint64]uint64, pathB string) (int, int, int, error) {
	f, err := os.Open(pathB)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256), 256)
	var nB, onlyB, mismatched int
	var sampleOnlyB []string
	var sampleMismatch []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		sv, label, err := parseLine(line)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("line %d: %w", nB+1, err)
		}
		nB++
		if nB%50_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "  compared %dM mappings...\n", nB/1_000_000)
		}
		labelA, ok := mapA[sv]
		if !ok {
			onlyB++
			if len(sampleOnlyB) < 10 {
				sampleOnlyB = append(sampleOnlyB, fmt.Sprintf("  %d %d", sv, label))
			}
			continue
		}
		if labelA != label {
			mismatched++
			if len(sampleMismatch) < 10 {
				sampleMismatch = append(sampleMismatch, fmt.Sprintf("  sv %d: A=%d B=%d", sv, labelA, label))
			}
		}
		delete(mapA, sv)
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, 0, err
	}

	if len(sampleOnlyB) > 0 {
		fmt.Printf("\nSample entries only in B (up to 10):\n")
		for _, s := range sampleOnlyB {
			fmt.Println(s)
		}
	}
	if len(sampleMismatch) > 0 {
		fmt.Printf("\nSample mismatched values (up to 10):\n")
		for _, s := range sampleMismatch {
			fmt.Println(s)
		}
	}

	return nB, onlyB, mismatched, nil
}

func parseLine(line string) (uint64, uint64, error) {
	parts := strings.Fields(line)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected 2 fields, got %d: %q", len(parts), line)
	}
	sv, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad supervoxel %q: %w", parts[0], err)
	}
	label, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad label %q: %w", parts[1], err)
	}
	return sv, label, nil
}
