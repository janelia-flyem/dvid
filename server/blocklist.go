package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

var (
	blockList blockListT
)

type blockListT struct {
	mu     sync.RWMutex
	active bool
	users  map[string]string // user id key, note value
	ips    map[string]string // ip match key, note value
}

func addBlock(blockMap map[string]string, data string) error {
	parts := strings.Split(data, ",")
	switch len(parts) {
	case 1:
		blockMap[parts[0]] = ""
	case 2:
		blockMap[parts[0]] = parts[1]
	default:
		return fmt.Errorf("bad blocklist line")
	}
	return nil
}

func loadBlockListFile() error {
	if len(tc.Server.BlockListFile) == 0 {
		return nil
	}
	dvid.Infof("Blocklist (%s) found.", tc.Server.BlockListFile)
	f, err := os.Open(tc.Server.BlockListFile)
	if err != nil {
		return err
	}
	blockList.mu.Lock()
	defer blockList.mu.Unlock()
	blockList.users = make(map[string]string)
	blockList.ips = make(map[string]string)

	// read each line in block list file
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "u="):
			if err := addBlock(blockList.users, line[2:]); err != nil {
				return fmt.Errorf("bad user blocklist line: %s", line)
			}
		case strings.HasPrefix(line, "ip="):
			if err := addBlock(blockList.ips, line[3:]); err != nil {
				return fmt.Errorf("bad ip blocklist line: %s", line)
			}
		default:
			return fmt.Errorf("bad line in blocklist file (%s): %s", tc.Server.BlockListFile, line)
		}
	}
	if len(blockList.users) > 0 || len(blockList.ips) > 0 {
		blockList.active = true
	}
	return nil
}

func blockedRequest(w http.ResponseWriter, r *http.Request) bool {
	if !blockList.active {
		return false
	}
	if len(blockList.users) > 0 {
		user := r.URL.Query().Get("u")
		note, found := blockList.users[user]
		if found {
			http.Error(w, fmt.Sprintf("User %q is blocked: %s", user, note), http.StatusTooManyRequests)
			return true
		}
	}
	if len(blockList.ips) > 0 {
		ip, err := requestSourceIP(r)
		if err != nil {
			dvid.Errorf("Error getting source IP for request: %v\n", err)
			return false
		}
		note, found := blockedIP(ip)
		if found {
			http.Error(w, fmt.Sprintf("IP %q is blocked: %s", ip, note), http.StatusTooManyRequests)
			return true
		}
	}
	return false
}

func blockedIP(ip string) (string, bool) {
	blockList.mu.RLock()
	defer blockList.mu.RUnlock()
	targetParts := strings.Split(ip, ".")
	for blockIP, note := range blockList.ips {
		match := true
		parts := strings.Split(blockIP, ".")
		for i := 0; i < len(parts); i++ {
			if parts[i] == "*" {
				continue
			}
			if parts[i] != targetParts[i] {
				match = false
				break
			}
		}
		if match {
			return note, true
		}
	}
	return "", false
}

// See https://www.refactoredtelegram.net/2021/01/a-simple-source-ip-address-filter-in-go/
func requestSourceIP(r *http.Request) (string, error) {
	// Check the Forward header
	forwardedHeader := r.Header.Get("Forwarded")
	if forwardedHeader != "" {
		parts := strings.Split(forwardedHeader, ",")
		firstPart := strings.TrimSpace(parts[0])
		subParts := strings.Split(firstPart, ";")
		for _, part := range subParts {
			normalisedPart := strings.ToLower(strings.TrimSpace(part))
			if strings.HasPrefix(normalisedPart, "for=") {
				return normalisedPart[4:], nil
			}
		}
	}

	// Check the X-Forwarded-For header
	xForwardedForHeader := r.Header.Get("X-Forwarded-For")
	if xForwardedForHeader != "" {
		parts := strings.Split(xForwardedForHeader, ",")
		firstPart := strings.TrimSpace(parts[0])
		return firstPart, nil
	}

	// Check on the request
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "", err
	}

	return host, nil
}
