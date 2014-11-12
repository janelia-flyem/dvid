package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"encoding/json"
)

func TestHTTP(t *testing.T, method, urlStr string, payload io.Reader) []byte {
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		t.Fatalf("Unsuccessful %s on %q: %s\n", method, urlStr, err.Error())
	}
	w := httptest.NewRecorder()
	ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Bad server response (%d) to %s on %q\n", w.Code, method, urlStr)
	}
	return w.Body.Bytes()
}

// NewTestRepo returns a repo on a running server suitable for testing.
func NewTestRepo(t *testing.T) (uuid string) {
	metadata := `{"alias": "testRepo", "description": "A test repository"}`
	apiStr := WebAPIPath + "repos"
	response := TestHTTP(t, "POST", apiStr, bytes.NewBufferString(metadata))

	// Parse the returned root UUID
	parsedResponse := struct {
		Root string
	}{}
	if err := json.Unmarshal(response, &parsedResponse); err != nil {
		t.Fatalf("Couldn't decode JSON response to new repo request: %s\n", err.Error())
	}
	return parsedResponse.Root
}
